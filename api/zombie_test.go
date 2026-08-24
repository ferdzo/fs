package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fs/logging"
	"fs/metadata"
	"fs/service"
	"fs/storage"
)

func newShortTimeoutHandler(t *testing.T, d time.Duration) (*Handler, *service.ObjectService) {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(root + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := storage.NewBlobStore(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() { _ = svc.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false, d)
	handler.setupRoutes()
	return handler, svc
}

// The zombie: validly-signed PUT headers declaring 1 MiB, then silence.
func sendZombie(t *testing.T, addr, accessKey, secret, key string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	scope := strings.Join([]string{date, "us-east-1", "s3", "aws4_request"}, "/")
	mode := "UNSIGNED-PAYLOAD"
	cl := "1048576"

	type kv struct{ k, v string }
	pairs := []kv{
		{"host", addr},
		{"x-amz-content-sha256", mode},
		{"x-amz-date", amzDate},
		{"x-amz-decoded-content-length", cl},
	}
	var canon strings.Builder
	var names []string
	for _, p := range pairs {
		canon.WriteString(p.k + ":" + p.v + "\n")
		names = append(names, p.k)
	}
	signedRaw := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		http.MethodPut, "/" + key, "", canon.String(), signedRaw, mode,
	}, "\n")
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope,
		func() string { h := sha256.Sum256([]byte(canonicalRequest)); return hex.EncodeToString(h[:]) }(),
	}, "\n")
	k := signingKeyFor(secret, date)
	sig := hex.EncodeToString(hmacSHA256For(k, sts))

	headers := fmt.Sprintf(
		"PUT /test-bucket/%s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"x-amz-date: %s\r\n"+
			"x-amz-content-sha256: %s\r\n"+
			"x-amz-decoded-content-length: %s\r\n"+
			"Content-Length: %s\r\n"+
			"Authorization: AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s\r\n\r\n",
		key, addr, amzDate, mode, cl, cl, accessKey, scope, signedRaw, sig)
	_, _ = conn.Write([]byte(headers))
	return conn
}

func signingKeyFor(secret, date string) []byte {
	k := hmacSHA256For([]byte("AWS4"+secret), date)
	k = hmacSHA256For(k, "us-east-1")
	k = hmacSHA256For(k, "s3")
	return hmacSHA256For(k, "aws4_request")
}

func hmacSHA256For(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func TestZombiePutCannotExhaustSlots(t *testing.T) {
	const timeout = 1500 * time.Millisecond

	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(root + "/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := storage.NewBlobStore(root, 1024)
	if err != nil {
		t.Fatal(err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() { _ = svc.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false, timeout)
	handler.setupRoutes()

	ts := httptest.NewServer(handler.router)
	defer ts.Close()
	addr := ts.Listener.Addr().String()

	createBucket := func() {
		req := httptest.NewRequest(http.MethodPut, "/test-bucket", nil)
		req.Host = addr
		rec := httptest.NewRecorder()
		handler.router.ServeHTTP(rec, req)
	}
	createBucket()

	// 6 zombies: signed headers, declared body, zero bytes sent.
	var zombies []net.Conn
	for i := 0; i < 6; i++ {
		zombies = append(zombies, sendZombie(t, addr, "delete-user", "delete-secret-1", fmt.Sprintf("z%d.bin", i)))
	}

	// Sanity: while zombies are stalled (pre-deadline) the service still answers.
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz during stall failed: err=%v code=%v", err, resp.StatusCode)
	}
	resp.Body.Close()

	// After the idle deadline every zombie must be culled by the server.
	deadline := time.Now().Add(timeout + 2500*time.Millisecond)
	culled := 0
	buf := make([]byte, 1)
	for _, c := range zombies {
		_ = c.SetReadDeadline(deadline)
		for {
			if _, err := c.Read(buf); err != nil {
				culled++ // EOF/RST = server reaped it
				break
			}
			if time.Now().After(deadline) {
				break
			}
		}
		c.Close()
	}
	if culled != len(zombies) {
		t.Fatalf("only %d/%d zombies were reaped", culled, len(zombies))
	}

	// And the listener keeps serving afterwards.
	resp2, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp2.StatusCode != 200 {
		t.Fatalf("healthz after reap failed: err=%v code=%v", err, resp2.StatusCode)
	}
	resp2.Body.Close()
	fmt.Println("all zombies reaped, listener healthy")
}
