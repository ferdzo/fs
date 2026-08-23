package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"fs/auth"
	"fs/logging"
	"fs/models"
	"fs/service"
)

func TestGetObjectAcceptsCaseInsensitiveRangeUnit(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "data.bin", "application/octet-stream", strings.NewReader("0123456789")); err != nil {
		t.Fatalf("put object: %v", err)
	}

	for _, rangeHeader := range []string{"bytes=2-5", "BYTES=2-5", "Bytes=2-5"} {
		req := httptest.NewRequest(http.MethodGet, "/test-bucket/data.bin", nil)
		req.Header.Set("Range", rangeHeader)
		rec := httptest.NewRecorder()
		handler.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusPartialContent {
			t.Fatalf("Range %q: status = %d, want %d (body: %s)", rangeHeader, rec.Code, http.StatusPartialContent, rec.Body.String())
		}
		body, _ := io.ReadAll(rec.Body)
		if string(body) != "2345" {
			t.Fatalf("Range %q: body = %q, want %q", rangeHeader, body, "2345")
		}
	}
}

func TestRecovererReturnsS3XMLErrorOnPanic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(nil, logger, logging.Config{}, nil, false)
	handler.router.Get("/boom", func(w http.ResponseWriter, r *http.Request) {
		panic("exploded")
	})

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<Code>InternalError</Code>") {
		t.Fatalf("expected S3 XML error body, got: %s", body)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/xml") {
		t.Fatalf("expected XML content type, got %q", rec.Header().Get("Content-Type"))
	}
}

func TestPutBucketSetsLocationHeader(t *testing.T) {
	handler, _ := newTestObjectHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/new-bucket", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != "/new-bucket" {
		t.Fatalf("Location header = %q, want %q", got, "/new-bucket")
	}
}

func TestHeadObjectEchoesContentTypeAndAcceptRanges(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "doc.txt", "text/plain", strings.NewReader("hello")); err != nil {
		t.Fatalf("put object: %v", err)
	}

	req := httptest.NewRequest(http.MethodHead, "/test-bucket/doc.txt", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("Content-Type = %q, want text/plain", got)
	}
	if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("Accept-Ranges = %q, want bytes", got)
	}
}

func TestCompleteMultipartUploadAcceptsUppercaseETagFromClient(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	initReq := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploads", nil)
	initRec := httptest.NewRecorder()
	handler.router.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("initiate multipart status = %d", initRec.Code)
	}

	uploadID := extractUploadID(t, initRec.Body.String())
	partData := bytes.Repeat([]byte("p"), 5*1024*1024+1)
	partReq := httptest.NewRequest(http.MethodPut, "/test-bucket/big.bin?uploadId="+uploadID+"&partNumber=1", bytes.NewReader(partData))
	partRec := httptest.NewRecorder()
	handler.router.ServeHTTP(partRec, partReq)
	if partRec.Code != http.StatusOK {
		t.Fatalf("upload part status = %d body=%s", partRec.Code, partRec.Body.String())
	}
	partETag := strings.ToUpper(strings.Trim(partRec.Header().Get("ETag"), `"`))

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;` + partETag + `&quot;</ETag></Part></CompleteMultipartUpload>`
	completeReq := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploadId="+uploadID, strings.NewReader(completeBody))
	completeRec := httptest.NewRecorder()
	handler.router.ServeHTTP(completeRec, completeReq)

	if completeRec.Code != http.StatusOK {
		t.Fatalf("complete multipart status = %d body=%s", completeRec.Code, completeRec.Body.String())
	}
	if !strings.Contains(completeRec.Body.String(), "<ETag>") {
		t.Fatalf("expected CompleteMultipartUploadResult XML, got: %s", completeRec.Body.String())
	}
}

func extractUploadID(t *testing.T, xmlBody string) string {
	t.Helper()
	marker := "<UploadId>"
	start := strings.Index(xmlBody, marker)
	if start < 0 {
		t.Fatalf("no UploadId in response: %s", xmlBody)
	}
	start += len(marker)
	end := strings.Index(xmlBody[start:], "</UploadId>")
	if end < 0 {
		t.Fatalf("unterminated UploadId in response: %s", xmlBody)
	}
	return xmlBody[start : start+end]
}

func hmacSha256Test(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func testSigningKey(secret, date string) []byte {
	k := hmacSha256Test([]byte("AWS4"+secret), date)
	k = hmacSha256Test(k, "us-east-1")
	k = hmacSha256Test(k, "s3")
	return hmacSha256Test(k, "aws4_request")
}

// signChunkedPutRequest turns req into a fully signed STREAMING-AWS4-HMAC-SHA256
// PUT whose body is the aws-chunked encoding of chunks (optionally with a
// checksum trailer). Returns nothing; mutates req headers/body.
func signChunkedPutRequest(t *testing.T, req *http.Request, secret string, chunks [][]byte, trailer bool) {
	t.Helper()
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	scope := strings.Join([]string{date, "us-east-1", "s3", "aws4_request"}, "/")
	mode := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	if trailer {
		mode += "-TRAILER"
	}
	decodedLen := 0
	for _, c := range chunks {
		decodedLen += len(c)
	}

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"}
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", mode)
	req.Header.Set("x-amz-decoded-content-length", strconv.Itoa(decodedLen))
	req.Header.Set("Content-Encoding", "aws-chunked")

	canonicalHeaders := strings.Join([]string{
		"host:" + strings.TrimSpace(req.Host),
		"x-amz-content-sha256:" + mode,
		"x-amz-date:" + amzDate,
		"x-amz-decoded-content-length:" + strconv.Itoa(decodedLen),
		"",
	}, "\n")
	signedHeadersRaw := strings.Join(signedHeaders, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalTestQuery(req.URL.RawQuery),
		canonicalHeaders,
		signedHeadersRaw,
		mode,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	key := testSigningKey(secret, date)
	seedSig := hex.EncodeToString(hmacSha256Test(key, stringToSign))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		"delete-user", scope, signedHeadersRaw, seedSig))

	var b strings.Builder
	prev := seedSig
	for _, chunk := range chunks {
		chunkHash := sha256.Sum256(chunk)
		sts := strings.Join([]string{"AWS4-HMAC-SHA256-PAYLOAD", amzDate, scope, prev,
			hex.EncodeToString(func() []byte { s := sha256.Sum256(nil); return s[:] }()),
			"", hex.EncodeToString(chunkHash[:])}, "\n")
		sig := hex.EncodeToString(hmacSha256Test(key, sts))
		fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(chunk), sig)
		b.Write(chunk)
		b.WriteString("\r\n")
		prev = sig
	}
	finalStsParts := []string{"AWS4-HMAC-SHA256-PAYLOAD", amzDate, scope, prev,
		hex.EncodeToString(func() []byte { s := sha256.Sum256(nil); return s[:] }()), "", ""}
	if trailer {
		b.WriteString("0;chunk-signature=")
		sts := strings.Join([]string{"AWS4-HMAC-SHA256-TRAILER", amzDate, scope, prev,
			"x-amz-checksum-crc32c:dGVzdA==\n"}, "\n")
		sig := hex.EncodeToString(hmacSha256Test(key, sts))
		b.WriteString(sig)
		b.WriteString("\r\nx-amz-checksum-crc32c:dGVzdA==\r\n\r\n")
	} else {
		sig := hex.EncodeToString(hmacSha256Test(key, strings.Join(finalStsParts[:len(finalStsParts)-1], "\n")+"\n"))
		b.WriteString("0;chunk-signature=" + sig + "\r\n\r\n")
	}
	req.Body = io.NopCloser(strings.NewReader(b.String()))
	req.ContentLength = int64(b.Len())
}

func newPutObjectUserHandler(t *testing.T) (*Handler, *service.ObjectService, *auth.Service) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:PutObject"}, Bucket: "test-bucket"},
	})
	return handler, svc, authSvc
}

func TestPutObjectAcceptsVerifiedSignedStreamingChunks(t *testing.T) {
	handler, svc, _ := newPutObjectUserHandler(t)

	payload := bytes.Repeat([]byte("streamed-payload-"), 100)
	chunks := [][]byte{payload[:700], payload[700:1400], payload[1400:]}

	req := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploads&delete=1", nil) // bypass attempt shape
	req = httptest.NewRequest(http.MethodPut, "/test-bucket/streamed.bin", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequest(t, req, "delete-secret-1", chunks, false)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	m, err := svc.HeadObject("test-bucket", "streamed.bin")
	if err != nil {
		t.Fatalf("head object: %v", err)
	}
	if m.Size != int64(len(payload)) {
		t.Fatalf("stored size = %d, want %d", m.Size, len(payload))
	}
	stream, _, err := svc.GetObject("test-bucket", "streamed.bin")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer stream.Close()
	got, _ := io.ReadAll(stream)
	if !bytes.Equal(got, payload) {
		t.Fatalf("decoded content mismatch: got %d bytes", len(got))
	}
}

func TestPutObjectRejectsTamperedSignedStreamingChunk(t *testing.T) {
	handler, _, _ := newPutObjectUserHandler(t)

	payload := []byte("tamper-target-payload-0123456789")
	chunks := [][]byte{payload}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/evil.bin", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequest(t, req, "delete-secret-1", chunks, false)
	// corrupt the first payload byte AFTER signing
	body, _ := io.ReadAll(req.Body)
	headerEnd := bytes.Index(body, []byte("\r\n"))
	body[headerEnd+2] ^= 0xFF
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))

	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 SignatureDoesNotMatch; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "SignatureDoesNotMatch") {
		t.Fatalf("expected SignatureDoesNotMatch code, body=%s", rec.Body.String())
	}
}

func TestPutObjectTrailerModeAccepted(t *testing.T) {

	handler, svc, _ := newPutObjectUserHandler(t)

	payload := []byte("trailer-mode-payload")
	req := httptest.NewRequest(http.MethodPut, "/test-bucket/trailer.bin", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequest(t, req, "delete-secret-1", [][]byte{payload}, true)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	m, err := svc.HeadObject("test-bucket", "trailer.bin")
	if err != nil || m.Size != int64(len(payload)) {
		t.Fatalf("trailer object wrong: size=%d err=%v", m.Size, err)
	}
}
