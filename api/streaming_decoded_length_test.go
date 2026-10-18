package api

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fs/metadata"
	"fs/models"
)

// TestPutObjectRejectsDecodedContentLengthMismatch proves an authenticated
// signed-streaming PUT whose signed x-amz-decoded-content-length disagrees with
// the bytes actually streamed is rejected with a 400 and leaves no object,
// both when more and when fewer bytes are streamed than declared.
func TestPutObjectRejectsDecodedContentLengthMismatch(t *testing.T) {
	payload := bytes.Repeat([]byte("streamed-payload-"), 100) // 1700 bytes
	chunks := [][]byte{payload[:700], payload[700:1400], payload[1400:]}

	cases := []struct {
		name     string
		declared int
	}{
		{"declared longer than streamed", len(payload) + 1},
		{"declared shorter than streamed", len(payload) - 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler, svc, _ := newPutObjectUserHandler(t)

			req := httptest.NewRequest(http.MethodPut, "/test-bucket/len.bin", nil)
			req.Host = "127.0.0.1"
			signChunkedPutRequestDeclared(t, req, "delete-secret-1", chunks, false, tc.declared)
			rec := httptest.NewRecorder()
			handler.router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "InvalidArgument") {
				t.Fatalf("expected InvalidArgument, body=%s", rec.Body.String())
			}
			if _, err := svc.HeadObject("test-bucket", "len.bin"); !errors.Is(err, metadata.ErrObjectNotFound) {
				t.Fatalf("rejected streaming upload must not create len.bin, got err=%v", err)
			}
		})
	}
}

// TestStreamingMultiDeleteConsumesEOFBeforeMutation proves a signed-streaming
// multi-delete whose decoded length is wrong is rejected before any key is
// deleted: the XML handler must read the authenticated stream to EOF (terminal
// chunk + trailer checks) before it acts on the decoded body.
func TestStreamingMultiDeleteConsumesEOFBeforeMutation(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket"},
	})
	putTestObject(t, svc, "victim.txt")

	body := `<Delete><Object><Key>victim.txt</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequestDeclared(t, req, "delete-secret-1", [][]byte{[]byte(body)}, false, len(body)+1)

	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); err != nil {
		t.Fatalf("streaming multi-delete must not run before EOF verification: %v", err)
	}
}

// TestStreamingMultiDeleteRejectsTamperedTrailerSignature covers the trailer
// path: a valid decoded body whose trailer signature was altered after signing
// must be rejected without deleting. The trailer signature is only checked when
// the terminal chunk is consumed, so this also asserts the handler drains the
// authenticated stream before mutating.
func TestStreamingMultiDeleteRejectsTamperedTrailerSignature(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket"},
	})
	putTestObject(t, svc, "victim.txt")

	body := `<Delete><Object><Key>victim.txt</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequest(t, req, "delete-secret-1", [][]byte{[]byte(body)}, true)

	// Corrupt one hex digit of the trailer signature after it was computed.
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read signed body: %v", err)
	}
	const marker = "x-amz-trailer-signature:"
	idx := bytes.Index(raw, []byte(marker))
	if idx < 0 {
		t.Fatalf("signed trailer body missing %q", marker)
	}
	sigStart := idx + len(marker)
	if raw[sigStart] == '0' {
		raw[sigStart] = '1'
	} else {
		raw[sigStart] = '0'
	}
	req.Body = io.NopCloser(bytes.NewReader(raw))
	req.ContentLength = int64(len(raw))

	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", rec.Code, rec.Body.String())
	}
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); err != nil {
		t.Fatalf("tampered trailer must not delete victim.txt: %v", err)
	}
}

// TestPutObjectTrailerDecodedContentLengthMismatch covers the same rejection on
// the trailer-signed framing.
func TestPutObjectTrailerDecodedContentLengthMismatch(t *testing.T) {
	payload := []byte("trailer-length-payload")
	handler, svc, _ := newPutObjectUserHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/trailer-len.bin", nil)
	req.Host = "127.0.0.1"
	signChunkedPutRequestDeclared(t, req, "delete-secret-1", [][]byte{payload}, true, len(payload)+5)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if _, err := svc.HeadObject("test-bucket", "trailer-len.bin"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("rejected trailer upload must not create trailer-len.bin, got err=%v", err)
	}
}
