package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fs/logging"
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

func TestPutObjectRejectsSignedStreamingPayloadWithoutAuth(t *testing.T) {
	handler, _ := newTestObjectHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/streamed.bin", strings.NewReader("fake-framed-body"))
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501; body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "NotImplemented") {
		t.Fatalf("expected NotImplemented error XML, got: %s", rec.Body.String())
	}
}
