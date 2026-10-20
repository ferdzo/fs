package api

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/storage"
)

// rangeGetPayload builds a deterministic payload whose bytes vary with their
// offset so range assertions can catch off-by-one slicing.
func rangeGetPayload(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return string(b)
}

// newRangeTestObject creates the standard test handler (1 KiB chunks) with a
// single 3500-byte object, which spans four chunks and ends mid-chunk.
func newRangeTestObject(t *testing.T) (*Handler, string) {
	t.Helper()
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	payload := rangeGetPayload(3500)
	if _, err := svc.PutObject("test-bucket", "data.bin", "application/octet-stream", strings.NewReader(payload)); err != nil {
		t.Fatalf("put object: %v", err)
	}
	return handler, payload
}

func TestGetObjectRangeHeadersAndBody(t *testing.T) {
	handler, payload := newRangeTestObject(t)

	cases := []struct {
		name        string
		rangeHeader string
		start, end  int64
	}{
		{"inside first chunk", "bytes=100-199", 100, 199},
		{"spans chunk boundary", "bytes=1000-1100", 1000, 1100},
		{"ends exactly on boundary", "bytes=0-1023", 0, 1023},
		{"length one on boundary", "bytes=1023-1023", 1023, 1023},
		{"starts on second chunk", "bytes=1024-1500", 1024, 1500},
		{"suffix", "bytes=-300", 3200, 3499},
		{"open ended", "bytes=3200-", 3200, 3499},
		{"whole object", "bytes=0-3499", 0, 3499},
		{"clamped end", "bytes=3000-9999", 3000, 3499},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test-bucket/data.bin", nil)
			req.Header.Set("Range", tc.rangeHeader)
			rec := httptest.NewRecorder()
			handler.router.ServeHTTP(rec, req)

			if rec.Code != http.StatusPartialContent {
				t.Fatalf("status = %d, want 206 (body: %s)", rec.Code, rec.Body.String())
			}
			length := tc.end - tc.start + 1
			if got := rec.Header().Get("Content-Length"); got != strconv.FormatInt(length, 10) {
				t.Fatalf("Content-Length = %q, want %d", got, length)
			}
			wantRange := "bytes " + strconv.FormatInt(tc.start, 10) + "-" + strconv.FormatInt(tc.end, 10) + "/3500"
			if got := rec.Header().Get("Content-Range"); got != wantRange {
				t.Fatalf("Content-Range = %q, want %q", got, wantRange)
			}
			if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
				t.Fatalf("Accept-Ranges = %q, want bytes", got)
			}
			if got := rec.Header().Get("ETag"); !strings.HasPrefix(got, `"`) {
				t.Fatalf("ETag = %q, want quoted", got)
			}
			wantBody := payload[tc.start : tc.end+1]
			if got := rec.Body.String(); got != wantBody {
				t.Fatalf("body = %q, want %q", got, wantBody)
			}
		})
	}
}

func TestGetObjectRangeInvalidAndMultiRange(t *testing.T) {
	handler, _ := newRangeTestObject(t)

	for _, tc := range []struct {
		name        string
		rangeHeader string
	}{
		{"start past end", "bytes=3500-4000"},
		{"malformed", "bytes=abc-def"},
		{"multi range", "bytes=0-1,3-4"},
		{"zero suffix", "bytes=-0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test-bucket/data.bin", nil)
			req.Header.Set("Range", tc.rangeHeader)
			rec := httptest.NewRecorder()
			handler.router.ServeHTTP(rec, req)

			if rec.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("status = %d, want 416 (body: %s)", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Range"); got != "bytes */3500" {
				t.Fatalf("Content-Range = %q, want %q", got, "bytes */3500")
			}
			if !strings.Contains(rec.Body.String(), "<Code>InvalidRange</Code>") {
				t.Fatalf("expected InvalidRange XML, got: %s", rec.Body.String())
			}
		})
	}
}

func TestGetObjectWithoutRangeStillFullBody(t *testing.T) {
	handler, payload := newRangeTestObject(t)

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/data.bin", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != "3500" {
		t.Fatalf("Content-Length = %q, want 3500", got)
	}
	body, _ := io.ReadAll(rec.Body)
	if string(body) != payload {
		t.Fatalf("body = %q, want full payload", body)
	}
}

func TestGetObjectInvalidRangeForMissingObjectIsNoSuchKey(t *testing.T) {
	handler, _ := newRangeTestObject(t)

	req := httptest.NewRequest(http.MethodGet, "/test-bucket/missing.bin", nil)
	req.Header.Set("Range", "bytes=0-9")
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body: %s)", rec.Code, rec.Body.String())
	}
}

const (
	rangeMultipartChunkSize = 8192000
	rangeMultipartPartSize  = 5 * 1024 * 1024
)

// newDefaultChunkMultipartHandler creates a handler whose blob store uses the
// production default chunk size and a two-part multipart object. Each 5 MiB
// part is shorter than the chunk size, so the object has two short chunks.
func newDefaultChunkMultipartHandler(t *testing.T) (*Handler, []byte) {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, rangeMultipartChunkSize)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() { _ = svc.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false)
	handler.setupRoutes()

	const bucket = "test-bucket"
	const key = "multipart.bin"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	payload := make([]byte, 2*rangeMultipartPartSize)
	for i := range payload {
		payload[i] = byte(i*37 + i/251)
	}
	uploadID := multipartLifeInit(t, handler, bucket, key)
	etag1 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 1, payload[:rangeMultipartPartSize])
	etag2 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 2, payload[rangeMultipartPartSize:])
	completed := multipartLifeComplete(t, handler, bucket, key, uploadID, []models.CompletedPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	})
	if completed.Code != http.StatusOK {
		t.Fatalf("complete multipart status = %d (body: %s)", completed.Code, completed.Body.String())
	}
	return handler, payload
}

func assertMultipartRangeRequest(t *testing.T, handler *Handler, key string, start, length int64, payload []byte) {
	t.Helper()
	end := start + length - 1
	req := httptest.NewRequest(http.MethodGet, "/test-bucket/"+key, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("%s [%d,%d] status = %d, want 206 (body: %s)", key, start, end, rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.FormatInt(length, 10) {
		t.Fatalf("%s Content-Length = %q, want %d", key, got, length)
	}
	wantRange := fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload))
	if got := rec.Header().Get("Content-Range"); got != wantRange {
		t.Fatalf("%s Content-Range = %q, want %q", key, got, wantRange)
	}
	want := payload[start : start+length]
	if got := rec.Body.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("%s [%d,%d] body returned %d bytes, want %d; content mismatch", key, start, end, len(got), len(want))
	}
}

func multipartRangeCases(payloadLen int64) []struct {
	name        string
	start, size int64
} {
	return []struct {
		name        string
		start, size int64
	}{
		{"crosses part boundary", rangeMultipartPartSize - 80, 101},
		{"inside second part", 6_000_000, 1000},
		{"review case one MiB near end", 9_437_184, 1_048_576},
		{"starts on part boundary", rangeMultipartPartSize, 100},
		{"ends exactly on part boundary", rangeMultipartPartSize - 100, 100},
		{"suffix at end", payloadLen - 100, 100},
	}
}

func TestGetObjectRangeMultipartPartBoundary(t *testing.T) {
	handler, payload := newDefaultChunkMultipartHandler(t)
	for _, tc := range multipartRangeCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertMultipartRangeRequest(t, handler, "multipart.bin", tc.start, tc.size, payload)
		})
	}
}

func TestGetObjectRangeMultipartCopy(t *testing.T) {
	handler, payload := newDefaultChunkMultipartHandler(t)

	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/copy.bin", nil)
	copyReq.Header.Set("x-amz-copy-source", "/test-bucket/multipart.bin")
	copyRec := httptest.NewRecorder()
	handler.router.ServeHTTP(copyRec, copyReq)
	if copyRec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want 200 (body: %s)", copyRec.Code, copyRec.Body.String())
	}

	for _, tc := range multipartRangeCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertMultipartRangeRequest(t, handler, "copy.bin", tc.start, tc.size, payload)
		})
	}
}
