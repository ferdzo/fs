package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fs/logging"
	"fs/metadata"
	"fs/service"
	"fs/storage"
)

func TestPutObjectReturnsEntityTooLarge(t *testing.T) {
	handler, svc := newUploadLimitHandler(t, 4)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/too-large.txt", strings.NewReader("12345"))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "EntityTooLarge") {
		t.Fatalf("expected EntityTooLarge response, body=%s", rec.Body.String())
	}
}

func TestUploadPartReturnsEntityTooLarge(t *testing.T) {
	handler, svc := newUploadLimitHandler(t, 4)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := svc.CreateMultipartUpload("test-bucket", "object.txt")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/object.txt?partNumber=1&uploadId="+upload.UploadID, bytes.NewReader([]byte("12345")))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "EntityTooLarge") {
		t.Fatalf("expected EntityTooLarge response, body=%s", rec.Body.String())
	}
}

func newUploadLimitHandler(t *testing.T, maxUploadSize int64) (*Handler, *service.ObjectService) {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("new metadata handler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	svc := service.NewObjectService(md, blob, time.Hour, maxUploadSize)
	t.Cleanup(func() {
		_ = svc.Close()
	})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false)
	handler.setupRoutes()
	return handler, svc
}
