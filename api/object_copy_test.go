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

func newTestObjectHandler(t *testing.T) (*Handler, *service.ObjectService) {
	t.Helper()

	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("new metadata handler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 1024)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() {
		_ = svc.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false)
	handler.setupRoutes()
	return handler, svc
}

func TestPutObjectStoresDecodedKey(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/test-bucket/jsp-data-raw/vehicle_positions/year%3D2026/month%3D03/day%3D12/file.parquet", bytes.NewReader([]byte("PAR1data")))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d body=%s", rec.Code, rec.Body.String())
	}

	_, err := svc.HeadObject("test-bucket", "jsp-data-raw/vehicle_positions/year=2026/month=03/day=12/file.parquet")
	if err != nil {
		t.Fatalf("head decoded key: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/jsp-data-raw/vehicle_positions/year=2026/month=03/day=12/file.parquet", nil)
	getRec := httptest.NewRecorder()
	handler.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("unexpected get status: got %d body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "PAR1data" {
		t.Fatalf("unexpected get body: got %q", got)
	}
}

func TestCopyObjectCopiesCanonicalObject(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	putReq := httptest.NewRequest(http.MethodPut, "/test-bucket/source/year%3D2026/file.parquet", bytes.NewReader([]byte("PAR1copy")))
	putRec := httptest.NewRecorder()
	handler.router.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("unexpected put status: got %d body=%s", putRec.Code, putRec.Body.String())
	}

	copyReq := httptest.NewRequest(http.MethodPut, "/test-bucket/copied/year=2026/file.parquet", http.NoBody)
	copyReq.Header.Set("x-amz-copy-source", "/test-bucket/source/year%3D2026/file.parquet")
	copyRec := httptest.NewRecorder()
	handler.router.ServeHTTP(copyRec, copyReq)

	if copyRec.Code != http.StatusOK {
		t.Fatalf("unexpected copy status: got %d body=%s", copyRec.Code, copyRec.Body.String())
	}
	if !strings.Contains(copyRec.Body.String(), "<CopyObjectResult") {
		t.Fatalf("unexpected copy response body: %s", copyRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/test-bucket/copied/year=2026/file.parquet", nil)
	getRec := httptest.NewRecorder()
	handler.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("unexpected get status after copy: got %d body=%s", getRec.Code, getRec.Body.String())
	}
	if got := getRec.Body.String(); got != "PAR1copy" {
		t.Fatalf("unexpected copied body: got %q", got)
	}
}
