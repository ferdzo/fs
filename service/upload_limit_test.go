package service

import (
	"errors"
	"fs/metadata"
	"fs/storage"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPutObjectRejectsOversizedUpload(t *testing.T) {
	svc := newTestObjectService(t, 4)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err := svc.PutObject("test-bucket", "too-large.txt", "text/plain", strings.NewReader("12345"))
	if !errors.Is(err, ErrEntityTooLarge) {
		t.Fatalf("PutObject error = %v, want ErrEntityTooLarge", err)
	}
	if _, err := svc.HeadObject("test-bucket", "too-large.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("HeadObject error = %v, want ErrObjectNotFound", err)
	}
}

func TestPutObjectAllowsExactUploadLimit(t *testing.T) {
	svc := newTestObjectService(t, 4)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	manifest, err := svc.PutObject("test-bucket", "exact.txt", "text/plain", strings.NewReader("1234"))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if manifest.Size != 4 {
		t.Fatalf("manifest size = %d, want 4", manifest.Size)
	}
}

func TestUploadPartRejectsOversizedUpload(t *testing.T) {
	svc := newTestObjectService(t, 4)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := svc.CreateMultipartUpload("test-bucket", "object.txt")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	_, err = svc.UploadPart("test-bucket", "object.txt", upload.UploadID, 1, strings.NewReader("12345"))
	if !errors.Is(err, ErrEntityTooLarge) {
		t.Fatalf("UploadPart error = %v, want ErrEntityTooLarge", err)
	}
	parts, err := svc.ListMultipartParts("test-bucket", "object.txt", upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("stored parts = %d, want 0", len(parts))
	}
}

func TestGarbageCollectRemovesExpiredPendingMultipartChunks(t *testing.T) {
	svc := newTestObjectService(t, 1024)
	svc.multipartRetention = time.Nanosecond
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := svc.CreateMultipartUpload("test-bucket", "object.txt")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := svc.UploadPart("test-bucket", "object.txt", upload.UploadID, 1, strings.NewReader("part-data")); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	chunks, err := svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks before GC: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatalf("expected uploaded part chunks")
	}
	time.Sleep(time.Millisecond)

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	if _, err := svc.metadata.GetMultipartUpload(upload.UploadID); !errors.Is(err, metadata.ErrMultipartNotFound) {
		t.Fatalf("GetMultipartUpload error = %v, want ErrMultipartNotFound", err)
	}
	chunks, err = svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks after GC: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("chunks after GC = %d, want 0", len(chunks))
	}
}

func newTestObjectService(t *testing.T, maxUploadSize int64) *ObjectService {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}
	svc := NewObjectService(md, blob, time.Hour, maxUploadSize)
	t.Cleanup(func() {
		_ = svc.Close()
	})
	return svc
}
