package metadata

import (
	"errors"
	"fs/models"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestCleanupMultipartUploadsDeletesExpiredPendingUpload(t *testing.T) {
	h := newTestMetadataHandler(t)
	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	if err := h.CreateBucket("test-bucket", now); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := h.CreateMultipartUpload("test-bucket", "object.txt", "upload-expired", "", now)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 1, ETag: "etag", Size: 4, Chunks: []string{"chunk-id"}}, now); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	setMultipartUploadCreatedAt(t, h, upload.UploadID, now.Add(-2*time.Hour))

	cleaned, err := h.CleanupMultipartUploads(time.Hour, now)
	if err != nil {
		t.Fatalf("CleanupMultipartUploads: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	if _, err := h.GetMultipartUpload(upload.UploadID); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("GetMultipartUpload error = %v, want ErrMultipartNotFound", err)
	}
	if _, err := h.ListMultipartParts(upload.UploadID); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("ListMultipartParts error = %v, want ErrMultipartNotFound", err)
	}
}

func TestCleanupMultipartUploadsKeepsRecentPendingUpload(t *testing.T) {
	h := newTestMetadataHandler(t)
	now := time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)
	if err := h.CreateBucket("test-bucket", now); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := h.CreateMultipartUpload("test-bucket", "object.txt", "upload-recent", "", now)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	cleaned, err := h.CleanupMultipartUploads(time.Hour, now)
	if err != nil {
		t.Fatalf("CleanupMultipartUploads: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("cleaned = %d, want 0", cleaned)
	}
	if _, err := h.GetMultipartUpload(upload.UploadID); err != nil {
		t.Fatalf("recent upload should remain: %v", err)
	}
}

func TestCleanupMultipartUploadsDisabledForNonPositiveRetention(t *testing.T) {
	h := newTestMetadataHandler(t)
	cleaned, err := h.CleanupMultipartUploads(0, time.Now().UTC())
	if err != nil {
		t.Fatalf("CleanupMultipartUploads: %v", err)
	}
	if cleaned != 0 {
		t.Fatalf("cleaned = %d, want 0", cleaned)
	}
}

func newTestMetadataHandler(t *testing.T) *MetadataHandler {
	t.Helper()
	h, err := NewMetadataHandler(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Close()
	})
	return h
}

func setMultipartUploadCreatedAt(t *testing.T, h *MetadataHandler, uploadID string, createdAt time.Time) {
	t.Helper()
	if err := h.update(func(tx *bbolt.Tx) error {
		upload, uploadsBucket, err := getMultipartUploadFromTx(tx, uploadID)
		if err != nil {
			return err
		}
		upload.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		return putMultipartUpload(uploadsBucket, uploadID, upload)
	}); err != nil {
		t.Fatalf("set multipart created_at: %v", err)
	}
}
