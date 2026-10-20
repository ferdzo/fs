package metadata

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"fs/models"

	bolt "go.etcd.io/bbolt"
)

func TestListMultipartUploadsBackfillsKeyIndex(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	handler, err := NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	const bucket = "backfill-bucket"
	if err := handler.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	first, err := handler.CreateMultipartUpload(bucket, "a/one.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload first: %v", err)
	}
	second, err := handler.CreateMultipartUpload(bucket, "a/two.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload second: %v", err)
	}

	// Simulate a database written before the key index existed.
	if err := handler.update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(multipartUploadKeyIndex)
	}); err != nil {
		t.Fatalf("drop key index: %v", err)
	}
	if err := handler.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("reopen NewMetadataHandler: %v", err)
	}
	defer reopened.Close()

	page, err := reopened.ListMultipartUploads(bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(page.Uploads) != 2 {
		t.Fatalf("uploads after backfill = %d, want 2", len(page.Uploads))
	}
	if page.Uploads[0].UploadID != first.UploadID || page.Uploads[1].UploadID != second.UploadID {
		t.Fatalf("upload order = %s,%s; want %s,%s",
			page.Uploads[0].UploadID, page.Uploads[1].UploadID, first.UploadID, second.UploadID)
	}
}

func TestListMultipartUploadsOrdersByInitiationThenUploadID(t *testing.T) {
	handler, err := NewMetadataHandler(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	defer handler.Close()
	const bucket = "order-bucket"
	if err := handler.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	records := []models.MultipartUpload{
		{Bucket: bucket, Key: "same.bin", UploadID: "u-late", CreatedAt: "2026-10-20T10:00:00Z", State: "pending"},
		{Bucket: bucket, Key: "same.bin", UploadID: "u-early-b", CreatedAt: "2026-10-20T09:00:00Z", State: "pending"},
		{Bucket: bucket, Key: "same.bin", UploadID: "u-early-a", CreatedAt: "2026-10-20T09:00:00Z", State: "pending"},
		{Bucket: bucket, Key: "a.bin", UploadID: "u-a", CreatedAt: "2026-10-20T11:00:00Z", State: "pending"},
		{Bucket: bucket, Key: "z.bin", UploadID: "u-z", CreatedAt: "2026-10-20T08:00:00Z", State: "pending"},
	}
	if err := handler.update(func(tx *bolt.Tx) error {
		uploadsBucket := tx.Bucket(multipartUploadIndex)
		keyIndexBucket := tx.Bucket(multipartUploadKeyIndex)
		for i := range records {
			payload, err := json.Marshal(records[i])
			if err != nil {
				return err
			}
			if err := uploadsBucket.Put([]byte(records[i].UploadID), payload); err != nil {
				return err
			}
			if err := keyIndexBucket.Put(multipartUploadKeyIndexKey(&records[i]), []byte{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed uploads: %v", err)
	}

	page, err := handler.ListMultipartUploads(bucket, "", "", "", "", 1000)
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	want := []string{"u-a", "u-early-a", "u-early-b", "u-late", "u-z"}
	if len(page.Uploads) != len(want) {
		t.Fatalf("uploads = %d, want %d", len(page.Uploads), len(want))
	}
	for i, uploadID := range want {
		if page.Uploads[i].UploadID != uploadID {
			t.Fatalf("upload[%d] = %s, want %s (order: %v)", i, page.Uploads[i].UploadID, uploadID, uploadIDs(page.Uploads))
		}
	}
}

func uploadIDs(uploads []models.MultipartUpload) []string {
	ids := make([]string, 0, len(uploads))
	for _, upload := range uploads {
		ids = append(ids, upload.UploadID)
	}
	return ids
}
