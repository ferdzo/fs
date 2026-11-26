package metadata

import (
	"fs/models"
	"path/filepath"
	"slices"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestChunkRefCountsAndGCQueueLifecycle(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "unver")
	mustCreateBucket(t, h, "ver")
	if err := h.SetBucketVersioning("ver", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	// Duplicate chunk inside a single manifest counts twice; shared chunk across
	// objects counts independently.
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k1", Chunks: []string{"c1", "c1", "c2"},
	}); err != nil {
		t.Fatalf("PutManifest k1: %v", err)
	}
	assertRefCount(t, h, "c1", 2)
	assertRefCount(t, h, "c2", 1)

	// Overwrite unversioned object: c2 drops to 0 and enters GC queue; c1 drops
	// from 2 to 1; c3 rises to 1.
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k1", Chunks: []string{"c1", "c3"},
	}); err != nil {
		t.Fatalf("PutManifest overwrite: %v", err)
	}
	assertRefCount(t, h, "c1", 1)
	assertRefCount(t, h, "c2", 0)
	assertRefCount(t, h, "c3", 1)
	assertGCQueueContains(t, h, "c2", true)

	// Reviving c2 removes it from the GC queue immediately.
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k2", Chunks: []string{"c2"},
	}); err != nil {
		t.Fatalf("PutManifest revive: %v", err)
	}
	assertRefCount(t, h, "c2", 1)
	assertGCQueueContains(t, h, "c2", false)

	// Deleting unversioned object enqueues its chunks.
	if err := h.DeleteManifest("unver", "k2"); err != nil {
		t.Fatalf("DeleteManifest k2: %v", err)
	}
	assertRefCount(t, h, "c2", 0)
	assertGCQueueContains(t, h, "c2", true)

	// Versioned puts keep historical refs alive; deleting only the explicit
	// version releases its chunk.
	v1 := mustPutVersion(t, h, versionedObject("ver", "vk", "e1", "vc1"))
	v2 := mustPutVersion(t, h, versionedObject("ver", "vk", "e2", "vc2"))
	assertRefCount(t, h, "vc1", 1)
	assertRefCount(t, h, "vc2", 1)

	// Soft delete creates a delete marker and preserves both versions.
	if _, err := h.DeleteObject("ver", "vk", ""); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	assertRefCount(t, h, "vc1", 1)
	assertRefCount(t, h, "vc2", 1)

	if _, err := h.DeleteObject("ver", "vk", v1); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	assertRefCount(t, h, "vc1", 0)
	assertRefCount(t, h, "vc2", 1)
	assertGCQueueContains(t, h, "vc1", true)

	if _, err := h.DeleteObject("ver", "vk", v2); err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	assertRefCount(t, h, "vc2", 0)
	assertGCQueueContains(t, h, "vc2", true)

	// Multipart upload part overwrite, completion, and abort.
	up, err := h.CreateMultipartUpload("unver", "mp")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(up.UploadID, models.UploadedPart{
		PartNumber: 1, ETag: "e1", Size: 5, Chunks: []string{"mp-old"},
	}); err != nil {
		t.Fatalf("PutMultipartPart old: %v", err)
	}
	assertRefCount(t, h, "mp-old", 1)

	if err := h.PutMultipartPart(up.UploadID, models.UploadedPart{
		PartNumber: 1, ETag: "e2", Size: 5, Chunks: []string{"mp-new"},
	}); err != nil {
		t.Fatalf("PutMultipartPart overwrite: %v", err)
	}
	assertRefCount(t, h, "mp-old", 0)
	assertRefCount(t, h, "mp-new", 1)
	assertGCQueueContains(t, h, "mp-old", true)

	if err := h.CompleteMultipartUpload(up.UploadID, &models.ObjectManifest{
		Size: 5, ETag: "e2-1", Chunks: []string{"mp-new"},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	assertRefCount(t, h, "mp-new", 1)

	upAbort, err := h.CreateMultipartUpload("unver", "mp-abort")
	if err != nil {
		t.Fatalf("CreateMultipartUpload abort: %v", err)
	}
	if err := h.PutMultipartPart(upAbort.UploadID, models.UploadedPart{
		PartNumber: 1, ETag: "ea", Size: 5, Chunks: []string{"mp-aborted"},
	}); err != nil {
		t.Fatalf("PutMultipartPart abort: %v", err)
	}
	assertRefCount(t, h, "mp-aborted", 1)
	if err := h.AbortMultipartUpload(upAbort.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	assertRefCount(t, h, "mp-aborted", 0)
	assertGCQueueContains(t, h, "mp-aborted", true)

	if err := h.RemoveGCQueueEntries([]string{"c2", "mp-old"}); err != nil {
		t.Fatalf("RemoveGCQueueEntries: %v", err)
	}
	assertGCQueueContains(t, h, "c2", false)
	assertGCQueueContains(t, h, "mp-old", false)
}

func TestChunkRefBackfillOnReopenAvoidsDoubleCountingVersionedLatest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	h, err := NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	mustCreateBucket(t, h, "legacy")
	mustCreateBucket(t, h, "versioned")
	if err := h.SetBucketVersioning("versioned", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	mustPutManifest(t, h, versionedObject("legacy", "k", "el", "chunk-legacy"))
	mustPutVersion(t, h, versionedObject("versioned", "k", "ev1", "chunk-v1"))
	mustPutVersion(t, h, versionedObject("versioned", "k", "ev2", "chunk-v2"))
	up, err := h.CreateMultipartUpload("legacy", "mp")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(up.UploadID, models.UploadedPart{
		PartNumber: 1, ETag: "ep", Size: 4, Chunks: []string{"chunk-mp"},
	}); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate an existing pre-refcount database by wiping __CHUNK_REFS__ and
	// __META_STATE__.
	rawDB, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	if err := rawDB.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket(chunkRefIndex)
		_ = tx.DeleteBucket(gcQueueIndex)
		_ = tx.DeleteBucket(metaStateIndex)
		return nil
	}); err != nil {
		t.Fatalf("wipe ref buckets: %v", err)
	}
	_ = rawDB.Close()

	reopened, err := NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("reopen NewMetadataHandler: %v", err)
	}
	defer reopened.Close()

	for _, id := range []string{"chunk-legacy", "chunk-v1", "chunk-v2", "chunk-mp"} {
		assertRefCount(t, reopened, id, 1)
	}
}

func assertRefCount(t *testing.T, h *MetadataHandler, chunkID string, want uint64) {
	t.Helper()
	got, err := h.ChunkRefCount(chunkID)
	if err != nil {
		t.Fatalf("ChunkRefCount(%q): %v", chunkID, err)
	}
	if got != want {
		t.Fatalf("ChunkRefCount(%q) = %d, want %d", chunkID, got, want)
	}
}

func assertGCQueueContains(t *testing.T, h *MetadataHandler, chunkID string, want bool) {
	t.Helper()
	queue, err := h.ListGCQueue(0)
	if err != nil {
		t.Fatalf("ListGCQueue: %v", err)
	}
	got := slices.Contains(queue, chunkID)
	if got != want {
		t.Fatalf("ListGCQueue contains %q = %v (queue=%v), want %v", chunkID, got, queue, want)
	}
}
