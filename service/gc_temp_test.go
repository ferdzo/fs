package service

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fs/metadata"
	"fs/storage"
)

// newGCTempTestService builds a service over a known root so the test can place
// ingest temp files next to the chunk shards, mirroring os.CreateTemp.
func newGCTempTestService(t *testing.T) (*ObjectService, string) {
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
	svc := NewObjectService(md, blob, time.Hour, 0)
	t.Cleanup(func() {
		_ = svc.Close()
	})
	return svc, root
}

func writeGCTempFile(t *testing.T, root, name string, age time.Duration) string {
	t.Helper()
	path := filepath.Join(root, "blobs", "ab", "cd", name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte("partial chunk"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if age > 0 {
		when := time.Now().Add(-age)
		if err := os.Chtimes(path, when, when); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
	}
	return path
}

func TestGarbageCollectRemovesStaleTempFilesAndPreservesChunkSweep(t *testing.T) {
	svc, root := newGCTempTestService(t)
	svc.gcGrace = time.Nanosecond // make orphan chunks sweep-eligible
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "keep.bin", "text/plain", strings.NewReader("keep")); err != nil {
		t.Fatalf("PutObject keep: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "gone.bin", "text/plain", strings.NewReader("gone")); err != nil {
		t.Fatalf("PutObject gone: %v", err)
	}
	if err := svc.DeleteObject("test-bucket", "gone.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	staleName := strings.Repeat("a", 64) + ".tmp-1111111111"
	freshName := strings.Repeat("b", 64) + ".tmp-2222222222"
	stalePath := writeGCTempFile(t, root, staleName, 2*time.Hour)
	freshPath := writeGCTempFile(t, root, freshName, 0)

	before, err := svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks before: %v", err)
	}
	if len(before) != 2 {
		t.Fatalf("chunks before GC = %d, want 2", len(before))
	}

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}

	if _, err := os.Lstat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("stale temp file survived GC: stat error = %v", err)
	}
	if _, err := os.Stat(freshPath); err != nil {
		t.Fatalf("fresh temp file was removed: %v", err)
	}

	after, err := svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks after: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("chunks after GC = %d, want 1 (referenced kept, orphan swept)", len(after))
	}

	stream, _, err := svc.GetObject("test-bucket", "keep.bin")
	if err != nil {
		t.Fatalf("GetObject keep: %v", err)
	}
	defer stream.Close()
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read keep: %v", err)
	}
	if string(got) != "keep" {
		t.Fatalf("keep.bin = %q, want %q", got, "keep")
	}
}

func TestGarbageCollectKeepsFreshInFlightTempFile(t *testing.T) {
	svc, root := newGCTempTestService(t)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	inFlightName := strings.Repeat("c", 64) + ".tmp-3333333333"
	inFlightPath := writeGCTempFile(t, root, inFlightName, 0)

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}

	if _, err := os.Stat(inFlightPath); err != nil {
		t.Fatalf("in-flight temp file was removed by GC: %v", err)
	}
}
