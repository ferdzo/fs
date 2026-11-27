package service

import (
	"errors"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func listTestChunks(tb testing.TB, store ChunkStore) ([]string, error) {
	tb.Helper()
	bs, ok := store.(*storage.BlobStore)
	if !ok {
		tb.Fatalf("ChunkStore is %T, want *storage.BlobStore", store)
	}
	return bs.ListChunks()
}

type faultInjectingMetadataStore struct {
	MetadataStore
	putErr       error
	enqueueCalls int
	lastEnqNow   time.Time
}

func (f *faultInjectingMetadataStore) PutObjectVersionConditional(manifest *models.ObjectManifest, ifNoneMatch, versionID string, now time.Time) (string, error) {
	if f.putErr != nil {
		return "", f.putErr
	}
	return f.MetadataStore.PutObjectVersionConditional(manifest, ifNoneMatch, versionID, now)
}

func (f *faultInjectingMetadataStore) EnqueueUnreferencedChunks(chunkIDs []string, now time.Time) error {
	f.enqueueCalls++
	f.lastEnqNow = now
	return f.MetadataStore.EnqueueUnreferencedChunks(chunkIDs, now)
}

type faultInjectingChunkStore struct {
	ChunkStore
	missingChunkID string
}

func (f *faultInjectingChunkStore) StatChunk(chunkID string) (time.Time, bool) {
	if chunkID == f.missingChunkID {
		return time.Time{}, false
	}
	return f.ChunkStore.StatChunk(chunkID)
}

func TestFaultInjectedMetadataFailureEnqueuesChunksAndReleasesPins(t *testing.T) {
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 4)
	if err != nil {
		_ = md.Close()
		t.Fatalf("NewBlobStore: %v", err)
	}

	injectedErr := errors.New("injected metadata commit failure")
	wrappedMD := &faultInjectingMetadataStore{
		MetadataStore: md,
		putErr:        injectedErr,
	}
	svc := NewObjectService(wrappedMD, blob, time.Hour, 0)
	svc.gcGrace = time.Nanosecond
	t.Cleanup(func() {
		_ = svc.Close()
	})

	if err := svc.CreateBucket("fault-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err = svc.PutObject("fault-bucket", "obj.bin", "text/plain", strings.NewReader("hello world"))
	if !errors.Is(err, injectedErr) {
		t.Fatalf("PutObject error = %v, want %v", err, injectedErr)
	}
	if wrappedMD.enqueueCalls != 1 {
		t.Fatalf("enqueueCalls = %d, want 1", wrappedMD.enqueueCalls)
	}
	if wrappedMD.lastEnqNow.IsZero() {
		t.Fatal("EnqueueUnreferencedChunks received zero timestamp")
	}
	if pins := svc.chunkPins.pinCount(); pins != 0 {
		t.Fatalf("pinCount after failed PutObject = %d, want 0", pins)
	}

	queued, err := wrappedMD.ListGCQueue(0)
	if err != nil {
		t.Fatalf("ListGCQueue: %v", err)
	}
	if len(queued) == 0 {
		t.Fatal("expected failed-ingest chunks in GC queue")
	}

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	remaining, err := listTestChunks(t, blob)
	if err != nil {
		t.Fatalf("listTestChunks: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining chunks after GC = %d, want 0", len(remaining))
	}
}

func TestFaultInjectedMissingChunkReleasesPins(t *testing.T) {
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 4)
	if err != nil {
		_ = md.Close()
		t.Fatalf("NewBlobStore: %v", err)
	}
	wrappedBlob := &faultInjectingChunkStore{ChunkStore: blob}
	svc := NewObjectService(md, wrappedBlob, time.Hour, 0)
	t.Cleanup(func() {
		_ = svc.Close()
	})

	if err := svc.CreateBucket("read-fault-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	manifest, err := svc.PutObject("read-fault-bucket", "item.txt", "text/plain", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	wrappedBlob.missingChunkID = manifest.Chunks[0]

	_, _, err = svc.GetObject("read-fault-bucket", "item.txt")
	if !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("GetObject error = %v, want ErrChunkMissing", err)
	}
	if pins := svc.chunkPins.pinCount(); pins != 0 {
		t.Fatalf("pinCount after ErrChunkMissing = %d, want 0", pins)
	}
}
