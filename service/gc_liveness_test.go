package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// Regression for review finding C1: a stalled download used to hold
// gcMu.RLock() for the whole stream, so GarbageCollect queued on the write
// lock and Go's RWMutex then blocked every new reader — total service hang.
func TestGarbageCollectCompletesWhileStreamOpen(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	body := strings.Repeat("x", 1<<20)
	if _, err := svc.PutObject("test-bucket", "big.bin", "application/octet-stream",
		strings.NewReader(body)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	stream, _, err := svc.GetObject("test-bucket", "big.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer stream.Close()
	// Intentionally NOT reading: simulates a stalled/slow client.

	done := make(chan error, 1)
	go func() { done <- svc.GarbageCollect() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("GarbageCollect: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GarbageCollect blocked behind an open stream — C1 deadlock present")
	}

	// Pinned chunks must survive that GC cycle: drain and verify readability.
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("drain stream after GC: %v", err)
	}
	if len(got) != len(body) {
		t.Fatalf("stream length = %d, want %d — chunks reaped mid-read?", len(got), len(body))
	}
}

func TestGCDoesNotReapPinnedChunks(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = time.Nanosecond // make everything sweep-eligible
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "pinned.bin", "text/plain",
		strings.NewReader("pinned-content")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	stream, _, err := svc.GetObject("test-bucket", "pinned.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect while pinned: %v", err)
	}

	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read pinned stream after GC: %v", err)
	}
	if string(got) != "pinned-content" {
		t.Fatalf("pinned chunks were reaped mid-read: got %q", string(got))
	}
}

func TestGCSweepsUnreferencedChunks(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = time.Nanosecond
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "gone.bin", "text/plain",
		strings.NewReader("short-lived")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if err := svc.DeleteObject("test-bucket", "gone.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	before, err := listTestChunks(t, svc.blob)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	after, err := listTestChunks(t, svc.blob)
	if err != nil {
		t.Fatalf("ListChunks after: %v", err)
	}
	if len(after) >= len(before) {
		t.Fatalf("orphan not swept: before=%d after=%d", len(before), len(after))
	}
}

func TestFailedIngestEnqueuesOrphanChunksForGC(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = time.Nanosecond
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// PutObject into a nonexistent bucket fails at metadata commit after writing
	// chunks to disk; those chunks must be enqueued and swept by GC.
	if _, err := svc.PutObject("no-such-bucket", "k", "text/plain", strings.NewReader("orphan-on-fail")); err == nil {
		t.Fatal("expected PutObject into missing bucket to fail")
	}
	chunksBefore, err := listTestChunks(t, svc.blob)
	if err != nil {
		t.Fatalf("ListChunks before: %v", err)
	}
	if len(chunksBefore) == 0 {
		t.Fatal("expected orphan chunk written before metadata failure")
	}

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	chunksAfter, err := listTestChunks(t, svc.blob)
	if err != nil {
		t.Fatalf("ListChunks after: %v", err)
	}
	if len(chunksAfter) != 0 {
		t.Fatalf("orphan chunks after GC = %d, want 0", len(chunksAfter))
	}
}

func TestCloseWaitsForActiveRunGC(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.RunGC(ctx, 5*time.Millisecond)
	time.Sleep(25 * time.Millisecond)

	if err := svc.Close(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Close: %v", err)
	}
}
