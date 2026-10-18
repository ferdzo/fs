package service

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"fs/models"
)

func TestGarbageCollectRetainsChunkWrittenAfterSnapshot(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	payload := "orphan-payload"
	if _, err := svc.PutObject("test-bucket", "old.bin", "text/plain", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject old: %v", err)
	}
	if err := svc.DeleteObject("test-bucket", "old.bin"); err != nil {
		t.Fatalf("DeleteObject old: %v", err)
	}

	snapshot := make(chan struct{})
	resume := make(chan struct{})
	svc.testHookAfterGCSnapshot = func() {
		close(snapshot)
		<-resume
	}

	gcDone := make(chan error, 1)
	go func() {
		gcDone <- svc.GarbageCollect()
	}()

	select {
	case <-snapshot:
	case <-time.After(5 * time.Second):
		t.Fatal("GarbageCollect did not reach the post-snapshot hook")
	}

	// The new object uses identical bytes, so it reuses the orphan chunk ID
	// after GC already captured its reference snapshot.
	if _, err := svc.PutObject("test-bucket", "new.bin", "text/plain", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject new: %v", err)
	}

	close(resume)
	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatalf("GarbageCollect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GarbageCollect did not finish after resume")
	}

	stream, _, err := svc.GetObject("test-bucket", "new.bin")
	if err != nil {
		t.Fatalf("GetObject new: %v", err)
	}
	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read new object after GC: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("new object content = %q, want %q", string(got), payload)
	}
}

func TestGarbageCollectCompletesWithUnreadMultiChunkStream(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = time.Nanosecond
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	payload := "abcdefghij"
	if _, err := svc.PutObject("test-bucket", "multi.bin", "text/plain", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	stream, _, err := svc.GetObject("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	// Intentionally do not read the body: the pump stays blocked in Write.
	if err := svc.DeleteObject("test-bucket", "multi.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	gcDone := make(chan error, 1)
	go func() {
		gcDone <- svc.GarbageCollect()
	}()
	select {
	case err := <-gcDone:
		if err != nil {
			t.Fatalf("GarbageCollect while stream unread: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GarbageCollect blocked on an unread stream")
	}

	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read pinned stream after GC: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("stream content = %q, want %q", string(got), payload)
	}

	// Once the reader closes the lease, the now-unreferenced chunks can be
	// collected by a later pass.
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect after close: %v", err)
	}
	chunks, err := svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("chunks after close and GC = %d, want 0", len(chunks))
	}
}

func TestGarbageCollectRetainsCopySourceChunks(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	payload := "copy-source-payload"
	if _, err := svc.PutObject("test-bucket", "src.bin", "text/plain", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject source: %v", err)
	}

	pinned := make(chan struct{})
	resume := make(chan struct{})
	svc.testHookCopyAfterPin = func() {
		close(pinned)
		<-resume
	}

	copyDone := make(chan error, 1)
	go func() {
		_, err := svc.CopyObject("test-bucket", "src.bin", "test-bucket", "dst.bin")
		copyDone <- err
	}()
	select {
	case <-pinned:
	case <-time.After(5 * time.Second):
		t.Fatal("CopyObject did not pin the source snapshot")
	}

	if err := svc.DeleteObject("test-bucket", "src.bin"); err != nil {
		t.Fatalf("DeleteObject source: %v", err)
	}
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect while copy paused: %v", err)
	}

	close(resume)
	select {
	case err := <-copyDone:
		if err != nil {
			t.Fatalf("CopyObject: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CopyObject did not resume")
	}

	stream, _, err := svc.GetObject("test-bucket", "dst.bin")
	if err != nil {
		t.Fatalf("GetObject copy: %v", err)
	}
	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read copied object: %v", err)
	}
	if string(got) != payload {
		t.Fatalf("copied content = %q, want %q", string(got), payload)
	}
}

func TestGarbageCollectRetainsMultipartSnapshotChunks(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	upload, err := svc.CreateMultipartUpload("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	etag, err := svc.UploadPart("test-bucket", "multi.bin", upload.UploadID, 1, strings.NewReader("AAAA"))
	if err != nil {
		t.Fatalf("UploadPart old: %v", err)
	}

	pinned := make(chan struct{})
	resume := make(chan struct{})
	svc.testHookCompleteAfterPin = func() {
		close(pinned)
		<-resume
	}

	completeDone := make(chan error, 1)
	go func() {
		_, err := svc.CompleteMultipartUpload("test-bucket", "multi.bin", upload.UploadID,
			[]models.CompletedPart{{PartNumber: 1, ETag: etag}})
		completeDone <- err
	}()
	select {
	case <-pinned:
	case <-time.After(5 * time.Second):
		t.Fatal("CompleteMultipartUpload did not pin the parts snapshot")
	}

	// Overwrite the part after completion captured it. The old chunks are no
	// longer in pending-part metadata, so only the completion's write lease
	// keeps them alive.
	if _, err := svc.UploadPart("test-bucket", "multi.bin", upload.UploadID, 1, strings.NewReader("BBBB")); err != nil {
		t.Fatalf("UploadPart overwrite: %v", err)
	}
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect while complete paused: %v", err)
	}

	close(resume)
	select {
	case err := <-completeDone:
		if err != nil {
			t.Fatalf("CompleteMultipartUpload: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CompleteMultipartUpload did not resume")
	}

	stream, _, err := svc.GetObject("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("GetObject completed multipart: %v", err)
	}
	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read completed multipart: %v", err)
	}
	if string(got) != "AAAA" {
		t.Fatalf("completed multipart content = %q, want %q", string(got), "AAAA")
	}
}

type failingIngestReader struct {
	payload string
	sent    bool
	err     error
}

func (r *failingIngestReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, r.err
	}
	r.sent = true
	return copy(p, r.payload), nil
}

func TestPutObjectIngestFailureReleasesWriteLease(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	_, err := svc.PutObject("test-bucket", "broken.bin", "text/plain",
		&failingIngestReader{payload: "abc", err: errors.New("ingest failed")})
	if err == nil {
		t.Fatal("PutObject succeeded with a failing reader")
	}
	if got := svc.chunkPins.pinCount(); got != 0 {
		t.Fatalf("write lease pins after failed ingest = %d, want 0", got)
	}
}

func TestGetObjectMissingChunkReleasesReadLease(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "object.bin", "text/plain", strings.NewReader("payload")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	manifest, err := svc.metadata.GetManifest("test-bucket", "object.bin")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if err := svc.blob.DeleteBlob(manifest.Chunks[0]); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}

	if _, _, err := svc.GetObject("test-bucket", "object.bin"); !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("GetObject error = %v, want ErrChunkMissing", err)
	}
	if got := svc.chunkPins.pinCount(); got != 0 {
		t.Fatalf("read lease pins after preflight failure = %d, want 0", got)
	}
}

func TestGetObjectEarlyCloseReleasesReadLease(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "multi.bin", "text/plain", strings.NewReader("abcdefghij")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	stream, _, err := svc.GetObject("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	var one [1]byte
	if _, err := io.ReadFull(stream, one[:]); err != nil {
		t.Fatalf("read one byte: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close early: %v", err)
	}
	if got := svc.chunkPins.pinCount(); got != 0 {
		t.Fatalf("read lease pins after early close = %d, want 0", got)
	}

	if err := svc.DeleteObject("test-bucket", "multi.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect after early close: %v", err)
	}
}

func TestGetObjectAssemblyErrorReleasesReadLease(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject("test-bucket", "multi.bin", "text/plain", strings.NewReader("abcdefgh")); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	stream, manifest, err := svc.GetObject("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if len(manifest.Chunks) < 2 {
		t.Fatalf("chunk count = %d, want at least 2", len(manifest.Chunks))
	}
	// Remove a later chunk after preflight while the unread pump is blocked on
	// the first pipe write. The pump must surface the error and release its
	// read lease instead of leaking it.
	if err := svc.blob.DeleteBlob(manifest.Chunks[1]); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}

	if _, err := io.ReadAll(stream); err == nil {
		t.Fatal("expected an assembly error after a chunk disappeared")
	}
	stream.Close()
	if got := svc.chunkPins.pinCount(); got != 0 {
		t.Fatalf("read lease pins after assembly error = %d, want 0", got)
	}
}

func TestCompleteMultipartUploadValidationErrorReleasesWriteLease(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := svc.CreateMultipartUpload("test-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if _, err := svc.UploadPart("test-bucket", "multi.bin", upload.UploadID, 1, strings.NewReader("AAAA")); err != nil {
		t.Fatalf("UploadPart: %v", err)
	}

	if _, err := svc.CompleteMultipartUpload("test-bucket", "multi.bin", upload.UploadID,
		[]models.CompletedPart{{PartNumber: 1, ETag: "wrong-etag"}}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("CompleteMultipartUpload error = %v, want ErrInvalidPart", err)
	}
	if got := svc.chunkPins.pinCount(); got != 0 {
		t.Fatalf("write lease pins after validation error = %d, want 0", got)
	}
}

func TestChunkLeaseDeduplicatesIDsAndReleaseIsIdempotent(t *testing.T) {
	registry := newChunkPinRegistry()

	writeLease := registry.newLease(true)
	writeLease.Pin("chunk-a")
	writeLease.Pin("chunk-a")
	if got := registry.pinCount(); got != 1 {
		t.Fatalf("pinCount after duplicate write pin = %d, want 1", got)
	}

	readLease := registry.newLease(false)
	readLease.Pin("chunk-a")
	if got := registry.pinCount(); got != 2 {
		t.Fatalf("pinCount across two leases = %d, want 2", got)
	}

	writeLease.Release()
	writeLease.Release()
	if got := registry.pinCount(); got != 1 {
		t.Fatalf("pinCount after idempotent write release = %d, want 1", got)
	}
	readLease.Release()
	if got := registry.pinCount(); got != 0 {
		t.Fatalf("pinCount after read release = %d, want 0", got)
	}
}

func TestGCEpochSeedsLiveWriteLeaseIDs(t *testing.T) {
	registry := newChunkPinRegistry()
	live := registry.newLease(true)
	live.Pin("chunk-seed")

	registry.mu.Lock()
	epoch := registry.beginEpochLocked()
	registry.mu.Unlock()
	live.Release()

	registry.mu.Lock()
	protected := registry.protectedLocked(epoch, "chunk-seed")
	registry.mu.Unlock()
	if !protected {
		t.Fatal("epoch born set lost a chunk pinned by a live write lease")
	}
}
