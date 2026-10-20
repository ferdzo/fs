package service

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fs/metadata"
	"fs/models"
	"fs/storage"
)

const rangeBucket = "test-bucket"

// putRangeObject uploads a deterministic object into a fresh bucket. The
// service helper uses 4-byte chunks, so a 20-byte payload becomes five chunks:
// abcd | efgh | ijkl | mnop | qrst.
func putRangeObject(t *testing.T, svc *ObjectService, payload string) []string {
	t.Helper()
	if err := svc.CreateBucket(rangeBucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := svc.PutObject(rangeBucket, "multi.bin", "text/plain", strings.NewReader(payload)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	manifest, err := svc.HeadObject(rangeBucket, "multi.bin")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	return manifest.Chunks
}

func readRange(t *testing.T, svc *ObjectService, start, length int64) (string, *int64) {
	t.Helper()
	stream, manifest, err := svc.GetObjectRange(rangeBucket, "multi.bin", start, length)
	if err != nil {
		t.Fatalf("GetObjectRange(%d,%d): %v", start, length, err)
	}
	got, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil {
		t.Fatalf("read range (%d,%d): %v", start, length, err)
	}
	if closeErr != nil {
		t.Fatalf("close range (%d,%d): %v", start, length, closeErr)
	}
	size := manifest.Size
	return string(got), &size
}

func TestGetObjectRangeChunkBoundaries(t *testing.T) {
	svc := newTestObjectService(t, 0)
	payload := "abcdefghijklmnopqrst" // 20 bytes, 4-byte chunks
	putRangeObject(t, svc, payload)

	cases := []struct {
		name        string
		start, size int64
		want        string
	}{
		{"whole object", 0, 20, payload},
		{"exact first chunk", 0, 4, "abcd"},
		{"inside first chunk", 1, 2, "bc"},
		{"spans 0->1", 2, 4, "cdef"},
		{"starts on boundary", 4, 4, "efgh"},
		{"length one on boundary", 3, 1, "d"},
		{"last partial chunk", 8, 2, "ij"},
		{"inside last chunk", 9, 1, "j"},
		{"covers tail chunks", 10, 6, "klmnop"},
		{"single byte middle", 7, 1, "h"},
		{"ends exactly at eof", 16, 4, "qrst"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, size := readRange(t, svc, tc.start, tc.size)
			if got != tc.want {
				t.Fatalf("range [%d,%d) = %q, want %q", tc.start, tc.start+tc.size, got, tc.want)
			}
			if *size != int64(len(payload)) {
				t.Fatalf("manifest size = %d, want %d", *size, len(payload))
			}
		})
	}
}

func TestGetObjectRangeEmptyInterval(t *testing.T) {
	svc := newTestObjectService(t, 0)
	putRangeObject(t, svc, "abcdefghijklmnopqrst")

	stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", 5, 0)
	if err != nil {
		t.Fatalf("GetObjectRange empty: %v", err)
	}
	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read empty range: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty range produced %q", got)
	}
	if n := svc.chunkPins.pinCount(); n != 0 {
		t.Fatalf("empty range pinned %d chunks, want 0", n)
	}
}

func TestGetObjectRangeRejectsInvalidBounds(t *testing.T) {
	svc := newTestObjectService(t, 0)
	putRangeObject(t, svc, "abcdefghijklmnopqrst")

	for _, tc := range []struct {
		name       string
		start, len int64
	}{
		{"negative start", -1, 1},
		{"negative length", 0, -1},
		{"start at size", 20, 1},
		{"start past size", 21, 1},
		{"extends past end", 18, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", tc.start, tc.len)
			if !errors.Is(err, ErrInvalidRange) {
				if stream != nil {
					stream.Close()
				}
				t.Fatalf("err = %v, want ErrInvalidRange", err)
			}
			if n := svc.chunkPins.pinCount(); n != 0 {
				t.Fatalf("invalid range leaked %d pins", n)
			}
		})
	}
}

func TestGetObjectRangePinsOnlyCoveringChunks(t *testing.T) {
	svc := newTestObjectService(t, 0)
	chunks := putRangeObject(t, svc, "abcdefghijklmnopqrst")

	// [8,10) lives entirely in chunk index 2.
	stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", 8, 2)
	if err != nil {
		t.Fatalf("GetObjectRange: %v", err)
	}

	svc.chunkPins.mu.Lock()
	for i, id := range chunks {
		want := 0
		if i == 2 {
			want = 1
		}
		if got := svc.chunkPins.counts[id]; got != want {
			svc.chunkPins.mu.Unlock()
			t.Fatalf("chunk %d (%s) pins = %d, want %d", i, id, got, want)
		}
	}
	svc.chunkPins.mu.Unlock()

	got, err := io.ReadAll(stream)
	stream.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "ij" {
		t.Fatalf("body = %q, want %q", got, "ij")
	}
}

func TestGetObjectRangeReleasesLeaseOnClose(t *testing.T) {
	svc := newTestObjectService(t, 0)
	putRangeObject(t, svc, "abcdefghijklmnopqrst")

	stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", 8, 4)
	if err != nil {
		t.Fatalf("GetObjectRange: %v", err)
	}
	if n := svc.chunkPins.pinCount(); n == 0 {
		t.Fatal("covering chunks were not pinned while the stream was open")
	}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(got) != "ijkl" {
		t.Fatalf("body = %q, want %q", got, "ijkl")
	}
	if n := svc.chunkPins.pinCount(); n != 0 {
		t.Fatalf("pins after close = %d, want 0", n)
	}
}

func TestGetObjectRangeMissingCoveringChunkFailsBeforeStream(t *testing.T) {
	svc := newTestObjectService(t, 0)
	chunks := putRangeObject(t, svc, "abcdefghijklmnopqrst")

	if err := svc.blob.DeleteBlob(chunks[2]); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}

	stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", 8, 2)
	if err == nil {
		stream.Close()
		t.Fatal("expected an error for a missing covering chunk")
	}
	if !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("err = %v, want ErrChunkMissing", err)
	}
	if n := svc.chunkPins.pinCount(); n != 0 {
		t.Fatalf("preflight failure leaked %d pins", n)
	}
}

func TestGetObjectRangeDoesNotTouchChunksBeforeRange(t *testing.T) {
	svc := newTestObjectService(t, 0)
	chunks := putRangeObject(t, svc, "abcdefghijklmnopqrst")

	// Remove every chunk before the covering one. A prefix-reading
	// implementation would fail or need them; the range path must not.
	for i := 0; i < 2; i++ {
		if err := svc.blob.DeleteBlob(chunks[i]); err != nil {
			t.Fatalf("DeleteBlob %d: %v", i, err)
		}
	}

	got, _ := readRange(t, svc, 8, 2)
	if got != "ij" {
		t.Fatalf("body = %q, want %q", got, "ij")
	}
}

func TestGetObjectRangeSuffixReadsOnlyLastChunk(t *testing.T) {
	svc := newTestObjectService(t, 0)
	chunks := putRangeObject(t, svc, "abcdefghijklmnopqrst")

	for i := 0; i < len(chunks)-1; i++ {
		if err := svc.blob.DeleteBlob(chunks[i]); err != nil {
			t.Fatalf("DeleteBlob %d: %v", i, err)
		}
	}

	got, _ := readRange(t, svc, 16, 4)
	if got != "qrst" {
		t.Fatalf("body = %q, want %q", got, "qrst")
	}
}

func TestGetObjectRangePinnedChunksSurviveGC(t *testing.T) {
	svc := newTestObjectService(t, 0)
	svc.gcGrace = 0
	payload := "abcdefghij" // 10 bytes -> abcd | efgh | ij
	chunks := putRangeObject(t, svc, payload)

	// [8,10) covers only the final chunk (index 2).
	stream, _, err := svc.GetObjectRange(rangeBucket, "multi.bin", 8, 2)
	if err != nil {
		t.Fatalf("GetObjectRange: %v", err)
	}
	if err := svc.DeleteObject(rangeBucket, "multi.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	// The unread stream keeps chunk 2 pinned; GC must not sweep it, but may
	// collect the unpinned prefix chunks.
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect while pinned: %v", err)
	}
	if _, ok := svc.blob.StatChunk(chunks[2]); !ok {
		t.Fatal("pinned covering chunk was swept by GC")
	}

	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read pinned range after GC: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if string(got) != "ij" {
		t.Fatalf("body = %q, want %q", got, "ij")
	}

	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect after close: %v", err)
	}
	remaining, err := svc.blob.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("chunks after close and GC = %d, want 0", len(remaining))
	}
}

func TestManifestHasChunkLayout(t *testing.T) {
	valid := &models.ObjectManifest{
		Size:       10,
		Chunks:     []string{"a", "b", "c"},
		ChunkSizes: []int64{4, 4, 2},
	}
	if !manifestHasChunkLayout(valid) {
		t.Fatal("valid manifest layout rejected")
	}

	cases := []struct {
		name     string
		manifest *models.ObjectManifest
	}{
		{"missing sizes", &models.ObjectManifest{Size: 10, Chunks: []string{"a", "b", "c"}}},
		{"short sizes", &models.ObjectManifest{Size: 10, Chunks: []string{"a", "b", "c"}, ChunkSizes: []int64{4, 6}}},
		{"size mismatch", &models.ObjectManifest{Size: 11, Chunks: []string{"a", "b", "c"}, ChunkSizes: []int64{4, 4, 2}}},
		{"zero-sized chunk", &models.ObjectManifest{Size: 10, Chunks: []string{"a", "b", "c"}, ChunkSizes: []int64{0, 5, 5}}},
	}
	for _, tc := range cases {
		if manifestHasChunkLayout(tc.manifest) {
			t.Fatalf("%s: layout accepted", tc.name)
		}
	}
}

const (
	rangeMultipartChunkSize = 8192000
	rangeMultipartPartSize  = 5 * 1024 * 1024
)

func newServiceWithChunkSize(t *testing.T, chunkSize int) *ObjectService {
	t.Helper()
	root := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, chunkSize)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}
	svc := NewObjectService(md, blob, time.Hour)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// multipartRangePayload returns deterministic bytes so a wrong chunk boundary
// cannot accidentally match the expected range.
func multipartRangePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*37 + i/251)
	}
	return payload
}

func createTwoPartMultipartObject(t *testing.T, svc *ObjectService, bucket, key string, payload []byte) *models.ObjectManifest {
	t.Helper()
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	upload, err := svc.CreateMultipartUpload(bucket, key)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	partSize := len(payload) / 2
	completed := make([]models.CompletedPart, 0, 2)
	for i := 0; i < 2; i++ {
		etag, err := svc.UploadPart(bucket, key, upload.UploadID, i+1,
			bytes.NewReader(payload[i*partSize:(i+1)*partSize]))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		completed = append(completed, models.CompletedPart{PartNumber: i + 1, ETag: etag})
	}
	manifest, err := svc.CompleteMultipartUpload(bucket, key, upload.UploadID, completed)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	return manifest
}

func assertObjectRange(t *testing.T, svc *ObjectService, bucket, key string, start, length int64, payload []byte) {
	t.Helper()
	if start < 0 || length < 0 || start+length > int64(len(payload)) {
		t.Fatalf("bad test range [%d,%d) for %d-byte payload", start, start+length, len(payload))
	}
	stream, manifest, err := svc.GetObjectRange(bucket, key, start, length)
	if err != nil {
		t.Fatalf("GetObjectRange(%s,%d,%d): %v", key, start, length, err)
	}
	got, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil {
		t.Fatalf("read %s [%d,%d): %v", key, start, start+length, readErr)
	}
	if closeErr != nil {
		t.Fatalf("close %s: %v", key, closeErr)
	}
	want := payload[start : start+length]
	if !bytes.Equal(got, want) {
		t.Fatalf("%s [%d,%d) returned %d bytes, want %d; content mismatch", key, start, start+length, len(got), len(want))
	}
	if manifest.Size != int64(len(payload)) {
		t.Fatalf("manifest size = %d, want %d", manifest.Size, len(payload))
	}
	if n := svc.chunkPins.pinCount(); n != 0 {
		t.Fatalf("%s [%d,%d) leaked %d pins", key, start, start+length, n)
	}
}

func multipartPartBoundaryCases(payloadLen int64) []struct {
	name        string
	start, size int64
} {
	return []struct {
		name        string
		start, size int64
	}{
		{"crosses part boundary", rangeMultipartPartSize - 80, 101},
		{"inside second part", 6_000_000, 1000},
		{"review case one MiB near end", 9_437_184, 1_048_576},
		{"starts on part boundary", rangeMultipartPartSize, 100},
		{"ends exactly on part boundary", rangeMultipartPartSize - 100, 100},
		{"suffix at end", payloadLen - 100, 100},
	}
}

func TestGetObjectRangeMultipartPartBoundary(t *testing.T) {
	svc := newServiceWithChunkSize(t, rangeMultipartChunkSize)
	const bucket = "mp-bucket"
	payload := multipartRangePayload(2 * rangeMultipartPartSize)
	manifest := createTwoPartMultipartObject(t, svc, bucket, "mp.bin", payload)

	// Each 5 MiB part is shorter than the configured 8,192,000-byte chunk size,
	// so the object has two short chunks rather than a fixed stride.
	if len(manifest.Chunks) != 2 || len(manifest.ChunkSizes) != 2 {
		t.Fatalf("layout = %d chunks, %d sizes; want 2/2", len(manifest.Chunks), len(manifest.ChunkSizes))
	}
	if manifest.ChunkSizes[0] != rangeMultipartPartSize || manifest.ChunkSizes[1] != rangeMultipartPartSize {
		t.Fatalf("chunk sizes = %v, want [%d %d]", manifest.ChunkSizes, rangeMultipartPartSize, rangeMultipartPartSize)
	}

	for _, tc := range multipartPartBoundaryCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertObjectRange(t, svc, bucket, "mp.bin", tc.start, tc.size, payload)
		})
	}
}

func TestGetObjectRangeMultipartCopy(t *testing.T) {
	svc := newServiceWithChunkSize(t, rangeMultipartChunkSize)
	const bucket = "mp-copy-bucket"
	payload := multipartRangePayload(2 * rangeMultipartPartSize)
	createTwoPartMultipartObject(t, svc, bucket, "mp.bin", payload)

	copied, err := svc.CopyObject(bucket, "mp.bin", bucket, "copy.bin")
	if err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	if len(copied.ChunkSizes) != 2 || copied.ChunkSizes[0] != rangeMultipartPartSize {
		t.Fatalf("copied chunk sizes = %v, want source layout", copied.ChunkSizes)
	}

	for _, tc := range multipartPartBoundaryCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertObjectRange(t, svc, bucket, "copy.bin", tc.start, tc.size, payload)
		})
	}
}

func TestGetObjectRangeLegacyManifestFallsBack(t *testing.T) {
	svc := newServiceWithChunkSize(t, rangeMultipartChunkSize)
	const bucket = "legacy-bucket"
	payload := multipartRangePayload(2 * rangeMultipartPartSize)
	manifest := createTwoPartMultipartObject(t, svc, bucket, "legacy.bin", payload)

	// Simulate an object persisted before per-chunk sizes existed.
	legacy := *manifest
	legacy.ChunkSizes = nil
	if err := svc.metadata.PutManifest(&legacy); err != nil {
		t.Fatalf("PutManifest legacy: %v", err)
	}
	head, err := svc.HeadObject(bucket, "legacy.bin")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if head.ChunkSizes != nil {
		t.Fatalf("legacy manifest still has chunk sizes: %v", head.ChunkSizes)
	}

	for _, tc := range multipartPartBoundaryCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertObjectRange(t, svc, bucket, "legacy.bin", tc.start, tc.size, payload)
		})
	}
}

func TestCompleteMultipartUploadLegacyPartFallsBack(t *testing.T) {
	svc := newServiceWithChunkSize(t, rangeMultipartChunkSize)
	const bucket = "legacy-complete-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	payload := multipartRangePayload(2 * rangeMultipartPartSize)
	upload, err := svc.CreateMultipartUpload(bucket, "legacy-complete.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	completed := make([]models.CompletedPart, 0, 2)
	for i := 0; i < 2; i++ {
		etag, err := svc.UploadPart(bucket, "legacy-complete.bin", upload.UploadID, i+1,
			bytes.NewReader(payload[i*rangeMultipartPartSize:(i+1)*rangeMultipartPartSize]))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		completed = append(completed, models.CompletedPart{PartNumber: i + 1, ETag: etag})
	}

	// Simulate an upload started before per-part chunk sizes were persisted.
	parts, err := svc.metadata.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("stored parts = %d, want 2", len(parts))
	}
	for i := range parts {
		parts[i].ChunkSizes = nil
		if err := svc.metadata.PutMultipartPart(upload.UploadID, parts[i]); err != nil {
			t.Fatalf("PutMultipartPart %d: %v", i+1, err)
		}
	}

	manifest, err := svc.CompleteMultipartUpload(bucket, "legacy-complete.bin", upload.UploadID, completed)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if len(manifest.ChunkSizes) != 0 {
		t.Fatalf("legacy completion persisted chunk sizes: %v", manifest.ChunkSizes)
	}
	if manifest.Size != int64(len(payload)) || len(manifest.Chunks) != 2 {
		t.Fatalf("completed manifest = size %d, %d chunks; want %d/2", manifest.Size, len(manifest.Chunks), len(payload))
	}

	// The object is complete and readable; range reads take the correct
	// prefix-discarding fallback because the layout is unknown.
	for _, tc := range multipartPartBoundaryCases(int64(len(payload))) {
		t.Run(tc.name, func(t *testing.T) {
			assertObjectRange(t, svc, bucket, "legacy-complete.bin", tc.start, tc.size, payload)
		})
	}
}

func TestGetObjectAfterChunkSizeReduction(t *testing.T) {
	root := t.TempDir()
	mdPath := filepath.Join(root, "metadata.db")

	md, err := metadata.NewMetadataHandler(mdPath)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	largeBlob, err := storage.NewBlobStore(root, 64<<10)
	if err != nil {
		t.Fatalf("NewBlobStore large: %v", err)
	}
	svc := NewObjectService(md, largeBlob, time.Hour)
	if err := svc.CreateBucket("shrink-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	payload := multipartRangePayload(200 << 10)
	if _, err := svc.PutObject("shrink-bucket", "big.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	written, err := svc.HeadObject("shrink-bucket", "big.bin")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close first service: %v", err)
	}

	// Re-open with a smaller chunk size than the chunks on disk.
	md, err = metadata.NewMetadataHandler(mdPath)
	if err != nil {
		t.Fatalf("reopen NewMetadataHandler: %v", err)
	}
	smallBlob, err := storage.NewBlobStore(root, 4<<10)
	if err != nil {
		t.Fatalf("NewBlobStore small: %v", err)
	}
	reopened := NewObjectService(md, smallBlob, time.Hour)
	defer reopened.Close()

	stream, manifest, err := reopened.GetObject("shrink-bucket", "big.bin")
	if err != nil {
		t.Fatalf("GetObject after reduction: %v", err)
	}
	got, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil {
		t.Fatalf("read after reduction: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close after reduction: %v", closeErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("full GET after reduction returned %d bytes, want %d; content mismatch", len(got), len(payload))
	}
	if manifest.Size != int64(len(payload)) || written.ChunkSizes[0] != 64<<10 {
		t.Fatalf("unexpected manifest: size=%d first chunk=%d", manifest.Size, written.ChunkSizes[0])
	}
}

// BenchmarkGetObjectRangeSuffix measures the new path: a 16-byte suffix read of
// an 8 MiB object reads a single 64 KiB chunk.
func BenchmarkGetObjectRangeSuffix(b *testing.B) {
	svc, payload := newRangeBenchService(b, 64<<10, 8<<20)
	const suffix = 16
	b.SetBytes(suffix)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, _, err := svc.GetObjectRange(rangeBucket, "big.bin", int64(len(payload))-suffix, suffix)
		if err != nil {
			b.Fatalf("GetObjectRange: %v", err)
		}
		if _, err := io.Copy(io.Discard, stream); err != nil {
			b.Fatalf("copy: %v", err)
		}
		stream.Close()
	}
}

// BenchmarkGetObjectSuffixByDiscard measures the legacy behavior (GetObject +
// discard the prefix) for comparison against BenchmarkGetObjectRangeSuffix.
func BenchmarkGetObjectSuffixByDiscard(b *testing.B) {
	svc, payload := newRangeBenchService(b, 64<<10, 8<<20)
	const suffix = 16
	b.SetBytes(suffix)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream, _, err := svc.GetObject(rangeBucket, "big.bin")
		if err != nil {
			b.Fatalf("GetObject: %v", err)
		}
		if _, err := io.CopyN(io.Discard, stream, int64(len(payload))-suffix); err != nil {
			b.Fatalf("discard prefix: %v", err)
		}
		if _, err := io.CopyN(io.Discard, stream, suffix); err != nil {
			b.Fatalf("read suffix: %v", err)
		}
		stream.Close()
	}
}

func newRangeBenchService(b *testing.B, chunkSize, payloadSize int) (*ObjectService, []byte) {
	b.Helper()
	root := b.TempDir()
	md, err := metadata.NewMetadataHandler(root + "/metadata.db")
	if err != nil {
		b.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, chunkSize)
	if err != nil {
		b.Fatalf("NewBlobStore: %v", err)
	}
	svc := NewObjectService(md, blob, time.Hour)
	b.Cleanup(func() { _ = svc.Close() })

	if err := svc.CreateBucket(rangeBucket); err != nil {
		b.Fatalf("CreateBucket: %v", err)
	}
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	if _, err := svc.PutObject(rangeBucket, "big.bin", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		b.Fatalf("PutObject: %v", err)
	}
	return svc, payload
}
