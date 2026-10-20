package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
)

const benchChunkSize = 1 << 20 // 1 MiB

func benchPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	return data
}

func newBenchBlobStore(b *testing.B) *BlobStore {
	b.Helper()
	bs, err := NewBlobStore(b.TempDir(), benchChunkSize)
	if err != nil {
		b.Fatalf("NewBlobStore: %v", err)
	}
	return bs
}

// benchmarkIngestStream measures the full write path: md5+sha256 hashing,
// chunking, temp-file write, fsync, rename and parent-directory sync. Each
// iteration uses a fresh store so dedup never short-circuits the write.
func benchmarkIngestStream(b *testing.B, size int) {
	b.Helper()
	payload := benchPayload(size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs, err := NewBlobStore(b.TempDir(), benchChunkSize)
		if err != nil {
			b.Fatalf("NewBlobStore: %v", err)
		}
		if _, _, _, err := bs.IngestStream(bytes.NewReader(payload)); err != nil {
			b.Fatalf("IngestStream: %v", err)
		}
	}
}

// BenchmarkIngestStreamSmall ingests a single 4 KiB chunk (one fsync).
func BenchmarkIngestStreamSmall(b *testing.B) { benchmarkIngestStream(b, 4<<10) }

// BenchmarkIngestStreamMedium ingests a single 1 MiB chunk (one fsync).
func BenchmarkIngestStreamMedium(b *testing.B) { benchmarkIngestStream(b, 1<<20) }

// BenchmarkIngestStreamLarge ingests a 16 MiB object as sixteen 1 MiB chunks
// (sixteen file fsyncs plus their directory syncs).
func BenchmarkIngestStreamLarge(b *testing.B) { benchmarkIngestStream(b, 16<<20) }

// BenchmarkIngestDedup re-ingests 1 MiB of identical content with the chunk
// already on disk, so saveBlob takes the Stat-only dedup path. Hashing still
// runs; no file write or fsync occurs.
func BenchmarkIngestDedup(b *testing.B) {
	bs := newBenchBlobStore(b)
	payload := benchPayload(1 << 20)
	if _, _, _, err := bs.IngestStream(bytes.NewReader(payload)); err != nil {
		b.Fatalf("initial IngestStream: %v", err)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := bs.IngestStream(bytes.NewReader(payload)); err != nil {
			b.Fatalf("IngestStream: %v", err)
		}
	}
}

// BenchmarkReadChunk measures GetBlob, which reads the chunk and re-hashes it
// with SHA-256 to verify integrity before returning.
func BenchmarkReadChunk(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"4KiB", 4 << 10},
		{"1MiB", 1 << 20},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bs := newBenchBlobStore(b)
			payload := benchPayload(tc.size)
			chunks, _, _, err := bs.IngestStream(bytes.NewReader(payload))
			if err != nil {
				b.Fatalf("IngestStream: %v", err)
			}
			if len(chunks) != 1 {
				b.Fatalf("chunk count = %d, want 1", len(chunks))
			}
			chunkID := chunks[0]

			b.SetBytes(int64(tc.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := bs.GetBlob(chunkID)
				if err != nil {
					b.Fatalf("GetBlob: %v", err)
				}
				if len(data) != tc.size {
					b.Fatalf("GetBlob returned %d bytes, want %d", len(data), tc.size)
				}
			}
		})
	}
}

// BenchmarkAssembleChunks assembles a 16 MiB object made of sixteen 1 MiB
// chunks through io.Pipe, draining the reader concurrently as
// ObjectService.GetObject does. Every chunk read is integrity-verified by
// GetBlob.
func BenchmarkAssembleChunks(b *testing.B) {
	bs := newBenchBlobStore(b)
	payload := benchPayload(16 << 20)
	chunks, _, _, err := bs.IngestStream(bytes.NewReader(payload))
	if err != nil {
		b.Fatalf("IngestStream: %v", err)
	}
	if len(chunks) != 16 {
		b.Fatalf("chunk count = %d, want 16", len(chunks))
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pr, pw := io.Pipe()
		readErr := make(chan error, 1)
		go func() {
			n, err := io.Copy(io.Discard, pr)
			if err == nil && n != int64(len(payload)) {
				err = fmt.Errorf("assembled %d bytes, want %d", n, len(payload))
			}
			readErr <- err
		}()

		if err := bs.AssembleStream(chunks, pw); err != nil {
			_ = pw.CloseWithError(err)
			b.Fatalf("AssembleStream: %v", err)
		}
		if err := pw.Close(); err != nil {
			b.Fatalf("close pipe writer: %v", err)
		}
		if err := <-readErr; err != nil {
			b.Fatalf("read assembled stream: %v", err)
		}
	}
}

func assembleChunks(bs *BlobStore, chunkIDs []string) ([]byte, error) {
	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		err := bs.AssembleStream(chunkIDs, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()
	data, readErr := io.ReadAll(pr)
	assembleErr := <-errCh
	if assembleErr != nil {
		return nil, assembleErr
	}
	if readErr != nil {
		return nil, readErr
	}
	return data, nil
}

// TestAssembleStreamPooledBuffersNotAliased runs many concurrent assemblies of
// different objects through the same store. A shared pooled buffer would mix
// their bytes and fail the comparison.
func TestAssembleStreamPooledBuffersNotAliased(t *testing.T) {
	bs, err := NewBlobStore(t.TempDir(), 32<<10)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	payloadA := bytes.Repeat([]byte{0x41}, 200<<10)
	payloadB := bytes.Repeat([]byte{0x42}, 200<<10)
	chunksA, _, _, err := bs.IngestStream(bytes.NewReader(payloadA))
	if err != nil {
		t.Fatalf("ingest A: %v", err)
	}
	chunksB, _, _, err := bs.IngestStream(bytes.NewReader(payloadB))
	if err != nil {
		t.Fatalf("ingest B: %v", err)
	}

	const workers = 16
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				payload, chunks := payloadA, chunksA
				if (worker+i)%2 == 1 {
					payload, chunks = payloadB, chunksB
				}
				got, err := assembleChunks(bs, chunks)
				if err != nil {
					errs <- fmt.Errorf("worker %d iteration %d: %w", worker, i, err)
					return
				}
				if !bytes.Equal(got, payload) {
					errs <- fmt.Errorf("worker %d iteration %d: assembled %d bytes of wrong content", worker, i, len(got))
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestAssembleStreamReusesBufferAfterIntegrityError proves a buffer used for a
// failed read is returned to the pool and that later reads do not observe stale
// bytes. It covers both a hash mismatch and an oversized chunk file.
func TestAssembleStreamReusesBufferAfterIntegrityError(t *testing.T) {
	root := t.TempDir()
	const chunkSize = 16 << 10
	bs, err := NewBlobStore(root, chunkSize)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	good := bytes.Repeat([]byte{0x11}, 3*chunkSize)
	goodChunks, _, _, err := bs.IngestStream(bytes.NewReader(good))
	if err != nil {
		t.Fatalf("ingest good: %v", err)
	}

	bad := bytes.Repeat([]byte{0x22}, chunkSize+4<<10)
	badChunks, _, _, err := bs.IngestStream(bytes.NewReader(bad))
	if err != nil {
		t.Fatalf("ingest bad: %v", err)
	}
	if len(badChunks) != 2 {
		t.Fatalf("bad chunk count = %d, want 2", len(badChunks))
	}

	corruptChunk(t, root, badChunks[0], []byte("corrupt"))
	if _, err := assembleChunks(bs, badChunks); !errors.Is(err, ErrChunkIntegrity) {
		t.Fatalf("assembling hash-mismatched chunk error = %v, want ErrChunkIntegrity", err)
	}
	if got := len(bs.chunkBufs.free); got != 1 {
		t.Fatalf("pooled buffers after integrity error = %d, want 1", got)
	}

	// Restore the first chunk, then make the second one larger than chunkSize.
	corruptChunk(t, root, badChunks[0], bad[:chunkSize])
	corruptChunk(t, root, badChunks[1], make([]byte, chunkSize+1))
	if _, err := assembleChunks(bs, badChunks); !errors.Is(err, ErrChunkIntegrity) {
		t.Fatalf("assembling oversized chunk error = %v, want ErrChunkIntegrity", err)
	}
	if got := len(bs.chunkBufs.free); got != 1 {
		t.Fatalf("pooled buffers after oversized-chunk error = %d, want 1", got)
	}

	for i := 0; i < 10; i++ {
		got, err := assembleChunks(bs, goodChunks)
		if err != nil {
			t.Fatalf("assembly %d after errors: %v", i, err)
		}
		if !bytes.Equal(got, good) {
			t.Fatalf("assembly %d after errors returned stale or mixed data", i)
		}
	}
}

// TestIngestStreamAdaptiveBufferPreservesChunks checks chunk boundaries,
// per-chunk IDs and the whole-file MD5 across the small-buffer promotion
// boundary and partial final chunks.
func TestIngestStreamAdaptiveBufferPreservesChunks(t *testing.T) {
	const chunkSize = 256 << 10
	bs, err := NewBlobStore(t.TempDir(), chunkSize)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	sizes := []int{0, 1, ingestInitialBufferSize - 1, ingestInitialBufferSize, ingestInitialBufferSize + 1, chunkSize, chunkSize + 1, 3*chunkSize + 5}
	for _, size := range sizes {
		payload := benchPayload(size)
		chunks, total, etag, err := bs.IngestStream(bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("size %d: IngestStream: %v", size, err)
		}
		if total != int64(size) {
			t.Fatalf("size %d: total = %d", size, total)
		}
		wantChunks := (size + chunkSize - 1) / chunkSize
		if len(chunks) != wantChunks {
			t.Fatalf("size %d: chunks = %d, want %d", size, len(chunks), wantChunks)
		}
		sum := md5.Sum(payload)
		if want := hex.EncodeToString(sum[:]); etag != want {
			t.Fatalf("size %d: etag = %s, want %s", size, etag, want)
		}
		var rebuilt bytes.Buffer
		for _, chunkID := range chunks {
			data, err := bs.GetBlob(chunkID)
			if err != nil {
				t.Fatalf("size %d: GetBlob: %v", size, err)
			}
			if len(data) > chunkSize {
				t.Fatalf("size %d: chunk %d bytes exceeds chunk size", size, len(data))
			}
			rebuilt.Write(data)
		}
		if !bytes.Equal(rebuilt.Bytes(), payload) {
			t.Fatalf("size %d: rebuilt content differs", size)
		}
	}
}

func TestChunkBufferPoolCapAndSizeGuard(t *testing.T) {
	p := newChunkBufferPool(4 << 10)
	if got := cap(p.free); got != maxPooledChunkBuffers {
		t.Fatalf("pool slots = %d, want %d", got, maxPooledChunkBuffers)
	}
	for i := 0; i < maxPooledChunkBuffers+10; i++ {
		p.put(make([]byte, 4<<10))
	}
	if got := len(p.free); got != maxPooledChunkBuffers {
		t.Fatalf("retained buffers = %d, want %d", got, maxPooledChunkBuffers)
	}
	p.put(make([]byte, 8<<10))
	if got := len(p.free); got != maxPooledChunkBuffers {
		t.Fatalf("wrong-sized buffer was retained: %d", got)
	}
	if got := cap(newChunkBufferPool(64 << 20).free); got != 1 {
		t.Fatalf("64 MiB chunk pool slots = %d, want 1", got)
	}
}
