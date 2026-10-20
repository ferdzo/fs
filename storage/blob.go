package storage

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"fs/metrics"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const blobRoot = "blobs"
const maxChunkSize = 64 * 1024 * 1024

const (
	// ingestInitialBufferSize is the read buffer used when an object is small.
	// It bounds the allocation for tiny uploads while keeping syscall counts
	// reasonable (a 4 KiB object costs one 64 KiB allocation instead of a full
	// chunk-sized buffer).
	ingestInitialBufferSize = 64 << 10
	// maxPooledChunkBytes caps the bytes retained by a store's assembly buffer
	// pool. In-flight buffers are not counted; only idle ones kept for reuse.
	maxPooledChunkBytes = 64 << 20
	// maxPooledChunkBuffers avoids an excessive number of channel slots when
	// chunks are tiny.
	maxPooledChunkBuffers = 64
)

var ErrChunkIntegrity = errors.New("chunk integrity check failed")

// ErrChunkMissing is returned when a referenced chunk file does not exist.
var ErrChunkMissing = errors.New("chunk file missing")

// chunkBufferPool is a bounded free list of chunk-sized read buffers. A plain
// sync.Pool has no byte cap and may retain one buffer per P until the next GC,
// which is significant when chunkSize is large. This pool retains at most
// min(maxPooledChunkBuffers, maxPooledChunkBytes/chunkSize) idle buffers and
// never stores a buffer of a different size.
type chunkBufferPool struct {
	size int
	free chan []byte
}

func newChunkBufferPool(size int) *chunkBufferPool {
	slots := maxPooledChunkBytes / size
	if slots < 1 {
		slots = 1
	}
	if slots > maxPooledChunkBuffers {
		slots = maxPooledChunkBuffers
	}
	return &chunkBufferPool{size: size, free: make(chan []byte, slots)}
}

func (p *chunkBufferPool) get() []byte {
	select {
	case buf := <-p.free:
		return buf
	default:
		return make([]byte, p.size)
	}
}

func (p *chunkBufferPool) put(buf []byte) {
	if cap(buf) != p.size {
		return
	}
	select {
	case p.free <- buf[:p.size]:
	default:
	}
}

type BlobStore struct {
	dataRoot  string
	chunkSize int
	chunkBufs *chunkBufferPool
}

func NewBlobStore(root string, chunkSize int) (*BlobStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("blob root is required")
	}
	if chunkSize <= 0 || chunkSize > maxChunkSize {
		return nil, fmt.Errorf("chunk size must be between 1 and %d bytes", maxChunkSize)
	}

	cleanRoot := filepath.Clean(root)
	if err := os.MkdirAll(filepath.Join(cleanRoot, blobRoot), 0o755); err != nil {
		return nil, err
	}
	return &BlobStore{
		chunkSize: chunkSize,
		dataRoot:  cleanRoot,
		chunkBufs: newChunkBufferPool(chunkSize),
	}, nil
}

func (bs *BlobStore) IngestStream(stream io.Reader) ([]string, int64, string, error) {
	chunks, _, size, etag, err := bs.IngestStreamWithSizes(stream, nil)
	return chunks, size, etag, err
}

// IngestStreamWithPin behaves like IngestStream but calls onChunk with each
// computed chunk ID after hashing and before the dedup Stat/save. Callers use it
// to pin a chunk under the GC registry before its bytes can reach disk.
func (bs *BlobStore) IngestStreamWithPin(stream io.Reader, onChunk func(chunkID string) error) ([]string, int64, string, error) {
	chunks, _, size, etag, err := bs.IngestStreamWithSizes(stream, onChunk)
	return chunks, size, etag, err
}

// IngestStreamWithSizes behaves like IngestStreamWithPin and additionally
// returns the byte length of every emitted chunk, parallel to the chunk ID
// slice. Chunk lengths are chunkSize except possibly the final chunk of the
// stream, so callers must persist them when they need to map object offsets
// back onto chunks later (for example range reads of multipart objects).
func (bs *BlobStore) IngestStreamWithSizes(stream io.Reader, onChunk func(chunkID string) error) ([]string, []int64, int64, string, error) {
	start := time.Now()
	fullFileHasher := md5.New()

	// Tiny objects should not pay for a full chunk-sized buffer. Start at
	// ingestInitialBufferSize and jump straight to chunkSize once that fills.
	// Geometric doubling was rejected because every intermediate step is an
	// allocation plus a copy, adding up to a full extra chunkSize of garbage per
	// ingest call; this path allocates at most initial+chunkSize in total.
	bufferSize := bs.chunkSize
	if bufferSize > ingestInitialBufferSize {
		bufferSize = ingestInitialBufferSize
	}
	buffer := make([]byte, bufferSize)
	filled := 0

	var totalSize int64
	var chunkIDs []string
	var chunkSizes []int64
	success := false
	defer func() {
		metrics.Default.ObserveBlob("ingest_stream", time.Since(start), 0, success)
	}()

	emitChunk := func(chunkData []byte) error {
		size := int64(len(chunkData))
		totalSize += size
		fullFileHasher.Write(chunkData)

		chunkHash := sha256.Sum256(chunkData)
		chunkID := hex.EncodeToString(chunkHash[:])

		if onChunk != nil {
			if err := onChunk(chunkID); err != nil {
				return err
			}
		}

		if err := bs.saveBlob(chunkID, chunkData); err != nil {
			return err
		}
		chunkIDs = append(chunkIDs, chunkID)
		chunkSizes = append(chunkSizes, size)
		return nil
	}

	for {
		bytesRead, err := io.ReadFull(stream, buffer[filled:])
		filled += bytesRead

		if err == nil {
			if len(buffer) == bs.chunkSize {
				if err := emitChunk(buffer[:filled]); err != nil {
					return nil, nil, 0, "", err
				}
				filled = 0
				continue
			}
			// The small initial buffer is full and the stream has more data,
			// so promote it to a full chunk buffer and continue reading the
			// same chunk. At most one 64 KiB copy per ingest call.
			grown := make([]byte, bs.chunkSize)
			copy(grown, buffer[:filled])
			buffer = grown
			continue
		}
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			if filled > 0 {
				if err := emitChunk(buffer[:filled]); err != nil {
					return nil, nil, 0, "", err
				}
			}
			break
		}
		return nil, nil, 0, "", err
	}

	etag := hex.EncodeToString(fullFileHasher.Sum(nil))
	success = true
	return chunkIDs, chunkSizes, totalSize, etag, nil
}

func (bs *BlobStore) saveBlob(chunkID string, data []byte) error {
	start := time.Now()
	success := false
	writtenBytes := int64(0)
	defer func() {
		metrics.Default.ObserveBlob("write_chunk", time.Since(start), writtenBytes, success)
	}()

	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	dir := filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4])
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fullPath := filepath.Join(dir, chunkID)
	if _, err := os.Stat(fullPath); err == nil {
		success = true
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, chunkID+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		if _, statErr := os.Stat(fullPath); statErr == nil {
			success = true
			return nil
		}
		return err
	}
	cleanup = false

	if err := syncDir(dir); err != nil {
		// A rename without a synced parent directory may be lost on crash;
		// drop the chunk so dedup never trusts a possibly-non-durable file.
		_ = os.Remove(fullPath)
		cleanup = true
		return err
	}
	writtenBytes = int64(len(data))
	success = true
	return nil
}

func (bs *BlobStore) AssembleStream(chunkIDs []string, w *io.PipeWriter) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveBlob("assemble_stream", time.Since(start), 0, success)
	}()

	// One pooled buffer is reused for the whole assembly. The buffer is only
	// returned after w.Write has consumed each chunk (io.PipeWriter.Write is
	// synchronous), and the defer covers error and panic paths.
	buf := bs.chunkBufs.get()
	defer bs.chunkBufs.put(buf)

	for _, chunkID := range chunkIDs {
		data, err := bs.readChunk(chunkID, buf)
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	success = true
	return nil
}

func verifyChunkData(chunkID string, data []byte) error {
	chunkHash := sha256.Sum256(data)
	actualChunkID := hex.EncodeToString(chunkHash[:])
	if actualChunkID != chunkID {
		return fmt.Errorf("%w: expected %s, got %s", ErrChunkIntegrity, chunkID, actualChunkID)
	}
	return nil
}

// readChunk reads and verifies a chunk, returning its bytes. It reuses pooled
// when the chunk fits in that buffer. A chunk may be larger than the store's
// currently configured chunk size when CHUNK_SIZE was reduced after the chunk
// was written, so oversized files are read into a right-sized allocation
// instead of being rejected. Only buffers allocated by chunkBufferPool are ever
// returned to the pool; caller-owned allocations are reclaimed by the GC.
func (bs *BlobStore) readChunk(chunkID string, pooled []byte) ([]byte, error) {
	start := time.Now()
	success := false
	var readBytes int64
	defer func() {
		metrics.Default.ObserveBlob("read_chunk", time.Since(start), readBytes, success)
	}()

	if !isValidChunkID(chunkID) {
		return nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	path := filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrChunkMissing
		}
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	// No historical configuration could produce a chunk larger than
	// maxChunkSize, so a larger file is corruption rather than a layout change.
	if size > maxChunkSize {
		return nil, fmt.Errorf("%w: chunk %s is %d bytes, exceeds maximum %d", ErrChunkIntegrity, chunkID, size, maxChunkSize)
	}
	var data []byte
	if size <= int64(len(pooled)) {
		data = pooled[:size]
	} else {
		data = make([]byte, size)
	}
	n, err := io.ReadFull(f, data)
	if err != nil {
		return nil, err
	}
	// Detect a file that grew after Stat: hashing only the expected prefix would
	// otherwise accept the chunk while silently ignoring trailing bytes.
	var probe [1]byte
	if extra, readErr := f.Read(probe[:]); extra > 0 {
		return nil, fmt.Errorf("%w: chunk %s is larger than %d bytes", ErrChunkIntegrity, chunkID, n)
	} else if readErr != nil && readErr != io.EOF {
		return nil, readErr
	}
	if err := verifyChunkData(chunkID, data[:n]); err != nil {
		return nil, err
	}
	readBytes = int64(n)
	success = true
	return data[:n], nil
}

func (bs *BlobStore) GetBlob(chunkID string) ([]byte, error) {
	start := time.Now()
	success := false
	var size int64
	defer func() {
		metrics.Default.ObserveBlob("read_chunk", time.Since(start), size, success)
	}()

	if !isValidChunkID(chunkID) {
		return nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	data, err := os.ReadFile(filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrChunkMissing
		}
		return nil, err
	}
	if err := verifyChunkData(chunkID, data); err != nil {
		return nil, err
	}
	size = int64(len(data))
	success = true
	return data, nil
}

func (bs *BlobStore) DeleteBlob(chunkID string) error {
	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	err := os.Remove(filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (bs *BlobStore) ListChunks() ([]string, error) {
	var chunkIDs []string
	err := bs.ForEachChunk(func(chunkID string) error {
		chunkIDs = append(chunkIDs, chunkID)
		return nil
	})
	return chunkIDs, err
}

func (bs *BlobStore) ForEachChunk(fn func(chunkID string) error) error {
	if fn == nil {
		return errors.New("chunk callback is required")
	}
	return filepath.Walk(filepath.Join(bs.dataRoot, blobRoot), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			chunkID := info.Name()
			if isValidChunkID(chunkID) {
				return fn(chunkID)
			}
		}
		return nil

	})
}

// RemoveStaleTempFiles deletes ingest temp files whose modification time is
// strictly older than olderThan. Ingest writes create their temp file next to
// the final chunk as "<64 lowercase hex>.tmp-<digits>" and rename it into place;
// a crash between create and rename leaves that file behind forever because
// ForEachChunk only enumerates valid chunk IDs. Only regular files matching the
// ingest temp name exactly are considered, so chunk files, uploads in progress
// and unrelated files are never touched. A missing blob directory is treated as
// empty. Individual removal failures are skipped; an unreadable directory is
// reported to the caller.
func (bs *BlobStore) RemoveStaleTempFiles(olderThan time.Time) (int, error) {
	removed := 0
	err := filepath.WalkDir(filepath.Join(bs.dataRoot, blobRoot), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() || !entry.Type().IsRegular() || !isIngestTempName(entry.Name()) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			// The file disappeared between ReadDir and Info; nothing to remove.
			return nil
		}
		if !info.ModTime().Before(olderThan) {
			return nil
		}
		if err := os.Remove(path); err != nil {
			// Renamed into place or already cleaned by another pass: not an
			// error for this sweep. Other failures are tolerated as well so one
			// stubborn file cannot block cleanup of the rest.
			return nil
		}
		removed++
		return nil
	})
	if err != nil {
		return removed, err
	}
	return removed, nil
}

// isIngestTempName reports whether name is exactly the pattern produced by
// os.CreateTemp(dir, chunkID+".tmp-*"): 64 lowercase hex characters, ".tmp-",
// and at least one decimal digit.
func isIngestTempName(name string) bool {
	const hexLen = sha256.Size * 2
	const suffix = ".tmp-"
	if len(name) <= hexLen+len(suffix) || name[hexLen:hexLen+len(suffix)] != suffix {
		return false
	}
	for i := 0; i < hexLen; i++ {
		ch := name[i]
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	for i := hexLen + len(suffix); i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

func isValidChunkID(chunkID string) bool {
	if len(chunkID) != sha256.Size*2 {
		return false
	}
	for _, ch := range chunkID {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func syncDir(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// StatChunk returns the modification time of a stored chunk.
func (bs *BlobStore) StatChunk(chunkID string) (time.Time, bool) {
	if !isValidChunkID(chunkID) {
		return time.Time{}, false
	}
	info, err := os.Stat(filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID))
	if err != nil {
		return time.Time{}, false
	}
	return info.ModTime(), true
}
