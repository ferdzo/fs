package storage

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"fs/metrics"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
)

const erasureBackendLabel = "erasure"

// ErasureConfig configures a multi-path Reed-Solomon ChunkStore.
type ErasureConfig struct {
	Paths        []string
	DataShards   int
	ParityShards int
	ChunkSize    int
	AutoHeal     bool
	StrictWrite  bool
}

// ErasureStore implements service.ChunkStore using Reed-Solomon (K data + M
// parity) shards striped deterministically across len(Paths) == K+M storage roots.
type ErasureStore struct {
	paths        []string
	dataShards   int
	parityShards int
	totalShards  int
	chunkSize    int
	autoHeal     bool
	strictWrite  bool
	encoder      reedsolomon.Encoder
}

// ChunkHealth summarizes the on-disk state of all K+M shards for one chunk.
type ChunkHealth struct {
	ChunkID       string `json:"chunkId"`
	Status        string `json:"status"` // healthy | degraded | unrecoverable | missing
	PresentShards []int  `json:"presentShards,omitempty"`
	MissingShards []int  `json:"missingShards,omitempty"`
	CorruptShards []int  `json:"corruptShards,omitempty"`
	ErrorShards   []int  `json:"errorShards,omitempty"`
}

// HealChunkResult reports the outcome of repairing one chunk's shards.
type HealChunkResult struct {
	ChunkID        string `json:"chunkId"`
	StatusBefore   string `json:"statusBefore"`
	StatusAfter    string `json:"statusAfter"`
	RepairedShards []int  `json:"repairedShards,omitempty"`
	FailedShards   []int  `json:"failedShards,omitempty"`
}

// ScrubReport summarizes a health check (and optional repair pass) across chunks.
type ScrubReport struct {
	DataShards         int           `json:"dataShards"`
	ParityShards       int           `json:"parityShards"`
	Paths              []string      `json:"paths"`
	CheckedChunks      int           `json:"checkedChunks"`
	HealthyChunks      int           `json:"healthyChunks"`
	DegradedChunks     int           `json:"degradedChunks"`
	HealedChunks       int           `json:"healedChunks"`
	UnrecoverableCount int           `json:"unrecoverableCount"`
	MissingCount       int           `json:"missingCount"`
	Issues             []ChunkHealth `json:"issues,omitempty"`
}

type shardStatus uint8

const (
	shardOK shardStatus = iota
	shardMissing
	shardCorrupt
	shardIOError
)

type shardReadResult struct {
	index  int
	status shardStatus
	header erasureShardHeader
	data   []byte
	err    error
}

// NewErasureStore initializes a multi-path Reed-Solomon ChunkStore.
// Up to ParityShards paths may be offline at startup unless StrictWrite is true.
// Existing slot markers (.fs-erasure-slot.json) are verified strictly so
// swapped disks or changed K/M geometry fail startup immediately.
func NewErasureStore(cfg ErasureConfig) (*ErasureStore, error) {
	if cfg.DataShards < 1 || cfg.ParityShards < 1 || cfg.DataShards+cfg.ParityShards > 255 {
		return nil, fmt.Errorf("invalid erasure geometry K=%d M=%d (require K>=1, M>=1, K+M<=255)",
			cfg.DataShards, cfg.ParityShards)
	}
	if cfg.ChunkSize <= 0 || cfg.ChunkSize > maxChunkSize {
		return nil, fmt.Errorf("chunk size must be between 1 and %d bytes", maxChunkSize)
	}
	total := cfg.DataShards + cfg.ParityShards
	if len(cfg.Paths) != total {
		return nil, fmt.Errorf("erasure store requires %d paths (%d+%d), got %d",
			total, cfg.DataShards, cfg.ParityShards, len(cfg.Paths))
	}

	cleanedPaths := make([]string, total)
	seen := make(map[string]int, total)
	for i, raw := range cfg.Paths {
		p := strings.TrimSpace(raw)
		if p == "" {
			return nil, fmt.Errorf("erasure path %d is empty", i)
		}
		p = filepath.Clean(p)
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		if prev, dup := seen[p]; dup {
			return nil, fmt.Errorf("duplicate erasure path %q at slots %d and %d", p, prev, i)
		}
		seen[p] = i
		cleanedPaths[i] = p
	}

	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}

	accessible := 0
	for slot, root := range cleanedPaths {
		blobDir := filepath.Join(root, blobRoot)
		if err := os.MkdirAll(blobDir, 0o755); err != nil {
			if cfg.StrictWrite {
				return nil, fmt.Errorf("erasure root %d (%s) unavailable: %w", slot, root, err)
			}
			slog.Warn("erasure_root_unavailable", "slot", slot, "path", root, "error", err)
			continue
		}
		if err := verifyOrInitSlotMarker(root, slot, cfg.DataShards, cfg.ParityShards); err != nil {
			// A slot/geometry mismatch must fail even in degraded mode to avoid silent disk swaps.
			return nil, err
		}
		accessible++
	}

	if cfg.StrictWrite && accessible < total {
		return nil, fmt.Errorf("strict erasure mode requires all %d paths accessible, only %d ready", total, accessible)
	}
	if accessible < cfg.DataShards {
		return nil, fmt.Errorf("erasure store requires at least %d accessible paths to operate, got %d", cfg.DataShards, accessible)
	}

	return &ErasureStore{
		paths:        cleanedPaths,
		dataShards:   cfg.DataShards,
		parityShards: cfg.ParityShards,
		totalShards:  total,
		chunkSize:    cfg.ChunkSize,
		autoHeal:     cfg.AutoHeal,
		strictWrite:  cfg.StrictWrite,
		encoder:      enc,
	}, nil
}

func (es *ErasureStore) IngestStreamWithSizes(stream io.Reader, onChunk func(chunkID string) error) ([]string, []int64, int64, string, error) {
	start := time.Now()
	fullFileHasher := md5.New()

	bufferSize := es.chunkSize
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
		metrics.Default.ObserveBlob("ingest_stream", time.Since(start), 0, success, erasureBackendLabel)
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

		if err := es.saveChunk(chunkID, chunkData); err != nil {
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
			if len(buffer) == es.chunkSize {
				if err := emitChunk(buffer[:filled]); err != nil {
					return nil, nil, 0, "", err
				}
				filled = 0
				continue
			}
			grown := make([]byte, es.chunkSize)
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

func (es *ErasureStore) encodeChunkShards(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		shards := make([][]byte, es.totalShards)
		for i := range shards {
			shards[i] = []byte{}
		}
		return shards, nil
	}
	// Clip slice capacity so reedsolomon.Split cannot zero bytes beyond len(data)
	// inside the caller's reusable read buffer.
	clipped := data[:len(data):len(data)]
	shards, err := es.encoder.Split(clipped)
	if err != nil {
		return nil, err
	}
	if err := es.encoder.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

func (es *ErasureStore) saveChunk(chunkID string, data []byte) error {
	start := time.Now()
	success := false
	writtenBytes := int64(0)
	defer func() {
		metrics.Default.ObserveBlob("write_chunk", time.Since(start), writtenBytes, success, erasureBackendLabel)
	}()

	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}

	needWrite := make([]bool, es.totalShards)
	alreadyPresent := 0
	for i, root := range es.paths {
		if _, err := os.Stat(erasureShardPath(root, chunkID, i)); err == nil {
			alreadyPresent++
			continue
		}
		needWrite[i] = true
	}
	if alreadyPresent == es.totalShards {
		success = true
		return nil
	}

	shards, err := es.encodeChunkShards(data)
	if err != nil {
		return err
	}

	origSize := uint64(len(data))
	writeErrs := make([]error, es.totalShards)
	var wg sync.WaitGroup
	for i := 0; i < es.totalShards; i++ {
		if !needWrite[i] {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			frame := encodeShardFrame(es.dataShards, es.parityShards, idx, origSize, shards[idx])
			shardPath := erasureShardPath(es.paths[idx], chunkID, idx)
			dir := filepath.Dir(shardPath)
			tmpPattern := erasureShardFileName(chunkID, idx) + ".tmp-*"
			writeErrs[idx] = writeFileAtomic(dir, tmpPattern, shardPath, frame)
		}(i)
	}
	wg.Wait()

	available := 0
	var firstErr error
	for i := 0; i < es.totalShards; i++ {
		if !needWrite[i] || writeErrs[i] == nil {
			available++
			continue
		}
		if firstErr == nil {
			firstErr = writeErrs[i]
		}
	}

	required := es.dataShards
	if es.strictWrite {
		required = es.totalShards
	}
	if available < required {
		return fmt.Errorf("erasure write quorum not met for chunk %s (%d/%d shards available, need %d): %w",
			chunkID, available, es.totalShards, required, firstErr)
	}
	if available < es.totalShards {
		slog.Warn("erasure_degraded_write",
			"chunk_id", chunkID,
			"available_shards", available,
			"total_shards", es.totalShards,
			"error", firstErr,
		)
	}
	writtenBytes = int64(len(data))
	success = true
	return nil
}

func (es *ErasureStore) AssembleStream(chunkIDs []string, w *io.PipeWriter) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveBlob("assemble_stream", time.Since(start), 0, success, erasureBackendLabel)
	}()

	for _, chunkID := range chunkIDs {
		data, err := es.GetBlob(chunkID)
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

func (es *ErasureStore) GetBlob(chunkID string) ([]byte, error) {
	start := time.Now()
	success := false
	var readBytes int64
	defer func() {
		metrics.Default.ObserveBlob("read_chunk", time.Since(start), readBytes, success, erasureBackendLabel)
	}()

	if !isValidChunkID(chunkID) {
		return nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}

	results := make([]shardReadResult, es.totalShards)
	es.readShardRange(chunkID, 0, es.dataShards, results)

	if data, ok := es.tryAssembleDataFastPath(chunkID, results[:es.dataShards]); ok {
		readBytes = int64(len(data))
		success = true
		return data, nil
	}

	// Read parity shards too so we can reconstruct and optionally auto-heal.
	es.readShardRange(chunkID, es.dataShards, es.totalShards, results)
	plaintext, shards, origSize, err := reconstructChunkFromResults(es.encoder, es.dataShards, es.parityShards, chunkID, results)
	if err != nil {
		return nil, err
	}

	if es.autoHeal {
		es.healDamagedShards(chunkID, origSize, shards, results)
	}

	readBytes = int64(len(plaintext))
	success = true
	return plaintext, nil
}

func (es *ErasureStore) readShardRange(chunkID string, startIdx, endIdx int, results []shardReadResult) {
	var wg sync.WaitGroup
	for i := startIdx; i < endIdx; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = readAndVerifyShard(es.paths[idx], chunkID, idx, es.dataShards, es.parityShards)
		}(i)
	}
	wg.Wait()
}

func readAndVerifyShard(root, chunkID string, expectedIdx, expectedK, expectedM int) shardReadResult {
	res := shardReadResult{index: expectedIdx}
	path := erasureShardPath(root, chunkID, expectedIdx)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			res.status = shardMissing
			res.err = err
			return res
		}
		res.status = shardIOError
		res.err = err
		return res
	}
	hdr, payload, err := decodeAndVerifyShardFrame(raw)
	if err != nil {
		res.status = shardCorrupt
		res.err = err
		return res
	}
	if int(hdr.DataShards) != expectedK || int(hdr.ParityShards) != expectedM || int(hdr.ShardIndex) != expectedIdx {
		res.status = shardCorrupt
		res.err = fmt.Errorf("%w: shard geometry/index mismatch (got idx=%d %d+%d, want idx=%d %d+%d)",
			ErrChunkIntegrity, hdr.ShardIndex, hdr.DataShards, hdr.ParityShards, expectedIdx, expectedK, expectedM)
		return res
	}
	res.status = shardOK
	res.header = hdr
	res.data = payload
	return res
}

func (es *ErasureStore) tryAssembleDataFastPath(chunkID string, dataResults []shardReadResult) ([]byte, bool) {
	if len(dataResults) != es.dataShards {
		return nil, false
	}
	var origSize uint64
	var payloadSize uint32
	for i, res := range dataResults {
		if res.status != shardOK {
			return nil, false
		}
		if i == 0 {
			origSize = res.header.OriginalChunkSize
			payloadSize = res.header.ShardPayloadSize
		} else if res.header.OriginalChunkSize != origSize || res.header.ShardPayloadSize != payloadSize {
			return nil, false
		}
	}
	plaintext := assembleDataShards(dataResults, origSize, es.dataShards)
	if err := verifyChunkData(chunkID, plaintext); err != nil {
		return nil, false
	}
	return plaintext, true
}

func assembleDataShards(dataResults []shardReadResult, origSize uint64, dataShards int) []byte {
	if origSize == 0 {
		return []byte{}
	}
	out := make([]byte, 0, int(origSize))
	remaining := int(origSize)
	for i := 0; i < dataShards && remaining > 0; i++ {
		take := len(dataResults[i].data)
		if take > remaining {
			take = remaining
		}
		out = append(out, dataResults[i].data[:take]...)
		remaining -= take
	}
	return out
}

type shardDims struct {
	origSize    uint64
	payloadSize uint32
}

func reconstructChunkFromResults(enc reedsolomon.Encoder, k, m int, chunkID string, results []shardReadResult) ([]byte, [][]byte, uint64, error) {
	total := k + m
	groups := make(map[shardDims][]shardReadResult)
	hasCorrupt := false
	hasIOErr := false
	var firstIOErr error

	for _, res := range results {
		switch res.status {
		case shardOK:
			dims := shardDims{origSize: res.header.OriginalChunkSize, payloadSize: res.header.ShardPayloadSize}
			groups[dims] = append(groups[dims], res)
		case shardCorrupt:
			hasCorrupt = true
		case shardIOError:
			hasIOErr = true
			if firstIOErr == nil {
				firstIOErr = res.err
			}
		}
	}

	var chosenDims shardDims
	var chosenShards []shardReadResult
	for dims, members := range groups {
		if len(members) >= k && len(members) > len(chosenShards) {
			chosenDims = dims
			chosenShards = members
		}
	}

	if len(chosenShards) < k {
		if hasCorrupt {
			return nil, nil, 0, fmt.Errorf("%w: chunk %s has insufficient valid shards (%d/%d needed)",
				ErrChunkIntegrity, chunkID, len(chosenShards), k)
		}
		if hasIOErr {
			return nil, nil, 0, fmt.Errorf("%w: chunk %s unavailable (%v)", ErrChunkMissing, chunkID, firstIOErr)
		}
		return nil, nil, 0, ErrChunkMissing
	}

	shards := make([][]byte, total)
	for _, res := range chosenShards {
		shards[res.index] = res.data
	}

	if err := enc.Reconstruct(shards); err != nil {
		return nil, nil, 0, fmt.Errorf("%w: reconstruct %s: %v", ErrChunkIntegrity, chunkID, err)
	}

	plaintext := make([]byte, 0, int(chosenDims.origSize))
	remaining := int(chosenDims.origSize)
	for i := 0; i < k && remaining > 0; i++ {
		take := len(shards[i])
		if take > remaining {
			take = remaining
		}
		plaintext = append(plaintext, shards[i][:take]...)
		remaining -= take
	}
	if err := verifyChunkData(chunkID, plaintext); err != nil {
		return nil, nil, 0, err
	}
	return plaintext, shards, chosenDims.origSize, nil
}

func (es *ErasureStore) healDamagedShards(chunkID string, origSize uint64, shards [][]byte, results []shardReadResult) ([]int, []int) {
	var repaired []int
	var failed []int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < es.totalShards; i++ {
		res := results[i]
		if res.status == shardOK && res.header.OriginalChunkSize == origSize {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			frame := encodeShardFrame(es.dataShards, es.parityShards, idx, origSize, shards[idx])
			shardPath := erasureShardPath(es.paths[idx], chunkID, idx)
			err := writeFileAtomic(filepath.Dir(shardPath), erasureShardFileName(chunkID, idx)+".tmp-*", shardPath, frame)
			mu.Lock()
			if err != nil {
				failed = append(failed, idx)
				slog.Warn("erasure_auto_heal_failed", "chunk_id", chunkID, "slot", idx, "path", shardPath, "error", err)
			} else {
				repaired = append(repaired, idx)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Ints(repaired)
	sort.Ints(failed)
	return repaired, failed
}

func (es *ErasureStore) DeleteBlob(chunkID string) error {
	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	var errs []error
	for i, root := range es.paths {
		err := os.Remove(erasureShardPath(root, chunkID, i))
		if err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// StatChunk returns the newest shard modification time and reports whether at
// least K shards exist on disk so reads/ranges preflight accurately even when
// up to M shards are degraded.
func (es *ErasureStore) StatChunk(chunkID string) (time.Time, bool) {
	if !isValidChunkID(chunkID) {
		return time.Time{}, false
	}
	found := 0
	var newest time.Time
	for i, root := range es.paths {
		info, err := os.Stat(erasureShardPath(root, chunkID, i))
		if err != nil {
			continue
		}
		found++
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}
	return newest, found >= es.dataShards
}

func (es *ErasureStore) RemoveStaleTempFiles(olderThan time.Time) (int, error) {
	removed := 0
	var errs []error
	for _, root := range es.paths {
		blobDir := filepath.Join(root, blobRoot)
		err := filepath.WalkDir(blobDir, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if os.IsNotExist(walkErr) {
					return nil
				}
				return walkErr
			}
			if entry.IsDir() || !entry.Type().IsRegular() || !isErasureTempName(entry.Name()) {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return nil
			}
			if !info.ModTime().Before(olderThan) {
				return nil
			}
			if err := os.Remove(path); err == nil {
				removed++
			}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, errors.Join(errs...)
}

// ListChunks returns all unique chunk IDs that have at least one shard file
// present on any configured path.
func (es *ErasureStore) ListChunks() ([]string, error) {
	seen := make(map[string]struct{})
	var errs []error
	for _, root := range es.paths {
		blobDir := filepath.Join(root, blobRoot)
		err := filepath.WalkDir(blobDir, func(_ string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if os.IsNotExist(walkErr) {
					return nil
				}
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if chunkID, _, ok := parseErasureShardFileName(entry.Name()); ok {
				seen[chunkID] = struct{}{}
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, errors.Join(errs...)
}

func (es *ErasureStore) inspectChunkResults(chunkID string) (ChunkHealth, []shardReadResult, error) {
	if !isValidChunkID(chunkID) {
		return ChunkHealth{}, nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	results := make([]shardReadResult, es.totalShards)
	es.readShardRange(chunkID, 0, es.totalShards, results)

	health := ChunkHealth{ChunkID: chunkID}
	validCount := 0
	for i, res := range results {
		switch res.status {
		case shardOK:
			health.PresentShards = append(health.PresentShards, i)
			validCount++
		case shardMissing:
			health.MissingShards = append(health.MissingShards, i)
		case shardCorrupt:
			health.CorruptShards = append(health.CorruptShards, i)
		case shardIOError:
			health.ErrorShards = append(health.ErrorShards, i)
		}
	}

	switch {
	case validCount == 0 && len(health.CorruptShards) == 0 && len(health.ErrorShards) == 0:
		health.Status = "missing"
	case validCount == es.totalShards:
		health.Status = "healthy"
	case validCount >= es.dataShards:
		// Confirm the whole chunk reconstructs & matches chunkID SHA-256.
		if _, _, _, err := reconstructChunkFromResults(es.encoder, es.dataShards, es.parityShards, chunkID, results); err != nil {
			health.Status = "unrecoverable"
		} else {
			health.Status = "degraded"
		}
	default:
		health.Status = "unrecoverable"
	}
	return health, results, nil
}

// InspectChunk verifies all K+M shards for chunkID without modifying disk state.
func (es *ErasureStore) InspectChunk(chunkID string) (ChunkHealth, error) {
	health, _, err := es.inspectChunkResults(chunkID)
	return health, err
}

// HealChunk reconstructs and rewrites any missing or corrupt shards for chunkID.
func (es *ErasureStore) HealChunk(chunkID string) (HealChunkResult, error) {
	health, results, err := es.inspectChunkResults(chunkID)
	if err != nil {
		return HealChunkResult{}, err
	}
	res := HealChunkResult{
		ChunkID:      chunkID,
		StatusBefore: health.Status,
		StatusAfter:  health.Status,
	}
	if health.Status == "healthy" {
		return res, nil
	}
	if health.Status != "degraded" {
		return res, fmt.Errorf("chunk %s is %s and cannot be healed", chunkID, health.Status)
	}

	_, shards, origSize, err := reconstructChunkFromResults(es.encoder, es.dataShards, es.parityShards, chunkID, results)
	if err != nil {
		return res, err
	}
	repaired, failed := es.healDamagedShards(chunkID, origSize, shards, results)
	res.RepairedShards = repaired
	res.FailedShards = failed

	afterHealth, _, err := es.inspectChunkResults(chunkID)
	if err == nil {
		res.StatusAfter = afterHealth.Status
	}
	if len(failed) > 0 {
		return res, fmt.Errorf("chunk %s repaired %d shard(s) but failed on slot(s) %v", chunkID, len(repaired), failed)
	}
	return res, nil
}

// ScrubAndHeal checks chunkIDs (or all discovered on-disk chunks when chunkIDs
// is nil/empty), optionally repairs degraded chunks, and returns a summary.
func (es *ErasureStore) ScrubAndHeal(repair bool, chunkIDs []string) (*ScrubReport, error) {
	var listErr error
	if len(chunkIDs) == 0 {
		chunkIDs, listErr = es.ListChunks()
	} else {
		dedup := make(map[string]struct{}, len(chunkIDs))
		unique := make([]string, 0, len(chunkIDs))
		for _, id := range chunkIDs {
			if _, ok := dedup[id]; ok {
				continue
			}
			dedup[id] = struct{}{}
			unique = append(unique, id)
		}
		sort.Strings(unique)
		chunkIDs = unique
	}

	report := &ScrubReport{
		DataShards:   es.dataShards,
		ParityShards: es.parityShards,
		Paths:        append([]string(nil), es.paths...),
	}
	for _, chunkID := range chunkIDs {
		health, results, err := es.inspectChunkResults(chunkID)
		if err != nil {
			return report, err
		}
		report.CheckedChunks++
		switch health.Status {
		case "healthy":
			report.HealthyChunks++
		case "degraded":
			report.DegradedChunks++
			if repair {
				_, shards, origSize, recErr := reconstructChunkFromResults(es.encoder, es.dataShards, es.parityShards, chunkID, results)
				if recErr == nil {
					_, failed := es.healDamagedShards(chunkID, origSize, shards, results)
					after, _, afterErr := es.inspectChunkResults(chunkID)
					if afterErr == nil && after.Status == "healthy" && len(failed) == 0 {
						report.HealedChunks++
						report.HealthyChunks++
						report.DegradedChunks--
						continue
					}
					if afterErr == nil {
						health = after
					}
				}
			}
			report.Issues = append(report.Issues, health)
		case "unrecoverable":
			report.UnrecoverableCount++
			report.Issues = append(report.Issues, health)
		case "missing":
			report.MissingCount++
			report.Issues = append(report.Issues, health)
		}
	}
	return report, listErr
}
