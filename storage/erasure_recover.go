package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
)

// RecoverChunkFromDirs reconstructs the plaintext bytes of chunkID by scanning
// searchDirs in any order (or a single flat rescue directory containing copied
// "<chunkID>.sNNN" files). It auto-detects K, M, and original chunk size from
// the self-describing 64-byte shard headers, verifies every shard's SHA-256,
// reconstructs any missing data shards, and verifies SHA256(plaintext) == chunkID.
func RecoverChunkFromDirs(searchDirs []string, chunkID string) ([]byte, error) {
	if !isValidChunkID(chunkID) {
		return nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	if len(searchDirs) == 0 {
		return nil, fmt.Errorf("%w: no search directories provided", ErrChunkMissing)
	}

	type recoverKey struct {
		k           uint8
		m           uint8
		origSize    uint64
		payloadSize uint32
	}

	byGeometry := make(map[recoverKey]map[int][]byte)
	hasCorrupt := false
	seenFiles := make(map[string]struct{})

	inspectCandidate := func(path string) {
		if _, done := seenFiles[path]; done {
			return
		}
		seenFiles[path] = struct{}{}

		raw, err := os.ReadFile(path)
		if err != nil {
			return
		}
		hdr, payload, err := decodeAndVerifyShardFrame(raw)
		if err != nil {
			hasCorrupt = true
			return
		}
		key := recoverKey{
			k:           hdr.DataShards,
			m:           hdr.ParityShards,
			origSize:    hdr.OriginalChunkSize,
			payloadSize: hdr.ShardPayloadSize,
		}
		slots := byGeometry[key]
		if slots == nil {
			slots = make(map[int][]byte)
			byGeometry[key] = slots
		}
		slots[int(hdr.ShardIndex)] = payload
	}

	prefix := chunkID + ".s"
	for _, rawDir := range searchDirs {
		dir := filepath.Clean(rawDir)
		candidateDirs := []string{
			filepath.Join(dir, blobRoot, chunkID[:2], chunkID[2:4]),
			dir,
		}
		for _, cdir := range candidateDirs {
			entries, err := os.ReadDir(cdir)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				name := entry.Name()
				if len(name) < len(prefix)+3 || name[:len(prefix)] != prefix {
					continue
				}
				foundChunkID, _, ok := parseErasureShardFileName(name)
				if !ok || foundChunkID != chunkID {
					continue
				}
				inspectCandidate(filepath.Join(cdir, name))
			}
		}
	}

	var chosenKey recoverKey
	var chosenSlots map[int][]byte
	for key, slots := range byGeometry {
		if len(slots) >= int(key.k) && len(slots) > len(chosenSlots) {
			chosenKey = key
			chosenSlots = slots
		}
	}

	if len(chosenSlots) == 0 {
		if hasCorrupt {
			return nil, fmt.Errorf("%w: no valid shards found for chunk %s", ErrChunkIntegrity, chunkID)
		}
		return nil, fmt.Errorf("%w: chunk %s", ErrChunkMissing, chunkID)
	}

	enc, err := reedsolomon.New(int(chosenKey.k), int(chosenKey.m))
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, int(chosenKey.k)+int(chosenKey.m))
	for idx, payload := range chosenSlots {
		if idx >= 0 && idx < len(shards) {
			shards[idx] = payload
		}
	}
	if err := enc.ReconstructData(shards); err != nil {
		return nil, fmt.Errorf("%w: reconstruct %s: %v", ErrChunkIntegrity, chunkID, err)
	}

	plaintext := make([]byte, 0, int(chosenKey.origSize))
	remaining := int(chosenKey.origSize)
	for i := 0; i < int(chosenKey.k) && remaining > 0; i++ {
		take := len(shards[i])
		if take > remaining {
			take = remaining
		}
		plaintext = append(plaintext, shards[i][:take]...)
		remaining -= take
	}
	if err := verifyChunkData(chunkID, plaintext); err != nil {
		return nil, err
	}
	return plaintext, nil
}
