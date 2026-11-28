package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const (
	erasureMagicStr          = "FSER"
	erasureFormatVersion     = uint8(1)
	erasureAlgoRSVandermonde = uint8(1)
	erasureHeaderSize        = 64
	erasureSlotMarkerFile    = ".fs-erasure-slot.json"
	erasureSlotVersion       = 1
)

type erasureShardHeader struct {
	Version           uint8
	Algorithm         uint8
	DataShards        uint8
	ParityShards      uint8
	ShardIndex        uint8
	ShardPayloadSize  uint32
	OriginalChunkSize uint64
	ShardSHA256       [sha256.Size]byte
}

type erasureSlotMarker struct {
	Version      int `json:"version"`
	Slot         int `json:"slot"`
	DataShards   int `json:"dataShards"`
	ParityShards int `json:"parityShards"`
}

func expectedShardPayloadSize(origSize uint64, dataShards int) (int, error) {
	if dataShards <= 0 {
		return 0, errors.New("data shards must be positive")
	}
	if origSize > maxChunkSize {
		return 0, fmt.Errorf("original chunk size %d exceeds maximum %d", origSize, maxChunkSize)
	}
	if origSize == 0 {
		return 0, nil
	}
	k := uint64(dataShards)
	return int((origSize + k - 1) / k), nil
}

func encodeShardFrame(k, m, shardIndex int, origSize uint64, payload []byte) []byte {
	frame := make([]byte, erasureHeaderSize+len(payload))
	copy(frame[0:4], erasureMagicStr)
	frame[4] = erasureFormatVersion
	frame[5] = erasureAlgoRSVandermonde
	frame[6] = uint8(k)
	frame[7] = uint8(m)
	frame[8] = uint8(shardIndex)
	// [9:12] reserved zeroes
	binary.BigEndian.PutUint32(frame[12:16], uint32(len(payload)))
	binary.BigEndian.PutUint64(frame[16:24], origSize)
	sum := sha256.Sum256(payload)
	copy(frame[24:56], sum[:])
	// [56:64] reserved zeroes
	copy(frame[erasureHeaderSize:], payload)
	return frame
}

func decodeAndVerifyShardFrame(raw []byte) (erasureShardHeader, []byte, error) {
	var hdr erasureShardHeader
	if len(raw) < erasureHeaderSize {
		return hdr, nil, fmt.Errorf("%w: truncated shard frame (%d bytes)", ErrChunkIntegrity, len(raw))
	}
	if string(raw[0:4]) != erasureMagicStr {
		return hdr, nil, fmt.Errorf("%w: invalid shard magic", ErrChunkIntegrity)
	}
	hdr.Version = raw[4]
	if hdr.Version != erasureFormatVersion {
		return hdr, nil, fmt.Errorf("%w: unsupported shard version %d", ErrChunkIntegrity, hdr.Version)
	}
	hdr.Algorithm = raw[5]
	if hdr.Algorithm != erasureAlgoRSVandermonde {
		return hdr, nil, fmt.Errorf("%w: unsupported erasure algorithm %d", ErrChunkIntegrity, hdr.Algorithm)
	}
	hdr.DataShards = raw[6]
	hdr.ParityShards = raw[7]
	if hdr.DataShards == 0 || hdr.ParityShards == 0 || int(hdr.DataShards)+int(hdr.ParityShards) > 255 {
		return hdr, nil, fmt.Errorf("%w: invalid shard geometry K=%d M=%d", ErrChunkIntegrity, hdr.DataShards, hdr.ParityShards)
	}
	hdr.ShardIndex = raw[8]
	totalShards := int(hdr.DataShards) + int(hdr.ParityShards)
	if int(hdr.ShardIndex) >= totalShards {
		return hdr, nil, fmt.Errorf("%w: shard index %d out of range [0,%d)", ErrChunkIntegrity, hdr.ShardIndex, totalShards)
	}
	hdr.ShardPayloadSize = binary.BigEndian.Uint32(raw[12:16])
	hdr.OriginalChunkSize = binary.BigEndian.Uint64(raw[16:24])
	copy(hdr.ShardSHA256[:], raw[24:56])

	expPayloadLen, err := expectedShardPayloadSize(hdr.OriginalChunkSize, int(hdr.DataShards))
	if err != nil {
		return hdr, nil, fmt.Errorf("%w: %v", ErrChunkIntegrity, err)
	}
	if int(hdr.ShardPayloadSize) != expPayloadLen {
		return hdr, nil, fmt.Errorf("%w: payload size %d does not match expected %d for chunk size %d (K=%d)",
			ErrChunkIntegrity, hdr.ShardPayloadSize, expPayloadLen, hdr.OriginalChunkSize, hdr.DataShards)
	}
	if len(raw) != erasureHeaderSize+expPayloadLen {
		return hdr, nil, fmt.Errorf("%w: frame length %d does not match header size %d+%d",
			ErrChunkIntegrity, len(raw), erasureHeaderSize, expPayloadLen)
	}

	payload := raw[erasureHeaderSize:]
	actualSum := sha256.Sum256(payload)
	if actualSum != hdr.ShardSHA256 {
		return hdr, nil, fmt.Errorf("%w: shard payload checksum mismatch", ErrChunkIntegrity)
	}
	return hdr, payload, nil
}

func erasureShardFileName(chunkID string, idx int) string {
	return fmt.Sprintf("%s.s%03d", chunkID, idx)
}

func erasureShardPath(root, chunkID string, idx int) string {
	return filepath.Join(root, blobRoot, chunkID[:2], chunkID[2:4], erasureShardFileName(chunkID, idx))
}

func parseErasureShardFileName(name string) (string, int, bool) {
	const hexLen = sha256.Size * 2
	// "<64-hex>.s<3-digits>" => 69 chars
	if len(name) != hexLen+5 || name[hexLen:hexLen+2] != ".s" {
		return "", 0, false
	}
	chunkID := name[:hexLen]
	if !isValidChunkID(chunkID) {
		return "", 0, false
	}
	idxStr := name[hexLen+2:]
	for i := 0; i < len(idxStr); i++ {
		if idxStr[i] < '0' || idxStr[i] > '9' {
			return "", 0, false
		}
	}
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 0 || idx > 254 {
		return "", 0, false
	}
	return chunkID, idx, true
}

func isErasureTempName(name string) bool {
	const hexLen = sha256.Size * 2
	const marker = ".tmp-"
	// "<64-hex>.s<3-digits>.tmp-<digits>"
	minLen := hexLen + 5 + len(marker) + 1
	if len(name) < minLen || name[hexLen+5:hexLen+5+len(marker)] != marker {
		return false
	}
	if _, _, ok := parseErasureShardFileName(name[:hexLen+5]); !ok {
		return false
	}
	for i := hexLen + 5 + len(marker); i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

func writeFileAtomic(dir, tempPattern, targetPath string, data []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(dir, tempPattern)
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
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return err
	}
	cleanup = false

	if err := syncDir(dir); err != nil {
		_ = os.Remove(targetPath)
		return err
	}
	return nil
}

func verifyOrInitSlotMarker(root string, slot, dataShards, parityShards int) error {
	markerPath := filepath.Join(root, erasureSlotMarkerFile)
	raw, err := os.ReadFile(markerPath)
	if err == nil {
		var marker erasureSlotMarker
		if jsonErr := json.Unmarshal(raw, &marker); jsonErr != nil {
			return fmt.Errorf("corrupt slot marker %s: %w", markerPath, jsonErr)
		}
		if marker.Version != erasureSlotVersion {
			return fmt.Errorf("unsupported slot marker version %d in %s", marker.Version, markerPath)
		}
		if marker.Slot != slot || marker.DataShards != dataShards || marker.ParityShards != parityShards {
			return fmt.Errorf("slot marker mismatch at %s: found slot=%d (%d+%d), configured slot=%d (%d+%d)",
				markerPath, marker.Slot, marker.DataShards, marker.ParityShards, slot, dataShards, parityShards)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}

	encoded, err := json.MarshalIndent(erasureSlotMarker{
		Version:      erasureSlotVersion,
		Slot:         slot,
		DataShards:   dataShards,
		ParityShards: parityShards,
	}, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return writeFileAtomic(root, erasureSlotMarkerFile+".tmp-*", markerPath, encoded)
}
