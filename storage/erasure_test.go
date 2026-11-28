package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func makeErasurePaths(t *testing.T, n int) []string {
	t.Helper()
	base := t.TempDir()
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		paths[i] = filepath.Join(base, filepath.FromSlash("disk-"+string(rune('0'+i))))
	}
	return paths
}

func TestErasureStoreRoundTripSizes(t *testing.T) {
	paths := makeErasurePaths(t, 6)
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    1024,
		AutoHeal:     true,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	sizes := []int{0, 1, 3, 4, 5, 17, 1023, 1024, 1025, 4099}
	for _, sz := range sizes {
		payload := bytes.Repeat([]byte{byte(sz%251 + 1), 0x7a, 0x00, 0xff}, sz/4+1)[:sz]
		chunkIDs, chunkSizes, totalSize, etag, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
		if err != nil {
			t.Fatalf("size=%d IngestStreamWithSizes: %v", sz, err)
		}
		if totalSize != int64(sz) {
			t.Fatalf("size=%d totalSize=%d", sz, totalSize)
		}
		if sz > 0 && (len(chunkIDs) == 0 || len(chunkSizes) != len(chunkIDs) || etag == "") {
			t.Fatalf("size=%d unexpected chunk metadata: ids=%d sizes=%d etag=%q", sz, len(chunkIDs), len(chunkSizes), etag)
		}

		pr, pw := io.Pipe()
		var assembleErr error
		go func() {
			assembleErr = es.AssembleStream(chunkIDs, pw)
			_ = pw.Close()
		}()
		got, err := io.ReadAll(pr)
		if err != nil {
			t.Fatalf("size=%d ReadAll: %v", sz, err)
		}
		if assembleErr != nil {
			t.Fatalf("size=%d AssembleStream: %v", sz, assembleErr)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("size=%d round trip byte mismatch", sz)
		}
	}
}

func TestErasureStoreSurvivesUpToMDeletesAndCorruptions(t *testing.T) {
	paths := makeErasurePaths(t, 6)
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    4096,
		AutoHeal:     false,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	payload := bytes.Repeat([]byte("erasure-integrity-check-"), 400)
	chunkIDs, _, _, _, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("IngestStreamWithSizes: %v", err)
	}
	cid := chunkIDs[0]

	// Delete M=2 data shards -> still readable.
	for _, slot := range []int{0, 2} {
		if err := os.Remove(erasureShardPath(paths[slot], cid, slot)); err != nil {
			t.Fatalf("Remove slot %d: %v", slot, err)
		}
	}
	got, err := es.GetBlob(cid)
	if err != nil {
		t.Fatalf("GetBlob with 2 deleted data shards: %v", err)
	}
	if !bytes.Equal(got, payload[:len(got)]) {
		t.Fatalf("reconstructed payload mismatch after deleting data shards")
	}

	// Because AutoHeal=false above, restore via HealChunk, then test corruption cases.
	if _, err := es.HealChunk(cid); err != nil {
		t.Fatalf("HealChunk: %v", err)
	}

	// Case 1: Flip a payload byte in shard 1 + truncate shard 4 (mixed data + parity).
	p1 := erasureShardPath(paths[1], cid, 1)
	raw1, err := os.ReadFile(p1)
	if err != nil {
		t.Fatalf("ReadFile p1: %v", err)
	}
	raw1[len(raw1)-1] ^= 0xff
	if err := os.WriteFile(p1, raw1, 0o644); err != nil {
		t.Fatalf("WriteFile p1: %v", err)
	}

	p4 := erasureShardPath(paths[4], cid, 4)
	if err := os.WriteFile(p4, []byte("short"), 0o644); err != nil {
		t.Fatalf("WriteFile p4: %v", err)
	}

	got, err = es.GetBlob(cid)
	if err != nil {
		t.Fatalf("GetBlob with 2 corrupt shards: %v", err)
	}
	if len(got) != len(payload[:len(got)]) {
		t.Fatalf("length mismatch after corrupt shards")
	}

	// Case 2: Append trailing byte to shard 0 (makes 3 damaged shards = M+1) -> must fail!
	p0 := erasureShardPath(paths[0], cid, 0)
	raw0, err := os.ReadFile(p0)
	if err != nil {
		t.Fatalf("ReadFile p0: %v", err)
	}
	if err := os.WriteFile(p0, append(raw0, 0x42), 0o644); err != nil {
		t.Fatalf("WriteFile p0: %v", err)
	}
	if _, err := es.GetBlob(cid); !errors.Is(err, ErrChunkIntegrity) {
		t.Fatalf("GetBlob with M+1 damaged shards err = %v, want ErrChunkIntegrity", err)
	}
}

func TestErasureStoreAutoHealPermitsRollingFailures(t *testing.T) {
	paths := makeErasurePaths(t, 6)
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    1024,
		AutoHeal:     true,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	payload := bytes.Repeat([]byte("rolling-heal-"), 150)[:900]
	ids, _, _, _, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cid := ids[0]

	// Delete slots 0 and 1, read to trigger auto-heal.
	_ = os.Remove(erasureShardPath(paths[0], cid, 0))
	_ = os.Remove(erasureShardPath(paths[1], cid, 1))
	got, err := es.GetBlob(cid)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("first degraded read failed: gotLen=%d err=%v", len(got), err)
	}

	// Verify slots 0 and 1 were recreated, then delete two *different* slots (2 and 5).
	_ = os.Remove(erasureShardPath(paths[2], cid, 2))
	_ = os.Remove(erasureShardPath(paths[5], cid, 5))
	got, err = es.GetBlob(cid)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("second rolling degraded read failed: err=%v", err)
	}
}

func TestErasureStoreMPlusOneMissingFailsCleanly(t *testing.T) {
	paths := makeErasurePaths(t, 5) // 3+2
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   3,
		ParityShards: 2,
		ChunkSize:    1024,
		AutoHeal:     true,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	ids, _, _, _, err := es.IngestStreamWithSizes(strings.NewReader("missing-quorum-check"), nil)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cid := ids[0]

	// Delete 3 of 5 shards (leaves only 2 < K=3).
	for _, slot := range []int{0, 1, 3} {
		_ = os.Remove(erasureShardPath(paths[slot], cid, slot))
	}
	if _, ok := es.StatChunk(cid); ok {
		t.Fatalf("StatChunk = true with only 2/3 shards, want false")
	}
	if _, err := es.GetBlob(cid); !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("GetBlob err = %v, want ErrChunkMissing", err)
	}
}

func TestErasureStoreDegradedWriteStrictWriteAndDedupHeal(t *testing.T) {
	paths := makeErasurePaths(t, 4) // 2+2
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   2,
		ParityShards: 2,
		ChunkSize:    1024,
		AutoHeal:     false,
		StrictWrite:  false,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	// Make slot 3 non-writable by replacing it with a regular file where blobs/ would go (or read-only).
	// Removing directory and creating a regular file blocks MkdirAll reliably even under different umasks.
	_ = os.RemoveAll(filepath.Join(paths[3], blobRoot))
	if err := os.WriteFile(filepath.Join(paths[3], blobRoot), []byte("offline"), 0o444); err != nil {
		t.Fatalf("block slot 3: %v", err)
	}

	payload := []byte("degraded-write-and-dedup-heal")
	ids, _, _, _, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("non-strict degraded write failed: %v", err)
	}
	cid := ids[0]

	// StrictWrite must fail on the exact same blocked disk when writing a new chunk.
	strictES, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   2,
		ParityShards: 2,
		ChunkSize:    1024,
		StrictWrite:  true,
	})
	if err == nil {
		_, _, _, _, err = strictES.IngestStreamWithSizes(strings.NewReader("strict-should-fail"), nil)
		if err == nil {
			t.Fatalf("expected strict write to fail with blocked disk")
		}
	}

	// Restore slot 3 and delete slot 0; re-uploading identical bytes must repopulate both missing shards.
	_ = os.Remove(filepath.Join(paths[3], blobRoot))
	_ = os.Remove(erasureShardPath(paths[0], cid, 0))

	ids2, _, _, _, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("re-upload dedup heal failed: %v", err)
	}
	if ids2[0] != cid {
		t.Fatalf("re-upload chunk ID mismatch")
	}
	health, err := es.InspectChunk(cid)
	if err != nil {
		t.Fatalf("InspectChunk: %v", err)
	}
	if health.Status != "healthy" {
		t.Fatalf("status after dedup re-upload = %q, want healthy (%+v)", health.Status, health)
	}
}

func TestErasureStoreSlotMismatchAndDegradedStartup(t *testing.T) {
	paths := makeErasurePaths(t, 4) // 2+2
	if _, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   2,
		ParityShards: 2,
		ChunkSize:    1024,
	}); err != nil {
		t.Fatalf("initial NewErasureStore: %v", err)
	}

	// Swapping paths[0] and paths[1] must fail immediately.
	swapped := []string{paths[1], paths[0], paths[2], paths[3]}
	if _, err := NewErasureStore(ErasureConfig{
		Paths:        swapped,
		DataShards:   2,
		ParityShards: 2,
		ChunkSize:    1024,
	}); err == nil {
		t.Fatalf("expected slot swap to fail startup")
	}

	// Changing K/M on existing roots must fail immediately.
	if _, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   3,
		ParityShards: 1,
		ChunkSize:    1024,
	}); err == nil {
		t.Fatalf("expected K/M mismatch to fail startup")
	}

	// Non-strict startup tolerates one inaccessible path when >= K paths work.
	blockedBase := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockedBase, []byte("file"), 0o644); err != nil {
		t.Fatalf("WriteFile blockedBase: %v", err)
	}
	degradedPaths := []string{paths[0], paths[1], paths[2], filepath.Join(blockedBase, "sub")}
	if _, err := NewErasureStore(ErasureConfig{
		Paths:        degradedPaths,
		DataShards:   2,
		ParityShards: 2,
		ChunkSize:    1024,
		StrictWrite:  false,
	}); err != nil {
		t.Fatalf("degraded startup with 3/4 roots failed: %v", err)
	}
}

func TestErasureStoreDeleteTempCleanupAndRecoverFromDirs(t *testing.T) {
	paths := makeErasurePaths(t, 6) // 4+2
	es, err := NewErasureStore(ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    512,
		AutoHeal:     true,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	payload := bytes.Repeat([]byte("recover-me-from-anywhere-"), 15)
	ids, _, _, _, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	cid := ids[0]

	// 1. Order-independent recovery using scrambled subset of 4 surviving roots (3 parity/data mix).
	scrambledSurviving := []string{paths[5], paths[2], paths[0], paths[4]}
	rec1, err := RecoverChunkFromDirs(scrambledSurviving, cid)
	if err != nil {
		t.Fatalf("RecoverChunkFromDirs scrambled: %v", err)
	}
	if !bytes.Equal(rec1, payload) {
		t.Fatalf("scrambled recovery mismatch")
	}

	// 2. Single flat rescue dir containing 4 arbitrary surviving shards + one corrupted extra shard.
	rescueDir := filepath.Join(t.TempDir(), "rescue")
	if err := os.MkdirAll(rescueDir, 0o755); err != nil {
		t.Fatalf("MkdirAll rescue: %v", err)
	}
	for _, slot := range []int{1, 3, 4, 5} {
		raw, err := os.ReadFile(erasureShardPath(paths[slot], cid, slot))
		if err != nil {
			t.Fatalf("ReadFile slot %d: %v", slot, err)
		}
		if err := os.WriteFile(filepath.Join(rescueDir, erasureShardFileName(cid, slot)), raw, 0o644); err != nil {
			t.Fatalf("copy shard %d to rescue: %v", slot, err)
		}
	}
	if err := os.WriteFile(filepath.Join(rescueDir, erasureShardFileName(cid, 0)), []byte("bad"), 0o644); err != nil {
		t.Fatalf("write corrupt shard 0 in rescue: %v", err)
	}
	rec2, err := RecoverChunkFromDirs([]string{rescueDir}, cid)
	if err != nil {
		t.Fatalf("RecoverChunkFromDirs flat rescue: %v", err)
	}
	if !bytes.Equal(rec2, payload) {
		t.Fatalf("flat rescue recovery mismatch")
	}

	// 3. Stale temp file cleanup removes only old ".sNNN.tmp-*" files.
	shardDir := filepath.Dir(erasureShardPath(paths[0], cid, 0))
	staleTmp := filepath.Join(shardDir, erasureShardFileName(cid, 0)+".tmp-12345")
	freshTmp := filepath.Join(shardDir, erasureShardFileName(cid, 0)+".tmp-99999")
	if err := os.WriteFile(staleTmp, []byte("old"), 0o644); err != nil {
		t.Fatalf("write staleTmp: %v", err)
	}
	if err := os.WriteFile(freshTmp, []byte("new"), 0o644); err != nil {
		t.Fatalf("write freshTmp: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(staleTmp, oldTime, oldTime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	removed, err := es.RemoveStaleTempFiles(time.Now().Add(-30 * time.Minute))
	if err != nil || removed != 1 {
		t.Fatalf("RemoveStaleTempFiles removed=%d err=%v, want 1/nil", removed, err)
	}
	if _, err := os.Stat(staleTmp); !os.IsNotExist(err) {
		t.Fatalf("staleTmp still exists")
	}
	if _, err := os.Stat(freshTmp); err != nil {
		t.Fatalf("freshTmp was deleted unexpectedly: %v", err)
	}

	// 4. DeleteBlob removes shards from all 6 roots.
	if err := es.DeleteBlob(cid); err != nil {
		t.Fatalf("DeleteBlob: %v", err)
	}
	for i, root := range paths {
		if _, err := os.Stat(erasureShardPath(root, cid, i)); !os.IsNotExist(err) {
			t.Fatalf("shard %d still exists after DeleteBlob", i)
		}
	}
}
