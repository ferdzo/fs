package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAdminErasureCLIStatusHealAndRecover(t *testing.T) {
	base := t.TempDir()
	paths := make([]string, 6) // 4+2
	for i := range paths {
		paths[i] = filepath.Join(base, fmt.Sprintf("disk-%d", i))
	}
	pathsCSV := strings.Join(paths, ",")

	es, err := storage.NewErasureStore(storage.ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    512,
		AutoHeal:     false,
	})
	if err != nil {
		t.Fatalf("NewErasureStore: %v", err)
	}

	payload := bytes.Repeat([]byte("cli-erasure-recovery-payload-"), 40)[:1100]
	ids, sizes, total, etag, err := es.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("IngestStreamWithSizes: %v", err)
	}

	dbPath := filepath.Join(base, "metadata.db")
	mh, err := metadata.NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	if err := mh.CreateBucket("bucket-1", time.Now().UTC()); err != nil {
		_ = mh.Close()
		t.Fatalf("CreateBucket: %v", err)
	}
	manifest := &models.ObjectManifest{
		Bucket:      "bucket-1",
		Key:         "dir/obj.bin",
		Size:        total,
		ETag:        etag,
		ContentType: "application/octet-stream",
		Chunks:      ids,
		ChunkSizes:  sizes,
		CreatedAt:   time.Now().UTC().Unix(),
	}
	if err := mh.PutManifest(manifest, time.Now().UTC()); err != nil {
		_ = mh.Close()
		t.Fatalf("PutManifest: %v", err)
	}
	if err := mh.Close(); err != nil {
		t.Fatalf("Close metadata: %v", err)
	}

	// Simulate losing disk 1 and corrupting disk 5 on chunk 0.
	c0 := ids[0]
	_ = os.RemoveAll(paths[1])
	p5 := filepath.Join(paths[5], "blobs", c0[:2], c0[2:4], fmt.Sprintf("%s.s%03d", c0, 5))
	if err := os.WriteFile(p5, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt slot 5: %v", err)
	}

	// 1. status reports degraded (and exits 0 because all chunks remain recoverable).
	var statusOut bytes.Buffer
	statusCmd := newRootCommand(BuildInfo{Version: "test"})
	statusCmd.SetOut(&statusOut)
	statusCmd.SetErr(&statusOut)
	statusCmd.SetArgs([]string{
		"admin", "erasure", "status",
		"--json",
		"--paths", pathsCSV,
		"--data-shards", "4",
		"--parity-shards", "2",
		"--metadata-db", dbPath,
	})
	if err := statusCmd.Execute(); err != nil {
		t.Fatalf("erasure status failed: %v\nout=%s", err, statusOut.String())
	}
	var report storage.ScrubReport
	if err := json.Unmarshal(statusOut.Bytes(), &report); err != nil {
		t.Fatalf("decode status json: %v\nraw=%s", err, statusOut.String())
	}
	if report.DegradedChunks != len(ids) || report.UnrecoverableCount != 0 || report.MissingCount != 0 {
		t.Fatalf("unexpected scrub report: %+v", report)
	}

	// 2. recover full object using scrambled subset of 4 surviving paths (without slot 1 and without slot 5!).
	recoveredObj := filepath.Join(base, "recovered.bin")
	recoverCmd := newRootCommand(BuildInfo{Version: "test"})
	var recoverOut bytes.Buffer
	recoverCmd.SetOut(&recoverOut)
	recoverCmd.SetErr(&recoverOut)
	recoverCmd.SetArgs([]string{
		"admin", "erasure", "recover",
		"--paths", strings.Join([]string{paths[4], paths[2], paths[0], paths[3]}, ","),
		"--metadata-db", dbPath,
		"--bucket", "bucket-1",
		"--key", "dir/obj.bin",
		"--out", recoveredObj,
	})
	if err := recoverCmd.Execute(); err != nil {
		t.Fatalf("erasure recover object failed: %v\nout=%s", err, recoverOut.String())
	}
	gotObj, err := os.ReadFile(recoveredObj)
	if err != nil {
		t.Fatalf("ReadFile recoveredObj: %v", err)
	}
	if !bytes.Equal(gotObj, payload) {
		t.Fatalf("recovered object mismatch: len=%d want=%d", len(gotObj), len(payload))
	}

	// 3. recover single chunk works too.
	recoveredChunk := filepath.Join(base, "chunk0.bin")
	recoverChunkCmd := newRootCommand(BuildInfo{Version: "test"})
	recoverChunkCmd.SetArgs([]string{
		"admin", "erasure", "recover",
		"--paths", strings.Join([]string{paths[3], paths[4], paths[0], paths[2]}, ","),
		"--chunk", c0,
		"--out", recoveredChunk,
	})
	if err := recoverChunkCmd.Execute(); err != nil {
		t.Fatalf("erasure recover chunk failed: %v", err)
	}
	gotChunk, err := os.ReadFile(recoveredChunk)
	if err != nil || !bytes.Equal(gotChunk, payload[:sizes[0]]) {
		t.Fatalf("recovered chunk mismatch: err=%v len=%d want=%d", err, len(gotChunk), sizes[0])
	}

	// 4. heal restores blank replacement disk-1 and rewrites corrupt shard on disk-5.
	if err := os.MkdirAll(paths[1], 0o755); err != nil {
		t.Fatalf("recreate disk-1: %v", err)
	}
	var healOut bytes.Buffer
	healCmd := newRootCommand(BuildInfo{Version: "test"})
	healCmd.SetOut(&healOut)
	healCmd.SetErr(&healOut)
	healCmd.SetArgs([]string{
		"admin", "erasure", "heal",
		"--json",
		"--paths", pathsCSV,
		"--data-shards", "4",
		"--parity-shards", "2",
		"--metadata-db", dbPath,
	})
	if err := healCmd.Execute(); err != nil {
		t.Fatalf("erasure heal failed: %v\nout=%s", err, healOut.String())
	}
	var healReport storage.ScrubReport
	if err := json.Unmarshal(healOut.Bytes(), &healReport); err != nil {
		t.Fatalf("decode heal json: %v", err)
	}
	if healReport.HealedChunks != len(ids) || healReport.HealthyChunks != len(ids) || healReport.DegradedChunks != 0 {
		t.Fatalf("unexpected heal report: %+v", healReport)
	}
}
