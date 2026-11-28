package service

import (
	"bytes"
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestObjectServiceWithErasureStoreEndToEnd(t *testing.T) {
	base := t.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(base, "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}

	paths := make([]string, 6)
	for i := range paths {
		paths[i] = filepath.Join(base, fmt.Sprintf("disk-%d", i))
	}
	es, err := storage.NewErasureStore(storage.ErasureConfig{
		Paths:        paths,
		DataShards:   4,
		ParityShards: 2,
		ChunkSize:    1 << 20,
		AutoHeal:     true,
		StrictWrite:  false,
	})
	if err != nil {
		_ = md.Close()
		t.Fatalf("NewErasureStore: %v", err)
	}

	svc := NewObjectService(md, es, time.Hour, 0)
	svc.SetGCGrace(time.Nanosecond)
	t.Cleanup(func() {
		_ = svc.Close()
	})

	if err := svc.CreateBucket("erasure-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// 1. Put multi-chunk object, delete M=2 disks' worth of shards on chunk 0,
	// verify GetObject & GetObjectRange both succeed and auto-heal.
	data := bytes.Repeat([]byte("erasure-service-roundtrip-0123456789-"), 70000)[:2500000]
	manifest, err := svc.PutObject("erasure-bucket", "multi.bin", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if len(manifest.Chunks) < 2 {
		t.Fatalf("expected >= 2 chunks, got %d", len(manifest.Chunks))
	}

	c0 := manifest.Chunks[0]
	for _, slot := range []int{0, 4} {
		p := filepath.Join(paths[slot], "blobs", c0[:2], c0[2:4], fmt.Sprintf("%s.s%03d", c0, slot))
		if err := os.Remove(p); err != nil {
			t.Fatalf("remove shard %d: %v", slot, err)
		}
	}

	rc, _, err := svc.GetObject("erasure-bucket", "multi.bin")
	if err != nil {
		t.Fatalf("GetObject degraded: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("GetObject degraded mismatch: err=%v len=%d", err, len(got))
	}

	// Confirm auto-heal restored both deleted shards.
	health, err := es.InspectChunk(c0)
	if err != nil || health.Status != "healthy" {
		t.Fatalf("chunk health after auto-heal = %+v, err=%v", health, err)
	}

	// Range read across chunk boundaries.
	rrc, _, err := svc.GetObjectRange("erasure-bucket", "multi.bin", (1<<20)-512, 2048)
	if err != nil {
		t.Fatalf("GetObjectRange: %v", err)
	}
	rangeGot, err := io.ReadAll(rrc)
	_ = rrc.Close()
	if err != nil || !bytes.Equal(rangeGot, data[(1<<20)-512:(1<<20)+1536]) {
		t.Fatalf("GetObjectRange mismatch: err=%v len=%d", err, len(rangeGot))
	}

	// 2. Multipart upload + Corrupt shard on part 2 + GetObject + Delete + GC cleanup across all roots.
	up, err := svc.CreateMultipartUpload("erasure-bucket", "mpu.bin", "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	part1 := bytes.Repeat([]byte("PART1---"), (5<<20)/8+1)[:5<<20]
	part2 := bytes.Repeat([]byte("PART2-"), 110)[:600]
	etag1, err := svc.UploadPart("erasure-bucket", "mpu.bin", up.UploadID, 1, bytes.NewReader(part1))
	if err != nil {
		t.Fatalf("UploadPart 1: %v", err)
	}
	etag2, err := svc.UploadPart("erasure-bucket", "mpu.bin", up.UploadID, 2, bytes.NewReader(part2))
	if err != nil {
		t.Fatalf("UploadPart 2: %v", err)
	}
	mpuManifest, err := svc.CompleteMultipartUpload("erasure-bucket", "mpu.bin", up.UploadID, []models.CompletedPart{
		{PartNumber: 1, ETag: etag1},
		{PartNumber: 2, ETag: etag2},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	cLast := mpuManifest.Chunks[len(mpuManifest.Chunks)-1]
	corruptPath := filepath.Join(paths[2], "blobs", cLast[:2], cLast[2:4], fmt.Sprintf("%s.s%03d", cLast, 2))
	if err := os.WriteFile(corruptPath, []byte("corrupted-shard"), 0o644); err != nil {
		t.Fatalf("corrupt shard: %v", err)
	}

	mrc, _, err := svc.GetObject("erasure-bucket", "mpu.bin")
	if err != nil {
		t.Fatalf("GetObject mpu degraded: %v", err)
	}
	mGot, err := io.ReadAll(mrc)
	_ = mrc.Close()
	wantMPU := append(append([]byte(nil), part1...), part2...)
	if err != nil || !bytes.Equal(mGot, wantMPU) {
		t.Fatalf("GetObject mpu mismatch: err=%v len=%d", err, len(mGot))
	}

	// Delete both objects and run GC -> all shards across all 6 roots must be reclaimed.
	if err := svc.DeleteObject("erasure-bucket", "multi.bin"); err != nil {
		t.Fatalf("DeleteObject multi.bin: %v", err)
	}
	if err := svc.DeleteObject("erasure-bucket", "mpu.bin"); err != nil {
		t.Fatalf("DeleteObject mpu.bin: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := svc.GarbageCollect(); err != nil {
		t.Fatalf("GarbageCollect: %v", err)
	}
	remaining, err := es.ListChunks()
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("remaining erasure chunks after GC = %v, want empty", remaining)
	}
}
