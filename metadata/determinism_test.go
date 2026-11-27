package metadata

import (
	"bytes"
	"encoding/binary"
	"fs/models"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func replayDeterministicScript(t *testing.T, h *MetadataHandler) {
	t.Helper()

	base := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)

	// Buckets & unversioned lifecycle.
	if err := h.CreateBucket("unver", base); err != nil {
		t.Fatalf("CreateBucket(unver): %v", err)
	}
	if err := h.CreateBucket("ver", base.Add(time.Second)); err != nil {
		t.Fatalf("CreateBucket(ver): %v", err)
	}
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k1", Size: 5, ETag: "e1", Chunks: []string{"c1", "c2"}, CreatedAt: base.Unix(),
	}, base.Add(2*time.Second)); err != nil {
		t.Fatalf("PutManifest(k1): %v", err)
	}
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k1", Size: 6, ETag: "e2", Chunks: []string{"c2", "c3"}, CreatedAt: base.Add(3 * time.Second).Unix(),
	}, base.Add(3*time.Second)); err != nil {
		t.Fatalf("PutManifest overwrite(k1): %v", err)
	}
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k2", Size: 3, ETag: "e3", Chunks: []string{"c4"}, CreatedAt: base.Add(4 * time.Second).Unix(),
	}, base.Add(4*time.Second)); err != nil {
		t.Fatalf("PutManifest(k2): %v", err)
	}
	if err := h.PutManifest(&models.ObjectManifest{
		Bucket: "unver", Key: "k3", Size: 4, ETag: "e4", Chunks: []string{"c5"}, CreatedAt: base.Add(5 * time.Second).Unix(),
	}, base.Add(5*time.Second)); err != nil {
		t.Fatalf("PutManifest(k3): %v", err)
	}
	if err := h.DeleteManifest("unver", "k2", base.Add(6*time.Second)); err != nil {
		t.Fatalf("DeleteManifest(k2): %v", err)
	}
	if _, err := h.DeleteManifests("unver", []string{"k3", "missing"}, base.Add(7*time.Second)); err != nil {
		t.Fatalf("DeleteManifests: %v", err)
	}

	// Versioned -> Suspended lifecycle.
	if err := h.SetBucketVersioning("ver", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning(Enabled): %v", err)
	}
	if _, err := h.PutObjectVersionConditional(&models.ObjectManifest{
		Bucket: "ver", Key: "doc", Size: 10, ETag: "ve1", Chunks: []string{"vc1"}, CreatedAt: base.Add(8 * time.Second).Unix(),
	}, "", "v-1", base.Add(8*time.Second)); err != nil {
		t.Fatalf("PutObjectVersionConditional(v-1): %v", err)
	}
	if _, err := h.PutObjectVersionConditional(&models.ObjectManifest{
		Bucket: "ver", Key: "doc", Size: 12, ETag: "ve2", Chunks: []string{"vc2"}, CreatedAt: base.Add(9 * time.Second).Unix(),
	}, "", "v-2", base.Add(9*time.Second)); err != nil {
		t.Fatalf("PutObjectVersionConditional(v-2): %v", err)
	}
	if _, err := h.DeleteObject("ver", "doc", "", "dm-1", base.Add(10*time.Second)); err != nil {
		t.Fatalf("DeleteObject(dm-1): %v", err)
	}
	if _, err := h.DeleteObject("ver", "doc", "v-1", "", base.Add(11*time.Second)); err != nil {
		t.Fatalf("DeleteObject(v-1): %v", err)
	}
	if _, err := h.PutObjectVersionConditional(&models.ObjectManifest{
		Bucket: "ver", Key: "batch-a", Size: 4, ETag: "va", Chunks: []string{"vca"}, CreatedAt: base.Add(12 * time.Second).Unix(),
	}, "", "v-a1", base.Add(12*time.Second)); err != nil {
		t.Fatalf("PutObjectVersionConditional(batch-a): %v", err)
	}
	if _, err := h.DeleteObjects("ver", []DeleteObjectSpec{
		{Key: "batch-a", DeleteMarkerID: "dm-batch-1"},
		{Key: "doc", VersionID: "dm-1"},
	}, base.Add(13*time.Second)); err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if err := h.SetBucketVersioning("ver", versioningSuspended); err != nil {
		t.Fatalf("SetBucketVersioning(Suspended): %v", err)
	}
	if _, err := h.PutObjectVersion(&models.ObjectManifest{
		Bucket: "ver", Key: "susp", Size: 2, ETag: "se1", Chunks: []string{"sc1"}, CreatedAt: base.Add(14 * time.Second).Unix(),
	}, "", base.Add(14*time.Second)); err != nil {
		t.Fatalf("PutObjectVersion(suspended): %v", err)
	}

	// Multipart create / part-put / overwrite / complete / abort / cleanup.
	recent := base.Add(90 * time.Minute)
	if _, err := h.CreateMultipartUpload("unver", "mp-done", "up-done", "text/plain", recent); err != nil {
		t.Fatalf("CreateMultipartUpload(up-done): %v", err)
	}
	if err := h.PutMultipartPart("up-done", models.UploadedPart{
		PartNumber: 1, ETag: "pe1", Size: 5, Chunks: []string{"mpc-old"}, CreatedAt: recent.Add(time.Second).Unix(),
	}, recent.Add(time.Second)); err != nil {
		t.Fatalf("PutMultipartPart(old): %v", err)
	}
	if err := h.PutMultipartPart("up-done", models.UploadedPart{
		PartNumber: 1, ETag: "pe2", Size: 6, Chunks: []string{"mpc-new"}, CreatedAt: recent.Add(2 * time.Second).Unix(),
	}, recent.Add(2*time.Second)); err != nil {
		t.Fatalf("PutMultipartPart(overwrite): %v", err)
	}
	if err := h.CompleteMultipartUpload("up-done", &models.ObjectManifest{
		Size: 6, ContentType: "text/plain", ETag: "pe2-1", Chunks: []string{"mpc-new"}, CreatedAt: recent.Add(3 * time.Second).Unix(),
	}, "", recent.Add(3*time.Second)); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	if _, err := h.CreateMultipartUpload("unver", "mp-abort", "up-abort", "application/octet-stream", recent.Add(4*time.Second)); err != nil {
		t.Fatalf("CreateMultipartUpload(up-abort): %v", err)
	}
	if err := h.PutMultipartPart("up-abort", models.UploadedPart{
		PartNumber: 1, ETag: "pa1", Size: 4, Chunks: []string{"mpc-abort"}, CreatedAt: recent.Add(5 * time.Second).Unix(),
	}, recent.Add(5*time.Second)); err != nil {
		t.Fatalf("PutMultipartPart(abort): %v", err)
	}
	if err := h.AbortMultipartUpload("up-abort", recent.Add(6*time.Second)); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	if _, err := h.CreateMultipartUpload("unver", "mp-exp", "up-exp", "application/octet-stream", base); err != nil {
		t.Fatalf("CreateMultipartUpload(up-exp): %v", err)
	}
	if err := h.PutMultipartPart("up-exp", models.UploadedPart{
		PartNumber: 1, ETag: "px1", Size: 4, Chunks: []string{"mpc-exp"}, CreatedAt: base.Unix(),
	}, base.Add(22*time.Second)); err != nil {
		t.Fatalf("PutMultipartPart(exp): %v", err)
	}
	if cleaned, err := h.CleanupMultipartUploads(time.Hour, base.Add(2*time.Hour)); err != nil || cleaned != 1 {
		t.Fatalf("CleanupMultipartUploads = (%d, %v), want (1, nil)", cleaned, err)
	}

	// GC queue explicit enqueue/remove.
	if err := h.EnqueueUnreferencedChunks([]string{"orphan-1", "orphan-2"}, base.Add(2*time.Hour+time.Second)); err != nil {
		t.Fatalf("EnqueueUnreferencedChunks: %v", err)
	}
	if err := h.RemoveGCQueueEntries([]string{"orphan-1"}); err != nil {
		t.Fatalf("RemoveGCQueueEntries: %v", err)
	}

	// Auth user create/upsert/update-policy/delete.
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{{Effect: "allow", Actions: []string{"s3:*"}, Bucket: "unver"}},
	}
	if err := h.CreateAuthUser(&models.AuthIdentity{
		AccessKeyID: "AKIA1", SecretEnc: "enc1", Status: "active", CreatedAt: 100, UpdatedAt: 100,
	}, policy); err != nil {
		t.Fatalf("CreateAuthUser: %v", err)
	}
	if err := h.UpsertAuthUser(&models.AuthIdentity{
		AccessKeyID: "AKIA2", SecretEnc: "enc2", Status: "active", CreatedAt: 101, UpdatedAt: 101,
	}, policy); err != nil {
		t.Fatalf("UpsertAuthUser: %v", err)
	}
	if _, err := h.UpdateAuthUserPolicy("AKIA2", &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{{Effect: "deny", Actions: []string{"s3:DeleteObject"}, Bucket: "unver"}},
	}, 200); err != nil {
		t.Fatalf("UpdateAuthUserPolicy: %v", err)
	}
	if err := h.DeleteAuthUser("AKIA1"); err != nil {
		t.Fatalf("DeleteAuthUser: %v", err)
	}
}

func dumpBoltState(t *testing.T, db *bolt.DB) map[string][][2][]byte {
	t.Helper()
	out := make(map[string][][2][]byte)
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucketName := string(name)
			var entries [][2][]byte
			if err := b.ForEach(func(k, v []byte) error {
				kc := append([]byte(nil), k...)
				vc := append([]byte(nil), v...)
				entries = append(entries, [2][]byte{kc, vc})
				return nil
			}); err != nil {
				return err
			}
			out[bucketName] = entries
			return nil
		})
	})
	if err != nil {
		t.Fatalf("dumpBoltState: %v", err)
	}
	return out
}

func TestDeterministicReplayIdenticalDB(t *testing.T) {
	dir := t.TempDir()
	h1, err := NewMetadataHandler(filepath.Join(dir, "db1.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler 1: %v", err)
	}
	defer h1.Close()

	h2, err := NewMetadataHandler(filepath.Join(dir, "db2.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler 2: %v", err)
	}
	defer h2.Close()

	replayDeterministicScript(t, h1)
	replayDeterministicScript(t, h2)

	s1 := dumpBoltState(t, h1.db)
	s2 := dumpBoltState(t, h2.db)

	if len(s1) != len(s2) {
		t.Fatalf("bucket count mismatch: db1=%d db2=%d", len(s1), len(s2))
	}
	for bName, e1 := range s1 {
		e2, ok := s2[bName]
		if !ok {
			t.Fatalf("bucket %q present in db1 but missing in db2", bName)
		}
		if len(e1) != len(e2) {
			t.Fatalf("bucket %q entry count mismatch: db1=%d db2=%d", bName, len(e1), len(e2))
		}
		for i := range e1 {
			if !bytes.Equal(e1[i][0], e2[i][0]) || !bytes.Equal(e1[i][1], e2[i][1]) {
				t.Fatalf("bucket %q entry %d differs:\n  db1 key=%q val=%q\n  db2 key=%q val=%q",
					bName, i, e1[i][0], e1[i][1], e2[i][0], e2[i][1])
			}
		}
	}
}

func TestDeterministicIDValidation(t *testing.T) {
	h, _ := newTestStore(t)
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	mustCreateBucket(t, h, "ver-bucket")
	if err := h.SetBucketVersioning("ver-bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	if _, err := h.CreateMultipartUpload("ver-bucket", "k", "", "", now); err == nil {
		t.Fatal("CreateMultipartUpload with empty uploadID succeeded, want error")
	}

	for _, invalidID := range []string{"", "   ", nullVersionID} {
		if _, err := h.PutObjectVersionConditional(&models.ObjectManifest{
			Bucket: "ver-bucket", Key: "k", Size: 1, ETag: "e", Chunks: []string{"c"},
		}, "", invalidID, now); err == nil {
			t.Fatalf("PutObjectVersionConditional with versionID=%q succeeded, want error", invalidID)
		}
		if _, err := h.DeleteObject("ver-bucket", "k", "", invalidID, now); err == nil {
			t.Fatalf("DeleteObject with deleteMarkerID=%q succeeded, want error", invalidID)
		}
		if _, err := h.DeleteObjects("ver-bucket", []DeleteObjectSpec{{Key: "k", DeleteMarkerID: invalidID}}, now); err == nil {
			t.Fatalf("DeleteObjects with deleteMarkerID=%q succeeded, want error", invalidID)
		}
	}

	// Failed versionID/deleteMarkerID validation must not advance the bucket sequence counter.
	if err := h.db.View(func(tx *bolt.Tx) error {
		state := tx.Bucket(bucketStateIndex)
		if state == nil {
			return nil
		}
		if raw := state.Get([]byte("ver-bucket")); len(raw) == 8 {
			if seq := binary.BigEndian.Uint64(raw); seq != 0 {
				t.Fatalf("bucket sequence advanced to %d on invalid ID rejection", seq)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view bucketStateIndex: %v", err)
	}
}
