package metadata

import (
	"errors"
	"fs/models"
	"testing"
)

func versionedObject(bucket, key, etag string, chunks ...string) *models.ObjectManifest {
	return &models.ObjectManifest{
		Bucket:    bucket,
		Key:       key,
		Size:      10,
		ETag:      etag,
		Chunks:    chunks,
		CreatedAt: 1,
	}
}

func mustPutVersion(t *testing.T, h *MetadataHandler, manifest *models.ObjectManifest) string {
	t.Helper()
	versionID, err := h.PutObjectVersion(manifest)
	if err != nil {
		t.Fatalf("PutObjectVersion(%q/%q): %v", manifest.Bucket, manifest.Key, err)
	}
	return versionID
}

func mustListVersions(t *testing.T, h *MetadataHandler, bucket string, maxKeys int) *ObjectVersionsPage {
	t.Helper()
	page, err := h.ListObjectVersions(bucket, "", "", "", "", maxKeys)
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	return page
}

func TestVersioningEnabledLifecycle(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	v1 := mustPutVersion(t, h, versionedObject("bucket", "k", "etag-1", "chunk-1"))
	v2 := mustPutVersion(t, h, versionedObject("bucket", "k", "etag-2", "chunk-2"))
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids = %q, %q", v1, v2)
	}

	latest, err := h.GetManifest("bucket", "k")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if latest.ETag != "etag-2" || latest.VersionID != v2 {
		t.Fatalf("latest = %+v", latest)
	}

	old, err := h.GetManifestVersion("bucket", "k", v1)
	if err != nil {
		t.Fatalf("GetManifestVersion: %v", err)
	}
	if old.ETag != "etag-1" || old.VersionID != v1 {
		t.Fatalf("old = %+v", old)
	}

	page := mustListVersions(t, h, "bucket", 100)
	if len(page.Versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(page.Versions))
	}
	if page.Versions[0].Manifest.VersionID != v2 || !page.Versions[0].IsLatest {
		t.Fatalf("first entry = %+v", page.Versions[0])
	}
	if page.Versions[1].Manifest.VersionID != v1 || page.Versions[1].IsLatest {
		t.Fatalf("second entry = %+v", page.Versions[1])
	}

	deleted, err := h.DeleteObject("bucket", "k", "")
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if !deleted.DeleteMarker || deleted.VersionID == "" {
		t.Fatalf("delete result = %+v", deleted)
	}
	marker, err := h.GetManifest("bucket", "k")
	if err != nil {
		t.Fatalf("GetManifest after marker: %v", err)
	}
	if !marker.DeleteMarker {
		t.Fatalf("latest is not a delete marker: %+v", marker)
	}
	// The previous version remains addressable while the marker is latest.
	if _, err := h.GetManifestVersion("bucket", "k", v2); err != nil {
		t.Fatalf("GetManifestVersion under marker: %v", err)
	}

	if _, err := h.DeleteObject("bucket", "k", deleted.VersionID); err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	restored, err := h.GetManifest("bucket", "k")
	if err != nil {
		t.Fatalf("GetManifest after marker removal: %v", err)
	}
	if restored.ETag != "etag-2" || restored.DeleteMarker {
		t.Fatalf("restored = %+v", restored)
	}

	if _, err := h.DeleteObject("bucket", "k", v1); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	if _, err := h.DeleteObject("bucket", "k", v2); err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	if _, err := h.GetManifest("bucket", "k"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("GetManifest after deleting all versions = %v, want ErrObjectNotFound", err)
	}
}

func TestVersioningSuspendedReplacesNull(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningSuspended); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	v1 := mustPutVersion(t, h, versionedObject("bucket", "k", "etag-1"))
	v2 := mustPutVersion(t, h, versionedObject("bucket", "k", "etag-2"))
	if v1 != nullVersionID || v2 != nullVersionID {
		t.Fatalf("version ids = %q, %q, want null", v1, v2)
	}
	if page := mustListVersions(t, h, "bucket", 100); len(page.Versions) != 1 {
		t.Fatalf("suspended versions = %d, want 1", len(page.Versions))
	}

	if _, err := h.DeleteObject("bucket", "k", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if page := mustListVersions(t, h, "bucket", 100); len(page.Versions) != 1 || !page.Versions[0].Manifest.DeleteMarker {
		t.Fatalf("suspended marker page = %+v", page.Versions)
	}
	if _, err := h.DeleteObject("bucket", "k", ""); err != nil {
		t.Fatalf("second DeleteObject: %v", err)
	}
	if page := mustListVersions(t, h, "bucket", 100); len(page.Versions) != 1 || !page.Versions[0].Manifest.DeleteMarker {
		t.Fatalf("repeated delete grew versions: %+v", page.Versions)
	}
}

func TestVersioningMaterializesLegacyObject(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	mustPutManifest(t, h, versionedObject("bucket", "k", "legacy-etag", "legacy-chunk"))

	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	v2 := mustPutVersion(t, h, versionedObject("bucket", "k", "new-etag", "new-chunk"))

	page := mustListVersions(t, h, "bucket", 100)
	if len(page.Versions) != 2 {
		t.Fatalf("versions = %d, want 2", len(page.Versions))
	}
	if page.Versions[0].Manifest.VersionID != v2 || !page.Versions[0].IsLatest {
		t.Fatalf("newest = %+v", page.Versions[0])
	}
	if page.Versions[1].Manifest.VersionID != nullVersionID || page.Versions[1].IsLatest {
		t.Fatalf("null version = %+v", page.Versions[1])
	}
	legacy, err := h.GetManifestVersion("bucket", "k", nullVersionID)
	if err != nil {
		t.Fatalf("GetManifestVersion(null): %v", err)
	}
	if legacy.ETag != "legacy-etag" {
		t.Fatalf("legacy etag = %q", legacy.ETag)
	}

	if err := h.PutManifest(versionedObject("bucket", "k", "bypass", "chunk")); err == nil {
		t.Fatal("PutManifest succeeded on a versioned bucket")
	}
}

func TestListObjectVersionsPagination(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	want := make([]string, 0, 6)
	for _, key := range []string{"a", "b", "c"} {
		for i := 0; i < 2; i++ {
			want = append(want, key+"/"+mustPutVersion(t, h, versionedObject("bucket", key, "etag", "chunk")))
		}
	}

	var got []string
	keyMarker, versionMarker := "", ""
	for page := 0; page < 10; page++ {
		result, err := h.ListObjectVersions("bucket", "", "", keyMarker, versionMarker, 2)
		if err != nil {
			t.Fatalf("ListObjectVersions page %d: %v", page, err)
		}
		for _, version := range result.Versions {
			got = append(got, version.Manifest.Key+"/"+version.Manifest.VersionID)
		}
		if !result.IsTruncated {
			break
		}
		keyMarker, versionMarker = result.NextKeyMarker, result.NextVersionIDMarker
	}
	if len(got) != len(want) {
		t.Fatalf("got %d versions, want %d: %v", len(got), len(want), got)
	}
	for _, entry := range want {
		found := false
		for _, value := range got {
			if value == entry {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing version %q in %v", entry, got)
		}
	}
}

func TestListObjectVersionsDelimiter(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	mustPutVersion(t, h, versionedObject("bucket", "a/1", "e"))
	mustPutVersion(t, h, versionedObject("bucket", "a/2", "e"))
	mustPutVersion(t, h, versionedObject("bucket", "b", "e"))

	page, err := h.ListObjectVersions("bucket", "", "/", "", "", 100)
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(page.CommonPrefixes) != 1 || page.CommonPrefixes[0] != "a/" {
		t.Fatalf("common prefixes = %v", page.CommonPrefixes)
	}
	if len(page.Versions) != 1 || page.Versions[0].Manifest.Key != "b" {
		t.Fatalf("versions = %+v", page.Versions)
	}
}

func TestReferencedChunksIncludeAllVersions(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	mustPutVersion(t, h, versionedObject("bucket", "k", "e1", "chunk-old"))
	mustPutVersion(t, h, versionedObject("bucket", "k", "e2", "chunk-new"))
	if _, err := h.DeleteObject("bucket", "k", ""); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	chunks, err := h.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet: %v", err)
	}
	for _, chunk := range []string{"chunk-old", "chunk-new"} {
		if _, ok := chunks[chunk]; !ok {
			t.Fatalf("chunk %q not referenced by any version", chunk)
		}
	}
}

func TestDeleteBucketRequiresAllVersionsRemoved(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	versionID := mustPutVersion(t, h, versionedObject("bucket", "k", "e", "chunk"))

	if err := h.DeleteBucket("bucket"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with versions = %v, want ErrBucketNotEmpty", err)
	}
	if _, err := h.DeleteObject("bucket", "k", versionID); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if err := h.DeleteBucket("bucket"); err != nil {
		t.Fatalf("DeleteBucket after removing versions: %v", err)
	}
}

func TestCompleteMultipartUploadCreatesVersion(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "bucket")
	if err := h.SetBucketVersioning("bucket", versioningEnabled); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	upload, err := h.CreateMultipartUpload("bucket", "multipart-key")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	final := &models.ObjectManifest{Size: 3, ETag: "etag", Chunks: []string{"chunk"}}
	if err := h.CompleteMultipartUpload(upload.UploadID, final); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	latest, err := h.GetManifest("bucket", "multipart-key")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if latest.VersionID == "" || latest.VersionID == nullVersionID {
		t.Fatalf("multipart version id = %q, want a unique id", latest.VersionID)
	}
}
