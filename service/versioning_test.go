package service

import (
	"errors"
	"io"
	"strings"
	"testing"

	"fs/models"
)

func mustPutObject(t *testing.T, svc *ObjectService, bucket, key, body string) string {
	t.Helper()
	manifest, err := svc.PutObject(bucket, key, "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatalf("PutObject(%q/%q): %v", bucket, key, err)
	}
	return manifest.VersionID
}

func mustReadAll(t *testing.T, stream io.ReadCloser) string {
	t.Helper()
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return string(data)
}

func TestVersionedGetAndDeleteMarker(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := svc.SetBucketVersioning("bucket", "Enabled"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	v1 := mustPutObject(t, svc, "bucket", "k", "one")
	v2 := mustPutObject(t, svc, "bucket", "k", "two")
	if v1 == "" || v2 == "" || v1 == v2 {
		t.Fatalf("version ids = %q, %q", v1, v2)
	}

	stream, manifest, err := svc.GetObject("bucket", "k")
	if err != nil {
		t.Fatalf("GetObject latest: %v", err)
	}
	if body := mustReadAll(t, stream); body != "two" || manifest.VersionID != v2 {
		t.Fatalf("latest = %q version %q", body, manifest.VersionID)
	}

	stream, manifest, err = svc.GetObjectVersion("bucket", "k", v1)
	if err != nil {
		t.Fatalf("GetObjectVersion: %v", err)
	}
	if body := mustReadAll(t, stream); body != "one" || manifest.VersionID != v1 {
		t.Fatalf("v1 = %q version %q", body, manifest.VersionID)
	}

	stream, _, err = svc.GetObjectRangeVersion("bucket", "k", v1, 1, 1)
	if err != nil {
		t.Fatalf("GetObjectRangeVersion: %v", err)
	}
	if body := mustReadAll(t, stream); body != "n" {
		t.Fatalf("range = %q, want n", body)
	}

	deleted, err := svc.DeleteObjectVersion("bucket", "k", "")
	if err != nil {
		t.Fatalf("DeleteObjectVersion: %v", err)
	}
	if !deleted.DeleteMarker || deleted.VersionID == "" {
		t.Fatalf("delete result = %+v", deleted)
	}
	if _, _, err := svc.GetObject("bucket", "k"); !errors.Is(err, ErrDeleteMarker) {
		t.Fatalf("GetObject under marker = %v, want ErrDeleteMarker", err)
	}
	stream, _, err = svc.GetObjectVersion("bucket", "k", v1)
	if err != nil {
		t.Fatalf("GetObjectVersion under marker: %v", err)
	}
	if body := mustReadAll(t, stream); body != "one" {
		t.Fatalf("v1 under marker = %q", body)
	}

	if _, err := svc.DeleteObjectVersion("bucket", "k", deleted.VersionID); err != nil {
		t.Fatalf("delete marker: %v", err)
	}
	stream, _, err = svc.GetObject("bucket", "k")
	if err != nil {
		t.Fatalf("GetObject after marker removal: %v", err)
	}
	if body := mustReadAll(t, stream); body != "two" {
		t.Fatalf("restored latest = %q", body)
	}
}

func TestCopyObjectFromExplicitVersion(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := svc.SetBucketVersioning("bucket", "Enabled"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	v1 := mustPutObject(t, svc, "bucket", "k", "original")
	mustPutObject(t, svc, "bucket", "k", "replacement")

	copied, err := svc.CopyObjectVersion("bucket", "k", v1, "bucket", "copy")
	if err != nil {
		t.Fatalf("CopyObjectVersion: %v", err)
	}
	if copied.VersionID == "" {
		t.Fatal("copy has no version id")
	}
	stream, _, err := svc.GetObject("bucket", "copy")
	if err != nil {
		t.Fatalf("GetObject copy: %v", err)
	}
	if body := mustReadAll(t, stream); body != "original" {
		t.Fatalf("copied body = %q, want original", body)
	}
}

func TestSuspendedVersioningUsesNull(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := svc.SetBucketVersioning("bucket", "Suspended"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}

	if versionID := mustPutObject(t, svc, "bucket", "k", "first"); versionID != "null" {
		t.Fatalf("suspended version id = %q, want null", versionID)
	}
	if versionID := mustPutObject(t, svc, "bucket", "k", "second"); versionID != "null" {
		t.Fatalf("suspended version id = %q, want null", versionID)
	}

	stream, _, err := svc.GetObject("bucket", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if body := mustReadAll(t, stream); body != "second" {
		t.Fatalf("latest = %q, want second", body)
	}
}

func TestCompleteMultipartReturnsVersion(t *testing.T) {
	svc := newTestObjectService(t, 0)
	if err := svc.CreateBucket("bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := svc.SetBucketVersioning("bucket", "Enabled"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	upload, err := svc.CreateMultipartUpload("bucket", "multipart")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	etag, err := svc.UploadPart("bucket", "multipart", upload.UploadID, 1, strings.NewReader("part-data"))
	if err != nil {
		t.Fatalf("UploadPart: %v", err)
	}
	manifest, err := svc.CompleteMultipartUpload("bucket", "multipart", upload.UploadID, []models.CompletedPart{{PartNumber: 1, ETag: etag}})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	if manifest.VersionID == "" || manifest.VersionID == "null" {
		t.Fatalf("multipart version id = %q", manifest.VersionID)
	}
}
