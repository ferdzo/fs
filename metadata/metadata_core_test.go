package metadata

import (
	"errors"
	"fs/models"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"
)

// newTestStore creates a MetadataHandler backed by a file in a temp dir and
// returns the handler plus the underlying db path so tests can reopen it.
func newTestStore(t *testing.T) (*MetadataHandler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.db")
	h, err := NewMetadataHandler(path)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Close()
	})
	return h, path
}

func mustCreateBucket(t *testing.T, h *MetadataHandler, bucket string) {
	t.Helper()
	if err := h.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket(%q): %v", bucket, err)
	}
}

func mustPutManifest(t *testing.T, h *MetadataHandler, manifest *models.ObjectManifest) {
	t.Helper()
	if err := h.PutManifest(manifest); err != nil {
		t.Fatalf("PutManifest(%q/%q): %v", manifest.Bucket, manifest.Key, err)
	}
}

func objectKeys(manifests []*models.ObjectManifest) []string {
	keys := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		keys = append(keys, manifest.Key)
	}
	return keys
}

func sortedStrings(values []string) []string {
	copied := slices.Clone(values)
	sort.Strings(copied)
	return copied
}

func TestIsValidBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		valid  bool
	}{
		{name: "simple", bucket: "bucket", valid: true},
		{name: "lowercase digits dots and dashes", bucket: "my-bucket.2026", valid: true},
		{name: "minimum length", bucket: "abc", valid: true},
		{name: "maximum length", bucket: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0", valid: true},
		{name: "too short", bucket: "ab", valid: false},
		{name: "empty", bucket: "", valid: false},
		{name: "uppercase", bucket: "MyBucket", valid: false},
		{name: "underscore", bucket: "my_bucket", valid: false},
		{name: "double dot", bucket: "my..bucket", valid: false},
		{name: "leading dot", bucket: ".bucket", valid: false},
		{name: "trailing dot", bucket: "bucket.", valid: false},
		{name: "leading dash", bucket: "-bucket", valid: false},
		{name: "trailing dash", bucket: "bucket-", valid: false},
		{name: "empty label", bucket: "bucket..name", valid: false},
		{name: "label starts with dash", bucket: "bucket.-name", valid: false},
		{name: "label ends with dash", bucket: "bucket-.name", valid: false},
		{name: "ipv4 address", bucket: "192.168.0.1", valid: false},
		{name: "too long", bucket: "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz01", valid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isValidBucketName(tt.bucket); got != tt.valid {
				t.Fatalf("isValidBucketName(%q) = %v, want %v", tt.bucket, got, tt.valid)
			}
		})
	}
}

func TestBucketLifecycle(t *testing.T) {
	h, _ := newTestStore(t)

	t.Run("list starts empty", func(t *testing.T) {
		buckets, err := h.ListBuckets()
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if len(buckets) != 0 {
			t.Fatalf("ListBuckets = %v, want empty", buckets)
		}
	})

	t.Run("create and get manifest", func(t *testing.T) {
		before := time.Now().Add(-time.Second)
		mustCreateBucket(t, h, "alpha-bucket")

		manifest, err := h.GetBucketManifest("alpha-bucket")
		if err != nil {
			t.Fatalf("GetBucketManifest: %v", err)
		}
		if manifest.Name != "alpha-bucket" {
			t.Fatalf("manifest.Name = %q, want alpha-bucket", manifest.Name)
		}
		if manifest.CreatedAt.IsZero() {
			t.Fatal("manifest.CreatedAt is zero")
		}
		if manifest.CreatedAt.Before(before) || manifest.CreatedAt.After(time.Now().Add(time.Second)) {
			t.Fatalf("manifest.CreatedAt = %v, want around now", manifest.CreatedAt)
		}
		if manifest.VersioningStatus != "" {
			t.Fatalf("manifest.VersioningStatus = %q, want empty", manifest.VersioningStatus)
		}
		if manifest.PublicAccessBlock {
			t.Fatal("manifest.PublicAccessBlock = true, want false")
		}
	})

	t.Run("duplicate create fails", func(t *testing.T) {
		err := h.CreateBucket("alpha-bucket")
		if !errors.Is(err, ErrBucketAlreadyExists) {
			t.Fatalf("CreateBucket duplicate error = %v, want ErrBucketAlreadyExists", err)
		}
	})

	t.Run("missing bucket errors", func(t *testing.T) {
		if _, err := h.GetBucketManifest("no-such-bucket"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("GetBucketManifest error = %v, want ErrBucketNotFound", err)
		}
		if _, err := h.GetManifest("no-such-bucket", "key"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("GetManifest error = %v, want ErrBucketNotFound", err)
		}
		if _, err := h.ListObjects("no-such-bucket", ""); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("ListObjects error = %v, want ErrBucketNotFound", err)
		}
		if _, err := h.CreateMultipartUpload("no-such-bucket", "key"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("CreateMultipartUpload error = %v, want ErrBucketNotFound", err)
		}
		if err := h.PutManifest(&models.ObjectManifest{Bucket: "no-such-bucket", Key: "key"}); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("PutManifest error = %v, want ErrBucketNotFound", err)
		}
	})

	t.Run("invalid names rejected", func(t *testing.T) {
		for _, bucket := range []string{"Bad_Bucket", "ab", ".dot"} {
			if err := h.CreateBucket(bucket); !errors.Is(err, ErrInvalidBucketName) {
				t.Fatalf("CreateBucket(%q) error = %v, want ErrInvalidBucketName", bucket, err)
			}
			if err := h.DeleteBucket(bucket); !errors.Is(err, ErrInvalidBucketName) {
				t.Fatalf("DeleteBucket(%q) error = %v, want ErrInvalidBucketName", bucket, err)
			}
		}
	})

	t.Run("list is sorted", func(t *testing.T) {
		mustCreateBucket(t, h, "zeta-bucket")
		mustCreateBucket(t, h, "mid-bucket")

		buckets, err := h.ListBuckets()
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		want := []string{"alpha-bucket", "mid-bucket", "zeta-bucket"}
		if !slices.Equal(buckets, want) {
			t.Fatalf("ListBuckets = %v, want %v", buckets, want)
		}
	})

	t.Run("delete empty bucket", func(t *testing.T) {
		mustCreateBucket(t, h, "doomed-bucket")
		if err := h.DeleteBucket("doomed-bucket"); err != nil {
			t.Fatalf("DeleteBucket: %v", err)
		}
		if _, err := h.GetBucketManifest("doomed-bucket"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("GetBucketManifest after delete error = %v, want ErrBucketNotFound", err)
		}
		buckets, err := h.ListBuckets()
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if slices.Contains(buckets, "doomed-bucket") {
			t.Fatalf("ListBuckets still contains deleted bucket: %v", buckets)
		}
		if err := h.DeleteBucket("doomed-bucket"); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("second DeleteBucket error = %v, want ErrBucketNotFound", err)
		}
		// The name becomes reusable after deletion.
		if err := h.CreateBucket("doomed-bucket"); err != nil {
			t.Fatalf("re-create after delete: %v", err)
		}
	})

	t.Run("delete non-empty bucket fails", func(t *testing.T) {
		mustCreateBucket(t, h, "occupied-bucket")
		mustPutManifest(t, h, &models.ObjectManifest{Bucket: "occupied-bucket", Key: "object.txt", ETag: "etag-1"})

		if err := h.DeleteBucket("occupied-bucket"); !errors.Is(err, ErrBucketNotEmpty) {
			t.Fatalf("DeleteBucket non-empty error = %v, want ErrBucketNotEmpty", err)
		}
		if err := h.DeleteManifest("occupied-bucket", "object.txt"); err != nil {
			t.Fatalf("DeleteManifest: %v", err)
		}
		if err := h.DeleteBucket("occupied-bucket"); err != nil {
			t.Fatalf("DeleteBucket after emptying: %v", err)
		}
	})
}

func TestObjectManifestRoundTripAndOverwrite(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "objects-bucket")

	original := &models.ObjectManifest{
		Bucket:      "objects-bucket",
		Key:         "dir/object.bin",
		Size:        1234,
		ContentType: "application/octet-stream",
		ETag:        "abc123",
		Chunks:      []string{"chunk-1", "chunk-2"},
		CreatedAt:   1700000000,
	}
	mustPutManifest(t, h, original)

	got, err := h.GetManifest("objects-bucket", "dir/object.bin")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	if got.Bucket != original.Bucket || got.Key != original.Key {
		t.Fatalf("identity = %q/%q, want %q/%q", got.Bucket, got.Key, original.Bucket, original.Key)
	}
	if got.Size != original.Size {
		t.Fatalf("Size = %d, want %d", got.Size, original.Size)
	}
	if got.ContentType != original.ContentType {
		t.Fatalf("ContentType = %q, want %q", got.ContentType, original.ContentType)
	}
	if got.ETag != original.ETag {
		t.Fatalf("ETag = %q, want %q", got.ETag, original.ETag)
	}
	if !slices.Equal(got.Chunks, original.Chunks) {
		t.Fatalf("Chunks = %v, want %v", got.Chunks, original.Chunks)
	}
	if got.CreatedAt != original.CreatedAt {
		t.Fatalf("CreatedAt = %d, want %d", got.CreatedAt, original.CreatedAt)
	}

	overwrite := &models.ObjectManifest{
		Bucket:      "objects-bucket",
		Key:         "dir/object.bin",
		Size:        5678,
		ContentType: "text/plain",
		ETag:        "def456",
		Chunks:      []string{"chunk-3"},
		CreatedAt:   1700000100,
	}
	mustPutManifest(t, h, overwrite)

	got, err = h.GetManifest("objects-bucket", "dir/object.bin")
	if err != nil {
		t.Fatalf("GetManifest after overwrite: %v", err)
	}
	if got.Size != 5678 || got.ETag != "def456" || got.ContentType != "text/plain" || got.CreatedAt != 1700000100 {
		t.Fatalf("overwrite not applied: %+v", got)
	}
	if !slices.Equal(got.Chunks, []string{"chunk-3"}) {
		t.Fatalf("Chunks after overwrite = %v, want [chunk-3]", got.Chunks)
	}

	if _, err := h.GetManifest("objects-bucket", "missing.bin"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("GetManifest missing error = %v, want ErrObjectNotFound", err)
	}
	if err := h.PutManifest(nil); err == nil {
		t.Fatal("PutManifest(nil) = nil, want error")
	}
	if err := h.DeleteManifest("objects-bucket", "missing.bin"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("DeleteManifest missing error = %v, want ErrObjectNotFound", err)
	}
	if err := h.DeleteManifest("objects-bucket", "dir/object.bin"); err != nil {
		t.Fatalf("DeleteManifest: %v", err)
	}
	if _, err := h.GetManifest("objects-bucket", "dir/object.bin"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("GetManifest after delete error = %v, want ErrObjectNotFound", err)
	}
}

func TestListObjectsPrefixAndOrdering(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "list-bucket")

	keys := []string{"videos/c.mp4", "photos/a.jpg", "photos/b.jpg", "photos2/d.jpg", "root.txt"}
	for _, key := range keys {
		mustPutManifest(t, h, &models.ObjectManifest{Bucket: "list-bucket", Key: key, ETag: "etag-" + key})
	}

	t.Run("all objects in key order", func(t *testing.T) {
		objects, err := h.ListObjects("list-bucket", "")
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		want := sortedStrings(keys)
		if got := objectKeys(objects); !slices.Equal(got, want) {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	})

	t.Run("prefix filters", func(t *testing.T) {
		objects, err := h.ListObjects("list-bucket", "photos/")
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		want := []string{"photos/a.jpg", "photos/b.jpg"}
		if got := objectKeys(objects); !slices.Equal(got, want) {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	})

	t.Run("prefix matching no keys returns empty", func(t *testing.T) {
		objects, err := h.ListObjects("list-bucket", "absent/")
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		if len(objects) != 0 {
			t.Fatalf("len(objects) = %d, want 0", len(objects))
		}
	})

	t.Run("exact key as prefix matches only that key", func(t *testing.T) {
		objects, err := h.ListObjects("list-bucket", "root.txt")
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		if got := objectKeys(objects); !slices.Equal(got, []string{"root.txt"}) {
			t.Fatalf("keys = %v, want [root.txt]", got)
		}
	})
}

func TestForEachObjectFrom(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "iterate-bucket")

	keys := []string{"a", "b", "c", "d"}
	for _, key := range keys {
		mustPutManifest(t, h, &models.ObjectManifest{Bucket: "iterate-bucket", Key: key, ETag: "etag-" + key})
	}

	collect := func(t *testing.T, startKey string) []string {
		t.Helper()
		got := []string{}
		if err := h.ForEachObjectFrom("iterate-bucket", startKey, func(object *models.ObjectManifest) error {
			got = append(got, object.Key)
			return nil
		}); err != nil {
			t.Fatalf("ForEachObjectFrom(%q): %v", startKey, err)
		}
		return got
	}

	t.Run("empty start key iterates everything in order", func(t *testing.T) {
		if got := collect(t, ""); !slices.Equal(got, keys) {
			t.Fatalf("keys = %v, want %v", got, keys)
		}
	})

	t.Run("start key is inclusive", func(t *testing.T) {
		if got := collect(t, "b"); !slices.Equal(got, []string{"b", "c", "d"}) {
			t.Fatalf("keys = %v, want [b c d]", got)
		}
	})

	t.Run("start key between keys resumes at next key", func(t *testing.T) {
		if got := collect(t, "bb"); !slices.Equal(got, []string{"c", "d"}) {
			t.Fatalf("keys = %v, want [c d]", got)
		}
	})

	t.Run("start key past end yields nothing", func(t *testing.T) {
		if got := collect(t, "z"); len(got) != 0 {
			t.Fatalf("keys = %v, want empty", got)
		}
	})

	t.Run("callback error aborts iteration", func(t *testing.T) {
		stop := errors.New("stop iteration")
		visited := []string{}
		err := h.ForEachObjectFrom("iterate-bucket", "", func(object *models.ObjectManifest) error {
			visited = append(visited, object.Key)
			if object.Key == "b" {
				return stop
			}
			return nil
		})
		if !errors.Is(err, stop) {
			t.Fatalf("error = %v, want %v", err, stop)
		}
		if !slices.Equal(visited, []string{"a", "b"}) {
			t.Fatalf("visited = %v, want [a b]", visited)
		}
	})

	t.Run("nil callback rejected", func(t *testing.T) {
		if err := h.ForEachObjectFrom("iterate-bucket", "", nil); err == nil {
			t.Fatal("ForEachObjectFrom(nil) = nil, want error")
		}
	})

	t.Run("missing bucket rejected", func(t *testing.T) {
		if err := h.ForEachObjectFrom("no-such-bucket", "", func(*models.ObjectManifest) error { return nil }); !errors.Is(err, ErrBucketNotFound) {
			t.Fatalf("error = %v, want ErrBucketNotFound", err)
		}
	})
}

func TestDeleteManifests(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "batch-delete-bucket")

	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		mustPutManifest(t, h, &models.ObjectManifest{Bucket: "batch-delete-bucket", Key: key, ETag: "etag-" + key})
	}

	deleted, err := h.DeleteManifests("batch-delete-bucket", []string{"a.txt", "missing.txt", "", "c.txt"})
	if err != nil {
		t.Fatalf("DeleteManifests: %v", err)
	}
	if want := []string{"a.txt", "missing.txt", "c.txt"}; !slices.Equal(deleted, want) {
		t.Fatalf("deleted = %v, want %v", deleted, want)
	}

	objects, err := h.ListObjects("batch-delete-bucket", "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if got := objectKeys(objects); !slices.Equal(got, []string{"b.txt"}) {
		t.Fatalf("remaining keys = %v, want [b.txt]", got)
	}

	if _, err := h.DeleteManifests("no-such-bucket", []string{"a.txt"}); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("DeleteManifests missing bucket error = %v, want ErrBucketNotFound", err)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "multipart-bucket")

	upload, err := h.CreateMultipartUpload("multipart-bucket", "dir/large.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if upload.UploadID == "" {
		t.Fatal("UploadID is empty")
	}
	if upload.Bucket != "multipart-bucket" || upload.Key != "dir/large.bin" {
		t.Fatalf("upload identity = %q/%q, want multipart-bucket/dir/large.bin", upload.Bucket, upload.Key)
	}
	if upload.State != "pending" {
		t.Fatalf("State = %q, want pending", upload.State)
	}
	if _, err := time.Parse(time.RFC3339, upload.CreatedAt); err != nil {
		t.Fatalf("CreatedAt %q is not RFC3339: %v", upload.CreatedAt, err)
	}

	stored, err := h.GetMultipartUpload(upload.UploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload: %v", err)
	}
	if stored.UploadID != upload.UploadID || stored.Bucket != upload.Bucket || stored.Key != upload.Key || stored.State != "pending" {
		t.Fatalf("stored upload = %+v, want %+v", stored, upload)
	}

	if _, err := h.GetMultipartUpload("missing-upload-id"); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("GetMultipartUpload missing error = %v, want ErrMultipartNotFound", err)
	}
	if err := h.PutMultipartPart("missing-upload-id", models.UploadedPart{PartNumber: 1}); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("PutMultipartPart missing upload error = %v, want ErrMultipartNotFound", err)
	}
	if err := h.AbortMultipartUpload("missing-upload-id"); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("AbortMultipartUpload missing upload error = %v, want ErrMultipartNotFound", err)
	}
	if err := h.CompleteMultipartUpload("missing-upload-id", &models.ObjectManifest{}); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("CompleteMultipartUpload missing upload error = %v, want ErrMultipartNotFound", err)
	}

	// Parts are stored unordered but listed in part-number order.
	parts := []models.UploadedPart{
		{PartNumber: 3, ETag: "etag-3", Size: 30, Chunks: []string{"chunk-3"}, CreatedAt: 1700000003},
		{PartNumber: 1, ETag: "etag-1", Size: 10, Chunks: []string{"chunk-1a", "chunk-1b"}, CreatedAt: 1700000001},
		{PartNumber: 2, ETag: "etag-2", Size: 20, Chunks: []string{"chunk-2"}, CreatedAt: 1700000002},
	}
	for _, part := range parts {
		if err := h.PutMultipartPart(upload.UploadID, part); err != nil {
			t.Fatalf("PutMultipartPart(%d): %v", part.PartNumber, err)
		}
	}

	listed, err := h.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("len(parts) = %d, want 3", len(listed))
	}
	for i, want := range []models.UploadedPart{parts[1], parts[2], parts[0]} {
		if listed[i].PartNumber != want.PartNumber || listed[i].ETag != want.ETag || listed[i].Size != want.Size || listed[i].CreatedAt != want.CreatedAt {
			t.Fatalf("part[%d] = %+v, want %+v", i, listed[i], want)
		}
		if !slices.Equal(listed[i].Chunks, want.Chunks) {
			t.Fatalf("part[%d].Chunks = %v, want %v", i, listed[i].Chunks, want.Chunks)
		}
	}

	// Re-uploading a part replaces the previous value.
	replacement := models.UploadedPart{PartNumber: 2, ETag: "etag-2b", Size: 25, Chunks: []string{"chunk-2b"}, CreatedAt: 1700000010}
	if err := h.PutMultipartPart(upload.UploadID, replacement); err != nil {
		t.Fatalf("PutMultipartPart replacement: %v", err)
	}
	listed, err = h.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("len(parts) after replacement = %d, want 3", len(listed))
	}
	if listed[1].ETag != "etag-2b" || listed[1].Size != 25 || !slices.Equal(listed[1].Chunks, []string{"chunk-2b"}) {
		t.Fatalf("replacement part = %+v, want etag-2b/25/[chunk-2b]", listed[1])
	}

	// Invalid part numbers are rejected before touching the store.
	for _, partNumber := range []int{-1, 0, 10001} {
		err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: partNumber})
		if err == nil {
			t.Fatalf("PutMultipartPart(%d) = nil, want error", partNumber)
		}
	}

	referenced, err := h.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet: %v", err)
	}
	for _, chunk := range []string{"chunk-1a", "chunk-1b", "chunk-2b", "chunk-3"} {
		if _, ok := referenced[chunk]; !ok {
			t.Fatalf("pending part chunk %q missing from referenced set %v", chunk, referenced)
		}
	}

	// Complete writes the object manifest and overrides bucket/key from the upload.
	final := &models.ObjectManifest{
		Bucket:      "caller-supplied-bucket",
		Key:         "caller-supplied-key",
		Size:        85,
		ContentType: "application/octet-stream",
		ETag:        "etag-final",
		Chunks:      []string{"chunk-1a", "chunk-1b", "chunk-2b", "chunk-3"},
		CreatedAt:   1700000020,
	}
	if err := h.CompleteMultipartUpload(upload.UploadID, final); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	manifest, err := h.GetManifest("multipart-bucket", "dir/large.bin")
	if err != nil {
		t.Fatalf("GetManifest after complete: %v", err)
	}
	if manifest.Bucket != "multipart-bucket" || manifest.Key != "dir/large.bin" {
		t.Fatalf("manifest identity = %q/%q, want multipart-bucket/dir/large.bin", manifest.Bucket, manifest.Key)
	}
	if manifest.ETag != "etag-final" || manifest.Size != 85 || manifest.ContentType != "application/octet-stream" {
		t.Fatalf("manifest = %+v, want etag-final/85/application/octet-stream", manifest)
	}
	if !slices.Equal(manifest.Chunks, final.Chunks) {
		t.Fatalf("manifest.Chunks = %v, want %v", manifest.Chunks, final.Chunks)
	}

	completed, err := h.GetMultipartUpload(upload.UploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload after complete: %v", err)
	}
	if completed.State != "completed" {
		t.Fatalf("State = %q, want completed", completed.State)
	}

	remainingParts, err := h.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts after complete: %v", err)
	}
	if len(remainingParts) != 0 {
		t.Fatalf("parts after complete = %v, want none", remainingParts)
	}

	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 1}); !errors.Is(err, ErrMultipartNotPending) {
		t.Fatalf("PutMultipartPart on completed error = %v, want ErrMultipartNotPending", err)
	}
	if err := h.CompleteMultipartUpload(upload.UploadID, &models.ObjectManifest{}); !errors.Is(err, ErrMultipartNotPending) {
		t.Fatalf("CompleteMultipartUpload again error = %v, want ErrMultipartNotPending", err)
	}
	if err := h.AbortMultipartUpload(upload.UploadID); !errors.Is(err, ErrMultipartNotPending) {
		t.Fatalf("AbortMultipartUpload on completed error = %v, want ErrMultipartNotPending", err)
	}

	if err := h.CompleteMultipartUpload(upload.UploadID, nil); err == nil {
		t.Fatal("CompleteMultipartUpload(nil) = nil, want error")
	}

	// Completed part chunks are still referenced because the final manifest has them.
	referenced, err = h.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet: %v", err)
	}
	for _, chunk := range final.Chunks {
		if _, ok := referenced[chunk]; !ok {
			t.Fatalf("manifest chunk %q missing from referenced set %v", chunk, referenced)
		}
	}
}

func TestAbortMultipartUploadCleansPartsAndBlocksBucketDelete(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "abort-bucket")

	upload, err := h.CreateMultipartUpload("abort-bucket", "pending.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 1, ETag: "etag-1", Size: 5, Chunks: []string{"abort-chunk"}}); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}

	// A pending upload keeps the bucket alive.
	if err := h.DeleteBucket("abort-bucket"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with pending upload error = %v, want ErrBucketNotEmpty", err)
	}

	if err := h.AbortMultipartUpload(upload.UploadID); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	aborted, err := h.GetMultipartUpload(upload.UploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload after abort: %v", err)
	}
	if aborted.State != "aborted" {
		t.Fatalf("State = %q, want aborted", aborted.State)
	}

	parts, err := h.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts after abort: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("parts after abort = %v, want none", parts)
	}

	referenced, err := h.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet: %v", err)
	}
	if _, ok := referenced["abort-chunk"]; ok {
		t.Fatalf("aborted part chunk still referenced: %v", referenced)
	}

	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 2}); !errors.Is(err, ErrMultipartNotPending) {
		t.Fatalf("PutMultipartPart after abort error = %v, want ErrMultipartNotPending", err)
	}
	if err := h.CompleteMultipartUpload(upload.UploadID, &models.ObjectManifest{}); !errors.Is(err, ErrMultipartNotPending) {
		t.Fatalf("CompleteMultipartUpload after abort error = %v, want ErrMultipartNotPending", err)
	}

	// Once aborted the upload no longer blocks bucket deletion.
	if err := h.DeleteBucket("abort-bucket"); err != nil {
		t.Fatalf("DeleteBucket after abort: %v", err)
	}
}

func TestCleanupMultipartUploadsRemovesParts(t *testing.T) {
	h, _ := newTestStore(t)
	mustCreateBucket(t, h, "cleanup-bucket")

	upload, err := h.CreateMultipartUpload("cleanup-bucket", "expired.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 1, Chunks: []string{"expired-chunk"}}); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	setMultipartUploadCreatedAt(t, h, upload.UploadID, time.Now().Add(-2*time.Hour))

	cleaned, err := h.CleanupMultipartUploads(time.Hour)
	if err != nil {
		t.Fatalf("CleanupMultipartUploads: %v", err)
	}
	if cleaned != 1 {
		t.Fatalf("cleaned = %d, want 1", cleaned)
	}
	if _, err := h.GetMultipartUpload(upload.UploadID); !errors.Is(err, ErrMultipartNotFound) {
		t.Fatalf("GetMultipartUpload after cleanup error = %v, want ErrMultipartNotFound", err)
	}

	referenced, err := h.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet: %v", err)
	}
	if _, ok := referenced["expired-chunk"]; ok {
		t.Fatalf("cleaned part chunk still referenced: %v", referenced)
	}
}

func TestAuthIdentityCRUD(t *testing.T) {
	h, _ := newTestStore(t)

	identity := &models.AuthIdentity{
		AccessKeyID: "AKIAFIRST",
		SecretEnc:   "ciphertext",
		SecretNonce: "nonce",
		EncAlg:      "AES-256-GCM",
		KeyVersion:  "v1",
		Status:      "active",
		CreatedAt:   1700000000,
		UpdatedAt:   1700000001,
	}
	if err := h.PutAuthIdentity(identity); err != nil {
		t.Fatalf("PutAuthIdentity: %v", err)
	}

	got, err := h.GetAuthIdentity("AKIAFIRST")
	if err != nil {
		t.Fatalf("GetAuthIdentity: %v", err)
	}
	if got.AccessKeyID != identity.AccessKeyID || got.SecretEnc != identity.SecretEnc || got.SecretNonce != identity.SecretNonce ||
		got.EncAlg != identity.EncAlg || got.KeyVersion != identity.KeyVersion || got.Status != identity.Status ||
		got.CreatedAt != identity.CreatedAt || got.UpdatedAt != identity.UpdatedAt {
		t.Fatalf("identity = %+v, want %+v", got, identity)
	}

	updated := *identity
	updated.Status = "disabled"
	updated.UpdatedAt = 1700000002
	if err := h.PutAuthIdentity(&updated); err != nil {
		t.Fatalf("PutAuthIdentity overwrite: %v", err)
	}
	got, err = h.GetAuthIdentity("AKIAFIRST")
	if err != nil {
		t.Fatalf("GetAuthIdentity after overwrite: %v", err)
	}
	if got.Status != "disabled" || got.UpdatedAt != 1700000002 {
		t.Fatalf("identity after overwrite = %+v, want status disabled", got)
	}

	if err := h.PutAuthIdentity(nil); err == nil {
		t.Fatal("PutAuthIdentity(nil) = nil, want error")
	}
	if err := h.PutAuthIdentity(&models.AuthIdentity{}); err == nil {
		t.Fatal("PutAuthIdentity(empty access key) = nil, want error")
	}
	if _, err := h.GetAuthIdentity("  "); err == nil {
		t.Fatal("GetAuthIdentity(blank) = nil error, want error")
	}
	if err := h.DeleteAuthIdentity(""); err == nil {
		t.Fatal("DeleteAuthIdentity(blank) = nil, want error")
	}
	if _, err := h.GetAuthIdentity("AKIAMISSING"); !errors.Is(err, ErrAuthIdentityNotFound) {
		t.Fatalf("GetAuthIdentity missing error = %v, want ErrAuthIdentityNotFound", err)
	}
	if err := h.DeleteAuthIdentity("AKIAMISSING"); !errors.Is(err, ErrAuthIdentityNotFound) {
		t.Fatalf("DeleteAuthIdentity missing error = %v, want ErrAuthIdentityNotFound", err)
	}

	for _, accessKey := range []string{"AKIA5", "AKIA1", "AKIA4", "AKIA2", "AKIA3"} {
		if err := h.PutAuthIdentity(&models.AuthIdentity{AccessKeyID: accessKey, Status: "active"}); err != nil {
			t.Fatalf("PutAuthIdentity(%s): %v", accessKey, err)
		}
	}

	ids := func(records []models.AuthIdentity) []string {
		out := make([]string, 0, len(records))
		for _, record := range records {
			out = append(out, record.AccessKeyID)
		}
		return out
	}

	// Non-positive limit falls back to the default (100), returning everything.
	page, cursor, err := h.ListAuthIdentities(0, "")
	if err != nil {
		t.Fatalf("ListAuthIdentities default limit: %v", err)
	}
	want := []string{"AKIA1", "AKIA2", "AKIA3", "AKIA4", "AKIA5", "AKIAFIRST"}
	if got := ids(page); !slices.Equal(got, want) {
		t.Fatalf("default limit page = %v, want %v", got, want)
	}
	if cursor != "" {
		t.Fatalf("cursor = %q, want empty", cursor)
	}

	// An explicit limit larger than the result set also reports no cursor.
	page, cursor, err = h.ListAuthIdentities(100, "")
	if err != nil {
		t.Fatalf("ListAuthIdentities limit 100: %v", err)
	}
	if got := ids(page); !slices.Equal(got, want) {
		t.Fatalf("limit 100 page = %v, want %v", got, want)
	}
	if cursor != "" {
		t.Fatalf("cursor = %q, want empty", cursor)
	}

	if err := h.DeleteAuthIdentity("AKIA3"); err != nil {
		t.Fatalf("DeleteAuthIdentity: %v", err)
	}
	if _, err := h.GetAuthIdentity("AKIA3"); !errors.Is(err, ErrAuthIdentityNotFound) {
		t.Fatalf("GetAuthIdentity after delete error = %v, want ErrAuthIdentityNotFound", err)
	}
}

func TestAuthIdentityListAfterCursorIsExclusive(t *testing.T) {
	h, _ := newTestStore(t)
	for _, accessKey := range []string{"AKIA1", "AKIA2", "AKIA3", "AKIA4", "AKIA5"} {
		if err := h.PutAuthIdentity(&models.AuthIdentity{AccessKeyID: accessKey, Status: "active"}); err != nil {
			t.Fatalf("PutAuthIdentity(%s): %v", accessKey, err)
		}
	}

	ids := func(records []models.AuthIdentity) []string {
		out := make([]string, 0, len(records))
		for _, record := range records {
			out = append(out, record.AccessKeyID)
		}
		return out
	}

	// `after` is the last key the caller has already seen, so the key named by
	// the cursor itself must not be returned again. The cursor must point at the
	// last returned key so feeding it back resumes without gaps.
	page, cursor, err := h.ListAuthIdentities(2, "AKIA2")
	if err != nil {
		t.Fatalf("ListAuthIdentities: %v", err)
	}
	if got, want := ids(page), []string{"AKIA3", "AKIA4"}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v", got, want)
	}
	if cursor != "AKIA4" {
		t.Fatalf("cursor = %q, want AKIA4 (last returned key)", cursor)
	}

	page, cursor, err = h.ListAuthIdentities(2, cursor)
	if err != nil {
		t.Fatalf("ListAuthIdentities: %v", err)
	}
	if got, want := ids(page), []string{"AKIA5"}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v", got, want)
	}
	if cursor != "" {
		t.Fatalf("cursor after final key = %q, want empty", cursor)
	}

	// An `after` key that does not exist resumes at the next greater key.
	page, _, err = h.ListAuthIdentities(1, "AKIA2X")
	if err != nil {
		t.Fatalf("ListAuthIdentities with unknown after: %v", err)
	}
	if got, want := ids(page), []string{"AKIA3"}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v", got, want)
	}

	// An empty `after` starts at the first key.
	page, _, err = h.ListAuthIdentities(2, "")
	if err != nil {
		t.Fatalf("ListAuthIdentities: %v", err)
	}
	if got, want := ids(page), []string{"AKIA1", "AKIA2"}; !slices.Equal(got, want) {
		t.Fatalf("page = %v, want %v", got, want)
	}
}

// TestAuthIdentityListCursorRoundTrip is the regression test for the cursor
// contract callers rely on: auth.Service.ListUsers passes the returned cursor
// straight back into ListAuthIdentities. The cursor must name the last returned
// key because `after` is exclusive (Seek then skip an equal key), otherwise one
// identity is silently dropped at every page boundary.
func TestAuthIdentityListCursorRoundTrip(t *testing.T) {
	h, _ := newTestStore(t)
	for _, accessKey := range []string{"AKIA1", "AKIA2", "AKIA3", "AKIA4", "AKIA5"} {
		if err := h.PutAuthIdentity(&models.AuthIdentity{AccessKeyID: accessKey, Status: "active"}); err != nil {
			t.Fatalf("PutAuthIdentity(%s): %v", accessKey, err)
		}
	}

	seen := []string{}
	cursor := ""
	pages := 0
	for {
		page, next, err := h.ListAuthIdentities(2, cursor)
		if err != nil {
			t.Fatalf("ListAuthIdentities: %v", err)
		}
		pages++
		for _, record := range page {
			seen = append(seen, record.AccessKeyID)
		}
		if next == "" {
			break
		}
		if next == cursor {
			t.Fatalf("cursor did not advance from %q", cursor)
		}
		cursor = next
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}

	want := []string{"AKIA1", "AKIA2", "AKIA3", "AKIA4", "AKIA5"}
	if !slices.Equal(seen, want) {
		t.Fatalf("paged identities = %v, want %v", seen, want)
	}
	if pages != 3 {
		t.Fatalf("pages = %d, want 3", pages)
	}
}

func TestAuthPolicyCRUD(t *testing.T) {
	h, _ := newTestStore(t)

	policy := &models.AuthPolicy{
		Principal: "alice",
		Statements: []models.AuthPolicyStatement{
			{Effect: "allow", Actions: []string{"s3:GetObject", "s3:ListBucket"}, Bucket: "policy-bucket", Prefix: "public/"},
			{Effect: "deny", Actions: []string{"s3:DeleteObject"}, Bucket: "policy-bucket", Prefix: "public/locked/"},
		},
	}
	if err := h.PutAuthPolicy(policy); err != nil {
		t.Fatalf("PutAuthPolicy: %v", err)
	}

	got, err := h.GetAuthPolicy("alice")
	if err != nil {
		t.Fatalf("GetAuthPolicy: %v", err)
	}
	if got.Principal != "alice" {
		t.Fatalf("Principal = %q, want alice", got.Principal)
	}
	if len(got.Statements) != 2 {
		t.Fatalf("len(Statements) = %d, want 2", len(got.Statements))
	}
	if got.Statements[0].Effect != "allow" || got.Statements[0].Bucket != "policy-bucket" || got.Statements[0].Prefix != "public/" {
		t.Fatalf("statement 0 = %+v", got.Statements[0])
	}
	if !slices.Equal(got.Statements[0].Actions, []string{"s3:GetObject", "s3:ListBucket"}) {
		t.Fatalf("statement 0 actions = %v", got.Statements[0].Actions)
	}
	if got.Statements[1].Effect != "deny" || !slices.Equal(got.Statements[1].Actions, []string{"s3:DeleteObject"}) {
		t.Fatalf("statement 1 = %+v", got.Statements[1])
	}

	// Principals are trimmed on write.
	if err := h.PutAuthPolicy(&models.AuthPolicy{Principal: "  bob  ", Statements: []models.AuthPolicyStatement{{Effect: "allow", Actions: []string{"s3:*"}}}}); err != nil {
		t.Fatalf("PutAuthPolicy trimmed principal: %v", err)
	}
	if _, err := h.GetAuthPolicy("bob"); err != nil {
		t.Fatalf("GetAuthPolicy(bob): %v", err)
	}

	overwrite := &models.AuthPolicy{Principal: "alice", Statements: []models.AuthPolicyStatement{{Effect: "deny", Actions: []string{"s3:*"}}}}
	if err := h.PutAuthPolicy(overwrite); err != nil {
		t.Fatalf("PutAuthPolicy overwrite: %v", err)
	}
	got, err = h.GetAuthPolicy("alice")
	if err != nil {
		t.Fatalf("GetAuthPolicy after overwrite: %v", err)
	}
	if len(got.Statements) != 1 || got.Statements[0].Effect != "deny" {
		t.Fatalf("policy after overwrite = %+v, want single deny statement", got)
	}

	if err := h.PutAuthPolicy(nil); err == nil {
		t.Fatal("PutAuthPolicy(nil) = nil, want error")
	}
	if err := h.PutAuthPolicy(&models.AuthPolicy{Principal: "   "}); err == nil {
		t.Fatal("PutAuthPolicy(blank principal) = nil, want error")
	}
	if _, err := h.GetAuthPolicy("nobody"); !errors.Is(err, ErrAuthPolicyNotFound) {
		t.Fatalf("GetAuthPolicy missing error = %v, want ErrAuthPolicyNotFound", err)
	}
	if err := h.DeleteAuthPolicy("nobody"); !errors.Is(err, ErrAuthPolicyNotFound) {
		t.Fatalf("DeleteAuthPolicy missing error = %v, want ErrAuthPolicyNotFound", err)
	}
	if err := h.DeleteAuthPolicy("alice"); err != nil {
		t.Fatalf("DeleteAuthPolicy: %v", err)
	}
	if _, err := h.GetAuthPolicy("alice"); !errors.Is(err, ErrAuthPolicyNotFound) {
		t.Fatalf("GetAuthPolicy after delete error = %v, want ErrAuthPolicyNotFound", err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.db")

	h, err := NewMetadataHandler(path)
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	if err := h.CreateBucket("persist-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	manifest := &models.ObjectManifest{
		Bucket:      "persist-bucket",
		Key:         "keep/me.txt",
		Size:        42,
		ContentType: "text/plain",
		ETag:        "persist-etag",
		Chunks:      []string{"persist-object-chunk"},
		CreatedAt:   1700000000,
	}
	if err := h.PutManifest(manifest); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	upload, err := h.CreateMultipartUpload("persist-bucket", "pending.bin")
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	if err := h.PutMultipartPart(upload.UploadID, models.UploadedPart{PartNumber: 1, ETag: "part-etag", Size: 7, Chunks: []string{"persist-part-chunk"}}); err != nil {
		t.Fatalf("PutMultipartPart: %v", err)
	}
	if err := h.PutAuthIdentity(&models.AuthIdentity{AccessKeyID: "AKIAPERSIST", SecretEnc: "sealed", Status: "active"}); err != nil {
		t.Fatalf("PutAuthIdentity: %v", err)
	}
	if err := h.PutAuthPolicy(&models.AuthPolicy{Principal: "persist-user", Statements: []models.AuthPolicyStatement{{Effect: "allow", Actions: []string{"s3:GetObject"}}}}); err != nil {
		t.Fatalf("PutAuthPolicy: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewMetadataHandler(path)
	if err != nil {
		t.Fatalf("NewMetadataHandler reopen: %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	got, err := reopened.GetManifest("persist-bucket", "keep/me.txt")
	if err != nil {
		t.Fatalf("GetManifest after reopen: %v", err)
	}
	if got.ETag != "persist-etag" || got.Size != 42 || got.ContentType != "text/plain" || got.CreatedAt != 1700000000 {
		t.Fatalf("manifest after reopen = %+v", got)
	}
	if !slices.Equal(got.Chunks, []string{"persist-object-chunk"}) {
		t.Fatalf("Chunks after reopen = %v", got.Chunks)
	}

	buckets, err := reopened.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets after reopen: %v", err)
	}
	if !slices.Equal(buckets, []string{"persist-bucket"}) {
		t.Fatalf("ListBuckets = %v, want [persist-bucket]", buckets)
	}

	uploadAfter, err := reopened.GetMultipartUpload(upload.UploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload after reopen: %v", err)
	}
	if uploadAfter.State != "pending" || uploadAfter.Key != "pending.bin" {
		t.Fatalf("upload after reopen = %+v", uploadAfter)
	}
	parts, err := reopened.ListMultipartParts(upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts after reopen: %v", err)
	}
	if len(parts) != 1 || parts[0].ETag != "part-etag" || parts[0].Size != 7 || !slices.Equal(parts[0].Chunks, []string{"persist-part-chunk"}) {
		t.Fatalf("parts after reopen = %+v", parts)
	}

	identity, err := reopened.GetAuthIdentity("AKIAPERSIST")
	if err != nil {
		t.Fatalf("GetAuthIdentity after reopen: %v", err)
	}
	if identity.SecretEnc != "sealed" || identity.Status != "active" {
		t.Fatalf("identity after reopen = %+v", identity)
	}

	policy, err := reopened.GetAuthPolicy("persist-user")
	if err != nil {
		t.Fatalf("GetAuthPolicy after reopen: %v", err)
	}
	if len(policy.Statements) != 1 || policy.Statements[0].Effect != "allow" {
		t.Fatalf("policy after reopen = %+v", policy)
	}

	referenced, err := reopened.GetReferencedChunkSet()
	if err != nil {
		t.Fatalf("GetReferencedChunkSet after reopen: %v", err)
	}
	for _, chunk := range []string{"persist-object-chunk", "persist-part-chunk"} {
		if _, ok := referenced[chunk]; !ok {
			t.Fatalf("chunk %q missing from referenced set %v", chunk, referenced)
		}
	}

	chunks, err := reopened.GetReferencedChunks()
	if err != nil {
		t.Fatalf("GetReferencedChunks after reopen: %v", err)
	}
	want := []string{"persist-object-chunk", "persist-part-chunk"}
	if got := sortedStrings(chunks); !slices.Equal(got, want) {
		t.Fatalf("GetReferencedChunks = %v, want %v", got, want)
	}
}
