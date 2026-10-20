package metadata

import (
	"errors"
	"fs/metrics"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"fs/models"

	bolt "go.etcd.io/bbolt"
)

func TestForEachObjectKeyFromIsLazyAndStopsNormally(t *testing.T) {
	h, err := NewMetadataHandler(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	const bucket = "lazy-bucket"
	if err := h.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := h.PutManifest(&models.ObjectManifest{
			Bucket: bucket,
			Key:    key,
			ETag:   "etag-" + key,
			Size:   int64(len(key)),
		}); err != nil {
			t.Fatalf("PutManifest(%q): %v", key, err)
		}
	}
	// A skipped key with invalid JSON must not be decoded by a key-only scan.
	if err := h.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Put([]byte("b"), []byte("{not-json"))
	}); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}

	var visited []string
	err = h.ForEachObjectKeyFrom(bucket, "b", func(key string, decode func() (*models.ObjectManifest, error)) error {
		visited = append(visited, key)
		if key == "c" {
			return StopIteration()
		}
		// Deliberately never call decode: a key-only scan must not touch JSON.
		return nil
	})
	if !IsIterationStopped(err) {
		t.Fatalf("stop error = %v, want IsIterationStopped", err)
	}
	if want := []string{"b", "c"}; !slices.Equal(visited, want) {
		t.Fatalf("visited = %v, want %v (Seek is inclusive and stop aborts immediately)", visited, want)
	}
	var decoded *models.ObjectManifest
	if err := h.ForEachObjectKeyFrom(bucket, "d", func(key string, decode func() (*models.ObjectManifest, error)) error {
		var err error
		decoded, err = decode()
		return err
	}); err != nil {
		t.Fatalf("decode d: %v", err)
	}
	if decoded == nil || decoded.Key != "d" || decoded.ETag != "etag-d" {
		t.Fatalf("decoded = %+v, want key d", decoded)
	}

	// Asking for the malformed manifest still fails and propagates normally.
	err = h.ForEachObjectKeyFrom(bucket, "b", func(key string, decode func() (*models.ObjectManifest, error)) error {
		if key != "b" {
			return StopIteration()
		}
		_, decodeErr := decode()
		return decodeErr
	})
	if err == nil {
		t.Fatal("decoding malformed manifest succeeded")
	}
	if IsIterationStopped(err) {
		t.Fatalf("malformed-manifest error %v was misclassified as a normal stop", err)
	}

	// A genuine callback error keeps its identity and is not a normal stop.
	boom := errors.New("callback boom")
	err = h.ForEachObjectKeyFrom(bucket, "", func(key string, decode func() (*models.ObjectManifest, error)) error {
		if key == "c" {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("callback error = %v, want %v", err, boom)
	}
	if IsIterationStopped(err) {
		t.Fatalf("callback error %v was misclassified as a normal stop", err)
	}

	// The eager compatibility wrapper still decodes for callers that need it.
	var eager []string
	if err := h.ForEachObjectFrom(bucket, "d", func(object *models.ObjectManifest) error {
		eager = append(eager, object.Key)
		return nil
	}); err != nil {
		t.Fatalf("ForEachObjectFrom: %v", err)
	}
	if want := []string{"d"}; !slices.Equal(eager, want) {
		t.Fatalf("eager keys = %v, want %v", eager, want)
	}
}

func TestStopIterationIsUnforgeableSentinel(t *testing.T) {
	if StopIteration() == nil {
		t.Fatal("StopIteration() = nil")
	}
	if !IsIterationStopped(StopIteration()) {
		t.Fatal("IsIterationStopped(StopIteration()) = false")
	}
	if IsIterationStopped(errors.New("other")) {
		t.Fatal("unrelated error classified as StopIteration")
	}
}

func metadataViewTxCount(t *testing.T, result string) float64 {
	t.Helper()
	text := metrics.Default.RenderPrometheus()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "fs_metadata_tx_total{") {
			continue
		}
		if !strings.Contains(line, `type="view"`) || !strings.Contains(line, `result="`+result+`"`) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("parse metric line %q: %v", line, err)
		}
		return value
	}
	return 0
}

func TestForEachObjectKeyFromRecordsStopAsSuccess(t *testing.T) {
	h, err := NewMetadataHandler(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetadataHandler: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	const bucket = "metrics-bucket"
	if err := h.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := h.PutManifest(&models.ObjectManifest{Bucket: bucket, Key: key}); err != nil {
			t.Fatalf("PutManifest(%q): %v", key, err)
		}
	}

	okBefore := metadataViewTxCount(t, "ok")
	errBefore := metadataViewTxCount(t, "error")
	err = h.ForEachObjectKeyFrom(bucket, "", func(key string, decode func() (*models.ObjectManifest, error)) error {
		return StopIteration()
	})
	if !IsIterationStopped(err) {
		t.Fatalf("stop error = %v, want IsIterationStopped", err)
	}
	if got := metadataViewTxCount(t, "ok"); got != okBefore+1 {
		t.Fatalf("ok view tx count = %v, want %v (normal stop must be success)", got, okBefore+1)
	}
	if got := metadataViewTxCount(t, "error"); got != errBefore {
		t.Fatalf("error view tx count = %v, want %v (normal stop must not be an error)", got, errBefore)
	}

	okBefore = metadataViewTxCount(t, "ok")
	errBefore = metadataViewTxCount(t, "error")
	boom := errors.New("callback boom")
	err = h.ForEachObjectKeyFrom(bucket, "", func(key string, decode func() (*models.ObjectManifest, error)) error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("callback error = %v, want %v", err, boom)
	}
	if got := metadataViewTxCount(t, "error"); got != errBefore+1 {
		t.Fatalf("error view tx count = %v, want %v (callback error must stay an error)", got, errBefore+1)
	}
	if got := metadataViewTxCount(t, "ok"); got != okBefore {
		t.Fatalf("ok view tx count = %v, want %v (callback error must not count as success)", got, okBefore)
	}
}
