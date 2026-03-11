package cmd

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

type snapshotArchiveEntry struct {
	Path string
	Data []byte
}

func TestInspectSnapshotArchiveRejectsUnsafePath(t *testing.T) {
	t.Parallel()

	archive := filepath.Join(t.TempDir(), "bad.tar.gz")
	manifest := manifestForEntries([]snapshotArchiveEntry{
		{Path: "metadata.db", Data: []byte("db")},
	})
	err := writeSnapshotArchiveForTest(archive, manifest, []snapshotArchiveEntry{
		{Path: "../escape", Data: []byte("oops")},
	}, true)
	if err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	_, _, err = inspectSnapshotArchive(archive)
	if err == nil || !strings.Contains(err.Error(), "unsafe archive path") {
		t.Fatalf("expected unsafe archive path error, got %v", err)
	}
}

func TestInspectSnapshotArchiveChecksumMismatch(t *testing.T) {
	t.Parallel()

	archive := filepath.Join(t.TempDir(), "mismatch.tar.gz")
	manifest := manifestForEntries([]snapshotArchiveEntry{
		{Path: "chunks/c1", Data: []byte("good")},
	})
	err := writeSnapshotArchiveForTest(archive, manifest, []snapshotArchiveEntry{
		{Path: "chunks/c1", Data: []byte("bad")},
	}, true)
	if err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	_, _, err = inspectSnapshotArchive(archive)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
}

func TestInspectSnapshotArchiveMissingManifest(t *testing.T) {
	t.Parallel()

	archive := filepath.Join(t.TempDir(), "no-manifest.tar.gz")
	err := writeSnapshotArchiveForTest(archive, nil, []snapshotArchiveEntry{
		{Path: "chunks/c1", Data: []byte("x")},
	}, false)
	if err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	_, _, err = inspectSnapshotArchive(archive)
	if err == nil || !strings.Contains(err.Error(), "manifest.json not found") {
		t.Fatalf("expected missing manifest error, got %v", err)
	}
}

func TestInspectSnapshotArchiveUnsupportedFormat(t *testing.T) {
	t.Parallel()

	archive := filepath.Join(t.TempDir(), "unsupported-format.tar.gz")
	manifest := manifestForEntries([]snapshotArchiveEntry{
		{Path: "chunks/c1", Data: []byte("x")},
	})
	manifest.FormatVersion = 99
	err := writeSnapshotArchiveForTest(archive, manifest, []snapshotArchiveEntry{
		{Path: "chunks/c1", Data: []byte("x")},
	}, true)
	if err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	_, _, err = inspectSnapshotArchive(archive)
	if err == nil || !strings.Contains(err.Error(), "unsupported snapshot format version") {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
}

func TestRestoreSnapshotArchiveDestinationBehavior(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	archive := filepath.Join(root, "ok.tar.gz")
	destination := filepath.Join(root, "dst")

	entries := []snapshotArchiveEntry{
		{Path: "metadata.db", Data: []byte("db-bytes")},
		{Path: "chunks/c1", Data: []byte("chunk-1")},
	}
	manifest := manifestForEntries(entries)
	if err := writeSnapshotArchiveForTest(archive, manifest, entries, true); err != nil {
		t.Fatalf("write test archive: %v", err)
	}

	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if err := os.WriteFile(filepath.Join(destination, "old.txt"), []byte("old"), 0o600); err != nil {
		t.Fatalf("seed destination: %v", err)
	}

	if _, err := restoreSnapshotArchive(context.Background(), archive, destination, false); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("expected non-empty destination error, got %v", err)
	}

	if _, err := restoreSnapshotArchive(context.Background(), archive, destination, true); err != nil {
		t.Fatalf("restore with force: %v", err)
	}

	if _, err := os.Stat(filepath.Join(destination, "old.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be removed, stat err=%v", err)
	}
	got, err := os.ReadFile(filepath.Join(destination, "chunks/c1"))
	if err != nil {
		t.Fatalf("read restored chunk: %v", err)
	}
	if string(got) != "chunk-1" {
		t.Fatalf("restored chunk mismatch: got %q", string(got))
	}
}

func TestCreateSnapshotArchiveRejectsOutputInsideDataPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "chunks"), 0o755); err != nil {
		t.Fatalf("mkdir chunks: %v", err)
	}
	if err := createBoltDBForTest(filepath.Join(root, "metadata.db")); err != nil {
		t.Fatalf("create metadata db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "chunks/c1"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write chunk: %v", err)
	}

	out := filepath.Join(root, "inside.tar.gz")
	if _, err := createSnapshotArchive(context.Background(), root, out); err == nil || !strings.Contains(err.Error(), "cannot be inside") {
		t.Fatalf("expected output-inside-data-path error, got %v", err)
	}
}

func writeSnapshotArchiveForTest(path string, manifest *snapshotManifest, entries []snapshotArchiveEntry, includeManifest bool) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gzw := gzip.NewWriter(file)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	if includeManifest {
		raw, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		if err := writeTarEntry(tw, snapshotManifestPath, raw); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		if err := writeTarEntry(tw, entry.Path, entry.Data); err != nil {
			return err
		}
	}
	return nil
}

func writeTarEntry(tw *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name: name,
		Mode: 0o600,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := ioCopyBytes(tw, data)
	return err
}

func manifestForEntries(entries []snapshotArchiveEntry) *snapshotManifest {
	files := make([]snapshotFileEntry, 0, len(entries))
	for _, entry := range entries {
		sum := sha256.Sum256(entry.Data)
		files = append(files, snapshotFileEntry{
			Path:   filepath.ToSlash(filepath.Clean(entry.Path)),
			Size:   int64(len(entry.Data)),
			SHA256: hex.EncodeToString(sum[:]),
		})
	}
	return &snapshotManifest{
		FormatVersion: snapshotFormat,
		CreatedAt:     "2026-03-11T00:00:00Z",
		SourcePath:    "/tmp/source",
		Files:         files,
	}
}

func createBoltDBForTest(path string) error {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("x"))
		return err
	})
}

func ioCopyBytes(w *tar.Writer, data []byte) (int64, error) {
	n, err := bytes.NewReader(data).WriteTo(w)
	return n, err
}
