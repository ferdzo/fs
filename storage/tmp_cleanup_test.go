package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTempCleanupFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("temp"), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func setTempCleanupAge(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("Chtimes(%s): %v", path, err)
	}
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be removed, stat error = %v", path, err)
	}
}

func TestRemoveStaleTempFilesRemovesOnlyStaleMatchingRegularFiles(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	shard := filepath.Join(root, blobRoot, "ab", "cd")
	oldName := strings.Repeat("a", 64) + ".tmp-1234567890"
	freshName := strings.Repeat("b", 64) + ".tmp-9876543210"
	chunkName := strings.Repeat("c", 64)
	nonHexName := strings.Repeat("z", 64) + ".tmp-1"
	upperHexName := strings.Repeat("A", 64) + ".tmp-2"
	nonDigitName := strings.Repeat("d", 64) + ".tmp-abc"
	noDigitsName := strings.Repeat("e", 64) + ".tmp-"
	dirName := strings.Repeat("f", 64) + ".tmp-3"
	symlinkName := strings.Repeat("1", 64) + ".tmp-4"

	oldPath := filepath.Join(shard, oldName)
	freshPath := filepath.Join(shard, freshName)
	chunkPath := filepath.Join(shard, chunkName)
	nonHexPath := filepath.Join(shard, nonHexName)
	upperHexPath := filepath.Join(shard, upperHexName)
	nonDigitPath := filepath.Join(shard, nonDigitName)
	noDigitsPath := filepath.Join(shard, noDigitsName)
	dirPath := filepath.Join(shard, dirName)
	symlinkPath := filepath.Join(shard, symlinkName)
	targetPath := filepath.Join(root, "symlink-target")

	writeTempCleanupFile(t, oldPath)
	writeTempCleanupFile(t, freshPath)
	writeTempCleanupFile(t, chunkPath)
	writeTempCleanupFile(t, nonHexPath)
	writeTempCleanupFile(t, upperHexPath)
	writeTempCleanupFile(t, nonDigitPath)
	writeTempCleanupFile(t, noDigitsPath)
	writeTempCleanupFile(t, targetPath)
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatalf("Mkdir(%s): %v", dirPath, err)
	}
	if err := os.Symlink(targetPath, symlinkPath); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	setTempCleanupAge(t, oldPath, 2*time.Hour)
	setTempCleanupAge(t, chunkPath, 2*time.Hour)
	setTempCleanupAge(t, nonHexPath, 2*time.Hour)
	setTempCleanupAge(t, upperHexPath, 2*time.Hour)
	setTempCleanupAge(t, nonDigitPath, 2*time.Hour)
	setTempCleanupAge(t, noDigitsPath, 2*time.Hour)
	setTempCleanupAge(t, dirPath, 2*time.Hour)
	setTempCleanupAge(t, targetPath, 2*time.Hour)

	removed, err := bs.RemoveStaleTempFiles(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("RemoveStaleTempFiles: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	assertMissing(t, oldPath)
	assertExists(t, freshPath)
	assertExists(t, chunkPath)
	assertExists(t, nonHexPath)
	assertExists(t, upperHexPath)
	assertExists(t, nonDigitPath)
	assertExists(t, noDigitsPath)
	assertExists(t, dirPath)
	assertExists(t, symlinkPath)
	assertExists(t, targetPath)
}

func TestRemoveStaleTempFilesMissingDirectory(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(root, blobRoot)); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}

	removed, err := bs.RemoveStaleTempFiles(time.Now())
	if err != nil {
		t.Fatalf("RemoveStaleTempFiles on missing directory: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}

func TestRemoveStaleTempFilesEmptyStore(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}

	removed, err := bs.RemoveStaleTempFiles(time.Now())
	if err != nil {
		t.Fatalf("RemoveStaleTempFiles: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}

func TestRemoveStaleTempFilesUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions are not enforced for root")
	}
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("NewBlobStore: %v", err)
	}
	blobDir := filepath.Join(root, blobRoot)
	if err := os.Chmod(blobDir, 0); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(blobDir, 0o755)
	})

	removed, err := bs.RemoveStaleTempFiles(time.Now())
	if err == nil {
		t.Fatalf("RemoveStaleTempFiles returned removed=%d, want an error", removed)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
}
