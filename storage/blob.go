package storage

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const blobRoot = "blobs"
const maxChunkSize = 64 * 1024 * 1024

type BlobStore struct {
	dataRoot  string
	chunkSize int
}

func NewBlobStore(root string, chunkSize int) (*BlobStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, errors.New("blob root is required")
	}
	if chunkSize <= 0 || chunkSize > maxChunkSize {
		return nil, fmt.Errorf("chunk size must be between 1 and %d bytes", maxChunkSize)
	}

	cleanRoot := filepath.Clean(root)
	if err := os.MkdirAll(filepath.Join(cleanRoot, blobRoot), 0o755); err != nil {
		return nil, err
	}
	return &BlobStore{chunkSize: chunkSize, dataRoot: cleanRoot}, nil
}

func (bs *BlobStore) IngestStream(stream io.Reader) ([]string, int64, string, error) {
	fullFileHasher := md5.New()

	buffer := make([]byte, bs.chunkSize)
	var totalSize int64
	var chunkIDs []string

	for {
		bytesRead, err := io.ReadFull(stream, buffer)
		if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, "", err
		}

		if bytesRead > 0 {
			chunkData := buffer[:bytesRead]
			totalSize += int64(bytesRead)

			fullFileHasher.Write(chunkData)

			chunkHash := sha256.Sum256(chunkData)
			chunkID := hex.EncodeToString(chunkHash[:])

			err := bs.saveBlob(chunkID, chunkData)
			if err != nil {
				return nil, 0, "", err
			}
			chunkIDs = append(chunkIDs, chunkID)
		}
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, 0, "", err
		}

	}

	etag := hex.EncodeToString(fullFileHasher.Sum(nil))
	return chunkIDs, totalSize, etag, nil
}

func (bs *BlobStore) saveBlob(chunkID string, data []byte) error {
	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	dir := filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4])
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fullPath := filepath.Join(dir, chunkID)
	if _, err := os.Stat(fullPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	tmpFile, err := os.CreateTemp(dir, chunkID+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		if _, statErr := os.Stat(fullPath); statErr == nil {
			return nil
		}
		return err
	}
	cleanup = false

	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func (bs *BlobStore) AssembleStream(chunkIDs []string, w *io.PipeWriter) error {
	for _, chunkID := range chunkIDs {
		chunkData, err := bs.GetBlob(chunkID)
		if err != nil {
			return err
		}
		if _, err := w.Write(chunkData); err != nil {
			return err
		}
	}
	return nil
}

func (bs *BlobStore) GetBlob(chunkID string) ([]byte, error) {
	if !isValidChunkID(chunkID) {
		return nil, fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	return os.ReadFile(filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID))
}

func (bs *BlobStore) DeleteBlob(chunkID string) error {
	if !isValidChunkID(chunkID) {
		return fmt.Errorf("invalid chunk id: %q", chunkID)
	}
	err := os.Remove(filepath.Join(bs.dataRoot, blobRoot, chunkID[:2], chunkID[2:4], chunkID))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func (bs *BlobStore) ListChunks() ([]string, error) {
	var chunkIDs []string
	err := bs.ForEachChunk(func(chunkID string) error {
		chunkIDs = append(chunkIDs, chunkID)
		return nil
	})
	return chunkIDs, err
}

func (bs *BlobStore) ForEachChunk(fn func(chunkID string) error) error {
	if fn == nil {
		return errors.New("chunk callback is required")
	}
	return filepath.Walk(filepath.Join(bs.dataRoot, blobRoot), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			chunkID := info.Name()
			if isValidChunkID(chunkID) {
				return fn(chunkID)
			}
		}
		return nil

	})
}

func isValidChunkID(chunkID string) bool {
	if len(chunkID) != sha256.Size*2 {
		return false
	}
	for _, ch := range chunkID {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func syncDir(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
