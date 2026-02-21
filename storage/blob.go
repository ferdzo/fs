package storage

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

const chunkSize = 64 * 1024
const blobRoot = "blobs/"

func IngestStream(stream io.Reader) ([]string, int64, string, error) {
	fullFileHasher := md5.New()

	buffer := make([]byte, chunkSize)
	var totalSize int64
	var chunkIDs []string

	for {
		bytesRead, err := io.ReadFull(stream, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, 0, "", err
		}

		if bytesRead > 0 {
			chunkData := buffer[:bytesRead]
			totalSize += int64(bytesRead)

			fullFileHasher.Write(chunkData)

			chunkHash := sha256.Sum256(chunkData)
			chunkID := hex.EncodeToString(chunkHash[:])

			err := saveBlob(chunkID, chunkData)
			if err != nil {
				return nil, 0, "", err
			}
			chunkIDs = append(chunkIDs, chunkID)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, 0, "", err
		}

	}
	
	etag := hex.EncodeToString(fullFileHasher.Sum(nil))
	return chunkIDs, totalSize, etag, nil
}

func saveBlob(chunkID string, data []byte) error {
	dir := filepath.Join(blobRoot, chunkID[:2], chunkID[2:4])
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	fullPath := filepath.Join(dir, chunkID)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		if err := os.WriteFile(fullPath, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

func AssembleStream(chunkIDs []string, w *io.PipeWriter) error {
	for _, chunkID := range chunkIDs {
		chunkData, err := GetBlob(chunkID)
		if err != nil {
			return err
		}
		if _, err := w.Write(chunkData); err != nil {
			return err
		}
	}
	return nil
}

func GetBlob(chunkID string) ([]byte, error) {

	return os.ReadFile(filepath.Join(blobRoot, chunkID[:2], chunkID[2:4], chunkID))
}
