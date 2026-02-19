package data

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"fs/models"
	"io"
	"os"
	"path/filepath"
)

const chunkSize = 64 * 1024
const blobRoot = "blobs/"

func IngestStream(bucket, key, contentType string, stream io.Reader) (*models.ObjectManifest, error) {
	manifest := &models.ObjectManifest{
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
	}

	fullFileHasher := md5.New()

	buffer := make([]byte, chunkSize)
	var totalSize int64

	for {
		bytesRead, err := io.ReadFull(stream, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return nil, err
		}

		if bytesRead > 0 {
			chunkData := buffer[:bytesRead]
			totalSize += int64(bytesRead)

			fullFileHasher.Write(chunkData)

			chunkHash := sha256.Sum256(chunkData)
			chunkID := hex.EncodeToString(chunkHash[:])

			err := saveBlob(chunkID, chunkData)
			if err != nil {
				return nil, err
			}
			manifest.Chunks = append(manifest.Chunks, chunkID)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}

	}

	manifest.Size = totalSize
	manifest.ETag = fmt.Sprintf(`"%s"`, hex.EncodeToString(fullFileHasher.Sum(nil)))

	return manifest, nil

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

func GetBlob(chunkID string) ([]byte, error) {

	return os.ReadFile(filepath.Join(blobRoot, chunkID[:2], chunkID[2:4], chunkID))
}

func GetObject(manifest *models.ObjectManifest) ([]byte, error) {
	var fullData []byte
	for _, chunkID := range manifest.Chunks {
		chunkData, err := GetBlob(chunkID)
		if err != nil {
			return nil, err
		}
		fullData = append(fullData, chunkData...)
	}
	return fullData, nil
}
