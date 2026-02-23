package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type ObjectService struct {
	metadata *metadata.MetadataHandler
	blob     *storage.BlobStore
	gcMu     sync.RWMutex
}

var (
	ErrInvalidPart            = errors.New("invalid multipart part")
	ErrInvalidPartOrder       = errors.New("invalid multipart part order")
	ErrInvalidCompleteRequest = errors.New("invalid complete multipart request")
	ErrEntityTooSmall         = errors.New("multipart entity too small")
)

func NewObjectService(metadataHandler *metadata.MetadataHandler, blobHandler *storage.BlobStore) *ObjectService {
	return &ObjectService{metadata: metadataHandler, blob: blobHandler}
}

func (s *ObjectService) PutObject(bucket, key, contentType string, input io.Reader) (*models.ObjectManifest, error) {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()

	chunks, size, etag, err := s.blob.IngestStream(input)
	if err != nil {
		return nil, err
	}
	timestamp := time.Now().Unix()

	manifest := &models.ObjectManifest{
		Bucket:      bucket,
		Key:         key,
		Size:        size,
		ContentType: contentType,
		ETag:        etag,
		Chunks:      chunks,
		CreatedAt:   timestamp,
	}
	slog.Debug("object_written_manifest",
		"bucket", manifest.Bucket,
		"key", manifest.Key,
		"size", manifest.Size,
		"chunk_count", len(manifest.Chunks),
		"etag", manifest.ETag,
	)
	if err = s.metadata.PutManifest(manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (s *ObjectService) GetObject(bucket, key string) (io.ReadCloser, *models.ObjectManifest, error) {
	manifest, err := s.metadata.GetManifest(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	pr, pw := io.Pipe()

	go func() {
		if err := s.blob.AssembleStream(manifest.Chunks, pw); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = pw.Close()
	}()
	return pr, manifest, nil
}

func (s *ObjectService) HeadObject(bucket, key string) (models.ObjectManifest, error) {
	manifest, err := s.metadata.GetManifest(bucket, key)
	if err != nil {
		return models.ObjectManifest{}, err
	}
	return *manifest, nil
}

func (s *ObjectService) DeleteObject(bucket, key string) error {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	return s.metadata.DeleteManifest(bucket, key)
}

func (s *ObjectService) ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error) {
	return s.metadata.ListObjects(bucket, prefix)
}

func (s *ObjectService) CreateBucket(bucket string) error {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	return s.metadata.CreateBucket(bucket)
}

func (s *ObjectService) HeadBucket(bucket string) error {
	_, err := s.metadata.GetBucketManifest(bucket)
	return err
}

func (s *ObjectService) GetBucketManifest(bucket string) (*models.BucketManifest, error) {
	return s.metadata.GetBucketManifest(bucket)
}

func (s *ObjectService) DeleteBucket(bucket string) error {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	return s.metadata.DeleteBucket(bucket)
}

func (s *ObjectService) ListBuckets() ([]string, error) {
	return s.metadata.ListBuckets()
}

func (s *ObjectService) DeleteObjects(bucket string, keys []string) ([]string, error) {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	return s.metadata.DeleteManifests(bucket, keys)
}

func (s *ObjectService) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()
	return s.metadata.CreateMultipartUpload(bucket, key)
}

func (s *ObjectService) UploadPart(bucket, key, uploadId string, partNumber int, input io.Reader) (string, error) {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()

	if partNumber < 1 || partNumber > 10000 {
		return "", ErrInvalidPart
	}

	upload, err := s.metadata.GetMultipartUpload(uploadId)
	if err != nil {
		return "", err
	}
	if upload.Bucket != bucket || upload.Key != key {
		return "", metadata.ErrMultipartNotFound
	}

	var uploadedPart models.UploadedPart
	chunkIds, totalSize, etag, err := s.blob.IngestStream(input)
	if err != nil {
		return "", err
	}
	uploadedPart = models.UploadedPart{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       totalSize,
		Chunks:     chunkIds,
		CreatedAt:  time.Now().Unix(),
	}
	err = s.metadata.PutMultipartPart(uploadId, uploadedPart)
	if err != nil {
		return "", err
	}
	return etag, nil
}

func (s *ObjectService) ListMultipartParts(bucket, key, uploadID string) ([]models.UploadedPart, error) {
	upload, err := s.metadata.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}
	if upload.Bucket != bucket || upload.Key != key {
		return nil, metadata.ErrMultipartNotFound
	}
	return s.metadata.ListMultipartParts(uploadID)
}

func (s *ObjectService) CompleteMultipartUpload(bucket, key, uploadID string, completed []models.CompletedPart) (*models.ObjectManifest, error) {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()

	if len(completed) == 0 {
		return nil, ErrInvalidCompleteRequest
	}

	upload, err := s.metadata.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}
	if upload.Bucket != bucket || upload.Key != key {
		return nil, metadata.ErrMultipartNotFound
	}

	storedParts, err := s.metadata.ListMultipartParts(uploadID)
	if err != nil {
		return nil, err
	}
	partsByNumber := make(map[int]models.UploadedPart, len(storedParts))
	for _, part := range storedParts {
		partsByNumber[part.PartNumber] = part
	}

	lastPartNumber := 0
	orderedParts := make([]models.UploadedPart, 0, len(completed))
	chunks := make([]string, 0)
	var totalSize int64

	for i, part := range completed {
		if part.PartNumber <= lastPartNumber {
			return nil, ErrInvalidPartOrder
		}
		lastPartNumber = part.PartNumber

		storedPart, ok := partsByNumber[part.PartNumber]
		if !ok {
			return nil, ErrInvalidPart
		}
		if normalizeETag(part.ETag) != normalizeETag(storedPart.ETag) {
			return nil, ErrInvalidPart
		}
		if i < len(completed)-1 && storedPart.Size < 5*1024*1024 {
			return nil, ErrEntityTooSmall
		}

		orderedParts = append(orderedParts, storedPart)
		chunks = append(chunks, storedPart.Chunks...)
		totalSize += storedPart.Size
	}

	finalETag := buildMultipartETag(orderedParts)
	manifest := &models.ObjectManifest{
		Bucket:      bucket,
		Key:         key,
		Size:        totalSize,
		ContentType: "application/octet-stream",
		ETag:        finalETag,
		Chunks:      chunks,
		CreatedAt:   time.Now().Unix(),
	}

	if err := s.metadata.CompleteMultipartUpload(uploadID, manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (s *ObjectService) AbortMultipartUpload(bucket, key, uploadID string) error {
	s.gcMu.RLock()
	defer s.gcMu.RUnlock()

	upload, err := s.metadata.GetMultipartUpload(uploadID)
	if err != nil {
		return err
	}
	if upload.Bucket != bucket || upload.Key != key {
		return metadata.ErrMultipartNotFound
	}
	return s.metadata.AbortMultipartUpload(uploadID)
}

func normalizeETag(etag string) string {
	return strings.Trim(etag, "\"")
}

func buildMultipartETag(parts []models.UploadedPart) string {
	hasher := md5.New()
	for _, part := range parts {
		etagBytes, err := hex.DecodeString(normalizeETag(part.ETag))
		if err == nil {
			_, _ = hasher.Write(etagBytes)
			continue
		}
		_, _ = hasher.Write([]byte(normalizeETag(part.ETag)))
	}
	return fmt.Sprintf("%x-%d", hasher.Sum(nil), len(parts))
}

func (s *ObjectService) Close() error {
	return s.metadata.Close()
}

func (s *ObjectService) GarbageCollect() error {
	s.gcMu.Lock()
	defer s.gcMu.Unlock()

	referencedChunkSet, err := s.metadata.GetReferencedChunkSet()
	if err != nil {
		return err
	}

	totalChunks := 0
	deletedChunks := 0

	if err := s.blob.ForEachChunk(func(chunkID string) error {
		totalChunks++
		if _, found := referencedChunkSet[chunkID]; found {
			return nil
		}
		if err := s.blob.DeleteBlob(chunkID); err != nil {
			return err
		}
		deletedChunks++
		return nil
	}); err != nil {
		return err
	}

	slog.Info("garbage_collect_completed",
		"referenced_chunks", len(referencedChunkSet),
		"total_chunks", totalChunks,
		"deleted_chunks", deletedChunks,
	)
	return nil
}

func (s *ObjectService) RunGC(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.GarbageCollect()
		}
	}
}
