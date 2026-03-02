package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"fs/metadata"
	"fs/metrics"
	"fs/models"
	"fs/storage"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type ObjectService struct {
	metadata           *metadata.MetadataHandler
	blob               *storage.BlobStore
	multipartRetention time.Duration
	gcMu               sync.RWMutex
}

var (
	ErrInvalidPart            = errors.New("invalid multipart part")
	ErrInvalidPartOrder       = errors.New("invalid multipart part order")
	ErrInvalidCompleteRequest = errors.New("invalid complete multipart request")
	ErrEntityTooSmall         = errors.New("multipart entity too small")
)

func NewObjectService(metadataHandler *metadata.MetadataHandler, blobHandler *storage.BlobStore, multipartRetention time.Duration) *ObjectService {
	if multipartRetention <= 0 {
		multipartRetention = 24 * time.Hour
	}
	return &ObjectService{
		metadata:           metadataHandler,
		blob:               blobHandler,
		multipartRetention: multipartRetention,
	}
}

func (s *ObjectService) acquireGCRLock() func() {
	waitStart := time.Now()
	s.gcMu.RLock()
	metrics.Default.ObserveLockWait("gc_mu_read", time.Since(waitStart))
	holdStart := time.Now()
	return func() {
		metrics.Default.ObserveLockHold("gc_mu_read", time.Since(holdStart))
		s.gcMu.RUnlock()
	}
}

func (s *ObjectService) acquireGCLock() func() {
	waitStart := time.Now()
	s.gcMu.Lock()
	metrics.Default.ObserveLockWait("gc_mu_write", time.Since(waitStart))
	holdStart := time.Now()
	return func() {
		metrics.Default.ObserveLockHold("gc_mu_write", time.Since(holdStart))
		s.gcMu.Unlock()
	}
}

func (s *ObjectService) PutObject(bucket, key, contentType string, input io.Reader) (*models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("put_object", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

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

	success = true
	return manifest, nil
}

func (s *ObjectService) GetObject(bucket, key string) (io.ReadCloser, *models.ObjectManifest, error) {
	start := time.Now()

	waitStart := time.Now()
	s.gcMu.RLock()
	metrics.Default.ObserveLockWait("gc_mu_read", time.Since(waitStart))
	holdStart := time.Now()

	manifest, err := s.metadata.GetManifest(bucket, key)
	if err != nil {
		metrics.Default.ObserveLockHold("gc_mu_read", time.Since(holdStart))
		s.gcMu.RUnlock()
		metrics.Default.ObserveService("get_object", time.Since(start), false)
		return nil, nil, err
	}
	pr, pw := io.Pipe()

	go func() {
		streamOK := false
		defer func() {
			metrics.Default.ObserveService("get_object", time.Since(start), streamOK)
		}()
		defer metrics.Default.ObserveLockHold("gc_mu_read", time.Since(holdStart))
		defer s.gcMu.RUnlock()
		if err := s.blob.AssembleStream(manifest.Chunks, pw); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if err := pw.Close(); err != nil {
			return
		}
		streamOK = true
	}()
	return pr, manifest, nil
}

func (s *ObjectService) HeadObject(bucket, key string) (models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("head_object", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

	manifest, err := s.metadata.GetManifest(bucket, key)
	if err != nil {
		return models.ObjectManifest{}, err
	}
	success = true
	return *manifest, nil
}

func (s *ObjectService) DeleteObject(bucket, key string) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("delete_object", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()
	err := s.metadata.DeleteManifest(bucket, key)
	success = err == nil
	return err
}

func (s *ObjectService) ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error) {
	unlock := s.acquireGCRLock()
	defer unlock()

	return s.metadata.ListObjects(bucket, prefix)
}

func (s *ObjectService) ForEachObjectFrom(bucket, startKey string, fn func(*models.ObjectManifest) error) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("for_each_object_from", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

	err := s.metadata.ForEachObjectFrom(bucket, startKey, fn)
	success = err == nil
	return err
}

func (s *ObjectService) CreateBucket(bucket string) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("create_bucket", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()
	err := s.metadata.CreateBucket(bucket)
	success = err == nil
	return err
}

func (s *ObjectService) HeadBucket(bucket string) error {
	unlock := s.acquireGCRLock()
	defer unlock()

	_, err := s.metadata.GetBucketManifest(bucket)
	return err
}

func (s *ObjectService) GetBucketManifest(bucket string) (*models.BucketManifest, error) {
	unlock := s.acquireGCRLock()
	defer unlock()

	return s.metadata.GetBucketManifest(bucket)
}

func (s *ObjectService) DeleteBucket(bucket string) error {
	unlock := s.acquireGCRLock()
	defer unlock()
	return s.metadata.DeleteBucket(bucket)
}

func (s *ObjectService) ListBuckets() ([]string, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("list_buckets", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

	buckets, err := s.metadata.ListBuckets()
	success = err == nil
	return buckets, err
}

func (s *ObjectService) DeleteObjects(bucket string, keys []string) ([]string, error) {
	unlock := s.acquireGCRLock()
	defer unlock()
	return s.metadata.DeleteManifests(bucket, keys)
}

func (s *ObjectService) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	unlock := s.acquireGCRLock()
	defer unlock()
	return s.metadata.CreateMultipartUpload(bucket, key)
}

func (s *ObjectService) UploadPart(bucket, key, uploadId string, partNumber int, input io.Reader) (string, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("upload_part", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

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
	success = true
	return etag, nil
}

func (s *ObjectService) ListMultipartParts(bucket, key, uploadID string) ([]models.UploadedPart, error) {
	unlock := s.acquireGCRLock()
	defer unlock()

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
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("complete_multipart_upload", time.Since(start), success)
	}()

	unlock := s.acquireGCRLock()
	defer unlock()

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

	success = true
	return manifest, nil
}

func (s *ObjectService) AbortMultipartUpload(bucket, key, uploadID string) error {
	unlock := s.acquireGCRLock()
	defer unlock()

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
	start := time.Now()
	success := false
	deletedChunks := 0
	deleteErrors := 0
	cleanedUploads := 0
	defer func() {
		metrics.Default.ObserveGC(time.Since(start), deletedChunks, deleteErrors, cleanedUploads, success)
	}()

	unlock := s.acquireGCLock()
	defer unlock()

	referencedChunkSet, err := s.metadata.GetReferencedChunkSet()
	if err != nil {
		return err
	}

	totalChunks := 0

	if err := s.blob.ForEachChunk(func(chunkID string) error {
		totalChunks++
		if _, found := referencedChunkSet[chunkID]; found {
			return nil
		}
		if err := s.blob.DeleteBlob(chunkID); err != nil {
			deleteErrors++
			slog.Warn("garbage_collect_delete_failed", "chunk_id", chunkID, "error", err)
			return nil
		}
		deletedChunks++
		return nil
	}); err != nil {
		return err
	}

	cleanedUploads, err = s.metadata.CleanupMultipartUploads(s.multipartRetention)
	if err != nil {
		return err
	}

	slog.Info("garbage_collect_completed",
		"referenced_chunks", len(referencedChunkSet),
		"total_chunks", totalChunks,
		"deleted_chunks", deletedChunks,
		"delete_errors", deleteErrors,
		"cleaned_uploads", cleanedUploads,
	)
	success = true
	return nil
}

func (s *ObjectService) RunGC(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		slog.Warn("garbage_collect_disabled_invalid_interval", "interval", interval.String())
		return
	}

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
