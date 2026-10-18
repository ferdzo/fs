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
	"fs/utils"
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
	maxUploadSize      int64
	gcMu               sync.RWMutex
	chunkPins          *chunkPinRegistry
	gcGrace            time.Duration
}

var (
	ErrChunkMissing           = errors.New("chunk file missing from blob store")
	ErrInvalidPart            = errors.New("invalid multipart part")
	ErrInvalidPartOrder       = errors.New("invalid multipart part order")
	ErrInvalidCompleteRequest = errors.New("invalid complete multipart request")
	ErrEntityTooSmall         = errors.New("multipart entity too small")
	ErrEntityTooLarge         = errors.New("entity too large")
)

const DefaultMaxUploadSize int64 = 5 * 1024 * 1024 * 1024

func NewObjectService(metadataHandler *metadata.MetadataHandler, blobHandler *storage.BlobStore, multipartRetention time.Duration, maxUploadSize ...int64) *ObjectService {
	if multipartRetention <= 0 {
		multipartRetention = 24 * time.Hour
	}
	limit := DefaultMaxUploadSize
	if len(maxUploadSize) > 0 {
		limit = maxUploadSize[0]
	}
	return &ObjectService{
		metadata:           metadataHandler,
		blob:               blobHandler,
		multipartRetention: multipartRetention,
		maxUploadSize:      limit,
		chunkPins:          newChunkPinRegistry(),
		gcGrace:            15 * time.Minute,
	}
}

// chunkPinRegistry tracks chunk sets actively being streamed so the garbage
// collector never deletes data mid-read. Pins are registered under the
// manifest lock and released when the response completes.
type chunkPinRegistry struct {
	mu   sync.Mutex
	next int64
	pins map[int64]map[string]struct{}
}

func newChunkPinRegistry() *chunkPinRegistry {
	return &chunkPinRegistry{pins: map[int64]map[string]struct{}{}}
}

func (r *chunkPinRegistry) add(chunkIDs []string) (int64, func()) {
	set := make(map[string]struct{}, len(chunkIDs))
	for _, id := range chunkIDs {
		set[id] = struct{}{}
	}
	r.mu.Lock()
	id := r.next
	r.next++
	r.pins[id] = set
	r.mu.Unlock()
	release := func() {
		r.mu.Lock()
		delete(r.pins, id)
		r.mu.Unlock()
	}
	return id, release
}

func (r *chunkPinRegistry) isPinned(chunkID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, set := range r.pins {
		if _, ok := set[chunkID]; ok {
			return true
		}
	}
	return false
}

func (s *ObjectService) PutObject(bucket, key, contentType string, input io.Reader) (*models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("put_object", time.Since(start), success)
	}()

	chunks, size, etag, err := s.blob.IngestStream(s.limitUpload(input))
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

func (s *ObjectService) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("copy_object", time.Since(start), success)
	}()

	source, err := s.metadata.GetManifest(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}

	manifest := &models.ObjectManifest{
		Bucket:      dstBucket,
		Key:         dstKey,
		Size:        source.Size,
		ContentType: source.ContentType,
		ETag:        source.ETag,
		Chunks:      append([]string(nil), source.Chunks...),
		CreatedAt:   time.Now().Unix(),
	}
	if err := s.metadata.PutManifest(manifest); err != nil {
		return nil, err
	}

	success = true
	return manifest, nil
}

func (s *ObjectService) GetObject(bucket, key string) (io.ReadCloser, *models.ObjectManifest, error) {
	start := time.Now()

	waitStart := time.Now()
	s.gcMu.RLock()
	manifest, mErr := s.metadata.GetManifest(bucket, key)
	var release func()
	if mErr == nil {
		_, release = s.chunkPins.add(manifest.Chunks)
	}
	s.gcMu.RUnlock()
	metrics.Default.ObserveLockWait("gc_mu_read", waitStart.Sub(start))
	metrics.Default.ObserveLockHold("gc_mu_read", time.Since(waitStart))

	if mErr != nil {
		metrics.Default.ObserveService("get_object", time.Since(start), false)
		return nil, nil, mErr
	}
	defer release()

	// Pre-flight: fail before a 200 header is committed when any referenced
	// chunk is missing on disk. In-chunk bitrot still surfaces mid-stream via
	// the reader's sha256 check; absent files are the silent-truncation class
	// this pass eliminates.
	for _, id := range manifest.Chunks {
		if _, ok := s.blob.StatChunk(id); !ok {
			release()
			err := fmt.Errorf("%w: chunk %s", ErrChunkMissing, id)
			metrics.Default.ObserveService("get_object", time.Since(start), false)
			return nil, nil, err
		}
	}

	pr, pw := io.Pipe()
	utils.Go("get-object-stream", func() {
		streamOK := false
		defer func() {
			metrics.Default.ObserveService("get_object", time.Since(start), streamOK)
		}()
		if err := s.blob.AssembleStream(manifest.Chunks, pw); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if err := pw.Close(); err != nil {
			return
		}
		streamOK = true
	})
	return pr, manifest, nil
}

func (s *ObjectService) HeadObject(bucket, key string) (models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("head_object", time.Since(start), success)
	}()

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

	err := s.metadata.DeleteManifest(bucket, key)
	success = err == nil
	return err
}

func (s *ObjectService) ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error) {

	return s.metadata.ListObjects(bucket, prefix)
}

func (s *ObjectService) ForEachObjectFrom(bucket, startKey string, fn func(*models.ObjectManifest) error) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("for_each_object_from", time.Since(start), success)
	}()

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

	err := s.metadata.CreateBucket(bucket)
	success = err == nil
	return err
}

func (s *ObjectService) HeadBucket(bucket string) error {

	_, err := s.metadata.GetBucketManifest(bucket)
	return err
}

func (s *ObjectService) GetBucketManifest(bucket string) (*models.BucketManifest, error) {

	return s.metadata.GetBucketManifest(bucket)
}

func (s *ObjectService) DeleteBucket(bucket string) error {
	return s.metadata.DeleteBucket(bucket)
}

func (s *ObjectService) ListBuckets() ([]string, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("list_buckets", time.Since(start), success)
	}()

	buckets, err := s.metadata.ListBuckets()
	success = err == nil
	return buckets, err
}

func (s *ObjectService) DeleteObjects(bucket string, keys []string) ([]string, error) {
	return s.metadata.DeleteManifests(bucket, keys)
}

func (s *ObjectService) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	return s.metadata.CreateMultipartUpload(bucket, key)
}

func (s *ObjectService) UploadPart(bucket, key, uploadId string, partNumber int, input io.Reader) (string, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("upload_part", time.Since(start), success)
	}()

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
	chunkIds, totalSize, etag, err := s.blob.IngestStream(s.limitUpload(input))
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
		if s.maxUploadSize > 0 && totalSize > s.maxUploadSize {
			return nil, ErrEntityTooLarge
		}
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

	upload, err := s.metadata.GetMultipartUpload(uploadID)
	if err != nil {
		return err
	}
	if upload.Bucket != bucket || upload.Key != key {
		return metadata.ErrMultipartNotFound
	}
	return s.metadata.AbortMultipartUpload(uploadID)
}

func (s *ObjectService) limitUpload(input io.Reader) io.Reader {
	if s.maxUploadSize <= 0 || input == nil {
		return input
	}
	return &maxBytesReader{inner: input, remaining: s.maxUploadSize}
}

type maxBytesReader struct {
	inner     io.Reader
	remaining int64
	tooLarge  bool
}

func (r *maxBytesReader) Read(p []byte) (int, error) {
	if r.tooLarge {
		return 0, ErrEntityTooLarge
	}
	if r.remaining <= 0 {
		var probe [1]byte
		n, err := r.inner.Read(probe[:])
		if n > 0 {
			r.tooLarge = true
			return 0, ErrEntityTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.inner.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(etag, "\""))
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
	skippedPinned := 0
	skippedFresh := 0
	defer func() {
		metrics.Default.ObserveGC(time.Since(start), deletedChunks, deleteErrors, cleanedUploads, success)
	}()

	var err error
	cleanedUploads, err = s.metadata.CleanupMultipartUploads(s.multipartRetention)
	if err != nil {
		return err
	}

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
		if s.chunkPins.isPinned(chunkID) {
			skippedPinned++
			return nil
		}
		if mt, ok := s.blob.StatChunk(chunkID); !ok || time.Since(mt) < s.gcGrace {
			skippedFresh++
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

	slog.Info("garbage_collect_completed",
		"referenced_chunks", len(referencedChunkSet),
		"total_chunks", totalChunks,
		"deleted_chunks", deletedChunks,
		"delete_errors", deleteErrors,
		"cleaned_uploads", cleanedUploads,
		"skipped_pinned", skippedPinned,
		"skipped_fresh", skippedFresh,
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
