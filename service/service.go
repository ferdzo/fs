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
	chunkPins          *chunkPinRegistry
	gcRunMu            sync.Mutex
	gcGrace            time.Duration

	// Narrow, unexported test hooks used only to make ordering races
	// deterministic in tests. They are nil in production.
	testHookAfterGCSnapshot  func()
	testHookCopyAfterPin     func()
	testHookCompleteAfterPin func()
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

// chunkPinRegistry tracks chunk sets actively needed by in-flight operations so
// the garbage collector never unlinks a chunk that a reader or writer still
// depends on. Pins are refcounted; each per-operation lease deduplicates IDs so
// repeated chunk IDs in one stream count once and release exactly once.
type chunkPinRegistry struct {
	mu     sync.Mutex
	next   uint64
	counts map[string]int
	leases map[uint64]*chunkLease
	epoch  *gcEpoch
}

type chunkLease struct {
	registry *chunkPinRegistry
	id       uint64
	write    bool
	held     map[string]struct{}
	released bool
}

// gcEpoch records chunk IDs that a write lease pinned while a GC pass was in
// flight. Entries are retained until the pass ends even if the writer releases,
// so a chunk written after the metadata snapshot cannot be unlinked on this
// same pass after its manifest becomes visible.
type gcEpoch struct {
	born map[string]struct{}
}

func newChunkPinRegistry() *chunkPinRegistry {
	return &chunkPinRegistry{
		counts: map[string]int{},
		leases: map[uint64]*chunkLease{},
	}
}

func (r *chunkPinRegistry) newLease(write bool) *chunkLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.newLeaseLocked(write)
}

func (r *chunkPinRegistry) newLeaseLocked(write bool) *chunkLease {
	lease := &chunkLease{
		registry: r,
		id:       r.next,
		write:    write,
		held:     map[string]struct{}{},
	}
	r.next++
	r.leases[lease.id] = lease
	return lease
}

func (l *chunkLease) Pin(chunkID string) {
	l.registry.mu.Lock()
	defer l.registry.mu.Unlock()
	l.pinLocked(chunkID)
}

func (l *chunkLease) pinAllLocked(chunkIDs []string) {
	if l.released {
		return
	}
	for _, chunkID := range chunkIDs {
		l.pinLocked(chunkID)
	}
}

func (l *chunkLease) pinLocked(chunkID string) {
	if l.released {
		return
	}
	if _, ok := l.held[chunkID]; ok {
		return
	}
	l.held[chunkID] = struct{}{}
	l.registry.counts[chunkID]++
	if l.write && l.registry.epoch != nil {
		l.registry.epoch.born[chunkID] = struct{}{}
	}
}

func (l *chunkLease) Release() {
	r := l.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	for chunkID := range l.held {
		if count := r.counts[chunkID]; count <= 1 {
			delete(r.counts, chunkID)
		} else {
			r.counts[chunkID] = count - 1
		}
	}
	delete(r.leases, l.id)
	l.held = nil
}

func (r *chunkPinRegistry) beginEpochLocked() *gcEpoch {
	epoch := &gcEpoch{born: map[string]struct{}{}}
	for _, lease := range r.leases {
		if !lease.write {
			continue
		}
		for chunkID := range lease.held {
			epoch.born[chunkID] = struct{}{}
		}
	}
	r.epoch = epoch
	return epoch
}

func (r *chunkPinRegistry) endEpoch(epoch *gcEpoch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.epoch == epoch {
		r.epoch = nil
	}
}

func (r *chunkPinRegistry) protectedLocked(epoch *gcEpoch, chunkID string) bool {
	if _, ok := epoch.born[chunkID]; ok {
		return true
	}
	return r.counts[chunkID] > 0
}

// pinCount reports the total refcount across leases. It is unexported and used
// by focused tests to prove leases are released on every exit path.
func (r *chunkPinRegistry) pinCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, count := range r.counts {
		total += count
	}
	return total
}

func (s *ObjectService) PutObject(bucket, key, contentType string, input io.Reader) (*models.ObjectManifest, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.Default.ObserveService("put_object", time.Since(start), success)
	}()

	writeLease := s.chunkPins.newLease(true)
	defer writeLease.Release()

	chunks, size, etag, err := s.blob.IngestStreamWithPin(s.limitUpload(input), func(chunkID string) error {
		writeLease.Pin(chunkID)
		return nil
	})
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

	s.chunkPins.mu.Lock()
	writeLease := s.chunkPins.newLeaseLocked(true)
	source, err := s.metadata.GetManifest(srcBucket, srcKey)
	if err == nil {
		writeLease.pinAllLocked(source.Chunks)
	}
	s.chunkPins.mu.Unlock()
	if err != nil {
		writeLease.Release()
		return nil, err
	}
	defer writeLease.Release()
	if s.testHookCopyAfterPin != nil {
		s.testHookCopyAfterPin()
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

	// Snapshot the manifest and pin its chunks under the registry mutex so a GC
	// final check either observes this read lease or runs after it. The registry
	// mutex is released before any pipe I/O.
	waitStart := time.Now()
	s.chunkPins.mu.Lock()
	manifest, mErr := s.metadata.GetManifest(bucket, key)
	var readLease *chunkLease
	if mErr == nil {
		readLease = s.chunkPins.newLeaseLocked(false)
		readLease.pinAllLocked(manifest.Chunks)
	}
	s.chunkPins.mu.Unlock()
	metrics.Default.ObserveLockWait("gc_mu_read", waitStart.Sub(start))
	metrics.Default.ObserveLockHold("gc_mu_read", time.Since(waitStart))

	if mErr != nil {
		metrics.Default.ObserveService("get_object", time.Since(start), false)
		return nil, nil, mErr
	}
	release := readLease.Release

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
			if r := recover(); r != nil {
				_ = pw.CloseWithError(fmt.Errorf("get-object-stream panic: %v", r))
				release()
				metrics.Default.ObserveService("get_object", time.Since(start), false)
				panic(r)
			}
			release()
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
	return &leasedReadCloser{PipeReader: pr, release: release}, manifest, nil
}

// leasedReadCloser releases the stream's read lease when the client closes the
// body, covering clients that abort before the pump finishes. The pump also
// releases on completion/error; chunkLease.Release is idempotent.
type leasedReadCloser struct {
	*io.PipeReader
	release func()
	once    sync.Once
}

func (r *leasedReadCloser) Close() error {
	r.once.Do(r.release)
	return r.PipeReader.Close()
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

	writeLease := s.chunkPins.newLease(true)
	defer writeLease.Release()

	var uploadedPart models.UploadedPart
	chunkIds, totalSize, etag, err := s.blob.IngestStreamWithPin(s.limitUpload(input), func(chunkID string) error {
		writeLease.Pin(chunkID)
		return nil
	})
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

	s.chunkPins.mu.Lock()
	writeLease := s.chunkPins.newLeaseLocked(true)
	storedParts, err := s.metadata.ListMultipartParts(uploadID)
	if err == nil {
		for _, part := range storedParts {
			writeLease.pinAllLocked(part.Chunks)
		}
	}
	s.chunkPins.mu.Unlock()
	if err != nil {
		writeLease.Release()
		return nil, err
	}
	defer writeLease.Release()
	if s.testHookCompleteAfterPin != nil {
		s.testHookCompleteAfterPin()
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

	s.gcRunMu.Lock()
	defer s.gcRunMu.Unlock()

	var err error
	if cleanedUploads, err = s.metadata.CleanupMultipartUploads(s.multipartRetention); err != nil {
		return err
	}

	// Start the epoch before the metadata scan. Live write leases are seeded so
	// chunks they already pinned survive this pass even though their manifests
	// are not visible yet; later write pins add themselves to the same epoch.
	s.chunkPins.mu.Lock()
	epoch := s.chunkPins.beginEpochLocked()
	s.chunkPins.mu.Unlock()
	defer s.chunkPins.endEpoch(epoch)

	referencedChunkSet, err := s.metadata.GetReferencedChunkSet()
	if err != nil {
		return err
	}

	if s.testHookAfterGCSnapshot != nil {
		s.testHookAfterGCSnapshot()
	}

	totalChunks := 0

	if err := s.blob.ForEachChunk(func(chunkID string) error {
		totalChunks++
		if _, found := referencedChunkSet[chunkID]; found {
			return nil
		}

		// Final born/pin check, Stat, and unlink are atomic with respect to new
		// leases. Once released, a writer/reader must take the same mutex, so it
		// either sees the chunk before deletion or pins it for a later pass.
		s.chunkPins.mu.Lock()
		defer s.chunkPins.mu.Unlock()

		if s.chunkPins.protectedLocked(epoch, chunkID) {
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
