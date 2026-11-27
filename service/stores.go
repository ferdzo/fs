package service

import (
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"io"
	"time"
)

// MetadataStore defines the metadata persistence contract used by ObjectService.
type MetadataStore interface {
	CreateBucket(bucketName string, createdAt time.Time) error
	DeleteBucket(bucketName string) error
	ListBuckets() ([]string, error)
	GetBucketManifest(bucketName string) (*models.BucketManifest, error)
	GetBucketVersioning(bucket string) (string, error)
	SetBucketVersioning(bucket, status string) error

	GetManifest(bucket, key string) (*models.ObjectManifest, error)
	GetManifestVersion(bucket, key, versionID string) (*models.ObjectManifest, error)
	PutObjectVersionConditional(manifest *models.ObjectManifest, ifNoneMatch, versionID string, now time.Time) (string, error)
	DeleteObject(bucket, key, versionID, deleteMarkerID string, now time.Time) (metadata.DeleteResult, error)
	DeleteObjects(bucket string, specs []metadata.DeleteObjectSpec, now time.Time) ([]metadata.BatchDeleteResult, error)
	DeleteManifests(bucket string, keys []string, now time.Time) ([]string, error)
	ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error)
	ForEachObjectKeyFrom(bucket, startKey string, fn func(key string, decode func() (*models.ObjectManifest, error)) error) error
	ListObjectVersions(bucket, prefix, delimiter, keyMarker, versionIDMarker string, maxKeys int) (*metadata.ObjectVersionsPage, error)

	CreateMultipartUpload(bucket, key, uploadID, contentType string, createdAt time.Time) (*models.MultipartUpload, error)
	GetMultipartUpload(uploadID string) (*models.MultipartUpload, error)
	PutMultipartPart(uploadID string, part models.UploadedPart, now time.Time) error
	ListMultipartParts(uploadID string) ([]models.UploadedPart, error)
	ListMultipartUploads(bucket, prefix, delimiter, keyMarker, uploadIDMarker string, maxUploads int) (*metadata.MultipartUploadsPage, error)
	CompleteMultipartUpload(uploadID string, final *models.ObjectManifest, versionID string, now time.Time) error
	AbortMultipartUpload(uploadID string, now time.Time) error
	CleanupMultipartUploads(retention time.Duration, now time.Time) (int, error)

	ChunkRefCount(chunkID string) (uint64, error)
	EnqueueUnreferencedChunks(chunkIDs []string, now time.Time) error
	ListGCQueue(limit int) ([]string, error)
	RemoveGCQueueEntries(chunkIDs []string) error
	Close() error
}

// ChunkStore defines the content-addressed chunk storage contract used by ObjectService.
type ChunkStore interface {
	IngestStreamWithSizes(stream io.Reader, onChunk func(chunkID string) error) ([]string, []int64, int64, string, error)
	AssembleStream(chunkIDs []string, w *io.PipeWriter) error
	GetBlob(chunkID string) ([]byte, error)
	DeleteBlob(chunkID string) error
	StatChunk(chunkID string) (time.Time, bool)
	RemoveStaleTempFiles(olderThan time.Time) (int, error)
}

var (
	_ MetadataStore = (*metadata.MetadataHandler)(nil)
	_ ChunkStore    = (*storage.BlobStore)(nil)
)
