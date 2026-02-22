package service

import (
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"io"
	"time"
)

type ObjectService struct {
	metadataHandler *metadata.MetadataHandler
}

func NewObjectService(metadataHandler *metadata.MetadataHandler) *ObjectService {
	return &ObjectService{metadataHandler: metadataHandler}
}

func (s *ObjectService) PutObject(bucket, key, contentType string, input io.Reader) (*models.ObjectManifest, error) {

	chunks, size, etag, err := storage.IngestStream(input)
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
	fmt.Println(manifest)
	if err = s.metadataHandler.PutManifest(manifest); err != nil {
		return nil, err
	}

	return manifest, nil
}

func (s *ObjectService) GetObject(bucket, key string) (io.ReadCloser, *models.ObjectManifest, error) {
	manifest, err := s.metadataHandler.GetManifest(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	pr, pw := io.Pipe()

	go func() {
		defer func(pw *io.PipeWriter) {
			err := pw.Close()
			if err != nil {

			}
		}(pw)

		err := storage.AssembleStream(manifest.Chunks, pw)
		if err != nil {
			return
		}
	}()
	return pr, manifest, nil
}

func (s *ObjectService) HeadObject(bucket, key string) (models.ObjectManifest, error) {
	manifest, err := s.metadataHandler.GetManifest(bucket, key)
	if err != nil {
		return models.ObjectManifest{}, err
	}
	return *manifest, nil
}

func (s *ObjectService) DeleteObject(bucket, key string) error {
	return s.metadataHandler.DeleteManifest(bucket, key)
}

func (s *ObjectService) ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error) {
	return s.metadataHandler.ListObjects(bucket, prefix)
}

func (s *ObjectService) CreateBucket(bucket string) error {
	return s.metadataHandler.CreateBucket(bucket)
}

func (s *ObjectService) HeadBucket(bucket string) error {
	_, err := s.metadataHandler.GetBucketManifest(bucket)
	return err
}

func (s *ObjectService) DeleteBucket(bucket string) error {
	return s.metadataHandler.DeleteBucket(bucket)
}

func (s *ObjectService) ListBuckets() ([]string, error) {
	return s.metadataHandler.ListBuckets()
}

func (s *ObjectService) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	return s.metadataHandler.CreateMultipartUpload(bucket, key)
}

func (s *ObjectService) PutMultipartObject(bucket, key, uploadId string, input io.Reader) (*models.MultipartUpload, error) {
	return nil, nil
}
