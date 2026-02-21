package service

import (
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"io"
	"strings"
	"time"
)

type ObjectService struct {
	metadataHandler *metadata.MetadataHandler
}

func NewObjectService(metadataHandler *metadata.MetadataHandler) *ObjectService {
	return &ObjectService{metadataHandler: metadataHandler}
}

func (s *ObjectService) PutObject(uri string, contentType string, input io.Reader) (*models.ObjectManifest, error) {

	bucket := strings.Split(uri, "/")[0]
	key := strings.Join(strings.Split(uri, "/")[1:], "/")

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
		defer pw.Close()

		err := storage.AssembleStream(manifest.Chunks, pw)
		if err != nil {
			return
		}
	}()
	return pr, manifest, nil
}
