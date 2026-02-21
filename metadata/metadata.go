package metadata

import (
	"encoding/json"
	"fmt"
	"fs/models"

	"go.etcd.io/bbolt"
)

const ManifestBucketName = "object_manifests"

type MetadataHandler struct {
	db *bbolt.DB
}

func NewMetadataHandler(dbPath string) (*MetadataHandler, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	return &MetadataHandler{db: db}, nil
}

func (h *MetadataHandler) PutManifest(manifest *models.ObjectManifest) error {
	err := h.db.Update(func(tx *bbolt.Tx) error {
		metadataBucket, err := tx.CreateBucketIfNotExists([]byte(ManifestBucketName))
		if err != nil {
			return err
		}
		key := fmt.Sprintf("%s/%s", manifest.Bucket, manifest.Key)
		data, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		return metadataBucket.Put([]byte(key), data)
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *MetadataHandler) GetManifest(bucket, key string) (*models.ObjectManifest, error) {
	var manifest *models.ObjectManifest

	err := h.db.View(func(tx *bbolt.Tx) error {
		metadataBucket := tx.Bucket([]byte(ManifestBucketName))
		if metadataBucket == nil {
			return fmt.Errorf("bucket %s not found", ManifestBucketName)
		}
		key := fmt.Sprintf("%s/%s", bucket, key)
		data := metadataBucket.Get([]byte(key))
		if data == nil {

			return fmt.Errorf("manifest not found for bucket %s and key %s", bucket, key)
		}
		err := json.Unmarshal(data, &manifest)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return manifest, nil
}
