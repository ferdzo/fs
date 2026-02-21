package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"fs/models"
	"regexp"
	"time"

	"go.etcd.io/bbolt"
)

type MetadataHandler struct {
	db *bbolt.DB
}

var systemIndex = []byte("__SYSTEM_BUCKETS__")

var validBucketName = regexp.MustCompile(`^[a-z0-9.-]{3,63}$`)

func NewMetadataHandler(dbPath string) (*MetadataHandler, error) {
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	h := &MetadataHandler{db: db}

	err = h.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(systemIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return h, nil
}

func (h *MetadataHandler) CreateBucket(bucketName string) error {
	if !validBucketName.MatchString(bucketName) {
		return fmt.Errorf("invalid bucket name: %s", bucketName)
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		indexBucket, err := tx.CreateBucketIfNotExists([]byte(systemIndex))
		if err != nil {
			return err
		}
		if indexBucket.Get([]byte(bucketName)) != nil {
			return fmt.Errorf("bucket %s already exists", bucketName)
		}

		_, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		manifest := models.BucketManifest{
			Name:      bucketName,
			CreatedAt: time.Now(),
		}
		data, _ := json.Marshal(manifest)

		return indexBucket.Put([]byte(bucketName), data)
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *MetadataHandler) DeleteBucket(bucketName string) error {
	if !validBucketName.MatchString(bucketName) {
		return fmt.Errorf("invalid bucket name: %s", bucketName)
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		indexBucket, err := tx.CreateBucketIfNotExists([]byte(systemIndex))
		if err != nil {
			return err
		}
		if indexBucket.Get([]byte(bucketName)) == nil {
			return fmt.Errorf("bucket %s not found", bucketName)
		}
		if err := tx.DeleteBucket([]byte(bucketName)); err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
			return fmt.Errorf("error deleting metadata bucket %s: %w", bucketName, err)
		}
		if err := indexBucket.Delete([]byte(bucketName)); err != nil {
			return fmt.Errorf("error deleting bucket %s from system index: %w", bucketName, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *MetadataHandler) ListBuckets() ([]string, error) {
	buckets := []string{}
	err := h.db.View(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		c := systemIndexBucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			buckets = append(buckets, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buckets, nil
}

func (h *MetadataHandler) GetBucketManifest(bucketName string) (*models.BucketManifest, error) {
	var manifest *models.BucketManifest

	err := h.db.View(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		data := systemIndexBucket.Get([]byte(bucketName))
		if data == nil {
			return fmt.Errorf("bucket manifest not found for bucket %s", bucketName)
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

func (h *MetadataHandler) PutManifest(manifest *models.ObjectManifest) error {
	bucket := manifest.Bucket
	key := manifest.Key

	if _, err := h.GetBucketManifest(bucket); err != nil {
		return err
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		data, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		metadataBucket := tx.Bucket([]byte(bucket))
		if metadataBucket == nil {
			return fmt.Errorf("metadata bucket %s not found; create it first", bucket)
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
		metadataBucket := tx.Bucket([]byte(bucket))
		if metadataBucket == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
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

func (h *MetadataHandler) ListObjects(bucket, prefix string) ([]*models.ObjectManifest, error) {

	var objects []*models.ObjectManifest

	err := h.db.View(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		if systemIndexBucket.Get([]byte(bucket)) == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		_bucket := tx.Bucket([]byte(bucket))
		if _bucket == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		err := _bucket.ForEach(func(k, v []byte) error {
			object := models.ObjectManifest{}
			err := json.Unmarshal(v, &object)
			if err != nil {
				return err
			}
			objects = append(objects, &object)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}
