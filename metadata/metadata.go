package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"fs/models"
	"regexp"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

type MetadataHandler struct {
	db *bbolt.DB
}

var systemIndex = []byte("__SYSTEM_BUCKETS__")

var validBucketName = regexp.MustCompile(`^[a-z0-9.-]{3,63}$`)

var (
	ErrInvalidBucketName   = errors.New("invalid bucket name")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrObjectNotFound      = errors.New("object not found")
)

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
		return fmt.Errorf("%w: %s", ErrInvalidBucketName, bucketName)
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		indexBucket, err := tx.CreateBucketIfNotExists([]byte(systemIndex))
		if err != nil {
			return err
		}
		if indexBucket.Get([]byte(bucketName)) != nil {
			return fmt.Errorf("%w: %s", ErrBucketAlreadyExists, bucketName)
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
		return fmt.Errorf("%w: %s", ErrInvalidBucketName, bucketName)
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		indexBucket, err := tx.CreateBucketIfNotExists([]byte(systemIndex))
		if err != nil {
			return err
		}
		if indexBucket.Get([]byte(bucketName)) == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
		}
		metadataBucket := tx.Bucket([]byte(bucketName))
		if metadataBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
		}
		if k, _ := metadataBucket.Cursor().First(); k != nil {
			return fmt.Errorf("%w: %s", ErrBucketNotEmpty, bucketName)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucketName)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		data := metadataBucket.Get([]byte(key))
		if data == nil {

			return fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
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
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		_bucket := tx.Bucket([]byte(bucket))
		if _bucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		err := _bucket.ForEach(func(k, v []byte) error {
			if prefix != "" && !strings.HasPrefix(string(k), prefix) {
				return nil
			}
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

func (h *MetadataHandler) DeleteManifest(bucket, key string) error {
	if _, err := h.GetManifest(bucket, key); err != nil {
		return err
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		metadataBucket := tx.Bucket([]byte(bucket))
		if metadataBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		return metadataBucket.Delete([]byte(key))
	})
	if err != nil {
		return err
	}
	return nil

}
