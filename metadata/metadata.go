package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"fs/models"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"go.etcd.io/bbolt"
)

type MetadataHandler struct {
	db *bbolt.DB
}

var systemIndex = []byte("__SYSTEM_BUCKETS__")
var multipartUploadIndex = []byte("__MULTIPART_UPLOADS__")
var multipartUploadPartsIndex = []byte("__MULTIPART_UPLOAD_PARTS__")

var validBucketName = regexp.MustCompile(`^[a-z0-9.-]{3,63}$`)

var (
	ErrInvalidBucketName   = errors.New("invalid bucket name")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrObjectNotFound      = errors.New("object not found")
	ErrMultipartNotFound   = errors.New("multipart upload not found")
	ErrMultipartNotPending = errors.New("multipart upload is not pending")
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
	err = h.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(multipartUploadIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(multipartUploadPartsIndex)
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

func (h *MetadataHandler) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	var upload *models.MultipartUpload

	err := h.db.View(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		if systemIndexBucket.Get([]byte(bucket)) != nil {
			return nil
		}
		return ErrBucketNotFound
	})
	if err != nil {
		return nil, err
	}
	uploadId := uuid.New().String()
	createdAt := time.Now().UTC().Format(time.RFC3339)
	upload = &models.MultipartUpload{
		Bucket:    bucket,
		Key:       key,
		UploadID:  uploadId,
		CreatedAt: createdAt,
		State:     "pending",
	}

	err = h.db.Update(func(tx *bbolt.Tx) error {
		multipartUploadBucket := tx.Bucket([]byte(multipartUploadIndex))
		if multipartUploadBucket == nil {
			return errors.New("multipart upload index not found")
		}
		payload, err := json.Marshal(upload)
		if err != nil {
			return err
		}
		err = multipartUploadBucket.Put([]byte(uploadId), payload)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return upload, nil
}

func getMultipartUploadBucket(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	multipartUploadBucket := tx.Bucket(multipartUploadIndex)
	if multipartUploadBucket == nil {
		return nil, errors.New("multipart upload index not found")
	}
	return multipartUploadBucket, nil
}

func getMultipartPartsBucket(tx *bbolt.Tx) (*bbolt.Bucket, error) {
	multipartPartsBucket := tx.Bucket(multipartUploadPartsIndex)
	if multipartPartsBucket == nil {
		return nil, errors.New("multipart upload parts index not found")
	}
	return multipartPartsBucket, nil
}

func getMultipartUploadFromBucket(multipartUploadBucket *bbolt.Bucket, uploadID string) (*models.MultipartUpload, error) {
	payload := multipartUploadBucket.Get([]byte(uploadID))
	if payload == nil {
		return nil, fmt.Errorf("%w: %s", ErrMultipartNotFound, uploadID)
	}
	upload := models.MultipartUpload{}
	if err := json.Unmarshal(payload, &upload); err != nil {
		return nil, err
	}
	return &upload, nil
}

func getMultipartUploadFromTx(tx *bbolt.Tx, uploadID string) (*models.MultipartUpload, *bbolt.Bucket, error) {
	multipartUploadBucket, err := getMultipartUploadBucket(tx)
	if err != nil {
		return nil, nil, err
	}
	upload, err := getMultipartUploadFromBucket(multipartUploadBucket, uploadID)
	if err != nil {
		return nil, nil, err
	}
	return upload, multipartUploadBucket, nil
}

func putMultipartUpload(multipartUploadBucket *bbolt.Bucket, uploadID string, upload *models.MultipartUpload) error {
	payload, err := json.Marshal(upload)
	if err != nil {
		return err
	}
	return multipartUploadBucket.Put([]byte(uploadID), payload)
}

func deleteMultipartPartsByUploadID(tx *bbolt.Tx, uploadID string) error {
	multipartPartsBucket, err := getMultipartPartsBucket(tx)
	if err != nil {
		return err
	}

	prefix := uploadID + ":"
	cursor := multipartPartsBucket.Cursor()
	keysToDelete := make([][]byte, 0)
	for k, _ := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, _ = cursor.Next() {
		keyCopy := make([]byte, len(k))
		copy(keyCopy, k)
		keysToDelete = append(keysToDelete, keyCopy)
	}
	for _, key := range keysToDelete {
		if err := multipartPartsBucket.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (h *MetadataHandler) GetMultipartUpload(uploadID string) (*models.MultipartUpload, error) {
	var upload *models.MultipartUpload
	err := h.db.View(func(tx *bbolt.Tx) error {
		var err error
		upload, _, err = getMultipartUploadFromTx(tx, uploadID)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return upload, nil
}
func (h *MetadataHandler) PutMultipartPart(uploadID string, part models.UploadedPart) error {
	if part.PartNumber < 1 || part.PartNumber > 10000 {
		return fmt.Errorf("invalid part number: %d", part.PartNumber)
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		upload, _, err := getMultipartUploadFromTx(tx, uploadID)
		if err != nil {
			return err
		}
		if upload.State != "pending" {
			return fmt.Errorf("%w: %s", ErrMultipartNotPending, uploadID)
		}

		multipartPartsBucket, err := getMultipartPartsBucket(tx)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("%s:%05d", uploadID, part.PartNumber)
		payload, err := json.Marshal(part)
		if err != nil {
			return err
		}
		return multipartPartsBucket.Put([]byte(key), payload)
	})
	if err != nil {
		return err
	}
	return nil
}
func (h *MetadataHandler) ListMultipartParts(uploadID string) ([]models.UploadedPart, error) {
	parts := make([]models.UploadedPart, 0)

	err := h.db.View(func(tx *bbolt.Tx) error {
		if _, _, err := getMultipartUploadFromTx(tx, uploadID); err != nil {
			return err
		}

		multipartPartsBucket, err := getMultipartPartsBucket(tx)
		if err != nil {
			return err
		}
		prefix := uploadID + ":"
		cursor := multipartPartsBucket.Cursor()
		for k, v := cursor.Seek([]byte(prefix)); k != nil && strings.HasPrefix(string(k), prefix); k, v = cursor.Next() {
			part := models.UploadedPart{}
			if err := json.Unmarshal(v, &part); err != nil {
				return err
			}
			parts = append(parts, part)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
}
func (h *MetadataHandler) CompleteMultipartUpload(uploadID string, final *models.ObjectManifest) error {
	if final == nil {
		return errors.New("final object manifest is required")
	}

	err := h.db.Update(func(tx *bbolt.Tx) error {
		upload, multipartUploadBucket, err := getMultipartUploadFromTx(tx, uploadID)
		if err != nil {
			return err
		}
		if upload.State != "pending" {
			return fmt.Errorf("%w: %s", ErrMultipartNotPending, uploadID)
		}

		metadataBucket := tx.Bucket([]byte(upload.Bucket))
		if metadataBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, upload.Bucket)
		}
		final.Bucket = upload.Bucket
		final.Key = upload.Key
		finalPayload, err := json.Marshal(final)
		if err != nil {
			return err
		}
		if err := metadataBucket.Put([]byte(upload.Key), finalPayload); err != nil {
			return err
		}

		upload.State = "completed"
		if err := putMultipartUpload(multipartUploadBucket, uploadID, upload); err != nil {
			return err
		}

		if err := deleteMultipartPartsByUploadID(tx, uploadID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
func (h *MetadataHandler) AbortMultipartUpload(uploadID string) error {
	err := h.db.Update(func(tx *bbolt.Tx) error {
		upload, multipartUploadBucket, err := getMultipartUploadFromTx(tx, uploadID)
		if err != nil {
			return err
		}
		if upload.State == "completed" {
			return fmt.Errorf("%w: %s", ErrMultipartNotPending, uploadID)
		}
		upload.State = "aborted"
		if err := putMultipartUpload(multipartUploadBucket, uploadID, upload); err != nil {
			return err
		}

		if err := deleteMultipartPartsByUploadID(tx, uploadID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
