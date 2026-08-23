package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"fs/metrics"
	"fs/models"
	"net"
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
var authIdentitiesIndex = []byte("__AUTH_IDENTITIES__")
var authPoliciesIndex = []byte("__AUTH_POLICIES__")

var validBucketName = regexp.MustCompile(`^[a-z0-9.-]+$`)

var (
	ErrInvalidBucketName    = errors.New("invalid bucket name")
	ErrBucketAlreadyExists  = errors.New("bucket already exists")
	ErrBucketNotFound       = errors.New("bucket not found")
	ErrBucketNotEmpty       = errors.New("bucket not empty")
	ErrObjectNotFound       = errors.New("object not found")
	ErrMultipartNotFound    = errors.New("multipart upload not found")
	ErrMultipartNotPending  = errors.New("multipart upload is not pending")
	ErrAuthIdentityNotFound = errors.New("auth identity not found")
	ErrAuthPolicyNotFound   = errors.New("auth policy not found")
)

func NewMetadataHandler(dbPath string) (*MetadataHandler, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	h := &MetadataHandler{db: db}

	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(systemIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(multipartUploadIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(multipartUploadPartsIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(authIdentitiesIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(authPoliciesIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return h, nil
}

func isValidBucketName(bucketName string) bool {
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return false
	}
	if !validBucketName.MatchString(bucketName) {
		return false
	}
	if strings.Contains(bucketName, "..") {
		return false
	}
	if bucketName[0] == '.' || bucketName[0] == '-' || bucketName[len(bucketName)-1] == '.' || bucketName[len(bucketName)-1] == '-' {
		return false
	}
	for _, label := range strings.Split(bucketName, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	if ip := net.ParseIP(bucketName); ip != nil && ip.To4() != nil {
		return false
	}
	return true
}

func (h *MetadataHandler) Close() error {
	return h.db.Close()
}

func (h *MetadataHandler) view(fn func(tx *bbolt.Tx) error) error {
	start := time.Now()
	err := h.db.View(fn)
	metrics.Default.ObserveMetadataTx("view", time.Since(start), err == nil)
	return err
}

func (h *MetadataHandler) update(fn func(tx *bbolt.Tx) error) error {
	start := time.Now()
	err := h.db.Update(fn)
	metrics.Default.ObserveMetadataTx("update", time.Since(start), err == nil)
	return err
}

func (h *MetadataHandler) PutAuthIdentity(identity *models.AuthIdentity) error {
	if identity == nil {
		return errors.New("auth identity is required")
	}
	if strings.TrimSpace(identity.AccessKeyID) == "" {
		return errors.New("access key id is required")
	}
	return h.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authIdentitiesIndex)
		if bucket == nil {
			return errors.New("auth identities index not found")
		}
		payload, err := json.Marshal(identity)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(identity.AccessKeyID), payload)
	})
}

func (h *MetadataHandler) DeleteAuthIdentity(accessKeyID string) error {
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return errors.New("access key id is required")
	}
	return h.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authIdentitiesIndex)
		if bucket == nil {
			return errors.New("auth identities index not found")
		}
		if bucket.Get([]byte(accessKeyID)) == nil {
			return fmt.Errorf("%w: %s", ErrAuthIdentityNotFound, accessKeyID)
		}
		return bucket.Delete([]byte(accessKeyID))
	})
}

func (h *MetadataHandler) GetAuthIdentity(accessKeyID string) (*models.AuthIdentity, error) {
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return nil, errors.New("access key id is required")
	}

	var identity *models.AuthIdentity
	err := h.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authIdentitiesIndex)
		if bucket == nil {
			return errors.New("auth identities index not found")
		}
		payload := bucket.Get([]byte(accessKeyID))
		if payload == nil {
			return fmt.Errorf("%w: %s", ErrAuthIdentityNotFound, accessKeyID)
		}
		record := models.AuthIdentity{}
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		identity = &record
		return nil
	})
	if err != nil {
		return nil, err
	}
	return identity, nil
}

func (h *MetadataHandler) PutAuthPolicy(policy *models.AuthPolicy) error {
	if policy == nil {
		return errors.New("auth policy is required")
	}
	principal := strings.TrimSpace(policy.Principal)
	if principal == "" {
		return errors.New("auth policy principal is required")
	}
	policy.Principal = principal
	return h.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authPoliciesIndex)
		if bucket == nil {
			return errors.New("auth policies index not found")
		}
		payload, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(principal), payload)
	})
}

func (h *MetadataHandler) DeleteAuthPolicy(accessKeyID string) error {
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return errors.New("access key id is required")
	}
	return h.update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authPoliciesIndex)
		if bucket == nil {
			return errors.New("auth policies index not found")
		}
		if bucket.Get([]byte(accessKeyID)) == nil {
			return fmt.Errorf("%w: %s", ErrAuthPolicyNotFound, accessKeyID)
		}
		return bucket.Delete([]byte(accessKeyID))
	})
}

func (h *MetadataHandler) GetAuthPolicy(accessKeyID string) (*models.AuthPolicy, error) {
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return nil, errors.New("access key id is required")
	}

	var policy *models.AuthPolicy
	err := h.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authPoliciesIndex)
		if bucket == nil {
			return errors.New("auth policies index not found")
		}
		payload := bucket.Get([]byte(accessKeyID))
		if payload == nil {
			return fmt.Errorf("%w: %s", ErrAuthPolicyNotFound, accessKeyID)
		}
		record := models.AuthPolicy{}
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		policy = &record
		return nil
	})
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func (h *MetadataHandler) ListAuthIdentities(limit int, after string) ([]models.AuthIdentity, string, error) {
	if limit <= 0 {
		limit = 100
	}
	after = strings.TrimSpace(after)

	identities := make([]models.AuthIdentity, 0, limit)
	nextCursor := ""

	err := h.view(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(authIdentitiesIndex)
		if bucket == nil {
			return errors.New("auth identities index not found")
		}

		cursor := bucket.Cursor()
		var k, v []byte
		if after == "" {
			k, v = cursor.First()
		} else {
			k, v = cursor.Seek([]byte(after))
			if k != nil && string(k) == after {
				k, v = cursor.Next()
			}
		}

		count := 0
		for ; k != nil; k, v = cursor.Next() {
			if count >= limit {
				nextCursor = string(k)
				break
			}
			record := models.AuthIdentity{}
			if err := json.Unmarshal(v, &record); err != nil {
				return err
			}
			identities = append(identities, record)
			count++
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return identities, nextCursor, nil
}

func (h *MetadataHandler) CreateBucket(bucketName string) error {
	if !isValidBucketName(bucketName) {
		return fmt.Errorf("%w: %s", ErrInvalidBucketName, bucketName)
	}

	err := h.update(func(tx *bbolt.Tx) error {
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
		data, err := json.Marshal(manifest)
		if err != nil {
			return err
		}

		return indexBucket.Put([]byte(bucketName), data)
	})
	if err != nil {
		return err
	}
	return nil
}

func (h *MetadataHandler) DeleteBucket(bucketName string) error {
	if !isValidBucketName(bucketName) {
		return fmt.Errorf("%w: %s", ErrInvalidBucketName, bucketName)
	}

	err := h.update(func(tx *bbolt.Tx) error {
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

		multipartUploadsBucket, err := getMultipartUploadBucket(tx)
		if err != nil {
			return err
		}
		cursor := multipartUploadsBucket.Cursor()
		for _, payload := cursor.First(); payload != nil; _, payload = cursor.Next() {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(payload, &upload); err != nil {
				return err
			}
			if upload.Bucket == bucketName && upload.State == "pending" {
				return fmt.Errorf("%w: %s", ErrBucketNotEmpty, bucketName)
			}
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
	err := h.view(func(tx *bbolt.Tx) error {
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

	err := h.view(func(tx *bbolt.Tx) error {
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

	err := h.update(func(tx *bbolt.Tx) error {
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

	err := h.view(func(tx *bbolt.Tx) error {
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

	err := h.view(func(tx *bbolt.Tx) error {
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

func (h *MetadataHandler) ForEachObjectFrom(bucket, startKey string, fn func(*models.ObjectManifest) error) error {
	if fn == nil {
		return errors.New("object callback is required")
	}

	return h.view(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		if systemIndexBucket.Get([]byte(bucket)) == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}

		metadataBucket := tx.Bucket([]byte(bucket))
		if metadataBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}

		cursor := metadataBucket.Cursor()
		var k, v []byte
		if startKey == "" {
			k, v = cursor.First()
		} else {
			k, v = cursor.Seek([]byte(startKey))
		}

		for ; k != nil; k, v = cursor.Next() {
			object := models.ObjectManifest{}
			if err := json.Unmarshal(v, &object); err != nil {
				return err
			}
			if err := fn(&object); err != nil {
				return err
			}
		}
		return nil
	})
}

func (h *MetadataHandler) DeleteManifest(bucket, key string) error {
	if _, err := h.GetManifest(bucket, key); err != nil {
		return err
	}

	err := h.update(func(tx *bbolt.Tx) error {
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

func (h *MetadataHandler) DeleteManifests(bucket string, keys []string) ([]string, error) {
	deleted := make([]string, 0, len(keys))

	err := h.update(func(tx *bbolt.Tx) error {
		metadataBucket := tx.Bucket([]byte(bucket))
		if metadataBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}

		for _, key := range keys {
			if key == "" {
				continue
			}
			if metadataBucket.Get([]byte(key)) != nil {
				if err := metadataBucket.Delete([]byte(key)); err != nil {
					return err
				}
			}
			deleted = append(deleted, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deleted, nil
}

func (h *MetadataHandler) CreateMultipartUpload(bucket, key string) (*models.MultipartUpload, error) {
	var upload *models.MultipartUpload

	err := h.view(func(tx *bbolt.Tx) error {
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

	err = h.update(func(tx *bbolt.Tx) error {
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
	err := h.view(func(tx *bbolt.Tx) error {
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

	err := h.update(func(tx *bbolt.Tx) error {
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

	err := h.view(func(tx *bbolt.Tx) error {
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

	err := h.update(func(tx *bbolt.Tx) error {
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
	err := h.update(func(tx *bbolt.Tx) error {
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

func (h *MetadataHandler) CleanupMultipartUploads(retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}

	cleaned := 0
	err := h.update(func(tx *bbolt.Tx) error {
		uploadsBucket, err := getMultipartUploadBucket(tx)
		if err != nil {
			return err
		}

		now := time.Now().UTC()
		keysToDelete := make([]string, 0)
		if err := uploadsBucket.ForEach(func(k, v []byte) error {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(v, &upload); err != nil {
				keysToDelete = append(keysToDelete, string(k))
				return nil
			}
			createdAt, err := time.Parse(time.RFC3339, upload.CreatedAt)
			if err != nil {
				keysToDelete = append(keysToDelete, string(k))
				return nil
			}
			if now.Sub(createdAt) >= retention {
				keysToDelete = append(keysToDelete, string(k))
			}
			return nil
		}); err != nil {
			return err
		}

		for _, uploadID := range keysToDelete {
			if err := uploadsBucket.Delete([]byte(uploadID)); err != nil {
				return err
			}
			if err := deleteMultipartPartsByUploadID(tx, uploadID); err != nil {
				return err
			}
			cleaned++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return cleaned, nil
}

func (h *MetadataHandler) GetReferencedChunkSet() (map[string]struct{}, error) {
	chunkSet := make(map[string]struct{})
	pendingUploadSet := make(map[string]struct{})

	err := h.view(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket([]byte(systemIndex))
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		c := systemIndexBucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			metadataBucket := tx.Bucket(k)
			if metadataBucket == nil {
				continue
			}
			err := metadataBucket.ForEach(func(k, v []byte) error {
				object := models.ObjectManifest{}
				err := json.Unmarshal(v, &object)
				if err != nil {
					return err
				}
				for _, chunkID := range object.Chunks {
					chunkSet[chunkID] = struct{}{}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		uploadsBucket := tx.Bucket(multipartUploadIndex)
		if uploadsBucket == nil {
			return errors.New("multipart upload index not found")
		}
		if err := uploadsBucket.ForEach(func(k, v []byte) error {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(v, &upload); err != nil {
				return err
			}
			if upload.State == "pending" {
				pendingUploadSet[string(k)] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}

		partsBucket := tx.Bucket(multipartUploadPartsIndex)
		if partsBucket == nil {
			return errors.New("multipart upload parts index not found")
		}
		if err := partsBucket.ForEach(func(k, v []byte) error {
			uploadID, _, ok := strings.Cut(string(k), ":")
			if !ok {
				return nil
			}
			if _, pending := pendingUploadSet[uploadID]; !pending {
				return nil
			}

			part := models.UploadedPart{}
			if err := json.Unmarshal(v, &part); err != nil {
				return err
			}
			for _, chunkID := range part.Chunks {
				chunkSet[chunkID] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return chunkSet, nil
}

func (h *MetadataHandler) GetReferencedChunks() ([]string, error) {
	chunkSet, err := h.GetReferencedChunkSet()
	if err != nil {
		return nil, err
	}

	chunks := make([]string, 0, len(chunkSet))
	for chunkID := range chunkSet {
		chunks = append(chunks, chunkID)
	}
	return chunks, nil

}
