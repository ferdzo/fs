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
var multipartUploadKeyIndex = []byte("__MULTIPART_UPLOADS_BY_KEY__")
var authIdentitiesIndex = []byte("__AUTH_IDENTITIES__")
var authPoliciesIndex = []byte("__AUTH_POLICIES__")

// multipartKeyIndexSeparator joins bucket, key, creation time, and upload ID in
// the forward-scannable multipart upload index. Bucket names and upload IDs
// cannot contain it; object keys in practice do not either.
const multipartKeyIndexSeparator = "\x00"

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

// errIterationStopped is an unexported sentinel used by iteration callbacks to
// end a scan early without it being reported as a failure. Callers must obtain
// it through StopIteration so the identity cannot be forged accidentally.
var errIterationStopped = errors.New("metadata iteration stopped")

// StopIteration asks an iterator to stop and returns nil-like success semantics
// to the caller: the metadata transaction and service metric are recorded as
// successful, and IsIterationStopped reports true.
func StopIteration() error {
	return errIterationStopped
}

// IsIterationStopped reports whether err is the normal early-stop sentinel.
func IsIterationStopped(err error) bool {
	return errors.Is(err, errIterationStopped)
}

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
	err = h.update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(multipartUploadKeyIndex)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	err = h.update(func(tx *bbolt.Tx) error {
		for _, name := range [][]byte{versionIndex, versionOrderIndex, bucketStateIndex} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// Backfill the key-ordered index from records written before it existed.
	// Only pending uploads are listed, so only those need indexing. This runs
	// once per handler start over the (retention-bounded) upload set and is
	// idempotent for already-indexed records.
	err = h.update(func(tx *bbolt.Tx) error {
		uploadsBucket := tx.Bucket(multipartUploadIndex)
		keyIndexBucket := tx.Bucket(multipartUploadKeyIndex)
		if uploadsBucket == nil || keyIndexBucket == nil {
			return errors.New("multipart upload index not found")
		}
		return uploadsBucket.ForEach(func(_, value []byte) error {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(value, &upload); err != nil {
				return nil
			}
			if upload.State != "pending" {
				return nil
			}
			return keyIndexBucket.Put(multipartUploadKeyIndexKey(&upload), []byte{})
		})
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
	// A caller-requested early stop is a normal outcome for paginated scans,
	// not a failed transaction.
	metrics.Default.ObserveMetadataTx("view", time.Since(start), err == nil || errors.Is(err, errIterationStopped))
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
		lastReturnedKey := ""
		for ; k != nil; k, v = cursor.Next() {
			if count >= limit {
				// `after` is exclusive, so hand back the key of the last
				// returned record to let callers resume without gaps.
				nextCursor = lastReturnedKey
				break
			}
			record := models.AuthIdentity{}
			if err := json.Unmarshal(v, &record); err != nil {
				return err
			}
			identities = append(identities, record)
			lastReturnedKey = string(k)
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

		for _, index := range [][]byte{versionIndex, versionOrderIndex} {
			indexBucket := tx.Bucket(index)
			if indexBucket == nil {
				continue
			}
			prefix := []byte(bucketName + versionSeparator)
			cursor := indexBucket.Cursor()
			if k, _ := cursor.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)) {
				return fmt.Errorf("%w: %s", ErrBucketNotEmpty, bucketName)
			}
		}
		if state := tx.Bucket(bucketStateIndex); state != nil {
			if err := state.Delete([]byte(bucketName)); err != nil {
				return err
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
	if manifest == nil {
		return errors.New("object manifest is required")
	}
	bucket := manifest.Bucket
	key := manifest.Key

	if _, err := h.GetBucketManifest(bucket); err != nil {
		return err
	}

	err := h.update(func(tx *bbolt.Tx) error {
		status, err := bucketVersioningTx(tx, bucket)
		if err != nil {
			return err
		}
		if status != "" {
			return errors.New("bucket versioning is enabled: use PutObjectVersion")
		}
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
			if object.DeleteMarker {
				return nil
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

// ForEachObjectKeyFrom iterates object keys in ascending order starting at
// startKey (inclusive), inside a single read transaction. The callback receives
// the raw key and a decode function; decode unmarshals the manifest lazily from
// the value read in this transaction. Callers that only make decisions from the
// key (prefix, delimiter, continuation) never pay for JSON decoding. The decode
// function is only valid during the callback: callers must not retain it or call
// it concurrently. It may be called more than once, each time returning a newly
// decoded manifest. The key string is owned by the caller.
//
// Returning StopIteration from fn stops the scan and is reported as a
// successful transaction. Any other error, including a decode error, aborts the
// scan and is returned unchanged.
func (h *MetadataHandler) ForEachObjectKeyFrom(bucket, startKey string, fn func(key string, decode func() (*models.ObjectManifest, error)) error) error {
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

		// One decoder closure is reused across the scan; value is updated per
		// iteration. Decoding stays inside the View so v remains valid.
		var value []byte
		decode := func() (*models.ObjectManifest, error) {
			object := models.ObjectManifest{}
			if err := json.Unmarshal(value, &object); err != nil {
				return nil, err
			}
			return &object, nil
		}

		for ; k != nil; k, v = cursor.Next() {
			value = v
			if err := fn(string(k), decode); err != nil {
				return err
			}
		}
		return nil
	})
}

// ForEachObjectFrom is the eager compatibility wrapper around
// ForEachObjectKeyFrom: it decodes every visited manifest before invoking fn.
// New hot paths should use ForEachObjectKeyFrom and decode only when needed.
func (h *MetadataHandler) ForEachObjectFrom(bucket, startKey string, fn func(*models.ObjectManifest) error) error {
	if fn == nil {
		return errors.New("object callback is required")
	}
	return h.ForEachObjectKeyFrom(bucket, startKey, func(_ string, decode func() (*models.ObjectManifest, error)) error {
		object, err := decode()
		if err != nil {
			return err
		}
		return fn(object)
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
		keyIndexBucket := tx.Bucket(multipartUploadKeyIndex)
		if keyIndexBucket == nil {
			return errors.New("multipart upload key index not found")
		}
		payload, err := json.Marshal(upload)
		if err != nil {
			return err
		}
		err = multipartUploadBucket.Put([]byte(uploadId), payload)
		if err != nil {
			return err
		}

		return keyIndexBucket.Put(multipartUploadKeyIndexKey(upload), []byte{})
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

// multipartUploadKeyIndexKey builds the forward-scannable index key. Ordering
// is bucket, key, initiation time, then upload ID, which matches S3's listing
// order and gives every record a unique, stable position.
func multipartUploadKeyIndexKey(upload *models.MultipartUpload) []byte {
	return []byte(upload.Bucket + multipartKeyIndexSeparator +
		upload.Key + multipartKeyIndexSeparator +
		upload.CreatedAt + multipartKeyIndexSeparator +
		upload.UploadID)
}

func multipartUploadKeyIndexPrefix(bucket, prefix string) []byte {
	return []byte(bucket + multipartKeyIndexSeparator + prefix)
}

func parseMultipartUploadKeyIndexKey(raw []byte) (bucket, key, createdAt, uploadID string, ok bool) {
	parts := strings.SplitN(string(raw), multipartKeyIndexSeparator, 4)
	if len(parts) != 4 {
		return "", "", "", "", false
	}
	return parts[0], parts[1], parts[2], parts[3], true
}

func deleteMultipartUploadKeyIndexEntry(tx *bbolt.Tx, upload *models.MultipartUpload) error {
	if upload == nil {
		return nil
	}
	keyIndexBucket := tx.Bucket(multipartUploadKeyIndex)
	if keyIndexBucket == nil {
		return nil
	}
	return keyIndexBucket.Delete(multipartUploadKeyIndexKey(upload))
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

// MultipartUploadsPage is one page of ListMultipartUploads output. Uploads and
// CommonPrefixes share the page budget, so callers must render both and must
// not re-page either list independently.
type MultipartUploadsPage struct {
	Uploads            []models.MultipartUpload
	CommonPrefixes     []string
	IsTruncated        bool
	NextKeyMarker      string
	NextUploadIDMarker string
}

// ListMultipartUploads returns pending uploads in bucket ordered by key, then
// initiation time, then upload ID. It scans the key-ordered index forward from
// the requested marker and stops after one page, so the work is bounded by the
// page size rather than the number of uploads in the bucket.
func (h *MetadataHandler) ListMultipartUploads(bucket, prefix, delimiter, keyMarker, uploadIDMarker string, maxUploads int) (*MultipartUploadsPage, error) {
	capacity := maxUploads
	if capacity < 0 {
		capacity = 0
	}
	page := &MultipartUploadsPage{
		Uploads:        make([]models.MultipartUpload, 0, capacity),
		CommonPrefixes: make([]string, 0),
	}

	err := h.view(func(tx *bbolt.Tx) error {
		systemIndexBucket := tx.Bucket(systemIndex)
		if systemIndexBucket == nil {
			return errors.New("system index not found")
		}
		if systemIndexBucket.Get([]byte(bucket)) == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		if maxUploads <= 0 {
			return nil
		}
		uploadsBucket := tx.Bucket(multipartUploadIndex)
		keyIndexBucket := tx.Bucket(multipartUploadKeyIndex)
		if uploadsBucket == nil || keyIndexBucket == nil {
			return errors.New("multipart upload index not found")
		}

		// Markers before the prefix are already excluded by the prefix seek.
		// upload-id-marker is only meaningful together with key-marker.
		startKey := multipartUploadKeyIndexPrefix(bucket, prefix)
		skipKeyMarker := ""
		exactMarkerResume := false
		applyMarker := keyMarker != "" && keyMarker >= prefix
		if applyMarker {
			if uploadIDMarker != "" {
				marker, err := getMultipartUploadFromBucket(uploadsBucket, uploadIDMarker)
				if err == nil && marker.Bucket == bucket && marker.Key == keyMarker {
					// Resume immediately after the marker pair's exact position.
					startKey = multipartUploadKeyIndexKey(marker)
					exactMarkerResume = true
				} else {
					skipKeyMarker = keyMarker
					startKey = multipartUploadKeyIndexPrefix(bucket, keyMarker)
				}
			} else {
				skipKeyMarker = keyMarker
				startKey = multipartUploadKeyIndexPrefix(bucket, keyMarker)
			}
		}

		cursor := keyIndexBucket.Cursor()
		entryCount := 0
		currentCommonPrefix := ""
		lastKeyMarker := ""
		lastUploadIDMarker := ""
		lastCommonPrefix := ""

		for k, _ := cursor.Seek(startKey); k != nil; k, _ = cursor.Next() {
			entryBucket, key, _, uploadID, ok := parseMultipartUploadKeyIndexKey(k)
			if !ok {
				continue
			}
			if entryBucket != bucket {
				if entryBucket > bucket {
					break
				}
				continue
			}
			if prefix != "" {
				if key < prefix {
					continue
				}
				if !strings.HasPrefix(key, prefix) {
					break
				}
			}
			if skipKeyMarker != "" && key <= skipKeyMarker {
				continue
			}
			if exactMarkerResume && string(k) == string(startKey) {
				continue
			}

			if delimiter != "" {
				relative := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(relative, delimiter); idx >= 0 {
					commonPrefix := prefix + relative[:idx+len(delimiter)]
					if keyMarker != "" && commonPrefix <= keyMarker {
						continue
					}
					if commonPrefix == currentCommonPrefix {
						continue
					}
					if entryCount >= maxUploads {
						page.IsTruncated = true
						page.NextKeyMarker = lastKeyMarker
						page.NextUploadIDMarker = lastUploadIDMarker
						if lastCommonPrefix != "" {
							page.NextKeyMarker = lastCommonPrefix
							page.NextUploadIDMarker = ""
						}
						return nil
					}
					currentCommonPrefix = commonPrefix
					page.CommonPrefixes = append(page.CommonPrefixes, commonPrefix)
					lastCommonPrefix = commonPrefix
					lastKeyMarker = ""
					lastUploadIDMarker = ""
					entryCount++
					continue
				}
			}

			upload, err := getMultipartUploadFromBucket(uploadsBucket, uploadID)
			if err != nil {
				if errors.Is(err, ErrMultipartNotFound) {
					continue
				}
				return err
			}
			if upload.State != "pending" || upload.Bucket != bucket || upload.Key != key {
				continue
			}
			if entryCount >= maxUploads {
				page.IsTruncated = true
				if lastCommonPrefix != "" {
					page.NextKeyMarker = lastCommonPrefix
					page.NextUploadIDMarker = ""
				} else {
					page.NextKeyMarker = lastKeyMarker
					page.NextUploadIDMarker = lastUploadIDMarker
				}
				return nil
			}
			page.Uploads = append(page.Uploads, *upload)
			lastKeyMarker = upload.Key
			lastUploadIDMarker = upload.UploadID
			lastCommonPrefix = ""
			entryCount++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return page, nil
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

		final.Bucket = upload.Bucket
		final.Key = upload.Key
		if _, err := putObjectVersionTx(tx, final); err != nil {
			return err
		}

		upload.State = "completed"
		if err := putMultipartUpload(multipartUploadBucket, uploadID, upload); err != nil {
			return err
		}
		if err := deleteMultipartUploadKeyIndexEntry(tx, upload); err != nil {
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
		if upload.State != "pending" {
			return fmt.Errorf("%w: %s", ErrMultipartNotPending, uploadID)
		}
		upload.State = "aborted"
		if err := putMultipartUpload(multipartUploadBucket, uploadID, upload); err != nil {
			return err
		}
		if err := deleteMultipartUploadKeyIndexEntry(tx, upload); err != nil {
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
		uploadsToDelete := make([]models.MultipartUpload, 0)
		orphanIDs := make([]string, 0)
		if err := uploadsBucket.ForEach(func(k, v []byte) error {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(v, &upload); err != nil {
				orphanIDs = append(orphanIDs, string(k))
				return nil
			}
			createdAt, err := time.Parse(time.RFC3339, upload.CreatedAt)
			if err != nil {
				orphanIDs = append(orphanIDs, string(k))
				return nil
			}
			if now.Sub(createdAt) >= retention {
				upload.UploadID = string(k)
				uploadsToDelete = append(uploadsToDelete, upload)
			}
			return nil
		}); err != nil {
			return err
		}

		for _, upload := range uploadsToDelete {
			if err := uploadsBucket.Delete([]byte(upload.UploadID)); err != nil {
				return err
			}
			if err := deleteMultipartUploadKeyIndexEntry(tx, &upload); err != nil {
				return err
			}
			if err := deleteMultipartPartsByUploadID(tx, upload.UploadID); err != nil {
				return err
			}
			cleaned++
		}
		for _, uploadID := range orphanIDs {
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

		versionsBucket := tx.Bucket(versionIndex)
		if versionsBucket != nil {
			if err := versionsBucket.ForEach(func(_, value []byte) error {
				object := models.ObjectManifest{}
				if err := json.Unmarshal(value, &object); err != nil {
					return err
				}
				for _, chunkID := range object.Chunks {
					chunkSet[chunkID] = struct{}{}
				}
				return nil
			}); err != nil {
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
