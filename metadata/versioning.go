package metadata

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"fs/models"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

var (
	versionIndex      = []byte("__OBJECT_VERSIONS__")
	versionOrderIndex = []byte("__OBJECT_VERSION_ORDER__")
	bucketStateIndex  = []byte("__BUCKET_STATE__")
)

const (
	versioningEnabled   = "Enabled"
	versioningSuspended = "Suspended"
	nullVersionID       = "null"
	versionSeparator    = "\x00"
	maxVersionSeq       = ^uint64(0)
)

// ErrVersionNotFound reports that a requested object version does not exist.
var ErrVersionNotFound = errors.New("object version not found")

// ErrPreconditionFailed reports that a conditional write precondition did not hold.
var ErrPreconditionFailed = errors.New("precondition failed")

// IfNoneMatchPreconditionFailed reports whether an If-None-Match header value
// matches an existing live object's ETag (or "*").
func IfNoneMatchPreconditionFailed(headerValue, etag string) bool {
	for _, rawToken := range strings.Split(headerValue, ",") {
		token := strings.TrimSpace(rawToken)
		if token == "" {
			continue
		}
		if token == "*" {
			return true
		}

		token = strings.TrimPrefix(token, "W/")
		token = strings.Trim(token, `"`)
		if strings.EqualFold(token, etag) {
			return true
		}
	}
	return false
}

// DeleteResult describes a version-aware object delete.
type DeleteResult struct {
	VersionID    string
	DeleteMarker bool
}

// ObjectVersion is one entry in a ListObjectVersions page.
type ObjectVersion struct {
	Manifest *models.ObjectManifest
	IsLatest bool
}

// ObjectVersionsPage is one page of ListObjectVersions results.
type ObjectVersionsPage struct {
	Versions            []ObjectVersion
	CommonPrefixes      []string
	NextKeyMarker       string
	NextVersionIDMarker string
	IsTruncated         bool
}

func objectVersionKey(bucket, key, versionID string) []byte {
	return []byte(bucket + versionSeparator + key + versionSeparator + versionID)
}

func objectVersionPrefix(bucket, key string) []byte {
	return []byte(bucket + versionSeparator + key + versionSeparator)
}

// objectVersionOrderKey inverts the sequence so ascending byte order yields
// newest versions first without in-memory sorting.
func objectVersionOrderKey(bucket, key string, seq uint64) []byte {
	return []byte(fmt.Sprintf("%s\x00%s\x00%020d", bucket, key, maxVersionSeq-seq))
}

func objectVersionOrderPrefix(bucket, key string) []byte {
	return []byte(bucket + versionSeparator + key + versionSeparator)
}

func bucketManifestFromTx(tx *bbolt.Tx, bucket string) (*models.BucketManifest, error) {
	index := tx.Bucket(systemIndex)
	if index == nil {
		return nil, errors.New("system index not found")
	}
	raw := index.Get([]byte(bucket))
	if raw == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
	}
	manifest := models.BucketManifest{}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func bucketVersioningTx(tx *bbolt.Tx, bucket string) (string, error) {
	manifest, err := bucketManifestFromTx(tx, bucket)
	if err != nil {
		return "", err
	}
	return manifest.VersioningStatus, nil
}

// GetBucketVersioning returns the bucket's versioning state ("" when off).
func (h *MetadataHandler) GetBucketVersioning(bucket string) (string, error) {
	var status string
	err := h.view(func(tx *bbolt.Tx) error {
		var err error
		status, err = bucketVersioningTx(tx, bucket)
		return err
	})
	return status, err
}

// SetBucketVersioning enables or suspends versioning for a bucket.
func (h *MetadataHandler) SetBucketVersioning(bucket, status string) error {
	if status != versioningEnabled && status != versioningSuspended {
		return fmt.Errorf("invalid versioning status %q", status)
	}
	return h.update(func(tx *bbolt.Tx) error {
		manifest, err := bucketManifestFromTx(tx, bucket)
		if err != nil {
			return err
		}
		manifest.VersioningStatus = status
		data, err := json.Marshal(manifest)
		if err != nil {
			return err
		}
		return tx.Bucket(systemIndex).Put([]byte(bucket), data)
	})
}

func nextVersionSeq(tx *bbolt.Tx, bucket string) (uint64, error) {
	state, err := tx.CreateBucketIfNotExists(bucketStateIndex)
	if err != nil {
		return 0, err
	}
	var seq uint64
	if raw := state.Get([]byte(bucket)); raw != nil {
		if len(raw) != 8 {
			return 0, errors.New("invalid bucket version state")
		}
		seq = binary.BigEndian.Uint64(raw)
	}
	seq++
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	if err := state.Put([]byte(bucket), buf[:]); err != nil {
		return 0, err
	}
	return seq, nil
}

func putVersionRecord(versions, order *bbolt.Bucket, manifest *models.ObjectManifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := versions.Put(objectVersionKey(manifest.Bucket, manifest.Key, manifest.VersionID), data); err != nil {
		return err
	}
	return order.Put(objectVersionOrderKey(manifest.Bucket, manifest.Key, manifest.VersionSeq), []byte(manifest.VersionID))
}

func setLatestFromOrder(versions, order, metaBucket *bbolt.Bucket, bucket, key string) error {
	prefix := objectVersionOrderPrefix(bucket, key)
	cursor := order.Cursor()
	k, v := cursor.Seek(prefix)
	if k == nil || !strings.HasPrefix(string(k), string(prefix)) {
		return metaBucket.Delete([]byte(key))
	}
	versionID := string(v)
	data := versions.Get(objectVersionKey(bucket, key, versionID))
	if data == nil {
		return fmt.Errorf("%w: %s", ErrVersionNotFound, versionID)
	}
	return metaBucket.Put([]byte(key), data)
}

func removeNullVersionTx(versions, order, metaBucket *bbolt.Bucket, bucket, key string, nowNano int64) error {
	data := versions.Get(objectVersionKey(bucket, key, nullVersionID))
	if data == nil {
		// An unmaterialized pre-versioning record lives only in metaBucket.
		if latest := metaBucket.Get([]byte(key)); latest != nil {
			current := models.ObjectManifest{}
			if err := json.Unmarshal(latest, &current); err != nil {
				return err
			}
			if current.VersionID == "" {
				return removeChunkRefsTx(metaBucket.Tx(), current.Chunks, nowNano)
			}
		}
		return nil
	}
	manifest := models.ObjectManifest{}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	if err := removeChunkRefsTx(metaBucket.Tx(), manifest.Chunks, nowNano); err != nil {
		return err
	}
	if err := versions.Delete(objectVersionKey(bucket, key, nullVersionID)); err != nil {
		return err
	}
	if err := order.Delete(objectVersionOrderKey(bucket, key, manifest.VersionSeq)); err != nil {
		return err
	}
	if latest := metaBucket.Get([]byte(key)); latest != nil {
		current := models.ObjectManifest{}
		if err := json.Unmarshal(latest, &current); err != nil {
			return err
		}
		if current.VersionID == nullVersionID {
			return setLatestFromOrder(versions, order, metaBucket, bucket, key)
		}
	}
	return nil
}

// putObjectVersionTx writes a manifest according to the bucket's versioning
// state. When ifNoneMatch is non-empty, it rejects the write with
// ErrPreconditionFailed if a live (non-delete-marker) object matches. All
// index updates happen in the caller's transaction.
func putObjectVersionTx(tx *bbolt.Tx, manifest *models.ObjectManifest, ifNoneMatch, versionID string, now time.Time) (string, error) {
	if manifest == nil {
		return "", errors.New("object manifest is required")
	}
	status, err := bucketVersioningTx(tx, manifest.Bucket)
	if err != nil {
		return "", err
	}
	metaBucket := tx.Bucket([]byte(manifest.Bucket))
	if metaBucket == nil {
		return "", fmt.Errorf("%w: %s", ErrBucketNotFound, manifest.Bucket)
	}
	if ifNoneMatch = strings.TrimSpace(ifNoneMatch); ifNoneMatch != "" {
		if existing := metaBucket.Get([]byte(manifest.Key)); existing != nil {
			current := models.ObjectManifest{}
			if err := json.Unmarshal(existing, &current); err != nil {
				return "", err
			}
			if !current.DeleteMarker && IfNoneMatchPreconditionFailed(ifNoneMatch, current.ETag) {
				return "", ErrPreconditionFailed
			}
		}
	}
	nowNano := now.UnixNano()
	if status == "" {
		manifest.VersionID = ""
		manifest.VersionSeq = 0
		manifest.DeleteMarker = false
		if existing := metaBucket.Get([]byte(manifest.Key)); existing != nil {
			old := models.ObjectManifest{}
			if err := json.Unmarshal(existing, &old); err != nil {
				return "", err
			}
			if err := removeChunkRefsTx(tx, old.Chunks, nowNano); err != nil {
				return "", err
			}
		}
		if err := addChunkRefsTx(tx, manifest.Chunks); err != nil {
			return "", err
		}
		data, err := json.Marshal(manifest)
		if err != nil {
			return "", err
		}
		if err := metaBucket.Put([]byte(manifest.Key), data); err != nil {
			return "", err
		}
		return "", nil
	}

	resolvedID := nullVersionID
	if status == versioningEnabled {
		resolvedID = strings.TrimSpace(versionID)
		if resolvedID == "" || resolvedID == nullVersionID {
			return "", errors.New("version id is required for versioned bucket")
		}
	}

	versions, err := tx.CreateBucketIfNotExists(versionIndex)
	if err != nil {
		return "", err
	}
	order, err := tx.CreateBucketIfNotExists(versionOrderIndex)
	if err != nil {
		return "", err
	}
	seq, err := nextVersionSeq(tx, manifest.Bucket)
	if err != nil {
		return "", err
	}

	if status == versioningEnabled {
		// Materialize the pre-versioning record so the first versioned write
		// does not silently discard it.
		if existing := metaBucket.Get([]byte(manifest.Key)); existing != nil {
			latest := models.ObjectManifest{}
			if err := json.Unmarshal(existing, &latest); err != nil {
				return "", err
			}
			if latest.VersionID == "" {
				latest.VersionID = nullVersionID
				latest.VersionSeq = seq - 1
				if err := putVersionRecord(versions, order, &latest); err != nil {
					return "", err
				}
			}
		}
	} else {
		if err := removeNullVersionTx(versions, order, metaBucket, manifest.Bucket, manifest.Key, nowNano); err != nil {
			return "", err
		}
	}

	manifest.VersionID = resolvedID
	manifest.VersionSeq = seq
	manifest.DeleteMarker = false
	if err := addChunkRefsTx(tx, manifest.Chunks); err != nil {
		return "", err
	}
	if err := putVersionRecord(versions, order, manifest); err != nil {
		return "", err
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if err := metaBucket.Put([]byte(manifest.Key), data); err != nil {
		return "", err
	}
	return resolvedID, nil
}

// PutObjectVersion stores a manifest as a new version (or overwrites when the
// bucket is unversioned) and returns the version ID.
func (h *MetadataHandler) PutObjectVersion(manifest *models.ObjectManifest, versionID string, now time.Time) (string, error) {
	return h.PutObjectVersionConditional(manifest, "", versionID, now)
}

// PutObjectVersionConditional stores a manifest while atomically enforcing an
// optional If-None-Match precondition inside the metadata write transaction.
func (h *MetadataHandler) PutObjectVersionConditional(manifest *models.ObjectManifest, ifNoneMatch, versionID string, now time.Time) (string, error) {
	var resolvedID string
	err := h.update(func(tx *bbolt.Tx) error {
		var err error
		resolvedID, err = putObjectVersionTx(tx, manifest, ifNoneMatch, versionID, now)
		return err
	})
	return resolvedID, err
}

func deleteObjectTx(tx *bbolt.Tx, bucket, key, versionID, deleteMarkerID string, now time.Time) (DeleteResult, error) {
	status, err := bucketVersioningTx(tx, bucket)
	if err != nil {
		return DeleteResult{}, err
	}
	metaBucket := tx.Bucket([]byte(bucket))
	if metaBucket == nil {
		return DeleteResult{}, fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
	}
	latestData := metaBucket.Get([]byte(key))
	nowNano := now.UnixNano()

	if versionID == "" {
		if status == "" {
			if latestData == nil {
				return DeleteResult{}, nil
			}
			latest := models.ObjectManifest{}
			if err := json.Unmarshal(latestData, &latest); err != nil {
				return DeleteResult{}, err
			}
			if err := removeChunkRefsTx(tx, latest.Chunks, nowNano); err != nil {
				return DeleteResult{}, err
			}
			return DeleteResult{}, metaBucket.Delete([]byte(key))
		}
		markerID := nullVersionID
		if status == versioningEnabled {
			markerID = strings.TrimSpace(deleteMarkerID)
			if markerID == "" || markerID == nullVersionID {
				return DeleteResult{}, errors.New("delete marker id is required for versioned bucket")
			}
		}
		versions, err := tx.CreateBucketIfNotExists(versionIndex)
		if err != nil {
			return DeleteResult{}, err
		}
		order, err := tx.CreateBucketIfNotExists(versionOrderIndex)
		if err != nil {
			return DeleteResult{}, err
		}
		seq, err := nextVersionSeq(tx, bucket)
		if err != nil {
			return DeleteResult{}, err
		}
		if status == versioningEnabled {
			if latestData != nil {
				latest := models.ObjectManifest{}
				if err := json.Unmarshal(latestData, &latest); err != nil {
					return DeleteResult{}, err
				}
				if latest.VersionID == "" {
					latest.VersionID = nullVersionID
					latest.VersionSeq = seq - 1
					if err := putVersionRecord(versions, order, &latest); err != nil {
						return DeleteResult{}, err
					}
				}
			}
		} else if err := removeNullVersionTx(versions, order, metaBucket, bucket, key, nowNano); err != nil {
			return DeleteResult{}, err
		}
		marker := models.ObjectManifest{
			Bucket:       bucket,
			Key:          key,
			VersionID:    markerID,
			VersionSeq:   seq,
			DeleteMarker: true,
			CreatedAt:    now.UTC().Unix(),
		}
		if err := putVersionRecord(versions, order, &marker); err != nil {
			return DeleteResult{}, err
		}
		data, err := json.Marshal(marker)
		if err != nil {
			return DeleteResult{}, err
		}
		if err := metaBucket.Put([]byte(key), data); err != nil {
			return DeleteResult{}, err
		}
		return DeleteResult{VersionID: markerID, DeleteMarker: true}, nil
	}

	// Explicit version. The "null" version may still be a legacy record.
	if versionID == nullVersionID && latestData != nil {
		latest := models.ObjectManifest{}
		if err := json.Unmarshal(latestData, &latest); err != nil {
			return DeleteResult{}, err
		}
		if latest.VersionID == "" {
			if err := removeChunkRefsTx(tx, latest.Chunks, nowNano); err != nil {
				return DeleteResult{}, err
			}
			if err := metaBucket.Delete([]byte(key)); err != nil {
				return DeleteResult{}, err
			}
			return DeleteResult{VersionID: nullVersionID}, nil
		}
	}

	versions := tx.Bucket(versionIndex)
	if versions == nil {
		return DeleteResult{}, nil
	}
	data := versions.Get(objectVersionKey(bucket, key, versionID))
	if data == nil {
		return DeleteResult{}, nil
	}
	manifest := models.ObjectManifest{}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return DeleteResult{}, err
	}
	if err := removeChunkRefsTx(tx, manifest.Chunks, nowNano); err != nil {
		return DeleteResult{}, err
	}
	if err := versions.Delete(objectVersionKey(bucket, key, versionID)); err != nil {
		return DeleteResult{}, err
	}
	if order := tx.Bucket(versionOrderIndex); order != nil {
		if err := order.Delete(objectVersionOrderKey(bucket, key, manifest.VersionSeq)); err != nil {
			return DeleteResult{}, err
		}
	}
	if latestData != nil {
		latest := models.ObjectManifest{}
		if err := json.Unmarshal(latestData, &latest); err != nil {
			return DeleteResult{}, err
		}
		if latest.VersionID == versionID {
			if err := setLatestFromOrder(versions, tx.Bucket(versionOrderIndex), metaBucket, bucket, key); err != nil {
				return DeleteResult{}, err
			}
		}
	}
	return DeleteResult{VersionID: versionID, DeleteMarker: manifest.DeleteMarker}, nil
}

// DeleteObject removes the named version, creates a delete marker, or deletes
// the object permanently depending on the bucket state and versionID argument.
func (h *MetadataHandler) DeleteObject(bucket, key, versionID, deleteMarkerID string, now time.Time) (DeleteResult, error) {
	var result DeleteResult
	err := h.update(func(tx *bbolt.Tx) error {
		var err error
		result, err = deleteObjectTx(tx, bucket, key, versionID, deleteMarkerID, now)
		return err
	})
	return result, err
}

// DeleteObjectSpec identifies one key (and optional version) in a batch delete.
type DeleteObjectSpec struct {
	Key            string
	VersionID      string
	DeleteMarkerID string
}

// BatchDeleteResult describes the outcome of one item in a batch delete.
type BatchDeleteResult struct {
	Key                string
	RequestedVersionID string
	VersionID          string
	DeleteMarker       bool
}

// DeleteObjects applies a batch of version-aware deletes in a single metadata
// transaction, honoring the bucket's versioning state whether or not items
// specify a VersionID.
func (h *MetadataHandler) DeleteObjects(bucket string, specs []DeleteObjectSpec, now time.Time) ([]BatchDeleteResult, error) {
	results := make([]BatchDeleteResult, 0, len(specs))
	err := h.update(func(tx *bbolt.Tx) error {
		results = results[:0]
		if _, err := bucketVersioningTx(tx, bucket); err != nil {
			return err
		}
		for _, spec := range specs {
			if spec.Key == "" {
				continue
			}
			reqVersion := strings.TrimSpace(spec.VersionID)
			res, err := deleteObjectTx(tx, bucket, spec.Key, reqVersion, spec.DeleteMarkerID, now)
			if err != nil {
				return err
			}
			results = append(results, BatchDeleteResult{
				Key:                spec.Key,
				RequestedVersionID: reqVersion,
				VersionID:          res.VersionID,
				DeleteMarker:       res.DeleteMarker,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// GetManifestVersion returns one version, resolving "null" to the legacy
// record when the bucket has never been versioned. Delete markers are returned
// with DeleteMarker set; callers decide how to surface them.
func (h *MetadataHandler) GetManifestVersion(bucket, key, versionID string) (*models.ObjectManifest, error) {
	var manifest *models.ObjectManifest
	err := h.view(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte(bucket))
		if metaBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		if versionID == nullVersionID {
			if data := metaBucket.Get([]byte(key)); data != nil {
				latest := models.ObjectManifest{}
				if err := json.Unmarshal(data, &latest); err != nil {
					return err
				}
				if latest.VersionID == "" {
					manifest = &latest
					return nil
				}
			}
		}
		versions := tx.Bucket(versionIndex)
		if versions == nil {
			return fmt.Errorf("%w: %s", ErrVersionNotFound, versionID)
		}
		data := versions.Get(objectVersionKey(bucket, key, versionID))
		if data == nil {
			return fmt.Errorf("%w: %s", ErrVersionNotFound, versionID)
		}
		version := models.ObjectManifest{}
		if err := json.Unmarshal(data, &version); err != nil {
			return err
		}
		manifest = &version
		return nil
	})
	if err != nil {
		return nil, err
	}
	return manifest, nil
}

func versionIDOrNull(manifest *models.ObjectManifest) string {
	if manifest.VersionID == "" {
		return nullVersionID
	}
	return manifest.VersionID
}

// ListObjectVersions returns one page ordered by key ascending and versions
// newest first, including delete markers.
func (h *MetadataHandler) ListObjectVersions(bucket, prefix, delimiter, keyMarker, versionIDMarker string, maxKeys int) (*ObjectVersionsPage, error) {
	if maxKeys < 0 {
		return nil, errors.New("max keys must not be negative")
	}
	page := &ObjectVersionsPage{}
	if maxKeys == 0 {
		return page, nil
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}
	budget := maxKeys
	var lastKey, lastVersion string

	err := h.view(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket([]byte(bucket))
		if metaBucket == nil {
			return fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
		}
		versions := tx.Bucket(versionIndex)
		order := tx.Bucket(versionOrderIndex)
		cursor := metaBucket.Cursor()
		start := prefix
		if keyMarker != "" && keyMarker > start {
			start = keyMarker
		}
		var k, v []byte
		if start == "" {
			k, v = cursor.First()
		} else {
			k, v = cursor.Seek([]byte(start))
		}
		seenPrefix := ""
		for k != nil {
			key := string(k)
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				break
			}
			if keyMarker != "" && key < keyMarker {
				k, v = cursor.Next()
				continue
			}
			if keyMarker != "" && versionIDMarker == "" && key == keyMarker {
				k, v = cursor.Next()
				continue
			}
			if keyMarker != "" && versionIDMarker == "" && delimiter != "" &&
				strings.HasSuffix(keyMarker, delimiter) && strings.HasPrefix(key, keyMarker) {
				if next, ok := prefixSuccessor(keyMarker); ok {
					k, v = cursor.Seek(next)
					continue
				}
				break
			}
			if delimiter != "" {
				rest := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(rest, delimiter); idx >= 0 {
					commonPrefix := prefix + rest[:idx+len(delimiter)]
					if commonPrefix == seenPrefix {
						if next, ok := prefixSuccessor(commonPrefix); ok {
							k, v = cursor.Seek(next)
							continue
						}
						break
					}
					if budget == 0 {
						page.IsTruncated = true
						page.NextKeyMarker, page.NextVersionIDMarker = lastKey, lastVersion
						return nil
					}
					page.CommonPrefixes = append(page.CommonPrefixes, commonPrefix)
					seenPrefix = commonPrefix
					lastKey, lastVersion = commonPrefix, ""
					budget--
					if next, ok := prefixSuccessor(commonPrefix); ok {
						k, v = cursor.Seek(next)
						continue
					}
					break
				}
			}

			latest := models.ObjectManifest{}
			if err := json.Unmarshal(v, &latest); err != nil {
				return err
			}
			latestID := versionIDOrNull(&latest)
			var entries []*models.ObjectManifest
			if latest.VersionID == "" {
				entries = append(entries, &latest)
			} else if versions != nil && order != nil {
				keyPrefix := objectVersionOrderPrefix(bucket, key)
				orderCursor := order.Cursor()
				for ok, ov := orderCursor.Seek(keyPrefix); ok != nil && strings.HasPrefix(string(ok), string(keyPrefix)); ok, ov = orderCursor.Next() {
					data := versions.Get(objectVersionKey(bucket, key, string(ov)))
					if data == nil {
						continue
					}
					entry := models.ObjectManifest{}
					if err := json.Unmarshal(data, &entry); err != nil {
						return err
					}
					entries = append(entries, &entry)
				}
			} else {
				entries = append(entries, &latest)
			}

			skipUntilMarker := key == keyMarker && versionIDMarker != ""
			foundMarker := false
			for _, entry := range entries {
				if skipUntilMarker {
					if versionIDOrNull(entry) == versionIDMarker {
						skipUntilMarker = false
						foundMarker = true
					}
					continue
				}
				if budget == 0 {
					page.IsTruncated = true
					page.NextKeyMarker, page.NextVersionIDMarker = lastKey, lastVersion
					return nil
				}
				page.Versions = append(page.Versions, ObjectVersion{
					Manifest: entry,
					IsLatest: versionIDOrNull(entry) == latestID,
				})
				lastKey, lastVersion = key, versionIDOrNull(entry)
				budget--
			}
			if key == keyMarker && versionIDMarker != "" && !foundMarker {
				return fmt.Errorf("%w: %s", ErrVersionNotFound, versionIDMarker)
			}
			k, v = cursor.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}
