package metadata

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fs/models"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

var (
	chunkRefIndex    = []byte("__CHUNK_REFS__")
	gcQueueIndex     = []byte("__GC_QUEUE__")
	metaStateIndex   = []byte("__META_STATE__")
	chunkRefsInitKey = []byte("chunk_refs_v1")
)

func addChunkRefsTx(tx *bbolt.Tx, chunks []string) error {
	if len(chunks) == 0 {
		return nil
	}
	refs, err := tx.CreateBucketIfNotExists(chunkRefIndex)
	if err != nil {
		return err
	}
	queue, err := tx.CreateBucketIfNotExists(gcQueueIndex)
	if err != nil {
		return err
	}

	for _, chunkID := range chunks {
		if chunkID == "" {
			continue
		}
		var count uint64
		if raw := refs.Get([]byte(chunkID)); len(raw) == 8 {
			count = binary.BigEndian.Uint64(raw)
		}
		count++
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, count)
		if err := refs.Put([]byte(chunkID), val); err != nil {
			return err
		}
		if err := queue.Delete([]byte(chunkID)); err != nil {
			return err
		}
	}
	return nil
}

func removeChunkRefsTx(tx *bbolt.Tx, chunks []string, nowNano int64) error {
	if len(chunks) == 0 {
		return nil
	}
	refs, err := tx.CreateBucketIfNotExists(chunkRefIndex)
	if err != nil {
		return err
	}
	queue, err := tx.CreateBucketIfNotExists(gcQueueIndex)
	if err != nil {
		return err
	}

	tsVal := make([]byte, 8)
	binary.BigEndian.PutUint64(tsVal, uint64(nowNano))

	for _, chunkID := range chunks {
		if chunkID == "" {
			continue
		}
		var count uint64
		if raw := refs.Get([]byte(chunkID)); len(raw) == 8 {
			count = binary.BigEndian.Uint64(raw)
		}
		if count <= 1 {
			if err := refs.Delete([]byte(chunkID)); err != nil {
				return err
			}
			if err := queue.Put([]byte(chunkID), tsVal); err != nil {
				return err
			}
			continue
		}
		count--
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, count)
		if err := refs.Put([]byte(chunkID), val); err != nil {
			return err
		}
	}
	return nil
}

func initChunkRefsTx(tx *bbolt.Tx) error {
	if _, err := tx.CreateBucketIfNotExists(chunkRefIndex); err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(gcQueueIndex); err != nil {
		return err
	}
	state, err := tx.CreateBucketIfNotExists(metaStateIndex)
	if err != nil {
		return err
	}
	if state.Get(chunkRefsInitKey) != nil {
		return nil
	}

	systemIndexBucket := tx.Bucket(systemIndex)
	if systemIndexBucket == nil {
		return errors.New("system index not found")
	}
	c := systemIndexBucket.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		metadataBucket := tx.Bucket(k)
		if metadataBucket == nil {
			continue
		}
		if err := metadataBucket.ForEach(func(_, v []byte) error {
			object := models.ObjectManifest{}
			if err := json.Unmarshal(v, &object); err != nil {
				return err
			}
			// Versioned latest entries also live in versionIndex; only legacy
			// pre-versioning records (VersionID == "") live solely here.
			if object.VersionID != "" {
				return nil
			}
			return addChunkRefsTx(tx, object.Chunks)
		}); err != nil {
			return err
		}
	}

	if versionsBucket := tx.Bucket(versionIndex); versionsBucket != nil {
		if err := versionsBucket.ForEach(func(_, value []byte) error {
			object := models.ObjectManifest{}
			if err := json.Unmarshal(value, &object); err != nil {
				return err
			}
			return addChunkRefsTx(tx, object.Chunks)
		}); err != nil {
			return err
		}
	}

	pendingUploads := make(map[string]struct{})
	if uploadsBucket := tx.Bucket(multipartUploadIndex); uploadsBucket != nil {
		if err := uploadsBucket.ForEach(func(k, v []byte) error {
			upload := models.MultipartUpload{}
			if err := json.Unmarshal(v, &upload); err != nil {
				return nil
			}
			if upload.State == "pending" {
				pendingUploads[string(k)] = struct{}{}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	if partsBucket := tx.Bucket(multipartUploadPartsIndex); partsBucket != nil {
		if err := partsBucket.ForEach(func(k, v []byte) error {
			uploadID, _, ok := strings.Cut(string(k), ":")
			if !ok {
				return nil
			}
			if _, pending := pendingUploads[uploadID]; !pending {
				return nil
			}
			part := models.UploadedPart{}
			if err := json.Unmarshal(v, &part); err != nil {
				return err
			}
			return addChunkRefsTx(tx, part.Chunks)
		}); err != nil {
			return err
		}
	}

	return state.Put(chunkRefsInitKey, []byte("1"))
}

// ChunkRefCount returns the number of live manifests or pending parts
// referencing chunkID.
func (h *MetadataHandler) ChunkRefCount(chunkID string) (uint64, error) {
	var count uint64
	err := h.view(func(tx *bbolt.Tx) error {
		refs := tx.Bucket(chunkRefIndex)
		if refs == nil {
			return nil
		}
		if raw := refs.Get([]byte(chunkID)); len(raw) == 8 {
			count = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	return count, err
}

// EnqueueUnreferencedChunks queues chunkIDs for garbage collection when they
// currently have zero metadata references (for example after an aborted or
// failed upload).
func (h *MetadataHandler) EnqueueUnreferencedChunks(chunkIDs []string) error {
	if len(chunkIDs) == 0 {
		return nil
	}
	nowNano := time.Now().UnixNano()
	return h.update(func(tx *bbolt.Tx) error {
		refs, err := tx.CreateBucketIfNotExists(chunkRefIndex)
		if err != nil {
			return err
		}
		queue, err := tx.CreateBucketIfNotExists(gcQueueIndex)
		if err != nil {
			return err
		}
		var tsBuf [8]byte
		binary.BigEndian.PutUint64(tsBuf[:], uint64(nowNano))
		for _, chunkID := range chunkIDs {
			if chunkID == "" {
				continue
			}
			if raw := refs.Get([]byte(chunkID)); len(raw) == 8 && binary.BigEndian.Uint64(raw) > 0 {
				continue
			}
			if err := queue.Put([]byte(chunkID), tsBuf[:]); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListGCQueue returns up to limit chunk IDs currently queued for garbage
// collection (all queued IDs when limit <= 0).
func (h *MetadataHandler) ListGCQueue(limit int) ([]string, error) {
	var chunkIDs []string
	err := h.view(func(tx *bbolt.Tx) error {
		queue := tx.Bucket(gcQueueIndex)
		if queue == nil {
			return nil
		}
		cursor := queue.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			chunkIDs = append(chunkIDs, string(k))
			if limit > 0 && len(chunkIDs) >= limit {
				break
			}
		}
		return nil
	})
	return chunkIDs, err
}

// RemoveGCQueueEntries deletes chunkIDs from the GC queue if they remain
// unreferenced.
func (h *MetadataHandler) RemoveGCQueueEntries(chunkIDs []string) error {
	if len(chunkIDs) == 0 {
		return nil
	}
	return h.update(func(tx *bbolt.Tx) error {
		queue := tx.Bucket(gcQueueIndex)
		if queue == nil {
			return nil
		}
		refs := tx.Bucket(chunkRefIndex)
		for _, chunkID := range chunkIDs {
			if chunkID == "" {
				continue
			}
			if refs != nil {
				if raw := refs.Get([]byte(chunkID)); len(raw) == 8 && binary.BigEndian.Uint64(raw) > 0 {
					continue
				}
			}
			if err := queue.Delete([]byte(chunkID)); err != nil {
				return err
			}
		}
		return nil
	})
}
