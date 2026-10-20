package service

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"fs/metadata"
	"fs/models"
	"fs/storage"
)

const benchChunkSize = 1 << 20 // 1 MiB

func benchPayload(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	return data
}

func newBenchService(b *testing.B) *ObjectService {
	b.Helper()
	root := b.TempDir()
	md, err := metadata.NewMetadataHandler(filepath.Join(root, "metadata.db"))
	if err != nil {
		b.Fatalf("NewMetadataHandler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, benchChunkSize)
	if err != nil {
		b.Fatalf("NewBlobStore: %v", err)
	}
	svc := NewObjectService(md, blob, time.Hour, 0)
	b.Cleanup(func() {
		_ = svc.Close()
	})
	return svc
}

func newBenchBucket(b *testing.B, svc *ObjectService) string {
	b.Helper()
	const bucket = "bench-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		b.Fatalf("CreateBucket: %v", err)
	}
	return bucket
}

// benchmarkPutObject measures the full PutObject path: upload limiting, blob
// ingest (hash, chunk write, fsync, dir sync), chunk pinning and the metadata
// transaction. Every iteration uses unique content so dedup cannot skip the
// blob write.
func benchmarkPutObject(b *testing.B, size int) {
	b.Helper()
	svc := newBenchService(b)
	bucket := newBenchBucket(b, svc)
	payload := benchPayload(size)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(payload, uint64(i))
		key := "object-" + strconv.Itoa(i)
		if _, err := svc.PutObject(bucket, key, "application/octet-stream", bytes.NewReader(payload)); err != nil {
			b.Fatalf("PutObject: %v", err)
		}
	}
}

// BenchmarkPutObjectSmall puts 4 KiB objects; each iteration includes one
// chunk fsync plus its directory sync.
func BenchmarkPutObjectSmall(b *testing.B) { benchmarkPutObject(b, 4<<10) }

// BenchmarkPutObjectMedium puts 1 MiB objects; each iteration includes one
// chunk fsync plus its directory sync.
func BenchmarkPutObjectMedium(b *testing.B) { benchmarkPutObject(b, 1<<20) }

// BenchmarkGetObject reads a 1 MiB object and fully drains the stream on every
// iteration. The measurement covers the chunk pre-flight Stat, the pipe
// hand-off, SHA-256 integrity verification per chunk and lease release.
func BenchmarkGetObject(b *testing.B) {
	svc := newBenchService(b)
	bucket := newBenchBucket(b, svc)
	const size = 1 << 20
	payload := benchPayload(size)
	if _, err := svc.PutObject(bucket, "read-target", "application/octet-stream", bytes.NewReader(payload)); err != nil {
		b.Fatalf("PutObject: %v", err)
	}

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body, _, err := svc.GetObject(bucket, "read-target")
		if err != nil {
			b.Fatalf("GetObject: %v", err)
		}
		n, err := io.Copy(io.Discard, body)
		closeErr := body.Close()
		if err != nil {
			b.Fatalf("drain GetObject body: %v", err)
		}
		if closeErr != nil {
			b.Fatalf("close GetObject body: %v", closeErr)
		}
		if n != size {
			b.Fatalf("GetObject body = %d bytes, want %d", n, size)
		}
	}
}

// BenchmarkListObjects lists an entire bucket with an empty prefix; the
// ListObjects service API does not paginate, so every iteration performs a
// full scan and JSON decode of all keys. Keys are seeded directly at the
// metadata layer so setup stays out of the timed loop.
func BenchmarkListObjects(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(fmt.Sprintf("%dKeysFullListing", count), func(b *testing.B) {
			svc := newBenchService(b)
			bucket := newBenchBucket(b, svc)
			for i := 0; i < count; i++ {
				manifest := &models.ObjectManifest{
					Bucket:      bucket,
					Key:         fmt.Sprintf("object-%06d", i),
					Size:        1024,
					ContentType: "application/octet-stream",
					ETag:        fmt.Sprintf("%032x", i+1),
					CreatedAt:   time.Now().Unix(),
				}
				if err := svc.metadata.PutManifest(manifest); err != nil {
					b.Fatalf("seed manifest %d: %v", i, err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				objects, err := svc.ListObjects(bucket, "")
				if err != nil {
					b.Fatalf("ListObjects: %v", err)
				}
				if len(objects) != count {
					b.Fatalf("ListObjects returned %d objects, want %d", len(objects), count)
				}
			}
		})
	}
}

// BenchmarkCompleteMultipartUpload completes a three-part upload. Non-final
// multipart parts must be at least 5 MiB (S3 rule enforced by the service), so
// the parts are 5 MiB, 5 MiB and 64 KiB. Uploads and their parts are seeded
// before the timed loop; completion only reads metadata and never touches
// chunk bytes, so parts carry sizes but no blob references. Each iteration
// completes a distinct pending upload, matching the one-shot production call.
func BenchmarkCompleteMultipartUpload(b *testing.B) {
	const (
		key          = "multipart-object"
		firstPart    = 5 << 20
		middlePart   = 5 << 20
		finalPart    = 64 << 10
		totalObject  = firstPart + middlePart + finalPart
		partSequence = 3
	)
	svc := newBenchService(b)
	bucket := newBenchBucket(b, svc)

	partSizes := []int64{firstPart, middlePart, finalPart}
	completed := make([]models.CompletedPart, 0, partSequence)
	for i := range partSizes {
		completed = append(completed, models.CompletedPart{
			PartNumber: i + 1,
			ETag:       fmt.Sprintf("%032x", i+1),
		})
	}

	uploadIDs := make([]string, b.N)
	for i := range uploadIDs {
		upload, err := svc.CreateMultipartUpload(bucket, key)
		if err != nil {
			b.Fatalf("CreateMultipartUpload: %v", err)
		}
		for j, size := range partSizes {
			part := models.UploadedPart{
				PartNumber: j + 1,
				ETag:       completed[j].ETag,
				Size:       size,
				CreatedAt:  time.Now().Unix(),
			}
			if err := svc.metadata.PutMultipartPart(upload.UploadID, part); err != nil {
				b.Fatalf("PutMultipartPart: %v", err)
			}
		}
		uploadIDs[i] = upload.UploadID
	}

	b.SetBytes(totalObject)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := svc.CompleteMultipartUpload(bucket, key, uploadIDs[i], completed); err != nil {
			b.Fatalf("CompleteMultipartUpload: %v", err)
		}
	}
}
