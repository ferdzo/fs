package api

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"fs/logging"
	"fs/metadata"
	"fs/models"
	"fs/service"
	"fs/storage"

	bolt "go.etcd.io/bbolt"
)

const listBenchBucket = "bench-bucket"

// newBulkListHandler creates a handler whose bucket is populated in a single
// bbolt transaction. It avoids paying one fsynced metadata transaction per key
// when seeding tens of thousands of entries for listing tests.
func newBulkListHandler(tb testing.TB, bucket string, manifests []models.ObjectManifest) *Handler {
	tb.Helper()

	root := tb.TempDir()
	dbPath := filepath.Join(root, "metadata.db")
	md, err := metadata.NewMetadataHandler(dbPath)
	if err != nil {
		tb.Fatalf("new metadata handler: %v", err)
	}
	if err := md.CreateBucket(bucket); err != nil {
		tb.Fatalf("create bucket: %v", err)
	}
	if err := md.Close(); err != nil {
		tb.Fatalf("close metadata handler: %v", err)
	}

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		tb.Fatalf("open metadata db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %q not found", bucket)
		}
		for _, manifest := range manifests {
			payload, err := json.Marshal(manifest)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(manifest.Key), payload); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		tb.Fatalf("seed manifests: %v", err)
	}
	if err := db.Close(); err != nil {
		tb.Fatalf("close seeded db: %v", err)
	}

	md, err = metadata.NewMetadataHandler(dbPath)
	if err != nil {
		tb.Fatalf("reopen metadata handler: %v", err)
	}
	blob, err := storage.NewBlobStore(root, 1024)
	if err != nil {
		tb.Fatalf("new blob store: %v", err)
	}
	svc := service.NewObjectService(md, blob, time.Hour)
	tb.Cleanup(func() {
		_ = svc.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler := NewHandler(svc, logger, logging.Config{}, nil, false)
	handler.setupRoutes()
	return handler
}

// BenchmarkListObjectsV2OneCommonPrefix100k measures the worst case for a
// delimiter listing: 100k keys share one common prefix, so the handler must
// scan every key to prove there is no second prefix, yet emits one entry at
// max-keys=1000. Before the lazy iterator this decoded 100k manifests per
// request; now it decodes none.
func BenchmarkListObjectsV2OneCommonPrefix100k(b *testing.B) {
	const keyCount = 100_000
	manifests := make([]models.ObjectManifest, keyCount)
	for i := range manifests {
		manifests[i] = models.ObjectManifest{
			Bucket:    listBenchBucket,
			Key:       fmt.Sprintf("common/same/object-%06d", i),
			Size:      int64(i),
			ETag:      fmt.Sprintf("etag-%06d", i),
			CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix() + int64(i%1000),
		}
	}
	handler := newBulkListHandler(b, listBenchBucket, manifests)

	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", "common/")
	query.Set("delimiter", "/")
	query.Set("max-keys", "1000")
	target := "/" + listBenchBucket + "?" + query.Encode()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		rec := httptest.NewRecorder()
		handler.router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}
}

type listRefEntry struct {
	key          string
	commonPrefix string
	object       bool
}

func referenceV1Page(keys []string, prefix, delimiter, marker string, maxKeys int) ([]listRefEntry, bool, string) {
	if maxKeys <= 0 {
		return nil, false, ""
	}
	entries := make([]listRefEntry, 0, maxKeys)
	seen := make(map[string]struct{})
	start := prefix
	if marker != "" && marker > start {
		start = marker
	}
	truncated := false
	for _, key := range keys[sort.SearchStrings(keys, start):] {
		if prefix != "" {
			if key < prefix {
				continue
			}
			if !strings.HasPrefix(key, prefix) {
				break
			}
		}
		if marker != "" && key <= marker {
			continue
		}
		if delimiter != "" {
			relative := strings.TrimPrefix(key, prefix)
			if idx := strings.Index(relative, delimiter); idx >= 0 {
				commonPrefix := prefix + relative[:idx+len(delimiter)]
				if marker != "" && commonPrefix <= marker {
					continue
				}
				if _, exists := seen[commonPrefix]; exists {
					continue
				}
				seen[commonPrefix] = struct{}{}
				if len(entries) >= maxKeys {
					truncated = true
					break
				}
				entries = append(entries, listRefEntry{commonPrefix: commonPrefix})
				continue
			}
		}
		if len(entries) >= maxKeys {
			truncated = true
			break
		}
		entries = append(entries, listRefEntry{key: key, object: true})
	}
	next := ""
	if truncated && len(entries) > 0 {
		last := entries[len(entries)-1]
		if last.object {
			next = last.key
		} else {
			next = last.commonPrefix
		}
	}
	return entries, truncated, next
}

func referenceV2Page(keys []string, prefix, delimiter, startAfter, token string, maxKeys int) ([]listRefEntry, bool, string) {
	if maxKeys <= 0 {
		return nil, false, ""
	}
	contType, contValue := "", ""
	if token != "" {
		decoded, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			panic("referenceV2Page: invalid token: " + err.Error())
		}
		contType, contValue, _ = strings.Cut(string(decoded), ":")
	}

	start := prefix
	if token != "" {
		start = contValue
	} else if startAfter != "" && startAfter > start {
		start = startAfter
	}

	entries := make([]listRefEntry, 0, maxKeys)
	seen := make(map[string]struct{})
	truncated := false
	for _, key := range keys[sort.SearchStrings(keys, start):] {
		if prefix != "" {
			if key < prefix {
				continue
			}
			if !strings.HasPrefix(key, prefix) {
				break
			}
		}
		if token != "" {
			if contType == "K" && key <= contValue {
				continue
			}
			if contType == "C" && strings.HasPrefix(key, contValue) {
				continue
			}
		} else if startAfter != "" && key <= startAfter {
			continue
		}
		if delimiter != "" {
			relative := strings.TrimPrefix(key, prefix)
			if idx := strings.Index(relative, delimiter); idx >= 0 {
				commonPrefix := prefix + relative[:idx+len(delimiter)]
				if token == "" && startAfter != "" && commonPrefix <= startAfter {
					continue
				}
				if _, exists := seen[commonPrefix]; exists {
					continue
				}
				seen[commonPrefix] = struct{}{}
				if len(entries) >= maxKeys {
					truncated = true
					break
				}
				entries = append(entries, listRefEntry{commonPrefix: commonPrefix})
				continue
			}
		}
		if len(entries) >= maxKeys {
			truncated = true
			break
		}
		entries = append(entries, listRefEntry{key: key, object: true})
	}
	next := ""
	if truncated && len(entries) > 0 {
		last := entries[len(entries)-1]
		if last.object {
			next = base64.StdEncoding.EncodeToString([]byte("K:" + last.key))
		} else {
			next = base64.StdEncoding.EncodeToString([]byte("C:" + last.commonPrefix))
		}
	}
	return entries, truncated, next
}

func expectedV1Page(bucket string, manifests map[string]models.ObjectManifest, keys []string, prefix, delimiter, marker string, maxKeys int, encodingType string) models.ListBucketResultV1 {
	entries, truncated, next := referenceV1Page(keys, prefix, delimiter, marker, maxKeys)
	result := models.ListBucketResultV1{
		Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:         bucket,
		Prefix:       s3EncodeIfNeeded(prefix, encodingType),
		Marker:       s3EncodeIfNeeded(marker, encodingType),
		Delimiter:    s3EncodeIfNeeded(delimiter, encodingType),
		MaxKeys:      maxKeys,
		EncodingType: encodingType,
		IsTruncated:  truncated,
	}
	if next != "" {
		result.NextMarker = s3EncodeIfNeeded(next, encodingType)
	}
	for _, entry := range entries {
		if entry.object {
			manifest := manifests[entry.key]
			result.Contents = append(result.Contents, models.Contents{
				Key:          s3EncodeIfNeeded(entry.key, encodingType),
				LastModified: time.Unix(manifest.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
				ETag:         `"` + manifest.ETag + `"`,
				Size:         manifest.Size,
				StorageClass: "STANDARD",
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, models.CommonPrefixes{
				Prefix: s3EncodeIfNeeded(entry.commonPrefix, encodingType),
			})
		}
	}
	return result
}

func expectedV2Page(bucket string, manifests map[string]models.ObjectManifest, keys []string, prefix, delimiter, startAfter, token string, maxKeys int, encodingType string) models.ListBucketResultV2 {
	entries, truncated, next := referenceV2Page(keys, prefix, delimiter, startAfter, token, maxKeys)
	result := models.ListBucketResultV2{
		Xmlns:             "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:              bucket,
		Prefix:            s3EncodeIfNeeded(prefix, encodingType),
		Delimiter:         s3EncodeIfNeeded(delimiter, encodingType),
		MaxKeys:           maxKeys,
		ContinuationToken: token,
		StartAfter:        s3EncodeIfNeeded(startAfter, encodingType),
		EncodingType:      encodingType,
		IsTruncated:       truncated,
		KeyCount:          len(entries),
	}
	if next != "" && truncated && len(entries) > 0 {
		result.NextContinuationToken = next
	}
	for _, entry := range entries {
		if entry.object {
			manifest := manifests[entry.key]
			result.Contents = append(result.Contents, models.Contents{
				Key:          s3EncodeIfNeeded(entry.key, encodingType),
				LastModified: time.Unix(manifest.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
				ETag:         `"` + manifest.ETag + `"`,
				Size:         manifest.Size,
				StorageClass: "STANDARD",
			})
		} else {
			result.CommonPrefixes = append(result.CommonPrefixes, models.CommonPrefixes{
				Prefix: s3EncodeIfNeeded(entry.commonPrefix, encodingType),
			})
		}
	}
	return result
}

func requestListV1(t *testing.T, handler *Handler, bucket string, params url.Values) models.ListBucketResultV1 {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("V1 status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var result models.ListBucketResultV1
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal V1: %v (body %s)", err, rec.Body.String())
	}
	return result
}

func requestListV2(t *testing.T, handler *Handler, bucket string, params url.Values) models.ListBucketResultV2 {
	t.Helper()
	params.Set("list-type", "2")
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+params.Encode(), nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("V2 status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var result models.ListBucketResultV2
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal V2: %v (body %s)", err, rec.Body.String())
	}
	return result
}

func compareV1(t *testing.T, label string, got, want models.ListBucketResultV1) {
	t.Helper()
	got.XMLName = xml.Name{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: V1 response mismatch\n got: %+v\nwant: %+v", label, got, want)
	}
}

func compareV2(t *testing.T, label string, got, want models.ListBucketResultV2) {
	t.Helper()
	got.XMLName = xml.Name{}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: V2 response mismatch\n got: %+v\nwant: %+v", label, got, want)
	}
}

func randomizedListKeys() []string {
	rng := rand.New(rand.NewSource(20261020))
	groups := []string{
		"docs", "images", "sp ace", "uni-é-日本", "plus+key", "pct%key",
		"amp&key", "eq=key", "quote'key", `dq"key`, "hash#key", "q?key",
	}
	suffixes := []string{
		".txt", ".bin", "", " two words", "#frag", "?query", "~tilde",
		"'single'", `"double"`, "[brackets]", "+plus%pct&and=eq",
	}
	keys := make([]string, 0, 1600)
	for i := 0; i < 1500; i++ {
		group := groups[rng.Intn(len(groups))]
		sub := fmt.Sprintf("sub-%02d", rng.Intn(17))
		suffix := suffixes[rng.Intn(len(suffixes))]
		keys = append(keys, fmt.Sprintf("%s/%s/object-%04d%s", group, sub, i, suffix))
	}
	for i, group := range groups {
		keys = append(keys, fmt.Sprintf("%s/dir/flat-%02d", group, i))
		keys = append(keys, group+"-root")
	}
	keys = append(keys,
		"docs/",
		"sp ace/key with spaces",
		"uni-é-日本/日本.txt",
		"zzz-last",
	)
	sort.Strings(keys)
	unique := keys[:0]
	for _, key := range keys {
		if len(unique) == 0 || unique[len(unique)-1] != key {
			unique = append(unique, key)
		}
	}
	return unique
}

func TestRandomizedListV1V2MatchesReference(t *testing.T) {
	const bucket = "list-scale-bucket"
	keys := randomizedListKeys()
	manifests := make(map[string]models.ObjectManifest, len(keys))
	ordered := make([]models.ObjectManifest, len(keys))
	for i, key := range keys {
		manifest := models.ObjectManifest{
			Bucket:    bucket,
			Key:       key,
			Size:      int64(i*7 + 1),
			ETag:      fmt.Sprintf("etag-%04d", i),
			CreatedAt: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC).Unix() + int64(i%97),
		}
		manifests[key] = manifest
		ordered[i] = manifest
	}
	handler := newBulkListHandler(t, bucket, ordered)

	prefixes := []string{"", "docs/", "sp ace/", "uni-é-日本/", "images/sub-0", "nonexistent/"}
	delimiters := []string{"", "/"}
	markers := []string{"", keys[0], keys[len(keys)/2], "docs/", "images/sub-1", "sp ace/key with spaces", "zzz"}
	limits := []int{0, 1, 2, 7, 1000}
	encodings := []string{"", "url"}

	// V1: single-page behavior across the full filter cross product.
	for _, prefix := range prefixes {
		for _, delimiter := range delimiters {
			for _, marker := range markers {
				for _, maxKeys := range limits {
					for _, encoding := range encodings {
						params := url.Values{}
						params.Set("prefix", prefix)
						params.Set("delimiter", delimiter)
						params.Set("marker", marker)
						params.Set("max-keys", strconv.Itoa(maxKeys))
						if encoding != "" {
							params.Set("encoding-type", encoding)
						}
						label := fmt.Sprintf("V1 prefix=%q delimiter=%q marker=%q max=%d enc=%q", prefix, delimiter, marker, maxKeys, encoding)
						got := requestListV1(t, handler, bucket, params)
						want := expectedV1Page(bucket, manifests, keys, prefix, delimiter, marker, maxKeys, encoding)
						compareV1(t, label, got, want)
					}
				}
			}
		}
	}

	// V1: walk pages via NextMarker and compare every page plus the final stop.
	for _, delimiter := range delimiters {
		for _, maxKeys := range []int{3, 7, 1000} {
			for _, encoding := range encodings {
				marker := ""
				seen := 0
				for page := 0; ; page++ {
					if page > 10000 {
						t.Fatal("V1 pagination did not terminate")
					}
					params := url.Values{}
					params.Set("delimiter", delimiter)
					params.Set("marker", marker)
					params.Set("max-keys", strconv.Itoa(maxKeys))
					if encoding != "" {
						params.Set("encoding-type", encoding)
					}
					label := fmt.Sprintf("V1 page delimiter=%q marker=%q max=%d enc=%q page=%d", delimiter, marker, maxKeys, encoding, page)
					got := requestListV1(t, handler, bucket, params)
					want := expectedV1Page(bucket, manifests, keys, "", delimiter, marker, maxKeys, encoding)
					compareV1(t, label, got, want)
					seen += len(got.Contents) + len(got.CommonPrefixes)
					if !got.IsTruncated {
						break
					}
					if got.NextMarker == "" {
						t.Fatalf("%s: truncated without NextMarker", label)
					}
					// NextMarker is URL-encoded in the response when requested;
					// decode it in the same way the query decoder would.
					marker = got.NextMarker
					if encoding == "url" {
						decoded, err := url.QueryUnescape(marker)
						if err != nil {
							t.Fatalf("%s: unescape NextMarker: %v", label, err)
						}
						marker = decoded
					}
				}
			}
		}
	}

	// V2: single-page behavior with start-after.
	startAfters := []string{"", keys[0], keys[10], "docs/", "sp ace/key with spaces", "zzz"}
	for _, prefix := range prefixes {
		for _, delimiter := range delimiters {
			for _, startAfter := range startAfters {
				for _, maxKeys := range []int{0, 1, 7, 1000} {
					for _, encoding := range encodings {
						params := url.Values{}
						params.Set("prefix", prefix)
						params.Set("delimiter", delimiter)
						params.Set("start-after", startAfter)
						params.Set("max-keys", strconv.Itoa(maxKeys))
						if encoding != "" {
							params.Set("encoding-type", encoding)
						}
						label := fmt.Sprintf("V2 prefix=%q delimiter=%q start-after=%q max=%d enc=%q", prefix, delimiter, startAfter, maxKeys, encoding)
						got := requestListV2(t, handler, bucket, params)
						want := expectedV2Page(bucket, manifests, keys, prefix, delimiter, startAfter, "", maxKeys, encoding)
						compareV2(t, label, got, want)
					}
				}
			}
		}
	}

	// V2: full pagination via continuation tokens (both K: and C: markers).
	paginationConfigs := []struct {
		prefix    string
		delimiter string
		maxKeys   int
	}{
		{"", "/", 7},
		{"docs/", "/", 5},
		{"", "", 13},
		{"sp ace/", "/", 4},
		{"uni-é-日本/", "", 9},
		{"", "/", 1000},
	}
	for _, cfg := range paginationConfigs {
		for _, encoding := range encodings {
			token := ""
			seen := 0
			for page := 0; ; page++ {
				if page > 10000 {
					t.Fatal("V2 pagination did not terminate")
				}
				params := url.Values{}
				params.Set("prefix", cfg.prefix)
				params.Set("delimiter", cfg.delimiter)
				params.Set("max-keys", strconv.Itoa(cfg.maxKeys))
				if token != "" {
					params.Set("continuation-token", token)
				}
				if encoding != "" {
					params.Set("encoding-type", encoding)
				}
				label := fmt.Sprintf("V2 page prefix=%q delimiter=%q max=%d enc=%q page=%d", cfg.prefix, cfg.delimiter, cfg.maxKeys, encoding, page)
				got := requestListV2(t, handler, bucket, params)
				want := expectedV2Page(bucket, manifests, keys, cfg.prefix, cfg.delimiter, "", token, cfg.maxKeys, encoding)
				compareV2(t, label, got, want)
				seen += got.KeyCount
				if !got.IsTruncated {
					break
				}
				if got.NextContinuationToken == "" {
					t.Fatalf("%s: truncated without NextContinuationToken", label)
				}
				token = got.NextContinuationToken
			}
			if len(cfg.prefix) == 0 && cfg.delimiter == "/" && seen == 0 {
				t.Fatalf("V2 delimiter pagination emitted no entries")
			}
		}
	}
}
