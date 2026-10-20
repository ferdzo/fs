package api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"fs/auth"
	"fs/models"
)

func listMultipartUploadsRequest(t *testing.T, handler *Handler, bucket string, params url.Values) (int, listMultipartUploadsResult, string) {
	t.Helper()
	target := "/" + bucket + "?uploads"
	if len(params) > 0 {
		target += "&" + params.Encode()
	}
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	var result listMultipartUploadsResult
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal ListMultipartUploads: %v (body: %s)", err, rec.Body.String())
		}
	}
	return rec.Code, result, rec.Body.String()
}

// initiateMultipartForList creates an upload for a key that may contain spaces,
// which must be percent-encoded in the request path.
func initiateMultipartForList(t *testing.T, handler *Handler, bucket, key string) string {
	t.Helper()
	escapedKey := strings.ReplaceAll(key, " ", "%20")
	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"/"+escapedKey+"?uploads", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate %q status = %d, want 200 (body: %s)", key, rec.Code, rec.Body.String())
	}
	var result models.InitiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal initiate %q: %v", key, err)
	}
	return result.UploadID
}

func uploadKeys(result listMultipartUploadsResult) []string {
	keys := make([]string, 0, len(result.Uploads))
	for _, upload := range result.Uploads {
		keys = append(keys, upload.Key)
	}
	return keys
}

func commonPrefixValues(result listMultipartUploadsResult) []string {
	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, cp := range result.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	return prefixes
}

func assertStringSlice(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}

func TestListMultipartUploadsPrefixAndDelimiter(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	for _, key := range []string{
		"alpha.txt",
		"dir/one.txt",
		"dir/sub/deep.txt",
		"dir/two.txt",
		"other.txt",
		"plus+key.txt",
		"space key.txt",
	} {
		initiateMultipartForList(t, handler, bucket, key)
	}

	code, result, body := listMultipartUploadsRequest(t, handler, bucket, url.Values{"prefix": {"dir/"}})
	if code != http.StatusOK {
		t.Fatalf("prefix list status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "prefix uploads", uploadKeys(result), []string{
		"dir/one.txt", "dir/sub/deep.txt", "dir/two.txt",
	})
	if len(result.CommonPrefixes) != 0 {
		t.Fatalf("prefix without delimiter returned common prefixes: %v", commonPrefixValues(result))
	}

	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{"delimiter": {"/"}})
	if code != http.StatusOK {
		t.Fatalf("delimiter list status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "delimiter uploads", uploadKeys(result), []string{
		"alpha.txt", "other.txt", "plus+key.txt", "space key.txt",
	})
	assertStringSlice(t, "common prefixes", commonPrefixValues(result), []string{"dir/"})
	if result.IsTruncated {
		t.Fatal("delimiter list unexpectedly truncated")
	}

	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{
		"prefix":    {"dir/"},
		"delimiter": {"/"},
	})
	if code != http.StatusOK {
		t.Fatalf("nested delimiter list status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "nested uploads", uploadKeys(result), []string{"dir/one.txt", "dir/two.txt"})
	assertStringSlice(t, "nested common prefixes", commonPrefixValues(result), []string{"dir/sub/"})
}

func TestListMultipartUploadsPaginationWalk(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	want := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for _, key := range want {
		initiateMultipartForList(t, handler, bucket, key)
	}

	for _, pageSize := range []int{1, 2} {
		t.Run("max-uploads-"+strconv.Itoa(pageSize), func(t *testing.T) {
			var got []string
			marker := url.Values{"max-uploads": {strconv.Itoa(pageSize)}}
			for page := 0; page < 10; page++ {
				code, result, body := listMultipartUploadsRequest(t, handler, bucket, marker)
				if code != http.StatusOK {
					t.Fatalf("page %d status = %d (body: %s)", page, code, body)
				}
				got = append(got, uploadKeys(result)...)
				if !result.IsTruncated {
					break
				}
				if result.NextKeyMarker == "" {
					t.Fatalf("page %d truncated without NextKeyMarker", page)
				}
				marker = url.Values{
					"max-uploads":      {strconv.Itoa(pageSize)},
					"key-marker":       {result.NextKeyMarker},
					"upload-id-marker": {result.NextUploadIDMarker},
				}
			}
			assertStringSlice(t, "walked uploads", got, want)
		})
	}
}

func TestListMultipartUploadsPaginationWithDelimiter(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	for _, key := range []string{"a/1.txt", "b/1.txt", "c/1.txt", "d/1.txt"} {
		initiateMultipartForList(t, handler, bucket, key)
	}

	code, result, body := listMultipartUploadsRequest(t, handler, bucket, url.Values{
		"delimiter":   {"/"},
		"max-uploads": {"2"},
	})
	if code != http.StatusOK {
		t.Fatalf("page 1 status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "page 1 common prefixes", commonPrefixValues(result), []string{"a/", "b/"})
	if !result.IsTruncated || result.NextKeyMarker != "b/" || result.NextUploadIDMarker != "" {
		t.Fatalf("page 1 next = %q/%q truncated=%v, want b/ and empty",
			result.NextKeyMarker, result.NextUploadIDMarker, result.IsTruncated)
	}

	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{
		"delimiter":   {"/"},
		"max-uploads": {"2"},
		"key-marker":  {result.NextKeyMarker},
	})
	if code != http.StatusOK {
		t.Fatalf("page 2 status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "page 2 common prefixes", commonPrefixValues(result), []string{"c/", "d/"})
	if result.IsTruncated {
		t.Fatalf("page 2 unexpectedly truncated (body: %s)", body)
	}
}

func TestListMultipartUploadsPaginationSameKey(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	want := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		want[initiateMultipartForList(t, handler, bucket, "same.txt")] = struct{}{}
	}

	marker := url.Values{"max-uploads": {"1"}}
	seen := make(map[string]struct{})
	for page := 0; page < 10; page++ {
		code, result, body := listMultipartUploadsRequest(t, handler, bucket, marker)
		if code != http.StatusOK {
			t.Fatalf("page %d status = %d (body: %s)", page, code, body)
		}
		for _, upload := range result.Uploads {
			if upload.Key != "same.txt" {
				t.Fatalf("page %d key = %q, want same.txt", page, upload.Key)
			}
			if _, dup := seen[upload.UploadID]; dup {
				t.Fatalf("upload %s returned twice", upload.UploadID)
			}
			seen[upload.UploadID] = struct{}{}
		}
		if !result.IsTruncated {
			break
		}
		marker = url.Values{
			"max-uploads":      {"1"},
			"key-marker":       {result.NextKeyMarker},
			"upload-id-marker": {result.NextUploadIDMarker},
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("walked %d uploads, want %d", len(seen), len(want))
	}
	for id := range want {
		if _, ok := seen[id]; !ok {
			t.Fatalf("upload %s missing from walk", id)
		}
	}
}

func TestListMultipartUploadsEmptyAndLimits(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	code, result, body := listMultipartUploadsRequest(t, handler, bucket, nil)
	if code != http.StatusOK {
		t.Fatalf("empty bucket status = %d (body: %s)", code, body)
	}
	if len(result.Uploads) != 0 || len(result.CommonPrefixes) != 0 || result.IsTruncated {
		t.Fatalf("empty bucket result = %+v", result)
	}
	if result.MaxUploads != 1000 {
		t.Fatalf("default MaxUploads = %d, want 1000", result.MaxUploads)
	}

	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{"max-uploads": {"0"}})
	if code != http.StatusOK || len(result.Uploads) != 0 || result.IsTruncated {
		t.Fatalf("max-uploads=0 status/result = %d/%+v (body: %s)", code, result, body)
	}

	initiateMultipartForList(t, handler, bucket, "one.txt")
	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{"max-uploads": {"5000"}})
	if code != http.StatusOK {
		t.Fatalf("clamped max-uploads status = %d (body: %s)", code, body)
	}
	if result.MaxUploads != 1000 {
		t.Fatalf("clamped MaxUploads = %d, want 1000", result.MaxUploads)
	}

	for _, raw := range []string{"-1", "not-a-number"} {
		code, _, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{"max-uploads": {raw}})
		if code != http.StatusBadRequest || !strings.Contains(body, "InvalidArgument") {
			t.Fatalf("max-uploads=%q status/body = %d/%s, want 400 InvalidArgument", raw, code, body)
		}
	}

	code, _, body = listMultipartUploadsRequest(t, handler, "missing-bucket", nil)
	if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
		t.Fatalf("missing bucket status/body = %d/%s, want 404 NoSuchBucket", code, body)
	}
	code, _, body = listMultipartUploadsRequest(t, handler, "missing-bucket", url.Values{"max-uploads": {"0"}})
	if code != http.StatusNotFound || !strings.Contains(body, "NoSuchBucket") {
		t.Fatalf("missing bucket max-uploads=0 status/body = %d/%s, want 404 NoSuchBucket", code, body)
	}
}

func TestListMultipartUploadsEncodingTypeURL(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	initiateMultipartForList(t, handler, bucket, "space key.txt")
	initiateMultipartForList(t, handler, bucket, "plus+key.txt")

	code, result, body := listMultipartUploadsRequest(t, handler, bucket, url.Values{
		"encoding-type": {"url"},
		"prefix":        {"space "},
	})
	if code != http.StatusOK {
		t.Fatalf("encoding list status = %d (body: %s)", code, body)
	}
	if result.EncodingType != "url" {
		t.Fatalf("EncodingType = %q, want url", result.EncodingType)
	}
	if result.Prefix != "space%20" {
		t.Fatalf("encoded Prefix = %q, want space%%20", result.Prefix)
	}
	assertStringSlice(t, "encoded upload keys", uploadKeys(result), []string{"space%20key.txt"})

	code, result, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{
		"encoding-type": {"url"},
		"prefix":        {"plus+"},
	})
	if code != http.StatusOK {
		t.Fatalf("plus encoding list status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "plus upload keys", uploadKeys(result), []string{"plus%2Bkey.txt"})
}

func TestListMultipartUploadsOmitsCompletedAndAborted(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	pendingID := initiateMultipartForList(t, handler, bucket, "life/pending.bin")

	completedKey := "life/completed.bin"
	completedID := initiateMultipartForList(t, handler, bucket, completedKey)
	etag := multipartLifeUploadPart(t, handler, bucket, completedKey, completedID, 1, []byte("completed payload"))
	completeRec := multipartLifeComplete(t, handler, bucket, completedKey, completedID, []models.CompletedPart{
		{PartNumber: 1, ETag: etag},
	})
	if completeRec.Code != http.StatusOK {
		t.Fatalf("complete status = %d (body: %s)", completeRec.Code, completeRec.Body.String())
	}

	abortedKey := "life/aborted.bin"
	abortedID := initiateMultipartForList(t, handler, bucket, abortedKey)
	abortReq := httptest.NewRequest(http.MethodDelete,
		"/"+bucket+"/"+abortedKey+"?uploadId="+url.QueryEscape(abortedID), nil)
	abortRec := httptest.NewRecorder()
	handler.router.ServeHTTP(abortRec, abortReq)
	if abortRec.Code != http.StatusNoContent {
		t.Fatalf("abort status = %d, want 204 (body: %s)", abortRec.Code, abortRec.Body.String())
	}

	code, result, body := listMultipartUploadsRequest(t, handler, bucket, nil)
	if code != http.StatusOK {
		t.Fatalf("list status = %d (body: %s)", code, body)
	}
	assertStringSlice(t, "pending uploads", uploadKeys(result), []string{"life/pending.bin"})
	if len(result.Uploads) != 1 || result.Uploads[0].UploadID != pendingID {
		t.Fatalf("pending upload = %+v, want upload id %s", result.Uploads, pendingID)
	}
	for _, upload := range result.Uploads {
		if upload.UploadID == completedID || upload.UploadID == abortedID {
			t.Fatalf("non-pending upload %s was listed", upload.UploadID)
		}
	}

	code, _, body = listMultipartUploadsRequest(t, handler, bucket, url.Values{"prefix": {"life/"}})
	if code != http.StatusOK {
		t.Fatalf("prefix list status = %d (body: %s)", code, body)
	}
}

func TestListMultipartUploadsRequiresListBucket(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()

	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := svc.CreateMultipartUpload(bucket, "allowed/a.bin"); err != nil {
		t.Fatalf("create allowed upload: %v", err)
	}
	if _, err := svc.CreateMultipartUpload(bucket, "private/b.bin"); err != nil {
		t.Fatalf("create private upload: %v", err)
	}

	if _, err := authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: "uploads-list-user",
		SecretKey:   "uploads-list-secret-1",
		Policy: models.AuthPolicy{
			Statements: []models.AuthPolicyStatement{
				{
					Effect:  "allow",
					Actions: []string{"s3:ListBucket"},
					Bucket:  bucket,
					Prefix:  "allowed/",
				},
			},
		},
	}); err != nil {
		t.Fatalf("create list user: %v", err)
	}

	unsigned := httptest.NewRequest(http.MethodGet, "/"+bucket+"?uploads", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, unsigned)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("unsigned status/body = %d/%s, want 403 AccessDenied", rec.Code, rec.Body.String())
	}

	noPrefix := httptest.NewRequest(http.MethodGet, "/"+bucket+"?uploads", nil)
	signTestSigV4Request(t, noPrefix, "uploads-list-user", "uploads-list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, noPrefix)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("signed no-prefix status/body = %d/%s, want 403 AccessDenied", rec.Code, rec.Body.String())
	}

	allowed := httptest.NewRequest(http.MethodGet, "/"+bucket+"?uploads&prefix=allowed%2F", nil)
	signTestSigV4Request(t, allowed, "uploads-list-user", "uploads-list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, allowed)
	if rec.Code != http.StatusOK {
		t.Fatalf("signed allowed status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var result listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal allowed list: %v (body: %s)", err, rec.Body.String())
	}
	assertStringSlice(t, "authorized uploads", uploadKeys(result), []string{"allowed/a.bin"})

	denied := httptest.NewRequest(http.MethodGet, "/"+bucket+"?uploads&prefix=private%2F", nil)
	signTestSigV4Request(t, denied, "uploads-list-user", "uploads-list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, denied)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("signed private status/body = %d/%s, want 403 AccessDenied", rec.Code, rec.Body.String())
	}
}
