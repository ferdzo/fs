package api

import (
	"bytes"
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"fs/models"
)

const multipartLifeMinPartSize = 5 * 1024 * 1024

func multipartLifeInit(t *testing.T, handler *Handler, bucket, key string) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"/"+key+"?uploads", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("initiate multipart %q status = %d, want 200 (body: %s)", key, rec.Code, rec.Body.String())
	}

	var result models.InitiateMultipartUploadResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal initiate multipart response: %v (body: %s)", err, rec.Body.String())
	}
	if result.UploadID == "" {
		t.Fatalf("initiate multipart response has no UploadId: %s", rec.Body.String())
	}
	if result.Bucket != bucket || result.Key != key {
		t.Fatalf("initiate multipart Bucket/Key = %q/%q, want %q/%q", result.Bucket, result.Key, bucket, key)
	}
	return result.UploadID
}

func multipartLifeUploadPart(t *testing.T, handler *Handler, bucket, key, uploadID string, partNumber int, data []byte) string {
	t.Helper()

	target := "/" + bucket + "/" + key +
		"?uploadId=" + url.QueryEscape(uploadID) +
		"&partNumber=" + strconv.Itoa(partNumber)
	req := httptest.NewRequest(http.MethodPut, target, bytes.NewReader(data))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload part %d status = %d, want 200 (body: %s)", partNumber, rec.Code, rec.Body.String())
	}

	etag := strings.Trim(rec.Header().Get("ETag"), `"`)
	if etag == "" {
		t.Fatalf("upload part %d returned no ETag", partNumber)
	}
	return etag
}

func multipartLifeUploadPartResponse(t *testing.T, handler *Handler, bucket, key, uploadID string, partNumber int, data []byte) *httptest.ResponseRecorder {
	t.Helper()

	target := "/" + bucket + "/" + key +
		"?uploadId=" + url.QueryEscape(uploadID) +
		"&partNumber=" + strconv.Itoa(partNumber)
	req := httptest.NewRequest(http.MethodPut, target, bytes.NewReader(data))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	return rec
}

func multipartLifeListParts(t *testing.T, handler *Handler, bucket, key, uploadID string) (int, listPartsResponse, string) {
	t.Helper()
	return multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID, nil)
}

func multipartLifeListPartsWithParams(t *testing.T, handler *Handler, bucket, key, uploadID string, params url.Values) (int, listPartsResponse, string) {
	t.Helper()

	query := url.Values{}
	for name, values := range params {
		query[name] = append([]string(nil), values...)
	}
	query.Set("uploadId", uploadID)
	target := "/" + bucket + "/" + key + "?" + query.Encode()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	var result listPartsResponse
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal list parts response: %v (body: %s)", err, rec.Body.String())
		}
	}
	return rec.Code, result, rec.Body.String()
}

func multipartLifeComplete(t *testing.T, handler *Handler, bucket, key, uploadID string, parts []models.CompletedPart) *httptest.ResponseRecorder {
	t.Helper()

	type completeRequest struct {
		XMLName xml.Name               `xml:"CompleteMultipartUpload"`
		Parts   []models.CompletedPart `xml:"Part"`
	}
	payload, err := xml.Marshal(completeRequest{Parts: parts})
	if err != nil {
		t.Fatalf("marshal complete multipart request: %v", err)
	}

	target := "/" + bucket + "/" + key + "?uploadId=" + url.QueryEscape(uploadID)
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(payload))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	return rec
}

func multipartLifeCompleteRaw(t *testing.T, handler *Handler, bucket, key, uploadID, body string) *httptest.ResponseRecorder {
	t.Helper()

	target := "/" + bucket + "/" + key + "?uploadId=" + url.QueryEscape(uploadID)
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	return rec
}

func multipartLifeErrorCode(t *testing.T, body string) string {
	t.Helper()

	var s3Err models.S3ErrorResponse
	if err := xml.Unmarshal([]byte(body), &s3Err); err != nil {
		t.Fatalf("unmarshal S3 error response: %v (body: %s)", err, body)
	}
	return s3Err.Code
}

func multipartLifeExpectedETag(parts ...[]byte) string {
	hasher := md5.New()
	for _, part := range parts {
		sum := md5.Sum(part)
		_, _ = hasher.Write(sum[:])
	}
	return fmt.Sprintf("%x-%d", hasher.Sum(nil), len(parts))
}

func multipartLifeAssertError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantCode string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, wantStatus, rec.Body.String())
	}
	if code := multipartLifeErrorCode(t, rec.Body.String()); code != wantCode {
		t.Fatalf("error code = %q, want %q (body: %s)", code, wantCode, rec.Body.String())
	}
}

func TestMultipartLifecycleHappyPath(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const key = "multipart/object.bin"
	part1 := bytes.Repeat([]byte("A"), multipartLifeMinPartSize)
	part2 := bytes.Repeat([]byte("B"), 1024)

	uploadID := multipartLifeInit(t, handler, bucket, key)

	// Upload out of order: part 2 first, then part 1.
	etag2 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 2, part2)
	etag1 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 1, part1)

	code, listed, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
	if code != http.StatusOK {
		t.Fatalf("list parts status = %d, want 200 (body: %s)", code, body)
	}
	if listed.Bucket != bucket || listed.Key != key || listed.UploadID != uploadID {
		t.Fatalf("list parts Bucket/Key/UploadId = %q/%q/%q, want %q/%q/%q",
			listed.Bucket, listed.Key, listed.UploadID, bucket, key, uploadID)
	}
	if len(listed.Parts) != 2 {
		t.Fatalf("list parts returned %d parts, want 2 (body: %s)", len(listed.Parts), body)
	}
	if listed.Parts[0].PartNumber != 1 || listed.Parts[1].PartNumber != 2 {
		t.Fatalf("part numbers = %d,%d, want 1,2", listed.Parts[0].PartNumber, listed.Parts[1].PartNumber)
	}
	if listed.Parts[0].Size != int64(len(part1)) || listed.Parts[1].Size != int64(len(part2)) {
		t.Fatalf("part sizes = %d,%d, want %d,%d",
			listed.Parts[0].Size, listed.Parts[1].Size, len(part1), len(part2))
	}
	if listed.Parts[0].ETag != `"`+etag1+`"` || listed.Parts[1].ETag != `"`+etag2+`"` {
		t.Fatalf("part ETags = %s,%s, want %q,%q",
			listed.Parts[0].ETag, listed.Parts[1].ETag, `"`+etag1+`"`, `"`+etag2+`"`)
	}
	if listed.Parts[0].LastModified == "" || listed.Parts[1].LastModified == "" {
		t.Fatal("list parts response is missing LastModified")
	}
	if listed.PartNumberMarker != 0 || listed.NextPartNumberMarker != 0 || listed.MaxParts != 1000 || listed.IsTruncated {
		t.Fatalf("default pagination fields = marker %d, next %d, max %d, truncated %v; want 0/0/1000/false",
			listed.PartNumberMarker, listed.NextPartNumberMarker, listed.MaxParts, listed.IsTruncated)
	}
	multipartLifeBodyContains(t, body,
		"<PartNumber>1</PartNumber>",
		"<PartNumber>2</PartNumber>",
		"<Size>"+strconv.Itoa(len(part1))+"</Size>",
	)

	completeRec := multipartLifeComplete(t, handler, bucket, key, uploadID, []models.CompletedPart{
		{PartNumber: 1, ETag: `"` + strings.ToUpper(etag1) + `"`},
		{PartNumber: 2, ETag: `"` + etag2 + `"`},
	})
	if completeRec.Code != http.StatusOK {
		t.Fatalf("complete multipart status = %d, want 200 (body: %s)", completeRec.Code, completeRec.Body.String())
	}
	var completed models.CompleteMultipartUploadResult
	if err := xml.Unmarshal(completeRec.Body.Bytes(), &completed); err != nil {
		t.Fatalf("unmarshal complete multipart response: %v (body: %s)", err, completeRec.Body.String())
	}

	expectedETag := multipartLifeExpectedETag(part1, part2)
	if completed.Bucket != bucket || completed.Key != key {
		t.Fatalf("complete result Bucket/Key = %q/%q, want %q/%q", completed.Bucket, completed.Key, bucket, key)
	}
	if completed.ETag != `"`+expectedETag+`"` {
		t.Fatalf("complete result ETag = %q, want %q", completed.ETag, `"`+expectedETag+`"`)
	}
	if !strings.HasSuffix(expectedETag, "-2") {
		t.Fatalf("multipart ETag %q does not have the -N form", expectedETag)
	}
	if completed.Location != "/"+bucket+"/"+key {
		t.Fatalf("complete result Location = %q, want %q", completed.Location, "/"+bucket+"/"+key)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	getRec := httptest.NewRecorder()
	handler.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get completed object status = %d, want 200 (body: %s)", getRec.Code, getRec.Body.String())
	}
	wantBody := append(append([]byte(nil), part1...), part2...)
	if !bytes.Equal(getRec.Body.Bytes(), wantBody) {
		t.Fatalf("completed object body length = %d, want %d", getRec.Body.Len(), len(wantBody))
	}
	if got := getRec.Header().Get("ETag"); got != `"`+expectedETag+`"` {
		t.Fatalf("get completed object ETag = %q, want %q", got, `"`+expectedETag+`"`)
	}

	manifest, err := svc.HeadObject(bucket, key)
	if err != nil {
		t.Fatalf("head completed object: %v", err)
	}
	if manifest.Size != int64(len(wantBody)) || manifest.ETag != expectedETag {
		t.Fatalf("completed manifest Size/ETag = %d/%q, want %d/%q",
			manifest.Size, manifest.ETag, len(wantBody), expectedETag)
	}

	// A completed upload id is no longer a valid lifecycle target.
	code, _, body = multipartLifeListParts(t, handler, bucket, key, uploadID)
	if code != http.StatusNotFound {
		t.Fatalf("list parts after complete status = %d, want 404 (body: %s)", code, body)
	}
	if code := multipartLifeErrorCode(t, body); code != "NoSuchUpload" {
		t.Fatalf("list parts after complete error code = %q, want NoSuchUpload", code)
	}
	multipartLifeAssertError(t,
		multipartLifeUploadPartResponse(t, handler, bucket, key, uploadID, 3, []byte("late")),
		http.StatusNotFound, "NoSuchUpload")
	multipartLifeAssertError(t,
		multipartLifeComplete(t, handler, bucket, key, uploadID, []models.CompletedPart{
			{PartNumber: 1, ETag: `"` + etag1 + `"`},
		}),
		http.StatusNotFound, "NoSuchUpload")
	secondAbort := httptest.NewRequest(http.MethodDelete,
		"/"+bucket+"/"+key+"?uploadId="+url.QueryEscape(uploadID), nil)
	secondAbortRec := httptest.NewRecorder()
	handler.router.ServeHTTP(secondAbortRec, secondAbort)
	multipartLifeAssertError(t, secondAbortRec, http.StatusNotFound, "NoSuchUpload")
}

func multipartLifeBodyContains(t *testing.T, body string, snippets ...string) {
	t.Helper()
	for _, snippet := range snippets {
		if !strings.Contains(body, snippet) {
			t.Fatalf("response body missing %q: %s", snippet, body)
		}
	}
}

func TestMultipartLifecycleAbort(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const key = "multipart/aborted.bin"
	uploadID := multipartLifeInit(t, handler, bucket, key)
	etag := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 1, []byte("aborted part data"))

	abortReq := httptest.NewRequest(http.MethodDelete,
		"/"+bucket+"/"+key+"?uploadId="+url.QueryEscape(uploadID), nil)
	abortRec := httptest.NewRecorder()
	handler.router.ServeHTTP(abortRec, abortReq)
	if abortRec.Code != http.StatusNoContent {
		t.Fatalf("abort multipart status = %d, want 204 (body: %s)", abortRec.Code, abortRec.Body.String())
	}

	code, _, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
	if code != http.StatusNotFound {
		t.Fatalf("list parts after abort status = %d, want 404 (body: %s)", code, body)
	}
	if code := multipartLifeErrorCode(t, body); code != "NoSuchUpload" {
		t.Fatalf("list parts after abort error code = %q, want NoSuchUpload", code)
	}

	reuseRec := multipartLifeUploadPartResponse(t, handler, bucket, key, uploadID, 2, []byte("reuse"))
	multipartLifeAssertError(t, reuseRec, http.StatusNotFound, "NoSuchUpload")

	completeRec := multipartLifeComplete(t, handler, bucket, key, uploadID, []models.CompletedPart{
		{PartNumber: 1, ETag: `"` + etag + `"`},
	})
	multipartLifeAssertError(t, completeRec, http.StatusNotFound, "NoSuchUpload")

	secondAbort := httptest.NewRequest(http.MethodDelete,
		"/"+bucket+"/"+key+"?uploadId="+url.QueryEscape(uploadID), nil)
	secondAbortRec := httptest.NewRecorder()
	handler.router.ServeHTTP(secondAbortRec, secondAbort)
	multipartLifeAssertError(t, secondAbortRec, http.StatusNotFound, "NoSuchUpload")

	getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	getRec := httptest.NewRecorder()
	handler.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("get aborted object status = %d, want 404 (body: %s)", getRec.Code, getRec.Body.String())
	}
	if code := multipartLifeErrorCode(t, getRec.Body.String()); code != "NoSuchKey" {
		t.Fatalf("get aborted object error code = %q, want NoSuchKey", code)
	}
}

func TestMultipartLifecycleCompletionValidation(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const key = "multipart/validation.bin"
	part1 := bytes.Repeat([]byte("C"), multipartLifeMinPartSize)
	part2 := bytes.Repeat([]byte("D"), multipartLifeMinPartSize)
	uploadID := multipartLifeInit(t, handler, bucket, key)
	etag1 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 1, part1)
	etag2 := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 2, part2)

	cases := []struct {
		name       string
		parts      []models.CompletedPart
		wantStatus int
		wantCode   string
	}{
		{
			name: "wrong-etag",
			parts: []models.CompletedPart{
				{PartNumber: 1, ETag: `"deadbeefdeadbeefdeadbeefdeadbeef"`},
				{PartNumber: 2, ETag: `"` + etag2 + `"`},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidPart",
		},
		{
			name: "non-ascending",
			parts: []models.CompletedPart{
				{PartNumber: 2, ETag: `"` + etag2 + `"`},
				{PartNumber: 1, ETag: `"` + etag1 + `"`},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidPartOrder",
		},
		{
			name: "duplicate-part-number",
			parts: []models.CompletedPart{
				{PartNumber: 1, ETag: `"` + etag1 + `"`},
				{PartNumber: 1, ETag: `"` + etag1 + `"`},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidPartOrder",
		},
		{
			name: "missing-part",
			parts: []models.CompletedPart{
				{PartNumber: 1, ETag: `"` + etag1 + `"`},
				{PartNumber: 99, ETag: `"` + etag2 + `"`},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidPart",
		},
		{
			name: "part-never-uploaded",
			parts: []models.CompletedPart{
				{PartNumber: 10000, ETag: `"` + etag1 + `"`},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidPart",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := multipartLifeComplete(t, handler, bucket, key, uploadID, tc.parts)
			multipartLifeAssertError(t, rec, tc.wantStatus, tc.wantCode)

			code, listed, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
			if code != http.StatusOK || len(listed.Parts) != 2 {
				t.Fatalf("failed completion changed upload state: status=%d parts=%d (body: %s)",
					code, len(listed.Parts), body)
			}
		})
	}

	t.Run("empty-part-list", func(t *testing.T) {
		rec := multipartLifeComplete(t, handler, bucket, key, uploadID, nil)
		multipartLifeAssertError(t, rec, http.StatusBadRequest, "InvalidRequest")

		code, listed, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
		if code != http.StatusOK || len(listed.Parts) != 2 {
			t.Fatalf("empty completion changed upload state: status=%d parts=%d (body: %s)",
				code, len(listed.Parts), body)
		}
	})

	t.Run("malformed-xml", func(t *testing.T) {
		rec := multipartLifeCompleteRaw(t, handler, bucket, key, uploadID,
			"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>")
		multipartLifeAssertError(t, rec, http.StatusBadRequest, "MalformedXML")
	})

	t.Run("wrong-upload-id", func(t *testing.T) {
		rec := multipartLifeComplete(t, handler, bucket, key, "missing-upload-id", []models.CompletedPart{
			{PartNumber: 1, ETag: `"` + etag1 + `"`},
		})
		multipartLifeAssertError(t, rec, http.StatusNotFound, "NoSuchUpload")
	})

	t.Run("upload-id-for-different-key", func(t *testing.T) {
		otherUploadID := multipartLifeInit(t, handler, bucket, "multipart/other.bin")
		rec := multipartLifeComplete(t, handler, bucket, key, otherUploadID, []models.CompletedPart{
			{PartNumber: 1, ETag: `"` + etag1 + `"`},
		})
		multipartLifeAssertError(t, rec, http.StatusNotFound, "NoSuchUpload")
	})

	t.Run("entity-too-small", func(t *testing.T) {
		const smallKey = "multipart/small.bin"
		smallUploadID := multipartLifeInit(t, handler, bucket, smallKey)
		smallETag1 := multipartLifeUploadPart(t, handler, bucket, smallKey, smallUploadID, 1, []byte("small part one"))
		smallETag2 := multipartLifeUploadPart(t, handler, bucket, smallKey, smallUploadID, 2, []byte("small part two"))
		rec := multipartLifeComplete(t, handler, bucket, smallKey, smallUploadID, []models.CompletedPart{
			{PartNumber: 1, ETag: `"` + smallETag1 + `"`},
			{PartNumber: 2, ETag: `"` + smallETag2 + `"`},
		})
		multipartLifeAssertError(t, rec, http.StatusBadRequest, "EntityTooSmall")
	})
}

func multipartLifePartNumbers(parts []models.PartItem) []int {
	numbers := make([]int, 0, len(parts))
	for _, part := range parts {
		numbers = append(numbers, part.PartNumber)
	}
	return numbers
}

func multipartLifeAssertPartNumbers(t *testing.T, parts []models.PartItem, want ...int) {
	t.Helper()
	got := multipartLifePartNumbers(parts)
	if len(got) != len(want) {
		t.Fatalf("part numbers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("part numbers = %v, want %v", got, want)
		}
	}
}

func TestListPartsPagination(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const key = "multipart/pagination.bin"
	uploadID := multipartLifeInit(t, handler, bucket, key)
	for partNumber := 1; partNumber <= 5; partNumber++ {
		multipartLifeUploadPart(t, handler, bucket, key, uploadID, partNumber, []byte("part-"+strconv.Itoa(partNumber)))
	}

	code, page, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
	if code != http.StatusOK {
		t.Fatalf("default list status = %d, want 200 (body: %s)", code, body)
	}
	multipartLifeAssertPartNumbers(t, page.Parts, 1, 2, 3, 4, 5)
	if page.PartNumberMarker != 0 || page.NextPartNumberMarker != 0 || page.MaxParts != 1000 || page.IsTruncated {
		t.Fatalf("default pagination fields = marker %d, next %d, max %d, truncated %v; want 0/0/1000/false",
			page.PartNumberMarker, page.NextPartNumberMarker, page.MaxParts, page.IsTruncated)
	}
	multipartLifeBodyContains(t, body,
		"<PartNumberMarker>0</PartNumberMarker>",
		"<MaxParts>1000</MaxParts>",
		"<IsTruncated>false</IsTruncated>",
	)
	if strings.Contains(body, "<NextPartNumberMarker>") {
		t.Fatalf("non-truncated page has NextPartNumberMarker: %s", body)
	}

	code, page, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID,
		url.Values{"max-parts": {"2"}})
	if code != http.StatusOK {
		t.Fatalf("max-parts page 1 status = %d, want 200 (body: %s)", code, body)
	}
	multipartLifeAssertPartNumbers(t, page.Parts, 1, 2)
	if page.PartNumberMarker != 0 || page.NextPartNumberMarker != 2 || page.MaxParts != 2 || !page.IsTruncated {
		t.Fatalf("max-parts page 1 fields = marker %d, next %d, max %d, truncated %v; want 0/2/2/true",
			page.PartNumberMarker, page.NextPartNumberMarker, page.MaxParts, page.IsTruncated)
	}
	multipartLifeBodyContains(t, body,
		"<NextPartNumberMarker>2</NextPartNumberMarker>",
		"<MaxParts>2</MaxParts>",
		"<IsTruncated>true</IsTruncated>",
	)

	code, page, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID,
		url.Values{"part-number-marker": {"2"}, "max-parts": {"2"}})
	if code != http.StatusOK {
		t.Fatalf("max-parts page 2 status = %d, want 200 (body: %s)", code, body)
	}
	multipartLifeAssertPartNumbers(t, page.Parts, 3, 4)
	if page.PartNumberMarker != 2 || page.NextPartNumberMarker != 4 || page.MaxParts != 2 || !page.IsTruncated {
		t.Fatalf("max-parts page 2 fields = marker %d, next %d, max %d, truncated %v; want 2/4/2/true",
			page.PartNumberMarker, page.NextPartNumberMarker, page.MaxParts, page.IsTruncated)
	}

	code, page, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID,
		url.Values{"part-number-marker": {"4"}, "max-parts": {"2"}})
	if code != http.StatusOK {
		t.Fatalf("max-parts page 3 status = %d, want 200 (body: %s)", code, body)
	}
	multipartLifeAssertPartNumbers(t, page.Parts, 5)
	if page.PartNumberMarker != 4 || page.NextPartNumberMarker != 0 || page.MaxParts != 2 || page.IsTruncated {
		t.Fatalf("max-parts page 3 fields = marker %d, next %d, max %d, truncated %v; want 4/0/2/false",
			page.PartNumberMarker, page.NextPartNumberMarker, page.MaxParts, page.IsTruncated)
	}

	code, page, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID,
		url.Values{"part-number-marker": {"5"}})
	if code != http.StatusOK {
		t.Fatalf("marker past end status = %d, want 200 (body: %s)", code, body)
	}
	if len(page.Parts) != 0 || page.IsTruncated || page.NextPartNumberMarker != 0 {
		t.Fatalf("marker past end parts/truncated/next = %d/%v/%d, want 0/false/0",
			len(page.Parts), page.IsTruncated, page.NextPartNumberMarker)
	}

	code, page, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID,
		url.Values{"max-parts": {"5000"}})
	if code != http.StatusOK {
		t.Fatalf("clamped max-parts status = %d, want 200 (body: %s)", code, body)
	}
	multipartLifeAssertPartNumbers(t, page.Parts, 1, 2, 3, 4, 5)
	if page.MaxParts != 1000 || page.IsTruncated {
		t.Fatalf("clamped max-parts fields = max %d, truncated %v; want 1000/false",
			page.MaxParts, page.IsTruncated)
	}

	for _, params := range []url.Values{
		{"max-parts": {"not-a-number"}},
		{"max-parts": {"-1"}},
		{"part-number-marker": {"not-a-number"}},
		{"part-number-marker": {"-1"}},
	} {
		code, _, body = multipartLifeListPartsWithParams(t, handler, bucket, key, uploadID, params)
		if code != http.StatusBadRequest || !strings.Contains(body, "InvalidArgument") {
			t.Fatalf("params %v status/body = %d/%s, want 400 InvalidArgument", params, code, body)
		}
	}
}

func TestMultipartLifecyclePartNumberBounds(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const key = "multipart/bounds.bin"
	uploadID := multipartLifeInit(t, handler, bucket, key)

	for _, rawPartNumber := range []string{"0", "-1", "10001", "not-a-number"} {
		target := "/" + bucket + "/" + key +
			"?uploadId=" + url.QueryEscape(uploadID) +
			"&partNumber=" + url.QueryEscape(rawPartNumber)
		req := httptest.NewRequest(http.MethodPut, target, bytes.NewReader([]byte("data")))
		rec := httptest.NewRecorder()
		handler.router.ServeHTTP(rec, req)
		multipartLifeAssertError(t, rec, http.StatusBadRequest, "InvalidPart")
	}

	missingPartNumber := httptest.NewRequest(http.MethodPut,
		"/"+bucket+"/"+key+"?uploadId="+url.QueryEscape(uploadID), bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, missingPartNumber)
	multipartLifeAssertError(t, rec, http.StatusBadRequest, "InvalidPart")

	missingUploadID := httptest.NewRequest(http.MethodPut,
		"/"+bucket+"/"+key+"?partNumber=1", bytes.NewReader([]byte("data")))
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, missingUploadID)
	multipartLifeAssertError(t, rec, http.StatusBadRequest, "InvalidPart")

	// Both bounds are inclusive and may be uploaded in any order.
	boundsETag := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 10000, []byte("last allowed part"))
	firstETag := multipartLifeUploadPart(t, handler, bucket, key, uploadID, 1, []byte("first allowed part"))

	code, listed, body := multipartLifeListParts(t, handler, bucket, key, uploadID)
	if code != http.StatusOK {
		t.Fatalf("list parts status = %d, want 200 (body: %s)", code, body)
	}
	if len(listed.Parts) != 2 {
		t.Fatalf("list parts returned %d parts, want 2 (body: %s)", len(listed.Parts), body)
	}
	if listed.Parts[0].PartNumber != 1 || listed.Parts[0].ETag != `"`+firstETag+`"` {
		t.Fatalf("first listed part = %d/%s, want 1/%q",
			listed.Parts[0].PartNumber, listed.Parts[0].ETag, `"`+firstETag+`"`)
	}
	if listed.Parts[1].PartNumber != 10000 || listed.Parts[1].ETag != `"`+boundsETag+`"` {
		t.Fatalf("last listed part = %d/%s, want 10000/%q",
			listed.Parts[1].PartNumber, listed.Parts[1].ETag, `"`+boundsETag+`"`)
	}
}

func TestMultipartLifecycleListMultipartUploads(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	uploadID := multipartLifeInit(t, handler, bucket, "multipart/pending.bin")

	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?uploads", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ListMultipartUploads status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	var result listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal ListMultipartUploads: %v (body: %s)", err, rec.Body.String())
	}
	if result.Xmlns != "http://s3.amazonaws.com/doc/2006-03-01/" {
		t.Fatalf("xmlns = %q", result.Xmlns)
	}
	if result.Bucket != bucket {
		t.Fatalf("Bucket = %q, want %q", result.Bucket, bucket)
	}
	if result.Prefix != "" || result.Delimiter != "" || result.KeyMarker != "" || result.UploadIDMarker != "" {
		t.Fatalf("unexpected echoed filters: %+v", result)
	}
	if result.MaxUploads != 1000 {
		t.Fatalf("MaxUploads = %d, want 1000", result.MaxUploads)
	}
	if result.IsTruncated {
		t.Fatal("IsTruncated = true for a single upload")
	}
	if len(result.Uploads) != 1 {
		t.Fatalf("uploads = %d, want 1 (body: %s)", len(result.Uploads), rec.Body.String())
	}
	upload := result.Uploads[0]
	if upload.Key != "multipart/pending.bin" || upload.UploadID != uploadID {
		t.Fatalf("upload = %q/%q, want %q/%q", upload.Key, upload.UploadID, "multipart/pending.bin", uploadID)
	}
	if upload.StorageClass != "STANDARD" {
		t.Fatalf("StorageClass = %q, want STANDARD", upload.StorageClass)
	}
	if upload.Initiated == "" {
		t.Fatal("Initiated is empty")
	}
	if _, err := time.Parse(time.RFC3339, upload.Initiated); err != nil {
		t.Fatalf("Initiated = %q, want RFC3339: %v", upload.Initiated, err)
	}
}
