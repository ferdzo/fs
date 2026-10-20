package api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"fs/auth"
	"fs/models"
	"fs/service"
)

func paginationSeedObjects(t *testing.T, svc *service.ObjectService, bucket string, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, err := svc.PutObject(bucket, key, "text/plain", strings.NewReader("payload:"+key)); err != nil {
			t.Fatalf("put object %q: %v", key, err)
		}
	}
}

func paginationRequestV2(t *testing.T, handler *Handler, bucket string, params url.Values) (int, models.ListBucketResultV2, string) {
	t.Helper()

	query := url.Values{}
	for key, values := range params {
		query[key] = append([]string(nil), values...)
	}
	query.Set("list-type", "2")

	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	var result models.ListBucketResultV2
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal V2 list response: %v (body: %s)", err, rec.Body.String())
		}
	}
	return rec.Code, result, rec.Body.String()
}

func paginationRequestV1(t *testing.T, handler *Handler, bucket string, params url.Values) (int, models.ListBucketResultV1, string) {
	t.Helper()

	query := url.Values{}
	for key, values := range params {
		query[key] = append([]string(nil), values...)
	}

	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	var result models.ListBucketResultV1
	if rec.Code == http.StatusOK {
		if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal V1 list response: %v (body: %s)", err, rec.Body.String())
		}
	}
	return rec.Code, result, rec.Body.String()
}

func paginationObjectKeys(contents []models.Contents) []string {
	keys := make([]string, 0, len(contents))
	for _, item := range contents {
		keys = append(keys, item.Key)
	}
	return keys
}

func paginationCommonPrefixValues(prefixes []models.CommonPrefixes) []string {
	values := make([]string, 0, len(prefixes))
	for _, item := range prefixes {
		values = append(values, item.Prefix)
	}
	return values
}

func paginationAssertKeys(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	}
}

func paginationAssertPrefixes(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("common prefixes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("common prefixes = %v, want %v", got, want)
		}
	}
}

func paginationAssertContains(t *testing.T, body string, snippets ...string) {
	t.Helper()
	for _, snippet := range snippets {
		if !strings.Contains(body, snippet) {
			t.Fatalf("response body missing %q: %s", snippet, body)
		}
	}
}

func paginationAssertNotContains(t *testing.T, body string, snippets ...string) {
	t.Helper()
	for _, snippet := range snippets {
		if strings.Contains(body, snippet) {
			t.Fatalf("response body unexpectedly contains %q: %s", snippet, body)
		}
	}
}

func TestListObjectsV2PaginationAndExactFields(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket, "alpha", "beta", "delta", "gamma", "omega")

	code, page1, body1 := paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"2"}})
	if code != http.StatusOK {
		t.Fatalf("page 1 status = %d, want 200 (body: %s)", code, body1)
	}
	if page1.Name != bucket {
		t.Fatalf("page 1 Name = %q, want %q", page1.Name, bucket)
	}
	if page1.Prefix != "" || page1.Delimiter != "" {
		t.Fatalf("page 1 Prefix/Delimiter = %q/%q, want empty", page1.Prefix, page1.Delimiter)
	}
	if page1.MaxKeys != 2 {
		t.Fatalf("page 1 MaxKeys = %d, want 2", page1.MaxKeys)
	}
	if page1.KeyCount != 2 {
		t.Fatalf("page 1 KeyCount = %d, want 2", page1.KeyCount)
	}
	if !page1.IsTruncated {
		t.Fatal("page 1 IsTruncated = false, want true")
	}
	if page1.NextContinuationToken == "" {
		t.Fatal("page 1 NextContinuationToken is empty, want a token")
	}
	paginationAssertKeys(t, paginationObjectKeys(page1.Contents), []string{"alpha", "beta"})
	paginationAssertContains(t, body1,
		"<KeyCount>2</KeyCount>",
		"<IsTruncated>true</IsTruncated>",
		"<NextContinuationToken>",
	)
	if strings.Count(body1, "<Contents>") != 2 {
		t.Fatalf("page 1 Contents elements = %d, want 2: %s", strings.Count(body1, "<Contents>"), body1)
	}

	code, page2, body2 := paginationRequestV2(t, handler, bucket, url.Values{
		"max-keys":           {"2"},
		"continuation-token": {page1.NextContinuationToken},
	})
	if code != http.StatusOK {
		t.Fatalf("page 2 status = %d, want 200 (body: %s)", code, body2)
	}
	if page2.ContinuationToken != page1.NextContinuationToken {
		t.Fatalf("page 2 ContinuationToken = %q, want %q", page2.ContinuationToken, page1.NextContinuationToken)
	}
	if page2.KeyCount != 2 || !page2.IsTruncated {
		t.Fatalf("page 2 KeyCount/IsTruncated = %d/%v, want 2/true", page2.KeyCount, page2.IsTruncated)
	}
	if page2.NextContinuationToken == "" || page2.NextContinuationToken == page1.NextContinuationToken {
		t.Fatalf("page 2 NextContinuationToken = %q, want a fresh token", page2.NextContinuationToken)
	}
	paginationAssertKeys(t, paginationObjectKeys(page2.Contents), []string{"delta", "gamma"})

	code, page3, body3 := paginationRequestV2(t, handler, bucket, url.Values{
		"max-keys":           {"2"},
		"continuation-token": {page2.NextContinuationToken},
	})
	if code != http.StatusOK {
		t.Fatalf("page 3 status = %d, want 200 (body: %s)", code, body3)
	}
	if page3.KeyCount != 1 {
		t.Fatalf("page 3 KeyCount = %d, want 1", page3.KeyCount)
	}
	if page3.IsTruncated {
		t.Fatal("page 3 IsTruncated = true, want false")
	}
	if page3.NextContinuationToken != "" {
		t.Fatalf("page 3 NextContinuationToken = %q, want empty", page3.NextContinuationToken)
	}
	paginationAssertKeys(t, paginationObjectKeys(page3.Contents), []string{"omega"})
	paginationAssertContains(t, body3,
		"<KeyCount>1</KeyCount>",
		"<IsTruncated>false</IsTruncated>",
	)
	paginationAssertNotContains(t, body3, "<NextContinuationToken>")
}

func TestListObjectsV2PrefixDelimiterStartAfter(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket,
		"docs/a.txt",
		"docs/b.txt",
		"docs/nested/c.txt",
		"docs-extra.txt",
		"images/logo.png",
		"root.txt",
	)

	code, result, body := paginationRequestV2(t, handler, bucket, url.Values{
		"prefix":    {"docs/"},
		"delimiter": {"/"},
	})
	if code != http.StatusOK {
		t.Fatalf("delimiter list status = %d, want 200 (body: %s)", code, body)
	}
	if result.Prefix != "docs/" || result.Delimiter != "/" {
		t.Fatalf("Prefix/Delimiter = %q/%q, want %q/%q", result.Prefix, result.Delimiter, "docs/", "/")
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{"docs/a.txt", "docs/b.txt"})
	paginationAssertPrefixes(t, paginationCommonPrefixValues(result.CommonPrefixes), []string{"docs/nested/"})
	if result.KeyCount != 3 {
		t.Fatalf("KeyCount = %d, want 3", result.KeyCount)
	}
	if result.IsTruncated {
		t.Fatal("IsTruncated = true, want false")
	}
	paginationAssertContains(t, body, "<Prefix>docs/nested/</Prefix>")

	code, result, body = paginationRequestV2(t, handler, bucket, url.Values{
		"prefix":      {"docs/"},
		"delimiter":   {"/"},
		"start-after": {"docs/b.txt"},
	})
	if code != http.StatusOK {
		t.Fatalf("start-after list status = %d, want 200 (body: %s)", code, body)
	}
	if result.StartAfter != "docs/b.txt" {
		t.Fatalf("StartAfter = %q, want docs/b.txt", result.StartAfter)
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), nil)
	paginationAssertPrefixes(t, paginationCommonPrefixValues(result.CommonPrefixes), []string{"docs/nested/"})
	if result.KeyCount != 1 {
		t.Fatalf("start-after KeyCount = %d, want 1", result.KeyCount)
	}

	code, result, body = paginationRequestV2(t, handler, bucket, url.Values{"prefix": {"docs"}})
	if code != http.StatusOK {
		t.Fatalf("prefix docs status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{
		"docs-extra.txt",
		"docs/a.txt",
		"docs/b.txt",
		"docs/nested/c.txt",
	})

	code, result, body = paginationRequestV2(t, handler, bucket, url.Values{"prefix": {"docs/"}})
	if code != http.StatusOK {
		t.Fatalf("prefix docs/ status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{
		"docs/a.txt",
		"docs/b.txt",
		"docs/nested/c.txt",
	})
	if result.KeyCount != 3 {
		t.Fatalf("prefix docs/ KeyCount = %d, want 3", result.KeyCount)
	}

	// Prefix matching is a prefix comparison, not a substring search.
	code, result, body = paginationRequestV2(t, handler, bucket, url.Values{"prefix": {"ocs"}})
	if code != http.StatusOK {
		t.Fatalf("substring prefix status = %d, want 200 (body: %s)", code, body)
	}
	if len(result.Contents) != 0 || result.KeyCount != 0 {
		t.Fatalf("substring prefix returned %d keys (%v), want none", result.KeyCount, paginationObjectKeys(result.Contents))
	}
}

func TestListObjectsV2ContinuationWithDelimiter(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket,
		"dir1/a", "dir1/b",
		"dir2/a", "dir2/b",
		"dir3/a", "dir3/b",
		"dir4/a",
		"file.txt",
	)

	code, page1, body1 := paginationRequestV2(t, handler, bucket, url.Values{
		"delimiter": {"/"},
		"max-keys":  {"2"},
	})
	if code != http.StatusOK {
		t.Fatalf("delimiter page 1 status = %d, want 200 (body: %s)", code, body1)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(page1.CommonPrefixes), []string{"dir1/", "dir2/"})
	paginationAssertKeys(t, paginationObjectKeys(page1.Contents), nil)
	if page1.KeyCount != 2 || !page1.IsTruncated || page1.NextContinuationToken == "" {
		t.Fatalf("delimiter page 1 KeyCount/IsTruncated/NextToken = %d/%v/%q, want 2/true/non-empty",
			page1.KeyCount, page1.IsTruncated, page1.NextContinuationToken)
	}

	code, page2, body2 := paginationRequestV2(t, handler, bucket, url.Values{
		"delimiter":          {"/"},
		"max-keys":           {"2"},
		"continuation-token": {page1.NextContinuationToken},
	})
	if code != http.StatusOK {
		t.Fatalf("delimiter page 2 status = %d, want 200 (body: %s)", code, body2)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(page2.CommonPrefixes), []string{"dir3/", "dir4/"})
	paginationAssertKeys(t, paginationObjectKeys(page2.Contents), nil)
	if page2.KeyCount != 2 || !page2.IsTruncated || page2.NextContinuationToken == "" {
		t.Fatalf("delimiter page 2 KeyCount/IsTruncated/NextToken = %d/%v/%q, want 2/true/non-empty",
			page2.KeyCount, page2.IsTruncated, page2.NextContinuationToken)
	}
	if page2.NextContinuationToken == page1.NextContinuationToken {
		t.Fatalf("delimiter page 2 repeated page 1 token %q", page2.NextContinuationToken)
	}

	code, page3, body3 := paginationRequestV2(t, handler, bucket, url.Values{
		"delimiter":          {"/"},
		"max-keys":           {"2"},
		"continuation-token": {page2.NextContinuationToken},
	})
	if code != http.StatusOK {
		t.Fatalf("delimiter page 3 status = %d, want 200 (body: %s)", code, body3)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(page3.CommonPrefixes), nil)
	paginationAssertKeys(t, paginationObjectKeys(page3.Contents), []string{"file.txt"})
	if page3.KeyCount != 1 || page3.IsTruncated || page3.NextContinuationToken != "" {
		t.Fatalf("delimiter page 3 KeyCount/IsTruncated/NextToken = %d/%v/%q, want 1/false/empty",
			page3.KeyCount, page3.IsTruncated, page3.NextContinuationToken)
	}
}

func TestListObjectsV2EncodingTypeURL(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket, "a b/c+d&e.txt", "a b/plain.txt")

	code, result, body := paginationRequestV2(t, handler, bucket, url.Values{
		"prefix":        {"a b/"},
		"encoding-type": {"url"},
	})
	if code != http.StatusOK {
		t.Fatalf("encoded list status = %d, want 200 (body: %s)", code, body)
	}
	if result.EncodingType != "url" {
		t.Fatalf("EncodingType = %q, want url", result.EncodingType)
	}
	if result.Prefix != "a%20b%2F" {
		t.Fatalf("encoded Prefix = %q, want a%%20b%%2F", result.Prefix)
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{
		"a%20b%2Fc%2Bd%26e.txt",
		"a%20b%2Fplain.txt",
	})
	paginationAssertContains(t, body,
		"<EncodingType>url</EncodingType>",
		"<Prefix>a%20b%2F</Prefix>",
		"<Key>a%20b%2Fc%2Bd%26e.txt</Key>",
	)
	paginationAssertNotContains(t, body, "<Key>a b/c+d&e.txt</Key>")
}

func TestListObjectsV1MarkerPagination(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket, "a", "b", "c", "d", "e")

	code, page1, body1 := paginationRequestV1(t, handler, bucket, url.Values{"max-keys": {"2"}})
	if code != http.StatusOK {
		t.Fatalf("page 1 status = %d, want 200 (body: %s)", code, body1)
	}
	if page1.Marker != "" {
		t.Fatalf("page 1 Marker = %q, want empty", page1.Marker)
	}
	if page1.MaxKeys != 2 || page1.IsTruncated != true {
		t.Fatalf("page 1 MaxKeys/IsTruncated = %d/%v, want 2/true", page1.MaxKeys, page1.IsTruncated)
	}
	paginationAssertKeys(t, paginationObjectKeys(page1.Contents), []string{"a", "b"})
	if page1.NextMarker != "b" {
		t.Fatalf("page 1 NextMarker = %q, want b", page1.NextMarker)
	}
	paginationAssertContains(t, body1,
		"<MaxKeys>2</MaxKeys>",
		"<IsTruncated>true</IsTruncated>",
		"<NextMarker>b</NextMarker>",
	)

	code, page2, body2 := paginationRequestV1(t, handler, bucket, url.Values{
		"max-keys": {"2"},
		"marker":   {"b"},
	})
	if code != http.StatusOK {
		t.Fatalf("page 2 status = %d, want 200 (body: %s)", code, body2)
	}
	if page2.Marker != "b" {
		t.Fatalf("page 2 Marker = %q, want b", page2.Marker)
	}
	paginationAssertKeys(t, paginationObjectKeys(page2.Contents), []string{"c", "d"})
	if page2.NextMarker != "d" || !page2.IsTruncated {
		t.Fatalf("page 2 NextMarker/IsTruncated = %q/%v, want d/true", page2.NextMarker, page2.IsTruncated)
	}

	code, page3, body3 := paginationRequestV1(t, handler, bucket, url.Values{
		"max-keys": {"2"},
		"marker":   {"d"},
	})
	if code != http.StatusOK {
		t.Fatalf("page 3 status = %d, want 200 (body: %s)", code, body3)
	}
	paginationAssertKeys(t, paginationObjectKeys(page3.Contents), []string{"e"})
	if page3.IsTruncated || page3.NextMarker != "" {
		t.Fatalf("page 3 IsTruncated/NextMarker = %v/%q, want false/empty", page3.IsTruncated, page3.NextMarker)
	}
	paginationAssertContains(t, body3,
		"<Marker>d</Marker>",
		"<IsTruncated>false</IsTruncated>",
	)
	paginationAssertNotContains(t, body3, "<NextMarker>")
}

func TestListObjectsV1DelimiterCommonPrefixes(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket, "a/1", "a/2", "b/1", "root.txt")

	code, result, body := paginationRequestV1(t, handler, bucket, url.Values{"delimiter": {"/"}})
	if code != http.StatusOK {
		t.Fatalf("delimiter list status = %d, want 200 (body: %s)", code, body)
	}
	if result.Delimiter != "/" {
		t.Fatalf("Delimiter = %q, want /", result.Delimiter)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(result.CommonPrefixes), []string{"a/", "b/"})
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{"root.txt"})
	if result.IsTruncated {
		t.Fatal("IsTruncated = true, want false")
	}
	paginationAssertContains(t, body, "<Prefix>a/</Prefix>", "<Prefix>b/</Prefix>", "<Key>root.txt</Key>")

	code, result, body = paginationRequestV1(t, handler, bucket, url.Values{
		"delimiter": {"/"},
		"max-keys":  {"2"},
	})
	if code != http.StatusOK {
		t.Fatalf("truncated delimiter status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(result.CommonPrefixes), []string{"a/", "b/"})
	if len(result.Contents) != 0 {
		t.Fatalf("truncated delimiter page contents = %v, want none", paginationObjectKeys(result.Contents))
	}
	if !result.IsTruncated || result.NextMarker != "b/" {
		t.Fatalf("truncated delimiter IsTruncated/NextMarker = %v/%q, want true/b/", result.IsTruncated, result.NextMarker)
	}

	code, result, body = paginationRequestV1(t, handler, bucket, url.Values{
		"delimiter": {"/"},
		"max-keys":  {"2"},
		"marker":    {"b/"},
	})
	if code != http.StatusOK {
		t.Fatalf("delimiter page 2 status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertPrefixes(t, paginationCommonPrefixValues(result.CommonPrefixes), nil)
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{"root.txt"})
	if result.IsTruncated || result.NextMarker != "" {
		t.Fatalf("delimiter page 2 IsTruncated/NextMarker = %v/%q, want false/empty", result.IsTruncated, result.NextMarker)
	}
}

func TestListObjectsEdgeCases(t *testing.T) {
	handler, svc := newTestObjectHandler(t)
	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	const emptyBucket = "empty-bucket"
	if err := svc.CreateBucket(emptyBucket); err != nil {
		t.Fatalf("create empty bucket: %v", err)
	}

	code, v2Empty, body := paginationRequestV2(t, handler, emptyBucket, nil)
	if code != http.StatusOK {
		t.Fatalf("empty bucket V2 status = %d, want 200 (body: %s)", code, body)
	}
	if v2Empty.KeyCount != 0 || len(v2Empty.Contents) != 0 || len(v2Empty.CommonPrefixes) != 0 {
		t.Fatalf("empty bucket V2 KeyCount/Contents/Prefixes = %d/%d/%d, want 0/0/0",
			v2Empty.KeyCount, len(v2Empty.Contents), len(v2Empty.CommonPrefixes))
	}
	if v2Empty.IsTruncated || v2Empty.NextContinuationToken != "" {
		t.Fatalf("empty bucket V2 IsTruncated/NextToken = %v/%q, want false/empty",
			v2Empty.IsTruncated, v2Empty.NextContinuationToken)
	}

	code, v1Empty, body := paginationRequestV1(t, handler, emptyBucket, nil)
	if code != http.StatusOK {
		t.Fatalf("empty bucket V1 status = %d, want 200 (body: %s)", code, body)
	}
	if len(v1Empty.Contents) != 0 || len(v1Empty.CommonPrefixes) != 0 {
		t.Fatalf("empty bucket V1 Contents/Prefixes = %d/%d, want 0/0",
			len(v1Empty.Contents), len(v1Empty.CommonPrefixes))
	}
	if v1Empty.IsTruncated || v1Empty.NextMarker != "" {
		t.Fatalf("empty bucket V1 IsTruncated/NextMarker = %v/%q, want false/empty",
			v1Empty.IsTruncated, v1Empty.NextMarker)
	}

	paginationSeedObjects(t, svc, bucket, "b", "a", "d", "c", "e")

	code, v2Zero, body := paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"0"}})
	if code != http.StatusOK {
		t.Fatalf("V2 max-keys=0 status = %d, want 200 (body: %s)", code, body)
	}
	if v2Zero.KeyCount != 0 || len(v2Zero.Contents) != 0 || v2Zero.IsTruncated {
		t.Fatalf("V2 max-keys=0 KeyCount/Contents/IsTruncated = %d/%d/%v, want 0/0/false",
			v2Zero.KeyCount, len(v2Zero.Contents), v2Zero.IsTruncated)
	}
	if v2Zero.NextContinuationToken != "" {
		t.Fatalf("V2 max-keys=0 NextToken = %q, want empty", v2Zero.NextContinuationToken)
	}

	code, v1Zero, body := paginationRequestV1(t, handler, bucket, url.Values{"max-keys": {"0"}})
	if code != http.StatusOK {
		t.Fatalf("V1 max-keys=0 status = %d, want 200 (body: %s)", code, body)
	}
	if len(v1Zero.Contents) != 0 || v1Zero.IsTruncated || v1Zero.NextMarker != "" {
		t.Fatalf("V1 max-keys=0 Contents/IsTruncated/NextMarker = %d/%v/%q, want 0/false/empty",
			len(v1Zero.Contents), v1Zero.IsTruncated, v1Zero.NextMarker)
	}

	code, v2One, body := paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"1"}})
	if code != http.StatusOK {
		t.Fatalf("V2 max-keys=1 status = %d, want 200 (body: %s)", code, body)
	}
	if v2One.KeyCount != 1 || !v2One.IsTruncated || v2One.NextContinuationToken == "" {
		t.Fatalf("V2 max-keys=1 KeyCount/IsTruncated/NextToken = %d/%v/%q, want 1/true/non-empty",
			v2One.KeyCount, v2One.IsTruncated, v2One.NextContinuationToken)
	}
	paginationAssertKeys(t, paginationObjectKeys(v2One.Contents), []string{"a"})

	code, v2Next, body := paginationRequestV2(t, handler, bucket, url.Values{
		"max-keys":           {"1"},
		"continuation-token": {v2One.NextContinuationToken},
	})
	if code != http.StatusOK {
		t.Fatalf("V2 max-keys=1 page 2 status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertKeys(t, paginationObjectKeys(v2Next.Contents), []string{"b"})

	code, v1One, body := paginationRequestV1(t, handler, bucket, url.Values{"max-keys": {"1"}})
	if code != http.StatusOK {
		t.Fatalf("V1 max-keys=1 status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertKeys(t, paginationObjectKeys(v1One.Contents), []string{"a"})
	if !v1One.IsTruncated || v1One.NextMarker != "a" {
		t.Fatalf("V1 max-keys=1 IsTruncated/NextMarker = %v/%q, want true/a", v1One.IsTruncated, v1One.NextMarker)
	}

	code, ordered, body := paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"10"}})
	if code != http.StatusOK {
		t.Fatalf("ordered list status = %d, want 200 (body: %s)", code, body)
	}
	paginationAssertKeys(t, paginationObjectKeys(ordered.Contents), []string{"a", "b", "c", "d", "e"})

	code, clamped, body := paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"5000"}})
	if code != http.StatusOK {
		t.Fatalf("clamped list status = %d, want 200 (body: %s)", code, body)
	}
	if clamped.MaxKeys != 1000 {
		t.Fatalf("MaxKeys = %d, want clamped 1000", clamped.MaxKeys)
	}

	code, _, body = paginationRequestV2(t, handler, bucket, url.Values{"max-keys": {"-1"}})
	if code != http.StatusBadRequest || !strings.Contains(body, "InvalidArgument") {
		t.Fatalf("V2 max-keys=-1 status/body = %d/%s, want 400 InvalidArgument", code, body)
	}
	code, _, body = paginationRequestV1(t, handler, bucket, url.Values{"max-keys": {"not-a-number"}})
	if code != http.StatusBadRequest || !strings.Contains(body, "InvalidArgument") {
		t.Fatalf("V1 invalid max-keys status/body = %d/%s, want 400 InvalidArgument", code, body)
	}
	code, _, body = paginationRequestV2(t, handler, bucket, url.Values{"encoding-type": {"base64"}})
	if code != http.StatusBadRequest || !strings.Contains(body, "InvalidArgument") {
		t.Fatalf("V2 invalid encoding-type status/body = %d/%s, want 400 InvalidArgument", code, body)
	}
}

func TestListObjectsRequiresCredentials(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()

	const bucket = "test-bucket"
	if err := svc.CreateBucket(bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	paginationSeedObjects(t, svc, bucket, "allowed/a.txt", "private/b.txt")

	if _, err := authSvc.CreateUser(auth.CreateUserInput{
		AccessKeyID: "list-user",
		SecretKey:   "list-secret-1",
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

	unsigned := httptest.NewRequest(http.MethodGet, "/"+bucket+"?list-type=2", nil)
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, unsigned)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("unsigned list status = %d, want 403 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("unsigned list body = %s, want AccessDenied", rec.Body.String())
	}

	noPrefix := httptest.NewRequest(http.MethodGet, "/"+bucket+"?list-type=2", nil)
	signTestSigV4Request(t, noPrefix, "list-user", "list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, noPrefix)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("signed no-prefix list status = %d, want 403 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("signed no-prefix list body = %s, want AccessDenied", rec.Body.String())
	}

	allowed := httptest.NewRequest(http.MethodGet, "/"+bucket+"?list-type=2&prefix=allowed%2F", nil)
	signTestSigV4Request(t, allowed, "list-user", "list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, allowed)
	if rec.Code != http.StatusOK {
		t.Fatalf("signed allowed-prefix list status = %d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var result models.ListBucketResultV2
	if err := xml.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal authorized list response: %v (body: %s)", err, rec.Body.String())
	}
	paginationAssertKeys(t, paginationObjectKeys(result.Contents), []string{"allowed/a.txt"})

	denied := httptest.NewRequest(http.MethodGet, "/"+bucket+"?list-type=2&prefix=private%2F", nil)
	signTestSigV4Request(t, denied, "list-user", "list-secret-1")
	rec = httptest.NewRecorder()
	handler.router.ServeHTTP(rec, denied)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("signed private-prefix list status = %d, want 403 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "AccessDenied") {
		t.Fatalf("signed private-prefix list body = %s, want AccessDenied", rec.Body.String())
	}
}
