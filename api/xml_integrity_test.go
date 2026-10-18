package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"fs/auth"
	"fs/metadata"
	"fs/models"
	"fs/service"
)

// integritySignRequest signs req with SigV4 using payloadHash as the value of
// x-amz-content-sha256. It lets a test declare a hash for one body while
// sending another, which is exactly the tampering case the payload verifier
// must catch before any handler mutation.
func integritySignRequest(t *testing.T, req *http.Request, accessKeyID, secretKey, payloadHash string) {
	t.Helper()

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	scope := strings.Join([]string{date, "us-east-1", "s3", "aws4_request"}, "/")
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	signedHeadersRaw := strings.Join(signedHeaders, ";")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		integrityCanonicalQuery(req.URL.RawQuery),
		"host:" + strings.TrimSpace(req.Host) + "\n" +
			"x-amz-content-sha256:" + payloadHash + "\n" +
			"x-amz-date:" + amzDate + "\n",
		signedHeadersRaw,
		payloadHash,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signature := hex.EncodeToString(integrityHMAC(integritySigningKey(secretKey, date), stringToSign))

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
		"Credential="+accessKeyID+"/"+scope+", "+
		"SignedHeaders="+signedHeadersRaw+", "+
		"Signature="+signature)
}

func integrityHMAC(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func integritySigningKey(secret, date string) []byte {
	k := integrityHMAC([]byte("AWS4"+secret), date)
	k = integrityHMAC(k, "us-east-1")
	k = integrityHMAC(k, "s3")
	return integrityHMAC(k, "aws4_request")
}

func integrityCanonicalQuery(rawQuery string) string {
	values, _ := url.ParseQuery(rawQuery)
	pairs := make([]string, 0)
	for key, valueList := range values {
		if len(valueList) == 0 {
			pairs = append(pairs, integrityQueryEscape(key)+"=")
			continue
		}
		for _, value := range valueList {
			pairs = append(pairs, integrityQueryEscape(key)+"="+integrityQueryEscape(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func integrityQueryEscape(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

func sha256Hex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func alwaysAllowDeleteUser(t *testing.T, authSvc *auth.Service) {
	t.Helper()
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:DeleteObject"}, Bucket: "test-bucket"},
	})
}

func TestMultiDeleteRejectsTamperedBodyBeforeMutation(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	alwaysAllowDeleteUser(t, authSvc)
	putTestObject(t, svc, "victim.txt")

	// The signed hash covers a harmless, empty delete request. The body that
	// actually arrives deletes victim.txt and is padded past the XML decoder's
	// 4 KiB buffer so the decoder cannot accidentally consume it to EOF while
	// decoding the root element.
	signedBody := `<Delete></Delete>`
	actualBody := `<Delete><Object><Key>victim.txt</Key></Object></Delete>` + strings.Repeat(" ", 16<<10)

	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(signedBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	body := decodeIntegrityResponse(t, rec, http.StatusForbidden)
	if !strings.Contains(body, "SignatureDoesNotMatch") {
		t.Fatalf("expected SignatureDoesNotMatch, body=%s", body)
	}
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); err != nil {
		t.Fatalf("tampered request must not delete victim.txt: %v", err)
	}
}

func TestMultiDeleteRejectsMalformedTrailingContent(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	alwaysAllowDeleteUser(t, authSvc)
	putTestObject(t, svc, "victim.txt")

	// Integrity is valid for the entire body; only the trailing element is
	// outside the document, so the request must be rejected without deleting.
	actualBody := `<Delete><Object><Key>victim.txt</Key></Object></Delete><Extra/>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	body := decodeIntegrityResponse(t, rec, http.StatusBadRequest)
	if !strings.Contains(body, "MalformedXML") {
		t.Fatalf("expected MalformedXML, body=%s", body)
	}
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); err != nil {
		t.Fatalf("malformed trailing content must not delete victim.txt: %v", err)
	}
}

func TestMultiDeleteAllowsTrailingWhitespace(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	alwaysAllowDeleteUser(t, authSvc)
	putTestObject(t, svc, "victim.txt")

	actualBody := "<Delete><Object><Key>victim.txt</Key></Object></Delete>\r\n\t  "
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	decodeIntegrityResponse(t, rec, http.StatusOK)
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("valid trailing whitespace must still delete victim.txt, got err=%v", err)
	}
}

func TestMultiDeleteAllowsSignedBodyWithoutTrailingBytes(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	alwaysAllowDeleteUser(t, authSvc)
	putTestObject(t, svc, "victim.txt")

	// Exact body with no trailing bytes: the drain must observe EOF, verify the
	// signed hash, and still perform the deletion.
	actualBody := `<Delete><Object><Key>victim.txt</Key></Object></Delete>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	decodeIntegrityResponse(t, rec, http.StatusOK)
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("valid signed body must delete victim.txt, got err=%v", err)
	}
}

func TestMultiDeleteOversizedBodyStillEntityTooLarge(t *testing.T) {
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	alwaysAllowDeleteUser(t, authSvc)
	putTestObject(t, svc, "victim.txt")

	// Correctly signed, but larger than maxXMLBodyBytes: mapping must stay
	// EntityTooLarge and the bounded drain must not run past the cap.
	actualBody := `<Delete><Object><Key>victim.txt</Key></Object></Delete>` + strings.Repeat(" ", (1<<20)+1024)
	req := httptest.NewRequest(http.MethodPost, "/test-bucket?delete", strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	body := decodeIntegrityResponse(t, rec, http.StatusRequestEntityTooLarge)
	if !strings.Contains(body, "EntityTooLarge") {
		t.Fatalf("expected EntityTooLarge, body=%s", body)
	}
	if _, err := svc.HeadObject("test-bucket", "victim.txt"); err != nil {
		t.Fatalf("oversized request must not delete victim.txt: %v", err)
	}
}

func newIntegrityCompleteHandler(t *testing.T) (*Handler, *service.ObjectService) {
	t.Helper()
	handler, svc, authSvc := newAuthorizedDeleteHandler(t)
	handler.setupRoutes()
	if err := svc.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	createDeleteUserWithStatements(t, authSvc, []models.AuthPolicyStatement{
		{Effect: "allow", Actions: []string{"s3:CompleteMultipartUpload"}, Bucket: "test-bucket"},
	})
	return handler, svc
}

func beginIntegrityUpload(t *testing.T, svc *service.ObjectService) (string, string) {
	t.Helper()
	upload, err := svc.CreateMultipartUpload("test-bucket", "big.bin")
	if err != nil {
		t.Fatalf("create multipart upload: %v", err)
	}
	partETag, err := svc.UploadPart("test-bucket", "big.bin", upload.UploadID, 1, bytes.NewReader([]byte("hello")))
	if err != nil {
		t.Fatalf("upload part: %v", err)
	}
	return upload.UploadID, partETag
}

func completeBodyWithPart(partETag string) string {
	return `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;` +
		partETag + `&quot;</ETag></Part></CompleteMultipartUpload>`
}

func TestCompleteMultipartRejectsTamperedBodyBeforeMutation(t *testing.T) {
	handler, svc := newIntegrityCompleteHandler(t)
	uploadID, partETag := beginIntegrityUpload(t, svc)

	signedBody := `<CompleteMultipartUpload></CompleteMultipartUpload>`
	actualBody := completeBodyWithPart(partETag) + strings.Repeat(" ", 16<<10)

	req := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploadId="+uploadID, strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(signedBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	body := decodeIntegrityResponse(t, rec, http.StatusForbidden)
	if !strings.Contains(body, "SignatureDoesNotMatch") {
		t.Fatalf("expected SignatureDoesNotMatch, body=%s", body)
	}
	if _, err := svc.HeadObject("test-bucket", "big.bin"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("tampered completion must not create big.bin, got err=%v", err)
	}
}

func TestCompleteMultipartRejectsMalformedTrailingContent(t *testing.T) {
	handler, svc := newIntegrityCompleteHandler(t)
	uploadID, partETag := beginIntegrityUpload(t, svc)

	actualBody := completeBodyWithPart(partETag) + `<Extra/>`
	req := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploadId="+uploadID, strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	body := decodeIntegrityResponse(t, rec, http.StatusBadRequest)
	if !strings.Contains(body, "MalformedXML") {
		t.Fatalf("expected MalformedXML, body=%s", body)
	}
	if _, err := svc.HeadObject("test-bucket", "big.bin"); !errors.Is(err, metadata.ErrObjectNotFound) {
		t.Fatalf("malformed completion must not create big.bin, got err=%v", err)
	}
}

func TestCompleteMultipartAllowsTrailingWhitespace(t *testing.T) {
	handler, svc := newIntegrityCompleteHandler(t)
	uploadID, partETag := beginIntegrityUpload(t, svc)

	actualBody := completeBodyWithPart(partETag) + "\r\n\t "
	req := httptest.NewRequest(http.MethodPost, "/test-bucket/big.bin?uploadId="+uploadID, strings.NewReader(actualBody))
	integritySignRequest(t, req, "delete-user", "delete-secret-1", sha256Hex(actualBody))
	rec := httptest.NewRecorder()
	handler.router.ServeHTTP(rec, req)

	decodeIntegrityResponse(t, rec, http.StatusOK)
	if _, err := svc.HeadObject("test-bucket", "big.bin"); err != nil {
		t.Fatalf("valid trailing whitespace must complete big.bin: %v", err)
	}
}

func decodeIntegrityResponse(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) string {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Body.String()
}
