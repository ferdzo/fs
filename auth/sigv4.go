package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	sigV4Algorithm = "AWS4-HMAC-SHA256"
)

type sigV4Input struct {
	AccessKeyID      string
	Date             string
	Region           string
	Service          string
	Scope            string
	SignedHeaders    []string
	SignedHeadersRaw string
	SignatureHex     string
	AmzDate          string
	ExpiresSeconds   int
	Presigned        bool
}

func parseSigV4(r *http.Request) (*sigV4Input, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: nil request", ErrAuthorizationHeaderMalformed)
	}
	if strings.EqualFold(r.URL.Query().Get("X-Amz-Algorithm"), sigV4Algorithm) {
		return parsePresignedSigV4(r)
	}
	return parseHeaderSigV4(r)
}

func parseHeaderSigV4(r *http.Request) (*sigV4Input, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return nil, ErrNoAuthCredentials
	}
	if !strings.HasPrefix(header, sigV4Algorithm+" ") {
		return nil, fmt.Errorf("%w: unsupported authorization algorithm", ErrUnsupportedAuthScheme)
	}

	params := parseAuthorizationParams(strings.TrimSpace(strings.TrimPrefix(header, sigV4Algorithm)))
	credentialRaw := params["Credential"]
	signedHeadersRaw := params["SignedHeaders"]
	signatureHex := params["Signature"]
	if credentialRaw == "" || signedHeadersRaw == "" || signatureHex == "" {
		return nil, fmt.Errorf("%w: missing required authorization fields", ErrAuthorizationHeaderMalformed)
	}

	accessKeyID, date, region, service, scope, err := parseCredential(credentialRaw)
	if err != nil {
		return nil, err
	}

	amzDate := strings.TrimSpace(r.Header.Get("x-amz-date"))
	if amzDate == "" {
		return nil, fmt.Errorf("%w: x-amz-date is required", ErrAuthorizationHeaderMalformed)
	}
	signedHeaders := splitSignedHeaders(signedHeadersRaw)
	if len(signedHeaders) == 0 {
		return nil, fmt.Errorf("%w: signed headers are required", ErrAuthorizationHeaderMalformed)
	}

	return &sigV4Input{
		AccessKeyID:      accessKeyID,
		Date:             date,
		Region:           region,
		Service:          service,
		Scope:            scope,
		SignedHeaders:    signedHeaders,
		SignedHeadersRaw: strings.ToLower(strings.TrimSpace(signedHeadersRaw)),
		SignatureHex:     strings.ToLower(strings.TrimSpace(signatureHex)),
		AmzDate:          amzDate,
		Presigned:        false,
	}, nil
}

func parsePresignedSigV4(r *http.Request) (*sigV4Input, error) {
	query := r.URL.Query()
	if !strings.EqualFold(query.Get("X-Amz-Algorithm"), sigV4Algorithm) {
		return nil, fmt.Errorf("%w: invalid X-Amz-Algorithm", ErrInvalidPresign)
	}

	credentialRaw := strings.TrimSpace(query.Get("X-Amz-Credential"))
	signedHeadersRaw := strings.TrimSpace(query.Get("X-Amz-SignedHeaders"))
	signatureHex := strings.TrimSpace(query.Get("X-Amz-Signature"))
	amzDate := strings.TrimSpace(query.Get("X-Amz-Date"))
	expiresRaw := strings.TrimSpace(query.Get("X-Amz-Expires"))
	if credentialRaw == "" || signedHeadersRaw == "" || signatureHex == "" || amzDate == "" || expiresRaw == "" {
		return nil, fmt.Errorf("%w: missing presigned query fields", ErrInvalidPresign)
	}
	expires, err := strconv.Atoi(expiresRaw)
	if err != nil || expires < 0 {
		return nil, fmt.Errorf("%w: invalid X-Amz-Expires", ErrInvalidPresign)
	}

	accessKeyID, date, region, service, scope, err := parseCredential(credentialRaw)
	if err != nil {
		return nil, err
	}
	signedHeaders := splitSignedHeaders(signedHeadersRaw)
	if len(signedHeaders) == 0 {
		return nil, fmt.Errorf("%w: signed headers are required", ErrInvalidPresign)
	}

	return &sigV4Input{
		AccessKeyID:      accessKeyID,
		Date:             date,
		Region:           region,
		Service:          service,
		Scope:            scope,
		SignedHeaders:    signedHeaders,
		SignedHeadersRaw: strings.ToLower(strings.TrimSpace(signedHeadersRaw)),
		SignatureHex:     strings.ToLower(signatureHex),
		AmzDate:          amzDate,
		ExpiresSeconds:   expires,
		Presigned:        true,
	}, nil
}

func parseCredential(raw string) (accessKeyID string, date string, region string, service string, scope string, err error) {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	if len(parts) != 5 {
		return "", "", "", "", "", fmt.Errorf("%w: invalid credential scope", ErrAuthorizationHeaderMalformed)
	}
	accessKeyID = strings.TrimSpace(parts[0])
	date = strings.TrimSpace(parts[1])
	region = strings.TrimSpace(parts[2])
	service = strings.TrimSpace(parts[3])
	terminal := strings.TrimSpace(parts[4])
	if accessKeyID == "" || date == "" || region == "" || service == "" || terminal != "aws4_request" {
		return "", "", "", "", "", fmt.Errorf("%w: invalid credential scope", ErrAuthorizationHeaderMalformed)
	}
	scope = strings.Join(parts[1:], "/")
	return accessKeyID, date, region, service, scope, nil
}

func splitSignedHeaders(raw string) []string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ";")
	headers := make([]string, 0, len(parts))
	for _, current := range parts {
		current = strings.TrimSpace(current)
		if current == "" {
			continue
		}
		headers = append(headers, current)
	}
	return headers
}

func parseAuthorizationParams(raw string) map[string]string {
	params := make(map[string]string)
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, " ")
	for _, token := range strings.Split(raw, ",") {
		token = strings.TrimSpace(token)
		key, value, found := strings.Cut(token, "=")
		if !found {
			continue
		}
		params[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return params
}

func validateSigV4Input(now time.Time, cfg Config, input *sigV4Input) error {
	if input == nil {
		return fmt.Errorf("%w: empty signature input", ErrAuthorizationHeaderMalformed)
	}
	if !strings.EqualFold(input.Service, "s3") {
		return fmt.Errorf("%w: unsupported service", ErrAuthorizationHeaderMalformed)
	}
	if !strings.EqualFold(input.Region, cfg.Region) {
		return fmt.Errorf("%w: region mismatch", ErrAuthorizationHeaderMalformed)
	}

	requestTime, err := time.Parse("20060102T150405Z", input.AmzDate)
	if err != nil {
		return fmt.Errorf("%w: invalid x-amz-date", ErrAuthorizationHeaderMalformed)
	}
	delta := now.Sub(requestTime)
	if delta > cfg.ClockSkew || delta < -cfg.ClockSkew {
		return ErrRequestTimeTooSkewed
	}

	if input.Presigned {
		if input.ExpiresSeconds > int(cfg.MaxPresignDuration.Seconds()) {
			return fmt.Errorf("%w: presign expires too large", ErrInvalidPresign)
		}
		expiresAt := requestTime.Add(time.Duration(input.ExpiresSeconds) * time.Second)
		if now.After(expiresAt) {
			return ErrExpiredToken
		}
	}
	return nil
}

func validatePayloadSigningMode(r *http.Request, input *sigV4Input) error {
	payloadHash := resolvePayloadHash(r, input.Presigned)
	if isSignedStreamingPayloadHash(payloadHash) {
		return fmt.Errorf("%w: signed streaming payload verification is not supported", ErrAuthorizationHeaderMalformed)
	}
	if payloadHashRequiresVerification(payloadHash) && !isHexSHA256(payloadHash) {
		return fmt.Errorf("%w: invalid x-amz-content-sha256", ErrAuthorizationHeaderMalformed)
	}
	return nil
}

func signatureMatches(secret string, r *http.Request, input *sigV4Input) (bool, error) {
	payloadHash := resolvePayloadHash(r, input.Presigned)
	canonicalRequest, err := buildCanonicalRequest(r, input.SignedHeaders, payloadHash, input.Presigned)
	if err != nil {
		return false, err
	}
	stringToSign := buildStringToSign(input.AmzDate, input.Scope, canonicalRequest)
	signingKey := deriveSigningKey(secret, input.Date, input.Region, input.Service)
	expectedSig := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	return hmac.Equal([]byte(expectedSig), []byte(input.SignatureHex)), nil
}

func resolvePayloadHash(r *http.Request, presigned bool) string {
	if presigned {
		return "UNSIGNED-PAYLOAD"
	}
	hash := strings.TrimSpace(r.Header.Get("x-amz-content-sha256"))
	if hash == "" {
		return "UNSIGNED-PAYLOAD"
	}
	return hash
}

func isSignedStreamingPayloadHash(payloadHash string) bool {
	payloadHash = strings.ToUpper(strings.TrimSpace(payloadHash))
	return strings.HasPrefix(payloadHash, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
}

func payloadHashRequiresVerification(payloadHash string) bool {
	payloadHash = strings.ToUpper(strings.TrimSpace(payloadHash))
	if payloadHash == "" || payloadHash == "UNSIGNED-PAYLOAD" {
		return false
	}
	if strings.HasPrefix(payloadHash, "STREAMING-UNSIGNED-PAYLOAD") {
		return false
	}
	return true
}

func isHexSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') && (ch < 'A' || ch > 'F') {
			return false
		}
	}
	return true
}

func buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string, presigned bool) (string, error) {
	canonicalURI := canonicalPath(r.URL)
	canonicalQuery := canonicalQueryString(r.URL.RawQuery, presigned)
	canonicalHeaders, signedHeadersRaw, err := canonicalHeadersForRequest(r, signedHeaders)
	if err != nil {
		return "", err
	}

	return strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeadersRaw,
		payloadHash,
	}, "\n"), nil
}

func canonicalPath(u *url.URL) string {
	if u == nil {
		return "/"
	}
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	return awsEncodePath(path)
}

func awsEncodePath(path string) string {
	var b strings.Builder
	b.Grow(len(path))
	for i := 0; i < len(path); i++ {
		ch := path[i]
		if ch == '/' || isUnreserved(ch) {
			b.WriteByte(ch)
			continue
		}
		if ch == '%' && i+2 < len(path) && isHex(path[i+1]) && isHex(path[i+2]) {
			b.WriteByte('%')
			b.WriteByte(toUpperHex(path[i+1]))
			b.WriteByte(toUpperHex(path[i+2]))
			i += 2
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexUpper(ch >> 4))
		b.WriteByte(hexUpper(ch & 0x0F))
	}
	return b.String()
}

func isUnreserved(ch byte) bool {
	return (ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '-' || ch == '_' || ch == '.' || ch == '~'
}

func isHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'a' && ch <= 'f') ||
		(ch >= 'A' && ch <= 'F')
}

func toUpperHex(ch byte) byte {
	if ch >= 'a' && ch <= 'f' {
		return ch - ('a' - 'A')
	}
	return ch
}

func hexUpper(nibble byte) byte {
	if nibble < 10 {
		return '0' + nibble
	}
	return 'A' + (nibble - 10)
}

type queryPair struct {
	Key   string
	Value string
}

func canonicalQueryString(rawQuery string, presigned bool) string {
	if rawQuery == "" {
		return ""
	}
	values, _ := url.ParseQuery(rawQuery)
	pairs := make([]queryPair, 0)
	for key, valueList := range values {
		if presigned && strings.EqualFold(key, "X-Amz-Signature") {
			continue
		}
		if len(valueList) == 0 {
			pairs = append(pairs, queryPair{Key: key, Value: ""})
			continue
		}
		for _, value := range valueList {
			pairs = append(pairs, queryPair{Key: key, Value: value})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Key == pairs[j].Key {
			return pairs[i].Value < pairs[j].Value
		}
		return pairs[i].Key < pairs[j].Key
	})

	encoded := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		encoded = append(encoded, awsEncodeQuery(pair.Key)+"="+awsEncodeQuery(pair.Value))
	}
	return strings.Join(encoded, "&")
}

func awsEncodeQuery(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

func canonicalHeadersForRequest(r *http.Request, signedHeaders []string) (canonical string, signedRaw string, err error) {
	if len(signedHeaders) == 0 {
		return "", "", fmt.Errorf("%w: empty signed headers", ErrAuthorizationHeaderMalformed)
	}

	normalized := make([]string, 0, len(signedHeaders))
	lines := make([]string, 0, len(signedHeaders))
	for _, headerName := range signedHeaders {
		headerName = strings.ToLower(strings.TrimSpace(headerName))
		if headerName == "" {
			continue
		}
		var value string
		if headerName == "host" {
			value = r.Host
		} else {
			values, ok := r.Header[http.CanonicalHeaderKey(headerName)]
			if !ok || len(values) == 0 {
				return "", "", fmt.Errorf("%w: missing signed header %q", ErrAuthorizationHeaderMalformed, headerName)
			}
			value = strings.Join(values, ",")
		}
		value = normalizeHeaderValue(value)
		normalized = append(normalized, headerName)
		lines = append(lines, headerName+":"+value)
	}

	if len(lines) == 0 {
		return "", "", fmt.Errorf("%w: no valid signed headers", ErrAuthorizationHeaderMalformed)
	}
	signedRaw = strings.Join(normalized, ";")
	canonical = strings.Join(lines, "\n") + "\n"
	return canonical, signedRaw, nil
}

func normalizeHeaderValue(value string) string {
	value = strings.TrimSpace(value)
	parts := strings.Fields(value)
	return strings.Join(parts, " ")
}

func buildStringToSign(amzDate string, scope string, canonicalRequest string) string {
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}
