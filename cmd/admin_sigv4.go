package cmd

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const sigV4Algorithm = "AWS4-HMAC-SHA256"

func signSigV4Request(req *http.Request, body []byte, accessKey, secretKey, region, service string) error {
	if req == nil {
		return fmt.Errorf("nil request")
	}
	if strings.TrimSpace(accessKey) == "" || strings.TrimSpace(secretKey) == "" {
		return fmt.Errorf("missing signing credentials")
	}
	if strings.TrimSpace(region) == "" || strings.TrimSpace(service) == "" {
		return fmt.Errorf("missing signing scope")
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	shortDate := now.Format("20060102")
	scope := shortDate + "/" + region + "/" + service + "/aws4_request"

	payloadHash := sha256Hex(body)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.URL.Host
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonicalRequest, signedHeadersRaw := buildCanonicalRequest(req, signedHeaders, payloadHash)
	stringToSign := buildStringToSign(amzDate, scope, canonicalRequest)
	signature := hex.EncodeToString(hmacSHA256(deriveSigningKey(secretKey, shortDate, region, service), stringToSign))

	authHeader := fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		sigV4Algorithm,
		accessKey,
		scope,
		signedHeadersRaw,
		signature,
	)
	req.Header.Set("Authorization", authHeader)
	req.Host = host
	return nil
}

func buildCanonicalRequest(req *http.Request, signedHeaders []string, payloadHash string) (string, string) {
	canonicalHeaders, signedHeadersRaw := canonicalHeaders(req, signedHeaders)
	return strings.Join([]string{
		req.Method,
		canonicalPath(req.URL),
		canonicalQuery(req.URL),
		canonicalHeaders,
		signedHeadersRaw,
		payloadHash,
	}, "\n"), signedHeadersRaw
}

func canonicalPath(u *url.URL) string {
	if u == nil {
		return "/"
	}
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

func canonicalQuery(u *url.URL) string {
	if u == nil {
		return ""
	}
	values := u.Query()
	type pair struct {
		key   string
		value string
	}
	pairs := make([]pair, 0, len(values))
	for key, vals := range values {
		if len(vals) == 0 {
			pairs = append(pairs, pair{key: key, value: ""})
			continue
		}
		for _, v := range vals {
			pairs = append(pairs, pair{key: key, value: v})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, awsEncodeQuery(p.key)+"="+awsEncodeQuery(p.value))
	}
	return strings.Join(out, "&")
}

func awsEncodeQuery(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
}

func canonicalHeaders(req *http.Request, headers []string) (string, string) {
	names := make([]string, 0, len(headers))
	lines := make([]string, 0, len(headers))
	for _, h := range headers {
		name := strings.ToLower(strings.TrimSpace(h))
		if name == "" {
			continue
		}
		var value string
		if name == "host" {
			value = req.URL.Host
		} else {
			value = strings.Join(req.Header.Values(http.CanonicalHeaderKey(name)), ",")
		}
		value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
		names = append(names, name)
		lines = append(lines, name+":"+value)
	}
	return strings.Join(lines, "\n") + "\n", strings.Join(names, ";")
}

func buildStringToSign(amzDate, scope, canonicalRequest string) string {
	hash := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		hex.EncodeToString(hash[:]),
	}, "\n")
}

func deriveSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
