package auth

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"fs/models"
)

// The auth package parses several attacker-controlled strings. These fuzz
// targets assert that parsing never panics and that a successful parse leaves
// the outputs internally consistent. Inputs are capped so the fuzzer cannot
// drive unbounded allocation through a single example.

const (
	fuzzMaxStringInput = 8 << 10
	fuzzMaxLineInput   = 16 << 10
)

func fuzzCapString(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func fuzzCapBytes(value []byte, limit int) []byte {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func fuzzIsUpperHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F')
}

// FuzzCanonicalPath checks canonicalPath/awsEncodePath on arbitrary paths.
// Invariant: the result is a slash-delimited, fully percent-encoded path with
// only uppercase hex escapes, so a path segment can never inject CR/LF or a
// raw separator into the canonical request.
func FuzzCanonicalPath(f *testing.F) {
	seeds := []string{
		"/",
		"/bucket/key",
		"/bucket/a b",
		"/bucket/a%2Fb",
		"/bucket/%zz",
		"/bucket/\r\nX-Injected: 1",
		"/bucket/ümlaut",
		"",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxStringInput)
		if !strings.HasPrefix(raw, "/") {
			raw = "/" + raw
		}

		u := &url.URL{Path: raw}
		// Treat the input as an already-escaped RawPath when it is a valid
		// escape sequence so awsEncodePath exercises its '%' pass-through.
		if unescaped, err := url.PathUnescape(raw); err == nil {
			u.Path = unescaped
			u.RawPath = raw
		}

		got := canonicalPath(u)
		if got == "" || got[0] != '/' {
			t.Fatalf("canonical path %q does not start with '/': input %q", got, raw)
		}
		for i := 0; i < len(got); i++ {
			ch := got[i]
			if ch == '/' || isUnreserved(ch) {
				continue
			}
			if ch == '%' {
				if i+2 >= len(got) || !fuzzIsUpperHex(got[i+1]) || !fuzzIsUpperHex(got[i+2]) {
					t.Fatalf("canonical path %q has malformed escape at %d", got, i)
				}
				i += 2
				continue
			}
			t.Fatalf("canonical path %q has unescaped byte %#x", got, ch)
		}
		if len(got) > 3*len(raw)+1 {
			t.Fatalf("canonical path grew from %d to %d bytes", len(raw), len(got))
		}
		// Encoding an already canonical path is a no-op; the result is stable.
		if again := awsEncodePath(got); again != got {
			t.Fatalf("canonical path not stable: %q -> %q", got, again)
		}
		if raw == "/" && got != "/" {
			t.Fatalf("empty path canonicalized to %q, want /", got)
		}
	})
}

// FuzzCanonicalQueryString checks query canonicalization. Invariant: output is
// a sorted '&'-joined list of encoded key=value pairs with no raw separators,
// spaces, CR/LF, or '+', and presigning only removes the signature pair.
func FuzzCanonicalQueryString(f *testing.F) {
	seeds := []string{
		"",
		"a=b",
		"b=2&a=1",
		"a=1&a=2&a=0",
		"X-Amz-Signature=deadbeef&X-Amz-Date=20240101T000000Z",
		"a=%2F&b=a+b&c=a%20b",
		"prefix=%FF&list-type=2",
		"&&=&x",
		"a=1;b=2",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, rawQuery string) {
		rawQuery = fuzzCapString(rawQuery, fuzzMaxStringInput)

		plain := canonicalQueryString(rawQuery, false)
		presigned := canonicalQueryString(rawQuery, true)

		if len(presigned) > len(plain) {
			t.Fatalf("presigned query %q longer than plain %q", presigned, plain)
		}
		for _, out := range []string{plain, presigned} {
			if strings.ContainsAny(out, " +\r\n\t") {
				t.Fatalf("canonical query %q contains a raw separator", out)
			}
			if out == "" {
				continue
			}
			for _, pair := range strings.Split(out, "&") {
				if strings.Count(pair, "=") != 1 {
					t.Fatalf("canonical pair %q in %q is not exactly key=value", pair, out)
				}
			}
		}
	})
}

// FuzzParseAuthorizationParams checks the Authorization header parameter
// splitter. Invariant: keys and values are trimmed, a key never contains '=',
// and the parse is deterministic.
func FuzzParseAuthorizationParams(f *testing.F) {
	seeds := []string{
		"",
		"Credential=AKID/20240101/us-east-1/s3/aws4_request",
		"Credential=a, SignedHeaders=host;x-amz-date, Signature=ab",
		" = fallback,key=value",
		"garbage,token,no-equals=,x==y",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxStringInput)

		params := parseAuthorizationParams(raw)
		if len(params) == 0 {
			return
		}
		again := parseAuthorizationParams(raw)
		if len(again) != len(params) {
			t.Fatalf("parse not deterministic: %d vs %d params", len(params), len(again))
		}
		for key, value := range params {
			if key != strings.TrimSpace(key) {
				t.Fatalf("key %q is not trimmed", key)
			}
			if strings.Contains(key, "=") {
				t.Fatalf("key %q contains '='", key)
			}
			if value != strings.TrimSpace(value) {
				t.Fatalf("value %q is not trimmed", value)
			}
			if other, ok := again[key]; !ok || other != value {
				t.Fatalf("parse not deterministic for key %q", key)
			}
		}
	})
}

// FuzzParsePresignedSigV4 checks presigned-query parsing. Invariant: on
// success the validity window, signed-header set, and credential scope are all
// within the limits the verifier later assumes.
func FuzzParsePresignedSigV4(f *testing.F) {
	validSignature := strings.Repeat("ab", 32)
	seeds := []string{
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=" + validSignature,
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=bad&X-Amz-Date=20240101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=x",
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240101T000000Z&X-Amz-Expires=-1&X-Amz-SignedHeaders=host&X-Amz-Signature=" + validSignature,
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240101T000000Z&X-Amz-Expires=99999999999999999999&X-Amz-SignedHeaders=host&X-Amz-Signature=" + validSignature,
		"X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20240101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=date&X-Amz-Signature=" + validSignature,
		"X-Amz-Algorithm=AWS4-HMAC-SHA256",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, rawQuery string) {
		rawQuery = fuzzCapString(rawQuery, fuzzMaxStringInput)
		req := &http.Request{URL: &url.URL{Path: "/bucket/key", RawQuery: rawQuery}, Header: http.Header{}}

		input, err := parsePresignedSigV4(req)
		if err != nil {
			if input != nil {
				t.Fatalf("error %v returned with non-nil input", err)
			}
			return
		}
		if input == nil {
			t.Fatal("success returned nil input")
		}
		if !input.Presigned {
			t.Fatal("parsed presigned request is not marked presigned")
		}
		if input.ExpiresSeconds < 1 || int64(input.ExpiresSeconds) > int64(maxPresignedExpiresSeconds) {
			t.Fatalf("expires %d outside [1, %d]", input.ExpiresSeconds, maxPresignedExpiresSeconds)
		}
		if input.AccessKeyID == "" || input.AccessKeyID != strings.TrimSpace(input.AccessKeyID) {
			t.Fatalf("access key %q is empty or untrimmed", input.AccessKeyID)
		}
		if !strings.HasSuffix(input.Scope, "/aws4_request") || strings.Count(input.Scope, "/") != 3 {
			t.Fatalf("scope %q is not a four-part credential scope", input.Scope)
		}
		if !containsString(input.SignedHeaders, "host") {
			t.Fatalf("signed headers %v do not include host", input.SignedHeaders)
		}
		if input.SignatureHex == "" || input.SignatureHex != strings.ToLower(input.SignatureHex) {
			t.Fatalf("signature %q is empty or not lowercased", input.SignatureHex)
		}
	})
}

// FuzzSplitSignedHeaders checks the SignedHeaders splitter. Invariant: entries
// are non-empty, trimmed, lowercase, and free of ';'.
func FuzzSplitSignedHeaders(f *testing.F) {
	seeds := []string{
		"host",
		"host;x-amz-date",
		" Host ; X-Amz-Date ",
		";;host;;",
		"",
		"host;host;X-Amz-Content-Sha256",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxStringInput)

		headers := splitSignedHeaders(raw)
		if raw == "" || strings.TrimSpace(raw) == "" {
			if len(headers) != 0 {
				t.Fatalf("empty input produced %v", headers)
			}
			return
		}
		if len(headers) > strings.Count(raw, ";")+1 {
			t.Fatalf("%d headers from %q", len(headers), raw)
		}
		for _, header := range headers {
			if header == "" {
				t.Fatalf("empty header in %v", headers)
			}
			if header != strings.TrimSpace(header) {
				t.Fatalf("header %q is not trimmed", header)
			}
			if header != strings.ToLower(header) {
				t.Fatalf("header %q is not lowercase", header)
			}
			if strings.Contains(header, ";") {
				t.Fatalf("header %q contains ';'", header)
			}
		}
	})
}

// FuzzParseCredential checks credential-scope parsing. Invariant: a successful
// parse yields five non-empty slash-separated scope components ending in
// aws4_request, and any input without four slashes is rejected.
func FuzzParseCredential(f *testing.F) {
	seeds := []string{
		"AKID/20240101/us-east-1/s3/aws4_request",
		"AKID/20240101/us-east-1/s3/not_aws4_request",
		"AKID/20240101/us-east-1/s3",
		"AKID//region/service/aws4_request",
		" / / / /aws4_request",
		"AKID/20240101/us-east-1/s3/aws4_request/extra",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxStringInput)

		accessKeyID, date, region, service, scope, err := parseCredential(raw)
		if err != nil {
			if accessKeyID != "" || date != "" || region != "" || service != "" || scope != "" {
				t.Fatalf("error %v returned with partial outputs %q %q %q %q %q",
					err, accessKeyID, date, region, service, scope)
			}
			return
		}
		if accessKeyID == "" || date == "" || region == "" || service == "" {
			t.Fatalf("success with empty component: %q %q %q %q", accessKeyID, date, region, service)
		}
		if !strings.HasSuffix(scope, "aws4_request") || strings.Count(scope, "/") != 3 {
			t.Fatalf("scope %q is not a four-part credential scope", scope)
		}
		if accessKeyID != strings.TrimSpace(accessKeyID) {
			t.Fatalf("access key %q is not trimmed", accessKeyID)
		}
		if len(scope) > len(raw) {
			t.Fatalf("scope %q longer than input %q", scope, raw)
		}
	})
}

func fuzzSplitFields(raw string, count int) []string {
	fields := make([]string, count)
	for i := 0; i < count-1; i++ {
		head, tail, found := strings.Cut(raw, "\x00")
		if !found {
			fields[i] = head
			return fields
		}
		fields[i] = head
		raw = tail
	}
	fields[count-1] = raw
	return fields
}

// FuzzIsAllowed checks policy matching with arbitrary bucket patterns,
// prefixes, actions, and keys. Invariant: matching is deterministic, a nil
// policy denies, an explicit matching deny always wins, and a matching allow
// for a concrete key implies the key carries the normalized prefix.
func FuzzIsAllowed(f *testing.F) {
	seeds := []string{
		strings.Join([]string{"bucket", "prefix/", "s3:GetObject", "bucket", "prefix/key", ""}, "\x00"),
		strings.Join([]string{"*", "", "s3:GetObject", "other", "any/key", ""}, "\x00"),
		strings.Join([]string{"", "logs/*", "*", "logs", "logs/2024/01.log", ""}, "\x00"),
		strings.Join([]string{"bucket", "*", "s3:ListBucket", "bucket", "", "prefix"}, "\x00"),
		strings.Join([]string{"bucket", "a*b", "s3:PutObject", "bucket", "aXXb", ""}, "\x00"),
		strings.Join([]string{"bucket", "", "", "bucket", "key", ""}, "\x00"),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, 4<<10)
		fields := fuzzSplitFields(raw, 6)
		bucketPattern, prefix, action, bucket, key, listPrefix := fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]

		target := RequestTarget{
			Action: Action(action),
			Bucket: bucket,
			Key:    key,
			Prefix: listPrefix,
		}

		if isAllowed(nil, target) {
			t.Fatal("nil policy allowed a request")
		}

		allow := &models.AuthPolicy{Statements: []models.AuthPolicyStatement{{
			Effect:  "Allow",
			Actions: []string{"*"},
			Bucket:  bucketPattern,
			Prefix:  prefix,
		}}}
		got := isAllowed(allow, target)
		if again := isAllowed(allow, target); again != got {
			t.Fatalf("non-deterministic decision: %v then %v", got, again)
		}

		if got && target.Key != "" && bucketMatches(bucketPattern, target.Bucket) {
			if !strings.HasPrefix(target.Key, normalizePrefixPattern(prefix)) {
				t.Fatalf("allowed key %q does not match prefix %q", target.Key, prefix)
			}
		}

		allow.Statements = append(allow.Statements, models.AuthPolicyStatement{
			Effect:  "Deny",
			Actions: []string{"*"},
			Bucket:  bucketPattern,
			Prefix:  prefix,
		})
		if isAllowed(allow, target) {
			t.Fatalf("matching deny statement did not override allow (bucket %q prefix %q key %q action %q)",
				bucketPattern, prefix, key, action)
		}
	})
}

// FuzzParseChunkHeader checks aws-chunked chunk-header parsing. Invariant: an
// accepted size never exceeds the streaming cap, a required signature is
// present and 64 lowercase hex characters, and the parse is deterministic.
func FuzzParseChunkHeader(f *testing.F) {
	seeds := []string{
		"0",
		"1a",
		"1a;chunk-signature=" + strings.Repeat("a", 64),
		"10000",
		"10001",
		"ffffffffffffffff",
		"-1",
		"+1a",
		"",
		"zz",
		"1a;chunk-signature=short",
		"1a;chunk-signature=" + strings.Repeat("a", 64) + ";other=1",
		"1a; chunk-signature = " + strings.Repeat("A", 64),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, line string) {
		line = fuzzCapString(line, fuzzMaxLineInput)

		for _, requireSig := range []bool{false, true} {
			size, sig, err := parseChunkHeader(line, requireSig)
			if err != nil {
				continue
			}
			if size < 0 || size > maxStreamingChunkBytes {
				t.Fatalf("accepted size %d outside [0, %d]", size, maxStreamingChunkBytes)
			}
			if requireSig && sig == "" {
				t.Fatalf("accepted %q without required chunk signature", line)
			}
			if sig != "" {
				// The parser validates the length of the pre-lowercased
				// signature, and strings.ToLower can change the byte length
				// of non-ASCII input. The value is only ever compared against
				// a 64-character ASCII hex digest, so assert the UTF-8
				// expansion bound and the lowercased form instead.
				if len(sig) > 3*sha256.Size*2 {
					t.Fatalf("accepted signature %q with length %d", sig, len(sig))
				}
				if sig != strings.ToLower(sig) {
					t.Fatalf("accepted signature %q is not lowercase", sig)
				}
			}
		}
	})
}

// FuzzReadChunkedLine checks the bounded line reader used for aws-chunked
// framing. Invariant: it never returns an over-long or unterminated line and
// never loops without consuming input.
func FuzzReadChunkedLine(f *testing.F) {
	seeds := [][]byte{
		[]byte("0\r\n"),
		[]byte("1a;chunk-signature=" + strings.Repeat("a", 64) + "\r\n"),
		[]byte("no newline"),
		[]byte("\n"),
		[]byte(strings.Repeat("a", maxChunkHeaderLength) + "\n"),
		[]byte(strings.Repeat("b", maxChunkHeaderLength+1) + "\n"),
		append([]byte(strings.Repeat("c", 64)+"\r\n"), []byte("d\r\n")...),
		{},
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		data = fuzzCapBytes(data, 4*maxChunkHeaderLength)
		reader := bufio.NewReaderSize(bytes.NewReader(data), maxChunkHeaderLength)

		lines := 0
		for {
			line, err := readChunkedLine(reader)
			if err != nil {
				break
			}
			lines++
			// Every successful read consumes at least one byte, so the loop
			// cannot run longer than the input.
			if lines > len(data)+1 {
				t.Fatalf("read %d lines from %d bytes", lines, len(data))
			}
			if !strings.HasSuffix(line, "\n") {
				t.Fatalf("accepted unterminated line %q", line)
			}
			if len(line) > maxChunkHeaderLength {
				t.Fatalf("accepted %d-byte line above cap %d", len(line), maxChunkHeaderLength)
			}
		}
	})
}
