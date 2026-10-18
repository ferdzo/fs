package auth

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequiredDecodedContentLength(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		set     bool
		want    int64
		wantErr bool
	}{
		{name: "missing", set: false, wantErr: true},
		{name: "blank", value: "   ", set: true, wantErr: true},
		{name: "non-numeric", value: "abc", set: true, wantErr: true},
		{name: "float", value: "1.5", set: true, wantErr: true},
		{name: "negative", value: "-1", set: true, wantErr: true},
		{name: "zero", value: "0", set: true, want: 0},
		{name: "valid", value: "66560", set: true, want: 66560},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://localhost:2600/b/k", nil)
			if tc.set {
				req.Header.Set("x-amz-decoded-content-length", tc.value)
			}
			got, err := requiredDecodedContentLength(req)
			if tc.wantErr {
				if !errors.Is(err, ErrAuthorizationHeaderMalformed) {
					t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("length = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestRequiredTrailerNames(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		set     bool
		want    []string
		wantErr bool
	}{
		{name: "missing", set: false, wantErr: true},
		{name: "blank", value: "  ", set: true, wantErr: true},
		{name: "empty entry", value: ",", set: true, wantErr: true},
		{name: "reserved", value: "x-amz-trailer-signature", set: true, wantErr: true},
		{name: "duplicate", value: "x-amz-checksum-crc32c,x-amz-checksum-crc32c", set: true, wantErr: true},
		{name: "single", value: "x-amz-checksum-crc32c", set: true, want: []string{"x-amz-checksum-crc32c"}},
		{name: "case and order", value: " X-Amz-Checksum-Crc32c , x-amz-checksum-crc32 ", set: true,
			want: []string{"x-amz-checksum-crc32c", "x-amz-checksum-crc32"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://localhost:2600/b/k", nil)
			if tc.set {
				req.Header.Set("x-amz-trailer", tc.value)
			}
			got, err := requiredTrailerNames(req)
			if tc.wantErr {
				if !errors.Is(err, ErrAuthorizationHeaderMalformed) {
					t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("names = %v, want %v", got, tc.want)
			}
		})
	}
}

// newSignedStreamingRequest builds a PUT request whose envelope signature is
// valid for the given credentials, payload mode, extra headers, and signed
// header list.
func newSignedStreamingRequest(t *testing.T, req *http.Request, accessKey, secret, mode string, extraHeaders map[string]string, signed []string) {
	t.Helper()

	amzDate := time.Now().UTC().Format("20060102T150405Z")
	date := amzDate[:8]
	scope := strings.Join([]string{date, "us-east-1", "s3", "aws4_request"}, "/")

	req.Host = "localhost:2600"
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", mode)
	req.Header.Set("Content-Encoding", "aws-chunked")
	for key, value := range extraHeaders {
		req.Header.Set(key, value)
	}

	canonicalHeaders, signedRaw, err := canonicalHeadersForRequest(req, signed)
	if err != nil {
		t.Fatalf("canonicalHeadersForRequest: %v", err)
	}
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonicalQueryString(req.URL.RawQuery, false),
		canonicalHeaders,
		signedRaw,
		mode,
	}, "\n")
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		amzDate,
		scope,
		testSHA256Hex([]byte(canonicalRequest)),
	}, "\n")
	key := testDeriveSigningKey(secret, date, "us-east-1", "s3")
	signature := hex.EncodeToString(testHMACBytes(key, stringToSign))

	req.Header.Set("Authorization", strings.Join([]string{
		sigV4Algorithm + " Credential=" + accessKey + "/" + scope,
		"SignedHeaders=" + signedRaw,
		"Signature=" + signature,
	}, ", "))
}

func newStreamingUserService(t *testing.T) *Service {
	t.Helper()
	_, svc := newTestAuthService(t)
	if err := svc.EnsureBootstrap(); err != nil {
		t.Fatalf("EnsureBootstrap: %v", err)
	}
	return svc
}

func streamingRequest(t *testing.T) *http.Request {
	t.Helper()
	return httptest.NewRequest(http.MethodPut, "http://localhost:2600/test-bucket/key.bin", nil)
}

var payloadSignedHeaders = []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length"}

var trailerSignedHeaders = []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-trailer"}

func TestAuthenticateRequestPopulatesStreamingDecodedContentLength(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode,
		map[string]string{"x-amz-decoded-content-length": "66560"}, payloadSignedHeaders)

	ctx, err := svc.AuthenticateRequest(req)
	if err != nil {
		t.Fatalf("AuthenticateRequest returned error: %v", err)
	}
	if ctx.Streaming == nil {
		t.Fatalf("Streaming is nil for signed streaming request")
	}
	if !ctx.Streaming.HasDecodedContentLength || ctx.Streaming.DecodedContentLength != 66560 {
		t.Fatalf("decoded length = (%v, %d), want (true, 66560)",
			ctx.Streaming.HasDecodedContentLength, ctx.Streaming.DecodedContentLength)
	}
	if ctx.Streaming.TrailerMode {
		t.Fatalf("TrailerMode = true for payload mode")
	}
	if len(ctx.Streaming.TrailerNames) != 0 {
		t.Fatalf("TrailerNames = %v, want none", ctx.Streaming.TrailerNames)
	}
}

func TestAuthenticateRequestRejectsStreamingWithoutDecodedLength(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode, nil,
		[]string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date"})

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestAuthenticateRequestRejectsUnsignedDecodedLength(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode,
		map[string]string{"x-amz-decoded-content-length": "66560"},
		[]string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date"})

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestAuthenticateRequestRejectsStreamingInvalidDecodedLength(t *testing.T) {
	for _, value := range []string{"abc", "-1", "1.5"} {
		t.Run(value, func(t *testing.T) {
			svc := newStreamingUserService(t)
			req := streamingRequest(t)
			newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode,
				map[string]string{"x-amz-decoded-content-length": value}, payloadSignedHeaders)

			if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
				t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
			}
		})
	}
}

func TestAuthenticateRequestPopulatesTrailerStreaming(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingTrailerPayloadMode,
		map[string]string{
			"x-amz-decoded-content-length": "3",
			"x-amz-trailer":                "x-amz-checksum-crc32c",
		}, trailerSignedHeaders)

	ctx, err := svc.AuthenticateRequest(req)
	if err != nil {
		t.Fatalf("AuthenticateRequest returned error: %v", err)
	}
	if ctx.Streaming == nil || !ctx.Streaming.TrailerMode {
		t.Fatalf("Streaming = %+v, want TrailerMode true", ctx.Streaming)
	}
	if len(ctx.Streaming.TrailerNames) != 1 || ctx.Streaming.TrailerNames[0] != "x-amz-checksum-crc32c" {
		t.Fatalf("TrailerNames = %v, want [x-amz-checksum-crc32c]", ctx.Streaming.TrailerNames)
	}
}

func TestAuthenticateRequestRejectsTrailerModeMissingTrailerHeader(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingTrailerPayloadMode,
		map[string]string{"x-amz-decoded-content-length": "3"}, payloadSignedHeaders)

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestAuthenticateRequestRejectsUnsignedTrailerHeader(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingTrailerPayloadMode,
		map[string]string{
			"x-amz-decoded-content-length": "3",
			"x-amz-trailer":                "x-amz-checksum-crc32c",
		}, payloadSignedHeaders)

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestAuthenticateRequestRejectsTrailerHeaderInPayloadMode(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode,
		map[string]string{
			"x-amz-decoded-content-length": "3",
			"x-amz-trailer":                "x-amz-checksum-crc32c",
		}, trailerSignedHeaders)

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

func TestAuthenticateRequestRejectsUnsupportedStreamingMode(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingPayloadMode+"-EXTRA",
		map[string]string{"x-amz-decoded-content-length": "3"}, payloadSignedHeaders)

	if _, err := svc.AuthenticateRequest(req); !errors.Is(err, ErrAuthorizationHeaderMalformed) {
		t.Fatalf("error = %v, want ErrAuthorizationHeaderMalformed", err)
	}
}

// TestAuthenticateRequestThenRejectsStrippedTrailers proves the accepted
// envelope cannot be downgraded to "no trailers" at the framing layer.
func TestAuthenticateRequestThenRejectsStrippedTrailers(t *testing.T) {
	svc := newStreamingUserService(t)
	req := streamingRequest(t)
	newSignedStreamingRequest(t, req, "root-user", "root-secret-123", streamingTrailerPayloadMode,
		map[string]string{
			"x-amz-decoded-content-length": "3",
			"x-amz-trailer":                "x-amz-checksum-crc32c",
		}, trailerSignedHeaders)

	ctx, err := svc.AuthenticateRequest(req)
	if err != nil {
		t.Fatalf("AuthenticateRequest returned error: %v", err)
	}
	sa := ctx.Streaming

	payload := []byte("abc")
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	zero := testSignChunk(sa, prev, nil)
	var body strings.Builder
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(payload), prev)
	body.Write(payload)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n\r\n", zero)

	out, err := pumpTestStream(t, body.String(), sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}
