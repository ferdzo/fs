package auth

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// Official SigV4 streaming vectors from
// https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-streaming.html
// and .../sigv4-streaming-trailers.html.
const (
	officialSecret    = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	officialAmzDate   = "20130524T000000Z"
	officialScope     = "20130524/us-east-1/s3/aws4_request"
	officialSeed      = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
	officialChunk1    = "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"
	officialChunk2    = "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"
	officialZero      = "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"
	officialTrailSeed = "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e"
	officialTrailCh1  = "b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2"
	officialTrailCh2  = "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7"
	officialTrailZero = "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"
	officialTrailSig  = "d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"
	officialTrailLine = "x-amz-checksum-crc32c:sOO8/Q=="
	officialTrailHash = "1e376db7e1a34a8ef1c4bcee131a2d60a1cb62503747488624e10995f448d774"
)

func testHMACBytes(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func testHMACHex(key []byte, data string) string {
	return hex.EncodeToString(testHMACBytes(key, data))
}

func testDeriveSigningKey(secret, date, region, service string) []byte {
	key := testHMACBytes([]byte("AWS4"+secret), date)
	key = testHMACBytes(key, region)
	key = testHMACBytes(key, service)
	return testHMACBytes(key, "aws4_request")
}

func testSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func officialStreamingAuth(seed string) *StreamingAuth {
	return &StreamingAuth{
		SigningKey:    testDeriveSigningKey(officialSecret, "20130524", "us-east-1", "s3"),
		AmzDate:       officialAmzDate,
		Scope:         officialScope,
		SeedSignature: seed,
	}
}

func testStreamingAuth() *StreamingAuth {
	return &StreamingAuth{
		SigningKey:    []byte("test-signing-key"),
		AmzDate:       "20260102T030405Z",
		Scope:         "20260102/us-east-1/s3/aws4_request",
		SeedSignature: strings.Repeat("a", sha256.Size*2),
	}
}

func testStreamingTrailerAuth() *StreamingAuth {
	sa := testStreamingAuth()
	sa.TrailerMode = true
	sa.TrailerNames = []string{"x-amz-checksum-crc32c"}
	return sa
}

// testSignChunk mirrors the six-line AWS4-HMAC-SHA256-PAYLOAD string-to-sign.
func testSignChunk(sa *StreamingAuth, prev string, payload []byte) string {
	stringToSign := strings.Join([]string{
		chunkSigLabel,
		sa.AmzDate,
		sa.Scope,
		prev,
		testSHA256Hex(nil),
		testSHA256Hex(payload),
	}, "\n")
	return testHMACHex(sa.SigningKey, stringToSign)
}

// testSignTrailer mirrors the five-line AWS4-HMAC-SHA256-TRAILER string-to-sign
// where the final line is the hash of the canonical trailer block.
func testSignTrailer(sa *StreamingAuth, prev string, canonicalTrailer string) string {
	stringToSign := strings.Join([]string{
		trailerSigLabel,
		sa.AmzDate,
		sa.Scope,
		prev,
		testSHA256Hex([]byte(canonicalTrailer)),
	}, "\n")
	return testHMACHex(sa.SigningKey, stringToSign)
}

// testSignLegacyTrailer reproduces the previous, nonstandard behavior where
// the raw canonical trailer block (not its hash) was signed.
func testSignLegacyTrailer(sa *StreamingAuth, prev string, canonicalTrailer string) string {
	stringToSign := strings.Join([]string{
		trailerSigLabel,
		sa.AmzDate,
		sa.Scope,
		prev,
		canonicalTrailer,
	}, "\n")
	return testHMACHex(sa.SigningKey, stringToSign)
}

func testCanonicalTrailer(line string) string {
	name, value, _ := strings.Cut(line, ":")
	return strings.ToLower(strings.TrimSpace(name)) + ":" + strings.TrimSpace(value) + "\n"
}

type testStreamOverrides struct {
	terminalSig *string
	trailerSig  *string
}

// testSignedChunkedBody builds an aws-chunked body with optional overrides for
// the zero-chunk and trailer signatures.
func testSignedChunkedBody(sa *StreamingAuth, chunks [][]byte, trailerLine string, ov testStreamOverrides) string {
	var body strings.Builder
	prev := strings.ToLower(sa.SeedSignature)
	for _, chunk := range chunks {
		sig := testSignChunk(sa, prev, chunk)
		fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(chunk), sig)
		body.Write(chunk)
		body.WriteString("\r\n")
		prev = sig
	}

	zeroSig := testSignChunk(sa, prev, nil)
	if ov.terminalSig != nil {
		zeroSig = *ov.terminalSig
	}
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", zeroSig)

	if trailerLine != "" {
		body.WriteString(trailerLine)
		body.WriteString("\r\n")
		trailerSig := testSignTrailer(sa, zeroSig, testCanonicalTrailer(trailerLine))
		if ov.trailerSig != nil {
			trailerSig = *ov.trailerSig
		}
		fmt.Fprintf(&body, "%s:%s\r\n", trailerSignatureHeader, trailerSig)
	}
	body.WriteString("\r\n")
	return body.String()
}

func pumpTestStream(t *testing.T, body string, sa *StreamingAuth) ([]byte, error) {
	t.Helper()
	var out bytes.Buffer
	err := pumpSignedChunks(strings.NewReader(body), &out, sa != nil, sa)
	return out.Bytes(), err
}

func TestComputeChunkSignatureMatchesOfficialVector(t *testing.T) {
	sa := officialStreamingAuth(officialSeed)
	if got := computeChunkSignature(sa, officialSeed, bytes.Repeat([]byte("a"), 65536)); got != officialChunk1 {
		t.Fatalf("chunk 1 signature = %s, want %s", got, officialChunk1)
	}
	if got := computeChunkSignature(sa, officialChunk1, bytes.Repeat([]byte("a"), 1024)); got != officialChunk2 {
		t.Fatalf("chunk 2 signature = %s, want %s", got, officialChunk2)
	}
	if got := computeChunkSignature(sa, officialChunk2, nil); got != officialZero {
		t.Fatalf("zero chunk signature = %s, want %s", got, officialZero)
	}
}

func TestComputeTrailerSignatureMatchesOfficialVector(t *testing.T) {
	if got := testSHA256Hex([]byte(canonicalTrailerBlock([]string{officialTrailLine}))); got != officialTrailHash {
		t.Fatalf("canonical trailer hash = %s, want %s", got, officialTrailHash)
	}
	sa := officialStreamingAuth(officialTrailZero)
	if got := computeTrailerSignature(sa, officialTrailZero, []string{officialTrailLine}); got != officialTrailSig {
		t.Fatalf("trailer signature = %s, want %s", got, officialTrailSig)
	}
}

func TestPumpSignedChunksOfficialPayloadVector(t *testing.T) {
	sa := officialStreamingAuth(officialSeed)
	payload := append(bytes.Repeat([]byte("a"), 65536), bytes.Repeat([]byte("a"), 1024)...)
	sa.DecodedContentLength = int64(len(payload))
	sa.HasDecodedContentLength = true

	var body strings.Builder
	fmt.Fprintf(&body, "10000;chunk-signature=%s\r\n", officialChunk1)
	body.Write(payload[:65536])
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "400;chunk-signature=%s\r\n", officialChunk2)
	body.Write(payload[65536:])
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n\r\n", officialZero)

	out, err := pumpTestStream(t, body.String(), sa)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("decoded payload mismatch: got %d bytes want %d", len(out), len(payload))
	}
}

func TestPumpSignedChunksOfficialTrailerVector(t *testing.T) {
	sa := officialStreamingAuth(officialTrailSeed)
	sa.TrailerMode = true
	sa.TrailerNames = []string{"x-amz-checksum-crc32c"}
	payload := append(bytes.Repeat([]byte("a"), 65536), bytes.Repeat([]byte("a"), 1024)...)
	sa.DecodedContentLength = int64(len(payload))
	sa.HasDecodedContentLength = true

	var body strings.Builder
	fmt.Fprintf(&body, "10000;chunk-signature=%s\r\n", officialTrailCh1)
	body.Write(payload[:65536])
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "400;chunk-signature=%s\r\n", officialTrailCh2)
	body.Write(payload[65536:])
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", officialTrailZero)
	body.WriteString(officialTrailLine + "\r\n")
	fmt.Fprintf(&body, "%s:%s\r\n", trailerSignatureHeader, officialTrailSig)
	body.WriteString("\r\n")

	out, err := pumpTestStream(t, body.String(), sa)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("decoded payload mismatch: got %d bytes want %d", len(out), len(payload))
	}
}

func TestPumpSignedChunksAcceptsValidTerminal(t *testing.T) {
	sa := testStreamingAuth()
	payload := []byte("valid-signed-payload")
	sa.DecodedContentLength = int64(len(payload))
	sa.HasDecodedContentLength = true

	out, err := pumpTestStream(t, testSignedChunkedBody(sa, [][]byte{payload}, "", testStreamOverrides{}), sa)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("decoded payload = %q, want %q", out, payload)
	}
}

func TestPumpSignedChunksAcceptsValidTerminalWithTrailer(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("valid-signed-payload-with-trailer")
	sa.DecodedContentLength = int64(len(payload))
	sa.HasDecodedContentLength = true

	body := testSignedChunkedBody(sa, [][]byte{payload}, "x-amz-checksum-crc32c:dGVzdA==", testStreamOverrides{})
	out, err := pumpTestStream(t, body, sa)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("decoded payload = %q, want %q", out, payload)
	}
}

func TestPumpSignedChunksRejectsEmptyTerminalSignature(t *testing.T) {
	sa := testStreamingAuth()
	payload := []byte("truncation-target")
	empty := ""
	body := testSignedChunkedBody(sa, [][]byte{payload}, "", testStreamOverrides{terminalSig: &empty})

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsIncorrectTerminalSignature(t *testing.T) {
	sa := testStreamingAuth()
	payload := []byte("truncation-target")
	forged := strings.Repeat("0", sha256.Size*2)
	body := testSignedChunkedBody(sa, [][]byte{payload}, "", testStreamOverrides{terminalSig: &forged})

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsLegacyTrailerSignedTerminal(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("legacy-terminal")
	trailer := "x-amz-checksum-crc32c:dGVzdA=="
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	legacy := testSignLegacyTrailer(sa, prev, testCanonicalTrailer(trailer))
	body := testSignedChunkedBody(sa, [][]byte{payload}, trailer, testStreamOverrides{terminalSig: &legacy})

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsLegacyRawBlockTrailerSignature(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("legacy-trailer-sig")
	trailer := "x-amz-checksum-crc32c:dGVzdA=="
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	zero := testSignChunk(sa, prev, nil)
	legacy := testSignLegacyTrailer(sa, zero, testCanonicalTrailer(trailer))

	var body strings.Builder
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(payload), prev)
	body.Write(payload)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", zero)
	body.WriteString(trailer + "\r\n")
	fmt.Fprintf(&body, "%s:%s\r\n\r\n", trailerSignatureHeader, legacy)

	out, err := pumpTestStream(t, body.String(), sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsForgedTrailerSignature(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("forged-trailer")
	forged := strings.Repeat("f", sha256.Size*2)
	body := testSignedChunkedBody(sa, [][]byte{payload}, "x-amz-checksum-crc32c:dGVzdA==", testStreamOverrides{trailerSig: &forged})

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsTrailerWithoutSignature(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("unsigned-trailer")
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	zero := testSignChunk(sa, prev, nil)

	var body strings.Builder
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(payload), prev)
	body.Write(payload)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", zero)
	body.WriteString("x-amz-checksum-crc32c:dGVzdA==\r\n\r\n")

	out, err := pumpTestStream(t, body.String(), sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestNewSignedChunkedReaderPropagatesTerminalSignatureError(t *testing.T) {
	sa := testStreamingAuth()
	empty := ""
	body := testSignedChunkedBody(sa, [][]byte{[]byte("truncation-target")}, "", testStreamOverrides{terminalSig: &empty})

	reader := NewSignedChunkedReader(io.NopCloser(strings.NewReader(body)), sa)
	defer reader.Close()

	out, err := io.ReadAll(reader)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsDecodedContentLengthMismatch(t *testing.T) {
	payload := []byte("declared-length-target")
	for _, tc := range []struct {
		name     string
		declared int64
	}{
		{name: "too small", declared: int64(len(payload) - 1)},
		{name: "too large", declared: int64(len(payload) + 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sa := testStreamingAuth()
			sa.DecodedContentLength = tc.declared
			sa.HasDecodedContentLength = true
			body := testSignedChunkedBody(sa, [][]byte{payload}, "", testStreamOverrides{})

			out, err := pumpTestStream(t, body, sa)
			if !errors.Is(err, ErrDecodedContentLengthMismatch) {
				t.Fatalf("error = %v, want ErrDecodedContentLengthMismatch (decoded %q)", err, out)
			}
		})
	}
}

func TestPumpSignedChunksRejectsTrailerStreamDecodedLengthMismatch(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("trailer-length-target")
	sa.DecodedContentLength = int64(len(payload) + 1)
	sa.HasDecodedContentLength = true
	body := testSignedChunkedBody(sa, [][]byte{payload}, "x-amz-checksum-crc32c:dGVzdA==", testStreamOverrides{})

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrDecodedContentLengthMismatch) {
		t.Fatalf("error = %v, want ErrDecodedContentLengthMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksAcceptsUnsignedStreamWithoutSignatures(t *testing.T) {
	body := "4\r\nWiki\r\n0\r\n\r\n"
	out, err := pumpTestStream(t, body, nil)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if string(out) != "Wiki" {
		t.Fatalf("decoded payload = %q, want %q", out, "Wiki")
	}
}

func TestPumpSignedChunksAcceptsUnsignedTrailerStream(t *testing.T) {
	body := "4\r\nWiki\r\n0\r\nx-amz-checksum-crc32c:dGVzdA==\r\n\r\n"
	out, err := pumpTestStream(t, body, nil)
	if err != nil {
		t.Fatalf("pumpSignedChunks returned error: %v", err)
	}
	if string(out) != "Wiki" {
		t.Fatalf("decoded payload = %q, want %q", out, "Wiki")
	}
}

func TestParseChunkHeaderRequiresNonEmptySignature(t *testing.T) {
	for _, header := range []string{
		"5;chunk-signature=",
		"5;chunk-signature=   ",
		"5",
	} {
		if _, _, err := parseChunkHeader(header, true); err == nil {
			t.Fatalf("parseChunkHeader(%q) accepted missing signature", header)
		}
	}
	if _, _, err := parseChunkHeader("5;chunk-signature=", false); err != nil {
		t.Fatalf("unsigned parse rejected empty signature: %v", err)
	}
}

func TestResolveSignedStreamingMode(t *testing.T) {
	cases := []struct {
		in      string
		trailer bool
		ok      bool
	}{
		{streamingPayloadMode, false, true},
		{streamingTrailerPayloadMode, true, true},
		{strings.ToLower(streamingPayloadMode), false, true},
		{streamingPayloadMode + "-EXTRA", false, false},
		{"STREAMING-UNSIGNED-PAYLOAD-TRAILER", false, false},
		{"", false, false},
	}
	for _, tc := range cases {
		trailer, ok := resolveSignedStreamingMode(tc.in)
		if trailer != tc.trailer || ok != tc.ok {
			t.Fatalf("resolveSignedStreamingMode(%q) = (%v, %v), want (%v, %v)",
				tc.in, trailer, ok, tc.trailer, tc.ok)
		}
	}
}

// testSignedTrailerBody builds a trailing-headers stream with fully explicit
// trailer lines so tests can exercise duplicate/undeclared/missing names.
func testSignedTrailerBody(sa *StreamingAuth, payload []byte, trailerLines []string, sigOverride *string) string {
	var body strings.Builder
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(payload), prev)
	body.Write(payload)
	body.WriteString("\r\n")
	zero := testSignChunk(sa, prev, nil)
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", zero)

	canonical := ""
	for _, line := range trailerLines {
		body.WriteString(line)
		body.WriteString("\r\n")
		canonical += testCanonicalTrailer(line)
	}
	sig := testSignTrailer(sa, zero, canonical)
	if sigOverride != nil {
		sig = *sigOverride
	}
	fmt.Fprintf(&body, "%s:%s\r\n\r\n", trailerSignatureHeader, sig)
	return body.String()
}

func TestPumpSignedChunksRejectsMissingTrailersInTrailerMode(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("stripped-trailer")
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

func TestPumpSignedChunksRejectsNonTrailerModeWithTrailers(t *testing.T) {
	sa := testStreamingAuth()
	payload := []byte("trailer-not-allowed")
	body := testSignedTrailerBody(sa, payload, []string{"x-amz-checksum-crc32c:dGVzdA=="}, nil)

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsDuplicateTrailerHeader(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("duplicate-trailer")
	dup := []string{
		"x-amz-checksum-crc32c:dGVzdA==",
		"x-amz-checksum-crc32c:dGVzdA==",
	}
	body := testSignedTrailerBody(sa, payload, dup, nil)

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsUndeclaredTrailerHeader(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("undeclared-trailer")
	body := testSignedTrailerBody(sa, payload, []string{"x-amz-checksum-crc32:AAAAAA=="}, nil)

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsMissingDeclaredTrailer(t *testing.T) {
	sa := testStreamingTrailerAuth()
	sa.TrailerNames = []string{"x-amz-checksum-crc32c", "x-amz-checksum-crc32"}
	payload := []byte("missing-declared-trailer")
	body := testSignedTrailerBody(sa, payload, []string{"x-amz-checksum-crc32c:dGVzdA=="}, nil)

	out, err := pumpTestStream(t, body, sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsMultipleTrailerSignatures(t *testing.T) {
	sa := testStreamingTrailerAuth()
	payload := []byte("multi-trailer-sig")
	trailer := "x-amz-checksum-crc32c:dGVzdA=="
	prev := testSignChunk(sa, strings.ToLower(sa.SeedSignature), payload)
	zero := testSignChunk(sa, prev, nil)
	sig := testSignTrailer(sa, zero, testCanonicalTrailer(trailer))

	var body strings.Builder
	fmt.Fprintf(&body, "%x;chunk-signature=%s\r\n", len(payload), prev)
	body.Write(payload)
	body.WriteString("\r\n")
	fmt.Fprintf(&body, "0;chunk-signature=%s\r\n", zero)
	body.WriteString(trailer + "\r\n")
	fmt.Fprintf(&body, "%s:%s\r\n", trailerSignatureHeader, sig)
	fmt.Fprintf(&body, "%s:%s\r\n\r\n", trailerSignatureHeader, sig)

	out, err := pumpTestStream(t, body.String(), sa)
	if !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Fatalf("error = %v, want ErrChunkSignatureMismatch (decoded %q)", err, out)
	}
}

func TestPumpSignedChunksRejectsPrematureEOFFraming(t *testing.T) {
	t.Run("payload mode", func(t *testing.T) {
		sa := testStreamingAuth()
		payload := []byte("missing-final-crlf")
		body := testSignedChunkedBody(sa, [][]byte{payload}, "", testStreamOverrides{})
		body = strings.TrimSuffix(body, "\r\n")

		out, err := pumpTestStream(t, body, sa)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("error = %v, want io.ErrUnexpectedEOF (decoded %q)", err, out)
		}
	})
	t.Run("trailer mode", func(t *testing.T) {
		sa := testStreamingTrailerAuth()
		body := testSignedTrailerBody(sa, []byte("trailer-missing-final-crlf"), []string{"x-amz-checksum-crc32c:dGVzdA=="}, nil)
		body = strings.TrimSuffix(body, "\r\n")

		out, err := pumpTestStream(t, body, sa)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("error = %v, want io.ErrUnexpectedEOF (decoded %q)", err, out)
		}
	})
	t.Run("unsigned tolerates eof", func(t *testing.T) {
		out, err := pumpTestStream(t, "4\r\nWiki\r\n0\r\n", nil)
		if err != nil {
			t.Fatalf("pumpSignedChunks returned error: %v", err)
		}
		if string(out) != "Wiki" {
			t.Fatalf("decoded payload = %q, want %q", out, "Wiki")
		}
	})
}

func TestReadChunkedLineBoundsAllocation(t *testing.T) {
	reader := bufio.NewReaderSize(strings.NewReader(strings.Repeat("a", maxChunkHeaderLength+1)), maxChunkHeaderLength)
	if _, err := readChunkedLine(reader); err == nil {
		t.Fatalf("oversized unterminated line was accepted")
	}

	maxTerminated := strings.Repeat("a", maxChunkHeaderLength-1) + "\n"
	reader = bufio.NewReaderSize(strings.NewReader(maxTerminated), maxChunkHeaderLength)
	line, err := readChunkedLine(reader)
	if err != nil {
		t.Fatalf("max-sized terminated line rejected: %v", err)
	}
	if line != maxTerminated {
		t.Fatalf("line mismatch: got %d bytes want %d", len(line), len(maxTerminated))
	}
}

func TestReadTrailerHeadersBoundsRetainedState(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		body := strings.Repeat("x:a\r\n", maxTrailerCount+1) + "\r\n"
		reader := bufio.NewReaderSize(strings.NewReader(body), maxChunkHeaderLength)
		if _, err := readTrailerHeaders(reader, false); err == nil {
			t.Fatalf("trailer count over cap was accepted")
		}
	})
	t.Run("bytes", func(t *testing.T) {
		var body strings.Builder
		for i := 0; i < 10; i++ {
			body.WriteString(strings.Repeat("a", 7000))
			body.WriteString("\r\n")
		}
		body.WriteString("\r\n")
		reader := bufio.NewReaderSize(strings.NewReader(body.String()), maxChunkHeaderLength)
		if _, err := readTrailerHeaders(reader, false); err == nil {
			t.Fatalf("trailer byte cap was not enforced")
		}
	})
}

type panicReadCloser struct {
	closeCalled chan struct{}
}

func (p *panicReadCloser) Read([]byte) (int, error) { panic("reader panic") }

func (p *panicReadCloser) Close() error {
	select {
	case p.closeCalled <- struct{}{}:
	default:
	}
	return nil
}

func TestNewSignedChunkedReaderRecoversPanicAndClosesSource(t *testing.T) {
	src := &panicReadCloser{closeCalled: make(chan struct{}, 1)}
	reader := NewSignedChunkedReader(src, testStreamingAuth())

	done := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(reader)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected error after reader panic")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("consumer stranded after reader panic")
	}

	select {
	case <-src.closeCalled:
	case <-time.After(time.Second):
		t.Fatalf("source was not closed after panic")
	}
}

type trackingReadCloser struct {
	inner       *strings.Reader
	closeCalled chan struct{}
}

func (t *trackingReadCloser) Read(p []byte) (int, error) { return t.inner.Read(p) }

func (t *trackingReadCloser) Close() error {
	select {
	case t.closeCalled <- struct{}{}:
	default:
	}
	return nil
}

func TestNewSignedChunkedReaderClosesSource(t *testing.T) {
	src := &trackingReadCloser{
		inner:       strings.NewReader("4\r\nWiki\r\n0\r\n\r\n"),
		closeCalled: make(chan struct{}, 1),
	}
	reader := NewSignedChunkedReader(src, nil)

	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if string(out) != "Wiki" {
		t.Fatalf("decoded payload = %q, want %q", out, "Wiki")
	}

	select {
	case <-src.closeCalled:
	case <-time.After(time.Second):
		t.Fatalf("source was not closed after completion")
	}
}
