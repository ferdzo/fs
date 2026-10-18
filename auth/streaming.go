package auth

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"fs/utils"
	"io"
	"strconv"
	"strings"
)

// StreamingAuth carries the SigV4 material needed to verify per-chunk
// signatures of an aws-chunked request body. It is populated only when auth
// is enabled and the request declared a STREAMING-AWS4-HMAC-SHA256* payload.
type StreamingAuth struct {
	SigningKey    []byte
	AmzDate       string
	Scope         string
	SeedSignature string
	// DecodedContentLength is the signed x-amz-decoded-content-length value.
	// It is enforced against the total decoded payload only when
	// HasDecodedContentLength is true.
	DecodedContentLength    int64
	HasDecodedContentLength bool
	// TrailerMode is true only for STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER.
	// When set, the stream must end with the exact set of trailers named in
	// TrailerNames and carry a valid x-amz-trailer-signature.
	TrailerMode bool
	// TrailerNames are the lowercased, deduplicated names declared in the
	// signed x-amz-trailer header, in declaration order.
	TrailerNames []string
}

// ErrChunkSignatureMismatch is returned when a streamed chunk's signature
// does not match the chained HMAC computation.
var ErrChunkSignatureMismatch = errors.New("chunk signature mismatch")

const (
	chunkSigLabel        = "AWS4-HMAC-SHA256-PAYLOAD"
	trailerSigLabel      = "AWS4-HMAC-SHA256-TRAILER"
	maxChunkHeaderLength = 8 << 10
	// maxStreamingChunkBytes caps a single aws-chunked data chunk per the
	// AWS SigV4 streaming spec (64KiB). Without a cap a peer can declare a
	// multi-GB chunk and force a matching allocation via make([]byte, size).
	maxStreamingChunkBytes = 64 << 10

	// The only two signed streaming payload modes fs accepts. Anything else
	// with a recognized prefix is rejected rather than prefix-matched.
	streamingPayloadMode        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingTrailerPayloadMode = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	// trailerSignatureHeader carries the SigV4 trailer signature inside the
	// trailing-headers chunk.
	trailerSignatureHeader = "x-amz-trailer-signature"

	// Trailer framing bounds. They cap retained trailer state before append so
	// a peer cannot grow memory with an unbounded trailer section.
	maxTrailerBytes = 64 << 10
	maxTrailerCount = 32
)

// ErrChunkTooLarge is returned when a streamed chunk declares a size above
// maxStreamingChunkBytes.
var ErrChunkTooLarge = errors.New("streaming chunk too large")

// ErrDecodedContentLengthMismatch is returned when the decoded bytes of a
// signed stream do not match the signed x-amz-decoded-content-length.
var ErrDecodedContentLengthMismatch = errors.New("decoded content length mismatch")

var emptyStringSHA256 = hex.EncodeToString(func() []byte {
	sum := sha256.Sum256(nil)
	return sum[:]
}())

// NewSignedChunkedReader wraps an aws-chunked request body. When sa is
// non-nil every chunk signature is verified against the chained HMAC;
// when sa is nil the framing is stripped without verification (auth
// disabled deployments have no secret to check against).
//
// The returned reader owns src and closes it when the pump exits. Closing the
// returned reader unblocks the pump at its next write, but cannot interrupt a
// Read already blocked inside src; callers retain responsibility for bounding
// request-body reads (HTTP server body deadlines / context cancellation).
//
// The worker closes the pipe with an error even if a src.Read panics, so a
// consumer is never stranded; the panic is re-raised for utils.Go to log.
func NewSignedChunkedReader(src io.ReadCloser, sa *StreamingAuth) io.ReadCloser {
	pr, pw := io.Pipe()
	utils.Go("signed-chunked-pump", func() {
		srcClosed := false
		closeSrc := func() {
			if srcClosed {
				return
			}
			srcClosed = true
			_ = src.Close()
		}
		defer func() {
			if r := recover(); r != nil {
				_ = pw.CloseWithError(fmt.Errorf("signed chunked reader panic: %v", r))
				closeSrc()
				panic(r)
			}
		}()

		err := pumpSignedChunks(src, pw, sa != nil, sa)
		if err == nil {
			_ = pw.Close()
		} else {
			_ = pw.CloseWithError(err)
		}
		closeSrc()
	})
	return pr
}

func pumpSignedChunks(src io.Reader, dst io.Writer, verify bool, sa *StreamingAuth) error {
	reader := bufio.NewReaderSize(src, maxChunkHeaderLength)
	prevSig := ""
	if sa != nil {
		prevSig = strings.ToLower(sa.SeedSignature)
	}
	var decodedLength int64

	for {
		headerLine, err := readChunkedLine(reader)
		if err != nil {
			return err
		}
		size, chunkSig, err := parseChunkHeader(headerLine, verify)
		if err != nil {
			return err
		}
		if sa != nil && sa.HasDecodedContentLength && size > 0 &&
			decodedLength+size > sa.DecodedContentLength {
			return fmt.Errorf("%w: decoded at least %d exceeds declared %d",
				ErrDecodedContentLengthMismatch, decodedLength+size, sa.DecodedContentLength)
		}

		var payload []byte
		if size > 0 {
			payload = make([]byte, size)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return fmt.Errorf("truncated chunk payload: %w", err)
			}
			crlf := make([]byte, 2)
			if _, err := io.ReadFull(reader, crlf); err != nil || crlf[0] != '\r' || crlf[1] != '\n' {
				return errors.New("malformed chunk terminator")
			}
		}

		if sa != nil && size > 0 {
			sig := computeChunkSignature(sa, prevSig, payload)
			if !equalSignature(sig, chunkSig) {
				return fmt.Errorf("%w: expected %s got %s", ErrChunkSignatureMismatch, sig, chunkSig)
			}
			prevSig = sig
		}

		if size > 0 {
			if _, err := dst.Write(payload); err != nil {
				return err
			}
			decodedLength += size
			continue
		}

		trailers, err := readTrailerHeaders(reader, sa != nil)
		if err != nil {
			return err
		}
		if sa != nil {
			// The zero-size chunk is a normal PAYLOAD chunk signing an empty
			// payload; trailing headers carry a separate TRAILER signature.
			terminalSig := computeChunkSignature(sa, prevSig, nil)
			if !equalSignature(terminalSig, chunkSig) {
				return fmt.Errorf("%w: terminal signature expected %s got %s",
					ErrChunkSignatureMismatch, terminalSig, chunkSig)
			}
			if err := verifyTrailerSection(sa, trailers, chunkSig); err != nil {
				return err
			}
			if sa.HasDecodedContentLength && decodedLength != sa.DecodedContentLength {
				return fmt.Errorf("%w: decoded %d declared %d",
					ErrDecodedContentLengthMismatch, decodedLength, sa.DecodedContentLength)
			}
		}
		return nil
	}
}

// splitTrailerSignature separates the x-amz-trailer-signature line from the
// trailer headers that are covered by the signature. sigCount > 1 is
// ambiguous and rejected by the caller.
func splitTrailerSignature(trailers []string) (canonical []string, sig string, sigCount int) {
	for _, line := range trailers {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			canonical = append(canonical, line)
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), trailerSignatureHeader) {
			sig = strings.TrimSpace(value)
			sigCount++
			continue
		}
		canonical = append(canonical, line)
	}
	return canonical, sig, sigCount
}

// resolveSignedStreamingMode reports the trailer flag for the two supported
// signed streaming payload modes. ok is false for every other value.
func resolveSignedStreamingMode(payloadHash string) (trailerMode bool, ok bool) {
	switch strings.ToUpper(strings.TrimSpace(payloadHash)) {
	case streamingPayloadMode:
		return false, true
	case streamingTrailerPayloadMode:
		return true, true
	default:
		return false, false
	}
}

// verifyTrailerSection enforces the signed trailer contract. A non-trailer
// stream must not carry trailer headers; a trailer stream must carry exactly
// the declared set of named trailers plus one valid x-amz-trailer-signature.
func verifyTrailerSection(sa *StreamingAuth, trailers []string, zeroSig string) error {
	if !sa.TrailerMode {
		if len(trailers) > 0 {
			return fmt.Errorf("%w: trailer headers in non-trailer signed stream", ErrChunkSignatureMismatch)
		}
		return nil
	}

	canonical, trailerSig, sigCount := splitTrailerSignature(trailers)
	if sigCount == 0 {
		return fmt.Errorf("%w: missing %s", ErrChunkSignatureMismatch, trailerSignatureHeader)
	}
	if sigCount > 1 {
		return fmt.Errorf("%w: multiple %s headers", ErrChunkSignatureMismatch, trailerSignatureHeader)
	}
	if len(canonical) == 0 {
		return fmt.Errorf("%w: trailer mode without trailing headers", ErrChunkSignatureMismatch)
	}

	present := make(map[string]struct{}, len(canonical))
	for _, line := range canonical {
		name, _, ok := strings.Cut(line, ":")
		if !ok {
			return fmt.Errorf("%w: malformed trailer header", ErrChunkSignatureMismatch)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || name == trailerSignatureHeader {
			return fmt.Errorf("%w: malformed trailer header name", ErrChunkSignatureMismatch)
		}
		if _, dup := present[name]; dup {
			return fmt.Errorf("%w: duplicate trailer header %q", ErrChunkSignatureMismatch, name)
		}
		if !trailerNameDeclared(sa.TrailerNames, name) {
			return fmt.Errorf("%w: undeclared trailer header %q", ErrChunkSignatureMismatch, name)
		}
		present[name] = struct{}{}
	}
	for _, declared := range sa.TrailerNames {
		if _, ok := present[declared]; !ok {
			return fmt.Errorf("%w: missing declared trailer %q", ErrChunkSignatureMismatch, declared)
		}
	}

	expected := computeTrailerSignature(sa, zeroSig, canonical)
	if !equalSignature(expected, trailerSig) {
		return fmt.Errorf("%w: trailer signature expected %s got %s",
			ErrChunkSignatureMismatch, expected, trailerSig)
	}
	return nil
}

func trailerNameDeclared(declared []string, name string) bool {
	for _, candidate := range declared {
		if strings.EqualFold(candidate, name) {
			return true
		}
	}
	return false
}

// parseChunkHeader extracts the chunk length and, when requireSig is true,
// the mandatory chunk-signature extension. Unverified streams may omit it.
func parseChunkHeader(line string, requireSig bool) (int64, string, error) {
	sizeToken := strings.TrimSpace(line)
	sig := ""
	hasSig := false
	if idx := strings.IndexByte(sizeToken, ';'); idx >= 0 {
		extensions := sizeToken[idx+1:]
		sizeToken = sizeToken[:idx]
		for _, extension := range strings.Split(extensions, ";") {
			name, value, found := strings.Cut(extension, "=")
			if !found || !strings.EqualFold(strings.TrimSpace(name), "chunk-signature") {
				continue
			}
			sig = strings.TrimSpace(value)
			hasSig = true
			break
		}
	}
	if requireSig && (!hasSig || sig == "") {
		return 0, "", fmt.Errorf("%w: chunk header missing chunk-signature extension", ErrChunkSignatureMismatch)
	}
	size, err := strconv.ParseInt(sizeToken, 16, 64)
	if err != nil || size < 0 {
		return 0, "", fmt.Errorf("invalid chunk size %q", sizeToken)
	}
	if size > maxStreamingChunkBytes {
		return 0, "", fmt.Errorf("%w: declared %d exceeds maximum %d", ErrChunkTooLarge, size, maxStreamingChunkBytes)
	}
	if sig != "" && len(sig) != sha256.Size*2 {
		return 0, "", fmt.Errorf("%w: invalid chunk signature length %d", ErrChunkSignatureMismatch, len(sig))
	}
	return size, strings.ToLower(sig), nil
}

// computeChunkSignature signs one streamed chunk with the SigV4
// AWS4-HMAC-SHA256-PAYLOAD string-to-sign, which is exactly these six lines:
//
//	AWS4-HMAC-SHA256-PAYLOAD
//	<amz-date>
//	<scope>
//	<previous-signature>
//	SHA256("")
//	SHA256(chunk-data)
//
// The zero-size terminal chunk is signed the same way with an empty payload.
func computeChunkSignature(sa *StreamingAuth, prevSig string, payload []byte) string {
	payloadHash := sha256.Sum256(payload)
	stringToSign := strings.Join([]string{
		chunkSigLabel,
		sa.AmzDate,
		sa.Scope,
		prevSig,
		emptyStringSHA256,
		hex.EncodeToString(payloadHash[:]),
	}, "\n")
	return hmacSHA256Hex(sa.SigningKey, stringToSign)
}

// computeTrailerSignature signs the trailing-headers chunk with the SigV4
// AWS4-HMAC-SHA256-TRAILER string-to-sign:
//
//	AWS4-HMAC-SHA256-TRAILER
//	<amz-date>
//	<scope>
//	<previous-signature>  (the zero-size chunk signature)
//	SHA256(canonical trailer block)
//
// The canonical trailer block is the lowercased-name:normalized-value lines
// separated by '\n' with a trailing '\n', and only the hash of that block is
// signed.
func computeTrailerSignature(sa *StreamingAuth, prevSig string, trailers []string) string {
	blockHash := sha256.Sum256([]byte(canonicalTrailerBlock(trailers)))
	stringToSign := strings.Join([]string{
		trailerSigLabel,
		sa.AmzDate,
		sa.Scope,
		prevSig,
		hex.EncodeToString(blockHash[:]),
	}, "\n")
	return hmacSHA256Hex(sa.SigningKey, stringToSign)
}

func hmacSHA256Hex(key []byte, data string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func equalSignature(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func canonicalTrailerBlock(trailers []string) string {
	normalized := make([]string, 0, len(trailers))
	for _, t := range trailers {
		name, value, found := strings.Cut(t, ":")
		if !found {
			continue
		}
		normalized = append(normalized,
			strings.ToLower(strings.TrimSpace(name))+":"+normalizeHeaderValue(value))
	}
	if len(normalized) == 0 {
		return ""
	}
	return strings.Join(normalized, "\n") + "\n"
}

// readTrailerHeaders consumes lines after the zero chunk until a blank line.
// For signed streams a missing terminator is incomplete framing and rejected;
// unsigned streams retain the historical EOF tolerance. Retained trailer state
// is bounded by maxTrailerBytes and maxTrailerCount.
func readTrailerHeaders(reader *bufio.Reader, requireTerminator bool) ([]string, error) {
	var trailers []string
	total := 0
	for {
		line, err := readChunkedLine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if requireTerminator {
					return nil, fmt.Errorf("incomplete chunked framing: %w", io.ErrUnexpectedEOF)
				}
				return trailers, nil
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return trailers, nil
		}
		total += len(line)
		if total > maxTrailerBytes || len(trailers) >= maxTrailerCount {
			return nil, errors.New("trailer section too large")
		}
		trailers = append(trailers, line)
	}
}

// readChunkedLine reads one newline-terminated line without allocating past
// the bufio buffer. ReadSlice reports bufio.ErrBufferFull instead of growing,
// so an unterminated oversized line is rejected before a large allocation.
func readChunkedLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", errors.New("chunked header line too long")
	}
	if err != nil {
		return "", fmt.Errorf("read chunked line: %w", err)
	}
	if len(line) > maxChunkHeaderLength {
		return "", errors.New("chunked header line too long")
	}
	return string(line), nil
}
