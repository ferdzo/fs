package auth

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
}

// ErrChunkSignatureMismatch is returned when a streamed chunk's signature
// does not match the chained HMAC computation.
var ErrChunkSignatureMismatch = errors.New("chunk signature mismatch")

const (
	chunkSigLabel        = "AWS4-HMAC-SHA256-PAYLOAD"
	trailerSigLabel      = "AWS4-HMAC-SHA256-TRAILER"
	chunkSigPrefix       = ";chunk-signature="
	maxChunkHeaderLength = 8 << 10
)

var emptyStringSHA256 = hex.EncodeToString(func() []byte {
	sum := sha256.Sum256(nil)
	return sum[:]
}())

// NewSignedChunkedReader wraps an aws-chunked request body. When sa is
// non-nil every chunk signature is verified against the chained HMAC;
// when sa is nil the framing is stripped without verification (auth
// disabled deployments have no secret to check against).
func NewSignedChunkedReader(src io.ReadCloser, sa *StreamingAuth) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		err := pumpSignedChunks(src, pw, sa != nil, sa)
		if err == nil {
			_ = pw.Close()
			return
		}
		println("DBG pump err:", err.Error())
		_ = pw.CloseWithError(err)
	}()
	return pr
}

func pumpSignedChunks(src io.Reader, dst io.Writer, verify bool, sa *StreamingAuth) error {
	reader := bufio.NewReaderSize(src, maxChunkHeaderLength)
	prevSig := ""
	if sa != nil {
		prevSig = strings.ToLower(sa.SeedSignature)
	}

	for {
		headerLine, err := readChunkedLine(reader)
		if err != nil {
			return err
		}
		size, chunkSig, err := parseChunkHeader(headerLine, verify)
		if err != nil {
			return err
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
			sig := computeChunkSignature(sa, prevSig, payload, nil)
			if sig != chunkSig {
				return fmt.Errorf("%w: expected %s got %s", ErrChunkSignatureMismatch, sig, chunkSig)
			}
			prevSig = sig
		}

		if size > 0 {
			if _, err := dst.Write(payload); err != nil {
				return err
			}
			continue
		}

		trailers, err := readTrailerHeaders(reader)
		if err != nil {
			return err
		}
		if sa != nil && len(trailers) > 0 {
			sig := computeChunkSignature(sa, prevSig, nil, trailers)
			if sig != chunkSig {
				return fmt.Errorf("%w: trailer signature expected %s got %s",
					ErrChunkSignatureMismatch, sig, chunkSig)
			}
		}
		return nil
	}
}

// parseChunkHeader extracts the chunk length and, when requireSig is true,
// the mandatory chunk-signature extension. Unverified streams may omit it.
func parseChunkHeader(line string, requireSig bool) (int64, string, error) {
	sizeToken := strings.TrimSpace(line)
	sig := ""
	if idx := strings.IndexByte(sizeToken, ';'); idx >= 0 {
		sig = strings.TrimSpace(sizeToken[idx+len(chunkSigPrefix):])
		sizeToken = sizeToken[:idx]
	} else if requireSig {
		return 0, "", errors.New("chunk header missing chunk-signature extension")
	}
	size, err := strconv.ParseInt(sizeToken, 16, 64)
	if err != nil || size < 0 {
		return 0, "", fmt.Errorf("invalid chunk size %q", sizeToken)
	}
	if sig != "" && len(sig) != sha256.Size*2 {
		return 0, "", fmt.Errorf("invalid chunk signature length %d", len(sig))
	}
	return size, strings.ToLower(sig), nil
}

// computeChunkSignature signs one streamed chunk. When trailers is non-nil
// the terminator uses the TRAILER label over the canonical trailer block;
// otherwise payload participates normally (empty payload => the spec's
// zero-chunk form).
func computeChunkSignature(sa *StreamingAuth, prevSig string, payload []byte, trailers []string) string {
	payloadHash := sha256.Sum256(payload)
	var label, tail string
	if trailers != nil {
		label = trailerSigLabel
		tail = canonicalTrailerBlock(trailers)
	} else {
		label = chunkSigLabel
		tail = strings.Join([]string{emptyStringSHA256, "", hex.EncodeToString(payloadHash[:])}, "\n")
	}
	stringToSign := strings.Join([]string{label, sa.AmzDate, sa.Scope, prevSig, tail}, "\n")
	mac := hmac.New(sha256.New, sa.SigningKey)
	mac.Write([]byte(stringToSign))
	return hex.EncodeToString(mac.Sum(nil))
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

// readTrailerHeaders consumes lines after the zero chunk until a blank line
// or EOF. A missing blank line at body end is tolerated.
func readTrailerHeaders(reader *bufio.Reader) ([]string, error) {
	var trailers []string
	for {
		line, err := readChunkedLine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return trailers, nil
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return trailers, nil
		}
		trailers = append(trailers, line)
	}
}

func readChunkedLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read chunked line: %w", err)
	}
	if len(line) > maxChunkHeaderLength {
		return "", errors.New("chunked header line too long")
	}
	return line, nil
}
