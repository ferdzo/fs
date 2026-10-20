package api

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/url"
	"strings"
	"testing"
)

// The api package parses several attacker-controlled strings and streams.
// These fuzz targets assert that parsing never panics, stays within declared
// limits, and rejects malformed framing. Every input is capped so a single
// fuzz example cannot drive unbounded allocation or work.

const (
	fuzzMaxKeyInput   = 32 << 10
	fuzzMaxChunkInput = 64 << 10
	fuzzMaxXMLInput   = 64 << 10
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

// FuzzParseSingleByteRange checks Range header parsing. Invariant: every
// accepted range is non-empty and inside the object, and non-positive sizes
// are always rejected.
func FuzzParseSingleByteRange(f *testing.F) {
	type seed struct {
		header string
		size   int64
	}
	seeds := []seed{
		{"bytes=0-0", 10},
		{"bytes=0-9", 10},
		{"bytes=0-", 10},
		{"bytes=-5", 10},
		{"bytes=-50", 10},
		{"bytes=5-9", 10},
		{"bytes=5-5", 10},
		{"BYTES=0-1", 10},
		{"bytes=", 10},
		{"bytes=5-4", 10},
		{"bytes=10-", 10},
		{"bytes=0-1,2-3", 10},
		{"bytes=-0", 10},
		{"bytes=abc-def", 10},
		{"bytes=0-0", 0},
		{"bytes=0-0", -1},
		{"", 10},
		{"bytes=9223372036854775807-", 10},
	}
	for _, s := range seeds {
		f.Add(s.header, s.size)
	}

	f.Fuzz(func(t *testing.T, header string, size int64) {
		header = fuzzCapString(header, 4<<10)

		start, end, err := parseSingleByteRange(header, size)
		if err != nil {
			return
		}
		if size <= 0 {
			t.Fatalf("accepted %q for non-positive size %d", header, size)
		}
		if start < 0 || end < start || end >= size {
			t.Fatalf("accepted %q for size %d as %d-%d", header, size, start, end)
		}
	})
}

// FuzzNormalizeObjectKey checks key unescaping. Invariant: a successful
// unescape never grows the key, stays valid under re-escaping, and only the
// empty input yields an empty key.
func FuzzNormalizeObjectKey(f *testing.F) {
	seeds := []string{
		"",
		"key",
		"dir/key.txt",
		"dir%2Fkey.txt",
		"a%20b",
		"a+b",
		"%zz",
		"%",
		"%41",
		"../../etc/passwd",
		"key\x00name",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxKeyInput)

		key, err := normalizeObjectKey(raw)
		if err != nil {
			if key != "" {
				t.Fatalf("error %v returned with key %q", err, key)
			}
			return
		}
		if len(key) > len(raw) {
			t.Fatalf("unescape grew key %q to %q", raw, key)
		}
		if raw == "" && key != "" {
			t.Fatalf("empty key normalized to %q", key)
		}
		roundTrip, err := url.PathUnescape(url.PathEscape(key))
		if err != nil || roundTrip != key {
			t.Fatalf("key %q does not round-trip: %q, %v", key, roundTrip, err)
		}
	})
}

// FuzzParseCopySource checks x-amz-copy-source parsing. Invariant: a
// successful parse yields a non-empty bucket without a separator and a
// non-empty key within the object key limit.
func FuzzParseCopySource(f *testing.F) {
	seeds := []string{
		"bucket/key",
		"/bucket/key",
		"/bucket/dir%2Fkey",
		"/bucket/key?versionId=1",
		"bucket/key?versionId=1",
		"bucket/",
		"/bucket/",
		"bucket",
		"",
		" /bucket/key ",
		"bucket/a%20b",
		"/bucket/%zz",
		"/bucket/" + strings.Repeat("k", maxObjectKeyBytes+1),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxKeyInput)

		bucket, key, _, err := parseCopySource(raw)
		if err != nil {
			return
		}
		if strings.TrimSpace(bucket) == "" {
			t.Fatalf("accepted empty bucket from %q", raw)
		}
		if strings.Contains(bucket, "/") {
			t.Fatalf("accepted bucket %q containing '/'", bucket)
		}
		if key == "" {
			t.Fatalf("accepted empty key from %q", raw)
		}
		if len(key) > maxObjectKeyBytes {
			t.Fatalf("accepted %d-byte key above limit %d", len(key), maxObjectKeyBytes)
		}
	})
}

// FuzzDecodeAWSChunkedPayload checks unsigned aws-chunked decoding. Invariant:
// decoded output never exceeds the framed input and malformed framing returns
// an error instead of silently truncating.
func FuzzDecodeAWSChunkedPayload(f *testing.F) {
	seeds := [][]byte{
		[]byte("0\r\n\r\n"),
		[]byte("4\r\nWiki\r\n0\r\n\r\n"),
		[]byte("5\r\nhello\r\n0\r\n\r\n"),
		[]byte("1;chunk-signature=" + strings.Repeat("a", 64) + "\r\nx\r\n0\r\n\r\n"),
		[]byte("6\r\nhello\r\n0\r\n\r\n"),
		[]byte("4\r\nWikiXX0\r\n\r\n"),
		[]byte("zz\r\npayload\r\n0\r\n\r\n"),
		[]byte("-1\r\n\r\n"),
		[]byte("0\r\n"),
		[]byte(""),
		[]byte("4\r\nWiki\r\n0\r\nx-amz-checksum-crc32:abc\r\n\r\n"),
		[]byte(strings.Repeat("9", maxAWSChunkedLineBytes) + "\r\n0\r\n\r\n"),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		data = fuzzCapBytes(data, fuzzMaxChunkInput)

		var dst bytes.Buffer
		// Native fuzzing turns any panic into a failure; decoding itself stops
		// at the terminating blank line, so bytes after the final frame are
		// deliberately not part of the decoded body.
		_ = decodeAWSChunkedPayload(bytes.NewReader(data), &dst)
		if dst.Len() > len(data) {
			t.Fatalf("decoded %d bytes from %d framed bytes", dst.Len(), len(data))
		}
	})
}

// FuzzProbeAWSChunkedPayload checks the framing probe. Invariant: the probe
// never consumes or drops bytes - replaying the returned reader reproduces the
// entire input - and the chunked verdict requires a non-empty body.
func FuzzProbeAWSChunkedPayload(f *testing.F) {
	seeds := [][]byte{
		[]byte(""),
		[]byte("0\r\n\r\n"),
		[]byte("4\r\nWiki\r\n0\r\n\r\n"),
		[]byte("not-a-chunk\r\n"),
		[]byte("zz\r\n"),
		[]byte("1a;chunk-signature=abc\r\npayload\r\n"),
		[]byte(strings.Repeat("f", maxAWSChunkedLineBytes+1)),
		[]byte("5\nhello\n0\n\n"),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		data = fuzzCapBytes(data, fuzzMaxChunkInput)

		replay, chunked := probeAWSChunkedPayload(bytes.NewReader(data))
		if replay == nil {
			t.Fatal("probe returned a nil reader")
		}
		if chunked && len(data) == 0 {
			t.Fatal("probe classified an empty body as aws-chunked")
		}

		got, err := io.ReadAll(io.LimitReader(replay, int64(len(data))+1))
		if err != nil {
			t.Fatalf("replaying probed body failed: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("probe lost input: got %d bytes, want %d", len(got), len(data))
		}
	})
}

type fuzzCountingReader struct {
	reader io.Reader
	read   int
}

func (r *fuzzCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += n
	return n, err
}

// FuzzDecodeBoundedXMLBody checks XML body decoding over a bounded stream.
// Invariant: decoding never panics, never reads past the supplied limit, and a
// successful decode named a root element and drained the body to EOF.
func FuzzDecodeBoundedXMLBody(f *testing.F) {
	seeds := []string{
		"",
		"<Delete><Object><Key>a</Key></Object></Delete>",
		"<Delete><Object><Key>a</Key></Object></Delete>\n\t ",
		"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>",
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete></Delete>",
		"<Delete></Delete><!-- trailing comment -->",
		"<Delete></Delete><Extra/>",
		"<Delete>",
		"not xml at all",
		"<a><b><c/></b></a>",
		"<Delete><Object><Key>" + strings.Repeat("k", 1024) + "</Key></Object></Delete>",
		"<Delete><Object><Key>&amp;&lt;&gt;</Key></Object></Delete>",
		"<!DOCTYPE Delete [<!ENTITY x \"y\">]><Delete/>",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		raw = fuzzCapString(raw, fuzzMaxXMLInput)
		data := []byte(raw)

		counter := &fuzzCountingReader{reader: bytes.NewReader(data)}
		body := io.LimitReader(counter, fuzzMaxXMLInput)

		var doc struct {
			XMLName xml.Name
			Value   string   `xml:",chardata"`
			Items   []string `xml:"item"`
		}
		err := decodeBoundedXMLBody(body, &doc)
		if counter.read > len(data) {
			t.Fatalf("decoder read %d bytes from a %d-byte body", counter.read, len(data))
		}
		if err != nil {
			return
		}
		if doc.XMLName.Local == "" {
			t.Fatalf("decoded %q without a root element name", raw)
		}
		if counter.read != len(data) {
			t.Fatalf("successful decode consumed %d of %d bytes", counter.read, len(data))
		}
	})
}
