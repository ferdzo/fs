package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetBlobDetectsCorruptedChunk(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}

	chunks, _, _, err := bs.IngestStream(strings.NewReader("good"))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	chunkID := chunks[0]
	corruptChunk(t, root, chunkID, []byte("bad"))

	got, err := bs.GetBlob(chunkID)
	if !errors.Is(err, ErrChunkIntegrity) {
		t.Fatalf("GetBlob error = %v, want ErrChunkIntegrity", err)
	}
	if got != nil {
		t.Fatalf("GetBlob returned data for corrupted chunk: %q", got)
	}
}

func TestAssembleStreamDetectsCorruptedChunk(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}

	chunks, _, _, err := bs.IngestStream(strings.NewReader("abcdefgh"))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}
	corruptChunk(t, root, chunks[1], []byte("corrupt"))

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		err := bs.AssembleStream(chunks, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()

	_, readErr := io.ReadAll(pr)
	assembleErr := <-errCh
	if !errors.Is(assembleErr, ErrChunkIntegrity) {
		t.Fatalf("AssembleStream error = %v, want ErrChunkIntegrity", assembleErr)
	}
	if !errors.Is(readErr, ErrChunkIntegrity) {
		t.Fatalf("pipe read error = %v, want ErrChunkIntegrity", readErr)
	}
}

func TestIngestStreamWithPinCallsBeforeSave(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}

	var seen []string
	if _, _, _, err := bs.IngestStreamWithPin(strings.NewReader("abcd"), func(chunkID string) error {
		if _, ok := bs.StatChunk(chunkID); ok {
			t.Fatalf("chunk %s was saved before the pin callback", chunkID)
		}
		seen = append(seen, chunkID)
		return nil
	}); err != nil {
		t.Fatalf("IngestStreamWithPin: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("callback calls = %d, want 1", len(seen))
	}
	if _, ok := bs.StatChunk(seen[0]); !ok {
		t.Fatalf("chunk %s was not saved after the callback", seen[0])
	}
}

func TestIngestStreamWithPinCallsBeforeDedupStat(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}
	first, _, _, err := bs.IngestStream(strings.NewReader("abcd"))
	if err != nil {
		t.Fatalf("initial ingest: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("initial chunk count = %d, want 1", len(first))
	}

	calls := 0
	second, _, _, err := bs.IngestStreamWithPin(strings.NewReader("abcd"), func(chunkID string) error {
		calls++
		if chunkID != first[0] {
			t.Fatalf("callback chunk = %s, want %s", chunkID, first[0])
		}
		if _, ok := bs.StatChunk(chunkID); !ok {
			t.Fatalf("dedup callback should run even when chunk already exists")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("duplicate IngestStreamWithPin: %v", err)
	}
	if calls != 1 {
		t.Fatalf("callback calls on duplicate ingest = %d, want 1", calls)
	}
	if len(second) != 1 || second[0] != first[0] {
		t.Fatalf("duplicate chunks = %v, want [%s]", second, first[0])
	}
}

func TestIngestStreamWithPinErrorAbortsSave(t *testing.T) {
	root := t.TempDir()
	bs, err := NewBlobStore(root, 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}

	var attempted string
	_, _, _, err = bs.IngestStreamWithPin(strings.NewReader("abcd"), func(chunkID string) error {
		attempted = chunkID
		return errors.New("pin failed")
	})
	if err == nil {
		t.Fatal("IngestStreamWithPin succeeded despite callback error")
	}
	if attempted == "" {
		t.Fatal("callback was not invoked")
	}
	if _, ok := bs.StatChunk(attempted); ok {
		t.Fatalf("chunk %s was saved after callback failure", attempted)
	}
}

func TestIngestStreamWithSizesReportsChunkLengths(t *testing.T) {
	bs, err := NewBlobStore(t.TempDir(), 4)
	if err != nil {
		t.Fatalf("new blob store: %v", err)
	}

	chunks, sizes, total, _, err := bs.IngestStreamWithSizes(strings.NewReader("abcdefghij"), nil)
	if err != nil {
		t.Fatalf("IngestStreamWithSizes: %v", err)
	}
	if total != 10 {
		t.Fatalf("total = %d, want 10", total)
	}
	if len(chunks) != len(sizes) {
		t.Fatalf("chunk IDs = %d, sizes = %d; want parallel", len(chunks), len(sizes))
	}
	want := []int64{4, 4, 2}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("sizes = %v, want %v", sizes, want)
		}
	}
}

func TestAssembleStreamReadsChunksLargerThanConfiguredChunkSize(t *testing.T) {
	root := t.TempDir()
	large, err := NewBlobStore(root, 64<<10)
	if err != nil {
		t.Fatalf("new large blob store: %v", err)
	}

	payload := make([]byte, 200<<10)
	for i := range payload {
		payload[i] = byte(i*17 + i/251)
	}
	chunks, sizes, total, _, err := large.IngestStreamWithSizes(bytes.NewReader(payload), nil)
	if err != nil {
		t.Fatalf("ingest with large chunk size: %v", err)
	}
	if total != int64(len(payload)) {
		t.Fatalf("total = %d, want %d", total, len(payload))
	}
	if len(sizes) != 4 || sizes[0] != 64<<10 || sizes[3] != 8<<10 {
		t.Fatalf("chunk sizes = %v, want four 64 KiB chunks with an 8 KiB tail", sizes)
	}

	// Re-open the same data root with a much smaller configured chunk size,
	// simulating a deployment where CHUNK_SIZE was reduced.
	small, err := NewBlobStore(root, 4<<10)
	if err != nil {
		t.Fatalf("new small blob store: %v", err)
	}

	pr, pw := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		err := small.AssembleStream(chunks, pw)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
		errCh <- err
	}()

	got, readErr := io.ReadAll(pr)
	assembleErr := <-errCh
	if readErr != nil {
		t.Fatalf("read assembled stream: %v", readErr)
	}
	if assembleErr != nil {
		t.Fatalf("AssembleStream: %v", assembleErr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("assembled %d bytes, want %d; content mismatch", len(got), len(payload))
	}
}

func corruptChunk(t *testing.T, root, chunkID string, data []byte) {
	t.Helper()
	path := filepath.Join(root, blobRoot, chunkID[:2], chunkID[2:4], chunkID)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("corrupt chunk: %v", err)
	}
}
