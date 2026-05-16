package storage

import (
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

func corruptChunk(t *testing.T, root, chunkID string, data []byte) {
	t.Helper()
	path := filepath.Join(root, blobRoot, chunkID[:2], chunkID[2:4], chunkID)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("corrupt chunk: %v", err)
	}
}
