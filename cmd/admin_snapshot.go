package cmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/spf13/cobra"
)

const (
	snapshotManifestPath = ".fs-snapshot/manifest.json"
	snapshotFormat       = 1
)

type snapshotFileEntry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type snapshotManifest struct {
	FormatVersion int                 `json:"formatVersion"`
	CreatedAt     string              `json:"createdAt"`
	SourcePath    string              `json:"sourcePath"`
	Files         []snapshotFileEntry `json:"files"`
}

type snapshotSummary struct {
	SnapshotFile string `json:"snapshotFile"`
	CreatedAt    string `json:"createdAt"`
	SourcePath   string `json:"sourcePath"`
	FileCount    int    `json:"fileCount"`
	TotalBytes   int64  `json:"totalBytes"`
}

func newAdminSnapshotCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Offline snapshot and restore utilities",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAdminSnapshotCreateCommand(opts))
	cmd.AddCommand(newAdminSnapshotInspectCommand(opts))
	cmd.AddCommand(newAdminSnapshotRestoreCommand(opts))
	return cmd
}

func newAdminSnapshotCreateCommand(opts *adminOptions) *cobra.Command {
	var dataPath string
	var outFile string
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create offline snapshot tarball (.tar.gz)",
		RunE: func(cmd *cobra.Command, args []string) error {
			dataPath = strings.TrimSpace(dataPath)
			outFile = strings.TrimSpace(outFile)
			if dataPath == "" {
				return usageError("fs admin snapshot create --data-path <path> --out <snapshot.tar.gz>", "--data-path is required")
			}
			if outFile == "" {
				return usageError("fs admin snapshot create --data-path <path> --out <snapshot.tar.gz>", "--out is required")
			}

			result, err := createSnapshotArchive(context.Background(), dataPath, outFile)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "snapshot created: %s (files=%d bytes=%d)\n", result.SnapshotFile, result.FileCount, result.TotalBytes)
			return err
		},
	}
	cmd.Flags().StringVar(&dataPath, "data-path", "", "Source data path (must contain metadata.db)")
	cmd.Flags().StringVar(&outFile, "out", "", "Output snapshot file path (.tar.gz)")
	return cmd
}

func newAdminSnapshotInspectCommand(opts *adminOptions) *cobra.Command {
	var filePath string
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect and verify snapshot archive integrity",
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath = strings.TrimSpace(filePath)
			if filePath == "" {
				return usageError("fs admin snapshot inspect --file <snapshot.tar.gz>", "--file is required")
			}

			manifest, summary, err := inspectSnapshotArchive(filePath)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), map[string]any{
					"summary":  summary,
					"manifest": manifest,
				})
			}
			_, err = fmt.Fprintf(
				cmd.OutOrStdout(),
				"snapshot ok: %s\ncreated_at=%s source=%s files=%d bytes=%d\n",
				summary.SnapshotFile,
				summary.CreatedAt,
				summary.SourcePath,
				summary.FileCount,
				summary.TotalBytes,
			)
			return err
		},
	}
	cmd.Flags().StringVar(&filePath, "file", "", "Snapshot file path (.tar.gz)")
	return cmd
}

func newAdminSnapshotRestoreCommand(opts *adminOptions) *cobra.Command {
	var filePath string
	var dataPath string
	var force bool
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore snapshot into a data path (offline only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath = strings.TrimSpace(filePath)
			dataPath = strings.TrimSpace(dataPath)
			if filePath == "" {
				return usageError("fs admin snapshot restore --file <snapshot.tar.gz> --data-path <path> [--force]", "--file is required")
			}
			if dataPath == "" {
				return usageError("fs admin snapshot restore --file <snapshot.tar.gz> --data-path <path> [--force]", "--data-path is required")
			}

			result, err := restoreSnapshotArchive(context.Background(), filePath, dataPath, force)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), result)
			}
			_, err = fmt.Fprintf(
				cmd.OutOrStdout(),
				"snapshot restored to %s (files=%d bytes=%d)\n",
				result.SourcePath,
				result.FileCount,
				result.TotalBytes,
			)
			return err
		},
	}
	cmd.Flags().StringVar(&filePath, "file", "", "Snapshot file path (.tar.gz)")
	cmd.Flags().StringVar(&dataPath, "data-path", "", "Destination data path")
	cmd.Flags().BoolVar(&force, "force", false, "Overwrite destination data path if it exists")
	return cmd
}

func createSnapshotArchive(ctx context.Context, dataPath, outFile string) (*snapshotSummary, error) {
	_ = ctx
	sourceAbs, err := filepath.Abs(filepath.Clean(dataPath))
	if err != nil {
		return nil, err
	}
	outAbs, err := filepath.Abs(filepath.Clean(outFile))
	if err != nil {
		return nil, err
	}
	if isPathWithin(sourceAbs, outAbs) {
		return nil, errors.New("output file cannot be inside --data-path")
	}

	info, err := os.Stat(sourceAbs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("data path %q is not a directory", sourceAbs)
	}
	if err := ensureMetadataExists(sourceAbs); err != nil {
		return nil, err
	}
	if err := ensureDataPathOffline(sourceAbs); err != nil {
		return nil, err
	}

	manifest, totalBytes, err := buildSnapshotManifest(sourceAbs)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(outAbs), 0o755); err != nil {
		return nil, err
	}
	tmpPath := outAbs + ".tmp-" + strconvNowNano()
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	gzw := gzip.NewWriter(file)
	tw := tar.NewWriter(gzw)
	if err := writeManifestToTar(tw, manifest); err != nil {
		_ = tw.Close()
		_ = gzw.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}

	for _, entry := range manifest.Files {
		absPath := filepath.Join(sourceAbs, filepath.FromSlash(entry.Path))
		if err := writeFileToTar(tw, absPath, entry.Path); err != nil {
			_ = tw.Close()
			_ = gzw.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}
	}

	if err := tw.Close(); err != nil {
		_ = gzw.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := gzw.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := os.Rename(tmpPath, outAbs); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := syncDir(filepath.Dir(outAbs)); err != nil {
		return nil, err
	}

	return &snapshotSummary{
		SnapshotFile: outAbs,
		CreatedAt:    manifest.CreatedAt,
		SourcePath:   sourceAbs,
		FileCount:    len(manifest.Files),
		TotalBytes:   totalBytes,
	}, nil
}

func inspectSnapshotArchive(filePath string) (*snapshotManifest, *snapshotSummary, error) {
	fileAbs, err := filepath.Abs(filepath.Clean(filePath))
	if err != nil {
		return nil, nil, err
	}
	manifest, actual, err := readSnapshotArchive(fileAbs)
	if err != nil {
		return nil, nil, err
	}
	expected := map[string]snapshotFileEntry{}
	var totalBytes int64
	for _, entry := range manifest.Files {
		expected[entry.Path] = entry
		totalBytes += entry.Size
	}
	if len(expected) != len(actual) {
		return nil, nil, fmt.Errorf("snapshot validation failed: expected %d files, got %d", len(expected), len(actual))
	}
	for path, exp := range expected {
		got, ok := actual[path]
		if !ok {
			return nil, nil, fmt.Errorf("snapshot validation failed: missing file %s", path)
		}
		if got.Size != exp.Size || got.SHA256 != exp.SHA256 {
			return nil, nil, fmt.Errorf("snapshot validation failed: checksum mismatch for %s", path)
		}
	}

	return manifest, &snapshotSummary{
		SnapshotFile: fileAbs,
		CreatedAt:    manifest.CreatedAt,
		SourcePath:   manifest.SourcePath,
		FileCount:    len(manifest.Files),
		TotalBytes:   totalBytes,
	}, nil
}

func restoreSnapshotArchive(ctx context.Context, filePath, destinationPath string, force bool) (*snapshotSummary, error) {
	_ = ctx
	manifest, summary, err := inspectSnapshotArchive(filePath)
	if err != nil {
		return nil, err
	}

	destAbs, err := filepath.Abs(filepath.Clean(destinationPath))
	if err != nil {
		return nil, err
	}

	if fi, statErr := os.Stat(destAbs); statErr == nil && fi.IsDir() {
		if err := ensureDataPathOffline(destAbs); err != nil {
			return nil, err
		}
		entries, err := os.ReadDir(destAbs)
		if err == nil && len(entries) > 0 && !force {
			return nil, errors.New("destination data path is not empty; use --force to overwrite")
		}
	}

	parent := filepath.Dir(destAbs)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return nil, err
	}
	stage := filepath.Join(parent, "."+filepath.Base(destAbs)+".restore-"+strconvNowNano())
	if err := os.MkdirAll(stage, 0o755); err != nil {
		return nil, err
	}
	cleanupStage := true
	defer func() {
		if cleanupStage {
			_ = os.RemoveAll(stage)
		}
	}()

	if err := extractSnapshotArchive(filePath, stage, manifest); err != nil {
		return nil, err
	}
	if err := syncDir(stage); err != nil {
		return nil, err
	}

	if _, err := os.Stat(destAbs); err == nil {
		if !force {
			return nil, errors.New("destination data path exists; use --force to overwrite")
		}
		if err := os.RemoveAll(destAbs); err != nil {
			return nil, err
		}
	}

	if err := os.Rename(stage, destAbs); err != nil {
		return nil, err
	}
	if err := syncDir(parent); err != nil {
		return nil, err
	}
	cleanupStage = false

	summary.SourcePath = destAbs
	return summary, nil
}

func ensureMetadataExists(dataPath string) error {
	dbPath := filepath.Join(dataPath, "metadata.db")
	info, err := os.Stat(dbPath)
	if err != nil {
		return fmt.Errorf("metadata.db not found in %s", dataPath)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("metadata.db in %s is not a regular file", dataPath)
	}
	return nil
}

func ensureDataPathOffline(dataPath string) error {
	dbPath := filepath.Join(dataPath, "metadata.db")
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 100 * time.Millisecond})
	if err != nil {
		return fmt.Errorf("data path appears in use (metadata.db locked): %w", err)
	}
	return db.Close()
}

func buildSnapshotManifest(dataPath string) (*snapshotManifest, int64, error) {
	entries := make([]snapshotFileEntry, 0, 128)
	var totalBytes int64
	err := filepath.WalkDir(dataPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		rel, err := filepath.Rel(dataPath, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(filepath.Clean(rel))
		if rel == "." || rel == "" {
			return nil
		}

		sum, err := sha256File(path)
		if err != nil {
			return err
		}
		totalBytes += info.Size()
		entries = append(entries, snapshotFileEntry{
			Path:   rel,
			Size:   info.Size(),
			SHA256: sum,
		})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	if len(entries) == 0 {
		return nil, 0, errors.New("data path contains no regular files to snapshot")
	}

	return &snapshotManifest{
		FormatVersion: snapshotFormat,
		CreatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		SourcePath:    dataPath,
		Files:         entries,
	}, totalBytes, nil
}

func writeManifestToTar(tw *tar.Writer, manifest *snapshotManifest) error {
	if tw == nil || manifest == nil {
		return errors.New("invalid manifest writer input")
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	header := &tar.Header{
		Name:    snapshotManifestPath,
		Mode:    0o600,
		Size:    int64(len(payload)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err = tw.Write(payload)
	return err
}

func writeFileToTar(tw *tar.Writer, absPath, relPath string) error {
	file, err := os.Open(absPath)
	if err != nil {
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}
	header := &tar.Header{
		Name:    relPath,
		Mode:    int64(info.Mode().Perm()),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err = io.Copy(tw, file)
	return err
}

func readSnapshotArchive(filePath string) (*snapshotManifest, map[string]snapshotFileEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return nil, nil, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	actual := make(map[string]snapshotFileEntry)
	var manifest *snapshotManifest

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		name, err := cleanArchivePath(header.Name)
		if err != nil {
			return nil, nil, err
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return nil, nil, fmt.Errorf("unsupported tar entry type for %s", name)
		}

		if name == snapshotManifestPath {
			raw, err := io.ReadAll(tr)
			if err != nil {
				return nil, nil, err
			}
			current := &snapshotManifest{}
			if err := json.Unmarshal(raw, current); err != nil {
				return nil, nil, err
			}
			manifest = current
			continue
		}

		size, hashHex, err := digestReader(tr)
		if err != nil {
			return nil, nil, err
		}
		actual[name] = snapshotFileEntry{
			Path:   name,
			Size:   size,
			SHA256: hashHex,
		}
	}

	if manifest == nil {
		return nil, nil, errors.New("snapshot manifest.json not found")
	}
	if manifest.FormatVersion != snapshotFormat {
		return nil, nil, fmt.Errorf("unsupported snapshot format version %d", manifest.FormatVersion)
	}
	return manifest, actual, nil
}

func extractSnapshotArchive(filePath, destination string, manifest *snapshotManifest) error {
	expected := make(map[string]snapshotFileEntry, len(manifest.Files))
	for _, entry := range manifest.Files {
		expected[entry.Path] = entry
	}
	seen := make(map[string]struct{}, len(expected))

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		name, err := cleanArchivePath(header.Name)
		if err != nil {
			return err
		}
		if name == snapshotManifestPath {
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return err
			}
			continue
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
			return fmt.Errorf("unsupported tar entry type for %s", name)
		}

		exp, ok := expected[name]
		if !ok {
			return fmt.Errorf("snapshot contains unexpected file %s", name)
		}
		targetPath := filepath.Join(destination, filepath.FromSlash(name))
		if !isPathWithin(destination, targetPath) {
			return fmt.Errorf("invalid archive path %s", name)
		}

		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(header.Mode)&0o777)
		if err != nil {
			return err
		}

		hasher := sha256.New()
		written, copyErr := io.Copy(io.MultiWriter(out, hasher), tr)
		syncErr := out.Sync()
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if syncErr != nil {
			return syncErr
		}
		if closeErr != nil {
			return closeErr
		}
		if err := syncDir(filepath.Dir(targetPath)); err != nil {
			return err
		}

		sum := hex.EncodeToString(hasher.Sum(nil))
		if written != exp.Size || sum != exp.SHA256 {
			return fmt.Errorf("checksum mismatch while extracting %s", name)
		}
		seen[name] = struct{}{}
	}

	if len(seen) != len(expected) {
		return fmt.Errorf("restore validation failed: extracted %d files, expected %d", len(seen), len(expected))
	}
	for path := range expected {
		if _, ok := seen[path]; !ok {
			return fmt.Errorf("restore validation failed: missing file %s", path)
		}
	}
	return nil
}

func cleanArchivePath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("empty archive path")
	}
	name = filepath.ToSlash(filepath.Clean(name))
	if strings.HasPrefix(name, "/") || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") || name == ".." {
		return "", fmt.Errorf("unsafe archive path %q", name)
	}
	return name, nil
}

func digestReader(r io.Reader) (int64, string, error) {
	hasher := sha256.New()
	n, err := io.Copy(hasher, r)
	if err != nil {
		return 0, "", err
	}
	return n, hex.EncodeToString(hasher.Sum(nil)), nil
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return sha256FromReader(file)
}

func sha256FromReader(reader io.Reader) (string, error) {
	hasher := sha256.New()
	if _, err := io.Copy(hasher, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func isPathWithin(base, candidate string) bool {
	base = filepath.Clean(base)
	candidate = filepath.Clean(candidate)
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func strconvNowNano() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
