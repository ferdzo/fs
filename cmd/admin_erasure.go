package cmd

import (
	"fmt"
	"fs/metadata"
	"fs/models"
	"fs/storage"
	"fs/utils"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

func newAdminErasureCommand(opts *adminOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "erasure",
		Short: "Offline Reed-Solomon erasure scrub, heal, and recovery utilities",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newAdminErasureStatusCommand(opts))
	cmd.AddCommand(newAdminErasureHealCommand(opts))
	cmd.AddCommand(newAdminErasureRecoverCommand(opts))
	return cmd
}

func newAdminErasureStatusCommand(opts *adminOptions) *cobra.Command {
	var pathsRaw string
	var dataShards int
	var parityShards int
	var metadataDB string

	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"scrub"},
		Short:   "Verify shard health across erasure roots",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveErasureStoreConfig(pathsRaw, dataShards, parityShards, false)
			if err != nil {
				return usageError("fs admin erasure status --paths <p1,p2,...> [--data-shards K] [--parity-shards M] [--metadata-db <path>]", err.Error())
			}
			store, err := storage.NewErasureStore(cfg)
			if err != nil {
				return err
			}

			chunkIDs, err := gatherTargetChunkIDs(store, metadataDB)
			if err != nil {
				return err
			}
			report, scrubErr := store.ScrubAndHeal(false, chunkIDs)
			if scrubErr != nil {
				return scrubErr
			}

			if opts.JSON {
				if err := writeJSON(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				if err := writeScrubReportText(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			}
			if report.UnrecoverableCount > 0 || report.MissingCount > 0 {
				return fmt.Errorf("erasure scrub detected %d unrecoverable and %d missing chunk(s)", report.UnrecoverableCount, report.MissingCount)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&pathsRaw, "paths", "", "Comma-separated erasure storage roots (fallback: FS_ERASURE_PATHS)")
	cmd.Flags().IntVar(&dataShards, "data-shards", 0, "Data shard count K (fallback: FS_ERASURE_DATA_SHARDS)")
	cmd.Flags().IntVar(&parityShards, "parity-shards", 0, "Parity shard count M (fallback: FS_ERASURE_PARITY_SHARDS)")
	cmd.Flags().StringVar(&metadataDB, "metadata-db", "", "Optional path to metadata.db to verify every referenced chunk")
	return cmd
}

func newAdminErasureHealCommand(opts *adminOptions) *cobra.Command {
	var pathsRaw string
	var dataShards int
	var parityShards int
	var chunkID string
	var metadataDB string

	cmd := &cobra.Command{
		Use:   "heal",
		Short: "Reconstruct and rewrite missing or corrupt shards",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := resolveErasureStoreConfig(pathsRaw, dataShards, parityShards, true)
			if err != nil {
				return usageError("fs admin erasure heal --paths <p1,p2,...> [--chunk <id> | --metadata-db <path>]", err.Error())
			}
			store, err := storage.NewErasureStore(cfg)
			if err != nil {
				return err
			}

			chunkID = strings.TrimSpace(chunkID)
			if chunkID != "" {
				res, err := store.HealChunk(chunkID)
				if opts.JSON {
					if jErr := writeJSON(cmd.OutOrStdout(), res); jErr != nil {
						return jErr
					}
				} else {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "chunk=%s before=%s after=%s repaired=%v failed=%v\n",
						res.ChunkID, res.StatusBefore, res.StatusAfter, res.RepairedShards, res.FailedShards)
				}
				return err
			}

			var targetChunks []string
			if strings.TrimSpace(metadataDB) != "" {
				targetChunks, err = gatherTargetChunkIDs(store, metadataDB)
				if err != nil {
					return err
				}
			}
			report, scrubErr := store.ScrubAndHeal(true, targetChunks)
			if scrubErr != nil {
				return scrubErr
			}
			if opts.JSON {
				if err := writeJSON(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			} else {
				if err := writeScrubReportText(cmd.OutOrStdout(), report); err != nil {
					return err
				}
			}
			if report.UnrecoverableCount > 0 || report.MissingCount > 0 {
				return fmt.Errorf("erasure heal completed with %d unrecoverable and %d missing chunk(s)", report.UnrecoverableCount, report.MissingCount)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&pathsRaw, "paths", "", "Comma-separated erasure storage roots (fallback: FS_ERASURE_PATHS)")
	cmd.Flags().IntVar(&dataShards, "data-shards", 0, "Data shard count K (fallback: FS_ERASURE_DATA_SHARDS)")
	cmd.Flags().IntVar(&parityShards, "parity-shards", 0, "Parity shard count M (fallback: FS_ERASURE_PARITY_SHARDS)")
	cmd.Flags().StringVar(&chunkID, "chunk", "", "Heal only a specific 64-hex chunk ID")
	cmd.Flags().StringVar(&metadataDB, "metadata-db", "", "Optional path to metadata.db to include every referenced chunk")
	return cmd
}

func newAdminErasureRecoverCommand(opts *adminOptions) *cobra.Command {
	var pathsRaw string
	var chunkID string
	var metadataDB string
	var bucket string
	var key string
	var versionID string
	var outPath string

	cmd := &cobra.Command{
		Use:   "recover",
		Short: "Recover verified object or chunk bytes from surviving shard directories",
		RunE: func(cmd *cobra.Command, args []string) error {
			dirs := splitCommaPaths(pathsRaw)
			if len(dirs) == 0 {
				dirs = utils.NewConfig().ErasurePaths
			}
			if len(dirs) == 0 {
				return usageError("fs admin erasure recover --paths <dir1,dir2,...> (--chunk <id> | --metadata-db <db> --bucket <b> --key <k>) --out <file>", "--paths is required")
			}

			chunkID = strings.TrimSpace(chunkID)
			metadataDB = strings.TrimSpace(metadataDB)
			bucket = strings.TrimSpace(bucket)
			key = strings.TrimSpace(key)
			versionID = strings.TrimSpace(versionID)
			outPath = strings.TrimSpace(outPath)
			if outPath == "" {
				return usageError("fs admin erasure recover --paths <dir1,dir2,...> (--chunk <id> | --metadata-db <db> --bucket <b> --key <k>) --out <file>", "--out is required (use - for stdout)")
			}
			if outPath == "-" && opts.JSON {
				return usageError("fs admin erasure recover ...", "--json cannot be combined with --out -")
			}

			if chunkID != "" {
				if metadataDB != "" || bucket != "" || key != "" {
					return usageError("fs admin erasure recover ...", "specify either --chunk OR (--metadata-db, --bucket, --key), not both")
				}
				data, err := storage.RecoverChunkFromDirs(dirs, chunkID)
				if err != nil {
					return err
				}
				if err := writeRecoveredOutput(cmd.OutOrStdout(), outPath, data); err != nil {
					return err
				}
				if opts.JSON {
					return writeJSON(cmd.OutOrStdout(), map[string]any{
						"chunkId": chunkID,
						"bytes":   len(data),
						"out":     outPath,
					})
				}
				if outPath != "-" {
					_, err = fmt.Fprintf(cmd.OutOrStdout(), "recovered chunk %s (%d bytes) -> %s\n", chunkID, len(data), outPath)
					return err
				}
				return nil
			}

			if metadataDB == "" || bucket == "" || key == "" {
				return usageError("fs admin erasure recover --paths <dir1,dir2,...> --metadata-db <db> --bucket <b> --key <k> --out <file>",
					"require either --chunk or all of --metadata-db, --bucket, and --key")
			}

			mh, err := metadata.NewReadOnlyMetadataHandler(metadataDB)
			if err != nil {
				return fmt.Errorf("open read-only metadata db: %w", err)
			}
			defer mh.Close()

			var manifest *models.ObjectManifest
			if versionID != "" {
				manifest, err = mh.GetManifestVersion(bucket, key, versionID)
			} else {
				manifest, err = mh.GetManifest(bucket, key)
			}
			if err != nil {
				return err
			}

			written, err := recoverObjectToTarget(dirs, manifest, cmd.OutOrStdout(), outPath)
			if err != nil {
				return err
			}
			if opts.JSON {
				return writeJSON(cmd.OutOrStdout(), map[string]any{
					"bucket":    bucket,
					"key":       key,
					"versionId": manifest.VersionID,
					"chunks":    len(manifest.Chunks),
					"bytes":     written,
					"out":       outPath,
				})
			}
			if outPath != "-" {
				_, err = fmt.Fprintf(cmd.OutOrStdout(), "recovered s3://%s/%s (%d chunks, %d bytes) -> %s\n",
					bucket, key, len(manifest.Chunks), written, outPath)
				return err
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&pathsRaw, "paths", "", "Comma-separated surviving shard roots or flat rescue directory (any order)")
	cmd.Flags().StringVar(&chunkID, "chunk", "", "Recover a single 64-hex chunk ID")
	cmd.Flags().StringVar(&metadataDB, "metadata-db", "", "Path to metadata.db (opened read-only)")
	cmd.Flags().StringVar(&bucket, "bucket", "", "Bucket name when recovering an object via --metadata-db")
	cmd.Flags().StringVar(&key, "key", "", "Object key when recovering an object via --metadata-db")
	cmd.Flags().StringVar(&versionID, "version-id", "", "Optional object version ID")
	cmd.Flags().StringVar(&outPath, "out", "", "Output file path (or - for stdout)")
	return cmd
}

func resolveErasureStoreConfig(pathsRaw string, dataShards, parityShards int, autoHeal bool) (storage.ErasureConfig, error) {
	base := utils.NewConfig()
	paths := splitCommaPaths(pathsRaw)
	if len(paths) == 0 {
		paths = base.ErasurePaths
	}
	if len(paths) == 0 {
		return storage.ErasureConfig{}, fmt.Errorf("--paths (or FS_ERASURE_PATHS) is required")
	}
	if dataShards <= 0 {
		dataShards = base.ErasureDataShards
	}
	if parityShards <= 0 {
		parityShards = base.ErasureParityShards
	}
	return storage.ErasureConfig{
		Paths:        paths,
		DataShards:   dataShards,
		ParityShards: parityShards,
		ChunkSize:    base.ChunkSize,
		AutoHeal:     autoHeal,
		StrictWrite:  false,
	}, nil
}

func splitCommaPaths(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			out = append(out, filepath.Clean(trimmed))
		}
	}
	return out
}

func gatherTargetChunkIDs(store *storage.ErasureStore, metadataDB string) ([]string, error) {
	diskChunks, err := store.ListChunks()
	if err != nil {
		return nil, err
	}
	metadataDB = strings.TrimSpace(metadataDB)
	if metadataDB == "" {
		return diskChunks, nil
	}

	mh, err := metadata.NewReadOnlyMetadataHandler(metadataDB)
	if err != nil {
		return nil, fmt.Errorf("open read-only metadata db: %w", err)
	}
	defer mh.Close()

	refChunks, err := mh.GetReferencedChunks()
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(diskChunks)+len(refChunks))
	for _, id := range diskChunks {
		seen[id] = struct{}{}
	}
	for _, id := range refChunks {
		if id != "" {
			seen[id] = struct{}{}
		}
	}
	merged := make([]string, 0, len(seen))
	for id := range seen {
		merged = append(merged, id)
	}
	sort.Strings(merged)
	return merged, nil
}

func writeRecoveredOutput(stdout io.Writer, outPath string, data []byte) error {
	if outPath == "-" {
		_, err := stdout.Write(data)
		return err
	}
	return writeFileAtomicPath(outPath, data)
}

func recoverObjectToTarget(dirs []string, manifest *models.ObjectManifest, stdout io.Writer, outPath string) (int64, error) {
	if manifest == nil {
		return 0, fmt.Errorf("nil object manifest")
	}

	var writer io.Writer
	var tmpFile *os.File
	var tmpPath string
	cleanup := false

	if outPath == "-" {
		writer = stdout
	} else {
		dir := filepath.Dir(outPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, err
		}
		var err error
		tmpFile, err = os.CreateTemp(dir, ".fs-recover-*.tmp")
		if err != nil {
			return 0, err
		}
		tmpPath = tmpFile.Name()
		cleanup = true
		defer func() {
			if cleanup {
				_ = os.Remove(tmpPath)
			}
		}()
		writer = tmpFile
	}

	var total int64
	for _, cid := range manifest.Chunks {
		data, err := storage.RecoverChunkFromDirs(dirs, cid)
		if err != nil {
			if tmpFile != nil {
				_ = tmpFile.Close()
			}
			return total, fmt.Errorf("chunk %s: %w", cid, err)
		}
		n, err := writer.Write(data)
		total += int64(n)
		if err != nil {
			if tmpFile != nil {
				_ = tmpFile.Close()
			}
			return total, err
		}
	}

	if total != manifest.Size {
		if tmpFile != nil {
			_ = tmpFile.Close()
		}
		return total, fmt.Errorf("recovered size mismatch: got %d bytes, manifest expects %d", total, manifest.Size)
	}

	if tmpFile != nil {
		if err := tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			return total, err
		}
		if err := tmpFile.Close(); err != nil {
			return total, err
		}
		if err := os.Rename(tmpPath, outPath); err != nil {
			return total, err
		}
		cleanup = false
	}
	return total, nil
}

func writeFileAtomicPath(outPath string, data []byte) error {
	dir := filepath.Dir(outPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".fs-recover-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func writeScrubReportText(out io.Writer, report *storage.ScrubReport) error {
	if _, err := fmt.Fprintf(out,
		"erasure (%d+%d): checked=%d healthy=%d degraded=%d healed=%d unrecoverable=%d missing=%d\n",
		report.DataShards, report.ParityShards,
		report.CheckedChunks, report.HealthyChunks, report.DegradedChunks,
		report.HealedChunks, report.UnrecoverableCount, report.MissingCount,
	); err != nil {
		return err
	}
	for _, issue := range report.Issues {
		if _, err := fmt.Fprintf(out, "  chunk=%s status=%s missing=%v corrupt=%v error=%v\n",
			issue.ChunkID, issue.Status, issue.MissingShards, issue.CorruptShards, issue.ErrorShards); err != nil {
			return err
		}
	}
	return nil
}
