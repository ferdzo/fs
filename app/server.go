package app

import (
	"context"
	"fmt"
	"fs/api"
	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/service"
	"fs/storage"
	"fs/utils"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func RunServer(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	config := utils.NewConfig()
	logConfig := logging.ConfigFromValues(config.LogLevel, config.LogFormat, config.AuditLog)
	authConfig := auth.ConfigFromValues(
		config.AuthEnabled,
		config.AuthRegion,
		config.AuthSkew,
		config.AuthMaxPresign,
		config.AuthMasterKey,
		config.AuthBootstrapAccessKey,
		config.AuthBootstrapSecretKey,
		config.AuthBootstrapPolicy,
		config.AuthFailureLimitPerMin,
	)
	logger := logging.NewLogger(logConfig)
	logger.Info("boot",
		"log_level", logConfig.LevelName,
		"log_format", logConfig.Format,
		"audit_log", logConfig.Audit,
		"data_path", config.DataPath,
		"gc_interval_seconds", int(config.GcInterval/time.Second),
		"gc_grace_seconds", int(config.GcGrace/time.Second),
		"multipart_retention_hours", int(config.MultipartCleanupRetention/time.Hour),
		"max_object_upload_bytes", config.MaxObjectUploadBytes,
		"auth_enabled", authConfig.Enabled,
		"auth_region", authConfig.Region,
		"admin_api_enabled", config.AdminAPIEnabled,
		"metrics_address", config.MetricsAddress,
		"metrics_port", config.MetricsPort,
		"storage_backend", config.StorageBackend,
	)

	if err := os.MkdirAll(config.DataPath, 0o755); err != nil {
		logger.Error("failed_to_prepare_data_path", "path", config.DataPath, "error", err)
		return err
	}

	dbPath := filepath.Join(config.DataPath, "metadata.db")
	metadataHandler, err := metadata.NewMetadataHandler(dbPath)
	if err != nil {
		logger.Error("failed_to_initialize_metadata_handler", "error", err)
		return err
	}

	var blobHandler service.ChunkStore
	switch strings.ToLower(strings.TrimSpace(config.StorageBackend)) {
	case "", "disk":
		blobHandler, err = storage.NewBlobStore(config.DataPath, config.ChunkSize)
	case "erasure":
		logger.Info("erasure_storage_config",
			"data_shards", config.ErasureDataShards,
			"parity_shards", config.ErasureParityShards,
			"paths_count", len(config.ErasurePaths),
			"auto_heal", config.ErasureAutoHeal,
			"strict_write", config.ErasureStrictWrite,
		)
		blobHandler, err = storage.NewErasureStore(storage.ErasureConfig{
			Paths:        config.ErasurePaths,
			DataShards:   config.ErasureDataShards,
			ParityShards: config.ErasureParityShards,
			ChunkSize:    config.ChunkSize,
			AutoHeal:     config.ErasureAutoHeal,
			StrictWrite:  config.ErasureStrictWrite,
		})
	default:
		err = fmt.Errorf("unsupported FS_STORAGE_BACKEND %q (expected disk or erasure)", config.StorageBackend)
	}
	if err != nil {
		_ = metadataHandler.Close()
		logger.Error("failed_to_initialize_blob_store", "backend", config.StorageBackend, "error", err)
		return err
	}

	objectService := service.NewObjectService(metadataHandler, blobHandler, config.MultipartCleanupRetention, config.MaxObjectUploadBytes)
	objectService.SetGCGrace(config.GcGrace)
	authService, err := auth.NewService(authConfig, metadataHandler)
	if err != nil {
		_ = metadataHandler.Close()
		logger.Error("failed_to_initialize_auth_service", "error", err)
		return err
	}
	if err := authService.EnsureBootstrap(); err != nil {
		_ = metadataHandler.Close()
		logger.Error("failed_to_ensure_bootstrap_auth_identity", "error", err)
		return err
	}

	handler := api.NewHandler(objectService, logger, logConfig, authService, config.AdminAPIEnabled,
		time.Duration(config.BodyReadTimeoutSeconds)*time.Second)
	var metricsSrv *api.MetricsServer
	if config.MetricsPort > 0 {
		metricsAddr := net.JoinHostPort(config.MetricsAddress, strconv.Itoa(config.MetricsPort))
		metricsSrv, err = api.StartMetricsServer(ctx, metricsAddr, logger)
		if err != nil {
			_ = metadataHandler.Close()
			logger.Error("failed_to_start_metrics_server", "address", metricsAddr, "error", err)
			return err
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = metricsSrv.Shutdown(shutdownCtx)
		}()
	}
	addr := config.Address + ":" + strconv.Itoa(config.Port)
	if config.GcEnabled {
		go objectService.RunGC(ctx, config.GcInterval)
	}

	if err := handler.Start(ctx, addr); err != nil {
		logger.Error("server_stopped_with_error", "error", err)
		return err
	}
	return nil
}
