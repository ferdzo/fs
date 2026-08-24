package app

import (
	"context"
	"fs/api"
	"fs/auth"
	"fs/logging"
	"fs/metadata"
	"fs/service"
	"fs/storage"
	"fs/utils"
	"os"
	"path/filepath"
	"strconv"
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
	)
	logger := logging.NewLogger(logConfig)
	logger.Info("boot",
		"log_level", logConfig.LevelName,
		"log_format", logConfig.Format,
		"audit_log", logConfig.Audit,
		"data_path", config.DataPath,
		"multipart_retention_hours", int(config.MultipartCleanupRetention/time.Hour),
		"max_object_upload_bytes", config.MaxObjectUploadBytes,
		"auth_enabled", authConfig.Enabled,
		"auth_region", authConfig.Region,
		"admin_api_enabled", config.AdminAPIEnabled,
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

	blobHandler, err := storage.NewBlobStore(config.DataPath, config.ChunkSize)
	if err != nil {
		_ = metadataHandler.Close()
		logger.Error("failed_to_initialize_blob_store", "error", err)
		return err
	}

	objectService := service.NewObjectService(metadataHandler, blobHandler, config.MultipartCleanupRetention, config.MaxObjectUploadBytes)
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
