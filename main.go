package main

import (
	"context"
	"fs/api"
	"fs/logging"
	"fs/metadata"
	"fs/service"
	"fs/storage"
	"fs/utils"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func main() {
	config := utils.NewConfig()
	logConfig := logging.ConfigFromValues(config.LogLevel, config.LogFormat, config.AuditLog)
	logger := logging.NewLogger(logConfig)
	logger.Info("boot",
		"log_level", logConfig.LevelName,
		"log_format", logConfig.Format,
		"audit_log", logConfig.Audit,
		"data_path", config.DataPath,
		"multipart_retention_hours", int(config.MultipartCleanupRetention/time.Hour),
	)

	if err := os.MkdirAll(config.DataPath, 0o755); err != nil {
		logger.Error("failed_to_prepare_data_path", "path", config.DataPath, "error", err)
		return
	}

	dbPath := filepath.Join(config.DataPath, "metadata.db")
	metadataHandler, err := metadata.NewMetadataHandler(dbPath)
	if err != nil {
		logger.Error("failed_to_initialize_metadata_handler", "error", err)
		return
	}
	blobHandler, err := storage.NewBlobStore(config.DataPath, config.ChunkSize)
	if err != nil {
		_ = metadataHandler.Close()
		logger.Error("failed_to_initialize_blob_store", "error", err)
		return
	}

	objectService := service.NewObjectService(metadataHandler, blobHandler, config.MultipartCleanupRetention)
	handler := api.NewHandler(objectService, logger, logConfig)
	addr := config.Address + ":" + strconv.Itoa(config.Port)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if config.GcEnabled {
		go objectService.RunGC(ctx, config.GcInterval)
	}

	if err = handler.Start(ctx, addr); err != nil {
		logger.Error("server_stopped_with_error", "error", err)
		return
	}

}
