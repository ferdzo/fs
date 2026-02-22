package main

import (
	"fs/api"
	"fs/logging"
	"fs/metadata"
	"fs/service"
)

func main() {
	logConfig := logging.ConfigFromEnv()
	logger := logging.NewLogger(logConfig)
	logger.Info("boot",
		"log_level", logConfig.LevelName,
		"log_format", logConfig.Format,
		"audit_log", logConfig.Audit,
	)

	metadataHandler, err := metadata.NewMetadataHandler("metadata.db")
	if err != nil {
		logger.Error("failed_to_initialize_metadata_handler", "error", err)
		return
	}

	objectService := service.NewObjectService(metadataHandler)
	handler := api.NewHandler(objectService, logger, logConfig)
	if err = handler.Start("0.0.0.0:3000"); err != nil {
		logger.Error("server_stopped_with_error", "error", err)
		return
	}
}
