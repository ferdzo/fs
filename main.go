package main

import (
	"fmt"
	"fs/api"
	"fs/metadata"
	"fs/service"
)

func main() {

	metadataHandler, err := metadata.NewMetadataHandler("metadata.db")
	if err != nil {
		fmt.Printf("Error initializing metadata handler: %v\n", err)
		return
	}

	objectService := service.NewObjectService(metadataHandler)
	handler := api.NewHandler(objectService)
	err = handler.Start("localhost:3000")
	if err != nil {
		return
	}
}
