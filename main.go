package main

import (
	"fmt"
	"fs/metadata"
	"fs/service"
	"io"
	"os"
)

func main() {
	fmt.Println("Hello, World!")
	imageStream, err := os.Open("fer.jpg")
	if err != nil {
		fmt.Printf("Error opening image stream: %v\n", err)
		return
	}
	defer imageStream.Close()

	metadataHandler, err := metadata.NewMetadataHandler("metadata.db")
	if err != nil {
		fmt.Printf("Error initializing metadata handler: %v\n", err)
		return
	}

	objectService := service.NewObjectService(metadataHandler)

	manifest, err := objectService.PutObject("test-bucket-ferdzo/fer.jpg", "image/jpeg", imageStream)
	if err != nil {
		fmt.Printf("Error ingesting stream: %v\n", err)
		return
	}
	fmt.Printf("Manifest: %+v\n", manifest)

	objectData, manifest2, err := objectService.GetObject("test-bucket-ferdzo", "fer.jpg")
	if err != nil {
		fmt.Printf("Error retrieving object: %v\n", err)
		return
	}
	fmt.Printf("Retrieved manifest: %+v\n", manifest2)
	recoveredFile, err := os.Create("recovered_" + manifest2.Key)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer recoveredFile.Close()

	bytesWritten, err := io.Copy(recoveredFile, objectData)
	if err != nil {
		fmt.Printf("Error streaming to recovered file: %v\n", err)
		return
	}
	fmt.Printf("Successfully streamed %d bytes to disk!\n", bytesWritten)
}
