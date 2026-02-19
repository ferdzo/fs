package main

import (
	"fmt"
	"os"

	"fs/data"
)

func main() {
	fmt.Println("Hello, World!")
	imageStream, err := os.Open("fer.jpg")
	if err != nil {
		fmt.Printf("Error opening image stream: %v\n", err)
		return
	}
	defer imageStream.Close()

	fmt.Fprint(imageStream)

	manifest, err := data.IngestStream("test-bucket-ferdzo", "fer.jpg", "image/jpeg", imageStream)
	if err != nil {
		fmt.Printf("Error ingesting stream: %v\n", err)
		return
	}
	fmt.Printf("Manifest: %+v\n", manifest)

	objectData, err := data.GetObject(manifest)
	if err != nil {
		fmt.Printf("Error retrieving object: %v\n", err)
		return
	}
	fmt.Printf("Retrieved object data length: %d\n", len(objectData))

	err = os.WriteFile("recovered"+manifest.Key, objectData, 0644)
	if err != nil {
		fmt.Printf("Error writing recovered file: %v\n", err)
		return
	}
}
