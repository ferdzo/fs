package models

type ObjectManifest struct {
	Bucket      string   `json:"bucket"`
	Key         string   `json:"key"`
	Size        int64    `json:"size"`
	ContentType string   `json:"content_type"`
	ETag        string   `json:"etag"`
	Chunks      []string `json:"chunks"`
	CreatedAt   int64    `json:"created_at"`
}
