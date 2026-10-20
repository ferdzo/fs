package service

import (
	"errors"
	"fs/metadata"
	"fs/models"
)

// ErrDeleteMarker reports that a version resolves to a delete marker.
var ErrDeleteMarker = errors.New("object version is a delete marker")

// getManifest resolves the latest or a specific version and rejects delete
// markers, which are not readable object data.
func (s *ObjectService) getManifest(bucket, key, versionID string) (*models.ObjectManifest, error) {
	var manifest *models.ObjectManifest
	var err error
	if versionID == "" {
		manifest, err = s.metadata.GetManifest(bucket, key)
	} else {
		manifest, err = s.metadata.GetManifestVersion(bucket, key, versionID)
	}
	if err != nil {
		return nil, err
	}
	if manifest.DeleteMarker {
		return nil, ErrDeleteMarker
	}
	return manifest, nil
}

// GetBucketVersioning returns the bucket's versioning state ("" when off).
func (s *ObjectService) GetBucketVersioning(bucket string) (string, error) {
	return s.metadata.GetBucketVersioning(bucket)
}

// SetBucketVersioning enables or suspends versioning for a bucket.
func (s *ObjectService) SetBucketVersioning(bucket, status string) error {
	return s.metadata.SetBucketVersioning(bucket, status)
}

// ListObjectVersions returns one page of versions, including delete markers.
func (s *ObjectService) ListObjectVersions(bucket, prefix, delimiter, keyMarker, versionIDMarker string, maxKeys int) (*metadata.ObjectVersionsPage, error) {
	return s.metadata.ListObjectVersions(bucket, prefix, delimiter, keyMarker, versionIDMarker, maxKeys)
}
