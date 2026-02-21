package utils

import (
	"encoding/xml"
	"fs/models"
	"sort"
	"strings"
	"time"
)

func ConstructXMLResponseForObjectList(bucket string, objects []*models.ObjectManifest) (string, error) {
	result := models.ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      "",
		KeyCount:    len(objects),
		MaxKeys:     1000,
		IsTruncated: false,
	}

	prefixSet := make(map[string]struct{})

	for _, object := range objects {
		result.Contents = append(result.Contents, models.Contents{
			Key:          object.Key,
			LastModified: time.Unix(object.CreatedAt, 0).UTC().Format("2006-01-02T15:04:05.000Z"),
			ETag:         "\"" + object.ETag + "\"",
			Size:         object.Size,
			StorageClass: "STANDARD",
		})

		if strings.Contains(object.Key, "/") {
			parts := strings.SplitN(object.Key, "/", 2)
			prefixSet[parts[0]+"/"] = struct{}{}
		}
	}

	prefixes := make([]string, 0, len(prefixSet))
	for prefix := range prefixSet {
		prefixes = append(prefixes, prefix)
	}
	sort.Strings(prefixes)

	for _, prefix := range prefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, models.CommonPrefixes{Prefix: prefix})
	}

	output, err := xml.MarshalIndent(result, "", "    ")
	if err != nil {
		return "", err
	}

	return xml.Header + string(output), nil
}
