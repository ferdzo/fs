package cmd

import (
	"fmt"
	"strings"
)

type rolePolicyOptions struct {
	Role   string
	Bucket string
	Prefix string
}

func buildPolicyFromRole(opts rolePolicyOptions) (adminPolicy, error) {
	role := strings.ToLower(strings.TrimSpace(opts.Role))
	bucket := strings.TrimSpace(opts.Bucket)
	prefix := strings.TrimSpace(opts.Prefix)
	if bucket == "" {
		bucket = "*"
	}
	if prefix == "" {
		prefix = "*"
	}

	var actions []string
	switch role {
	case "admin":
		actions = []string{"s3:*"}
	case "readwrite":
		actions = []string{
			"s3:ListBucket",
			"s3:ListBucketVersions",
			"s3:ListBucketMultipartUploads",
			"s3:GetBucketLocation",
			"s3:GetBucketVersioning",
			"s3:GetObject",
			"s3:GetObjectVersion",
			"s3:PutObject",
			"s3:DeleteObject",
			"s3:CreateMultipartUpload",
			"s3:UploadPart",
			"s3:ListMultipartUploadParts",
			"s3:CompleteMultipartUpload",
			"s3:AbortMultipartUpload",
		}
	case "readonly":
		actions = []string{
			"s3:ListBucket",
			"s3:ListBucketVersions",
			"s3:GetBucketLocation",
			"s3:GetBucketVersioning",
			"s3:GetObject",
			"s3:GetObjectVersion",
		}
	default:
		return adminPolicy{}, fmt.Errorf("invalid role %q (allowed: admin, readwrite, readonly)", opts.Role)
	}

	return adminPolicy{
		Statements: []adminPolicyStatement{
			{
				Effect:  "allow",
				Actions: actions,
				Bucket:  bucket,
				Prefix:  prefix,
			},
		},
	}, nil
}
