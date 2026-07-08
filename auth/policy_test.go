package auth

import (
	"fs/models"
	"testing"
)

func TestListBucketPolicyAppliesPrefix(t *testing.T) {
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{
			{
				Effect:  "allow",
				Actions: []string{"s3:ListBucket"},
				Bucket:  "test-bucket",
				Prefix:  "allowed/",
			},
		},
	}

	if !isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket", Prefix: "allowed/"}) {
		t.Fatalf("expected matching list prefix to be allowed")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket", Prefix: "allowed/nested/"}) {
		t.Fatalf("expected nested list prefix to be allowed")
	}
	if isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket"}) {
		t.Fatalf("expected empty list prefix to be denied")
	}
	if isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket", Prefix: "private/"}) {
		t.Fatalf("expected non-matching list prefix to be denied")
	}
}

func TestWildcardListBucketPolicyAllowsAnyPrefix(t *testing.T) {
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{
			{
				Effect:  "allow",
				Actions: []string{"s3:ListBucket"},
				Bucket:  "test-bucket",
				Prefix:  "*",
			},
		},
	}

	if !isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket"}) {
		t.Fatalf("expected wildcard list policy to allow empty prefix")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionListBucket, Bucket: "test-bucket", Prefix: "private/"}) {
		t.Fatalf("expected wildcard list policy to allow arbitrary prefix")
	}
}
