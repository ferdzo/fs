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

func TestTrailingWildcardPrefixMatchesDocumentedPolicyShape(t *testing.T) {
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{
			{
				Effect:  "allow",
				Actions: []string{"s3:GetObject"},
				Bucket:  "backup",
				Prefix:  "restic/*",
			},
		},
	}

	if !isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "backup", Key: "restic/"}) {
		t.Fatalf("expected restic/* policy to allow key restic/")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "backup", Key: "restic/data/0001"}) {
		t.Fatalf("expected restic/* policy to allow nested key under restic/")
	}
	if isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "backup", Key: "resticfoo"}) {
		t.Fatalf("expected restic/* policy to deny sibling key resticfoo")
	}
	if isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "backup", Key: "other/data"}) {
		t.Fatalf("expected restic/* policy to deny unrelated key")
	}
}

func TestObjectPrefixWithoutSlashKeepsLiteralStartsWithSemantics(t *testing.T) {
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{
			{
				Effect:  "allow",
				Actions: []string{"s3:GetObject"},
				Bucket:  "b",
				Prefix:  "a",
			},
		},
	}

	if !isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "b", Key: "ab"}) {
		t.Fatalf("expected bare prefix 'a' to keep starts-with semantics for 'ab'")
	}
	if isAllowed(policy, RequestTarget{Action: ActionGetObject, Bucket: "b", Key: "ba"}) {
		t.Fatalf("expected prefix 'a' to deny key not starting with it")
	}
}

func TestBucketSubresourceAndPrefixScopedListActions(t *testing.T) {
	policy := &models.AuthPolicy{
		Statements: []models.AuthPolicyStatement{
			{
				Effect: "allow",
				Actions: []string{
					"s3:GetBucketLocation",
					"s3:GetBucketVersioning",
					"s3:ListBucket",
					"s3:ListBucketVersions",
					"s3:ListBucketMultipartUploads",
				},
				Bucket: "backup",
				Prefix: "restic/",
			},
		},
	}

	if !isAllowed(policy, RequestTarget{Action: ActionGetBucketLocation, Bucket: "backup"}) {
		t.Fatal("expected GetBucketLocation to succeed under prefix-scoped bucket policy")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionGetBucketVersioning, Bucket: "backup"}) {
		t.Fatal("expected GetBucketVersioning to succeed under prefix-scoped bucket policy")
	}
	if isAllowed(policy, RequestTarget{Action: ActionPutBucketVersioning, Bucket: "backup"}) {
		t.Fatal("expected PutBucketVersioning to be denied when not in policy")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionListBucketVersions, Bucket: "backup", Prefix: "restic/daily/"}) {
		t.Fatal("expected ListBucketVersions under allowed prefix to succeed")
	}
	if isAllowed(policy, RequestTarget{Action: ActionListBucketVersions, Bucket: "backup", Prefix: "other/"}) {
		t.Fatal("expected ListBucketVersions outside allowed prefix to be denied")
	}
	if !isAllowed(policy, RequestTarget{Action: ActionListBucketMultipart, Bucket: "backup", Prefix: "restic/"}) {
		t.Fatal("expected ListBucketMultipartUploads under allowed prefix to succeed")
	}
	if isAllowed(policy, RequestTarget{Action: ActionListBucketMultipart, Bucket: "backup"}) {
		t.Fatal("expected ListBucketMultipartUploads with empty prefix to be denied under restic/ policy")
	}
}
