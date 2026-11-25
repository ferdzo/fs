package cmd

import "testing"

func TestBuildPolicyFromRolePresets(t *testing.T) {
	readonly, err := buildPolicyFromRole(rolePolicyOptions{Role: "readonly", Bucket: "b", Prefix: "p/"})
	if err != nil {
		t.Fatalf("readonly: %v", err)
	}
	roActions := make(map[string]bool)
	for _, a := range readonly.Statements[0].Actions {
		roActions[a] = true
	}
	for _, want := range []string{
		"s3:ListBucket",
		"s3:ListBucketVersions",
		"s3:GetBucketLocation",
		"s3:GetBucketVersioning",
		"s3:GetObject",
		"s3:GetObjectVersion",
	} {
		if !roActions[want] {
			t.Fatalf("readonly missing %q", want)
		}
	}
	for _, forbidden := range []string{"s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutBucketVersioning"} {
		if roActions[forbidden] {
			t.Fatalf("readonly must not include %q", forbidden)
		}
	}

	readwrite, err := buildPolicyFromRole(rolePolicyOptions{Role: "readwrite"})
	if err != nil {
		t.Fatalf("readwrite: %v", err)
	}
	rwActions := make(map[string]bool)
	for _, a := range readwrite.Statements[0].Actions {
		rwActions[a] = true
	}
	for _, want := range []string{
		"s3:PutObject",
		"s3:DeleteObject",
		"s3:ListBucketMultipartUploads",
		"s3:GetObjectVersion",
	} {
		if !rwActions[want] {
			t.Fatalf("readwrite missing %q", want)
		}
	}
	for _, forbidden := range []string{"s3:PutBucketVersioning", "s3:DeleteObjectVersion"} {
		if rwActions[forbidden] {
			t.Fatalf("readwrite must not include privileged action %q", forbidden)
		}
	}

	admin, err := buildPolicyFromRole(rolePolicyOptions{Role: "admin"})
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	if len(admin.Statements[0].Actions) != 1 || admin.Statements[0].Actions[0] != "s3:*" {
		t.Fatalf("admin actions = %v, want [s3:*]", admin.Statements[0].Actions)
	}
}
