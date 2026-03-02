package auth

import (
	"encoding/base64"
	"errors"
	"fs/metadata"
	"fs/models"
	"path/filepath"
	"testing"
)

func TestAdminCreateListGetUser(t *testing.T) {
	meta, svc := newTestAuthService(t)

	created, err := svc.CreateUser(CreateUserInput{
		AccessKeyID: "backup-user",
		Policy: models.AuthPolicy{
			Statements: []models.AuthPolicyStatement{
				{
					Effect:  "allow",
					Actions: []string{"s3:GetObject"},
					Bucket:  "backup-bucket",
					Prefix:  "restic/",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateUser returned error: %v", err)
	}
	if created.SecretKey == "" {
		t.Fatalf("CreateUser should return generated secret")
	}
	if created.AccessKeyID != "backup-user" {
		t.Fatalf("CreateUser access key mismatch: got %q", created.AccessKeyID)
	}
	if created.Policy.Principal != "backup-user" {
		t.Fatalf("policy principal mismatch: got %q", created.Policy.Principal)
	}

	users, nextCursor, err := svc.ListUsers(100, "")
	if err != nil {
		t.Fatalf("ListUsers returned error: %v", err)
	}
	if nextCursor != "" {
		t.Fatalf("unexpected next cursor: %q", nextCursor)
	}
	if len(users) != 1 {
		t.Fatalf("ListUsers returned %d users, want 1", len(users))
	}
	if users[0].AccessKeyID != "backup-user" {
		t.Fatalf("ListUsers returned wrong user: %q", users[0].AccessKeyID)
	}

	got, err := svc.GetUser("backup-user")
	if err != nil {
		t.Fatalf("GetUser returned error: %v", err)
	}
	if got.AccessKeyID != "backup-user" {
		t.Fatalf("GetUser access key mismatch: got %q", got.AccessKeyID)
	}
	if got.Policy.Principal != "backup-user" {
		t.Fatalf("GetUser policy principal mismatch: got %q", got.Policy.Principal)
	}
	if len(got.Policy.Statements) != 1 {
		t.Fatalf("GetUser policy statement count = %d, want 1", len(got.Policy.Statements))
	}

	_ = meta
}

func TestCreateUserDuplicateFails(t *testing.T) {
	_, svc := newTestAuthService(t)

	input := CreateUserInput{
		AccessKeyID: "duplicate-user",
		SecretKey:   "super-secret-1",
		Policy: models.AuthPolicy{
			Statements: []models.AuthPolicyStatement{
				{Effect: "allow", Actions: []string{"s3:*"}, Bucket: "*", Prefix: "*"},
			},
		},
	}
	if _, err := svc.CreateUser(input); err != nil {
		t.Fatalf("first CreateUser returned error: %v", err)
	}
	if _, err := svc.CreateUser(input); !errors.Is(err, ErrUserAlreadyExists) {
		t.Fatalf("second CreateUser error = %v, want ErrUserAlreadyExists", err)
	}
}

func TestCreateUserRejectsInvalidAccessKey(t *testing.T) {
	_, svc := newTestAuthService(t)

	_, err := svc.CreateUser(CreateUserInput{
		AccessKeyID: "x",
		SecretKey:   "super-secret-1",
		Policy: models.AuthPolicy{
			Statements: []models.AuthPolicyStatement{
				{Effect: "allow", Actions: []string{"s3:*"}, Bucket: "*", Prefix: "*"},
			},
		},
	})
	if !errors.Is(err, ErrInvalidUserInput) {
		t.Fatalf("CreateUser error = %v, want ErrInvalidUserInput", err)
	}
}

func newTestAuthService(t *testing.T) (*metadata.MetadataHandler, *Service) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "metadata.db")
	meta, err := metadata.NewMetadataHandler(dbPath)
	if err != nil {
		t.Fatalf("NewMetadataHandler returned error: %v", err)
	}
	t.Cleanup(func() {
		_ = meta.Close()
	})

	masterKey := base64.StdEncoding.EncodeToString(make([]byte, 32))
	cfg := ConfigFromValues(
		true,
		"us-east-1",
		0,
		0,
		masterKey,
		"root-user",
		"root-secret-123",
		"",
	)
	svc, err := NewService(cfg, meta)
	if err != nil {
		t.Fatalf("NewService returned error: %v", err)
	}
	return meta, svc
}
