package auth

import (
	"errors"
	"fs/models"
	"testing"
)

func TestAuthenticateCredentials(t *testing.T) {
	_, svc := newTestAuthService(t)
	created, err := svc.CreateUser(CreateUserInput{
		AccessKeyID: "dash-user",
		Policy: models.AuthPolicy{Statements: []models.AuthPolicyStatement{{
			Effect:  "allow",
			Actions: []string{"s3:ListBucket"},
			Bucket:  "*",
		}}},
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	if err := svc.AuthenticateCredentials("dash-user", created.SecretKey); err != nil {
		t.Fatalf("valid credentials rejected: %v", err)
	}
	if err := svc.AuthenticateCredentials("dash-user", "wrong-secret"); !errors.Is(err, ErrSignatureDoesNotMatch) {
		t.Fatalf("wrong secret = %v, want ErrSignatureDoesNotMatch", err)
	}
	if err := svc.AuthenticateCredentials("missing-user", created.SecretKey); !errors.Is(err, ErrInvalidAccessKeyID) {
		t.Fatalf("missing user = %v, want ErrInvalidAccessKeyID", err)
	}
	if _, err := svc.SetUserStatus("dash-user", "disabled"); err != nil {
		t.Fatalf("SetUserStatus: %v", err)
	}
	if err := svc.AuthenticateCredentials("dash-user", created.SecretKey); !errors.Is(err, ErrCredentialDisabled) {
		t.Fatalf("disabled user = %v, want ErrCredentialDisabled", err)
	}
}
