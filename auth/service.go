package auth

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"fs/metadata"
	"fs/models"
	"net/http"
	"regexp"
	"strings"
	"time"
)

type Store interface {
	GetAuthIdentity(accessKeyID string) (*models.AuthIdentity, error)
	PutAuthIdentity(identity *models.AuthIdentity) error
	DeleteAuthIdentity(accessKeyID string) error
	ListAuthIdentities(limit int, after string) ([]models.AuthIdentity, string, error)
	GetAuthPolicy(accessKeyID string) (*models.AuthPolicy, error)
	PutAuthPolicy(policy *models.AuthPolicy) error
	DeleteAuthPolicy(accessKeyID string) error
}

type CreateUserInput struct {
	AccessKeyID string
	SecretKey   string
	Status      string
	Policy      models.AuthPolicy
}

type UserSummary struct {
	AccessKeyID string
	Status      string
	CreatedAt   int64
	UpdatedAt   int64
}

type UserDetails struct {
	AccessKeyID string
	Status      string
	CreatedAt   int64
	UpdatedAt   int64
	Policy      models.AuthPolicy
}

type CreateUserResult struct {
	UserDetails
	SecretKey string
}

var validAccessKeyID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{2,127}$`)

type Service struct {
	cfg       Config
	store     Store
	masterKey []byte
	now       func() time.Time
}

func NewService(cfg Config, store Store) (*Service, error) {
	if store == nil {
		return nil, errors.New("auth store is required")
	}

	svc := &Service{
		cfg:   cfg,
		store: store,
		now:   func() time.Time { return time.Now().UTC() },
	}
	if !cfg.Enabled {
		return svc, nil
	}

	if strings.TrimSpace(cfg.MasterKey) == "" {
		return nil, ErrMasterKeyRequired
	}
	masterKey, err := decodeMasterKey(cfg.MasterKey)
	if err != nil {
		return nil, err
	}
	svc.masterKey = masterKey
	return svc, nil
}

func (s *Service) Config() Config {
	return s.cfg
}

func (s *Service) EnsureBootstrap() error {
	if !s.cfg.Enabled {
		return nil
	}
	accessKey := strings.TrimSpace(s.cfg.BootstrapAccessKey)
	secret := strings.TrimSpace(s.cfg.BootstrapSecretKey)
	if accessKey == "" || secret == "" {
		return nil
	}

	if len(accessKey) < 3 {
		return errors.New("bootstrap access key must be at least 3 characters")
	}
	if len(secret) < 8 {
		return errors.New("bootstrap secret key must be at least 8 characters")
	}

	now := s.now().Unix()
	ciphertext, nonce, err := encryptSecret(s.masterKey, accessKey, secret)
	if err != nil {
		return err
	}
	identity := &models.AuthIdentity{
		AccessKeyID: accessKey,
		SecretEnc:   ciphertext,
		SecretNonce: nonce,
		EncAlg:      "AES-256-GCM",
		KeyVersion:  "v1",
		Status:      "active",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if existing, err := s.store.GetAuthIdentity(accessKey); err == nil && existing != nil {
		identity.CreatedAt = existing.CreatedAt
	}
	if err := s.store.PutAuthIdentity(identity); err != nil {
		return err
	}

	policy := defaultBootstrapPolicy(accessKey)
	if strings.TrimSpace(s.cfg.BootstrapPolicy) != "" {
		parsed, err := parsePolicyJSON(s.cfg.BootstrapPolicy)
		if err != nil {
			return err
		}
		policy = parsed
		policy.Principal = accessKey
	}
	return s.store.PutAuthPolicy(policy)
}

func (s *Service) AuthenticateRequest(r *http.Request) (RequestContext, error) {
	if !s.cfg.Enabled {
		return RequestContext{Authenticated: false, AuthType: "disabled"}, nil
	}
	input, err := parseSigV4(r)
	if err != nil {
		return RequestContext{}, err
	}

	if err := validateSigV4Input(s.now(), s.cfg, input); err != nil {
		return RequestContext{}, err
	}
	if err := validatePayloadSigningMode(r, input); err != nil {
		return RequestContext{}, err
	}

	identity, err := s.store.GetAuthIdentity(input.AccessKeyID)
	if err != nil {
		return RequestContext{}, ErrInvalidAccessKeyID
	}
	if !strings.EqualFold(identity.Status, "active") {
		return RequestContext{}, ErrCredentialDisabled
	}

	secret, err := decryptSecret(s.masterKey, identity.AccessKeyID, identity.SecretEnc, identity.SecretNonce)
	if err != nil {
		return RequestContext{}, ErrSignatureDoesNotMatch
	}
	signingKey, ok, err := signatureMatchsWithKey(secret, r, input)
	if err != nil {
		return RequestContext{}, err
	}
	if !ok {
		return RequestContext{}, ErrSignatureDoesNotMatch
	}

	var streaming *StreamingAuth
	payloadMode := resolvePayloadHash(r, input.Presigned)
	if isSignedStreamingPayloadHash(payloadMode) && !input.Presigned {
		streaming = &StreamingAuth{
			SigningKey:    signingKey,
			AmzDate:       input.AmzDate,
			Scope:         input.Scope,
			SeedSignature: strings.ToLower(input.SignatureHex),
		}
	}

	authType := "sigv4-header"
	if input.Presigned {
		authType = "sigv4-presign"
	}

	if strings.HasPrefix(r.URL.Path, "/_admin/") {
		return RequestContext{
			Authenticated: true,
			AccessKeyID:   identity.AccessKeyID,
			AuthType:      authType,
			Streaming:     streaming,
		}, nil
	}
	if RequiresHandlerAuthorization(r) {
		return RequestContext{
			Authenticated: true,
			AccessKeyID:   identity.AccessKeyID,
			AuthType:      authType,
			Streaming:     streaming,
		}, nil
	}

	policy, err := s.store.GetAuthPolicy(identity.AccessKeyID)
	if err != nil {
		return RequestContext{}, ErrAccessDenied
	}
	target := resolveTarget(r)
	if target.Action == "" {
		return RequestContext{}, ErrAccessDenied
	}
	if !isAllowed(policy, target) {
		return RequestContext{}, ErrAccessDenied
	}

	return RequestContext{
		Authenticated: true,
		AccessKeyID:   identity.AccessKeyID,
		AuthType:      authType,
		Streaming:     streaming,
	}, nil
}

func (s *Service) Authorize(accessKeyID string, target RequestTarget) error {
	if !s.cfg.Enabled {
		return nil
	}

	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return ErrAccessDenied
	}
	if target.Action == "" {
		return ErrAccessDenied
	}

	policy, err := s.store.GetAuthPolicy(accessKeyID)
	if err != nil {
		return ErrAccessDenied
	}
	if !isAllowed(policy, target) {
		return ErrAccessDenied
	}
	return nil
}

func (s *Service) CreateUser(input CreateUserInput) (*CreateUserResult, error) {
	if !s.cfg.Enabled {
		return nil, ErrAuthNotEnabled
	}

	accessKeyID := strings.TrimSpace(input.AccessKeyID)
	if !validAccessKeyID.MatchString(accessKeyID) {
		return nil, fmt.Errorf("%w: invalid access key id", ErrInvalidUserInput)
	}

	secretKey := strings.TrimSpace(input.SecretKey)
	if secretKey == "" {
		generated, err := generateSecretKey(32)
		if err != nil {
			return nil, err
		}
		secretKey = generated
	}
	if len(secretKey) < 8 {
		return nil, fmt.Errorf("%w: secret key must be at least 8 characters", ErrInvalidUserInput)
	}

	status := normalizeUserStatus(input.Status)
	if status == "" {
		return nil, fmt.Errorf("%w: status must be active or disabled", ErrInvalidUserInput)
	}

	policy, err := normalizePolicy(input.Policy, accessKeyID)
	if err != nil {
		return nil, err
	}

	existing, err := s.store.GetAuthIdentity(accessKeyID)
	if err == nil && existing != nil {
		return nil, ErrUserAlreadyExists
	}
	if err != nil && !errors.Is(err, metadata.ErrAuthIdentityNotFound) {
		return nil, err
	}

	now := s.now().Unix()
	ciphertext, nonce, err := encryptSecret(s.masterKey, accessKeyID, secretKey)
	if err != nil {
		return nil, err
	}
	identity := &models.AuthIdentity{
		AccessKeyID: accessKeyID,
		SecretEnc:   ciphertext,
		SecretNonce: nonce,
		EncAlg:      "AES-256-GCM",
		KeyVersion:  "v1",
		Status:      status,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := s.store.PutAuthIdentity(identity); err != nil {
		return nil, err
	}
	if err := s.store.PutAuthPolicy(&policy); err != nil {
		return nil, err
	}

	return &CreateUserResult{
		UserDetails: UserDetails{
			AccessKeyID: accessKeyID,
			Status:      status,
			CreatedAt:   now,
			UpdatedAt:   now,
			Policy:      policy,
		},
		SecretKey: secretKey,
	}, nil
}

func (s *Service) ListUsers(limit int, cursor string) ([]UserSummary, string, error) {
	if !s.cfg.Enabled {
		return nil, "", ErrAuthNotEnabled
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	identities, nextCursor, err := s.store.ListAuthIdentities(limit, cursor)
	if err != nil {
		return nil, "", err
	}
	users := make([]UserSummary, 0, len(identities))
	for _, identity := range identities {
		users = append(users, UserSummary{
			AccessKeyID: identity.AccessKeyID,
			Status:      normalizeUserStatus(identity.Status),
			CreatedAt:   identity.CreatedAt,
			UpdatedAt:   identity.UpdatedAt,
		})
	}
	return users, nextCursor, nil
}

func (s *Service) GetUser(accessKeyID string) (*UserDetails, error) {
	if !s.cfg.Enabled {
		return nil, ErrAuthNotEnabled
	}
	accessKeyID = strings.TrimSpace(accessKeyID)
	if accessKeyID == "" {
		return nil, fmt.Errorf("%w: access key id is required", ErrInvalidUserInput)
	}

	identity, err := s.store.GetAuthIdentity(accessKeyID)
	if err != nil {
		if errors.Is(err, metadata.ErrAuthIdentityNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	policy, err := s.store.GetAuthPolicy(accessKeyID)
	if err != nil {
		if errors.Is(err, metadata.ErrAuthPolicyNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return &UserDetails{
		AccessKeyID: identity.AccessKeyID,
		Status:      normalizeUserStatus(identity.Status),
		CreatedAt:   identity.CreatedAt,
		UpdatedAt:   identity.UpdatedAt,
		Policy:      *policy,
	}, nil
}

func (s *Service) DeleteUser(accessKeyID string) error {
	if !s.cfg.Enabled {
		return ErrAuthNotEnabled
	}
	accessKeyID = strings.TrimSpace(accessKeyID)
	if !validAccessKeyID.MatchString(accessKeyID) {
		return fmt.Errorf("%w: invalid access key id", ErrInvalidUserInput)
	}

	bootstrap := strings.TrimSpace(s.cfg.BootstrapAccessKey)
	if bootstrap != "" && accessKeyID == bootstrap {
		return fmt.Errorf("%w: bootstrap user cannot be deleted", ErrInvalidUserInput)
	}

	if _, err := s.store.GetAuthIdentity(accessKeyID); err != nil {
		if errors.Is(err, metadata.ErrAuthIdentityNotFound) {
			return ErrUserNotFound
		}
		return err
	}

	if err := s.store.DeleteAuthIdentity(accessKeyID); err != nil {
		if errors.Is(err, metadata.ErrAuthIdentityNotFound) {
			return ErrUserNotFound
		}
		return err
	}

	if err := s.store.DeleteAuthPolicy(accessKeyID); err != nil && !errors.Is(err, metadata.ErrAuthPolicyNotFound) {
		return err
	}
	return nil
}

func (s *Service) SetUserPolicy(accessKeyID string, policy models.AuthPolicy) (*UserDetails, error) {
	if !s.cfg.Enabled {
		return nil, ErrAuthNotEnabled
	}
	accessKeyID = strings.TrimSpace(accessKeyID)
	if !validAccessKeyID.MatchString(accessKeyID) {
		return nil, fmt.Errorf("%w: invalid access key id", ErrInvalidUserInput)
	}

	identity, err := s.store.GetAuthIdentity(accessKeyID)
	if err != nil {
		if errors.Is(err, metadata.ErrAuthIdentityNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	normalizedPolicy, err := normalizePolicy(policy, accessKeyID)
	if err != nil {
		return nil, err
	}
	if err := s.store.PutAuthPolicy(&normalizedPolicy); err != nil {
		return nil, err
	}

	identity.UpdatedAt = s.now().Unix()
	if err := s.store.PutAuthIdentity(identity); err != nil {
		return nil, err
	}

	return &UserDetails{
		AccessKeyID: identity.AccessKeyID,
		Status:      normalizeUserStatus(identity.Status),
		CreatedAt:   identity.CreatedAt,
		UpdatedAt:   identity.UpdatedAt,
		Policy:      normalizedPolicy,
	}, nil
}

func (s *Service) SetUserStatus(accessKeyID, status string) (*UserDetails, error) {
	if !s.cfg.Enabled {
		return nil, ErrAuthNotEnabled
	}
	accessKeyID = strings.TrimSpace(accessKeyID)
	if !validAccessKeyID.MatchString(accessKeyID) {
		return nil, fmt.Errorf("%w: invalid access key id", ErrInvalidUserInput)
	}

	status = strings.TrimSpace(status)
	if status == "" {
		return nil, fmt.Errorf("%w: status is required", ErrInvalidUserInput)
	}
	normalizedStatus := normalizeUserStatus(status)
	if normalizedStatus == "" {
		return nil, fmt.Errorf("%w: status must be active or disabled", ErrInvalidUserInput)
	}

	if bootstrap := strings.TrimSpace(s.cfg.BootstrapAccessKey); bootstrap != "" && accessKeyID == bootstrap && normalizedStatus != "active" {
		return nil, fmt.Errorf("%w: bootstrap user cannot be disabled", ErrInvalidUserInput)
	}

	identity, err := s.store.GetAuthIdentity(accessKeyID)
	if err != nil {
		if errors.Is(err, metadata.ErrAuthIdentityNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	identity.Status = normalizedStatus
	identity.UpdatedAt = s.now().Unix()
	if err := s.store.PutAuthIdentity(identity); err != nil {
		return nil, err
	}

	policy, err := s.store.GetAuthPolicy(accessKeyID)
	if err != nil {
		if errors.Is(err, metadata.ErrAuthPolicyNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}

	return &UserDetails{
		AccessKeyID: identity.AccessKeyID,
		Status:      normalizeUserStatus(identity.Status),
		CreatedAt:   identity.CreatedAt,
		UpdatedAt:   identity.UpdatedAt,
		Policy:      *policy,
	}, nil
}

func parsePolicyJSON(raw string) (*models.AuthPolicy, error) {
	policy := models.AuthPolicy{}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return nil, fmt.Errorf("invalid bootstrap policy: %w", err)
	}
	if len(policy.Statements) == 0 {
		return nil, errors.New("bootstrap policy must contain at least one statement")
	}
	return &policy, nil
}

func defaultBootstrapPolicy(principal string) *models.AuthPolicy {
	return &models.AuthPolicy{
		Principal: principal,
		Statements: []models.AuthPolicyStatement{
			{
				Effect:  "allow",
				Actions: []string{"s3:*"},
				Bucket:  "*",
				Prefix:  "*",
			},
		},
	}
}

func generateSecretKey(length int) (string, error) {
	if length <= 0 {
		length = 32
	}
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func normalizeUserStatus(raw string) string {
	status := strings.ToLower(strings.TrimSpace(raw))
	if status == "" {
		return "active"
	}
	if status != "active" && status != "disabled" {
		return ""
	}
	return status
}

func normalizePolicy(policy models.AuthPolicy, principal string) (models.AuthPolicy, error) {
	if len(policy.Statements) == 0 {
		return models.AuthPolicy{}, fmt.Errorf("%w: at least one policy statement is required", ErrInvalidUserInput)
	}

	out := models.AuthPolicy{
		Principal:  principal,
		Statements: make([]models.AuthPolicyStatement, 0, len(policy.Statements)),
	}
	for _, stmt := range policy.Statements {
		effect := strings.ToLower(strings.TrimSpace(stmt.Effect))
		if effect != "allow" && effect != "deny" {
			return models.AuthPolicy{}, fmt.Errorf("%w: invalid policy effect %q", ErrInvalidUserInput, stmt.Effect)
		}

		actions := make([]string, 0, len(stmt.Actions))
		for _, action := range stmt.Actions {
			action = strings.TrimSpace(action)
			if action == "" {
				continue
			}
			actions = append(actions, action)
		}
		if len(actions) == 0 {
			return models.AuthPolicy{}, fmt.Errorf("%w: policy statement must include at least one action", ErrInvalidUserInput)
		}

		bucket := strings.TrimSpace(stmt.Bucket)
		if bucket == "" {
			bucket = "*"
		}
		prefix := strings.TrimSpace(stmt.Prefix)
		if prefix == "" {
			prefix = "*"
		}

		out.Statements = append(out.Statements, models.AuthPolicyStatement{
			Effect:  effect,
			Actions: actions,
			Bucket:  bucket,
			Prefix:  prefix,
		})
	}
	return out, nil
}
