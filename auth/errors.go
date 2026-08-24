package auth

import "errors"

var (
	ErrAccessDenied                 = errors.New("access denied")
	ErrInvalidAccessKeyID           = errors.New("invalid access key id")
	ErrUserAlreadyExists            = errors.New("user already exists")
	ErrUserNotFound                 = errors.New("user not found")
	ErrInvalidUserInput             = errors.New("invalid user input")
	ErrSignatureDoesNotMatch        = errors.New("signature does not match")
	ErrAuthorizationHeaderMalformed = errors.New("authorization header malformed")
	ErrRequestTimeTooSkewed         = errors.New("request time too skewed")
	ErrExpiredToken                 = errors.New("expired token")
	ErrCredentialDisabled           = errors.New("credential disabled")
	ErrRateLimited                  = errors.New("authentication rate limited")
	ErrAuthNotEnabled               = errors.New("authentication is not enabled")
	ErrMasterKeyRequired            = errors.New("auth master key is required")
	ErrInvalidMasterKey             = errors.New("invalid auth master key")
	ErrNoAuthCredentials            = errors.New("no auth credentials found")
	ErrUnsupportedAuthScheme        = errors.New("unsupported auth scheme")
	ErrInvalidPresign               = errors.New("invalid presigned request")
)
