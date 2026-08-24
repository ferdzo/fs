package auth

import (
	"strings"
	"time"
)

type Config struct {
	Enabled bool
	// FailureLimitPerMinute throttles failed authentications per source IP.
	// Zero disables the limiter. Parsed from FS_AUTH_FAILURE_LIMIT_PER_MIN.
	FailureLimitPerMinute int
	Region                string
	ClockSkew             time.Duration
	MaxPresignDuration    time.Duration
	MasterKey             string
	BootstrapAccessKey    string
	BootstrapSecretKey    string
	BootstrapPolicy       string
}

func ConfigFromValues(
	enabled bool,
	region string,
	skew time.Duration,
	maxPresign time.Duration,
	masterKey string,
	bootstrapAccessKey string,
	bootstrapSecretKey string,
	bootstrapPolicy string,
	failureLimitPerMinute int,
) Config {
	region = strings.TrimSpace(region)
	if region == "" {
		region = "us-east-1"
	}
	if failureLimitPerMinute < 0 {
		failureLimitPerMinute = 0
	}
	if skew <= 0 {
		skew = 5 * time.Minute
	}
	if maxPresign <= 0 {
		maxPresign = 24 * time.Hour
	}

	return Config{
		Enabled:               enabled,
		Region:                region,
		ClockSkew:             skew,
		MaxPresignDuration:    maxPresign,
		MasterKey:             strings.TrimSpace(masterKey),
		BootstrapAccessKey:    strings.TrimSpace(bootstrapAccessKey),
		BootstrapSecretKey:    strings.TrimSpace(bootstrapSecretKey),
		BootstrapPolicy:       strings.TrimSpace(bootstrapPolicy),
		FailureLimitPerMinute: failureLimitPerMinute,
	}
}
