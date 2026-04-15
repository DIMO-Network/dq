// Package config holds application configuration settings.
package config

import (
	"github.com/DIMO-Network/clickhouse-infra/pkg/connect/config"
)

// Settings contains the application config.
type Settings struct {
	LogLevel             string          `yaml:"LOG_LEVEL"`
	Port                 int             `yaml:"PORT"`
	GRPCPort             int             `yaml:"GRPC_PORT"`
	MonPort              int             `yaml:"MON_PORT"`
	EnablePprof          bool            `yaml:"ENABLE_PPROF"`
	MaxRequestDuration   string          `yaml:"MAX_REQUEST_DURATION"`
	Clickhouse           config.Settings `yaml:",inline"`
	TokenExchangeJWTKeySetURL string     `yaml:"TOKEN_EXCHANGE_JWK_KEY_SET_URL"`
	TokenExchangeIssuer  string          `yaml:"TOKEN_EXCHANGE_ISSUER_URL"`
	// S3 storage (cloud events)
	CloudEventBucket     string          `yaml:"CLOUDEVENT_BUCKET"`
	EphemeralBucket      string          `yaml:"EPHEMERAL_BUCKET"`
	ParquetBucket        string          `yaml:"PARQUET_BUCKET"`
	S3AWSRegion          string          `yaml:"S3_AWS_REGION"`
	S3AWSAccessKeyID     string          `yaml:"S3_AWS_ACCESS_KEY_ID"`
	S3AWSSecretAccessKey string          `yaml:"S3_AWS_SECRET_ACCESS_KEY"`
	// Identity API for device→vehicle DID resolution
	IdentityAPIURL       string          `yaml:"IDENTITY_API_URL"`
	// Signals
	DeviceLastSeenBinHrs int64           `yaml:"DEVICE_LAST_SEEN_BIN_HOURS"`
}
