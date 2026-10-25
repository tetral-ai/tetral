package computerhub

import (
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
)

// ServiceName names the service in records, metrics and its declarations.
const ServiceName = "computer-hub"

const (
	EnvHTTPAddress                = "TETRAL_COMPUTER_HUB_HTTP_ADDR"
	EnvMetricsAddress             = "TETRAL_COMPUTER_HUB_METRICS_ADDR"
	EnvDatabaseURL                = "TETRAL_DATABASE_URL"
	EnvDatabaseTLSCAPath          = "TETRAL_DATABASE_TLS_CA_PATH"
	EnvDatabaseTLSServerName      = "TETRAL_DATABASE_TLS_SERVER_NAME"
	EnvInternalPrincipalPublicKey = "TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64"

	DefaultHTTPAddress    = ":8080"
	DefaultMetricsAddress = ":8081"

	// ShutdownTimeout bounds the drain of each listener after shutdown
	// begins.
	ShutdownTimeout = 10 * time.Second
)

type Env interface {
	Getenv(string) string
}

// Config is the validated process configuration. The blob store settings are
// internal/blob's TETRAL_BLOB_* variables, read by blob.LoadConfig.
type Config struct {
	HTTPTransport         transportsecurity.HTTPConfig
	HTTPAddress           string
	MetricsAddress        string
	DatabaseURL           string
	DatabaseTLSCAPath     string
	DatabaseTLSServerName string
	DeploymentEnvironment string
	ServiceVersion        string
	PrincipalVerifier     *auth.InternalPrincipalVerifier
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	httpTransport, err := transportsecurity.HTTPConfigFromEnv(env.Getenv)
	if err != nil {
		return Config{}, workload.NewConfigError(err.Error())
	}
	resource := workload.ResourceConfigFromEnv(env.Getenv)
	cfg := Config{
		HTTPTransport:         httpTransport,
		HTTPAddress:           valueOrDefault(env.Getenv(EnvHTTPAddress), DefaultHTTPAddress),
		MetricsAddress:        valueOrDefault(env.Getenv(EnvMetricsAddress), DefaultMetricsAddress),
		DatabaseURL:           strings.TrimSpace(env.Getenv(EnvDatabaseURL)),
		DatabaseTLSCAPath:     strings.TrimSpace(env.Getenv(EnvDatabaseTLSCAPath)),
		DatabaseTLSServerName: strings.TrimSpace(env.Getenv(EnvDatabaseTLSServerName)),
		DeploymentEnvironment: resource.DeploymentEnvironment,
		ServiceVersion:        resource.ServiceVersion,
	}
	if cfg.HTTPAddress == cfg.MetricsAddress {
		return Config{}, workload.NewConfigError(EnvMetricsAddress + " must not equal " + EnvHTTPAddress)
	}
	if cfg.DatabaseURL == "" {
		return Config{}, workload.NewConfigError(EnvDatabaseURL + " is required")
	}
	rawKey := strings.TrimSpace(env.Getenv(EnvInternalPrincipalPublicKey))
	if rawKey == "" {
		return Config{}, workload.NewConfigError(EnvInternalPrincipalPublicKey + " is required")
	}
	cfg.PrincipalVerifier, err = auth.NewInternalPrincipalVerifierFromBase64(rawKey)
	if err != nil {
		return Config{}, workload.NewConfigError(EnvInternalPrincipalPublicKey + " must be a base64 Ed25519 public key")
	}
	return cfg, nil
}

func valueOrDefault(value, fallback string) string {
	if value = strings.TrimSpace(value); value == "" {
		return fallback
	}
	return value
}
