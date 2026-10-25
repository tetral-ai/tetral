package computerhub

import (
	"encoding/hex"
	"strings"
	"time"

	"golang.org/x/mod/semver"

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
	EnvMaxCredentialLifetime      = "TETRAL_COMPUTER_HUB_MAX_CREDENTIAL_LIFETIME" //nolint:gosec // G101: environment variable name, not credential material.
	EnvPageTokenKey               = "TETRAL_COMPUTER_HUB_PAGE_TOKEN_KEY"          //nolint:gosec // G101: environment variable name, not credential material.
	EnvMinCLIVersion              = "TETRAL_COMPUTER_HUB_MIN_CLI_VERSION"
	EnvRecommendedCLIVersion      = "TETRAL_COMPUTER_HUB_RECOMMENDED_CLI_VERSION"
	EnvPodName                    = "TETRAL_COMPUTER_HUB_POD_NAME"

	DefaultHTTPAddress           = ":8080"
	DefaultMetricsAddress        = ":8081"
	DefaultMaxCredentialLifetime = 720 * time.Hour
	DefaultCLIVersion            = "0.0.0"

	// maxPodNameLength is the longest Kubernetes object name.
	maxPodNameLength = 253

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
	// MaxCredentialLifetime bounds how far ahead an issued credential's
	// expires_at may lie, and is the lifetime of one issued without it.
	MaxCredentialLifetime time.Duration
	// PageTokenKey signs the list page tokens; every hub pod holds the same
	// 32 bytes.
	PageTokenKey []byte
	// MinCLIVersion refuses a ready from an older CLI; RecommendedCLIVersion,
	// never lower, marks older CLIs cli_update_available. Both are semantic
	// versions without a "v" prefix.
	MinCLIVersion         string
	RecommendedCLIVersion string
	// PodName names this pod: the hub_pod of the connections whose streams
	// it holds, and the name its connection notice channel is derived from.
	PodName string
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
	cfg.MaxCredentialLifetime = DefaultMaxCredentialLifetime
	if raw := strings.TrimSpace(env.Getenv(EnvMaxCredentialLifetime)); raw != "" {
		cfg.MaxCredentialLifetime, err = time.ParseDuration(raw)
		if err != nil || cfg.MaxCredentialLifetime <= 0 {
			return Config{}, workload.NewConfigError(EnvMaxCredentialLifetime + " must be a positive duration")
		}
	}
	cfg.PageTokenKey, err = hex.DecodeString(strings.TrimSpace(env.Getenv(EnvPageTokenKey)))
	if err != nil || len(cfg.PageTokenKey) != 32 {
		return Config{}, workload.NewConfigError(EnvPageTokenKey + " must be 32 bytes as 64 hexadecimal characters")
	}
	cfg.MinCLIVersion = valueOrDefault(env.Getenv(EnvMinCLIVersion), DefaultCLIVersion)
	cfg.RecommendedCLIVersion = valueOrDefault(env.Getenv(EnvRecommendedCLIVersion), DefaultCLIVersion)
	for key, version := range map[string]string{EnvMinCLIVersion: cfg.MinCLIVersion, EnvRecommendedCLIVersion: cfg.RecommendedCLIVersion} {
		if !semanticVersion(version) {
			return Config{}, workload.NewConfigError(key + " must be a semantic version such as 1.4.0")
		}
	}
	if semver.Compare("v"+cfg.RecommendedCLIVersion, "v"+cfg.MinCLIVersion) < 0 {
		return Config{}, workload.NewConfigError(EnvRecommendedCLIVersion + " must not be lower than " + EnvMinCLIVersion)
	}
	cfg.PodName = strings.TrimSpace(env.Getenv(EnvPodName))
	if cfg.PodName == "" || len(cfg.PodName) > maxPodNameLength {
		return Config{}, workload.NewConfigError(EnvPodName + " must be the pod name, 1 to 253 characters")
	}
	return cfg, nil
}

// semanticVersion reports whether version is a full semantic version
// (major.minor.patch with optional pre-release and build) without a "v"
// prefix; golang.org/x/mod/semver alone also admits "1" and "1.2".
func semanticVersion(version string) bool {
	core, _, _ := strings.Cut(version, "+")
	core, _, _ = strings.Cut(core, "-")
	return strings.Count(core, ".") == 2 && semver.IsValid("v"+version)
}

func valueOrDefault(value, fallback string) string {
	if value = strings.TrimSpace(value); value == "" {
		return fallback
	}
	return value
}
