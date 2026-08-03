package gitproxy

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	EnvHTTPAddress           = "TETRAL_GIT_PROXY_HTTP_ADDR"
	EnvMetricsAddress        = "TETRAL_GIT_PROXY_METRICS_ADDR"
	EnvDatabaseURL           = "TETRAL_DATABASE_URL"
	EnvVaultKey              = "ENGINE_VAULT_KEY"
	EnvPublicBaseURL         = "TETRAL_GIT_PROXY_PUBLIC_BASE_URL"
	EnvDeploymentEnvironment = "TETRAL_DEPLOYMENT_ENVIRONMENT"
	EnvServiceVersion        = "TETRAL_SERVICE_VERSION"
	EnvDrainGraceSeconds     = "TETRAL_GIT_PROXY_DRAIN_GRACE_SECONDS"
	EnvLegacyPathCutover     = "TETRAL_GIT_PROXY_LEGACY_PATH_CUTOVER"

	DefaultHTTPAddress    = ":8080"
	DefaultMetricsAddress = ":8081"
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	HTTPAddress           string
	MetricsAddress        string
	DatabaseURL           string
	VaultKey              string
	PublicBaseURL         *url.URL
	DeploymentEnvironment string
	ServiceVersion        string
	DrainGrace            time.Duration
	LegacyPathCutover     bool
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	cfg := Config{
		HTTPAddress:           valueOrDefault(env.Getenv(EnvHTTPAddress), DefaultHTTPAddress),
		MetricsAddress:        valueOrDefault(env.Getenv(EnvMetricsAddress), DefaultMetricsAddress),
		DatabaseURL:           strings.TrimSpace(env.Getenv(EnvDatabaseURL)),
		VaultKey:              strings.TrimSpace(env.Getenv(EnvVaultKey)),
		DeploymentEnvironment: valueOrDefault(env.Getenv(EnvDeploymentEnvironment), "local"),
		ServiceVersion:        valueOrDefault(env.Getenv(EnvServiceVersion), "unknown"),
		DrainGrace:            time.Duration(DefaultDrainGraceSeconds) * time.Second,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, workload.NewConfigError(EnvDatabaseURL + " is required")
	}
	if err := ValidateVaultKey(cfg.VaultKey); err != nil {
		return Config{}, err
	}
	if cfg.HTTPAddress == cfg.MetricsAddress {
		return Config{}, workload.NewConfigError(EnvMetricsAddress + " must not equal " + EnvHTTPAddress)
	}
	if raw := strings.TrimSpace(env.Getenv(EnvPublicBaseURL)); raw != "" {
		parsed, err := parsePublicBaseURL(raw)
		if err != nil {
			return Config{}, err
		}
		cfg.PublicBaseURL = parsed
	}
	// DrainGrace bounds the SIGTERM drain window (in-flight transfers run to
	// completion within it before exit) and, wired through run.go into
	// TicketValidator.RotationGrace, is also the window a rotated git ticket
	// stays honored: a rotated row is accepted while now - rotated_at <= grace.
	// Default DefaultDrainGraceSeconds (1800s); DefaultTicketRotationGraceSeconds
	// defaults equal to DrainGraceSeconds (ticket.go) so the accept window
	// matches the drain window — a single git operation whose sequential
	// requests (GET /info/refs then POST git-*-pack) straddle a credential rotation
	// ticket rotation during a rolling deploy completes instead of failing
	// mid-way. This override moves both roles together.
	// UPDATE-WITH: run.go (wires DrainGrace into RotationGrace and
	// ShutdownTimeout), ticket.go (DefaultTicketRotationGraceSeconds /
	// rotationGrace default).
	if raw := strings.TrimSpace(env.Getenv(EnvDrainGraceSeconds)); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 {
			return Config{}, workload.NewConfigError(EnvDrainGraceSeconds + " must be a positive integer")
		}
		cfg.DrainGrace = time.Duration(seconds) * time.Second
	}
	if raw := strings.TrimSpace(env.Getenv(EnvLegacyPathCutover)); raw != "" {
		switch raw {
		case "true":
			cfg.LegacyPathCutover = true
		case "false":
			cfg.LegacyPathCutover = false
		default:
			return Config{}, workload.NewConfigError(EnvLegacyPathCutover + " must be true or false")
		}
	}
	return cfg, nil
}

func ValidateVaultKey(key string) error {
	if key == "" {
		return workload.NewConfigError(EnvVaultKey + " is required")
	}
	if len(key) != 64 {
		return workload.NewConfigError(fmt.Sprintf("%s must be 64 hex characters (32 bytes), got %d characters", EnvVaultKey, len(key)))
	}
	if _, err := hex.DecodeString(key); err != nil {
		return workload.NewConfigError(EnvVaultKey + " must be 64 hexadecimal characters (32 bytes); it contains non-hexadecimal characters")
	}
	return nil
}

func parsePublicBaseURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil ||
		parsed.Scheme != "https" ||
		parsed.Host == "" ||
		parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") ||
		parsed.RawQuery != "" ||
		parsed.Fragment != "" {
		return nil, workload.NewConfigError(EnvPublicBaseURL + " must be an https origin without credentials, query, or fragment")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed, nil
}

func valueOrDefault(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
