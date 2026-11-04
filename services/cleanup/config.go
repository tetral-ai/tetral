package tetralcleanup

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	EnvClaimLimit           = "TETRAL_CLEANUP_CLAIM_LIMIT"
	EnvMetricsExportURL     = "TETRAL_CLEANUP_METRICS_EXPORT_URL"
	EnvMetricsExportTimeout = "TETRAL_CLEANUP_METRICS_EXPORT_TIMEOUT"
	EnvRequestClaimTimeout  = "TETRAL_CLEANUP_REQUEST_CLAIM_TIMEOUT"
	EnvRequestPendingExpiry = "TETRAL_CLEANUP_REQUEST_PENDING_EXPIRY"

	defaultMetricsExportTimeout = 2 * time.Second
	defaultRequestClaimTimeout  = 5 * time.Minute
	defaultRequestPendingExpiry = 10 * time.Minute
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	ClaimLimit           int
	MetricsExportURL     string
	MetricsExportTimeout time.Duration
	// RequestClaimTimeout is how long a claimed Controller request may stay
	// unfulfilled before it returns to pending; RequestPendingExpiry how
	// long one may stay pending before it expires.
	RequestClaimTimeout  time.Duration
	RequestPendingExpiry time.Duration
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment reader is required")
	}
	limit := defaultClaimLimit
	if raw := strings.TrimSpace(env.Getenv(EnvClaimLimit)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", EnvClaimLimit))
		}
		limit = parsed
	}
	metricsExportURL := strings.TrimSpace(env.Getenv(EnvMetricsExportURL))
	if metricsExportURL != "" {
		parsed, err := url.ParseRequestURI(metricsExportURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil {
			return Config{}, workload.NewConfigError(fmt.Sprintf("%s must be an HTTP(S) URL without credentials", EnvMetricsExportURL))
		}
	}
	cfg := Config{ClaimLimit: limit, MetricsExportURL: metricsExportURL}
	for _, d := range []struct {
		name     string
		value    *time.Duration
		fallback time.Duration
	}{
		{EnvMetricsExportTimeout, &cfg.MetricsExportTimeout, defaultMetricsExportTimeout},
		{EnvRequestClaimTimeout, &cfg.RequestClaimTimeout, defaultRequestClaimTimeout},
		{EnvRequestPendingExpiry, &cfg.RequestPendingExpiry, defaultRequestPendingExpiry},
	} {
		*d.value = d.fallback
		if raw := strings.TrimSpace(env.Getenv(d.name)); raw != "" {
			parsed, err := time.ParseDuration(raw)
			if err != nil || parsed <= 0 {
				return Config{}, workload.NewConfigError(fmt.Sprintf("%s must be a positive duration", d.name))
			}
			*d.value = parsed
		}
	}
	return cfg, nil
}

// Mode is what one invocation runs: the per-minute phases, or, with the
// argument drives, the daily drive cleanup.
type Mode string

const (
	ModePhases Mode = "phases"
	ModeDrives Mode = "drives"
)

// ModeFromArgs reads the invocation's arguments: none, or drives.
func ModeFromArgs(args []string) (Mode, error) {
	switch {
	case len(args) == 0:
		return ModePhases, nil
	case len(args) == 1 && args[0] == string(ModeDrives):
		return ModeDrives, nil
	}
	return "", workload.NewConfigError("tetral-cleanup takes no argument, or the argument drives")
}

// DriveBlobConfig is the blob store of the drive mode: internal/blob's
// TETRAL_BLOB_* variables, production-ready. The per-minute phases read no
// blob store.
func DriveBlobConfig() (*blob.Config, error) {
	cfg, err := blob.LoadConfig()
	if err != nil {
		return nil, err
	}
	if err := cfg.AssertProductionReady(); err != nil {
		return nil, err
	}
	return cfg, nil
}
