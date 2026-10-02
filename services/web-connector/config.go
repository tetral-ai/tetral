package webconnector

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	// Listener policy remains independent from backend request and Git relay deadlines.
	DefaultOpsReadHeaderTimeout    = 5 * time.Second
	DefaultListenerShutdownTimeout = 10 * time.Second
	ServiceName                    = "web-connector"
	DefaultSearchEndpoint          = "https://s.jina.ai/"
	DefaultReaderEndpoint          = "https://r.jina.ai/"
	DefaultGRPCAddress             = "0.0.0.0:9092"
	DefaultMetricsAddress          = "0.0.0.0:9464"
	EnvDrainTimeout                = "TETRAL_SERVICE_DRAIN_TIMEOUT_MS"
	EnvSearchEndpoint              = "TETRAL_WEB_SEARCH_ENDPOINT"
	EnvReaderEndpoint              = "TETRAL_WEB_READER_ENDPOINT"
	EnvAPIKeys                     = "TETRAL_WEB_API_KEYS" //nolint:gosec // configuration name, not a value
	EnvGRPCAddress                 = "TETRAL_WEB_CONNECTOR_GRPC_ADDR"
	EnvMetricsAddress              = "TETRAL_WEB_CONNECTOR_METRICS_ADDR"
	EnvBindingHMACKey              = "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY" //nolint:gosec // configuration name, not a value
)

type Env interface{ Getenv(string) string }
type Config struct {
	DrainTimeout                                                time.Duration
	SearchEndpoint, ReaderEndpoint, GRPCAddress, MetricsAddress string
	APIKeys                                                     []string
	BindingHMACKey                                              []byte
}

func LoadConfig(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	cfg := Config{SearchEndpoint: valueOrDefault(env.Getenv(EnvSearchEndpoint), DefaultSearchEndpoint), ReaderEndpoint: valueOrDefault(env.Getenv(EnvReaderEndpoint), DefaultReaderEndpoint), GRPCAddress: valueOrDefault(env.Getenv(EnvGRPCAddress), DefaultGRPCAddress), MetricsAddress: valueOrDefault(env.Getenv(EnvMetricsAddress), DefaultMetricsAddress)}
	if err := validateBackendEndpoint(cfg.SearchEndpoint, EnvSearchEndpoint); err != nil {
		return Config{}, err
	}
	if err := validateBackendEndpoint(cfg.ReaderEndpoint, EnvReaderEndpoint); err != nil {
		return Config{}, err
	}
	if err := json.Unmarshal([]byte(env.Getenv(EnvAPIKeys)), &cfg.APIKeys); err != nil || len(cfg.APIKeys) == 0 {
		return Config{}, workload.NewConfigError(EnvAPIKeys + " must be a non-empty JSON string array")
	}
	for _, key := range cfg.APIKeys {
		if strings.TrimSpace(key) == "" {
			return Config{}, workload.NewConfigError(EnvAPIKeys + " contains an empty key")
		}
	}
	hmacKey := env.Getenv(EnvBindingHMACKey)
	if len(hmacKey) < 32 || len(hmacKey) > 4096 {
		return Config{}, workload.NewConfigError(EnvBindingHMACKey + " must contain 32 to 4096 bytes")
	}

	cfg.DrainTimeout = DefaultListenerShutdownTimeout
	if raw := env.Getenv(EnvDrainTimeout); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 20000 {
			return Config{}, workload.NewConfigError(EnvDrainTimeout + " must be 1..20000 milliseconds (30s Pod budget includes joins/proxy)")
		}
		cfg.DrainTimeout = time.Duration(value) * time.Millisecond
	}
	cfg.BindingHMACKey = []byte(hmacKey)
	return cfg, nil
}
func validateBackendEndpoint(raw, name string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return workload.NewConfigError(name + " must be an HTTPS URL without credentials, query, or fragment")
	}
	return nil
}
func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
