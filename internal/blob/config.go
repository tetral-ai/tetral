package blob

import (
	"errors"
	"net/url"
	"os"
	"strings"
	"unicode/utf8"
)

// Environment variable names that configure the production S3 BlobStore.
// Keep this surface stable so operators can use one S3-compatible
// configuration path across AWS S3, MinIO, R2, and B2.
const (
	EnvEndpoint       = "TETRAL_BLOB_ENDPOINT"
	EnvRegion         = "TETRAL_BLOB_REGION"
	EnvBucket         = "TETRAL_BLOB_BUCKET"
	EnvAccessKey      = "TETRAL_BLOB_ACCESS_KEY"
	EnvSecretKey      = "TETRAL_BLOB_SECRET_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvAllowInsecure  = "TETRAL_BLOB_ALLOW_INSECURE"
	EnvLocalTestMode  = "TETRAL_BLOB_LOCAL_TEST_MODE"
	maxEndpointLength = 4096
)

// Config holds the validated S3-compatible BlobStore configuration. All
// fields are server-issued (env-var sourced); no client request body
// ever populates this struct. Construction requires Endpoint to either
// be empty (use AWS default endpoints) or to validate as a `https://`
// URL with no embedded credentials. `http://` is permitted only when
// AllowInsecure is true AND LocalTestMode is true; production
// preflight rejects AllowInsecure outside local/test.
type Config struct {
	Endpoint      string
	Region        string
	Bucket        string
	AccessKey     string
	SecretKey     string
	SessionToken  string
	AllowInsecure bool
	LocalTestMode bool
}

// ConfigPresent reports whether any of the BlobStore env vars are set.
// Engine startup uses this to decide whether the Skill upload path is
// enabled. When all five required vars are empty, BlobStore stays
// uninstalled and the /v1/skills routes return 501 stubs.
func ConfigPresent() bool {
	for _, name := range []string{EnvEndpoint, EnvRegion, EnvBucket, EnvAccessKey, EnvSecretKey} {
		if os.Getenv(name) != "" {
			return true
		}
	}
	return false
}

// LoadConfig reads BlobStore configuration from the environment and
// validates it. Validation errors are typed *ConfigError and never
// embed secret values (access key, secret key, raw endpoint URL with
// userinfo, or any other secret-bearing fragment).
func LoadConfig() (*Config, error) {
	cfg := &Config{
		Endpoint:      os.Getenv(EnvEndpoint),
		Region:        os.Getenv(EnvRegion),
		Bucket:        os.Getenv(EnvBucket),
		AccessKey:     os.Getenv(EnvAccessKey),
		SecretKey:     os.Getenv(EnvSecretKey),
		AllowInsecure: os.Getenv(EnvAllowInsecure) == "true",
		LocalTestMode: os.Getenv(EnvLocalTestMode) == "true",
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate enforces the contract's config rules. Required fields:
// region, bucket, access key, secret key. Endpoint is optional
// (defaults to AWS endpoints) but if provided must satisfy the URL
// safety rules.
func (c *Config) Validate() error {
	if c == nil {
		return &ConfigError{Message: "blob: configuration is nil"}
	}
	if strings.TrimSpace(c.Region) == "" {
		return &ConfigError{Message: "blob: " + EnvRegion + " is required"}
	}
	if strings.TrimSpace(c.Bucket) == "" {
		return &ConfigError{Message: "blob: " + EnvBucket + " is required"}
	}
	if strings.TrimSpace(c.AccessKey) == "" {
		return &ConfigError{Message: "blob: " + EnvAccessKey + " is required"}
	}
	if strings.TrimSpace(c.SecretKey) == "" {
		return &ConfigError{Message: "blob: " + EnvSecretKey + " is required"}
	}
	if c.Endpoint != "" {
		if err := validateEndpointURL(c.Endpoint, c.AllowInsecure, c.LocalTestMode); err != nil {
			return err
		}
	}
	return nil
}

// validateEndpointURL parses raw and rejects credential-bearing URLs,
// fragments, query strings, and overly long inputs. Production
// requires `https`; `http` is allowed only when AllowInsecure AND
// LocalTestMode are both set, so a startup check can refuse to honor
// AllowInsecure outside local/test mode.
func validateEndpointURL(raw string, allowInsecure, localTestMode bool) error {
	if utf8.RuneCountInString(raw) > maxEndpointLength {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " is too long"}
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " is not a valid URL"}
	}
	if parsed.Scheme == "" {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " must include a scheme (https://)"}
	}
	if parsed.User != nil {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " must not include embedded credentials"}
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " must not include a query string"}
	}
	if parsed.Fragment != "" || parsed.RawFragment != "" {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " must not include a fragment"}
	}
	if parsed.Host == "" {
		return &ConfigError{Message: "blob: " + EnvEndpoint + " must include a host"}
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		if !allowInsecure {
			return &ConfigError{Message: "blob: " + EnvEndpoint + " must use https; set " + EnvAllowInsecure + "=true and " + EnvLocalTestMode + "=true to opt in to http for local/test"}
		}
		if !localTestMode {
			return &ConfigError{Message: "blob: " + EnvAllowInsecure + " requires " + EnvLocalTestMode + "=true; production must use https"}
		}
		return nil
	default:
		return &ConfigError{Message: "blob: " + EnvEndpoint + " scheme is not supported; use https"}
	}
}

// AssertProductionReady is the startup-time check Engine runs before
// serving traffic. It refuses the local/test insecure opt-in unless
// the process is explicitly running in local/test mode. Returns nil
// on production-safe configurations.
func (c *Config) AssertProductionReady() error {
	if c == nil {
		return errors.New("blob: configuration is nil")
	}
	if c.AllowInsecure && !c.LocalTestMode {
		return &ConfigError{Message: "blob: " + EnvAllowInsecure + " requires " + EnvLocalTestMode + "=true; production must reject insecure transport"}
	}
	return nil
}
