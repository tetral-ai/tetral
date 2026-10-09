package workload

import "strings"

const (
	EnvDeploymentEnvironment     = "TETRAL_DEPLOYMENT_ENVIRONMENT"
	EnvServiceVersion            = "TETRAL_SERVICE_VERSION"
	DefaultDeploymentEnvironment = "local"
	DefaultServiceVersion        = "unknown"
)

// ResourceConfig owns Go process metadata. It is read at boot, never reloaded.
type ResourceConfig struct{ DeploymentEnvironment, ServiceVersion string }

// ResourceConfigFromEnv preserves the usual exact-empty default policy.
func ResourceConfigFromEnv(getenv func(string) string) ResourceConfig {
	return ResourceConfigFromEnvWithTrimPolicy(getenv, false)
}

// ResourceConfigFromEnvWithTrimPolicy preserves callers that trim all metadata before defaulting.
func ResourceConfigFromEnvWithTrimPolicy(getenv func(string) string, trimSpace bool) ResourceConfig {
	cfg := ResourceConfig{DefaultDeploymentEnvironment, DefaultServiceVersion}
	if getenv == nil {
		return cfg
	}
	read := func(key, fallback string) string {
		value := getenv(key)
		if trimSpace {
			value = strings.TrimSpace(value)
		}
		if value == "" {
			return fallback
		}
		return value
	}
	cfg.DeploymentEnvironment = read(EnvDeploymentEnvironment, cfg.DeploymentEnvironment)
	cfg.ServiceVersion = read(EnvServiceVersion, cfg.ServiceVersion)
	return cfg
}
