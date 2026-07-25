package tetralapi

import "github.com/tetral-ai/tetral/internal/workload"

const (
	EnvHTTPAddress           = "TETRAL_API_HTTP_ADDR"
	EnvMetricsAddress        = "TETRAL_API_METRICS_ADDR"
	EnvLegacyPort            = "ENGINE_PORT"
	EnvVaultKey              = "ENGINE_VAULT_KEY"
	EnvDataDir               = "ENGINE_DATA_DIR"
	EnvDeploymentEnvironment = "TETRAL_DEPLOYMENT_ENVIRONMENT"
	EnvServiceVersion        = "TETRAL_SERVICE_VERSION"

	defaultDataDir     = "/var/tetral"
	defaultPort        = "8080"
	defaultMetricsPort = "8081"
)

// Env reads process configuration from the deployment environment.
type Env interface {
	Getenv(string) string
}

// Config is the service-local api workload configuration.
type Config struct {
	ListenAddress         string
	MetricsAddress        string
	VaultKey              string
	DataDir               string
	DeploymentEnvironment string
	ServiceVersion        string
}

// ConfigFromEnv loads only process and workload-level configuration. Domain
// packages receive typed config from wiring.go rather than reading env directly.
func ConfigFromEnv(env Env) (Config, error) {
	listenAddress := env.Getenv(EnvHTTPAddress)
	if listenAddress == "" {
		port := env.Getenv(EnvLegacyPort)
		if port == "" {
			port = defaultPort
		}
		listenAddress = ":" + port
	}
	metricsAddress := env.Getenv(EnvMetricsAddress)
	if metricsAddress == "" {
		metricsAddress = ":" + defaultMetricsPort
	}
	if metricsAddress == listenAddress {
		return Config{}, workload.NewConfigError(EnvMetricsAddress + " must not equal " + EnvHTTPAddress)
	}
	dataDir := env.Getenv(EnvDataDir)
	if dataDir == "" {
		dataDir = defaultDataDir
	}
	deploymentEnvironment := env.Getenv(EnvDeploymentEnvironment)
	if deploymentEnvironment == "" {
		deploymentEnvironment = "local"
	}
	serviceVersion := env.Getenv(EnvServiceVersion)
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	return Config{
		ListenAddress:         listenAddress,
		MetricsAddress:        metricsAddress,
		VaultKey:              env.Getenv(EnvVaultKey),
		DataDir:               dataDir,
		DeploymentEnvironment: deploymentEnvironment,
		ServiceVersion:        serviceVersion,
	}, nil
}
