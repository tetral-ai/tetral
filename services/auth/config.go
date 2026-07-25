package tetralauth

import (
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	EnvHTTPAddress                    = "TETRAL_AUTH_HTTP_ADDR"
	EnvMetricsAddress                 = "TETRAL_AUTH_METRICS_ADDR"
	EnvBootstrapAPIKey                = "ENGINE_API_KEY" //nolint:gosec // Env-var name, not an API key value.
	EnvBootstrapWorkspaceID           = "ENGINE_BOOTSTRAP_WORKSPACE_ID"
	EnvInternalPrincipalPrivateKeyB64 = "TETRAL_AUTH_INTERNAL_PRINCIPAL_PRIVATE_KEY_B64"
	EnvInternalPrincipalTTLSeconds    = "TETRAL_AUTH_INTERNAL_PRINCIPAL_TTL_SECONDS"
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	HTTPAddress                    string
	MetricsAddress                 string
	DeploymentEnvironment          string
	ServiceVersion                 string
	BootstrapAPIKey                string
	BootstrapWorkspaceID           workspace.ID
	InternalPrincipalPrivateKeyB64 string
	InternalPrincipalTTL           time.Duration
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment reader is required")
	}
	httpAddress := env.Getenv(EnvHTTPAddress)
	if httpAddress == "" {
		httpAddress = ":8080"
	}
	metricsAddress := env.Getenv(EnvMetricsAddress)
	if metricsAddress == "" {
		metricsAddress = ":8081"
	}
	if metricsAddress == httpAddress {
		return Config{}, workload.NewConfigError(EnvMetricsAddress + " must not equal " + EnvHTTPAddress)
	}
	deploymentEnvironment := env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT")
	if deploymentEnvironment == "" {
		deploymentEnvironment = "local"
	}
	serviceVersion := env.Getenv("TETRAL_SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	bootstrapWorkspaceID := env.Getenv(EnvBootstrapWorkspaceID)
	if bootstrapWorkspaceID == "" {
		return Config{}, workload.NewConfigError(EnvBootstrapWorkspaceID + " is required")
	}
	bootstrapAPIKey := env.Getenv(EnvBootstrapAPIKey)
	if err := auth.ValidateBootstrapKey(bootstrapAPIKey); err != nil {
		return Config{}, workload.NewConfigError(err.Error())
	}
	privateKey := env.Getenv(EnvInternalPrincipalPrivateKeyB64)
	if privateKey == "" {
		return Config{}, workload.NewConfigError(EnvInternalPrincipalPrivateKeyB64 + " is required")
	}
	if _, err := auth.DecodeEd25519PrivateKey(privateKey); err != nil {
		return Config{}, workload.NewConfigError(err.Error())
	}
	ttl := 60 * time.Second
	if raw := env.Getenv(EnvInternalPrincipalTTLSeconds); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 {
			return Config{}, workload.NewConfigError(EnvInternalPrincipalTTLSeconds + " must be a positive integer")
		}
		ttl = time.Duration(seconds) * time.Second
	}
	return Config{
		HTTPAddress:                    httpAddress,
		MetricsAddress:                 metricsAddress,
		DeploymentEnvironment:          deploymentEnvironment,
		ServiceVersion:                 serviceVersion,
		BootstrapAPIKey:                bootstrapAPIKey,
		BootstrapWorkspaceID:           workspace.ID(bootstrapWorkspaceID),
		InternalPrincipalPrivateKeyB64: privateKey,
		InternalPrincipalTTL:           ttl,
	}, nil
}
