package tetralauth

import (
	"net/url"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/transportsecurity"
	"github.com/tetral-ai/tetral/internal/workload"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	DefaultInternalPrincipalTTL       = 60 * time.Second
	EnvJWKSCacheTTLSeconds            = "TETRAL_AUTH_JWKS_CACHE_TTL_SECONDS"
	EnvExchangeBodyBytes              = "TETRAL_AUTH_EXCHANGE_BODY_BYTES"
	EnvExchangeConcurrency            = "TETRAL_AUTH_EXCHANGE_CONCURRENCY"
	EnvExchangeRequestsPerMinute      = "TETRAL_AUTH_EXCHANGE_REQUESTS_PER_MINUTE"
	EnvExchangeBodyReadTimeoutMS      = "TETRAL_AUTH_EXCHANGE_BODY_READ_TIMEOUT_MS"
	EnvGRPCAddress                    = "TETRAL_AUTH_GRPC_ADDR"
	EnvGRPCTransport                  = "TETRAL_AUTH_GRPC_TRANSPORT"
	EnvGRPCTLSCAPath                  = "TETRAL_AUTH_GRPC_TLS_CA_PATH"
	EnvGRPCTLSCertPath                = "TETRAL_AUTH_GRPC_TLS_CERT_PATH"
	EnvGRPCTLSKeyPath                 = "TETRAL_AUTH_GRPC_TLS_KEY_PATH"
	EnvGRPCTLSEdgeClientURI           = "TETRAL_AUTH_GRPC_TLS_EDGE_CLIENT_URI"
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
	HTTPTransport                                                        transportsecurity.HTTPConfig
	JWKSCacheTTL                                                         time.Duration
	ExchangeLimits                                                       ExchangeLimits
	GRPCAddress                                                          string
	GRPCTransport                                                        string
	GRPCTLSCAPath, GRPCTLSCertPath, GRPCTLSKeyPath, GRPCTLSEdgeClientURI string
	HTTPAddress                                                          string
	MetricsAddress                                                       string
	DeploymentEnvironment                                                string
	ServiceVersion                                                       string
	BootstrapAPIKey                                                      string
	BootstrapWorkspaceID                                                 workspace.ID
	InternalPrincipalPrivateKeyB64                                       string
	InternalPrincipalTTL                                                 time.Duration
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
	grpcAddress := env.Getenv(EnvGRPCAddress)
	if grpcAddress == "" {
		grpcAddress = ":9095"
	}
	if grpcAddress == httpAddress || grpcAddress == metricsAddress {
		return Config{}, workload.NewConfigError(EnvGRPCAddress + " must be separate from HTTP and metrics listeners")
	}
	transport := env.Getenv(EnvGRPCTransport)
	if transport == "" {
		transport = "plaintext"
	}
	if transport != "plaintext" && transport != "native-mtls" {
		return Config{}, workload.NewConfigError(EnvGRPCTransport + " must be plaintext or native-mtls")
	}
	ca, cert, key, peer := env.Getenv(EnvGRPCTLSCAPath), env.Getenv(EnvGRPCTLSCertPath), env.Getenv(EnvGRPCTLSKeyPath), env.Getenv(EnvGRPCTLSEdgeClientURI)
	if transport == "native-mtls" {
		uri, err := url.Parse(peer)
		if ca == "" || cert == "" || key == "" || err != nil || uri.Scheme != "spiffe" || uri.Host == "" || uri.Path != "/ns/envoy-gateway-system/sa/tetral-public-edge" || uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" {
			return Config{}, workload.NewConfigError("native-mtls requires " + EnvGRPCTLSEdgeClientURI + " to be spiffe://<trust-domain>/ns/envoy-gateway-system/sa/tetral-public-edge with complete " + EnvGRPCTLSCAPath + ", " + EnvGRPCTLSCertPath + " and " + EnvGRPCTLSKeyPath)
		}
	} else if ca != "" || cert != "" || key != "" || peer != "" {
		return Config{}, workload.NewConfigError("plaintext Auth gRPC transport must not configure native TLS material")
	}
	httpTransport, err := transportsecurity.HTTPConfigFromEnv(env.Getenv)
	if err != nil {
		return Config{}, workload.NewConfigError(err.Error())
	}
	resource := workload.ResourceConfigFromEnv(env.Getenv)
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
	ttl := DefaultInternalPrincipalTTL
	if raw := env.Getenv(EnvInternalPrincipalTTLSeconds); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 || seconds > int(auth.MaxInternalPrincipalTTL.Seconds()) {
			return Config{}, workload.NewConfigError(EnvInternalPrincipalTTLSeconds + " must be a positive integer at most 300")
		}
		ttl = time.Duration(seconds) * time.Second
	}
	cacheTTL := 600
	limits := DefaultExchangeLimits()
	readMS := int(limits.BodyReadTimeout / time.Millisecond)
	for _, setting := range []struct {
		name             string
		target           *int
		minimum, maximum int
	}{
		{EnvJWKSCacheTTLSeconds, &cacheTTL, 1, 600},
		{EnvExchangeBodyBytes, &limits.BodyBytes, 1024, 65536},
		{EnvExchangeConcurrency, &limits.ConcurrentRequests, 1, 32},
		{EnvExchangeRequestsPerMinute, &limits.RequestsPerMinute, 1, 6000},
		{EnvExchangeBodyReadTimeoutMS, &readMS, 100, 5000},
	} {
		if raw := env.Getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < setting.minimum || value > setting.maximum {
				return Config{}, workload.NewConfigError(setting.name + " is outside its supported integer range")
			}
			*setting.target = value
		}
	}
	limits.BodyReadTimeout = time.Duration(readMS) * time.Millisecond

	return Config{
		HTTPTransport:  httpTransport,
		JWKSCacheTTL:   time.Duration(cacheTTL) * time.Second,
		ExchangeLimits: limits,
		GRPCAddress:    grpcAddress, GRPCTransport: transport, GRPCTLSCAPath: ca, GRPCTLSCertPath: cert, GRPCTLSKeyPath: key, GRPCTLSEdgeClientURI: peer,
		HTTPAddress:                    httpAddress,
		MetricsAddress:                 metricsAddress,
		DeploymentEnvironment:          resource.DeploymentEnvironment,
		ServiceVersion:                 resource.ServiceVersion,
		BootstrapAPIKey:                bootstrapAPIKey,
		BootstrapWorkspaceID:           workspace.ID(bootstrapWorkspaceID),
		InternalPrincipalPrivateKeyB64: privateKey,
		InternalPrincipalTTL:           ttl,
	}, nil
}
