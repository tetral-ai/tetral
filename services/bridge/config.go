package agentruntimebridge

import (
	"strconv"
	"strings"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/workload"
)

const ServiceNameBridgeAPI = "bridge"

const EnvBridgeAPIHTTPAddress = "TETRAL_BRIDGE_API_HTTP_ADDR"

const EnvBridgeAPIGRPCAddress = "TETRAL_BRIDGE_API_GRPC_ADDR"

const EnvRuntimeBindingTokenHMACKey = "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY" //nolint:gosec // G101: env-var name, not a credential value

const EnvBridgeMCPConnectorGRPCAddr = "TETRAL_BRIDGE_MCP_CONNECTOR_GRPC_ADDR"

const EnvBridgeGatewayTokenPath = "TETRAL_BRIDGE_GATEWAY_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.

const EnvProviderRescheduleBudget = "TETRAL_PROVIDER_RESCHEDULE_BUDGET"

const EnvCompactionRescheduleBudget = "TETRAL_COMPACTION_RESCHEDULE_BUDGET"

const EnvDatabaseURL = "TETRAL_DATABASE_URL"

const defaultProviderRescheduleBudget = int64(3)

const defaultCompactionRescheduleBudget = int64(2)

type Env interface {
	Getenv(string) string
}

type BridgeAPIConfig struct {
	MCPConnectorGRPCAddress    string
	GatewayTokenPath           string
	ProviderRescheduleBudget   int64
	CompactionRescheduleBudget int64
}

func BridgeAPIConfigFromEnv(env Env) (BridgeAPIConfig, error) {
	if env == nil {
		return BridgeAPIConfig{}, workload.NewConfigError("environment is required")
	}
	cfg := BridgeAPIConfig{
		MCPConnectorGRPCAddress:    strings.TrimSpace(env.Getenv(EnvBridgeMCPConnectorGRPCAddr)),
		GatewayTokenPath:           strings.TrimSpace(env.Getenv(EnvBridgeGatewayTokenPath)),
		ProviderRescheduleBudget:   defaultProviderRescheduleBudget,
		CompactionRescheduleBudget: defaultCompactionRescheduleBudget,
	}
	if cfg.MCPConnectorGRPCAddress == "" {
		return BridgeAPIConfig{}, workload.NewConfigError(EnvBridgeMCPConnectorGRPCAddr + " is required")
	}
	if cfg.GatewayTokenPath == "" {
		return BridgeAPIConfig{}, workload.NewConfigError(EnvBridgeGatewayTokenPath + " is required")
	}
	poolConfig, poolErr := dbconnect.PoolConfigFromEnv(env.Getenv)
	if poolErr != nil {
		return BridgeAPIConfig{}, workload.NewConfigError("database pool config invalid: " + poolErr.Error())
	}
	// The result listener holds one connection for its lifetime; RPCs must
	// still be able to borrow a connection to read and settle durable results.
	if poolConfig.MaxOpenConns < 2 {
		return BridgeAPIConfig{}, workload.NewConfigError(dbconnect.EnvDBMaxOpenConns + " must be at least 2 for the bridge API (one connection is reserved for result notifications)")
	}
	var err error
	if cfg.ProviderRescheduleBudget, err = parseBoundedRescheduleBudget(env.Getenv(EnvProviderRescheduleBudget), EnvProviderRescheduleBudget, defaultProviderRescheduleBudget); err != nil {
		return BridgeAPIConfig{}, err
	}
	if cfg.CompactionRescheduleBudget, err = parseBoundedRescheduleBudget(env.Getenv(EnvCompactionRescheduleBudget), EnvCompactionRescheduleBudget, defaultCompactionRescheduleBudget); err != nil {
		return BridgeAPIConfig{}, err
	}
	return cfg, nil
}

func parseBoundedRescheduleBudget(raw string, key string, fallback int64) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || value < 0 || value > 10 {
		return 0, workload.NewConfigError(key + " must be an integer between 0 and 10")
	}
	return value, nil
}

func RuntimeBindingTokenHMACKeyFromEnv(env Env) ([]byte, error) {
	if env == nil {
		return nil, workload.NewConfigError("environment is required")
	}
	key := strings.TrimSpace(env.Getenv(EnvRuntimeBindingTokenHMACKey))
	if len(key) < 32 {
		return nil, workload.NewConfigError(EnvRuntimeBindingTokenHMACKey + " is required")
	}
	return []byte(key), nil
}
