package agentruntimebridge

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	ServiceNameBridgeAPI = "bridge"
	ServiceNameJobRunner = "bridge-job-runner"

	EnvBridgeAPIHTTPAddress       = "TETRAL_BRIDGE_API_HTTP_ADDR"
	EnvBridgeAPIGRPCAddress       = "TETRAL_BRIDGE_API_GRPC_ADDR"
	EnvRuntimeBindingTokenHMACKey = "TETRAL_RUNTIME_BINDING_TOKEN_HMAC_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvBridgeMCPConnectorGRPCAddr = "TETRAL_BRIDGE_MCP_CONNECTOR_GRPC_ADDR"
	EnvBridgeGatewayTokenPath     = "TETRAL_BRIDGE_GATEWAY_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.
	EnvProviderRescheduleBudget   = "TETRAL_PROVIDER_RESCHEDULE_BUDGET"
	EnvCompactionRescheduleBudget = "TETRAL_COMPACTION_RESCHEDULE_BUDGET"

	EnvJobRunnerHTTPAddress                = "TETRAL_BRIDGE_JOB_RUNNER_HTTP_ADDR"
	EnvQueueGRPCAddress                    = "TETRAL_QUEUE_GRPC_ADDR"
	EnvJobRunnerLeaseOwner                 = "TETRAL_BRIDGE_JOB_RUNNER_LEASE_OWNER"
	EnvJobRunnerLeaseDurationMS            = "TETRAL_BRIDGE_JOB_RUNNER_LEASE_DURATION_MS"
	EnvJobRunnerHeartbeatIntervalMS        = "TETRAL_BRIDGE_JOB_RUNNER_HEARTBEAT_INTERVAL_MS"
	EnvJobRunnerMaxJobs                    = "TETRAL_BRIDGE_JOB_RUNNER_MAX_JOBS"
	EnvJobRunnerPollIntervalMS             = "TETRAL_BRIDGE_JOB_RUNNER_POLL_INTERVAL_MS"
	EnvJobRunnerBridgeAPIGRPCAddress       = "TETRAL_BRIDGE_JOB_RUNNER_BRIDGE_API_GRPC_ADDR"
	EnvJobRunnerBridgeAPITokenPath         = "TETRAL_BRIDGE_JOB_RUNNER_BRIDGE_API_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.
	EnvJobRunnerMCPConnectorGRPCAddress    = "TETRAL_BRIDGE_JOB_RUNNER_MCP_CONNECTOR_GRPC_ADDR"
	EnvJobRunnerGatewayTokenPath           = "TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.
	EnvSandboxServiceGRPCAddress           = "TETRAL_SANDBOX_GRPC_ADDR"
	EnvSandboxServiceTokenPath             = "TETRAL_BRIDGE_SANDBOX_TOKEN_PATH"    //nolint:gosec // Env-var name, not a token value.
	EnvResourceCredRefreshMargin           = "TETRAL_RESOURCE_CRED_REFRESH_MARGIN" //nolint:gosec // Env-var name, not a credential value.
	EnvSandboxStatusFreshnessWindow        = "TETRAL_SANDBOX_STATUS_FRESHNESS_WINDOW"
	EnvMemoryProjectionPushTimeout         = "TETRAL_MEMORY_PROJECTION_PUSH_TIMEOUT"
	EnvDatabaseURL                         = "TETRAL_DATABASE_URL"
	EnvKubernetesNamespace                 = "TETRAL_KUBERNETES_NAMESPACE"
	EnvAgentRuntimeLabelSelector           = "TETRAL_AGENT_RUNTIME_LABEL_SELECTOR"
	EnvAgentRuntimeGRPCPort                = "TETRAL_AGENT_RUNTIME_GRPC_PORT"
	EnvRuntimePodServiceTokenPath          = "TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.
	EnvSandboxDriver                       = "TETRAL_SANDBOX_DRIVER"
	EnvDaytonaAPIURL                       = "DAYTONA_API_URL"
	EnvDaytonaTarget                       = "DAYTONA_TARGET"
	EnvDaytonaAPIKey                       = "DAYTONA_API_KEY" //nolint:gosec // G101: env-var name, not a credential value
	defaultJobRunnerLeaseDuration          = 30 * time.Second
	defaultJobRunnerMaxJobs                = 1
	defaultRuntimeInboxRepairBatch         = 32
	defaultJobRunnerPollInterval           = time.Second
	defaultJobRunnerHTTPAddress            = ":8081"
	defaultJobRunnerBridgeAPIAddr          = "127.0.0.1:9090"
	defaultAgentRuntimeGRPCPort            = 9090
	defaultSandboxStatusFreshness          = time.Minute
	defaultResourceCredentialRefreshMargin = 30 * time.Minute
	defaultMemoryProjectionPushTimeout     = 30 * time.Second
	defaultProviderRescheduleBudget        = int64(3)
	defaultCompactionRescheduleBudget      = int64(2)
)

type Env interface {
	Getenv(string) string
}

type SandboxDriverConfig struct {
	Driver        string
	DaytonaAPIURL string
	DaytonaTarget string
	DaytonaAPIKey string
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

func SandboxDriverConfigFromEnv(env Env) (SandboxDriverConfig, error) {
	if env == nil {
		return SandboxDriverConfig{}, workload.NewConfigError("environment is required")
	}
	cfg := SandboxDriverConfig{
		Driver:        strings.TrimSpace(env.Getenv(EnvSandboxDriver)),
		DaytonaAPIURL: strings.TrimSpace(env.Getenv(EnvDaytonaAPIURL)),
		DaytonaTarget: strings.TrimSpace(env.Getenv(EnvDaytonaTarget)),
		DaytonaAPIKey: strings.TrimSpace(env.Getenv(EnvDaytonaAPIKey)),
	}
	if cfg.Driver == "" {
		return SandboxDriverConfig{}, workload.NewConfigError(EnvSandboxDriver + " is required")
	}
	if cfg.Driver != "daytona" {
		return SandboxDriverConfig{}, workload.NewConfigError(fmt.Sprintf("%s must be daytona", EnvSandboxDriver))
	}
	if cfg.DaytonaAPIURL == "" {
		return SandboxDriverConfig{}, workload.NewConfigError(EnvDaytonaAPIURL + " is required")
	}
	if cfg.DaytonaAPIKey == "" {
		return SandboxDriverConfig{}, workload.NewConfigError(EnvDaytonaAPIKey + " is required")
	}
	return cfg, nil
}

func ResourceCredentialRefreshMarginFromEnv(env Env) (time.Duration, error) {
	if env == nil {
		return 0, workload.NewConfigError("environment is required")
	}
	raw := strings.TrimSpace(env.Getenv(EnvResourceCredRefreshMargin))
	if raw == "" {
		return defaultResourceCredentialRefreshMargin, nil
	}
	return parsePositiveDuration(raw, EnvResourceCredRefreshMargin)
}

func SandboxStatusFreshnessWindowFromEnv(env Env) (time.Duration, error) {
	if env == nil {
		return 0, workload.NewConfigError("environment is required")
	}
	raw := strings.TrimSpace(env.Getenv(EnvSandboxStatusFreshnessWindow))
	if raw == "" {
		return defaultSandboxStatusFreshness, nil
	}
	return parsePositiveDuration(raw, EnvSandboxStatusFreshnessWindow)
}

func MemoryProjectionPushTimeoutFromEnv(env Env) (time.Duration, error) {
	if env == nil {
		return 0, workload.NewConfigError("environment is required")
	}
	raw := strings.TrimSpace(env.Getenv(EnvMemoryProjectionPushTimeout))
	if raw == "" {
		return defaultMemoryProjectionPushTimeout, nil
	}
	return parsePositiveDuration(raw, EnvMemoryProjectionPushTimeout)
}

type JobRunnerConfig struct {
	HTTPAddress                     string
	QueueGRPCAddress                string
	BridgeAPIGRPCAddress            string
	LeaseOwner                      string
	LeaseDuration                   time.Duration
	HeartbeatInterval               time.Duration
	MaxJobs                         int
	PollInterval                    time.Duration
	DeploymentEnvironment           string
	ServiceVersion                  string
	DatabaseURL                     string
	KubernetesNamespace             string
	AgentRuntimeLabelSelector       string
	AgentRuntimeGRPCPort            int
	RuntimePodTokenPath             string
	BridgeAPITokenPath              string
	MCPConnectorGRPCAddress         string
	GatewayTokenPath                string
	SandboxServiceGRPCAddress       string
	SandboxServiceTokenPath         string
	SandboxStatusFreshnessWindow    time.Duration
	ResourceCredentialRefreshMargin time.Duration
}

func JobRunnerConfigFromEnv(env Env) (JobRunnerConfig, error) {
	if env == nil {
		return JobRunnerConfig{}, workload.NewConfigError("environment is required")
	}
	cfg := JobRunnerConfig{
		HTTPAddress:           valueOrDefault(env.Getenv(EnvJobRunnerHTTPAddress), defaultJobRunnerHTTPAddress),
		QueueGRPCAddress:      strings.TrimSpace(env.Getenv(EnvQueueGRPCAddress)),
		BridgeAPIGRPCAddress:  valueOrDefault(strings.TrimSpace(env.Getenv(EnvJobRunnerBridgeAPIGRPCAddress)), defaultJobRunnerBridgeAPIAddr),
		LeaseOwner:            valueOrDefault(env.Getenv(EnvJobRunnerLeaseOwner), ServiceNameJobRunner),
		LeaseDuration:         defaultJobRunnerLeaseDuration,
		MaxJobs:               defaultJobRunnerMaxJobs,
		PollInterval:          defaultJobRunnerPollInterval,
		DeploymentEnvironment: valueOrDefault(env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT"), "local"),
		ServiceVersion:        valueOrDefault(env.Getenv("TETRAL_SERVICE_VERSION"), "unknown"),
		DatabaseURL:           strings.TrimSpace(env.Getenv(EnvDatabaseURL)),
		KubernetesNamespace:   strings.TrimSpace(env.Getenv(EnvKubernetesNamespace)),
		AgentRuntimeLabelSelector: strings.TrimSpace(
			env.Getenv(EnvAgentRuntimeLabelSelector),
		),
		AgentRuntimeGRPCPort:            defaultAgentRuntimeGRPCPort,
		RuntimePodTokenPath:             strings.TrimSpace(env.Getenv(EnvRuntimePodServiceTokenPath)),
		BridgeAPITokenPath:              strings.TrimSpace(env.Getenv(EnvJobRunnerBridgeAPITokenPath)),
		MCPConnectorGRPCAddress:         strings.TrimSpace(env.Getenv(EnvJobRunnerMCPConnectorGRPCAddress)),
		GatewayTokenPath:                strings.TrimSpace(env.Getenv(EnvJobRunnerGatewayTokenPath)),
		SandboxStatusFreshnessWindow:    defaultSandboxStatusFreshness,
		ResourceCredentialRefreshMargin: defaultResourceCredentialRefreshMargin,
		SandboxServiceGRPCAddress: strings.TrimSpace(
			env.Getenv(EnvSandboxServiceGRPCAddress),
		),
		SandboxServiceTokenPath: strings.TrimSpace(env.Getenv(EnvSandboxServiceTokenPath)),
	}
	if cfg.QueueGRPCAddress == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvQueueGRPCAddress + " is required")
	}
	if cfg.DatabaseURL == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvDatabaseURL + " is required")
	}
	if cfg.KubernetesNamespace == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvKubernetesNamespace + " is required")
	}
	if cfg.AgentRuntimeLabelSelector == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvAgentRuntimeLabelSelector + " is required")
	}
	if cfg.RuntimePodTokenPath == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvRuntimePodServiceTokenPath + " is required")
	}
	if cfg.SandboxServiceGRPCAddress == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvSandboxServiceGRPCAddress + " is required")
	}
	if cfg.SandboxServiceTokenPath == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvSandboxServiceTokenPath + " is required")
	}
	if cfg.MCPConnectorGRPCAddress == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerMCPConnectorGRPCAddress + " is required")
	}
	if cfg.GatewayTokenPath == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerGatewayTokenPath + " is required")
	}
	var err error
	if raw := env.Getenv(EnvJobRunnerLeaseDurationMS); raw != "" {
		cfg.LeaseDuration, err = parsePositiveMilliseconds(raw, EnvJobRunnerLeaseDurationMS)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	if raw := env.Getenv(EnvJobRunnerHeartbeatIntervalMS); raw != "" {
		cfg.HeartbeatInterval, err = parsePositiveMilliseconds(raw, EnvJobRunnerHeartbeatIntervalMS)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	} else {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	if cfg.HeartbeatInterval >= cfg.LeaseDuration {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerHeartbeatIntervalMS + " must be less than " + EnvJobRunnerLeaseDurationMS)
	}
	if raw := env.Getenv(EnvJobRunnerPollIntervalMS); raw != "" {
		cfg.PollInterval, err = parsePositiveMilliseconds(raw, EnvJobRunnerPollIntervalMS)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	if raw := env.Getenv(EnvJobRunnerMaxJobs); raw != "" {
		cfg.MaxJobs, err = parsePositiveInt(raw, EnvJobRunnerMaxJobs)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	if err := queue.ValidateLeaseBatchSize(cfg.MaxJobs); err != nil {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerMaxJobs + " " + err.Error())
	}
	if err := queue.ValidateLeaseOwner(cfg.LeaseOwner); err != nil {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerLeaseOwner + " " + err.Error())
	}
	if raw := env.Getenv(EnvAgentRuntimeGRPCPort); raw != "" {
		cfg.AgentRuntimeGRPCPort, err = parsePositiveInt(raw, EnvAgentRuntimeGRPCPort)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	if raw := strings.TrimSpace(env.Getenv(EnvResourceCredRefreshMargin)); raw != "" {
		cfg.ResourceCredentialRefreshMargin, err = parsePositiveDuration(raw, EnvResourceCredRefreshMargin)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	if raw := strings.TrimSpace(env.Getenv(EnvSandboxStatusFreshnessWindow)); raw != "" {
		cfg.SandboxStatusFreshnessWindow, err = parsePositiveDuration(raw, EnvSandboxStatusFreshnessWindow)
		if err != nil {
			return JobRunnerConfig{}, err
		}
	}
	return cfg, nil
}

func parsePositiveDuration(raw string, envName string) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive duration", envName))
	}
	return value, nil
}

func parsePositiveMilliseconds(raw string, envName string) (time.Duration, error) {
	value, err := parsePositiveInt(raw, envName)
	if err != nil {
		return 0, err
	}
	if int64(value) > int64(math.MaxInt64)/int64(time.Millisecond) {
		return 0, workload.NewConfigError(envName + " is too large")
	}
	return time.Duration(value) * time.Millisecond, nil
}

func parsePositiveInt(raw string, envName string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", envName))
	}
	return value, nil
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
