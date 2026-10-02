package jobrunner

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
)

const ServiceNameJobRunner = "job-runner"

const defaultJobRunnerLeaseOwner = "bridge-job-runner"

const EnvJobRunnerHTTPAddress = "TETRAL_BRIDGE_JOB_RUNNER_HTTP_ADDR"

const EnvQueueGRPCAddress = "TETRAL_QUEUE_GRPC_ADDR"

const EnvJobRunnerLeaseOwner = "TETRAL_BRIDGE_JOB_RUNNER_LEASE_OWNER"

const EnvJobRunnerLeaseDurationMS = "TETRAL_BRIDGE_JOB_RUNNER_LEASE_DURATION_MS"

const EnvJobRunnerHeartbeatIntervalMS = "TETRAL_BRIDGE_JOB_RUNNER_HEARTBEAT_INTERVAL_MS"

const EnvJobRunnerMaxJobs = "TETRAL_BRIDGE_JOB_RUNNER_MAX_JOBS"

const EnvJobRunnerPollIntervalMS = "TETRAL_BRIDGE_JOB_RUNNER_POLL_INTERVAL_MS"

const EnvJobRunnerMCPConnectorGRPCAddress = "TETRAL_BRIDGE_JOB_RUNNER_MCP_CONNECTOR_GRPC_ADDR"

const EnvJobRunnerGatewayTokenPath = "TETRAL_BRIDGE_JOB_RUNNER_GATEWAY_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.

const EnvDatabaseURL = "TETRAL_DATABASE_URL"

const EnvKubernetesNamespace = "TETRAL_KUBERNETES_NAMESPACE"

const EnvAgentRuntimeLabelSelector = "TETRAL_AGENT_RUNTIME_LABEL_SELECTOR"

const EnvAgentRuntimeGRPCPort = "TETRAL_AGENT_RUNTIME_GRPC_PORT"

const EnvRuntimePodServiceTokenPath = "TETRAL_BRIDGE_RUNTIME_POD_TOKEN_PATH" //nolint:gosec // Env-var name, not a token value.

const defaultJobRunnerLeaseDuration = 30 * time.Second

const defaultJobRunnerMaxJobs = 8

const defaultJobRunnerPollInterval = time.Second

const defaultJobRunnerHTTPAddress = ":8081"

const defaultAgentRuntimeGRPCPort = 9090

type Env interface {
	Getenv(string) string
}

type JobRunnerConfig struct {
	HTTPAddress               string
	QueueGRPCAddress          string
	LeaseOwner                string
	LeaseDuration             time.Duration
	HeartbeatInterval         time.Duration
	MaxJobs                   int
	PollInterval              time.Duration
	DeploymentEnvironment     string
	ServiceVersion            string
	DatabaseURL               string
	KubernetesNamespace       string
	AgentRuntimeLabelSelector string
	AgentRuntimeGRPCPort      int
	RuntimePodTokenPath       string
	MCPConnectorGRPCAddress   string
	GatewayTokenPath          string
}

func JobRunnerConfigFromEnv(env Env) (JobRunnerConfig, error) {
	if env == nil {
		return JobRunnerConfig{}, workload.NewConfigError("environment is required")
	}
	resource := workload.ResourceConfigFromEnv(env.Getenv)
	cfg := JobRunnerConfig{
		HTTPAddress:           valueOrDefault(env.Getenv(EnvJobRunnerHTTPAddress), defaultJobRunnerHTTPAddress),
		QueueGRPCAddress:      strings.TrimSpace(env.Getenv(EnvQueueGRPCAddress)),
		LeaseOwner:            valueOrDefault(env.Getenv(EnvJobRunnerLeaseOwner), defaultJobRunnerLeaseOwner),
		LeaseDuration:         defaultJobRunnerLeaseDuration,
		MaxJobs:               defaultJobRunnerMaxJobs,
		PollInterval:          defaultJobRunnerPollInterval,
		DeploymentEnvironment: resource.DeploymentEnvironment,
		ServiceVersion:        resource.ServiceVersion,
		DatabaseURL:           strings.TrimSpace(env.Getenv(EnvDatabaseURL)),
		KubernetesNamespace:   strings.TrimSpace(env.Getenv(EnvKubernetesNamespace)),
		AgentRuntimeLabelSelector: strings.TrimSpace(
			env.Getenv(EnvAgentRuntimeLabelSelector),
		),
		AgentRuntimeGRPCPort:    defaultAgentRuntimeGRPCPort,
		RuntimePodTokenPath:     strings.TrimSpace(env.Getenv(EnvRuntimePodServiceTokenPath)),
		MCPConnectorGRPCAddress: strings.TrimSpace(env.Getenv(EnvJobRunnerMCPConnectorGRPCAddress)),
		GatewayTokenPath:        strings.TrimSpace(env.Getenv(EnvJobRunnerGatewayTokenPath)),
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
	if cfg.MCPConnectorGRPCAddress == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerMCPConnectorGRPCAddress + " is required")
	}
	if cfg.GatewayTokenPath == "" {
		return JobRunnerConfig{}, workload.NewConfigError(EnvJobRunnerGatewayTokenPath + " is required")
	}
	poolConfig, poolErr := dbconnect.PoolConfigFromEnv(env.Getenv)
	if poolErr != nil {
		return JobRunnerConfig{}, workload.NewConfigError("database pool config invalid: " + poolErr.Error())
	}
	if poolConfig.MaxOpenConns < 2 {
		return JobRunnerConfig{}, workload.NewConfigError(dbconnect.EnvDBMaxOpenConns + " must be at least 2 for the bridge job runner")
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
	return cfg, nil
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
