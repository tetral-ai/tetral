package tetralqueue

import (
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

// The retry/backoff policy knobs in this block -- EnvRetryBaseMS, EnvRetryCapMS,
// and EnvRetryMaxAttempts -- are each read by ConfigFromEnv as a positive integer,
// using the paired default* constant as the blank fallback; zero and negative values
// are startup errors, and EnvRetryCapMS is additionally rejected when it resolves
// below EnvRetryBaseMS. They populate the RetryBaseDelay/RetryMaxDelay/RetryMaxAttempts
// Config fields that cmd/tetral-queue feeds into queue.RetryPolicy: base and cap bound
// the capped-exponential-backoff floor and ceiling used per attempt, and max-attempts
// is the service-default attempt budget. A queue_jobs row stored with max_attempts = 0
// means "unset" and is resolved to this max-attempts default downstream at both the
// lease projection and the dead-letter comparison; a positive per-job value overrides
// it there instead.
// UPDATE-WITH: cmd/tetral-queue/main.go (RetryPolicy wiring);
// internal/queue/postgresql_store.go (leaseCandidate max_attempts projection, Retry
// dead-letter comparison, queueRetryDelay).
const (
	EnvHTTPAddress                     = "TETRAL_QUEUE_HTTP_ADDR"
	EnvGRPCAddress                     = "TETRAL_QUEUE_GRPC_ADDR"
	EnvLeaseReclaimIntervalSeconds     = "TETRAL_QUEUE_LEASE_RECLAIM_INTERVAL_SECONDS"
	EnvLeaseReclaimLimit               = "TETRAL_QUEUE_LEASE_RECLAIM_LIMIT"
	EnvRetryBaseMS                     = "TETRAL_QUEUE_RETRY_BASE_MS"
	EnvRetryCapMS                      = "TETRAL_QUEUE_RETRY_CAP_MS"
	EnvRetryMaxAttempts                = "TETRAL_QUEUE_RETRY_MAX_ATTEMPTS"
	defaultLeaseReclaimIntervalSeconds = 30
	defaultLeaseReclaimLimit           = 100
	defaultRetryBaseMS                 = 1000
	defaultRetryCapMS                  = 60000
	defaultRetryMaxAttempts            = 10
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	HTTPAddress            string
	GRPCAddress            string
	DeploymentEnvironment  string
	ServiceVersion         string
	LeaseReclaimInterval   time.Duration
	LeaseReclaimBatchLimit int
	RetryBaseDelay         time.Duration
	RetryMaxDelay          time.Duration
	RetryMaxAttempts       int
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment reader is required")
	}
	httpAddress := env.Getenv(EnvHTTPAddress)
	if httpAddress == "" {
		httpAddress = ":8080"
	}
	grpcAddress := env.Getenv(EnvGRPCAddress)
	if grpcAddress == "" {
		grpcAddress = ":9090"
	}
	deploymentEnvironment := env.Getenv("TETRAL_DEPLOYMENT_ENVIRONMENT")
	if deploymentEnvironment == "" {
		deploymentEnvironment = "local"
	}
	serviceVersion := env.Getenv("TETRAL_SERVICE_VERSION")
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	reclaimInterval := time.Duration(defaultLeaseReclaimIntervalSeconds) * time.Second
	if raw := env.Getenv(EnvLeaseReclaimIntervalSeconds); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 {
			return Config{}, workload.NewConfigError(EnvLeaseReclaimIntervalSeconds + " must be a positive integer")
		}
		reclaimInterval = time.Duration(seconds) * time.Second
	}
	reclaimLimit := defaultLeaseReclaimLimit
	if raw := env.Getenv(EnvLeaseReclaimLimit); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return Config{}, workload.NewConfigError(EnvLeaseReclaimLimit + " must be a positive integer")
		}
		reclaimLimit = limit
	}
	retryBaseMS, err := nonNegativeOrDefault(env.Getenv(EnvRetryBaseMS), defaultRetryBaseMS, EnvRetryBaseMS)
	if err != nil {
		return Config{}, err
	}
	retryCapMS, err := nonNegativeOrDefault(env.Getenv(EnvRetryCapMS), defaultRetryCapMS, EnvRetryCapMS)
	if err != nil {
		return Config{}, err
	}
	retryMaxAttempts, err := nonNegativeOrDefault(env.Getenv(EnvRetryMaxAttempts), defaultRetryMaxAttempts, EnvRetryMaxAttempts)
	if err != nil {
		return Config{}, err
	}
	if retryBaseMS == 0 || retryCapMS == 0 || retryMaxAttempts == 0 {
		return Config{}, workload.NewConfigError("queue retry policy values must be positive")
	}
	if retryCapMS < retryBaseMS {
		return Config{}, workload.NewConfigError(EnvRetryCapMS + " must be greater than or equal to " + EnvRetryBaseMS)
	}
	return Config{
		HTTPAddress:            httpAddress,
		GRPCAddress:            grpcAddress,
		DeploymentEnvironment:  deploymentEnvironment,
		ServiceVersion:         serviceVersion,
		LeaseReclaimInterval:   reclaimInterval,
		LeaseReclaimBatchLimit: reclaimLimit,
		RetryBaseDelay:         time.Duration(retryBaseMS) * time.Millisecond,
		RetryMaxDelay:          time.Duration(retryCapMS) * time.Millisecond,
		RetryMaxAttempts:       retryMaxAttempts,
	}, nil
}

func nonNegativeOrDefault(raw string, fallback int, field string) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, workload.NewConfigError(field + " must be a non-negative integer")
	}
	return value, nil
}
