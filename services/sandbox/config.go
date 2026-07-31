package tetralsandbox

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	ServiceName = "sandbox"

	EnvHTTPAddress           = "TETRAL_SANDBOX_HTTP_ADDR"
	EnvGRPCAddress           = "TETRAL_SANDBOX_GRPC_ADDR"
	EnvInternalGRPCTokenPath = "TETRAL_SANDBOX_GRPC_BEARER_TOKEN_PATH" //nolint:gosec // env-var name, not a credential value
	EnvPostgresDSN           = "TETRAL_POSTGRES_DSN"

	EnvSandboxDriver                            = "TETRAL_SANDBOX_DRIVER"
	EnvSandboxBaseImage                         = "TETRAL_SANDBOX_BASE_IMAGE"
	EnvDaytonaAPIURL                            = "DAYTONA_API_URL"
	EnvDaytonaTarget                            = "DAYTONA_TARGET"
	EnvDaytonaAPIKey                            = "DAYTONA_API_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvQueueGRPCAddress                         = "TETRAL_QUEUE_GRPC_ADDR"
	EnvSandboxLeaseHeartbeatInterval            = "TETRAL_SANDBOX_LEASE_HEARTBEAT_INTERVAL"
	EnvSandboxSessionPrepareLeaseDuration       = "TETRAL_SANDBOX_SESSION_PREPARE_LEASE_DURATION"
	EnvSandboxPreparationCommandTimeout         = "TETRAL_SANDBOX_PREPARATION_COMMAND_TIMEOUT"
	EnvSandboxLateCommandMargin                 = "TETRAL_SANDBOX_LATE_COMMAND_MARGIN"
	EnvSandboxJobPollInterval                   = "TETRAL_SANDBOX_JOB_POLL_INTERVAL"
	EnvSandboxEnvironmentBuildConcurrency       = "TETRAL_SANDBOX_ENVIRONMENT_BUILD_CONCURRENCY"
	EnvSandboxEnvironmentReadyFanoutConcurrency = "TETRAL_SANDBOX_ENVIRONMENT_READY_FANOUT_CONCURRENCY"
	EnvSandboxSessionPrepareConcurrency         = "TETRAL_SANDBOX_SESSION_PREPARE_CONCURRENCY"
	EnvSandboxDaytonaStopTimeout                = "TETRAL_SANDBOX_DAYTONA_STOP_TIMEOUT"
	EnvSandboxDaytonaStopForceAfter             = "TETRAL_SANDBOX_DAYTONA_STOP_FORCE_AFTER"
	EnvSandboxAutoStopInterval                  = "TETRAL_SANDBOX_AUTO_STOP_INTERVAL"
	EnvSandboxAutoArchiveInterval               = "TETRAL_SANDBOX_AUTO_ARCHIVE_INTERVAL"
	EnvSandboxAutoDeleteInterval                = "TETRAL_SANDBOX_AUTO_DELETE_INTERVAL"
	EnvSandboxStatusFreshnessWindow             = "TETRAL_SANDBOX_STATUS_FRESHNESS_WINDOW"
	EnvSandboxCleanupRetryBackoff               = "TETRAL_SANDBOX_CLEANUP_RETRY_BACKOFF"
	EnvSandboxCleanupLeaseDuration              = "TETRAL_SANDBOX_CLEANUP_LEASE_DURATION"
	EnvSandboxCleanupMaxAttempts                = "SANDBOX_CLEANUP_MAX_ATTEMPTS"
	EnvBlobEndpoint                             = "TETRAL_BLOB_ENDPOINT"
	EnvBlobRegion                               = "TETRAL_BLOB_REGION"
	EnvBlobBucket                               = "TETRAL_BLOB_BUCKET"
	EnvBlobAccessKey                            = "TETRAL_BLOB_ACCESS_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvBlobSecretKey                            = "TETRAL_BLOB_SECRET_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvR2AccountID                              = "TETRAL_R2_ACCOUNT_ID"
	EnvR2ParentAPIToken                         = "TETRAL_R2_PARENT_API_TOKEN"          //nolint:gosec // G101: env-var name, not a credential value
	EnvR2ParentAccessKeyID                      = "TETRAL_R2_PARENT_ACCESS_KEY"         //nolint:gosec // G101: env-var name, not a credential value
	EnvResourceCredentialTTL                    = "TETRAL_RESOURCE_CRED_TTL"            //nolint:gosec // Env-var name, not a credential value.
	EnvResourceCredentialRefreshMargin          = "TETRAL_RESOURCE_CRED_REFRESH_MARGIN" //nolint:gosec // Environment variable name for a duration setting, not credential material.
	EnvRcloneVFSCacheMaxSize                    = "TETRAL_RCLONE_VFS_CACHE_MAX_SIZE"
	EnvRcloneVFSMinFree                         = "TETRAL_RCLONE_VFS_MIN_FREE"
	EnvGitProxyHost                             = "TETRAL_GIT_PROXY_HOST"
	defaultHTTPAddress                          = ":8080"
	defaultGRPCAddress                          = ":9090"
	defaultSandboxLeaseHeartbeatInterval        = 15 * time.Second
	defaultSandboxSessionPrepareLeaseDuration   = 120 * time.Second
	defaultSandboxPreparationCommandTimeout     = 45 * time.Second
	defaultSandboxLateCommandMargin             = 30 * time.Second
	defaultSandboxJobPollInterval               = time.Second
	defaultSandboxEnvironmentBuildConcurrency   = 1
	defaultSandboxEnvironmentReadyFanout        = 1
	defaultSandboxSessionPrepareConcurrency     = 1
	defaultSandboxDaytonaStopTimeout            = 30 * time.Second
	defaultSandboxAutoStopInterval              = 30 * time.Minute
	defaultSandboxAutoArchiveInterval           = 24 * time.Hour
	defaultSandboxAutoDeleteInterval            = 30 * 24 * time.Hour
	defaultSandboxStatusFreshnessWindow         = 60 * time.Second
	defaultSandboxCleanupRetryBackoff           = 30 * time.Second
	defaultSandboxCleanupLeaseDuration          = 120 * time.Second
	defaultSandboxCleanupMaxAttempts            = 20
	defaultResourceCredentialTTL                = 24 * time.Hour
	defaultResourceCredentialRefreshMargin      = 30 * time.Minute
	internalSandboxProviderName                 = "tetral"
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	HTTPAddress                       string
	GRPCAddress                       string
	PostgresDSN                       string
	SandboxDriver                     string
	Daytona                           driver.Config
	QueueGRPCAddress                  string
	LeaseHeartbeatInterval            time.Duration
	SessionPrepareLeaseDuration       time.Duration
	PreparationCommandTimeout         time.Duration
	LateCommandMargin                 time.Duration
	JobPollInterval                   time.Duration
	EnvironmentBuildConcurrency       int
	EnvironmentReadyFanoutConcurrency int
	SessionPrepareConcurrency         int
	DaytonaStopTimeout                time.Duration
	DaytonaStopForceAfter             time.Duration
	AutoStopInterval                  time.Duration
	AutoArchiveInterval               time.Duration
	AutoDeleteInterval                time.Duration
	StatusFreshnessWindow             time.Duration
	CleanupRetryBackoff               time.Duration
	CleanupLeaseDuration              time.Duration
	CleanupMaxAttempts                int
	BlobEndpoint                      string
	BlobRegion                        string
	BlobBucket                        string
	BlobAccessKey                     string
	BlobSecretKey                     string
	R2AccountID                       string
	R2ParentAPIToken                  string
	R2ParentAccessKeyID               string
	ResourceCredentialTTL             time.Duration
	ResourceCredentialRefreshMargin   time.Duration
	RcloneVFSCacheMaxSize             string
	RcloneVFSMinFree                  string
	GitProxyHost                      string
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	cfg := Config{
		HTTPAddress:                       valueOrDefault(env.Getenv(EnvHTTPAddress), defaultHTTPAddress),
		GRPCAddress:                       valueOrDefault(env.Getenv(EnvGRPCAddress), defaultGRPCAddress),
		PostgresDSN:                       strings.TrimSpace(env.Getenv(EnvPostgresDSN)),
		SandboxDriver:                     strings.TrimSpace(env.Getenv(EnvSandboxDriver)),
		QueueGRPCAddress:                  strings.TrimSpace(env.Getenv(EnvQueueGRPCAddress)),
		LeaseHeartbeatInterval:            defaultSandboxLeaseHeartbeatInterval,
		SessionPrepareLeaseDuration:       defaultSandboxSessionPrepareLeaseDuration,
		PreparationCommandTimeout:         defaultSandboxPreparationCommandTimeout,
		LateCommandMargin:                 defaultSandboxLateCommandMargin,
		JobPollInterval:                   defaultSandboxJobPollInterval,
		EnvironmentBuildConcurrency:       defaultSandboxEnvironmentBuildConcurrency,
		EnvironmentReadyFanoutConcurrency: defaultSandboxEnvironmentReadyFanout,
		SessionPrepareConcurrency:         defaultSandboxSessionPrepareConcurrency,
		DaytonaStopTimeout:                defaultSandboxDaytonaStopTimeout,
		AutoStopInterval:                  defaultSandboxAutoStopInterval,
		AutoArchiveInterval:               defaultSandboxAutoArchiveInterval,
		AutoDeleteInterval:                defaultSandboxAutoDeleteInterval,
		StatusFreshnessWindow:             defaultSandboxStatusFreshnessWindow,
		CleanupRetryBackoff:               defaultSandboxCleanupRetryBackoff,
		CleanupLeaseDuration:              defaultSandboxCleanupLeaseDuration,
		CleanupMaxAttempts:                defaultSandboxCleanupMaxAttempts,
		BlobEndpoint:                      strings.TrimSpace(env.Getenv(EnvBlobEndpoint)),
		BlobRegion:                        strings.TrimSpace(env.Getenv(EnvBlobRegion)),
		BlobBucket:                        strings.TrimSpace(env.Getenv(EnvBlobBucket)),
		BlobAccessKey:                     strings.TrimSpace(env.Getenv(EnvBlobAccessKey)),
		BlobSecretKey:                     strings.TrimSpace(env.Getenv(EnvBlobSecretKey)),
		R2AccountID:                       strings.TrimSpace(env.Getenv(EnvR2AccountID)),
		R2ParentAPIToken:                  strings.TrimSpace(env.Getenv(EnvR2ParentAPIToken)),
		R2ParentAccessKeyID:               strings.TrimSpace(env.Getenv(EnvR2ParentAccessKeyID)),
		ResourceCredentialTTL:             defaultResourceCredentialTTL,
		ResourceCredentialRefreshMargin:   defaultResourceCredentialRefreshMargin,
		RcloneVFSCacheMaxSize:             valueOrDefault(env.Getenv(EnvRcloneVFSCacheMaxSize), defaultRcloneVFSCacheMaxSize),
		RcloneVFSMinFree:                  valueOrDefault(env.Getenv(EnvRcloneVFSMinFree), defaultRcloneVFSMinFree),
		GitProxyHost:                      strings.TrimSpace(env.Getenv(EnvGitProxyHost)),
		Daytona: driver.Config{
			DaytonaAPIURL:     strings.TrimSpace(env.Getenv(EnvDaytonaAPIURL)),
			DaytonaTarget:     strings.TrimSpace(env.Getenv(EnvDaytonaTarget)),
			DaytonaAPIKey:     strings.TrimSpace(env.Getenv(EnvDaytonaAPIKey)),
			ArtifactBaseImage: strings.TrimSpace(env.Getenv(EnvSandboxBaseImage)),
		},
	}
	if cfg.PostgresDSN == "" {
		return Config{}, workload.NewConfigError(EnvPostgresDSN + " is required")
	}
	if cfg.SandboxDriver == "" {
		return Config{}, workload.NewConfigError(EnvSandboxDriver + " is required")
	}
	if cfg.SandboxDriver != driver.DaytonaProviderName {
		return Config{}, workload.NewConfigError(fmt.Sprintf("%s must be daytona", EnvSandboxDriver))
	}
	if cfg.Daytona.DaytonaAPIURL == "" {
		return Config{}, workload.NewConfigError(EnvDaytonaAPIURL + " is required")
	}
	if cfg.Daytona.DaytonaAPIKey == "" {
		return Config{}, workload.NewConfigError(EnvDaytonaAPIKey + " is required")
	}
	if cfg.Daytona.ArtifactBaseImage == "" {
		return Config{}, workload.NewConfigError(EnvSandboxBaseImage + " is required")
	}
	if cfg.QueueGRPCAddress == "" {
		return Config{}, workload.NewConfigError(EnvQueueGRPCAddress + " is required")
	}
	if cfg.BlobEndpoint == "" {
		return Config{}, workload.NewConfigError(EnvBlobEndpoint + " is required")
	}
	if cfg.BlobRegion == "" {
		return Config{}, workload.NewConfigError(EnvBlobRegion + " is required")
	}
	if cfg.BlobBucket == "" {
		return Config{}, workload.NewConfigError(EnvBlobBucket + " is required")
	}
	if cfg.BlobAccessKey == "" {
		return Config{}, workload.NewConfigError(EnvBlobAccessKey + " is required")
	}
	if cfg.BlobSecretKey == "" {
		return Config{}, workload.NewConfigError(EnvBlobSecretKey + " is required")
	}
	if cfg.R2AccountID == "" {
		return Config{}, workload.NewConfigError(EnvR2AccountID + " is required")
	}
	if cfg.R2ParentAPIToken == "" {
		return Config{}, workload.NewConfigError(EnvR2ParentAPIToken + " is required")
	}
	if cfg.R2ParentAccessKeyID == "" {
		return Config{}, workload.NewConfigError(EnvR2ParentAccessKeyID + " is required")
	}
	if cfg.GitProxyHost == "" {
		return Config{}, workload.NewConfigError(EnvGitProxyHost + " is required")
	}
	var err error
	durationFields := []struct {
		envName string
		target  *time.Duration
	}{
		{EnvSandboxLeaseHeartbeatInterval, &cfg.LeaseHeartbeatInterval},
		{EnvSandboxJobPollInterval, &cfg.JobPollInterval},
		{EnvSandboxDaytonaStopTimeout, &cfg.DaytonaStopTimeout},
		{EnvSandboxDaytonaStopForceAfter, &cfg.DaytonaStopForceAfter},
		{EnvSandboxAutoStopInterval, &cfg.AutoStopInterval},
		{EnvSandboxAutoArchiveInterval, &cfg.AutoArchiveInterval},
		{EnvSandboxAutoDeleteInterval, &cfg.AutoDeleteInterval},
		{EnvSandboxStatusFreshnessWindow, &cfg.StatusFreshnessWindow},
		{EnvSandboxCleanupRetryBackoff, &cfg.CleanupRetryBackoff},
		{EnvSandboxCleanupLeaseDuration, &cfg.CleanupLeaseDuration},
		{EnvResourceCredentialTTL, &cfg.ResourceCredentialTTL},
		{EnvResourceCredentialRefreshMargin, &cfg.ResourceCredentialRefreshMargin},
	}
	for _, field := range durationFields {
		raw := strings.TrimSpace(env.Getenv(field.envName))
		if raw == "" {
			continue
		}
		*field.target, err = parsePositiveDuration(raw, field.envName)
		if err != nil {
			return Config{}, err
		}
	}
	wholeSecondFields := []struct {
		envName string
		target  *time.Duration
	}{
		{EnvSandboxSessionPrepareLeaseDuration, &cfg.SessionPrepareLeaseDuration},
		{EnvSandboxPreparationCommandTimeout, &cfg.PreparationCommandTimeout},
		{EnvSandboxLateCommandMargin, &cfg.LateCommandMargin},
	}
	for _, field := range wholeSecondFields {
		raw := strings.TrimSpace(env.Getenv(field.envName))
		if raw == "" {
			continue
		}
		*field.target, err = parsePositiveWholeSeconds(raw, field.envName)
		if err != nil {
			return Config{}, err
		}
	}
	if cfg.PreparationCommandTimeout/time.Second > time.Duration(math.MaxInt32) {
		return Config{}, workload.NewConfigError(EnvSandboxPreparationCommandTimeout + " exceeds the Daytona integer-seconds wire range")
	}
	// Sandbox provider-command lease fence. A provider command may dispatch
	// up to one full LeaseHeartbeatInterval after the last successful lease
	// renewal, so the usable window is SessionPrepareLeaseDuration minus one
	// heartbeat, and it must still cover PreparationCommandTimeout plus
	// LateCommandMargin. LateCommandMargin absorbs dispatch latency, the
	// provider daemon kill grace, and cross-clock skew (the lease expires on the
	// queue clock while the kill fires on the sandbox daemon clock). Defaults
	// 120/15/45/30s satisfy 120-15 = 105 >= 45+30 = 75.
	//
	// Provider-mutating Sandbox runners take the explicit
	// SessionPrepareLeaseDuration; environment build and fanout runners use the derived
	// heartbeat*4 lease. UPDATE-WITH: wiring.go (SessionPrepareQueueLeaseDuration,
	// EnvironmentQueueLeaseDuration).
	if cfg.SessionPrepareLeaseDuration-cfg.LeaseHeartbeatInterval < cfg.PreparationCommandTimeout+cfg.LateCommandMargin {
		return Config{}, workload.NewConfigError(EnvSandboxSessionPrepareLeaseDuration + " minus " + EnvSandboxLeaseHeartbeatInterval + " must be at least " + EnvSandboxPreparationCommandTimeout + " plus " + EnvSandboxLateCommandMargin)
	}
	// Sandbox-cleanup lifecycle budgets. CleanupLeaseDuration
	// (TETRAL_SANDBOX_CLEANUP_LEASE_DURATION, default 120s) bounds one
	// startup-cleanup attempt; its in-flight provider legs run under a deadline
	// of lease-expiry minus a pinned 10s completion margin
	// (sandbox.CleanupLeaseCompletionWriteMargin), which is reserved to record
	// the outcome before an expired lease can be stolen. Startup rejects any
	// lease whose remaining budget cannot cover the stop legs (DaytonaStopTimeout,
	// then DaytonaStopForceAfter). CleanupMaxAttempts
	// (SANDBOX_CLEANUP_MAX_ATTEMPTS, default 20) bounds how many attempts a
	// failed cleanup makes before it terminalizes to permanent_failed.
	// StatusFreshnessWindow (TETRAL_SANDBOX_STATUS_FRESHNESS_WINDOW, default 60s)
	// bounds how stale a recorded provider status may be before a reconciler
	// re-observes it, and doubles as the stale-startup threshold. UPDATE-WITH:
	// internal/sandbox (CleanupLeaseCompletionWriteMargin, the cleanup claim and
	// attempt transitions), wiring.go (WithStatusFreshnessTTL,
	// WithStaleStartupThreshold, WithCleanupLeaseDuration, WithCleanupMaxAttempts).
	cleanupProviderBudget := cfg.CleanupLeaseDuration - sandbox.CleanupLeaseCompletionWriteMargin
	if cfg.DaytonaStopTimeout >= cleanupProviderBudget ||
		cfg.DaytonaStopForceAfter >= cleanupProviderBudget-cfg.DaytonaStopTimeout {
		return Config{}, workload.NewConfigError(EnvSandboxCleanupLeaseDuration + " minus the cleanup completion margin must be greater than " + EnvSandboxDaytonaStopTimeout + " plus " + EnvSandboxDaytonaStopForceAfter)
	}
	// AutoDeleteInterval floor of 720h. This is the checkpoint-retention cutoff
	// for a sleeping sandbox, and it must never drop below the 30-day (720h)
	// cold-return retention: a shorter value would force-delete the checkpoints
	// of live idle sessions and turn every warm return into a rebuild.
	if cfg.AutoDeleteInterval < 30*24*time.Hour {
		return Config{}, workload.NewConfigError(EnvSandboxAutoDeleteInterval + " must be at least 720h")
	}
	intFields := []struct {
		envName string
		target  *int
	}{
		{EnvSandboxEnvironmentBuildConcurrency, &cfg.EnvironmentBuildConcurrency},
		{EnvSandboxEnvironmentReadyFanoutConcurrency, &cfg.EnvironmentReadyFanoutConcurrency},
		{EnvSandboxSessionPrepareConcurrency, &cfg.SessionPrepareConcurrency},
		{EnvSandboxCleanupMaxAttempts, &cfg.CleanupMaxAttempts},
	}
	for _, field := range intFields {
		raw := strings.TrimSpace(env.Getenv(field.envName))
		if raw == "" {
			continue
		}
		*field.target, err = parsePositiveInt(raw, field.envName)
		if err != nil {
			return Config{}, err
		}
	}
	for _, field := range []struct {
		envName string
		value   int
	}{
		{EnvSandboxEnvironmentBuildConcurrency, cfg.EnvironmentBuildConcurrency},
		{EnvSandboxEnvironmentReadyFanoutConcurrency, cfg.EnvironmentReadyFanoutConcurrency},
		{EnvSandboxSessionPrepareConcurrency, cfg.SessionPrepareConcurrency},
	} {
		if err := queue.ValidateLeaseBatchSize(field.value); err != nil {
			return Config{}, workload.NewConfigError(field.envName + " " + err.Error())
		}
	}
	cfg.Daytona.Lifecycle = driver.LifecyclePolicy{
		StopTimeout:         cfg.DaytonaStopTimeout,
		StopForceAfter:      cfg.DaytonaStopForceAfter,
		AutoStopInterval:    cfg.AutoStopInterval,
		AutoArchiveInterval: cfg.AutoArchiveInterval,
		AutoDeleteInterval:  cfg.AutoDeleteInterval,
	}
	cfg.Daytona.PreparationCommandTimeout = cfg.PreparationCommandTimeout
	return cfg, nil
}

// parsePositiveWholeSeconds enforces whole seconds >= 1s for the lease/command
// fence durations (SessionPrepareLeaseDuration, PreparationCommandTimeout,
// LateCommandMargin) because the provider wire timeout is an int32 count of
// whole seconds: a sub-second value truncates to 0, which the provider reads as
// "no server-side timeout" and silently voids the late-command fence.
func parsePositiveWholeSeconds(raw string, envName string) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value < time.Second || value%time.Second != 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive whole-second duration", envName))
	}
	return value, nil
}

func parsePositiveDuration(raw string, envName string) (time.Duration, error) {
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive duration", envName))
	}
	return value, nil
}

func parsePositiveInt(raw string, envName string) (int, error) {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0, workload.NewConfigError(fmt.Sprintf("%s must be a positive integer", envName))
	}
	return value, nil
}

func valueOrDefault(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
