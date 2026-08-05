package tetralsandbox

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workload"
)

const (
	ServiceName = "sandbox"

	EnvHTTPAddress = "TETRAL_SANDBOX_HTTP_ADDR"
	EnvPostgresDSN = "TETRAL_POSTGRES_DSN"

	EnvSandboxBaseImage                         = "TETRAL_SANDBOX_BASE_IMAGE"
	EnvDaytonaAPIURL                            = "DAYTONA_API_URL"
	EnvDaytonaTarget                            = "DAYTONA_TARGET"
	EnvDaytonaAPIKey                            = "DAYTONA_API_KEY" //nolint:gosec // G101: env-var name, not a credential value
	EnvQueueGRPCAddress                         = "TETRAL_QUEUE_GRPC_ADDR"
	EnvSandboxLeaseHeartbeatInterval            = "TETRAL_SANDBOX_LEASE_HEARTBEAT_INTERVAL"
	EnvSandboxJobLeaseDuration                  = "TETRAL_SANDBOX_JOB_LEASE_DURATION"
	EnvSandboxProviderCommandTimeout            = "TETRAL_SANDBOX_PROVIDER_COMMAND_TIMEOUT"
	EnvSandboxLateCommandMargin                 = "TETRAL_SANDBOX_LATE_COMMAND_MARGIN"
	EnvSandboxJobPollInterval                   = "TETRAL_SANDBOX_JOB_POLL_INTERVAL"
	EnvSandboxEnvironmentBuildConcurrency       = "TETRAL_SANDBOX_ENVIRONMENT_BUILD_CONCURRENCY"
	EnvSandboxEnvironmentReadyFanoutConcurrency = "TETRAL_SANDBOX_ENVIRONMENT_READY_FANOUT_CONCURRENCY"
	EnvSandboxWorkerConcurrency                 = "TETRAL_SANDBOX_WORKER_CONCURRENCY"
	EnvSandboxDaytonaStopTimeout                = "TETRAL_SANDBOX_DAYTONA_STOP_TIMEOUT"
	EnvSandboxAutoStopInterval                  = "TETRAL_SANDBOX_AUTO_STOP_INTERVAL"
	EnvSandboxAutoArchiveInterval               = "TETRAL_SANDBOX_AUTO_ARCHIVE_INTERVAL"
	EnvSandboxAutoDeleteInterval                = "TETRAL_SANDBOX_AUTO_DELETE_INTERVAL"
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
	EnvSandboxDebugLogging                      = "TETRAL_SANDBOX_DEBUG_LOGGING"
	defaultHTTPAddress                          = ":8080"
	defaultSandboxLeaseHeartbeatInterval        = 15 * time.Second
	defaultSandboxJobLeaseDuration              = 120 * time.Second
	defaultSandboxProviderCommandTimeout        = 45 * time.Second
	defaultSandboxLateCommandMargin             = 30 * time.Second
	defaultSandboxJobPollInterval               = time.Second
	defaultSandboxEnvironmentBuildConcurrency   = 1
	defaultSandboxEnvironmentReadyFanout        = 1
	defaultSandboxWorkerConcurrency             = 1
	defaultSandboxDaytonaStopTimeout            = 30 * time.Second
	defaultSandboxAutoStopInterval              = 30 * time.Minute
	defaultSandboxAutoArchiveInterval           = 24 * time.Hour
	defaultSandboxAutoDeleteInterval            = 30 * 24 * time.Hour
	defaultResourceCredentialTTL                = 24 * time.Hour
	defaultResourceCredentialRefreshMargin      = 30 * time.Minute
)

type Env interface {
	Getenv(string) string
}

type Config struct {
	HTTPAddress                       string
	PostgresDSN                       string
	Daytona                           driver.Config
	QueueGRPCAddress                  string
	LeaseHeartbeatInterval            time.Duration
	JobLeaseDuration                  time.Duration
	ProviderCommandTimeout            time.Duration
	LateCommandMargin                 time.Duration
	JobPollInterval                   time.Duration
	EnvironmentBuildConcurrency       int
	EnvironmentReadyFanoutConcurrency int
	WorkerConcurrency                 int
	DaytonaStopTimeout                time.Duration
	AutoStopInterval                  time.Duration
	AutoArchiveInterval               time.Duration
	AutoDeleteInterval                time.Duration
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
	DebugLogging                      bool
}

func ConfigFromEnv(env Env) (Config, error) {
	if env == nil {
		return Config{}, workload.NewConfigError("environment is required")
	}
	cfg := Config{
		HTTPAddress:                       valueOrDefault(env.Getenv(EnvHTTPAddress), defaultHTTPAddress),
		PostgresDSN:                       strings.TrimSpace(env.Getenv(EnvPostgresDSN)),
		QueueGRPCAddress:                  strings.TrimSpace(env.Getenv(EnvQueueGRPCAddress)),
		LeaseHeartbeatInterval:            defaultSandboxLeaseHeartbeatInterval,
		JobLeaseDuration:                  defaultSandboxJobLeaseDuration,
		ProviderCommandTimeout:            defaultSandboxProviderCommandTimeout,
		LateCommandMargin:                 defaultSandboxLateCommandMargin,
		JobPollInterval:                   defaultSandboxJobPollInterval,
		EnvironmentBuildConcurrency:       defaultSandboxEnvironmentBuildConcurrency,
		EnvironmentReadyFanoutConcurrency: defaultSandboxEnvironmentReadyFanout,
		WorkerConcurrency:                 defaultSandboxWorkerConcurrency,
		DaytonaStopTimeout:                defaultSandboxDaytonaStopTimeout,
		AutoStopInterval:                  defaultSandboxAutoStopInterval,
		AutoArchiveInterval:               defaultSandboxAutoArchiveInterval,
		AutoDeleteInterval:                defaultSandboxAutoDeleteInterval,
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
	if raw := strings.TrimSpace(env.Getenv(EnvSandboxDebugLogging)); raw != "" {
		cfg.DebugLogging, err = strconv.ParseBool(raw)
		if err != nil {
			return Config{}, workload.NewConfigError(EnvSandboxDebugLogging + " must be true or false")
		}
	}
	durationFields := []struct {
		envName string
		target  *time.Duration
	}{
		{EnvSandboxLeaseHeartbeatInterval, &cfg.LeaseHeartbeatInterval},
		{EnvSandboxJobPollInterval, &cfg.JobPollInterval},
		{EnvSandboxDaytonaStopTimeout, &cfg.DaytonaStopTimeout},
		{EnvSandboxAutoStopInterval, &cfg.AutoStopInterval},
		{EnvSandboxAutoArchiveInterval, &cfg.AutoArchiveInterval},
		{EnvSandboxAutoDeleteInterval, &cfg.AutoDeleteInterval},
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
	if cfg.ResourceCredentialTTL <= cfg.ResourceCredentialRefreshMargin {
		return Config{}, workload.NewConfigError(EnvResourceCredentialTTL + " must be greater than " + EnvResourceCredentialRefreshMargin)
	}
	wholeSecondFields := []struct {
		envName string
		target  *time.Duration
	}{
		{EnvSandboxJobLeaseDuration, &cfg.JobLeaseDuration},
		{EnvSandboxProviderCommandTimeout, &cfg.ProviderCommandTimeout},
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
	if cfg.ProviderCommandTimeout/time.Second > time.Duration(math.MaxInt32) {
		return Config{}, workload.NewConfigError(EnvSandboxProviderCommandTimeout + " exceeds the Daytona integer-seconds wire range")
	}
	// Sandbox provider-command lease fence. A provider command may dispatch
	// up to one full LeaseHeartbeatInterval after the last successful lease
	// renewal, so the usable window is JobLeaseDuration minus one
	// heartbeat, and it must still cover ProviderCommandTimeout plus
	// LateCommandMargin. LateCommandMargin absorbs dispatch latency, the
	// provider daemon kill grace, and cross-clock skew (the lease expires on the
	// queue clock while the kill fires on the sandbox daemon clock). Defaults
	// 120/15/45/30s satisfy 120-15 = 105 >= 45+30 = 75.
	//
	// Provider-mutating Sandbox runners take the explicit
	// JobLeaseDuration; environment build and fanout runners use the derived
	// heartbeat*4 lease. Provider-mutating runners use JobLeaseDuration directly.
	if cfg.JobLeaseDuration-cfg.LeaseHeartbeatInterval < cfg.ProviderCommandTimeout+cfg.LateCommandMargin {
		return Config{}, workload.NewConfigError(EnvSandboxJobLeaseDuration + " minus " + EnvSandboxLeaseHeartbeatInterval + " must be at least " + EnvSandboxProviderCommandTimeout + " plus " + EnvSandboxLateCommandMargin)
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
		{EnvSandboxWorkerConcurrency, &cfg.WorkerConcurrency},
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
		{EnvSandboxWorkerConcurrency, cfg.WorkerConcurrency},
	} {
		if err := queue.ValidateLeaseBatchSize(field.value); err != nil {
			return Config{}, workload.NewConfigError(field.envName + " " + err.Error())
		}
	}
	cfg.Daytona.Lifecycle = driver.LifecyclePolicy{
		StopTimeout:         cfg.DaytonaStopTimeout,
		AutoStopInterval:    cfg.AutoStopInterval,
		AutoArchiveInterval: cfg.AutoArchiveInterval,
		AutoDeleteInterval:  cfg.AutoDeleteInterval,
	}
	cfg.Daytona.CommandTimeout = cfg.ProviderCommandTimeout
	return cfg, nil
}

// parsePositiveWholeSeconds enforces whole seconds >= 1s for the lease/command
// fence durations (JobLeaseDuration, ProviderCommandTimeout,
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
