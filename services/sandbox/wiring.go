package tetralsandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

func EnvironmentQueueLeaseDuration(cfg Config) time.Duration {
	return cfg.LeaseHeartbeatInterval * 4
}

func SessionPrepareQueueLeaseDuration(cfg Config) time.Duration {
	return cfg.SessionPrepareLeaseDuration
}

func NewSandboxLifecycleProvider(cfg Config) (sandbox.LifecycleProvider, error) {
	switch cfg.SandboxDriver {
	case driver.DaytonaProviderName:
		return driver.NewDaytonaLifecycleProvider(cfg.Daytona)
	default:
		return nil, &ConfigError{Message: EnvSandboxDriver + " must be daytona"}
	}
}

func SandboxServiceOptions(cfg Config) []sandbox.Option {
	return []sandbox.Option{
		sandbox.WithProviderName(internalSandboxProviderName),
		sandbox.WithStatusFreshnessTTL(cfg.StatusFreshnessWindow),
		sandbox.WithStaleStartupThreshold(cfg.StatusFreshnessWindow),
		sandbox.WithCleanupRetryBackoff(cfg.CleanupRetryBackoff),
		sandbox.WithCleanupLeaseDuration(cfg.CleanupLeaseDuration),
		sandbox.WithCleanupMaxAttempts(cfg.CleanupMaxAttempts),
	}
}

func SandboxServiceOptionsWithDatabase(cfg Config, client *dbconnect.Client, materializer sandbox.GitHubRepositoryMaterializer) []sandbox.Option {
	options := SandboxServiceOptions(cfg)
	options = append(options, sandbox.WithGitHubRepositoryPreparation(gitticket.NewPostgreSQLStore(client), materializer, cfg.GitProxyHost))
	return options
}

func NewResourceProjectionPreparerFromConfig(ctx context.Context, cfg Config, runner driver.PreparationCommandRunner) (*ResourceProjectionPreparer, error) {
	blobStore, err := blob.NewS3BlobStore(ctx, &blob.Config{
		Endpoint:  cfg.BlobEndpoint,
		Region:    cfg.BlobRegion,
		Bucket:    cfg.BlobBucket,
		AccessKey: cfg.BlobAccessKey,
		SecretKey: cfg.BlobSecretKey,
	})
	if err != nil {
		return nil, err
	}
	minter, err := resourceprojection.NewCredentialMinter(resourceprojection.CredentialMintConfig{
		AccountID:         cfg.R2AccountID,
		Bucket:            cfg.BlobBucket,
		ParentAccessKeyID: cfg.R2ParentAccessKeyID,
		ParentAPIToken:    cfg.R2ParentAPIToken,
	})
	if err != nil {
		return nil, err
	}
	return NewResourceProjectionPreparer(ResourceProjectionPreparerConfig{
		Blob:                    blobStore,
		CredentialMinter:        minter,
		CommandRunner:           runner,
		Bucket:                  cfg.BlobBucket,
		AccountID:               cfg.R2AccountID,
		CredentialTTL:           cfg.ResourceCredentialTTL,
		CredentialRefreshMargin: cfg.ResourceCredentialRefreshMargin,
		CommandTimeout:          cfg.PreparationCommandTimeout,
		RcloneVFSCacheMaxSize:   cfg.RcloneVFSCacheMaxSize,
		RcloneVFSMinFree:        cfg.RcloneVFSMinFree,
	})
}

type ConfigError struct {
	Message string
}

func (e *ConfigError) Error() string {
	if e == nil || e.Message == "" {
		return "sandbox configuration is invalid"
	}
	return e.Message
}
