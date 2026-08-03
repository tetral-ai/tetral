package tetralsandbox

import (
	"context"
	"time"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/daytona"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

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

func NewDaytonaAdapter(ctx context.Context, cfg Config, client *dbconnect.Client) (*DaytonaAdapter, error) {
	if client == nil {
		return nil, &ConfigError{Message: "sandbox database client is required"}
	}
	daytonaClient, err := daytona.NewClientWithConfig(&types.DaytonaConfig{
		APIKey: cfg.Daytona.DaytonaAPIKey, APIUrl: cfg.Daytona.DaytonaAPIURL, Target: cfg.Daytona.DaytonaTarget,
	})
	if err != nil {
		return nil, err
	}
	lifecycle, err := driver.NewDaytonaLifecycleProviderForSDKClient(daytonaClient, cfg.Daytona)
	if err != nil {
		return nil, err
	}
	helper, err := driver.NewDaytonaHelperExecutorForSDKClient(daytonaClient, cfg.Daytona.CommandTimeout)
	if err != nil {
		return nil, err
	}
	artifacts := driver.NewDaytonaArtifactBuilderForClient(daytonaClient.Snapshot, cfg.Daytona.ArtifactBaseImage)
	projection, err := NewDaytonaFileResourceMaterializerFromConfig(ctx, cfg, helper)
	if err != nil {
		return nil, err
	}
	store := sandbox.NewPostgreSQLStore(client)
	resources := &DaytonaResourceMaterializer{
		Projection: projection,
		Memory: &DaytonaMemoryMaterializer{
			Reader: store, Locker: store, Materializer: helper,
		},
		GitHub: &sandbox.GitHubRepositoryConverger{
			Rotator: gitticket.NewPostgreSQLStore(client), Materializer: helper,
			GitProxyHost: cfg.GitProxyHost,
		},
	}
	return &DaytonaAdapter{
		Lifecycle: lifecycle,
		Resolver:  lifecycle,
		Tools:     helper,
		Resources: resources,
		Artifacts: artifacts,
		BlobStore: projection.blob,
	}, nil
}

func NewDaytonaFileResourceMaterializerFromConfig(ctx context.Context, cfg Config, runner driver.DaytonaCommandRunner) (*DaytonaFileResourceMaterializer, error) {
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
	return NewDaytonaFileResourceMaterializer(DaytonaFileResourceMaterializerConfig{
		Blob:                    blobStore,
		CredentialMinter:        minter,
		CommandRunner:           runner,
		Bucket:                  cfg.BlobBucket,
		AccountID:               cfg.R2AccountID,
		CredentialTTL:           cfg.ResourceCredentialTTL,
		CredentialRefreshMargin: cfg.ResourceCredentialRefreshMargin,
		CommandTimeout:          cfg.ProviderCommandTimeout,
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
