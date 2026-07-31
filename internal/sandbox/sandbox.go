package sandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const CleanupLeaseCompletionWriteMargin = 10 * time.Second

type Status string

const (
	StatusCreating  Status = "creating"
	StatusActive    Status = "active"
	StatusStopped   Status = "stopped"
	StatusArchived  Status = "archived"
	StatusResuming  Status = "resuming"
	StatusReleasing Status = "releasing"
	StatusReleased  Status = "released"
	StatusFailed    Status = "failed"
)

func (s Status) IsLive() bool {
	switch s {
	case StatusCreating, StatusActive, StatusStopped, StatusArchived, StatusResuming, StatusReleasing:
		return true
	default:
		return false
	}
}

type ReleaseReason string

const (
	ReleaseReasonArchive        ReleaseReason = "archive"
	ReleaseReasonDelete         ReleaseReason = "delete"
	ReleaseReasonCleanup        ReleaseReason = "cleanup"
	ReleaseReasonRuntimePodLost ReleaseReason = "runtime_pod_lost"
)

type Sandbox struct {
	ID                    string
	WorkspaceID           workspace.ID
	SessionID             string
	EnvironmentID         string
	EnvironmentGeneration int64
	Status                Status
	Provider              string
	ProviderSandboxID     string
	ProviderMetadata      map[string]string
	ProviderHandle        ProviderHandle
	StartupFailureReason  string
	CleanupStatus         CleanupStatus
	CleanupMethod         string
	CleanupErrorKind      string
	CleanupRetryable      bool
	CleanupLastAttemptAt  *time.Time
	CleanupNextAttemptAt  *time.Time
	CleanupAttemptCount   int
	CleanupLeaseToken     string
	CleanupLeaseExpiresAt *time.Time
	MachineWasUsable      bool
	StatusRefreshedAt     *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
	ReleasedAt            *time.Time
	FailedAt              *time.Time
}

type CleanupStatus string

const (
	CleanupStatusNone            CleanupStatus = "none"
	CleanupStatusPending         CleanupStatus = "pending"
	CleanupStatusInProgress      CleanupStatus = "in_progress"
	CleanupStatusReleased        CleanupStatus = "released"
	CleanupStatusRetryableFailed CleanupStatus = "retryable_failed"
	CleanupStatusPermanentFailed CleanupStatus = "permanent_failed"
)

func cleanupStatusTerminal(status CleanupStatus) bool {
	switch status {
	case CleanupStatusReleased, CleanupStatusPermanentFailed:
		return true
	default:
		return false
	}
}

type ProviderHandle struct {
	Provider  string
	SandboxID string
	Metadata  map[string]string
}

type ProviderAvailability string

const (
	ProviderAvailable   ProviderAvailability = "available"
	ProviderMissing     ProviderAvailability = "missing"
	ProviderUnavailable ProviderAvailability = "unavailable"
	ProviderUnknown     ProviderAvailability = "unknown"
)

type ProviderStatus struct {
	Availability  ProviderAvailability
	SandboxStatus Status
	Retryable     bool
	SafeMessage   string
	Metadata      map[string]string
}

type CreateForSessionRequest struct {
	WorkspaceID           workspace.ID
	SessionID             string
	SandboxID             string
	EnvironmentID         string
	EnvironmentGeneration int64
	PreparationAttemptID  string
	ProviderArtifactRef   string
	Network               NetworkSetup
	Resources             ResourceSetup
	ResourceCleanup       SessionResourceCleanupCoordinator
}

type SandboxSetup struct {
	WorkspaceID           workspace.ID
	SessionID             string
	SandboxID             string
	LifecycleOperationID  string
	EnvironmentID         string
	EnvironmentGeneration int64
	ResourceRevision      int64
	ProviderArtifactRef   string
	Network               NetworkSetup
	Resources             ResourceSetup
	ResourceCleanup       SessionResourceCleanupCoordinator
}

type PackageSetup map[string][]string

type NetworkSetup struct {
	Type             string
	NetworkAllowList string
}

// ResourceSetup carries the resolved resources for one materialization. Two of
// its fields are readiness facts that Sandbox Service records on the Session's
// Sandbox binding and checks before provider execution:
//   - ResourceCredExpiresAt is the credential-expiry gate compared against the
//     PostgreSQL clock before execution.
//   - ResourceRootsJSON is a JSON array of {"path":<mount_path>,"mode":"read"}, one
//     entry per projected file, serialized verbatim as the helper payload roots[]
//     entries. Because every projected file is thereby its own most-specific read
//     root, a Write/Edit/apply_patch to any projected mount_path — even one nested
//     under a read_write root — fails path_escape at helper containment; that is
//     what makes projected mounts reject writes.
type ResourceSetup struct {
	Files                       []FileMount
	DeletedFiles                []FileMount
	MemoryStores                []MemoryStoreMount
	DeletedMemoryStores         []MemoryStoreMount
	GitHubRepositories          []GitHubRepositoryMount
	DeletedGitHubRepositories   []GitHubRepositoryMount
	Skills                      []SkillMount
	ResourceCredExpiresAt       *time.Time
	ResourceRootsJSON           string
	PreparationAttemptCreatedAt time.Time
}

type FileMount struct {
	ResourceID    string
	SourceFileID  string
	SessionFileID string
	ObjectID      string
	MountPath     string
	ReadOnly      bool
}

type MemoryStoreMount struct {
	ResourceID    string
	MemoryStoreID string
	MountPath     string
	Access        string
	Instructions  string
	Name          string
	Description   string
}

type GitHubRepositoryMount struct {
	ResourceID   string
	URL          string
	MountPath    string
	CheckoutType string
	CheckoutRef  string
}

type SkillMount struct {
	SkillID        string
	SkillVersionID string
	Version        string
	Name           string
	Description    string
	Directory      string
	BlobKey        string
	SHA256         string
	SizeBytes      int64
}

type CreateSandboxRequest struct {
	Setup SandboxSetup
}

type BuildArtifactRequest struct {
	WorkspaceID        workspace.ID
	EnvironmentID      string
	Generation         int64
	ArtifactInputHash  string
	NormalizedPackages PackageSetup
}

type BuildArtifactResult struct {
	ProviderArtifactRef string
}

type ArtifactBuilder interface {
	BuildArtifact(ctx context.Context, request BuildArtifactRequest) (BuildArtifactResult, error)
}

type LifecycleProvider interface {
	CreateSandbox(ctx context.Context, request CreateSandboxRequest) (ProviderHandle, error)
	StartSandbox(ctx context.Context, handle ProviderHandle) error
	CheckBaseTemplateHealth(ctx context.Context, handle ProviderHandle) error
	ApplyNetworkPolicy(ctx context.Context, handle ProviderHandle, network NetworkSetup) error
	PrepareBaseDirectories(ctx context.Context, handle ProviderHandle) error
	GetStatus(ctx context.Context, handle ProviderHandle) (ProviderStatus, error)
	ReleaseSandbox(ctx context.Context, handle ProviderHandle, reason ReleaseReason) error
}

func cloneSandbox(in *Sandbox) *Sandbox {
	if in == nil {
		return nil
	}
	out := *in
	out.ProviderMetadata = cloneStringMap(in.ProviderMetadata)
	out.ProviderHandle = cloneProviderHandle(in.ProviderHandle)
	if in.ReleasedAt != nil {
		value := *in.ReleasedAt
		out.ReleasedAt = &value
	}
	if in.FailedAt != nil {
		value := *in.FailedAt
		out.FailedAt = &value
	}
	if in.CleanupLastAttemptAt != nil {
		value := *in.CleanupLastAttemptAt
		out.CleanupLastAttemptAt = &value
	}
	if in.CleanupNextAttemptAt != nil {
		value := *in.CleanupNextAttemptAt
		out.CleanupNextAttemptAt = &value
	}
	if in.CleanupLeaseExpiresAt != nil {
		value := *in.CleanupLeaseExpiresAt
		out.CleanupLeaseExpiresAt = &value
	}
	if in.StatusRefreshedAt != nil {
		value := *in.StatusRefreshedAt
		out.StatusRefreshedAt = &value
	}
	return &out
}

func cloneProviderHandle(in ProviderHandle) ProviderHandle {
	return ProviderHandle{
		Provider:  in.Provider,
		SandboxID: in.SandboxID,
		Metadata:  cloneStringMap(in.Metadata),
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneNetworkSetup(in NetworkSetup) NetworkSetup {
	return in
}

func cloneResourceSetup(in ResourceSetup) ResourceSetup {
	out := ResourceSetup{
		Files:                       append([]FileMount(nil), in.Files...),
		DeletedFiles:                append([]FileMount(nil), in.DeletedFiles...),
		MemoryStores:                append([]MemoryStoreMount(nil), in.MemoryStores...),
		DeletedMemoryStores:         append([]MemoryStoreMount(nil), in.DeletedMemoryStores...),
		GitHubRepositories:          append([]GitHubRepositoryMount(nil), in.GitHubRepositories...),
		DeletedGitHubRepositories:   append([]GitHubRepositoryMount(nil), in.DeletedGitHubRepositories...),
		Skills:                      append([]SkillMount(nil), in.Skills...),
		ResourceRootsJSON:           in.ResourceRootsJSON,
		PreparationAttemptCreatedAt: in.PreparationAttemptCreatedAt,
	}
	if in.ResourceCredExpiresAt != nil {
		value := *in.ResourceCredExpiresAt
		out.ResourceCredExpiresAt = &value
	}
	return out
}
