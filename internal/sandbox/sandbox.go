package sandbox

import (
	"context"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type ProviderHandle struct {
	Provider  string
	SandboxID string
	Metadata  map[string]string
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
	Files                     []FileMount
	DeletedFiles              []FileMount
	MemoryStores              []MemoryStoreMount
	DeletedMemoryStores       []MemoryStoreMount
	GitHubRepositories        []GitHubRepositoryMount
	DeletedGitHubRepositories []GitHubRepositoryMount
	Skills                    []SkillMount
	ResourceCredExpiresAt     *time.Time
	ResourceRootsJSON         string
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
	// GitIdentityName/GitIdentityEmail declare the repository-local commit
	// identity installed after clone or same-origin recognition. Both empty
	// keeps the session-scoped platform fallback installed by the global
	// configuration phase.
	GitIdentityName  string
	GitIdentityEmail string
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
	WorkspaceID             workspace.ID
	EnvironmentID           string
	Generation              int64
	ArtifactInputHash       string
	NormalizedPackages      PackageSetup
	AuthorizeProviderCreate func(context.Context) (bool, error)
}

type BuildArtifactResult struct {
	ProviderArtifactRef string
}

type ArtifactBuilder interface {
	BuildArtifact(ctx context.Context, request BuildArtifactRequest) (BuildArtifactResult, error)
}
