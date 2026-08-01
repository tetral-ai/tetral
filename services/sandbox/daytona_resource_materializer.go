package tetralsandbox

import (
	"context"
	"errors"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

type daytonaFileResourceMaterialization interface {
	MaterializeFileResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) (sandbox.ResourceSetup, error)
}

type memoryResourceMaterialization interface {
	MaterializeMemoryResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error
}

type gitHubResourceMaterialization interface {
	RemoveDeletedGitHubRepositories(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error
	MaterializeGitHubRepositories(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error
}

// DaytonaResourceMaterializer converges every provider-backed resource family
// for one immutable Session resource revision before the binding can authorize
// tool execution.
type DaytonaResourceMaterializer struct {
	Projection daytonaFileResourceMaterialization
	Memory     memoryResourceMaterialization
	GitHub     gitHubResourceMaterialization
}

func (m *DaytonaResourceMaterializer) MaterializeResources(ctx context.Context, setup sandbox.SandboxSetup, handle sandbox.ProviderHandle) (sandbox.ResourceSetup, error) {
	if m == nil || m.Projection == nil || m.Memory == nil || m.GitHub == nil {
		return sandbox.ResourceSetup{}, errors.New("daytona resource materializer is incomplete")
	}
	if handle.SandboxID == "" {
		return sandbox.ResourceSetup{}, errors.New("daytona resource materialization requires a provider resource")
	}
	if err := m.GitHub.RemoveDeletedGitHubRepositories(ctx, setup, handle); err != nil {
		return sandbox.ResourceSetup{}, err
	}
	resources, err := m.Projection.MaterializeFileResources(ctx, setup, handle)
	if err != nil {
		return sandbox.ResourceSetup{}, err
	}
	materializedSetup := setup
	materializedSetup.Resources = resources
	if err := m.Memory.MaterializeMemoryResources(ctx, materializedSetup, handle); err != nil {
		return sandbox.ResourceSetup{}, err
	}
	if err := m.GitHub.MaterializeGitHubRepositories(ctx, materializedSetup, handle); err != nil {
		return sandbox.ResourceSetup{}, err
	}
	return resources, nil
}

type DaytonaMemoryMaterializer struct {
	Reader       sandbox.MemorySnapshotReader
	Locker       sandbox.MemoryStoreMutationLocker
	Materializer sandbox.MemoryStoreMaterializer
}

func (m *DaytonaMemoryMaterializer) MaterializeMemoryResources(ctx context.Context, setup sandbox.SandboxSetup, handle sandbox.ProviderHandle) error {
	if m == nil {
		return errors.New("daytona memory materializer is required")
	}
	return sandbox.MaterializeMemoryProjections(ctx, m.Reader, m.Materializer, sandbox.MemoryProjectionMaterializationRequest{
		WorkspaceID:       setup.WorkspaceID,
		ProviderSandboxID: handle.SandboxID,
		Resources:         setup.Resources,
		MutationLocker:    m.Locker,
	})
}
