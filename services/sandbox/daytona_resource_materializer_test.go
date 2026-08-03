package tetralsandbox

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/sandbox"
)

func TestDaytonaResourceMaterializerConvergesEveryResourceFamily(t *testing.T) {
	events := []string{}
	expiresAt := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	projection := &recordingDaytonaFileResourceMaterialization{
		events: &events,
		result: sandbox.ResourceSetup{
			MemoryStores:          []sandbox.MemoryStoreMount{{ResourceID: "memory", MemoryStoreID: "mem_1", MountPath: "/mnt/memory/project"}},
			GitHubRepositories:    []sandbox.GitHubRepositoryMount{{ResourceID: "repo", URL: "https://github.com/tetral-ai/tetral", MountPath: "/workspace/tetral"}},
			ResourceCredExpiresAt: &expiresAt,
			ResourceRootsJSON:     `[]`,
		},
	}
	memory := &recordingMemoryProjection{events: &events}
	github := &recordingGitHubMaterialization{events: &events}
	materializer := &DaytonaResourceMaterializer{
		Projection: projection,
		Memory:     memory,
		GitHub:     github,
	}
	setup := sandbox.SandboxSetup{
		WorkspaceID: "ws_default",
		SessionID:   "sesn_materialize",
		Resources: sandbox.ResourceSetup{
			DeletedGitHubRepositories: []sandbox.GitHubRepositoryMount{{ResourceID: "old-repo", URL: "https://github.com/tetral-ai/old", MountPath: "/workspace/old"}},
		},
	}
	handle := sandbox.ProviderHandle{Provider: "daytona", SandboxID: "provider-sandbox"}

	got, err := materializer.MaterializeResources(context.Background(), setup, handle)
	if err != nil {
		t.Fatalf("MaterializeResources: %v", err)
	}
	wantEvents := []string{"github-delete", "projection", "memory", "github-clone"}
	if len(events) != len(wantEvents) {
		t.Fatalf("events = %v; want %v", events, wantEvents)
	}
	for i := range wantEvents {
		if events[i] != wantEvents[i] {
			t.Fatalf("events = %v; want %v", events, wantEvents)
		}
	}
	if got.ResourceCredExpiresAt == nil || !got.ResourceCredExpiresAt.Equal(expiresAt) {
		t.Fatalf("credential expiry = %v; want %v", got.ResourceCredExpiresAt, expiresAt)
	}
}

type recordingDaytonaFileResourceMaterialization struct {
	events *[]string
	result sandbox.ResourceSetup
}

func (p *recordingDaytonaFileResourceMaterialization) MaterializeFileResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) (sandbox.ResourceSetup, error) {
	*p.events = append(*p.events, "projection")
	return p.result, nil
}

type recordingMemoryProjection struct {
	events *[]string
}

func (p *recordingMemoryProjection) MaterializeMemoryResources(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error {
	*p.events = append(*p.events, "memory")
	return nil
}

type recordingGitHubMaterialization struct {
	events *[]string
}

func (p *recordingGitHubMaterialization) RemoveDeletedGitHubRepositories(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error {
	*p.events = append(*p.events, "github-delete")
	return nil
}

func (p *recordingGitHubMaterialization) MaterializeGitHubRepositories(context.Context, sandbox.SandboxSetup, sandbox.ProviderHandle) error {
	*p.events = append(*p.events, "github-clone")
	return nil
}
