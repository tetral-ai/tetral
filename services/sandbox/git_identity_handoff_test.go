package tetralsandbox

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestSandboxLifecycleRunnersDeliverGitIdentityFromDurableResources(t *testing.T) {
	queueDB, adminDB := newSandboxServiceTestDB(t)
	sandboxDB := storagetest.OpenWorkloadDB(t, adminDB, "sandbox").DB
	seedSandboxExecutionStoreFixture(t, adminDB)
	// The input boundary is persisted resources. After this fixture, only
	// production code creates/leases jobs, freezes/reads snapshots and builds
	// provider requests. RunOnce controls scheduling, not the intermediate data.
	for _, repo := range []struct {
		id, path    string
		name, email any
	}{
		{"sesrsc_declared", "/workspace/declared", "Example Automation", "automation@example.test"},
		{"sesrsc_omitted", "/workspace/omitted", nil, nil},
	} {
		if _, err := adminDB.Exec(`INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at
		) VALUES ('ws_execution_store', 'sesn_execution_store', $1, 'github_repository', now(), now())`, repo.id); err != nil {
			t.Fatal(err)
		}
		if _, err := adminDB.Exec(`INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path,
			git_identity_name, git_identity_email, authorization_token_encrypted
		) VALUES ('ws_execution_store', 'sesn_execution_store', $1,
			'https://github.com/tetral-ai/' || $1, $2, $3, $4, 'encrypted-token')`,
			repo.id, repo.path, repo.name, repo.email); err != nil {
			t.Fatal(err)
		}
	}
	client := dbconnect.NewClientForTesting(sandboxDB)
	coordinator := NewPostgreSQLSandboxExecutionCoordinator(client, 30*time.Minute)
	work := loadSandboxExecutionWork(t, coordinator, "evt_execution_a")
	if err := coordinator.WaitForActivation(sandboxTestQueueContext(t, sandboxDB), work, ExecutionNeedsCreation); err != nil {
		t.Fatalf("WaitForActivation: %v", err)
	}
	lifecycle := NewPostgreSQLSandboxLifecycleStore(client, sandbox.NewPostgreSQLStore(client), 30*time.Minute)
	queueClient := sandboxProductionQueueClient(t, queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(queueDB)))
	// The provider is the output boundary: record what it receives, without
	// repairing/reconstructing the incoming identity or executing remote work.
	adapter := &recordingLifecycleAdapter{
		activation: ProviderOutcome[sandbox.ProviderHandle]{Value: sandbox.ProviderHandle{
			Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_identity_handoff",
		}},
		inspection: ProviderOutcome[ExecutionReadiness]{Value: ExecutionReady},
		materialization: ProviderOutcome[MaterializationResult]{Value: MaterializationResult{
			MaterializedEnvironmentGeneration: 1, MaterializedResourceRevision: 1,
		}},
	}
	registry, err := NewProviderRegistry(map[string]ProviderAdapter{sandboxdriver.DaytonaProviderName: adapter})
	if err != nil {
		t.Fatal(err)
	}
	config := SandboxLifecycleRunnerConfig{
		WorkspaceID: "ws_execution_store", LeaseOwner: "identity-handoff", MaxJobs: 1,
		LeaseDuration: time.Minute, HeartbeatInterval: 15 * time.Second,
	}
	activationRunner := &SandboxActivationJobRunner{Queue: queueClient, Store: lifecycle, Providers: registry, Config: config}
	if err := activationRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("activation RunOnce: %v", err)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "waiting_materialization", 1)
	materializationRunner := &SandboxMaterializationJobRunner{Queue: queueClient, Store: lifecycle, Providers: registry, Config: config}
	if err := materializationRunner.RunOnce(context.Background()); err != nil {
		t.Fatalf("materialization RunOnce: %v", err)
	}
	if len(adapter.materializationRequests) != 1 {
		t.Fatalf("provider received %d materialization requests; want 1", len(adapter.materializationRequests))
	}
	repositories := adapter.materializationRequests[0].Setup.Resources.GitHubRepositories
	if len(repositories) != 2 {
		t.Fatalf("provider repositories = %+v; want declared and omitted identities", repositories)
	}
	want := map[string][2]string{
		"sesrsc_declared": {"Example Automation", "automation@example.test"},
		"sesrsc_omitted":  {"", ""},
	}
	for _, repo := range repositories {
		identity, ok := want[repo.ResourceID]
		if !ok || repo.GitIdentityName != identity[0] || repo.GitIdentityEmail != identity[1] {
			t.Fatalf("provider repository = %+v; want identity %q (known resource: %t)", repo, identity, ok)
		}
		delete(want, repo.ResourceID)
	}
	assertSandboxExecutionState(t, adminDB, "evt_execution_a", "pending", 2)
	var acknowledged int
	if err := adminDB.QueryRow(`SELECT count(*) FROM queue_jobs
		WHERE workspace_id='ws_execution_store' AND kind IN ('sandbox_activate', 'sandbox_materialize')
		AND status='acknowledged'`).Scan(&acknowledged); err != nil {
		t.Fatal(err)
	}
	if acknowledged != 2 {
		t.Fatalf("acknowledged lifecycle jobs = %d; want activation and materialization", acknowledged)
	}
}
