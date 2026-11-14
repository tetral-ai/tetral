package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func settleSandboxExecutionForHotReceiptProof(
	t *testing.T,
	runtimeDB *sql.DB,
	adminDB *sql.DB,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	resultJSON string,
) {
	t.Helper()
	sessionfixture.SeedReadySandboxForSharedToolExecution(t, adminDB, scope.GetWorkspaceId(), scope.GetSessionId())
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	queueConnection := startBackgroundNotificationQueueServer(t, queueStore)
	provider := &hotReceiptSandboxProvider{
		bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{},
		resultJSON:                     resultJSON,
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		sandboxdriver.DaytonaProviderName: provider,
	})
	if err != nil {
		t.Fatalf("create Sandbox provider registry: %v", err)
	}
	runner := &tetralsandbox.SandboxToolExecutionJobRunner{
		Queue:       tetralsandbox.SandboxQueueFromGRPC(queuev1.NewQueueServiceClient(queueConnection)),
		Coordinator: tetralsandbox.NewPostgreSQLSandboxExecutionCoordinator(dbconnect.NewClientForTesting(runtimeDB), 30*time.Minute),
		Providers:   registry,
		Media:       backgroundNotificationMedia{},
		Config: tetralsandbox.SandboxToolExecutionRunnerConfig{
			WorkspaceID: scope.GetWorkspaceId(), LeaseOwner: "hot-receipt-proof", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second, PreparationTimeout: 45 * time.Second,
		},
	}
	active, err := runner.RunOnceWithActivity(context.Background())
	if err != nil || !active {
		t.Fatalf("run Sandbox execution through its production owner = active %v, error %v; want true, nil", active, err)
	}
}

type hotReceiptSandboxProvider struct {
	*bridgeMemoryProjectionProvider
	resultJSON string
}

func (*hotReceiptSandboxProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}

func (p *hotReceiptSandboxProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: p.resultJSON}}
}
