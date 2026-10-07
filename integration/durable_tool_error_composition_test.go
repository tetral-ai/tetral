package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
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

func runRuntimeProviderComposition(t *testing.T, contextJSON string) []json.RawMessage {
	t.Helper()
	input, err := json.Marshal(map[string]any{"contextJson": contextJSON, "providerComposition": true})
	if err != nil {
		t.Fatalf("encode Runtime provider composition: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-provider-composition.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Runtime provider composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", inputPath) //nolint:gosec // Fixed production composition fixture and test-owned input.
	command.Dir = "../services/agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run Runtime provider composition: %v: %s", err, output)
	}
	var result struct {
		ProviderComposition struct {
			Strategies []struct {
				ProviderFamily string `json:"providerFamily"`
				Validation     struct {
					Ok bool `json:"ok"`
				} `json:"validation"`
				ProviderRequest json.RawMessage `json:"providerRequest"`
			} `json:"strategies"`
		} `json:"providerComposition"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode Runtime provider composition: %v: %s", err, output)
	}
	requests := make([]json.RawMessage, 0, len(result.ProviderComposition.Strategies))
	for _, strategy := range result.ProviderComposition.Strategies {
		if !strategy.Validation.Ok || len(strategy.ProviderRequest) == 0 {
			t.Fatalf("Runtime Provider composition for %s was invalid or absent: %s", strategy.ProviderFamily, output)
		}
		requests = append(requests, strategy.ProviderRequest)
	}
	if len(requests) == 0 {
		t.Fatal("Runtime Provider composition returned no strategies")
	}
	return requests
}

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
