package integration

import (
	"context"
	"testing"

	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type bridgeMemoryProjectionProvider struct {
	requests []sandboxdriver.MemoryProjectionRefresh
}

func (*bridgeMemoryProjectionProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: tetralsandbox.ExecutionReady}
}

func (*bridgeMemoryProjectionProvider) InspectForRelease(context.Context, string) tetralsandbox.ProviderOutcome[bool] {
	return tetralsandbox.ProviderOutcome[bool]{Value: true}
}

func (*bridgeMemoryProjectionProvider) ResolveActivation(context.Context, tetralsandbox.ActivationResolutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution]{}
}

func (*bridgeMemoryProjectionProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{}
}

func (*bridgeMemoryProjectionProvider) MaterializeResources(context.Context, tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{}
}

func (*bridgeMemoryProjectionProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{}
}

func (*bridgeMemoryProjectionProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (*bridgeMemoryProjectionProvider) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}

func (*bridgeMemoryProjectionProvider) Release(context.Context, tetralsandbox.ReleaseRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult]{}
}

func (p *bridgeMemoryProjectionProvider) RefreshMemoryProjection(_ context.Context, request sandboxdriver.MemoryProjectionRefresh) tetralsandbox.ProviderOutcome[struct{}] {
	p.requests = append(p.requests, request)
	return tetralsandbox.ProviderOutcome[struct{}]{}
}

func writeDurableOrdinaryToolUseForTest(
	t *testing.T,
	store *agentruntimebridge.PostgreSQLBridgeAPIStore,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	modelToolCallID string,
	toolName string,
	inputJSON string,
) string {
	t.Helper()
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_"+modelRequestID+"_start", modelRequestID, "agent_provider_request", 0)
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_" + modelRequestID + "_tool", ModelRequestId: modelRequestID,
		ToolDeclaration: bridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, "allow"),
	})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("write durable ordinary Tool use: response=%#v err=%v", response, err)
	}
	return response.GetCommitted().GetEventId()
}
