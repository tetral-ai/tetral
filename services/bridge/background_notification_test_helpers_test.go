package agentruntimebridge

import (
	"context"
	"net"
	"testing"
	"time"

	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func startBackgroundNotificationQueueServer(t *testing.T, store *queue.PostgreSQLQueueStore) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, tetralqueue.NewServer(store, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient("passthrough:///background-notification-queue",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial background notification Queue: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

type backgroundNotificationTokenSource struct{}

func (backgroundNotificationTokenSource) Token(context.Context) (string, error) {
	return "test-token", nil
}

var _ internalgrpcauth.TokenSource = backgroundNotificationTokenSource{}

type backgroundNotificationMedia struct{}

func (backgroundNotificationMedia) MaterializeResult(_ context.Context, _ tetralsandbox.SandboxExecutionRef, _, _, result string, _ time.Time) (string, error) {
	return result, nil
}

func (backgroundNotificationMedia) RecoverResult(context.Context, tetralsandbox.SandboxExecutionRef) (tetralsandbox.SandboxMediaRecovery, error) {
	return tetralsandbox.SandboxMediaRecovery{}, nil
}

type backgroundNotificationProvider struct {
	taskID string
}

func (*backgroundNotificationProvider) InspectForExecution(context.Context, string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: tetralsandbox.ExecutionReady}
}
func (*backgroundNotificationProvider) InspectForRelease(context.Context, string) tetralsandbox.ProviderOutcome[bool] {
	return tetralsandbox.ProviderOutcome[bool]{Value: true}
}
func (*backgroundNotificationProvider) ResolveActivation(context.Context, tetralsandbox.ActivationResolutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ActivationResolution]{Value: tetralsandbox.ActivationResolution{Found: false}}
}
func (*backgroundNotificationProvider) Activate(context.Context, tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{Value: sandboxmodel.ProviderHandle{
		Provider: sandboxdriver.DaytonaProviderName, SandboxID: "provider_background_notification_e2e",
	}}
}
func (*backgroundNotificationProvider) MaterializeResources(_ context.Context, request tetralsandbox.MaterializationRequest) tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult] {
	expiresAt := time.Now().UTC().Add(2 * time.Hour)
	return tetralsandbox.ProviderOutcome[tetralsandbox.MaterializationResult]{Value: tetralsandbox.MaterializationResult{
		MaterializedEnvironmentGeneration: request.TargetEnvironmentGeneration,
		MaterializedResourceRevision:      request.TargetResourceRevision,
		Resources: sandboxmodel.ResourceSetup{
			ResourceCredExpiresAt: &expiresAt,
			ResourceRootsJSON:     `[]`,
		},
	}}
}
func (*backgroundNotificationProvider) PrepareTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ToolPreparationResult]{Value: tetralsandbox.ToolPreparationResult{}}
}
func (p *backgroundNotificationProvider) ExecuteTool(context.Context, tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{
		ResultJSON: `{"status":"running","result":{"task_id":"` + p.taskID + `"}}`,
		BackgroundTask: &sandboxdriver.BackgroundTask{
			TaskID: p.taskID, ProviderSessionID: "provider_background_e2e",
			ProviderCommandID: p.taskID, ProviderCommandMetadataJSON: `{}`,
		},
	}}
}
func (*backgroundNotificationProvider) ObserveTool(context.Context, sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{}
}
func (*backgroundNotificationProvider) Release(context.Context, tetralsandbox.ReleaseRequest) tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult] {
	return tetralsandbox.ProviderOutcome[tetralsandbox.ReleaseResult]{}
}
func (*backgroundNotificationProvider) PollBackground(context.Context, sandboxdriver.CommandReference) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{Value: sandboxdriver.CommandResult{
		ResultJSON:     `{"status":"completed","result":{"stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":0}}`,
		TerminalStatus: "completed",
	}}
}
func (*backgroundNotificationProvider) SendBackgroundInput(context.Context, sandboxdriver.CommandInput) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}
func (*backgroundNotificationProvider) CancelBackground(context.Context, sandboxdriver.CommandCancel) tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.CommandResult]{}
}

var _ tetralsandbox.ProviderAdapter = (*backgroundNotificationProvider)(nil)
var _ tetralsandbox.BackgroundCommandAdapter = (*backgroundNotificationProvider)(nil)
