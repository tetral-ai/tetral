package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

// Invoke the owning command preparation and observe its returned wire.
func prepareFixtureConfigPayload(ctx context.Context, client *dbconnect.Client, job jobrunner.RuntimeJob) (string, error) {
	plan, err := jobrunner.NewPostgreSQLRuntimeDeliveryStore(client, 9090).PrepareRuntimeCommand(ctx, job)
	if err != nil {
		return "", err
	}
	if plan.RuntimeConfig == nil {
		return "", errors.New("configuration command absent")
	}
	if plan.RuntimeConfig.GetSessionConfig() != nil {
		return plan.RuntimeConfig.GetSessionConfig().GetContentJson(), nil
	}
	if plan.RuntimeConfig.GetMcpManifest() != nil {
		return plan.RuntimeConfig.GetMcpManifest().GetContentJson(), nil
	}
	return "", errors.New("configuration wire absent")
}

// RuntimeScope here is fixture input assembled from the attempt returned by the
// production Runner. It performs no authority validation or payload derivation.
func observedAttemptScope(job jobrunner.RuntimeJob, attempt jobrunner.RuntimeAttemptedBinding) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID, Binding: &bridgev1.RuntimeBindingRef{BindingId: attempt.BindingID, BindingGeneration: attempt.Generation, TargetPodUid: attempt.TargetPodUID}}
}

// A replacement cold scope comes from a real Runner-created binding. The
// eligibility snapshot supplies the replacement Pod, never a binding row.
func declareReplacementScope(t *testing.T, client *dbconnect.Client, previous *bridgev1.RuntimeScope) *bridgev1.RuntimeScope {
	store := jobrunner.NewJobRunnerRuntimeDeliveryStore(client, nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090}, func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-replacement", PodUID: "pod_replacement", PodIP: "10.255.0.10"}})
	})
	plan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: previous.GetWorkspaceId(), SessionID: previous.GetSessionId(), ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + previous.GetSessionId() + ":1"})
	if err != nil || plan.RuntimeConfig == nil || plan.AttemptedBinding.BindingID == "" {
		t.Fatalf("declare replacement Runtime binding: %#v/%v", plan, err)
	}
	return &bridgev1.RuntimeScope{WorkspaceId: previous.GetWorkspaceId(), SessionId: previous.GetSessionId(), SessionThreadId: previous.GetSessionThreadId(), Binding: &bridgev1.RuntimeBindingRef{BindingId: plan.AttemptedBinding.BindingID, BindingGeneration: plan.AttemptedBinding.Generation, TargetPodUid: plan.AttemptedBinding.TargetPodUID}}
}
