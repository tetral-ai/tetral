package integration

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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
	return &bridgev1.RuntimeScope{WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, SessionThreadId: job.SessionThreadID, Binding: &bridgev1.RuntimeBindingRef{BindingId: attempt.BindingID, BindingGeneration: attempt.Generation, TargetPodUid: attempt.TargetPodUID, RuntimeProcessId: attempt.RuntimeProcessID}}
}

// A replacement cold scope comes from a real Runner-created binding. The
// eligibility snapshot supplies the replacement Pod, never a binding row.
func declareReplacementScope(t *testing.T, client *dbconnect.Client, previous *bridgev1.RuntimeScope) *bridgev1.RuntimeScope {
	seedFixtureRuntimeProcess(t, client, "tetral-agent-runtime", "pod_replacement")
	store := jobrunner.NewJobRunnerRuntimeDeliveryStore(client, nil, jobrunner.JobRunnerConfig{AgentRuntimeGRPCPort: 9090}, func() kubernetes.BindingVisibilitySnapshot {
		return kubernetes.NewBindingVisibilitySnapshotForTest(true, []kubernetes.BindingCandidate{{Namespace: "tetral-agent-runtime", PodName: "runtime-replacement", PodUID: "pod_replacement", PodIP: "10.255.0.10"}})
	})
	installFixtureRuntimeLoad(t, store)
	plan, err := store.PrepareRuntimeCommand(context.Background(), jobrunner.RuntimeJob{Kind: queue.KindRuntimeConfigUpdate, WorkspaceID: previous.GetWorkspaceId(), SessionID: previous.GetSessionId(), ConfigGeneration: "1", RuntimeInputID: "runtime_config_update:" + previous.GetSessionId() + ":1"})
	if err != nil || plan.RuntimeConfig == nil || plan.AttemptedBinding.BindingID == "" {
		t.Fatalf("declare replacement Runtime binding: %#v/%v", plan, err)
	}
	return &bridgev1.RuntimeScope{WorkspaceId: previous.GetWorkspaceId(), SessionId: previous.GetSessionId(), SessionThreadId: previous.GetSessionThreadId(), Binding: &bridgev1.RuntimeBindingRef{BindingId: plan.AttemptedBinding.BindingID, BindingGeneration: plan.AttemptedBinding.Generation, TargetPodUid: plan.AttemptedBinding.TargetPodUID, RuntimeProcessId: plan.AttemptedBinding.RuntimeProcessID}}
}

// Legacy business compositions supply a ready, under-capacity HTTP endpoint.
// Production performs its real bounded metrics request outside the Session
// transaction; only the local dial destination is mapped to this fixture.
func fixtureRuntimeLoadClient(t *testing.T) *http.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "runtimepod_active_sessions 1\nruntimepod_session_capacity 8\nruntimepod_container_memory_usage_bytes 100\nruntimepod_container_memory_limit_bytes 1000\nruntimepod_ready 1\nruntimepod_accepting_commands 1\n")
	}))
	t.Cleanup(server.Close)
	address := strings.TrimPrefix(server.URL, "http://")
	transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport}
}
func installFixtureRuntimeLoad(t *testing.T, store *jobrunner.PostgreSQLRuntimeDeliveryStore) {
	t.Helper()
	resolver, ok := store.TargetResolver.(jobrunner.KubernetesRuntimeTargetResolver)
	if !ok {
		t.Fatalf("fixture expected production Kubernetes target resolver, got %T", store.TargetResolver)
	}
	resolver.LoadClient = fixtureRuntimeLoadClient(t)
	store.TargetResolver = resolver
}
