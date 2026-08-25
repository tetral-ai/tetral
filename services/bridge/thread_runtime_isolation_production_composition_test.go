package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func TestPostgreSQLJobRunnerExecutesSiblingThreadsInOneRuntimeSession(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_hot_thread_isolation"
		threadA   = "thr_hot_thread_isolation_a"
		threadB   = "thr_hot_thread_isolation_b"
		bindingID = "bind_hot_thread_isolation"
		podUID    = "pod_hot_thread_isolation"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadA)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadA, threadB)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadA, "evt_hot_thread_a", 1, "user.message", `{"content":[{"type":"text","text":"hold thread A"}]}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadB, "evt_hot_thread_b", 1, "user.message", `{"content":[{"type":"text","text":"complete thread B"}]}`)
	jobs := []RuntimeJob{
		{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadA, RuntimeInputID: "rin_hot_thread_a", InputKind: "messages", EventIDs: []string{"evt_hot_thread_a"}, SequenceFrom: 1, SequenceTo: 1,
			PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadA + `","runtime_input_id":"rin_hot_thread_a","event_ids":["evt_hot_thread_a"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`},
		{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadB, RuntimeInputID: "rin_hot_thread_b", InputKind: "messages", EventIDs: []string{"evt_hot_thread_b"}, SequenceFrom: 1, SequenceTo: 1,
			PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadB + `","runtime_input_id":"rin_hot_thread_b","event_ids":["evt_hot_thread_b"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`},
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	for _, job := range jobs {
		seedRuntimeInboxBirthForJob(t, admin, job)
		enqueueRuntimeCompositionJob(t, queueStore, sessionID, job, 0)
	}

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	bridgeStore.RuntimeBindingTokenHMACKey = []byte("hot-thread-isolation-signing-key")
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for hot Thread Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})

	tempDir := t.TempDir()
	readyPath := tempDir + "/ready.json"
	toolStartedPath := tempDir + "/thread-a-tool-started"
	operationCompletedPath := tempDir + "/thread-a-operation-completed"
	acceptResultPath := tempDir + "/accept-result.json"
	closePath := tempDir + "/close"
	inputPath := tempDir + "/input.json"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": bridgeListener.Addr().String(), "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": threadA, "bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID,
		"readyPath": readyPath, "toolStartedPath": toolStartedPath, "acceptResultPath": acceptResultPath,
		"durableOperationCompletedPath": operationCompletedPath, "closePath": closePath,
		"fastThreadText": "complete thread B",
	})
	if err != nil {
		t.Fatalf("encode hot Thread Runtime input: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write hot Thread Runtime input: %v", err)
	}
	var output bytes.Buffer
	command := exec.Command("bun", "packages/runtime-pod/test/fixtures/interrupt-closeout-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start hot Thread Runtime: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	var ready struct {
		Port int `json:"port"`
	}
	waitForJSONFile(t, readyPath, &ready, "hot Thread Runtime readiness", &output)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align hot Thread Runtime binding: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtimeDB)
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, ready.Port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "hot-thread-isolation", MaxJobs: 2, LeaseDuration: time.Minute, HeartbeatInterval: time.Second},
	}
	runDone := make(chan error, 1)
	go func() { runDone <- runner.RunOnce(context.Background()) }()
	waitForCompositionFile(t, toolStartedPath, "Thread A Tool execution", &output)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var requestEnds int
		var queueStatus string
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'),
			(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3)`,
			sessionID, threadB, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, jobs[1].RuntimeInputID)).Scan(&requestEnds, &queueStatus); err == nil && requestEnds == 1 && queueStatus == queue.StatusAcknowledged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var threadBEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'`, sessionID, threadB).Scan(&threadBEnds); err != nil || threadBEnds != 1 {
		t.Fatalf("Thread B did not finish while Thread A Tool was blocked: ends=%d err=%v output=%s", threadBEnds, err, output.String())
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run concurrent Thread jobs: %v; output=%s", err, output.String())
	}

	target := RuntimePodTarget{
		Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1", Port: ready.Port,
	}
	sender := NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})
	configResponse, err := sender.ApplyRuntimeConfig(context.Background(), target, &agentruntimev1.ApplyRuntimeConfigRequest{
		WorkspaceId: "default", SessionId: sessionID, BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID,
		Config: &agentruntimev1.ApplyRuntimeConfigRequest_SessionConfig{SessionConfig: &agentruntimev1.RuntimeSessionConfig{
			Generation: 2, ContentJson: `{"approval_mode":"full_access","tool_policy":{"tools":[]}}`,
		}},
	})
	if err != nil || configResponse.GetRejected().GetReason() != agentruntimev1.ApplyRuntimeConfigFailure_APPLY_RUNTIME_CONFIG_FAILURE_CONTROL_BUSY || !configResponse.GetRejected().GetRetryable() {
		t.Fatalf("config while Thread A hot = %#v/%v; want retryable control busy", configResponse, err)
	}
	cleanupResponse, err := sender.CleanupSession(context.Background(), target, &agentruntimev1.CleanupSessionRequest{
		WorkspaceId: "default", SessionId: sessionID, BindingId: bindingID, BindingGeneration: 1, TargetPodUid: podUID,
		CleanupOperationId: "cleanup_hot_thread_isolation", Reason: agentruntimev1.CleanupSessionReason_CLEANUP_SESSION_REASON_EXPIRED,
	})
	if err != nil || cleanupResponse.GetRejected().GetReason() != agentruntimev1.CleanupSessionFailure_CLEANUP_SESSION_FAILURE_SESSION_BUSY || !cleanupResponse.GetRejected().GetRetryable() {
		t.Fatalf("cleanup while Thread A hot = reason:%s retryable:%t err:%v; want retryable session busy",
			cleanupResponse.GetRejected().GetReason(), cleanupResponse.GetRejected().GetRetryable(), err)
	}

	followupEventID := "evt_hot_thread_a_followup"
	followupSequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadA)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadA, followupEventID, followupSequence, "user.message", `{"content":[{"type":"text","text":"join existing run"}]}`)
	followup := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadA,
		RuntimeInputID: "rin_hot_thread_a_followup", InputKind: "messages", EventIDs: []string{followupEventID},
		SequenceFrom: followupSequence, SequenceTo: followupSequence,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadA + `","runtime_input_id":"rin_hot_thread_a_followup","event_ids":["` + followupEventID + `"],"sequence_from":` + fmt.Sprint(followupSequence) + `,"sequence_to":` + fmt.Sprint(followupSequence) + `,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, followup)
	enqueueRuntimeCompositionJob(t, queueStore, sessionID, followup, 0)
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("deliver second same-Thread input: %v", err)
	}
	var followupQueueStatus string
	var threadAStartsAfterFollowup int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start')`,
		sessionID, threadA, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, followup.RuntimeInputID)).Scan(&followupQueueStatus, &threadAStartsAfterFollowup); err != nil {
		t.Fatalf("read second same-Thread delivery: %v", err)
	}
	if followupQueueStatus != queue.StatusAcknowledged || threadAStartsAfterFollowup != 1 {
		t.Fatalf("second same-Thread input = Queue:%s starts:%d; want acknowledged and one existing run", followupQueueStatus, threadAStartsAfterFollowup)
	}
	var threadAStarts, threadAToolUses, threadAEnds int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end')`,
		sessionID, threadA).Scan(&threadAStarts, &threadAToolUses, &threadAEnds); err != nil {
		t.Fatalf("read blocked Thread A facts: %v", err)
	}
	if threadAStarts != 1 || threadAToolUses != 1 || threadAEnds != 0 {
		t.Fatalf("blocked Thread A facts = starts:%d ToolUses:%d ends:%d; want 1/1/0", threadAStarts, threadAToolUses, threadAEnds)
	}
	if err := os.WriteFile(closePath, []byte("close"), 0o600); err != nil {
		t.Fatalf("close hot Thread Runtime: %v", err)
	}
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build hot Thread output-capture registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(client),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "hot-thread-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete hot Thread output capture: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("hot Thread completion did not enqueue output capture: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("wait hot Thread Runtime: %v; output=%s", err, output.String())
	}
	var result struct {
		ProviderInvocations int `json:"providerInvocations"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode hot Thread Runtime output: %v; output=%s", err, output.String())
	}
	if result.ProviderInvocations != 2 {
		t.Fatalf("hot Thread provider invocations = %d; want one blocked A and one completed B", result.ProviderInvocations)
	}
}

func waitForJSONFile(t *testing.T, path string, target any, description string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && json.Unmarshal(raw, target) == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", description, output.String())
}
