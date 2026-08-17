package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type interruptRuntimeCompositionProcess struct {
	command *exec.Cmd
	output  bytes.Buffer
	port    int
}

type interruptRuntimeCompositionOutput struct {
	InterruptResult             json.RawMessage `json:"interruptResult"`
	ProviderInvocations         int             `json:"providerInvocations"`
	DurableOperationCompletions int             `json:"durableOperationCompletions"`
}

func TestPostgreSQLInterruptCrossesRuntimeFiberAndBridgeCloseoutWithinDeliveryTimeout(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_interrupt_production_composition"
		threadID     = "thr_interrupt_production_composition"
		bindingID    = "bind_interrupt_production_composition"
		podUID       = "pod_interrupt_production_composition"
		messageID    = "rin_interrupt_production_message"
		messageEvent = "evt_interrupt_production_message"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, messageEvent, 1, "user.message", `{"content":[{"type":"text","text":"run the durable operation"}]}`)
	messageJob := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: messageID, InputKind: "messages", EventIDs: []string{messageEvent}, SequenceFrom: 1, SequenceTo: 1,
		PayloadJSON: `{"workspace_id":"default","session_id":"` + sessionID + `","session_thread_id":"` + threadID + `","runtime_input_id":"` + messageID + `","event_ids":["` + messageEvent + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`,
	}
	seedRuntimeInboxBirthForJob(t, admin, messageJob)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueRuntimeCompositionJob(t, queueStore, sessionID, messageJob, 0)

	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	bridgeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for interrupt composition Bridge: %v", err)
	}
	bridgeServer := grpc.NewServer()
	RegisterBridgeAPI(bridgeServer, bridgeStore)
	go func() { _ = bridgeServer.Serve(bridgeListener) }()
	t.Cleanup(func() {
		bridgeServer.Stop()
		_ = bridgeListener.Close()
	})

	tempDir := t.TempDir()
	runtimeProcess, paths := startInterruptRuntimeComposition(t, tempDir, bridgeListener.Addr().String(), sessionID, threadID, bindingID, podUID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings
		SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1 AND binding_id=$2`, sessionID, bindingID); err != nil {
		t.Fatalf("align Runtime binding: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), runtimeProcess.port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: NewRuntimePodCommandClient(taskNotificationRuntimeTokenSource{})},
		Config:    JobRunnerConfig{LeaseOwner: "interrupt-production-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver initial input through Runtime gRPC = active:%t err:%v", active, err)
	}
	if rawAccept, err := os.ReadFile(paths.acceptResult); err == nil {
		t.Logf("Runtime accept result: %s", rawAccept)
	}
	var diagnosticInbox string
	var diagnosticMessages, diagnosticStarts, diagnosticToolUses int
	diagnosticDeadline := time.Now().Add(10 * time.Second)
	for {
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='span.model_request_start'),
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='agent.tool_use')`,
			messageID, sessionID).Scan(&diagnosticInbox, &diagnosticMessages, &diagnosticStarts, &diagnosticToolUses); err != nil {
			t.Fatalf("read initial Runtime composition diagnostics: %v", err)
		}
		if diagnosticStarts == 1 && diagnosticToolUses == 1 {
			break
		}
		if time.Now().After(diagnosticDeadline) {
			t.Fatalf("Runtime did not persist the open Request and pending Tool: Inbox=%s Messages=%d starts=%d toolUses=%d: %s",
				diagnosticInbox, diagnosticMessages, diagnosticStarts, diagnosticToolUses, runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("initial Runtime composition Inbox=%s Messages=%d starts=%d toolUses=%d", diagnosticInbox, diagnosticMessages, diagnosticStarts, diagnosticToolUses)
	waitForCompositionFile(t, paths.toolStarted, "durable Runtime operation", &runtimeProcess.output)

	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime)))
	interruptBirth, err := eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID,
		"interrupt-production-composition", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
			Type: sessionevent.EventTypeUserInterrupt, SessionThreadID: threadID,
		}}})
	if err != nil || len(interruptBirth.Data) != 1 {
		t.Fatalf("birth interrupt through SessionEvent owner = %#v/%v", interruptBirth, err)
	}
	interruptEvent := interruptBirth.Data[0].ID
	var interruptID string
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'
		  AND event_ids_json::jsonb ? $2`, sessionID, interruptEvent).Scan(&interruptID); err != nil {
		t.Fatalf("read production-born interrupt Inbox: %v", err)
	}

	type deliveryResult struct {
		active  bool
		err     error
		elapsed time.Duration
	}
	delivery := make(chan deliveryResult, 1)
	started := time.Now()
	go func() {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		delivery <- deliveryResult{active: active, err: runErr, elapsed: time.Since(started)}
	}()

	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build output capture provider registry: %v", err)
	}
	captureRunner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queueStore, nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtime)),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "interrupt-production-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	captureDeadline := time.Now().Add(10 * time.Second)
	for {
		active, captureErr := captureRunner.RunOnceWithActivity(context.Background())
		if captureErr != nil {
			t.Fatalf("complete interrupt output capture operation: %v", captureErr)
		}
		if active {
			break
		}
		if time.Now().After(captureDeadline) {
			t.Fatalf("interrupt closeout did not enqueue output capture: %s", runtimeProcess.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	var delivered deliveryResult
	select {
	case delivered = <-delivery:
	case <-time.After(10 * time.Second):
		t.Fatalf("interrupt delivery did not finish after output capture: %s", runtimeProcess.output.String())
	}
	if delivered.err != nil || !delivered.active {
		t.Fatalf("deliver interrupt through Runtime closeout = active:%t err:%v", delivered.active, delivered.err)
	}
	elapsed := delivered.elapsed
	if elapsed >= runtimeInterruptDeliveryTimeout {
		t.Fatalf("production interrupt closeout elapsed %s; delivery timeout is %s: %s", elapsed, runtimeInterruptDeliveryTimeout, runtimeProcess.output.String())
	}
	waitForCompositionFile(t, paths.operationCompleted, "interrupted durable operation completion", &runtimeProcess.output)
	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release Runtime composition: %v", err)
	}
	composed := runtimeProcess.wait(t)
	if composed.ProviderInvocations != 1 || composed.DurableOperationCompletions != 1 || len(composed.InterruptResult) == 0 {
		t.Fatalf("Runtime composition = %+v; want one Provider, one joined operation, and interrupt response", composed)
	}

	var inboxStatus, queueStatus string
	var starts, ends, toolUses, toolResults, receipts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='agent.tool_use'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='agent.tool_result'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
		 AND operation IN ('commit_inputs','write_request_end') AND source_kind='interrupt_control' AND idempotency_key=$1 AND receipt_json <> '')`,
		interruptID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptID), sessionID,
	).Scan(&inboxStatus, &queueStatus, &starts, &ends, &toolUses, &toolResults, &receipts); err != nil {
		t.Fatalf("read interrupt production closeout: %v", err)
	}
	if inboxStatus != "committed" || queueStatus != queue.StatusAcknowledged || starts != 1 || ends != 1 || toolUses != 1 || toolResults != 1 || receipts != 1 {
		t.Fatalf("production closeout = Inbox:%s Queue:%s starts:%d ends:%d tool uses/results:%d/%d receipts:%d elapsed:%s",
			inboxStatus, queueStatus, starts, ends, toolUses, toolResults, receipts, elapsed)
	}
	t.Logf("production Runtime interrupt closeout elapsed=%s", elapsed)
}

func enqueueRuntimeCompositionJob(t *testing.T, store *queue.PostgreSQLQueueStore, sessionID string, job RuntimeJob, priority int) {
	t.Helper()
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, job.RuntimeInputID),
		PayloadVersion: 1, PayloadJSON: []byte(job.PayloadJSON), Priority: priority,
		MaxAttempts: queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("enqueue Runtime composition job %s: %v", job.RuntimeInputID, err)
	}
}

type interruptRuntimeCompositionPaths struct {
	toolStarted        string
	operationCompleted string
	acceptResult       string
	close              string
}

func startInterruptRuntimeComposition(t *testing.T, tempDir, bridgeAddress, sessionID, threadID, bindingID, podUID string) (*interruptRuntimeCompositionProcess, interruptRuntimeCompositionPaths) {
	t.Helper()
	readyPath := tempDir + "/ready.json"
	paths := interruptRuntimeCompositionPaths{
		toolStarted: tempDir + "/tool-started", operationCompleted: tempDir + "/operation-completed",
		acceptResult: tempDir + "/accept-result.json", close: tempDir + "/close",
	}
	inputPath := tempDir + "/input.json"
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": bridgeAddress, "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": threadID, "bindingId": bindingID, "bindingGeneration": 1,
		"targetPodUid": podUID, "readyPath": readyPath, "toolStartedPath": paths.toolStarted,
		"acceptResultPath":              paths.acceptResult,
		"durableOperationCompletedPath": paths.operationCompleted, "closePath": paths.close,
	})
	if err != nil {
		t.Fatalf("encode interrupt Runtime composition: %v", err)
	}
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write interrupt Runtime composition: %v", err)
	}
	process := &interruptRuntimeCompositionProcess{}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/interrupt-closeout-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start interrupt Runtime composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(readyPath)
		if readErr == nil {
			var ready struct {
				Port int `json:"port"`
			}
			if json.Unmarshal(raw, &ready) == nil && ready.Port > 0 {
				process.port = ready.Port
				return process, paths
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("interrupt Runtime composition did not become ready: %s", process.output.String())
	return nil, interruptRuntimeCompositionPaths{}
}

func waitForCompositionFile(t *testing.T, path, description string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", description, output.String())
}

func (p *interruptRuntimeCompositionProcess) wait(t *testing.T) interruptRuntimeCompositionOutput {
	t.Helper()
	if err := p.command.Wait(); err != nil {
		t.Fatalf("interrupt Runtime composition: %v: %s", err, p.output.String())
	}
	var output interruptRuntimeCompositionOutput
	if err := json.Unmarshal(p.output.Bytes(), &output); err != nil {
		t.Fatalf("decode interrupt Runtime composition: %v: %s", err, p.output.String())
	}
	return output
}
