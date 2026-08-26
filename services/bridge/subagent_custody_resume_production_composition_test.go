package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

type closedThreadResumeCompositionResult struct {
	Raw    string `json:"-"`
	Result struct {
		Type string `json:"type"`
	} `json:"result"`
	Inspected struct {
		OK       bool   `json:"ok"`
		Observed bool   `json:"observed"`
		Status   string `json:"status"`
	} `json:"inspected"`
	Checkpoint struct {
		PendingInputSequences []int64 `json:"pendingInputContextSequences"`
	} `json:"checkpoint"`
	Decision struct {
		State struct {
			State string `json:"state"`
		} `json:"state"`
		NextStep struct {
			Action string `json:"action"`
		} `json:"nextStep"`
	} `json:"decision"`
	ContextEntries   []bridgeRuntimeContextEntry `json:"contextEntries"`
	TurnFacts        bridgeLoadContextTurnFacts  `json:"turnFacts"`
	ProviderRequests int                         `json:"providerRequests"`
	RuntimeEvents    int                         `json:"runtimeEvents"`
}

type closedThreadResumeRuntimeProcess struct {
	command             *exec.Cmd
	output              bytes.Buffer
	port                int
	acceptResultPath    string
	providerStartedPath string
	closePath           string
}

func runClosedThreadResumeProductionComposition(t *testing.T, runtime *sql.DB, input map[string]any) closedThreadResumeCompositionResult {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("closed-thread-resume-composition-key")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for closed Thread resume composition: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	input["address"] = listener.Addr().String()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode closed Thread resume input: %v", err)
	}
	inputPath := filepath.Join(t.TempDir(), "closed-thread-resume.json")
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write closed Thread resume input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/closed-thread-resume-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run closed Thread through Runtime resume owner: %v: %s", err, output)
	}
	var result closedThreadResumeCompositionResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode closed Thread resume result: %v: %s", err, output)
	}
	result.Raw = string(output)
	return result
}

func startClosedThreadResumeProductionComposition(t *testing.T, runtime *sql.DB, input map[string]any) (*closedThreadResumeRuntimeProcess, closedThreadResumeCompositionResult) {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("closed-thread-resume-composition-key")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for serving closed Thread resume composition: %v", err)
	}
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready.json")
	resumeResultPath := filepath.Join(tempDir, "resume-result.json")
	acceptResultPath := filepath.Join(tempDir, "accept-result.json")
	process := &closedThreadResumeRuntimeProcess{
		acceptResultPath:    acceptResultPath,
		providerStartedPath: filepath.Join(tempDir, "provider-started.json"),
		closePath:           filepath.Join(tempDir, "close"),
	}
	input["address"] = listener.Addr().String()
	input["readyPath"] = readyPath
	input["resumeResultPath"] = resumeResultPath
	input["acceptResultPath"] = acceptResultPath
	input["providerStartedPath"] = process.providerStartedPath
	input["closePath"] = process.closePath
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encode serving closed Thread resume input: %v", err)
	}
	inputPath := filepath.Join(tempDir, "closed-thread-resume.json")
	if err := os.WriteFile(inputPath, encoded, 0o600); err != nil {
		t.Fatalf("write serving closed Thread resume input: %v", err)
	}
	process.command = exec.Command("bun", "packages/runtime-pod/test/fixtures/closed-thread-resume-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	process.command.Dir = "../agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatalf("start serving closed Thread resume composition: %v", err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	var result closedThreadResumeCompositionResult
	for time.Now().Before(deadline) {
		readyJSON, readyErr := os.ReadFile(readyPath)
		resumeJSON, resumeErr := os.ReadFile(resumeResultPath)
		var ready struct {
			Port int `json:"port"`
		}
		if readyErr == nil && resumeErr == nil && json.Unmarshal(readyJSON, &ready) == nil && ready.Port > 0 && json.Unmarshal(resumeJSON, &result) == nil {
			process.port = ready.Port
			result.Raw = string(resumeJSON)
			return process, result
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("serving closed Thread resume composition did not become ready: %s", process.output.String())
	return nil, closedThreadResumeCompositionResult{}
}

func (p *closedThreadResumeRuntimeProcess) providerStart(t *testing.T, admin *sql.DB, sessionID, threadID string) struct {
	ProviderRequests int             `json:"providerRequests"`
	Request          json.RawMessage `json:"request"`
} {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(p.providerStartedPath)
		if err == nil {
			var started struct {
				ProviderRequests int             `json:"providerRequests"`
				Request          json.RawMessage `json:"request"`
			}
			var request struct {
				SessionThreadID string `json:"sessionThreadId"`
			}
			if json.Unmarshal(raw, &started) == nil &&
				json.Unmarshal(started.Request, &request) == nil &&
				request.SessionThreadID == threadID {
				return started
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	accepted, _ := os.ReadFile(p.acceptResultPath)
	var events string
	_ = admin.QueryRowContext(context.Background(), `SELECT coalesce(string_agg(sequence::text || ':' || type || ':' || payload_json, ',' ORDER BY sequence),'')
		FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&events)
	t.Fatalf("resumed Runtime Provider invocation did not start: accepted=%s events=%s output=%s", accepted, events, p.output.String())
	return struct {
		ProviderRequests int             `json:"providerRequests"`
		Request          json.RawMessage `json:"request"`
	}{}
}

func (p *closedThreadResumeRuntimeProcess) close(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(p.closePath, []byte("close"), 0o600); err != nil {
		t.Fatalf("release serving closed Thread resume composition: %v", err)
	}
	if err := p.command.Wait(); err != nil {
		t.Fatalf("wait for serving closed Thread resume composition: %v: %s", err, p.output.String())
	}
}

func assertQuiescentClosedThreadResume(t *testing.T, result closedThreadResumeCompositionResult) {
	t.Helper()
	if result.Result.Type != "completed" || !result.Inspected.OK || !result.Inspected.Observed ||
		result.Inspected.Status != "idle" || len(result.Checkpoint.PendingInputSequences) != 0 ||
		result.Decision.State.State != "idle" || result.Decision.NextStep.Action != "await_input" ||
		result.ProviderRequests != 0 || result.RuntimeEvents != 0 {
		t.Fatalf("closed Thread resume = %s; want completed idle/await_input with no pending input, Provider request, or Runtime write", result.Raw)
	}
}

func seedChildResumeRoute(t *testing.T, admin *sql.DB, sessionID, parentID, sourceID string) {
	t.Helper()
	seedActorSourceEvent(t, admin, sessionID, parentID, sourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","evaluated_permission":"allow"}`)
	modelRequestID := "mreq_" + sourceID
	modelToolCallID := "call_" + sourceID
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$3,projection_json=$4
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
		sessionID, sourceID, modelRequestID, `{"model_tool_call_id":"`+modelToolCallID+`"}`); err != nil {
		t.Fatalf("project resume source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, modelRequestID, sourceID, modelToolCallID, "resume_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
}

func TestSubagentFirstMailRemainsOwnedAfterLocalAdmissionRejection(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_subagent_first_mail_custody"
		parentID  = "thr_subagent_first_mail_custody_parent"
		bindingID = "bind_subagent_first_mail_custody"
		podUID    = "pod_subagent_first_mail_custody"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_subagent_first_mail_parent", "evt_subagent_first_mail_parent", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"spawn the custody worker"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, parentID); err != nil {
		t.Fatalf("seed spawning parent context: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-first_mail-custody-signing-key")
	spawned := runSubagentProductionComposition(
		t, BridgeAPIServer{store: store}, sessionID, parentID, bindingID, 1, podUID,
		"custody-worker", "execute the first_mail task exactly once", "all",
	)
	var childID, runtimeInputID, jobID string
	if err := admin.QueryRowContext(context.Background(), `SELECT child.id,inbox.runtime_input_id,job.id
		FROM session_threads child
		JOIN session_runtime_inbox inbox ON inbox.workspace_id=child.workspace_id AND inbox.session_id=child.session_id AND inbox.session_thread_id=child.id
		JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id
		 AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE child.workspace_id='default' AND child.session_id=$1 AND child.parent_thread_id=$2
		 AND child.role='subagent' AND inbox.input_kind='agent_mail'`, sessionID, parentID).Scan(&childID, &runtimeInputID, &jobID); err != nil {
		t.Fatalf("read production first_mail custody: %v", err)
	}

	rejectingRuntime := startAttachmentRecoveryRuntime(
		t, spawned.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID, true,
	)
	runSubagentRuntimeQueueOnce(t, runtimeDB, admin, rejectingRuntime.port, sessionID, podUID)
	rejectionReason, rejectedProviderInvocations, rejectionJSON := readAgentMailAdmission(t, rejectingRuntime)
	if rejectionReason != "local_session_capacity_exceeded" || rejectedProviderInvocations != 0 {
		t.Fatalf("local first_mail admission = reason:%q providers:%d result:%s; want local_session_capacity_exceeded/0", rejectionReason, rejectedProviderInvocations, rejectionJSON)
	}
	var firstInbox, firstQueue, childStatus string
	var firstAttempts, requests int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		runtimeInputID, jobID, sessionID, childID).Scan(&firstInbox, &firstQueue, &firstAttempts, &childStatus, &requests); err != nil {
		t.Fatalf("read custody after local admission rejection: %v", err)
	}
	if firstInbox != "committed" || firstQueue != queue.StatusPending || firstAttempts != 1 || childStatus != "idle" || requests != 0 {
		t.Fatalf("rejected first_mail custody = Inbox:%s Queue:%s attempts:%d child:%s requests:%d; want committed/pending/1/idle/0",
			firstInbox, firstQueue, firstAttempts, childStatus, requests)
	}
	rejectingRuntime.kill(t)

	acceptingRuntime := startAttachmentRecoveryRuntime(
		t, spawned.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID,
	)
	waitForQueueJobAvailable(t, admin, jobID)
	runSubagentRuntimeQueueOnce(t, runtimeDB, admin, acceptingRuntime.port, sessionID, podUID)
	var retryInbox, retryQueue string
	var retryAttempts, retryStarts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		runtimeInputID, jobID, sessionID, childID).Scan(&retryInbox, &retryQueue, &retryAttempts, &retryStarts); err != nil {
		t.Fatalf("read retry custody: %v", err)
	}
	if retryInbox != "accepted" || retryQueue != queue.StatusAcknowledged {
		t.Fatalf("retry custody = Inbox:%s Queue:%s attempts:%d RequestStarts:%d; want accepted/acknowledged before execution completion",
			retryInbox, retryQueue, retryAttempts, retryStarts)
	}
	started := acceptingRuntime.providerStart(t)
	waitForThreadRequestEnds(t, admin, sessionID, childID, 1)
	acceptingRuntime.kill(t)
	var finalInbox, finalQueue string
	var finalAttempts, messages, starts int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		runtimeInputID, jobID, sessionID, childID).Scan(&finalInbox, &finalQueue, &finalAttempts, &messages, &starts); err != nil {
		t.Fatalf("read converged first_mail custody: %v", err)
	}
	if finalInbox != "accepted" || finalQueue != queue.StatusAcknowledged || finalAttempts != 2 || messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("converged first_mail custody = Inbox:%s Queue:%s attempts:%d messages:%d starts:%d providers:%d",
			finalInbox, finalQueue, finalAttempts, messages, starts, started.ProviderInvocations)
	}
}

func TestSubagentFirstMailExhaustionFailsOnlyExactChild(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_first_mail_final_failure"
		parentID  = "thr_first_mail_final_parent"
		bindingID = "bind_first_mail_final_failure"
		podUID    = "pod_first_mail_final_failure"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_first_mail_final_parent", "evt_first_mail_final_parent", 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("first_mail-final-failure-signing-key")
	spawned := runSubagentProductionComposition(
		t, BridgeAPIServer{store: store}, sessionID, parentID, bindingID, 1, podUID,
		"failure-worker", "reach the real Runtime admission boundary", "all",
	)
	var childID, runtimeInputID, jobID string
	var maxAttempts int
	if err := admin.QueryRowContext(context.Background(), `SELECT child.id,inbox.runtime_input_id,job.id
		FROM session_threads child
		JOIN session_runtime_inbox inbox ON inbox.workspace_id=child.workspace_id AND inbox.session_id=child.session_id AND inbox.session_thread_id=child.id
		JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id
		 AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE child.workspace_id='default' AND child.session_id=$1 AND child.parent_thread_id=$2
		 AND child.role='subagent' AND inbox.input_kind='agent_mail'`, sessionID, parentID).Scan(&childID, &runtimeInputID, &jobID); err != nil {
		t.Fatalf("read final first_mail custody: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&maxAttempts); err != nil {
		t.Fatalf("read first_mail attempt budget: %v", err)
	}
	rejectingRuntime := startAttachmentRecoveryRuntime(t, spawned.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID, true)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			waitForQueueJobAvailable(t, admin, jobID)
		}
		runSubagentRuntimeQueueOnce(t, runtimeDB, admin, rejectingRuntime.port, sessionID, podUID)
	}
	waitForQueueJobAvailable(t, admin, jobID)
	runSubagentRuntimeQueueOnce(t, runtimeDB, admin, rejectingRuntime.port, sessionID, podUID)
	reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("final first_mail admission = %q/%d/%s; want capacity rejection with zero Provider calls", reason, providers, raw)
	}
	rejectingRuntime.kill(t)

	var inboxStatus, queueStatus, childStatus, parentStatus string
	var attempts, messages, starts, parentNotifications int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$5),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4
		  AND type='agent.thread_message_sent' AND payload_json::jsonb ->> 'target_thread_id'=$5)`,
		runtimeInputID, jobID, sessionID, childID, parentID).Scan(
		&inboxStatus, &queueStatus, &attempts, &childStatus, &parentStatus, &messages, &starts, &parentNotifications,
	); err != nil {
		t.Fatalf("read final first_mail settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts+1 ||
		childStatus != "failed" || parentStatus == "failed" || messages != 1 || starts != 0 || parentNotifications != 1 {
		t.Fatalf("final first_mail settlement = Inbox:%s Queue:%s attempts:%d child:%s parent:%s Messages:%d starts:%d notifications:%d",
			inboxStatus, queueStatus, attempts, childStatus, parentStatus, messages, starts, parentNotifications)
	}
	messageBoundary := int64(1)
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeAPIScope(sessionID, childID, bindingID, 1, podUID), RuntimeWriteId: "rwrite_late_first_mail_start",
		ModelRequestId: "mreq_late_first_mail_start", EventType: "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_late_first_mail_start"}`,
		ContextThroughMessageSequence: &messageBoundary, RequestKind: "agent_provider_request",
	}); err == nil {
		t.Fatal("late Request Start succeeded after first_mail-child terminal fence")
	}
}

func TestSubagentFirstMailPreparationExhaustionSettlesCustodyAtomically(t *testing.T) {
	fixture := newSubagentMailFixture(t, "final_preparation_failure")
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(client, queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	})
	baseStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	baseStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: fixture.podUID, PodIP: "127.0.0.1",
		}})
	}}
	failedStore := &agentMailPreparationFailureStore{PostgreSQLRuntimeDeliveryStore: baseStore}
	sender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: failedStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "first_mail-preparation-failure", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	var maxAttempts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, fixture.jobID).Scan(&maxAttempts); err != nil {
		t.Fatalf("read first_mail preparation budget: %v", err)
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
		}
		if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
			t.Fatalf("run first_mail preparation failure %d = active:%t err:%v", attempt+1, active, err)
		}
	}
	waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run first-mail finalization-only lease = active:%t err:%v", active, err)
	}
	var inboxStatus, queueStatus, childStatus string
	var attempts, messages, starts, failures int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='session.thread_status_terminated')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &attempts, &childStatus, &messages, &starts, &failures); err != nil {
		t.Fatalf("read final preparation settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts+1 ||
		childStatus != "failed" || messages != 0 || starts != 0 || failures != 1 || sender.agentMailCalls != 0 {
		t.Fatalf("final preparation settlement = Inbox:%s Queue:%s attempts:%d child:%s Messages:%d starts:%d failures:%d Runtime:%d",
			inboxStatus, queueStatus, attempts, childStatus, messages, starts, failures, sender.agentMailCalls)
	}
}

func TestSubagentFirstMailPodLossBeforeCommitReturnsOriginalCustodyThenExecutesOnce(t *testing.T) {
	fixture := newSubagentMailFixture(t, "pod_loss_before_commit")
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	baseBridge := NewPostgreSQLBridgeAPIStore(client)
	baseBridge.RuntimeBindingTokenHMACKey = []byte("first_mail-pod-loss-before-commit-key")
	cutBridge := &firstCommitInputsPodLossCutStore{BridgeAPIStore: baseBridge, entered: make(chan struct{})}
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, cutBridge)
	defer stopBridge()

	lostRuntime := startAttachmentRecoveryRuntime(
		t, bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	lostRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, lostRuntime.port, fixture.sessionID, fixture.podUID,
		nil, NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}),
	)
	type runnerResult struct {
		active bool
		err    error
	}
	finished := make(chan runnerResult, 1)
	go func() {
		active, err := lostRunner.RunOnceWithActivity(context.Background())
		finished <- runnerResult{active: active, err: err}
	}()
	select {
	case <-cutBridge.entered:
	case <-time.After(10 * time.Second):
		t.Fatalf("first_mail Runtime did not reach CommitInputs cut: %s", lostRuntime.output.String())
	}
	lostRuntime.kill(t)
	repairStore := runtimePodLossSweepStore(fixture.runtimeDB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := repairStore.RepairLostRuntimeBindings(context.Background(), workspace.DefaultID.String()); err != nil || repaired != 1 {
		t.Fatalf("repair first_mail input before CommitInputs = %d/%v", repaired, err)
	}
	select {
	case result := <-finished:
		if !result.active {
			t.Fatalf("lost Runtime runner activity = false err:%v", result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lost Runtime runner did not return after Pod loss")
	}
	var inboxStatus, queueStatus, jobID, runtimeInputID string
	var attempts, jobs, messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT id FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT runtime_input_id FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=(SELECT dedupe_key FROM queue_jobs WHERE workspace_id='default' AND id=$2)),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &jobID, &runtimeInputID, &attempts, &jobs, &messages, &starts); err != nil {
		t.Fatalf("read pre-Commit Pod-loss custody: %v", err)
	}
	if inboxStatus != "queued" || queueStatus != queue.StatusPending || jobID != fixture.jobID || runtimeInputID != fixture.runtimeInputID || jobs != 1 || messages != 0 || starts != 0 {
		t.Fatalf("pre-Commit Pod-loss custody = Inbox:%s Queue:%s job:%s input:%s attempts:%d jobs:%d Messages:%d starts:%d",
			inboxStatus, queueStatus, jobID, runtimeInputID, attempts, jobs, messages, starts)
	}

	const replacementPodUID = "pod_mail_pod_loss_before_commit_replacement"
	seedBridgeAPIRuntimeBinding(t, fixture.admin, "default", fixture.sessionID, fixture.bindingID, 2, replacementPodUID)
	replacementRuntime := startAttachmentRecoveryRuntime(
		t, bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 2, replacementPodUID,
	)
	lostStartResponse := &lostResponseAfterRequestStartSender{
		RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}),
		admin:                fixture.admin, sessionID: fixture.sessionID, childID: fixture.childID,
	}
	replacementRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, replacementRuntime.port, fixture.sessionID, replacementPodUID,
		nil, lostStartResponse,
	)
	if active, err := replacementRunner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("drop replacement response after Request Start = active:%t err:%v", active, err)
	}
	started := replacementRuntime.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, replacementRuntime.port, fixture.sessionID, replacementPodUID)
	replayed := replacementRuntime.providerStart(t)
	replacementRuntime.kill(t)
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &messages, &starts); err != nil {
		t.Fatalf("read replacement first_mail convergence: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || messages != 1 || starts != 1 ||
		started.ProviderInvocations != 1 || replayed.ProviderInvocations != 1 {
		t.Fatalf("replacement first_mail convergence = Inbox:%s Queue:%s Messages:%d starts:%d providers:%d",
			inboxStatus, queueStatus, messages, starts, replayed.ProviderInvocations)
	}
}

func TestSubagentMailProgressesDuringUnrelatedThreadInterrupt(t *testing.T) {
	fixture := newSubagentMailFixture(t, "thread_interrupt_independence")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	baseQueue := tetralqueue.NewServer(queue.NewPostgreSQLStoreWithRetryPolicy(client, queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	}), nil)
	queueWithBarrierCut := &birthInterruptAfterLeaseQueueClient{
		QueueClient: baseQueue,
		birth: func(ctx context.Context) error {
			born, err := events.AppendClientEvents(ctx, workspace.DefaultID, fixture.sessionID,
				"first_mail-thread-interrupt-independence", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
					Type: sessionevent.EventTypeUserInterrupt,
				}}})
			if err == nil && len(born.Data) != 1 {
				return errors.New("main Thread interrupt owner did not birth exactly one Event")
			}
			return err
		},
	}
	mailRuntime := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	var attemptsBeforeDelivery int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT attempt_count FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, fixture.jobID).Scan(&attemptsBeforeDelivery); err != nil {
		t.Fatalf("read first_mail attempts before interrupt cut: %v", err)
	}
	deliveryRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, mailRuntime.port, fixture.sessionID, fixture.podUID,
		queueWithBarrierCut, nil,
	)
	if active, err := deliveryRunner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("deliver first_mail alongside unrelated interrupt = active:%t err:%v", active, err)
	}
	var inboxStatus, queueStatus string
	var attempts, messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &attempts, &messages, &starts); err != nil {
		t.Fatalf("read interrupt-deferred first_mail custody: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != attemptsBeforeDelivery+1 || messages != 1 || starts != 0 {
		t.Fatalf("cross-Thread delivery custody = Inbox:%s Queue:%s attempts:%d (before:%d) Messages:%d starts:%d", inboxStatus, queueStatus, attempts, attemptsBeforeDelivery, messages, starts)
	}
	started := mailRuntime.providerStart(t)

	interruptRuntime, interruptPaths := startInterruptRuntimeComposition(
		t, t.TempDir(), fixture.bridgeAddress, fixture.sessionID, parentID,
		fixture.bindingID, 1, fixture.podUID,
	)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, interruptRuntime.port, fixture.sessionID, fixture.podUID)
	if err := os.WriteFile(interruptPaths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release main Thread interrupt Runtime: %v", err)
	}
	if composed := interruptRuntime.wait(t); len(composed.InterruptResult) == 0 || composed.ProviderInvocations != 0 {
		t.Fatalf("main Thread interrupt closeout = %+v", composed)
	}

	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	mailRuntime.kill(t)
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &attempts, &messages, &starts); err != nil {
		t.Fatalf("read resumed first_mail custody: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != attemptsBeforeDelivery+1 || messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("resumed first_mail custody = Inbox:%s Queue:%s attempts:%d Messages:%d starts:%d providers:%d",
			inboxStatus, queueStatus, attempts, messages, starts, started.ProviderInvocations)
	}
}

func TestSubagentMailAcknowledgesWhileEarlierToolRemainsActive(t *testing.T) {
	fixture := newSubagentMailFixture(t, "busy_tool_mail")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	runtimeProcess, paths := startInterruptRuntimeComposition(
		t, t.TempDir(), fixture.bridgeAddress, fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	runQueueUntilInputSettled(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port,
		fixture.sessionID, fixture.podUID, fixture.runtimeInputID, paths.acceptResult,
	)
	waitForCompositionFile(t, paths.toolStarted, "subagent Tool start", &runtimeProcess.output)

	client := startActorProductionBridge(t, fixture.runtimeDB)
	sourceID := "evt_busy_tool_later_mail"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, sourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, sourceID)
	deliveryID := agentMailDeliveryID(sourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope:      bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		DeliveryId: deliveryID, TargetThreadId: fixture.childID,
		SourceToolUseEventId: sourceID, Content: "continue after the active tool settles",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver mail while subagent Tool is active = %#v/%v", delivered, err)
	}
	runtimeInputID := "agent_mail:" + deliveryID
	runQueueUntilInputSettled(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port,
		fixture.sessionID, fixture.podUID, runtimeInputID, paths.acceptResult,
	)

	var inboxStatus, queueStatus string
	var attempts, requestStarts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3
		 AND session_thread_id=$4 AND type='span.model_request_start')`,
		runtimeInputID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, runtimeInputID),
		fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &attempts, &requestStarts); err != nil {
		t.Fatalf("read mail accepted during active Tool: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != 1 || requestStarts != 1 {
		t.Fatalf("mail during active Tool = Inbox:%s Queue:%s attempts:%d starts:%d; want accepted/acknowledged/1/1",
			inboxStatus, queueStatus, attempts, requestStarts)
	}

	if err := os.WriteFile(paths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release busy Tool Runtime composition: %v", err)
	}
	result := runtimeProcess.wait(t)
	if result.ProviderInvocations != 1 {
		t.Fatalf("active Tool admitted a second Provider request before settlement: %#v", result)
	}
}

func TestSubagentFirstMailHotAdmissionAcknowledgesBeforeRequestStart(t *testing.T) {
	fixture := newSubagentMailFixture(t, "transport_loss_before_start")
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	baseBridge := NewPostgreSQLBridgeAPIStore(client)
	baseBridge.RuntimeBindingTokenHMACKey = []byte("first_mail-transport-loss-key")
	barrierBridge := &requestStartBarrierBridgeStore{
		BridgeAPIStore: baseBridge, entered: make(chan struct{}), release: make(chan struct{}),
	}
	bridgeAddress, stopBridge := serveAttachmentCompositionBridge(t, barrierBridge)
	defer stopBridge()
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		nil, nil,
	)
	runner.Config.LeaseDuration = 250 * time.Millisecond
	runner.Config.HeartbeatInterval = time.Hour
	deliveryContext, cancelDelivery := context.WithCancel(context.Background())
	type deliveryResult struct {
		active bool
		err    error
	}
	finished := make(chan deliveryResult, 1)
	go func() {
		active, err := runner.RunOnceWithActivity(deliveryContext)
		finished <- deliveryResult{active: active, err: err}
	}()
	select {
	case <-barrierBridge.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first_mail delivery did not reach the pre-Start transport cut")
	}
	cancelDelivery()
	select {
	case result := <-finished:
		if !result.active {
			t.Fatalf("cancelled first_mail delivery reported no activity: %v", result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled first_mail transport did not return")
	}

	var inboxStatus, queueStatus string
	var messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &messages, &starts); err != nil {
		t.Fatalf("read pre-Start transport-loss custody: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || messages != 1 || starts != 0 {
		t.Fatalf("hot admission before Request Start = Inbox:%s Queue:%s Messages:%d starts:%d",
			inboxStatus, queueStatus, messages, starts)
	}

	runtimeProcess.kill(t)
	close(barrierBridge.release)
	repairStore := runtimePodLossSweepStore(fixture.runtimeDB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if repaired, err := repairStore.RepairLostRuntimeBindings(context.Background(), workspace.DefaultID.String()); err != nil || repaired != 1 {
		t.Fatalf("repair accepted first mail after Pod loss = %d/%v", repaired, err)
	}
	var activeJobs int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2 AND status IN ('pending','leased')),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, fixture.runtimeInputID),
		fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &activeJobs, &messages, &starts); err != nil {
		t.Fatalf("read Pod-loss handoff custody: %v", err)
	}
	if inboxStatus != "queued" || activeJobs != 1 || messages != 1 || starts != 0 {
		t.Fatalf("Pod-loss handoff = Inbox:%s active jobs:%d Messages:%d starts:%d; want queued/1/1/0",
			inboxStatus, activeJobs, messages, starts)
	}

	const replacementPodUID = "pod_subagent_mail_hot_admission_replacement"
	seedBridgeAPIRuntimeBinding(t, fixture.admin, "default", fixture.sessionID, fixture.bindingID, 2, replacementPodUID)
	replacementRuntime := startAttachmentRecoveryRuntime(
		t, bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 2, replacementPodUID,
	)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, replacementRuntime.port,
		fixture.sessionID, replacementPodUID)
	started := replacementRuntime.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	replacementRuntime.kill(t)
	var attempts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2 ORDER BY created_at DESC LIMIT 1),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$2 ORDER BY created_at DESC LIMIT 1),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, fixture.runtimeInputID), fixture.sessionID, fixture.childID,
	).Scan(&inboxStatus, &queueStatus, &attempts, &messages, &starts); err != nil {
		t.Fatalf("read replacement convergence: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != 1 ||
		messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("replacement convergence = Inbox:%s Queue:%s attempts:%d Messages:%d starts:%d providers:%d",
			inboxStatus, queueStatus, attempts, messages, starts, started.ProviderInvocations)
	}
}

func TestSubagentMailFinalizerCrashUsesClampedNPlusOne(t *testing.T) {
	fixture := newSubagentMailFixture(t, "finalizer_crash")
	var maxAttempts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, fixture.jobID).Scan(&maxAttempts); err != nil {
		t.Fatalf("read finalizer crash attempt budget: %v", err)
	}
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID, true,
	)
	for attempt := 1; attempt < maxAttempts; attempt++ {
		if attempt > 1 {
			waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
		}
		runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID)
	}
	waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
	finalRuntimeRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID, nil, nil,
	)
	finalRuntimeRunner.Deliverer = &finalizationCutDeliverer{
		RuntimePodDirectDeliverer: finalRuntimeRunner.Deliverer.(RuntimePodDirectDeliverer),
		failBeforeCommit:          true,
	}
	if active, err := finalRuntimeRunner.RunOnceWithActivity(context.Background()); !active || err == nil {
		t.Fatalf("final Runtime opportunity finalizer cut = active:%t err:%v; want active/error", active, err)
	}
	reason, providers, raw := readAgentMailAdmission(t, runtimeProcess)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("finalizer crash Runtime boundary = %q/%d/%s; want capacity rejection with zero Provider calls", reason, providers, raw)
	}
	runtimeProcess.kill(t)

	expireAndReclaimQueueJob(t, fixture.runtimeDB, fixture.admin, fixture.jobID)
	baseQueue := tetralqueue.NewServer(queue.NewPostgreSQLStoreWithRetryPolicy(
		dbconnect.NewClientForTesting(fixture.runtimeDB),
		queue.RetryPolicy{BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 }},
	), nil)
	observedQueue := &retryObservingQueueClient{QueueClient: baseQueue}
	countedSender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	nPlusOneRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		observedQueue, countedSender,
	)
	nPlusOneRunner.Deliverer = &finalizationCutDeliverer{
		RuntimePodDirectDeliverer: nPlusOneRunner.Deliverer.(RuntimePodDirectDeliverer),
		failBeforeCommit:          true,
	}
	if active, err := nPlusOneRunner.RunOnceWithActivity(context.Background()); !active || err == nil {
		t.Fatalf("N+1 finalizer cut = active:%t err:%v; want active/error", active, err)
	}
	if countedSender.agentMailCalls != 0 || observedQueue.retryCalls != 0 {
		t.Fatalf("N+1 cut called Runtime/Queue.Retry = %d/%d; want 0/0", countedSender.agentMailCalls, observedQueue.retryCalls)
	}

	expireAndReclaimQueueJob(t, fixture.runtimeDB, fixture.admin, fixture.jobID)
	countedSender = &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	runFinalizer := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		nil, countedSender,
	)
	if active, err := runFinalizer.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("replayed N+1 finalizer = active:%t err:%v", active, err)
	}
	if countedSender.agentMailCalls != 0 {
		t.Fatalf("replayed N+1 called Runtime %d times; want 0", countedSender.agentMailCalls)
	}
	var inboxStatus, queueStatus, childStatus string
	var attempts, starts, notifications int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4
		  AND type='agent.thread_message_sent')`, fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID).Scan(
		&inboxStatus, &queueStatus, &attempts, &childStatus, &starts, &notifications,
	); err != nil {
		t.Fatalf("read N+1 final settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts+1 ||
		childStatus != "failed" || starts != 0 || notifications != 1 {
		t.Fatalf("N+1 settlement = Inbox:%s Queue:%s attempts:%d child:%s starts:%d notifications:%d",
			inboxStatus, queueStatus, attempts, childStatus, starts, notifications)
	}
}

func TestSubagentFirstMailInterruptedCloseColdResumeAndLaterInputProductionComposition(t *testing.T) {
	fixture := newSubagentMailFixture(t, "first_mail_interrupted_resume")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	siblingID := "thr_first_mail_interrupted_sibling"
	seedBridgeAPIChildThread(t, fixture.admin, "default", fixture.sessionID, parentID, siblingID)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE session_threads
		SET agent_type='worker' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, fixture.sessionID, siblingID); err != nil {
		t.Fatalf("complete sibling fixture: %v", err)
	}
	client := startActorProductionBridge(t, fixture.runtimeDB)
	interruptRuntime, interruptPaths := startInterruptRuntimeComposition(
		t, t.TempDir(), fixture.bridgeAddress, fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, interruptRuntime.port,
		fixture.sessionID, fixture.podUID, fixture.runtimeInputID, interruptPaths.acceptResult)
	waitForCompositionFile(t, interruptPaths.toolStarted, "first_mail request Tool start", &interruptRuntime.output)

	var firstMessageID, firstSourceEventID, firstInboxEventID, startID, modelRequestID, runningID, toolUseID string
	var startBoundary int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT message_id FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'),
		(SELECT source_event_id FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'),
		(SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT model_request_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT (projection_json::jsonb->>'context_through_message_sequence')::int FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.thread_status_running' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_use' ORDER BY sequence DESC LIMIT 1)`,
		fixture.sessionID, fixture.childID, fixture.runtimeInputID).Scan(
		&firstMessageID, &firstSourceEventID, &firstInboxEventID, &startID, &modelRequestID, &startBoundary, &runningID, &toolUseID,
	); err != nil {
		t.Fatalf("read in-flight first_mail identities: %v", err)
	}
	if firstMessageID == "" || firstSourceEventID == "" || firstSourceEventID != firstInboxEventID || startID == "" || modelRequestID == "" ||
		toolUseID == "" || startBoundary != 1 {
		t.Fatalf("first_mail identity chain = Message:%s source:%s Start:%s request:%s Tool:%s boundary:%d",
			firstMessageID, firstSourceEventID, startID, modelRequestID, toolUseID, startBoundary)
	}

	parentScope := bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID)
	closeSourceID := "evt_close_inflight_first_mail"
	controlID := admitChildCloseThroughProduction(t, fixture.admin, client, parentScope,
		fixture.sessionID, parentID, fixture.childID, closeSourceID)
	type interruptDeliveryResult struct {
		active bool
		err    error
	}
	interruptDelivery := make(chan interruptDeliveryResult, 1)
	interruptRunner := newSubagentRuntimeQueueRunner(t, fixture.runtimeDB, fixture.admin,
		interruptRuntime.port, fixture.sessionID, fixture.podUID, nil, nil)
	go func() {
		active, runErr := interruptRunner.RunOnceWithActivity(context.Background())
		interruptDelivery <- interruptDeliveryResult{active: active, err: runErr}
	}()
	waitForCompositionFile(t, interruptPaths.operationCompleted, "interrupted first_mail Tool completion", &interruptRuntime.output)
	runSubagentOutputCaptureOnce(t, fixture.runtimeDB)
	select {
	case delivered := <-interruptDelivery:
		if delivered.err != nil || !delivered.active {
			t.Fatalf("deliver first_mail interrupt = active:%t err:%v", delivered.active, delivered.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("first_mail interrupt did not settle after output capture: %s", interruptRuntime.output.String())
	}
	if err := os.WriteFile(interruptPaths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release interrupted first_mail Runtime: %v", err)
	}
	interrupted := interruptRuntime.wait(t)
	if interrupted.ProviderInvocations != 1 || interrupted.DurableOperationCompletions != 1 || len(interrupted.InterruptResult) == 0 {
		t.Fatalf("first_mail interrupt Runtime = Provider:%d durable completions:%d result:%s",
			interrupted.ProviderInvocations, interrupted.DurableOperationCompletions, interrupted.InterruptResult)
	}
	settleChildCloseThroughProduction(t, client, parentScope, controlID, closeSourceID)

	var firstInbox, firstQueue, childStatus, siblingStatus, endID, endStartID, endDisposition, toolResultID, closeID string
	var messages, starts, ends, toolResults, activeFirstMailJobs int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$5),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_end'),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_end'),
		(SELECT payload_json::jsonb->>'model_request_start_id' FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_end'),
		(SELECT payload_json::jsonb#>>'{provider_context_retention,disposition}' FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='agent.tool_result' AND COALESCE(payload_json::jsonb->>'tool_use_event_id',payload_json::jsonb->>'tool_use_id')=$6),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='agent.tool_result' AND COALESCE(payload_json::jsonb->>'tool_use_event_id',payload_json::jsonb->>'tool_use_id')=$6),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND id=$2 AND status IN ('pending','leased'))`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID, siblingID, toolUseID).Scan(
		&firstInbox, &firstQueue, &childStatus, &siblingStatus, &messages, &starts, &ends,
		&endID, &endStartID, &endDisposition, &toolResults, &toolResultID, &closeID, &activeFirstMailJobs,
	); err != nil {
		t.Fatalf("read interrupted first_mail close facts: %v", err)
	}
	if firstInbox != "committed" || firstQueue != queue.StatusAcknowledged || childStatus != "closed_for_runtime" || siblingStatus != "idle" ||
		messages != 1 || starts != 1 || ends != 1 || endStartID != startID || endDisposition != "interrupted" ||
		toolResults != 1 || toolResultID == "" || closeID == "" || activeFirstMailJobs != 0 {
		t.Fatalf("interrupted first_mail close = Inbox:%s Queue:%s child:%s sibling:%s messages:%d starts:%d ends:%d End:%s/%s/%s ToolResult:%d/%s close:%s active:%d",
			firstInbox, firstQueue, childStatus, siblingStatus, messages, starts, ends, endID, endStartID, endDisposition,
			toolResults, toolResultID, closeID, activeFirstMailJobs)
	}

	resumeSourceID := "evt_resume_interrupted_first_mail"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumedRuntime, resumed := startClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-first_mail_interrupted_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, resumed)
	wantFacts := map[string]bool{
		runningID: false, startID: false, endID: false, toolUseID: false, toolResultID: false, closeID: false,
	}
	for _, event := range resumed.TurnFacts.Events {
		if _, expected := wantFacts[event.EventID]; expected {
			wantFacts[event.EventID] = true
		}
	}
	for eventID, present := range wantFacts {
		if !present {
			t.Fatalf("closed LoadContext omitted interrupted first_mail Event %s: %#v", eventID, resumed.TurnFacts.Events)
		}
	}
	if resumed.ProviderRequests != 0 || resumed.RuntimeEvents != 0 {
		t.Fatalf("cold Resume replayed historical first_mail work: Provider=%d Runtime=%d", resumed.ProviderRequests, resumed.RuntimeEvents)
	}

	laterSourceID := "evt_later_after_interrupted_resume"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, laterSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, laterSourceID)
	laterDeliveryID := agentMailDeliveryID(laterSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID), DeliveryId: laterDeliveryID,
		TargetThreadId: fixture.childID, SourceToolUseEventId: laterSourceID, Content: "one later input after interrupted first_mail resume",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver later input after interrupted resume = %#v/%v", delivered, err)
	}
	laterRuntimeInputID := "agent_mail:" + laterDeliveryID
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, resumedRuntime.port, fixture.sessionID, fixture.podUID, laterRuntimeInputID, resumedRuntime.acceptResultPath)
	laterExecution := resumedRuntime.providerStart(t, fixture.admin, fixture.sessionID, fixture.childID)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 2)
	resumedRuntime.close(t)
	var laterSequence int64
	var laterMessageID, laterSourceEventID, finalSiblingStatus string
	var finalStarts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT sequence FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT message_id FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT source_event_id FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$4)`,
		fixture.sessionID, fixture.childID, laterRuntimeInputID, siblingID).Scan(
		&laterSequence, &laterMessageID, &laterSourceEventID, &finalStarts, &finalSiblingStatus,
	); err != nil {
		t.Fatalf("read later resumed Message sequence: %v", err)
	}
	if laterExecution.ProviderRequests != 1 || laterSequence <= 1 || laterMessageID == "" || laterSourceEventID == firstSourceEventID ||
		finalStarts != 2 || finalSiblingStatus != "idle" ||
		!strings.Contains(string(laterExecution.Request), "one later input after interrupted first_mail resume") {
		t.Fatalf("later resumed execution = providers:%d sequence:%d Message:%s source:%s starts:%d sibling:%s wire:%s",
			laterExecution.ProviderRequests, laterSequence, laterMessageID, laterSourceEventID, finalStarts, finalSiblingStatus, laterExecution.Request)
	}
}

func TestSubagentClosedResumeUsesPostCompactionRequestBoundary(t *testing.T) {
	fixture := newSubagentMailFixture(t, "post_compaction_resume")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	runtimeProcess := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID, fixture.runtimeInputID)
	if execution := runtimeProcess.providerStart(t); execution.ProviderInvocations != 1 {
		t.Fatalf("pre-compaction Provider invocations = %d; want 1", execution.ProviderInvocations)
	}
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	runtimeProcess.kill(t)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(fixture.runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-first_mail-composition-key")
	childScope := bridgeAPIScope(fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	var messageBoundary int64
	var preCompactionStartID string
	var parentBoundaryEventID string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT COALESCE(MAX(sequence),0) FROM session_messages
		 WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2),
		(SELECT event_id FROM session_events
		 WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'
		 ORDER BY sequence DESC LIMIT 1),
		(SELECT parent_boundary_event_id FROM session_thread_context_prefixes
		 WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2)`, fixture.sessionID, fixture.childID).Scan(
		&messageBoundary, &preCompactionStartID, &parentBoundaryEventID,
	); err != nil {
		t.Fatalf("read pre-compaction boundary: %v", err)
	}
	compactionRequestID := "mreq_post_compaction_resume"
	compactionRunning, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: childScope, RuntimeWriteId: "rwrite_post_compaction_running",
		EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`,
	})
	if err != nil || compactionRunning.GetCommitted() == nil {
		t.Fatalf("open compaction durable turn = %#v/%v", compactionRunning, err)
	}
	compactionStart := seedBridgeAPIRequestStart(t, store, childScope,
		"rwrite_post_compaction_start", compactionRequestID, requestKindCompactionSummary, messageBoundary)
	compactionBoundary := messageBoundary
	compactionEnd, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: childScope, RuntimeWriteId: "rwrite_post_compaction_end", ModelRequestId: compactionRequestID,
		FinishReason: "end_turn", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "compacted", ToolUseEventIds: []string{}, RepairEventIds: []string{},
		},
		PrefixConsumption: &bridgev1.PrefixConsumptionDraft{
			ChildThreadId: fixture.childID, ParentBoundaryEventId: parentBoundaryEventID,
		},
		CompactionContext:               bridgeTextContextDeltaForTest("first_mail request completed before this retained summary"),
		CompactedThroughMessageSequence: &compactionBoundary,
		CompactionEventPayloadJson:      `{"type":"agent.thread_context_compacted"}`,
	})
	if err != nil || compactionEnd.GetCommitted() == nil {
		t.Fatalf("commit compaction before close = %#v/%v", compactionEnd, err)
	}
	compactionIdle, err := finishIdleWithStagedCaptureForTest(t, fixture.admin, store, &bridgev1.FinishIdleRequest{
		Scope: childScope, DurableTurnId: compactionRunning.GetCommitted().GetEventId(),
		StopReasonJson: `{"type":"end_turn"}`,
	})
	if err != nil || compactionIdle.GetCommitted() == nil {
		t.Fatalf("close compaction durable turn = %#v/%v", compactionIdle, err)
	}

	client := startActorProductionBridge(t, fixture.runtimeDB)
	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_close_after_compaction")
	var latestClosedIdleID string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1`,
		fixture.sessionID, fixture.childID).Scan(&latestClosedIdleID); err != nil {
		t.Fatalf("read latest post-compaction idle owner: %v", err)
	}
	resumeSourceID := "evt_resume_after_compaction"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumed := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-post_compaction_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, resumed)
	compactionRunningID := compactionRunning.GetCommitted().GetEventId()
	compactionStartID := compactionStart.GetCommitted().GetEventId()
	compactionEndID := compactionEnd.GetCommitted().GetRequestEndEventId()
	compactionIdleID := latestClosedIdleID
	seenCompactionRunning := false
	seenCompactionStart := false
	seenCompactionEnd := false
	seenCompactionIdle := false
	for _, event := range resumed.TurnFacts.Events {
		if event.EventID == preCompactionStartID {
			t.Fatalf("pre-compaction Request became active after cold Resume: %#v", resumed.TurnFacts.Events)
		}
		if event.EventID == compactionRunningID && event.Type == "session.thread_status_running" {
			seenCompactionRunning = true
		}
		if event.EventID == compactionStartID && event.RequestStart != nil &&
			event.RequestStart.ContextThroughMessageSequence == messageBoundary {
			seenCompactionStart = true
		}
		if event.EventID == compactionEndID && event.RequestEnd != nil && event.RequestEnd.RequestStartEventID == compactionStartID {
			seenCompactionEnd = true
		}
		if event.EventID == compactionIdleID && event.Idle != nil {
			seenCompactionIdle = true
		}
	}
	if !seenCompactionRunning || !seenCompactionStart || !seenCompactionEnd || !seenCompactionIdle ||
		len(resumed.ContextEntries) != 1 || resumed.ContextEntries[0].ContextKind != "compaction" {
		t.Fatalf("post-compaction cold facts = running:%t start:%t end:%t idle:%t context:%#v facts:%#v",
			seenCompactionRunning, seenCompactionStart, seenCompactionEnd, seenCompactionIdle,
			resumed.ContextEntries, resumed.TurnFacts.Events)
	}
}

func TestSubagentNoWorkCloseResumeCyclePreservesCompletedRequest(t *testing.T) {
	fixture := newSubagentMailFixture(t, "no_work_close_resume")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID)
	if execution := runtimeProcess.providerStart(t); execution.ProviderInvocations != 1 {
		t.Fatalf("no-work cycle first_mail Provider invocations = %d; want 1", execution.ProviderInvocations)
	}
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	runtimeProcess.kill(t)

	actorClient := startActorProductionBridge(t, fixture.runtimeDB)
	parentScope := bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID)
	closeChildThroughProductionInterrupt(
		t, fixture.runtimeDB, fixture.admin, actorClient, fixture.bridgeAddress, parentScope,
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_no_work_close_first",
	)
	var runningID, requestStartID, requestEndID, firstIdleID string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.thread_status_running' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1)`,
		fixture.sessionID, fixture.childID,
	).Scan(&runningID, &requestStartID, &requestEndID, &firstIdleID); err != nil {
		t.Fatalf("read first closed lifecycle: %v", err)
	}
	firstResumeSourceID := "evt_no_work_resume_first"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, firstResumeSourceID)
	first := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-no_work_close_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": firstResumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, first)

	closeChildThroughProductionInterrupt(
		t, fixture.runtimeDB, fixture.admin, actorClient, fixture.bridgeAddress, parentScope,
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_no_work_close_second",
	)
	var secondIdleID string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		  AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1`,
		fixture.sessionID, fixture.childID).Scan(&secondIdleID); err != nil {
		t.Fatalf("read second closed idle owner: %v", err)
	}
	secondResumeSourceID := "evt_no_work_resume_second"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, secondResumeSourceID)
	second := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-no_work_close_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": secondResumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, second)

	for _, sample := range []struct {
		name   string
		result closedThreadResumeCompositionResult
		idleID string
	}{{name: "first", result: first, idleID: firstIdleID}, {name: "second", result: second, idleID: secondIdleID}} {
		want := map[string]bool{runningID: false, requestStartID: false, requestEndID: false, sample.idleID: false}
		for _, event := range sample.result.TurnFacts.Events {
			if _, expected := want[event.EventID]; expected {
				want[event.EventID] = true
			}
		}
		for eventID, present := range want {
			if !present {
				t.Fatalf("%s no-work Resume omitted completed lifecycle Event %s: %#v", sample.name, eventID, sample.result.TurnFacts.Events)
			}
		}
	}
	var starts, ends int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end')`,
		fixture.sessionID, fixture.childID,
	).Scan(&starts, &ends); err != nil {
		t.Fatalf("read no-work cycle request cardinality: %v", err)
	}
	if starts != 1 || ends != 1 {
		t.Fatalf("no-work close/Resume created request work = starts:%d ends:%d; want 1/1", starts, ends)
	}
}

func TestSubagentRetainedAssistantAndTerminalToolResultColdResume(t *testing.T) {
	fixture := newSubagentMailFixture(t, "terminal_tool_resume")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	executingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, executingRuntime.port, fixture.sessionID, fixture.podUID)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	executingRuntime.kill(t)

	client := startActorProductionBridge(t, fixture.runtimeDB)
	parentScope := bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID)
	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		parentScope, fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_close_before_terminal_tool_resume")
	resumeSourceID := "evt_resume_for_terminal_tool"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumedRuntime, resumed := startClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-terminal_tool_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
		"providerScenario": "terminal-tool",
	})
	assertQuiescentClosedThreadResume(t, resumed)

	laterSourceID := "evt_terminal_tool_after_resume"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, laterSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, laterSourceID)
	laterDeliveryID := agentMailDeliveryID(laterSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: laterDeliveryID, TargetThreadId: fixture.childID,
		SourceToolUseEventId: laterSourceID, Content: "produce and retain one terminal tool pair",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver terminal-Tool input after Resume = %#v/%v", delivered, err)
	}
	runtimeInputID := "agent_mail:" + laterDeliveryID
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, resumedRuntime.port, fixture.sessionID, fixture.podUID, runtimeInputID, resumedRuntime.acceptResultPath)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 2)
	resumedRuntime.close(t)

	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		parentScope, fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_close_after_terminal_tool")
	var retainedAssistant, terminalResults int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		count(*) FILTER (WHERE data_json::jsonb::text LIKE '%call_closed_resume_terminal_tool%'),
		count(*) FILTER (WHERE data_json::jsonb::text LIKE '%\"type\": \"tool_result\"%' OR data_json::jsonb::text LIKE '%\"type\":\"tool_result\"%')
		FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='assistant'`,
		fixture.sessionID, fixture.childID).Scan(&retainedAssistant, &terminalResults); err != nil {
		t.Fatalf("read retained Assistant terminal Tool pair: %v", err)
	}
	if retainedAssistant != 1 || terminalResults != 1 {
		var messages string
		var events string
		_ = fixture.admin.QueryRowContext(context.Background(), `SELECT coalesce(string_agg(sequence::text || ':' || kind || ':' || data_json, E'\n' ORDER BY sequence),'')
			FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`,
			fixture.sessionID, fixture.childID).Scan(&messages)
		_ = fixture.admin.QueryRowContext(context.Background(), `SELECT coalesce(string_agg(sequence::text || ':' || type || ':' || payload_json, E'\n' ORDER BY sequence),'')
			FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`,
			fixture.sessionID, fixture.childID).Scan(&events)
		t.Fatalf("retained Assistant/terminal Tool result rows = %d/%d; want 1/1; messages=%s events=%s runtime=%s", retainedAssistant, terminalResults, messages, events, resumedRuntime.output.String())
	}

	finalResumeSourceID := "evt_resume_terminal_tool_history"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, finalResumeSourceID)
	finalResume := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-terminal_tool_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": finalResumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, finalResume)
	retainedJSON, err := json.Marshal(finalResume.ContextEntries)
	if err != nil {
		t.Fatalf("encode retained terminal Tool context: %v", err)
	}
	if !bytes.Contains(retainedJSON, []byte("call_closed_resume_terminal_tool")) ||
		!bytes.Contains(retainedJSON, []byte(`"type":"tool_result"`)) ||
		!bytes.Contains(retainedJSON, []byte("checking the retained tool result")) {
		t.Fatalf("cold Resume omitted retained Assistant terminal Tool pair: %s", retainedJSON)
	}
}

func TestSubagentFirstMailCloseBeforeRequestStartCancelsExactCustody(t *testing.T) {
	fixture := newSubagentMailFixture(t, "close_before_start")
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID, true,
	)
	delayedQueue := tetralqueue.NewServer(queue.NewPostgreSQLStoreWithRetryPolicy(
		dbconnect.NewClientForTesting(fixture.runtimeDB),
		queue.RetryPolicy{BaseDelay: time.Hour, MaxDelay: time.Hour, RandomInt64: func(int64) int64 { return 0 }},
	), nil)
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		delayedQueue, nil,
	)
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("first_mail delivery before close = active:%t err:%v", active, err)
	}
	reason, providers, raw := readAgentMailAdmission(t, runtimeProcess)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("pre-close Runtime boundary = %q/%d/%s; want capacity rejection with zero Provider calls", reason, providers, raw)
	}
	runtimeProcess.kill(t)

	client := startActorProductionBridge(t, fixture.runtimeDB)
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	siblingID := "thr_close_before_start_sibling"
	siblingDeliveryID := "delivery_close_before_start_sibling"
	seedBridgeAPIChildThread(t, fixture.admin, "default", fixture.sessionID, parentID, siblingID)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE session_threads SET agent_type='worker',task_name='close-before-start-sibling'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, fixture.sessionID, siblingID); err != nil {
		t.Fatalf("name close-before-start sibling: %v", err)
	}
	siblingRuntimeInputID := completionRuntimeInputID(siblingDeliveryID)
	parentScope := bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID)
	closeSourceID := "evt_close_before_first_mail_start"
	controlID := admitChildCloseThroughProduction(
		t, fixture.admin, client, parentScope, fixture.sessionID, parentID, fixture.childID, closeSourceID,
	)
	closeFirstSourceID := "evt_mail_after_close_admission"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, closeFirstSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, closeFirstSourceID)
	closeFirstDeliveryID := agentMailDeliveryID(closeFirstSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: closeFirstDeliveryID, TargetThreadId: fixture.childID,
		SourceToolUseEventId: closeFirstSourceID, Content: "must not be born after close admission",
	}); status.Code(err) != codes.FailedPrecondition || delivered != nil {
		t.Fatalf("mail after close admission = %#v/%v; want FailedPrecondition before birth", delivered, err)
	}
	var lateSent, lateReceived, lateInbox, lateQueue, lateReceipt int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1
		 AND payload_json::jsonb->>'delivery_id'=$2 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1
		 AND payload_json::jsonb->>'delivery_id'=$2 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1
		 AND operation='deliver_inter_agent_mail' AND idempotency_key=$2)`,
		fixture.sessionID, closeFirstDeliveryID, completionRuntimeInputID(closeFirstDeliveryID),
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, completionRuntimeInputID(closeFirstDeliveryID)),
	).Scan(&lateSent, &lateReceived, &lateInbox, &lateQueue, &lateReceipt); err != nil {
		t.Fatalf("read close-first mail birth: %v", err)
	}
	if lateSent != 0 || lateReceived != 0 || lateInbox != 0 || lateQueue != 0 || lateReceipt != 0 {
		t.Fatalf("close-first mail artifacts = sent %d received %d Inbox %d Queue %d receipt %d; want zero",
			lateSent, lateReceived, lateInbox, lateQueue, lateReceipt)
	}
	interruptRuntime, interruptPaths := startInterruptRuntimeComposition(
		t, t.TempDir(), fixture.bridgeAddress, fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(fixture.runtimeDB))
	queueWithSiblingBirth := &birthInterruptAfterLeaseQueueClient{
		QueueClient: tetralqueue.NewServer(queueStore, nil),
		birth: func(context.Context) error {
			seedCompletionMailSentAt(t, fixture.admin, fixture.sessionID, siblingID, parentID,
				siblingDeliveryID, 100, "2026-08-22T00:00:00Z")
			return nil
		},
	}
	interruptRunner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, interruptRuntime.port, fixture.sessionID, fixture.podUID,
		queueWithSiblingBirth, nil,
	)
	if active, err := interruptRunner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("close child while sibling becomes eligible = active:%t err:%v", active, err)
	}
	if err := os.WriteFile(interruptPaths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release close-before-start Runtime: %v", err)
	}
	interruptRuntime.wait(t)
	settleChildCloseThroughProduction(t, client, parentScope, controlID, closeSourceID)

	var inboxStatus, queueStatus, childStatus, siblingInbox, siblingQueue, siblingStatus string
	var messages, events, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$5),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$6),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$7)`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID,
		siblingRuntimeInputID, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, siblingRuntimeInputID), siblingID).Scan(
		&inboxStatus, &queueStatus, &childStatus, &messages, &events, &starts,
		&siblingInbox, &siblingQueue, &siblingStatus,
	); err != nil {
		t.Fatalf("read close-before-start settlement: %v", err)
	}
	if inboxStatus != "cancelled" || queueStatus != queue.StatusCancelled || childStatus != "closed_for_runtime" ||
		messages != 1 || events != 1 || starts != 0 || siblingInbox != "queued" || siblingQueue != queue.StatusPending || siblingStatus != "idle" {
		t.Fatalf("close-before-start = Inbox:%s Queue:%s child:%s Messages:%d Events:%d starts:%d sibling:%s/%s/%s",
			inboxStatus, queueStatus, childStatus, messages, events, starts, siblingInbox, siblingQueue, siblingStatus)
	}
	messageBoundary := int64(1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(fixture.runtimeDB))
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope(fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID),
		RuntimeWriteId: "rwrite_late_close_winner_start", ModelRequestId: "mreq_late_close_winner_start",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_late_close_winner_start"}`,
		ContextThroughMessageSequence: &messageBoundary, RequestKind: "agent_provider_request",
	}); err == nil {
		t.Fatal("late Request Start succeeded after CLOSE won first_mail custody")
	}

	resumeSourceID := "evt_resume_close_before_first_mail_start"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumedRuntime, resumed := startClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-close_before_start", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, resumed)
	if len(resumed.ContextEntries) != 0 || len(resumed.TurnFacts.Events) != 0 {
		t.Fatalf("CLOSE-won cold context = entries:%#v facts:%#v; want cancelled first_mail omitted without fabricated Turn", resumed.ContextEntries, resumed.TurnFacts)
	}

	laterSourceID := "evt_later_after_close_before_start"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, laterSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, laterSourceID)
	laterDeliveryID := agentMailDeliveryID(laterSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID), DeliveryId: laterDeliveryID,
		TargetThreadId: fixture.childID, SourceToolUseEventId: laterSourceID, Content: "execute only the later resumed input",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver later input after CLOSE-won resume = %#v/%v", delivered, err)
	}
	laterRuntimeInputID := "agent_mail:" + laterDeliveryID
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, resumedRuntime.port, fixture.sessionID, fixture.podUID, laterRuntimeInputID, resumedRuntime.acceptResultPath)
	laterStart := resumedRuntime.providerStart(t, fixture.admin, fixture.sessionID, fixture.childID)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	resumedRuntime.close(t)
	var laterSequence int64
	var finalStarts, siblingStarts int
	var finalSiblingInbox, finalSiblingQueue string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT sequence FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$4),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$5),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$6 AND type='span.model_request_start')`,
		fixture.sessionID, fixture.childID, laterRuntimeInputID, siblingRuntimeInputID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, siblingRuntimeInputID), siblingID,
	).Scan(&laterSequence, &finalStarts, &finalSiblingInbox, &finalSiblingQueue, &siblingStarts); err != nil {
		t.Fatalf("read later CLOSE-won execution: %v", err)
	}
	providerWire := string(laterStart.Request)
	if laterStart.ProviderRequests != 2 || laterSequence != 2 || finalStarts != 1 ||
		finalSiblingInbox != "accepted" || finalSiblingQueue != queue.StatusAcknowledged || siblingStarts != 1 ||
		strings.Contains(providerWire, "execute this input once") || !strings.Contains(providerWire, "execute only the later resumed input") {
		t.Fatalf("later CLOSE-won execution = providers:%d sequence:%d starts:%d sibling:%s/%s/%d wire:%s",
			laterStart.ProviderRequests, laterSequence, finalStarts, finalSiblingInbox, finalSiblingQueue, siblingStarts, providerWire)
	}
}

func TestSubagentFirstMailLeaseTakeoverFencesStaleRunner(t *testing.T) {
	fixture := newSubagentMailFixture(t, "lease_takeover")
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	baseQueue := tetralqueue.NewServer(queueStore, nil)
	countedSender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		baseQueue, countedSender,
	)
	direct := runner.Deliverer.(RuntimePodDirectDeliverer)
	baseStore := direct.Store.(*PostgreSQLRuntimeDeliveryStore)
	takeoverStore := &takeoverAtAgentMailAuthorityStore{
		PostgreSQLRuntimeDeliveryStore: baseStore,
		queueStore:                     queueStore,
		admin:                          fixture.admin,
		jobID:                          fixture.jobID,
	}
	runner.Deliverer = RuntimePodDirectDeliverer{Store: takeoverStore, Sender: countedSender}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("stale runner after lease takeover = active:%t err:%v", active, err)
	}
	var inboxStatus, queueStatus, currentLease string
	var attempts, messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID).Scan(
		&inboxStatus, &queueStatus, &currentLease, &attempts, &messages, &starts,
	); err != nil {
		t.Fatalf("read stale runner custody: %v", err)
	}
	if countedSender.agentMailCalls != 0 || inboxStatus != "delivering" || queueStatus != queue.StatusLeased ||
		currentLease == "" || currentLease == takeoverStore.staleJob.LeaseToken || attempts != 2 || messages != 0 || starts != 0 {
		t.Fatalf("stale runner custody = Runtime:%d Inbox:%s Queue:%s leaseChanged:%t attempts:%d Messages:%d starts:%d",
			countedSender.agentMailCalls, inboxStatus, queueStatus, currentLease != takeoverStore.staleJob.LeaseToken, attempts, messages, starts)
	}
	currentJob := takeoverStore.staleJob
	currentJob.LeaseToken = takeoverStore.currentLease.LeaseToken
	currentJob.AttemptCount = int32(takeoverStore.currentLease.AttemptCount)
	currentJob.MaxAttempts = int32(takeoverStore.currentLease.MaxAttempts)
	result, err := (RuntimePodDirectDeliverer{Store: baseStore, Sender: countedSender}).DeliverRuntimeJob(context.Background(), currentJob)
	if err != nil || (result.Status != RuntimeDeliveryAccepted && result.Status != RuntimeDeliveryDuplicate) {
		t.Fatalf("current lease delivery = %#v/%v", result, err)
	}
	if err := (&JobRunner{Queue: baseQueue}).applyRuntimeDeliveryResult(context.Background(), currentJob, result); err != nil {
		t.Fatalf("current lease Queue settlement: %v", err)
	}
	started := runtimeProcess.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	runtimeProcess.kill(t)
	if countedSender.agentMailCalls != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("current owner convergence = Runtime:%d Provider:%d; want one same-ID execution", countedSender.agentMailCalls, started.ProviderInvocations)
	}
}

func TestSubagentFirstMailLeaseTakeoverAfterFenceConvergesSameIDOnce(t *testing.T) {
	fixture := newSubagentMailFixture(t, "lease_takeover_after_fence")
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	baseQueue := tetralqueue.NewServer(queueStore, nil)
	blockingSender := &blockingAgentMailSender{
		RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}),
		entered:              make(chan struct{}), release: make(chan struct{}),
	}
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		baseQueue, blockingSender,
	)
	finished := make(chan error, 1)
	go func() {
		_, err := runner.RunOnceWithActivity(context.Background())
		finished <- err
	}()
	select {
	case <-blockingSender.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("stale first_mail owner did not cross the final authority fence")
	}
	var staleLease string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT lease_token FROM queue_jobs
		WHERE workspace_id='default' AND id=$1 AND status='leased'`, fixture.jobID).Scan(&staleLease); err != nil {
		t.Fatalf("read stale post-fence lease: %v", err)
	}
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND lease_token=$2`, fixture.jobID, staleLease); err != nil {
		t.Fatalf("expire post-fence stale lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim post-fence stale lease = %d/%v", reclaimed, err)
	}
	current, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "first_mail-after-fence-current",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(current) != 1 || current[0].ID != fixture.jobID || current[0].LeaseToken == staleLease {
		t.Fatalf("install post-fence current lease = %#v/%v", current, err)
	}
	currentJob, err := DecodeRuntimeJob(queueJobProto(current[0]))
	if err != nil {
		t.Fatalf("decode post-fence current job: %v", err)
	}
	direct := runner.Deliverer.(RuntimePodDirectDeliverer)
	result, err := (RuntimePodDirectDeliverer{Store: direct.Store, Sender: blockingSender.RuntimeCommandSender}).DeliverRuntimeJob(context.Background(), currentJob)
	if err != nil || (result.Status != RuntimeDeliveryAccepted && result.Status != RuntimeDeliveryDuplicate) {
		t.Fatalf("current owner before stale transport = %#v/%v", result, err)
	}
	if err := (&JobRunner{Queue: baseQueue}).applyRuntimeDeliveryResult(context.Background(), currentJob, result); err != nil {
		t.Fatalf("settle current owner before stale transport: %v", err)
	}
	started := runtimeProcess.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	close(blockingSender.release)
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("stale post-fence owner did not return")
	}
	var queueStatus string
	var currentLease sql.NullString
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT status,lease_token FROM queue_jobs
		WHERE workspace_id='default' AND id=$1`, fixture.jobID).Scan(&queueStatus, &currentLease); err != nil {
		t.Fatalf("read post-fence current authority: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || currentLease.Valid {
		t.Fatalf("stale post-fence owner changed Queue = %s/%v; want acknowledged with no lease", queueStatus, currentLease)
	}
	runtimeProcess.kill(t)
	var messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start')`,
		fixture.sessionID, fixture.childID).Scan(&messages, &starts); err != nil {
		t.Fatalf("read post-fence convergence: %v", err)
	}
	if blockingSender.calls != 1 || messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("post-fence convergence = stale Runtime calls:%d Messages:%d starts:%d providers:%d", blockingSender.calls, messages, starts, started.ProviderInvocations)
	}
}

func TestLaterSubagentMailNPlusOneFailsOnlyExactChild(t *testing.T) {
	fixture := newSubagentMailFixture(t, "ordinary_n_plus_one")
	acceptingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, acceptingRuntime.port, fixture.sessionID, fixture.podUID)
	firstRun := acceptingRuntime.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	acceptingRuntime.kill(t)
	if firstRun.ProviderInvocations != 1 {
		t.Fatalf("first_mail control Provider calls = %d; want 1", firstRun.ProviderInvocations)
	}

	connection, err := grpc.NewClient(fixture.bridgeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial production Bridge: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	sourceID := "evt_ordinary_n_plus_one_mail"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, sourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, sourceID)
	deliveryID := agentMailDeliveryID(sourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope:      bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		DeliveryId: deliveryID, TargetThreadId: fixture.childID, SourceToolUseEventId: sourceID,
		Content: "ordinary follow-up that must not acquire first_mail lineage",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver ordinary agent mail through Bridge gRPC = %#v/%v", delivered, err)
	}
	runtimeInputID := "agent_mail:" + deliveryID
	var jobID string
	var maxAttempts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT id,max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, runtimeInputID)).Scan(&jobID, &maxAttempts); err != nil {
		t.Fatalf("read ordinary mail Queue custody: %v", err)
	}
	rejectingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID, true)
	for attempt := 1; attempt < maxAttempts; attempt++ {
		if attempt > 1 {
			waitForQueueJobAvailable(t, fixture.admin, jobID)
		}
		runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID)
	}
	waitForQueueJobAvailable(t, fixture.admin, jobID)
	finalRuntimeRunner := newSubagentRuntimeQueueRunner(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID, nil, nil)
	finalRuntimeRunner.Deliverer = &finalizationCutDeliverer{RuntimePodDirectDeliverer: finalRuntimeRunner.Deliverer.(RuntimePodDirectDeliverer), failBeforeCommit: true}
	if active, err := finalRuntimeRunner.RunOnceWithActivity(context.Background()); !active || err == nil {
		t.Fatalf("ordinary final Runtime opportunity cut = active:%t err:%v", active, err)
	}
	reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("ordinary rejection boundary = %q/%d/%s", reason, providers, raw)
	}
	rejectingRuntime.kill(t)
	expireAndReclaimQueueJob(t, fixture.runtimeDB, fixture.admin, jobID)
	countedSender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	finalizer := newSubagentRuntimeQueueRunner(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID, nil, countedSender)
	if active, err := finalizer.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("ordinary N+1 finalizer = active:%t err:%v", active, err)
	}
	var inboxStatus, queueStatus, childStatus string
	var attempts, starts, parentNotifications int
	parentID = parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4
		  AND type='agent.thread_message_sent' AND payload_json::jsonb ->> 'target_thread_id'=$5)`,
		runtimeInputID, jobID, fixture.sessionID, fixture.childID, parentID).Scan(&inboxStatus, &queueStatus, &attempts, &childStatus, &starts, &parentNotifications); err != nil {
		t.Fatalf("read ordinary N+1 settlement: %v", err)
	}
	if countedSender.agentMailCalls != 0 || inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts+1 ||
		childStatus != "failed" || starts != 1 || parentNotifications != 1 {
		t.Fatalf("later-mail N+1 = Runtime:%d Inbox:%s Queue:%s attempts:%d child:%s starts:%d notifications:%d", countedSender.agentMailCalls, inboxStatus, queueStatus, attempts, childStatus, starts, parentNotifications)
	}
}

func TestSubagentMailNPlusOneLeaseTakeoverFencesStaleFinalizer(t *testing.T) {
	fixture := prepareOrdinaryAgentMailNPlusOnePending(t, "ordinary_takeover")
	oldLease := leaseOrdinaryAgentMailFinalizer(t, fixture.queueStore, fixture.jobID, "ordinary-stale-finalizer")
	oldJob, err := DecodeRuntimeJob(queueJobProto(oldLease))
	if err != nil {
		t.Fatalf("decode stale ordinary finalizer lease: %v", err)
	}
	observer := &agentMailFinalizationQueueObserver{QueueClient: tetralqueue.NewServer(fixture.queueStore, nil)}
	blocked := &blockingAgentMailFinalizer{
		RuntimePodDirectDeliverer: fixture.direct,
		entered:                   make(chan RuntimeJob, 1),
		release:                   make(chan struct{}),
	}
	runner := &JobRunner{Queue: observer, Deliverer: blocked}
	finished := make(chan error, 1)
	go func() {
		finished <- runner.processRuntimeJob(context.Background(), queueJobProto(oldLease), JobRunnerConfig{
			LeaseDuration: time.Minute, HeartbeatInterval: time.Hour,
		})
	}()
	planned := <-blocked.entered
	if planned.JobID != oldJob.JobID || planned.LeaseToken != oldJob.LeaseToken || !runtimeJobAgentMailFinalizationOnly(planned) {
		t.Fatalf("stale ordinary finalizer plan = %#v; want exact N+1 lease", planned)
	}
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND lease_token=$2`, fixture.jobID, oldJob.LeaseToken); err != nil {
		t.Fatalf("expire stale ordinary finalizer lease: %v", err)
	}
	if reclaimed, err := fixture.queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput, Limit: 1,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim stale ordinary finalizer lease = %d/%v; want one", reclaimed, err)
	}
	currentLease := leaseOrdinaryAgentMailFinalizer(t, fixture.queueStore, fixture.jobID, "ordinary-current-finalizer")
	if currentLease.LeaseToken == oldJob.LeaseToken || currentLease.AttemptCount != fixture.maxAttempts+1 {
		t.Fatalf("ordinary finalizer takeover = tokenChanged:%t attempts:%d; want true/%d",
			currentLease.LeaseToken != oldJob.LeaseToken, currentLease.AttemptCount, fixture.maxAttempts+1)
	}
	currentJob, err := DecodeRuntimeJob(queueJobProto(currentLease))
	if err != nil {
		t.Fatalf("decode current ordinary finalizer lease: %v", err)
	}
	before := readOrdinaryAgentMailFinalizationState(t, fixture.admin, currentJob)
	close(blocked.release)
	if err := <-finished; err != nil {
		t.Fatalf("stale ordinary finalizer returned error: %v", err)
	}
	if blocked.result.Status != RuntimeDeliveryAuthorityLost || blocked.result.QueueLeaseSettled || blocked.err != nil {
		t.Fatalf("stale ordinary finalizer result = %#v/%v; want authority lost", blocked.result, blocked.err)
	}
	after := readOrdinaryAgentMailFinalizationState(t, fixture.admin, currentJob)
	if after != before || observer.deadLetterCalls != 0 || observer.retryCalls != 0 || fixture.sender.agentMailCalls != 0 {
		t.Fatalf("stale ordinary finalizer mutated custody: before=%+v after=%+v QueueDeadLetter=%d QueueRetry=%d Runtime=%d",
			before, after, observer.deadLetterCalls, observer.retryCalls, fixture.sender.agentMailCalls)
	}
	if err := runner.processRuntimeJob(context.Background(), queueJobProto(currentLease), JobRunnerConfig{
		LeaseDuration: time.Minute, HeartbeatInterval: time.Hour,
	}); err != nil {
		t.Fatalf("current ordinary finalizer: %v", err)
	}
	assertOrdinaryAgentMailFinalizationSettled(t, fixture, currentJob, observer)
}

func TestSubagentMailNPlusOneResponseLossReplaysOneTerminalSettlement(t *testing.T) {
	fixture := prepareOrdinaryAgentMailNPlusOnePending(t, "ordinary_response_loss")
	lease := leaseOrdinaryAgentMailFinalizer(t, fixture.queueStore, fixture.jobID, "ordinary-response-loss-finalizer")
	job, err := DecodeRuntimeJob(queueJobProto(lease))
	if err != nil {
		t.Fatalf("decode ordinary response-loss lease: %v", err)
	}
	observer := &agentMailFinalizationQueueObserver{QueueClient: tetralqueue.NewServer(fixture.queueStore, nil)}
	lost := &lostAgentMailFinalizationResponse{RuntimePodDirectDeliverer: fixture.direct}
	runner := &JobRunner{Queue: observer, Deliverer: lost}
	if err := runner.processRuntimeJob(context.Background(), queueJobProto(lease), JobRunnerConfig{
		LeaseDuration: time.Minute, HeartbeatInterval: time.Hour,
	}); !errors.Is(err, errSyntheticAgentMailFinalizationResponseLoss) {
		t.Fatalf("ordinary finalizer response loss = %v; want synthetic post-commit loss", err)
	}
	if !lost.committed.QueueLeaseSettled || lost.committed.Status != RuntimeDeliveryRejected || lost.calls != 1 {
		t.Fatalf("ordinary finalizer committed result = %#v calls:%d", lost.committed, lost.calls)
	}
	assertOrdinaryAgentMailFinalizationSettled(t, fixture, job, observer)
	beforeReplay := readOrdinaryAgentMailFinalizationState(t, fixture.admin, job)
	replayRunner := &JobRunner{Queue: observer, Deliverer: fixture.direct}
	if err := replayRunner.processRuntimeJob(context.Background(), queueJobProto(lease), JobRunnerConfig{
		LeaseDuration: time.Minute, HeartbeatInterval: time.Hour,
	}); err != nil {
		t.Fatalf("replay lost ordinary finalizer response: %v", err)
	}
	afterReplay := readOrdinaryAgentMailFinalizationState(t, fixture.admin, job)
	if afterReplay != beforeReplay || observer.deadLetterCalls != 0 || observer.retryCalls != 0 || fixture.sender.agentMailCalls != 0 {
		t.Fatalf("ordinary finalizer replay duplicated settlement: before=%+v after=%+v QueueDeadLetter=%d QueueRetry=%d Runtime=%d",
			beforeReplay, afterReplay, observer.deadLetterCalls, observer.retryCalls, fixture.sender.agentMailCalls)
	}
}

type ordinaryAgentMailFinalizationFixture struct {
	subagentMailFixture
	runtimeInputID string
	jobID          string
	maxAttempts    int
	queueStore     *queue.PostgreSQLQueueStore
	direct         RuntimePodDirectDeliverer
	sender         *countingAgentMailSender
}

func prepareOrdinaryAgentMailNPlusOnePending(t *testing.T, suffix string) ordinaryAgentMailFinalizationFixture {
	t.Helper()
	fixture := newSubagentMailFixture(t, suffix)
	acceptingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, acceptingRuntime.port, fixture.sessionID, fixture.podUID)
	if execution := acceptingRuntime.providerStart(t); execution.ProviderInvocations != 1 {
		t.Fatalf("ordinary finalizer first_mail Provider calls = %d; want 1", execution.ProviderInvocations)
	}
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	acceptingRuntime.kill(t)

	connection, err := grpc.NewClient(fixture.bridgeAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial ordinary finalizer Bridge: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client := bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	sourceID := "evt_" + suffix + "_mail"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, sourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, sourceID)
	deliveryID := agentMailDeliveryID(sourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope:      bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		DeliveryId: deliveryID, TargetThreadId: fixture.childID, SourceToolUseEventId: sourceID,
		Content: "ordinary follow-up finalization ownership proof",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver ordinary finalizer mail = %#v/%v", delivered, err)
	}
	runtimeInputID := "agent_mail:" + deliveryID
	var jobID string
	var maxAttempts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT id,max_attempts FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`, queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, runtimeInputID)).Scan(&jobID, &maxAttempts); err != nil {
		t.Fatalf("read ordinary finalizer Queue custody: %v", err)
	}
	rejectingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID, true)
	for attempt := 1; attempt < maxAttempts; attempt++ {
		if attempt > 1 {
			waitForQueueJobAvailable(t, fixture.admin, jobID)
		}
		runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID)
	}
	waitForQueueJobAvailable(t, fixture.admin, jobID)
	finalRuntimeRunner := newSubagentRuntimeQueueRunner(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID, nil, nil)
	finalRuntimeRunner.Deliverer = &finalizationCutDeliverer{
		RuntimePodDirectDeliverer: finalRuntimeRunner.Deliverer.(RuntimePodDirectDeliverer), failBeforeCommit: true,
	}
	if active, err := finalRuntimeRunner.RunOnceWithActivity(context.Background()); !active || err == nil {
		t.Fatalf("ordinary final Runtime opportunity cut = active:%t err:%v", active, err)
	}
	if reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime); reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("ordinary finalizer rejection boundary = %q/%d/%s", reason, providers, raw)
	}
	rejectingRuntime.kill(t)
	expireAndReclaimQueueJob(t, fixture.runtimeDB, fixture.admin, jobID)
	clientForStore := dbconnect.NewClientForTesting(fixture.runtimeDB)
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(clientForStore, queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	})
	sender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	preparedRunner := newSubagentRuntimeQueueRunner(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID, nil, sender)
	return ordinaryAgentMailFinalizationFixture{
		subagentMailFixture: fixture, runtimeInputID: runtimeInputID, jobID: jobID, maxAttempts: maxAttempts,
		queueStore: queueStore, direct: preparedRunner.Deliverer.(RuntimePodDirectDeliverer), sender: sender,
	}
}

func leaseOrdinaryAgentMailFinalizer(t *testing.T, store *queue.PostgreSQLQueueStore, jobID, owner string) *queue.Job {
	t.Helper()
	leased, err := store.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: owner,
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(leased) != 1 || leased[0].ID != jobID {
		t.Fatalf("lease ordinary N+1 finalizer = %#v/%v; want %s", leased, err, jobID)
	}
	return leased[0]
}

type ordinaryAgentMailFinalizationState struct {
	inboxStatus, inboxUpdated, queueStatus, queueToken, queueError, queueUpdated, childStatus string
	receivedProcessed                                                                         bool
	attempts, parentNotifications                                                             int
}

func readOrdinaryAgentMailFinalizationState(t *testing.T, admin *sql.DB, job RuntimeJob) ordinaryAgentMailFinalizationState {
	t.Helper()
	var state ordinaryAgentMailFinalizationState
	parentID := parentThreadIDForChild(t, admin, job.SessionID, job.SessionThreadID)
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT updated_at::text FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
		(SELECT processed_at IS NOT NULL FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND event_id=$4),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT COALESCE(lease_token,'') FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT COALESCE(last_error_kind,'') FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT updated_at::text FROM queue_jobs WHERE workspace_id='default' AND id=$5),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3
		  AND type='agent.thread_message_sent' AND payload_json::jsonb ->> 'target_thread_id'=$6),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3)`,
		job.SessionID, job.RuntimeInputID, job.SessionThreadID, stableRuntimeID("agent_mail_received_event", job.WorkspaceID, job.SessionID, job.SessionThreadID, strings.TrimPrefix(job.RuntimeInputID, "agent_mail:")),
		job.JobID, parentID).Scan(
		&state.inboxStatus, &state.inboxUpdated, &state.receivedProcessed,
		&state.queueStatus, &state.queueToken, &state.attempts, &state.queueError, &state.queueUpdated,
		&state.parentNotifications, &state.childStatus,
	); err != nil {
		t.Fatalf("read ordinary finalization state: %v", err)
	}
	return state
}

func assertOrdinaryAgentMailFinalizationSettled(
	t *testing.T,
	fixture ordinaryAgentMailFinalizationFixture,
	job RuntimeJob,
	observer *agentMailFinalizationQueueObserver,
) {
	t.Helper()
	state := readOrdinaryAgentMailFinalizationState(t, fixture.admin, job)
	if state.inboxStatus != "dead_lettered" || !state.receivedProcessed || state.queueStatus != queue.StatusDeadLettered ||
		state.queueToken != "" || state.attempts != fixture.maxAttempts+1 || state.queueError != "runtime_delivery_exhausted" ||
		state.parentNotifications != 1 || state.childStatus != "failed" || observer.deadLetterCalls != 0 ||
		observer.retryCalls != 0 || fixture.sender.agentMailCalls != 0 {
		t.Fatalf("ordinary atomic finalization = %+v QueueDeadLetter=%d QueueRetry=%d Runtime=%d",
			state, observer.deadLetterCalls, observer.retryCalls, fixture.sender.agentMailCalls)
	}
}

type subagentMailFixture struct {
	runtimeDB, admin              *sql.DB
	bridgeAddress                 string
	sessionID, childID, bindingID string
	podUID, runtimeInputID, jobID string
}

func newSubagentMailFixture(t *testing.T, suffix string) subagentMailFixture {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_mail_" + suffix
	parentID := "thr_parent_" + suffix
	bindingID := "bind_mail_" + suffix
	podUID := "pod_mail_" + suffix
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_parent_"+suffix, "evt_parent_"+suffix, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("first_mail-loss-signing-key")
	spawned := runSubagentProductionComposition(t, BridgeAPIServer{store: store}, sessionID, parentID, bindingID, 1, podUID,
		"worker-"+suffix, "execute this input once", "all")
	var childID, runtimeInputID, jobID string
	if err := admin.QueryRowContext(context.Background(), `SELECT child.id,inbox.runtime_input_id,job.id
		FROM session_threads child
		JOIN session_runtime_inbox inbox ON inbox.workspace_id=child.workspace_id AND inbox.session_id=child.session_id AND inbox.session_thread_id=child.id
		JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id
		 AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE child.workspace_id='default' AND child.session_id=$1 AND child.parent_thread_id=$2
		 AND child.role='subagent' AND inbox.input_kind='agent_mail'`, sessionID, parentID).Scan(&childID, &runtimeInputID, &jobID); err != nil {
		t.Fatalf("read first_mail loss fixture: %v", err)
	}
	return subagentMailFixture{runtimeDB: runtimeDB, admin: admin, bridgeAddress: spawned.BridgeAddress,
		sessionID: sessionID, childID: childID, bindingID: bindingID, podUID: podUID, runtimeInputID: runtimeInputID, jobID: jobID}
}

func parentThreadIDForChild(t *testing.T, admin *sql.DB, sessionID, childID string) string {
	t.Helper()
	var parentID string
	if err := admin.QueryRowContext(context.Background(), `SELECT parent_thread_id FROM session_threads
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&parentID); err != nil {
		t.Fatalf("read child parent: %v", err)
	}
	return parentID
}

func closeChildThroughProductionInterrupt(
	t *testing.T,
	runtimeDB, admin *sql.DB,
	client bridgev1.AgentRuntimeBridgeServiceClient,
	bridgeAddress string,
	parentScope *bridgev1.RuntimeScope,
	sessionID, parentID, childID, bindingID, podUID, sourceID string,
) {
	t.Helper()
	controlID := admitChildCloseThroughProduction(t, admin, client, parentScope, sessionID, parentID, childID, sourceID)
	interruptRuntime, interruptPaths := startInterruptRuntimeComposition(
		t, t.TempDir(), bridgeAddress, sessionID, childID, bindingID, 1, podUID,
	)
	runQueueUntilInterruptSettled(t, runtimeDB, admin, interruptRuntime.port, sessionID, podUID)
	if err := os.WriteFile(interruptPaths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release child-close Runtime composition: %v", err)
	}
	interruptRuntime.wait(t)
	settleChildCloseThroughProduction(t, client, parentScope, controlID, sourceID)
}

func admitChildCloseThroughProduction(
	t *testing.T,
	admin *sql.DB,
	client bridgev1.AgentRuntimeBridgeServiceClient,
	parentScope *bridgev1.RuntimeScope,
	sessionID, parentID, childID, sourceID string,
) string {
	t.Helper()
	seedActorSourceEvent(t, admin, sessionID, parentID, sourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$3,projection_json=$4
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
		sessionID, sourceID, "mreq_"+sourceID, `{"model_tool_call_id":"call_`+sourceID+`"}`); err != nil {
		t.Fatalf("project close source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, "mreq_"+sourceID, sourceID, "call_"+sourceID, "close_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, sourceID)
	admitted, err := client.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: parentScope, SourceToolUseEventId: sourceID, TargetChildThreadId: childID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	})
	if err != nil || admitted.GetCommitted().GetControlOperationId() == "" {
		t.Fatalf("admit child close through Bridge gRPC = %#v/%v", admitted, err)
	}
	return admitted.GetCommitted().GetControlOperationId()
}

func settleChildCloseThroughProduction(
	t *testing.T,
	client bridgev1.AgentRuntimeBridgeServiceClient,
	parentScope *bridgev1.RuntimeScope,
	controlID, sourceID string,
) {
	t.Helper()
	if awaited, err := client.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
		Scope: parentScope, ControlOperationId: controlID,
	}); err != nil || len(awaited.GetCompleted().GetTargets()) != 1 {
		t.Fatalf("await production child close = %#v/%v", awaited, err)
	}
	if closed, err := client.CloseChildControl(context.Background(), &bridgev1.CloseChildControlRequest{
		Scope: parentScope, ControlOperationId: controlID,
	}); err != nil || len(closed.GetCommitted().GetChildren()) != 1 {
		t.Fatalf("commit production child close = %#v/%v", closed, err)
	}
	if settled, err := client.SettleToolResult(context.Background(), &bridgev1.SettleToolResultRequest{
		Scope: parentScope, Settlement: bridgeCompletedToolSettlementForTest(sourceID, "child closed"),
	}); err != nil || settled.GetCommitted() == nil {
		t.Fatalf("settle production child close Tool result = %#v/%v", settled, err)
	}
}

type lostResponseAfterRequestStartSender struct {
	RuntimeCommandSender
	admin              *sql.DB
	sessionID, childID string
}

func (s *lostResponseAfterRequestStartSender) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	response, err := s.RuntimeCommandSender.AcceptAgentMail(ctx, target, request)
	if err != nil {
		return response, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var starts int
		if queryErr := s.admin.QueryRowContext(ctx, `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'`,
			s.sessionID, s.childID).Scan(&starts); queryErr == nil && starts == 1 {
			return nil, status.Error(codes.Unavailable, "fixture transport response lost after durable Request Start")
		}
		time.Sleep(time.Millisecond)
	}
	return nil, status.Error(codes.DeadlineExceeded, "fixture Request Start barrier was not reached")
}

func (s *lostResponseAfterRequestStartSender) RecoverThread(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	return recoverThreadThroughSender(ctx, s.RuntimeCommandSender, target, request)
}

type birthInterruptAfterLeaseQueueClient struct {
	QueueClient
	once     sync.Once
	birth    func(context.Context) error
	birthErr error
}

type agentMailPreparationFailureStore struct {
	*PostgreSQLRuntimeDeliveryStore
}

type firstCommitInputsPodLossCutStore struct {
	BridgeAPIStore
	mu      sync.Mutex
	cut     bool
	entered chan struct{}
}

type requestStartBarrierBridgeStore struct {
	BridgeAPIStore
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *requestStartBarrierBridgeStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	if request.GetEventType() == "span.model_request_start" {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.BridgeAPIStore.WriteEvent(ctx, request)
}

func (s *firstCommitInputsPodLossCutStore) CommitInputs(ctx context.Context, request *bridgev1.CommitInputsRequest) (*bridgev1.CommitInputsResponse, error) {
	s.mu.Lock()
	cut := !s.cut && request.GetRuntimeInputId() != ""
	if cut {
		s.cut = true
		close(s.entered)
	}
	s.mu.Unlock()
	if cut {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.BridgeAPIStore.CommitInputs(ctx, request)
}

func (s *agentMailPreparationFailureStore) PrepareRuntimeCommand(ctx context.Context, job RuntimeJob) (RuntimeCommandPlan, error) {
	if job.InputKind == "agent_mail" {
		return RuntimeCommandPlan{}, runtimeDeliveryPrepareError{
			kind: "runtime_binding_unavailable", message: "fixture-controlled failure before materialization", retryable: true,
		}
	}
	return s.PostgreSQLRuntimeDeliveryStore.PrepareRuntimeCommand(ctx, job)
}

func (c *birthInterruptAfterLeaseQueueClient) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	response, err := c.QueueClient.Lease(ctx, request)
	if err == nil && len(response.GetJobs()) == 1 {
		c.once.Do(func() { c.birthErr = c.birth(ctx) })
	}
	if err == nil && c.birthErr != nil {
		return nil, c.birthErr
	}
	return response, err
}

type finalizationCutDeliverer struct {
	RuntimePodDirectDeliverer
	failBeforeCommit bool
}

func (d *finalizationCutDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if d.failBeforeCommit {
		d.failBeforeCommit = false
		return RuntimeDeliveryResult{}, errors.New("fixture finalizer crashed before transaction commit")
	}
	return d.RuntimePodDirectDeliverer.FinalizeRuntimeDelivery(ctx, job, result)
}

type blockingAgentMailFinalizer struct {
	RuntimePodDirectDeliverer
	entered chan RuntimeJob
	release chan struct{}
	result  RuntimeDeliveryResult
	err     error
}

func (d *blockingAgentMailFinalizer) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	d.entered <- job
	<-d.release
	d.result, d.err = d.RuntimePodDirectDeliverer.FinalizeRuntimeDelivery(ctx, job, result)
	return d.result, d.err
}

var errSyntheticAgentMailFinalizationResponseLoss = errors.New("synthetic agent-mail finalization response loss")

type lostAgentMailFinalizationResponse struct {
	RuntimePodDirectDeliverer
	committed RuntimeDeliveryResult
	calls     int
}

func (d *lostAgentMailFinalizationResponse) FinalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	d.calls++
	committed, err := d.RuntimePodDirectDeliverer.FinalizeRuntimeDelivery(ctx, job, result)
	if err != nil {
		return committed, err
	}
	d.committed = committed
	return RuntimeDeliveryResult{}, errSyntheticAgentMailFinalizationResponseLoss
}

type agentMailFinalizationQueueObserver struct {
	QueueClient
	deadLetterCalls int
	retryCalls      int
}

func (c *agentMailFinalizationQueueObserver) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	c.deadLetterCalls++
	return c.QueueClient.DeadLetter(ctx, request)
}

func (c *agentMailFinalizationQueueObserver) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	c.retryCalls++
	return c.QueueClient.Retry(ctx, request)
}

type retryObservingQueueClient struct {
	QueueClient
	retryCalls int
}

type takeoverAtAgentMailAuthorityStore struct {
	*PostgreSQLRuntimeDeliveryStore
	queueStore   *queue.PostgreSQLQueueStore
	admin        *sql.DB
	jobID        string
	staleJob     RuntimeJob
	currentLease *queue.Job
}

func (s *takeoverAtAgentMailAuthorityStore) RuntimeInputDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeInputDeliveryAuthority, error) {
	s.staleJob = job
	if _, err := s.admin.ExecContext(ctx, `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND lease_token=$2`, s.jobID, job.LeaseToken); err != nil {
		return RuntimeInputDeliveryAuthority{}, err
	}
	if reclaimed, err := s.queueStore.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID}); err != nil || reclaimed != 1 {
		return RuntimeInputDeliveryAuthority{}, errors.New("fixture failed to reclaim stale Queue lease")
	}
	takeover, err := s.queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "subagent-custody-takeover",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(takeover) != 1 || takeover[0].ID != s.jobID || takeover[0].LeaseToken == job.LeaseToken {
		return RuntimeInputDeliveryAuthority{}, errors.New("fixture failed to install replacement Queue lease")
	}
	s.currentLease = takeover[0]
	return s.PostgreSQLRuntimeDeliveryStore.RuntimeInputDeliveryAuthority(ctx, job)
}

func (c *retryObservingQueueClient) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	c.retryCalls++
	return c.QueueClient.Retry(ctx, request)
}

type countingAgentMailSender struct {
	RuntimeCommandSender
	agentMailCalls  int
	recoveryCalls   int
	recoveryRequest *agentruntimev1.RecoverThreadRequest
}

type blockingAgentMailSender struct {
	RuntimeCommandSender
	entered chan struct{}
	release chan struct{}
	calls   int
}

func recoverThreadThroughSender(
	ctx context.Context,
	sender RuntimeCommandSender,
	target RuntimePodTarget,
	request *agentruntimev1.RecoverThreadRequest,
) (*agentruntimev1.RecoverThreadResponse, error) {
	recoverySender, ok := sender.(RuntimeRecoveryCommandSender)
	if !ok {
		return nil, status.Error(codes.Unavailable, "fixture Runtime recovery sender is unavailable")
	}
	return recoverySender.RecoverThread(ctx, target, request)
}

func (s *blockingAgentMailSender) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	s.calls++
	close(s.entered)
	<-s.release
	return &agentruntimev1.AcceptAgentMailResponse{
		Outcome: &agentruntimev1.AcceptAgentMailResponse_Duplicate{
			Duplicate: &agentruntimev1.AcceptAgentMailDuplicate{},
		},
	}, nil
}

func (s *blockingAgentMailSender) RecoverThread(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	return recoverThreadThroughSender(ctx, s.RuntimeCommandSender, target, request)
}

func (s *countingAgentMailSender) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	s.agentMailCalls++
	return s.RuntimeCommandSender.AcceptAgentMail(ctx, target, request)
}

func (s *countingAgentMailSender) RecoverThread(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.RecoverThreadRequest) (*agentruntimev1.RecoverThreadResponse, error) {
	s.recoveryCalls++
	s.recoveryRequest = request
	return recoverThreadThroughSender(ctx, s.RuntimeCommandSender, target, request)
}

func runSubagentRuntimeQueueOnce(t *testing.T, runtimeDB, admin *sql.DB, port int, sessionID, podUID string) {
	t.Helper()
	runner := newSubagentRuntimeQueueRunner(t, runtimeDB, admin, port, sessionID, podUID, nil, nil)
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("run subagent Queue owner = active:%t err:%v", active, err)
	}
}

func newSubagentRuntimeQueueRunner(
	t *testing.T,
	runtimeDB, admin *sql.DB,
	port int,
	sessionID, podUID string,
	queueClient QueueClient,
	sender RuntimeCommandSender,
) *JobRunner {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_bindings SET agent_runtime_pod_ip='127.0.0.1'
		WHERE workspace_id='default' AND session_id=$1`, sessionID); err != nil {
		t.Fatalf("align subagent Runtime binding: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	queueStore := queue.NewPostgreSQLStoreWithRetryPolicy(client, queue.RetryPolicy{
		BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
	})
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, port)
	deliveryStore.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, []enginekubernetes.BindingCandidate{{
			Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: podUID, PodIP: "127.0.0.1",
		}})
	}}
	if queueClient == nil {
		queueClient = tetralqueue.NewServer(queueStore, nil)
	}
	if sender == nil {
		sender = NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})
	}
	return &JobRunner{
		Queue: queueClient, Workspaces: staticWorkspaceLister{workspace.DefaultID},
		Deliverer: RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender},
		Config:    JobRunnerConfig{LeaseOwner: "subagent-custody-composition", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
}

func waitForQueueJobAvailable(t *testing.T, admin *sql.DB, jobID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var available bool
		if err := admin.QueryRowContext(context.Background(), `SELECT status='pending' AND available_at <= clock_timestamp()
			FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&available); err == nil && available {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Queue job %s did not become available", jobID)
}

func expireAndReclaimQueueJob(t *testing.T, runtimeDB, admin *sql.DB, jobID string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND status='leased'`, jobID); err != nil {
		t.Fatalf("expire Queue lease: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim Queue lease = %d/%v; want 1/nil", reclaimed, err)
	}
}

func waitForThreadRequestEnds(t *testing.T, admin *sql.DB, sessionID, threadID string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'`, sessionID, threadID).Scan(&count); err == nil && count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Thread %s did not reach %d Request Ends", threadID, want)
}

func runSubagentOutputCaptureOnce(t *testing.T, runtimeDB *sql.DB) {
	t.Helper()
	registry, err := tetralsandbox.NewProviderRegistry(map[string]tetralsandbox.ProviderAdapter{
		"daytona": &bridgeMemoryProjectionProvider{},
	})
	if err != nil {
		t.Fatalf("build Subagent output-capture provider registry: %v", err)
	}
	runner := &tetralsandbox.SandboxOutputCaptureJobRunner{
		Queue:     tetralqueue.NewServer(queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB)), nil),
		Store:     tetralsandbox.NewPostgreSQLSandboxOutputCaptureStore(dbconnect.NewClientForTesting(runtimeDB)),
		Providers: registry,
		BlobStore: blob.NewFakeBlobStore(),
		Config: tetralsandbox.SandboxOutputCaptureRunnerConfig{
			WorkspaceID: "default", LeaseOwner: "subagent-resume-output-capture", MaxJobs: 1,
			LeaseDuration: time.Minute, HeartbeatInterval: 10 * time.Second,
		},
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		active, runErr := runner.RunOnceWithActivity(context.Background())
		if runErr != nil {
			t.Fatalf("run Subagent output-capture owner: %v", runErr)
		}
		if active {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("Subagent Runtime did not enqueue output capture")
}

func runQueueUntilInputSettled(t *testing.T, runtimeDB, admin *sql.DB, port int, sessionID, podUID, runtimeInputID string, diagnosticPaths ...string) {
	t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		var statusValue string
		if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
			WHERE workspace_id='default' AND runtime_input_id=$1`, runtimeInputID).Scan(&statusValue); err == nil && (statusValue == "accepted" || statusValue == "committed") {
			var queueStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs WHERE workspace_id='default'
				AND dedupe_key='runtime_input:default:' || $1 || ':' || $2`, sessionID, runtimeInputID).Scan(&queueStatus); err == nil && queueStatus == queue.StatusAcknowledged {
				return
			}
		}
		runner := newSubagentRuntimeQueueRunner(t, runtimeDB, admin, port, sessionID, podUID, nil, nil)
		if _, err := runner.RunOnceWithActivity(context.Background()); err != nil {
			t.Fatalf("run Queue while waiting for Runtime input %s: %v", runtimeInputID, err)
		}
		time.Sleep(time.Millisecond)
	}
	var inboxStatus, queueStatus, errorKind, errorMessage string
	_ = admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status,coalesce(job.last_error_kind,''),coalesce(job.last_error_message,'')
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id
		AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, runtimeInputID).Scan(&inboxStatus, &queueStatus, &errorKind, &errorMessage)
	diagnostic := ""
	if len(diagnosticPaths) == 1 {
		if raw, err := os.ReadFile(diagnosticPaths[0]); err == nil {
			diagnostic = string(raw)
		}
	}
	t.Fatalf("Runtime input %s did not settle: Inbox=%s Queue=%s error=%s/%s Runtime=%s", runtimeInputID, inboxStatus, queueStatus, errorKind, errorMessage, diagnostic)
}

func runQueueUntilInterruptSettled(t *testing.T, runtimeDB, admin *sql.DB, port int, sessionID, podUID string) {
	t.Helper()
	for attempt := 0; attempt < 6; attempt++ {
		var pending int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox
			WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'
			 AND status IN ('queued','delivering','accepted')`, sessionID).Scan(&pending); err == nil && pending == 0 {
			return
		}
		runSubagentRuntimeQueueOnce(t, runtimeDB, admin, port, sessionID, podUID)
	}
	t.Fatal("child interrupt did not settle")
}

func readAgentMailAdmission(t *testing.T, process *attachmentRecoveryProcess) (string, int, string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(process.acceptResultPath)
		if err == nil {
			var result struct {
				Result struct {
					Reason string `json:"reason"`
				} `json:"result"`
				ProviderInvocations int `json:"providerInvocations"`
			}
			if json.Unmarshal(raw, &result) == nil {
				return result.Result.Reason, result.ProviderInvocations, string(raw)
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("agent-mail admission result was not recorded: %s", process.output.String())
	return "", 0, ""
}
