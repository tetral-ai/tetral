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
	"slices"
	"strings"
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
		Action struct {
			Action string `json:"action"`
		} `json:"action"`
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
			if json.Unmarshal(raw, &started) == nil {
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
		result.Decision.State.State != "idle" || result.Decision.Action.Action != "await_input" ||
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

func TestSubagentOpeningInputRemainsOwnedAfterLocalAdmissionRejection(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_subagent_opening_custody"
		parentID  = "thr_subagent_opening_custody_parent"
		bindingID = "bind_subagent_opening_custody"
		podUID    = "pod_subagent_opening_custody"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_subagent_opening_parent", "evt_subagent_opening_parent", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"spawn the custody worker"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, parentID); err != nil {
		t.Fatalf("seed spawning parent context: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-opening-custody-signing-key")
	spawned := runSubagentProductionComposition(
		t, BridgeAPIServer{store: store}, sessionID, parentID, bindingID, 1, podUID,
		"custody-worker", "execute the opening task exactly once", "all",
	)
	var childID, runtimeInputID, jobID string
	if err := admin.QueryRowContext(context.Background(), `SELECT child.id,inbox.runtime_input_id,job.id
		FROM session_threads child
		JOIN session_runtime_inbox inbox ON inbox.workspace_id=child.workspace_id AND inbox.session_id=child.session_id AND inbox.session_thread_id=child.id
		JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id
		 AND job.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		WHERE child.workspace_id='default' AND child.session_id=$1 AND child.parent_thread_id=$2
		 AND child.role='subagent' AND inbox.input_kind='agent_mail'`, sessionID, parentID).Scan(&childID, &runtimeInputID, &jobID); err != nil {
		t.Fatalf("read production opening custody: %v", err)
	}

	rejectingRuntime := startAttachmentRecoveryRuntime(
		t, spawned.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID, true,
	)
	runSubagentRuntimeQueueOnce(t, runtimeDB, admin, rejectingRuntime.port, sessionID, podUID)
	rejectionReason, rejectedProviderInvocations, rejectionJSON := readAgentMailAdmission(t, rejectingRuntime)
	if rejectionReason != "local_session_capacity_exceeded" || rejectedProviderInvocations != 0 {
		t.Fatalf("local opening admission = reason:%q providers:%d result:%s; want local_session_capacity_exceeded/0", rejectionReason, rejectedProviderInvocations, rejectionJSON)
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
		t.Fatalf("rejected opening custody = Inbox:%s Queue:%s attempts:%d child:%s requests:%d; want committed/pending/1/idle/0",
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
		t.Fatalf("read converged opening custody: %v", err)
	}
	if finalInbox != "accepted" || finalQueue != queue.StatusAcknowledged || finalAttempts != 2 || messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("converged opening custody = Inbox:%s Queue:%s attempts:%d messages:%d starts:%d providers:%d",
			finalInbox, finalQueue, finalAttempts, messages, starts, started.ProviderInvocations)
	}
}

func TestSubagentOpeningInputFinalAttemptFailsOnlyExactChild(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_opening_final_failure"
		parentID  = "thr_opening_final_parent"
		bindingID = "bind_opening_final_failure"
		podUID    = "pod_opening_final_failure"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_opening_final_parent", "evt_opening_final_parent", 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("opening-final-failure-signing-key")
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
		t.Fatalf("read final opening custody: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT max_attempts FROM queue_jobs WHERE workspace_id='default' AND id=$1`, jobID).Scan(&maxAttempts); err != nil {
		t.Fatalf("read opening attempt budget: %v", err)
	}
	rejectingRuntime := startAttachmentRecoveryRuntime(t, spawned.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID, true)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			waitForQueueJobAvailable(t, admin, jobID)
		}
		runSubagentRuntimeQueueOnce(t, runtimeDB, admin, rejectingRuntime.port, sessionID, podUID)
	}
	reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("final opening admission = %q/%d/%s; want capacity rejection with zero Provider calls", reason, providers, raw)
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
		t.Fatalf("read final opening settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts ||
		childStatus != "failed" || parentStatus == "failed" || messages != 1 || starts != 0 || parentNotifications != 1 {
		t.Fatalf("final opening settlement = Inbox:%s Queue:%s attempts:%d child:%s parent:%s Messages:%d starts:%d notifications:%d",
			inboxStatus, queueStatus, attempts, childStatus, parentStatus, messages, starts, parentNotifications)
	}
	messageBoundary := int64(1)
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: bridgeAPIScope(sessionID, childID, bindingID, 1, podUID), RuntimeWriteId: "rwrite_late_opening_start",
		ModelRequestId: "mreq_late_opening_start", EventType: "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_late_opening_start"}`,
		ContextThroughMessageSequence: &messageBoundary, RequestKind: "agent_provider_request",
	}); err == nil {
		t.Fatal("late Request Start succeeded after opening-child terminal fence")
	}
}

func TestSubagentOpeningInputResponseAndAckLossReplayExecutionWitness(t *testing.T) {
	for _, test := range []struct {
		name         string
		dropResponse bool
		dropAck      bool
	}{
		{name: "Runtime response lost before accepted stamp", dropResponse: true},
		{name: "Queue ACK lost after accepted stamp", dropAck: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newOpeningRuntimeFixture(t, "loss_"+stableRuntimeID("case", test.name))
			runtimeProcess := startAttachmentRecoveryRuntime(
				t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
				fixture.bindingID, 1, fixture.podUID,
			)
			baseSender := RuntimeCommandSender(NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}))
			if test.dropResponse {
				baseSender = &lostAgentMailResponseSender{RuntimeCommandSender: baseSender}
			}
			queueClient := QueueClient(nil)
			if test.dropAck {
				queueClient = &lostAckQueueClient{QueueClient: tetralqueue.NewServer(
					queue.NewPostgreSQLStoreWithRetryPolicy(dbconnect.NewClientForTesting(fixture.runtimeDB), queue.RetryPolicy{
						BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, RandomInt64: func(int64) int64 { return 0 },
					}), nil,
				)}
			}
			runner := newSubagentRuntimeQueueRunner(
				t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
				queueClient, baseSender,
			)
			active, firstErr := runner.RunOnceWithActivity(context.Background())
			if !active || (test.dropResponse && firstErr != nil) || (test.dropAck && firstErr == nil) {
				t.Fatalf("first loss-cut run = active:%t err:%v", active, firstErr)
			}
			started := runtimeProcess.providerStart(t)
			waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)

			if test.dropAck {
				if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs
					SET leased_until=clock_timestamp()-interval '1 second'
					WHERE workspace_id='default' AND id=$1`, fixture.jobID); err != nil {
					t.Fatalf("expire lost-ACK lease: %v", err)
				}
				queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(fixture.runtimeDB))
				if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID}); err != nil || reclaimed != 1 {
					t.Fatalf("reclaim lost-ACK lease = %d/%v", reclaimed, err)
				}
			} else {
				waitForQueueJobAvailable(t, fixture.admin, fixture.jobID)
			}
			runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID)
			runtimeProcess.kill(t)

			var inboxStatus, queueStatus string
			var attempts, messages, starts int
			if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
				(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
				(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
				(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
				fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID).Scan(
				&inboxStatus, &queueStatus, &attempts, &messages, &starts,
			); err != nil {
				t.Fatalf("read loss-cut convergence: %v", err)
			}
			if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != 2 ||
				messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
				t.Fatalf("loss-cut convergence = Inbox:%s Queue:%s attempts:%d Messages:%d starts:%d providers:%d",
					inboxStatus, queueStatus, attempts, messages, starts, started.ProviderInvocations)
			}
		})
	}
}

func TestSubagentOpeningInputFinalUnknownUsesExactRequestStart(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "final_unknown_started")
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs SET max_attempts=1
		WHERE workspace_id='default' AND id=$1 AND status='pending' AND attempt_count=0`, fixture.jobID); err != nil {
		t.Fatalf("set one-attempt Queue policy: %v", err)
	}
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	sender := &lostResponseAfterRequestStartSender{
		RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{}),
		admin:                fixture.admin, sessionID: fixture.sessionID, childID: fixture.childID,
	}
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		nil, sender,
	)
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("final unknown delivery = active:%t err:%v", active, err)
	}
	started := runtimeProcess.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	runtimeProcess.kill(t)
	var inboxStatus, queueStatus, childStatus string
	var attempts, messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		fixture.runtimeInputID, fixture.jobID, fixture.sessionID, fixture.childID).Scan(
		&inboxStatus, &queueStatus, &attempts, &childStatus, &messages, &starts,
	); err != nil {
		t.Fatalf("read final unknown witness: %v", err)
	}
	if inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged || attempts != 1 || childStatus == "failed" ||
		messages != 1 || starts != 1 || started.ProviderInvocations != 1 {
		t.Fatalf("final unknown witness = Inbox:%s Queue:%s attempts:%d child:%s Messages:%d starts:%d providers:%d",
			inboxStatus, queueStatus, attempts, childStatus, messages, starts, started.ProviderInvocations)
	}
}

func TestSubagentOpeningInputFinalizerCrashUsesClampedNPlusOne(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "finalizer_crash")
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

func TestSubagentExecutedCloseColdResumeAndLaterInputProductionComposition(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "executed_close_resume")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	rejectingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID, true)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID)
	reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("materialize opening before second input = %q/%d/%s", reason, providers, raw)
	}
	rejectingRuntime.kill(t)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()+interval '1 hour'
		WHERE workspace_id='default' AND id=$1 AND status='pending'`, fixture.jobID); err != nil {
		t.Fatalf("hold opening retry while second input executes: %v", err)
	}

	client := startActorProductionBridge(t, fixture.runtimeDB)
	secondSourceID := "evt_executed_close_second_mail"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, secondSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, secondSourceID)
	secondDeliveryID := agentMailDeliveryID(secondSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID), DeliveryId: secondDeliveryID,
		TargetThreadId: fixture.childID, SourceToolUseEventId: secondSourceID, Content: "second message consumed with the opening history",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver second rehearsal Message = %#v/%v", delivered, err)
	}
	secondRuntimeInputID := "agent_mail:" + secondDeliveryID
	executingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, executingRuntime.port, fixture.sessionID, fixture.podUID, secondRuntimeInputID)
	firstExecution := executingRuntime.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	runSubagentOutputCaptureOnce(t, fixture.runtimeDB)
	waitForThreadIdleEvents(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	executingRuntime.kill(t)
	if firstExecution.ProviderInvocations != 1 {
		t.Fatalf("rehearsal Provider invocations = %d; want 1", firstExecution.ProviderInvocations)
	}

	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID, "evt_executed_close_after_start")
	var openingInbox, openingQueue, secondInbox, secondQueue, childStatus, startID, endID, runningID, turnIdleID, closeID string
	var startBoundary, messages, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$4),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$5 AND id=$6),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND kind='user'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='span.model_request_start'),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='span.model_request_start' ORDER BY sequence DESC LIMIT 1),
		(SELECT (projection_json::jsonb->>'context_through_message_sequence')::int FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='span.model_request_start' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='span.model_request_end' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='session.thread_status_running' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='session.thread_status_idle' AND payload_json::jsonb #>> '{stop_reason,type}' <> 'closed_for_runtime' ORDER BY sequence DESC LIMIT 1),
		(SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$5 AND session_thread_id=$6 AND type='session.thread_status_idle' ORDER BY sequence DESC LIMIT 1)`,
		fixture.runtimeInputID, fixture.jobID, secondRuntimeInputID,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, secondRuntimeInputID), fixture.sessionID, fixture.childID).Scan(
		&openingInbox, &openingQueue, &secondInbox, &secondQueue, &childStatus, &messages, &starts,
		&startID, &startBoundary, &endID, &runningID, &turnIdleID, &closeID,
	); err != nil {
		t.Fatalf("read Request-Start-won close facts: %v", err)
	}
	if openingInbox != "committed" || openingQueue != queue.StatusCancelled || secondInbox != "committed" || secondQueue != queue.StatusAcknowledged ||
		childStatus != "closed_for_runtime" || messages != 2 || starts != 1 || startBoundary != 2 {
		t.Fatalf("Request-Start-won close = opening:%s/%s second:%s/%s child:%s messages:%d starts:%d boundary:%d",
			openingInbox, openingQueue, secondInbox, secondQueue, childStatus, messages, starts, startBoundary)
	}
	resumeSourceID := "evt_resume_executed_closed_child"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumedRuntime, resumed := startClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-executed_close_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, resumed)
	if got := contextEntrySequences(resumed.ContextEntries); !slices.Equal(got, []int64{1, 2, 3}) {
		t.Fatalf("Request-Start-won historical context sequences = %v; want consumed users [1 2] and retained Assistant [3]", got)
	}
	wantFacts := map[string]bool{runningID: false, startID: false, endID: false, turnIdleID: false, closeID: false}
	for _, event := range resumed.TurnFacts.Events {
		if _, expected := wantFacts[event.EventID]; expected {
			wantFacts[event.EventID] = true
		}
	}
	for eventID, present := range wantFacts {
		if !present {
			t.Fatalf("closed LoadContext omitted exact latest-run Event %s: %#v", eventID, resumed.TurnFacts.Events)
		}
	}

	laterSourceID := "evt_later_after_executed_resume"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, laterSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, laterSourceID)
	laterDeliveryID := agentMailDeliveryID(laterSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID), DeliveryId: laterDeliveryID,
		TargetThreadId: fixture.childID, SourceToolUseEventId: laterSourceID, Content: "one later input after historical cold resume",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver later input after executed resume = %#v/%v", delivered, err)
	}
	laterRuntimeInputID := "agent_mail:" + laterDeliveryID
	runQueueUntilInputSettled(t, fixture.runtimeDB, fixture.admin, resumedRuntime.port, fixture.sessionID, fixture.podUID, laterRuntimeInputID, resumedRuntime.acceptResultPath)
	laterExecution := resumedRuntime.providerStart(t, fixture.admin, fixture.sessionID, fixture.childID)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 2)
	resumedRuntime.close(t)
	var laterSequence int64
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT sequence FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)`,
		fixture.sessionID, fixture.childID, laterRuntimeInputID).Scan(&laterSequence); err != nil {
		t.Fatalf("read later resumed Message sequence: %v", err)
	}
	if laterExecution.ProviderRequests != 1 || laterSequence != 4 || !strings.Contains(string(laterExecution.Request), "one later input after historical cold resume") {
		t.Fatalf("later resumed execution = providers:%d sequence:%d wire:%s", laterExecution.ProviderRequests, laterSequence, laterExecution.Request)
	}

	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID, "evt_close_after_second_run")
	var latestStartID string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT event_id FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start'
		ORDER BY sequence DESC LIMIT 1`, fixture.sessionID, fixture.childID).Scan(&latestStartID); err != nil {
		t.Fatalf("read second-run Request Start: %v", err)
	}
	secondResumeSourceID := "evt_resume_latest_closed_run"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, secondResumeSourceID)
	secondResume := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-executed_close_resume", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": secondResumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, secondResume)
	latestBoundary := int64(0)
	latestFactID := ""
	for _, event := range secondResume.TurnFacts.Events {
		if event.RequestStart != nil && event.EventSequence > latestBoundary {
			latestBoundary = event.EventSequence
			latestFactID = event.EventID
		}
	}
	if latestFactID != latestStartID {
		t.Fatalf("latest closed checkpoint rooted Request Start %s; want latest run %s", latestFactID, latestStartID)
	}
}

func TestSubagentRetainedAssistantAndTerminalToolResultColdResume(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "terminal_tool_resume")
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

func TestSubagentClosedResumeRejectsPostBoundaryMessageWithoutNewRequestStart(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "closed_post_boundary")
	parentID := parentThreadIDForChild(t, fixture.admin, fixture.sessionID, fixture.childID)
	executingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, executingRuntime.port, fixture.sessionID, fixture.podUID)
	if started := executingRuntime.providerStart(t); started.ProviderInvocations != 1 {
		t.Fatalf("post-boundary control execution Provider calls = %d; want 1", started.ProviderInvocations)
	}
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	executingRuntime.kill(t)

	client := startActorProductionBridge(t, fixture.runtimeDB)
	pendingSourceID := "evt_closed_post_boundary_mail"
	seedActorSourceEvent(t, fixture.admin, fixture.sessionID, parentID, pendingSourceID, "agent.tool_use", `{"type":"agent.tool_use","name":"send_message","evaluated_permission":"allow"}`)
	seedBridgeAPIAllowedToolRoute(t, fixture.admin, "default", fixture.sessionID, parentID, pendingSourceID)
	pendingDeliveryID := agentMailDeliveryID(pendingSourceID, fixture.childID)
	if delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID), DeliveryId: pendingDeliveryID,
		TargetThreadId: fixture.childID, SourceToolUseEventId: pendingSourceID, Content: "remain pending above the last Request Start boundary",
	}); err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver post-boundary Message = %#v/%v", delivered, err)
	}
	pendingRuntimeInputID := "agent_mail:" + pendingDeliveryID
	rejectingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID, true)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, rejectingRuntime.port, fixture.sessionID, fixture.podUID)
	reason, providers, raw := readAgentMailAdmission(t, rejectingRuntime)
	if reason != "local_session_capacity_exceeded" || providers != 0 {
		t.Fatalf("post-boundary materialization = %q/%d/%s", reason, providers, raw)
	}
	rejectingRuntime.kill(t)

	closeChildThroughProductionInterrupt(t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID, "evt_close_post_boundary")
	resumeSourceID := "evt_resume_post_boundary"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	result := runClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-closed_post_boundary", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	var pendingSequence int64
	var childStatus, pendingInbox string
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT sequence FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$3)`,
		fixture.sessionID, fixture.childID, pendingRuntimeInputID).Scan(&pendingSequence, &childStatus, &pendingInbox); err != nil {
		t.Fatalf("read rejected post-boundary Resume state: %v", err)
	}
	if result.Result.Type != "error" || !slices.Equal(result.Checkpoint.PendingInputSequences, []int64{pendingSequence}) ||
		result.ProviderRequests != 0 || result.RuntimeEvents != 0 || childStatus != "closed_for_runtime" || pendingInbox != "committed" {
		t.Fatalf("post-boundary Resume = result:%s pending:%v Provider:%d Runtime:%d child:%s Inbox:%s sequence:%d",
			result.Raw, result.Checkpoint.PendingInputSequences, result.ProviderRequests, result.RuntimeEvents, childStatus, pendingInbox, pendingSequence)
	}
}

func TestSubagentOpeningInputCloseBeforeRequestStartCancelsExactCustody(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "close_before_start")
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
		t.Fatalf("opening delivery before close = active:%t err:%v", active, err)
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
	seedCompletionMailSentAt(t, fixture.admin, fixture.sessionID, siblingID, parentID, siblingDeliveryID, 100, "2026-08-22T00:00:00Z")
	siblingRuntimeInputID := completionRuntimeInputID(siblingDeliveryID)
	if _, err := fixture.admin.ExecContext(context.Background(), `UPDATE queue_jobs SET available_at=clock_timestamp()+interval '1 hour'
		WHERE workspace_id='default' AND dedupe_key=$1`,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, fixture.sessionID, siblingRuntimeInputID)); err != nil {
		t.Fatalf("delay sibling control mail: %v", err)
	}
	closeChildThroughProductionInterrupt(
		t, fixture.runtimeDB, fixture.admin, client, fixture.bridgeAddress,
		bridgeAPIScope(fixture.sessionID, parentID, fixture.bindingID, 1, fixture.podUID),
		fixture.sessionID, parentID, fixture.childID, fixture.bindingID, fixture.podUID,
		"evt_close_before_opening_start",
	)

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
		t.Fatal("late Request Start succeeded after CLOSE won opening custody")
	}

	resumeSourceID := "evt_resume_close_before_opening_start"
	seedChildResumeRoute(t, fixture.admin, fixture.sessionID, parentID, resumeSourceID)
	resumedRuntime, resumed := startClosedThreadResumeProductionComposition(t, fixture.runtimeDB, map[string]any{
		"workspaceId": "default", "sessionId": fixture.sessionID, "parentThreadId": parentID,
		"childThreadId": fixture.childID, "childTaskName": "worker-close_before_start", "bindingId": fixture.bindingID,
		"bindingGeneration": 1, "targetPodUid": fixture.podUID, "sourceToolUseEventId": resumeSourceID,
	})
	assertQuiescentClosedThreadResume(t, resumed)
	if len(resumed.ContextEntries) != 0 || len(resumed.TurnFacts.Events) != 0 {
		t.Fatalf("CLOSE-won cold context = entries:%#v facts:%#v; want cancelled opening omitted without fabricated Turn", resumed.ContextEntries, resumed.TurnFacts)
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
	var finalStarts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT sequence FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		 AND source_event_id IN (SELECT jsonb_array_elements_text(event_ids_json::jsonb) FROM session_runtime_inbox WHERE runtime_input_id=$3)),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_start')`,
		fixture.sessionID, fixture.childID, laterRuntimeInputID).Scan(&laterSequence, &finalStarts); err != nil {
		t.Fatalf("read later CLOSE-won execution: %v", err)
	}
	providerWire := string(laterStart.Request)
	if laterStart.ProviderRequests != 1 || laterSequence != 2 || finalStarts != 1 ||
		strings.Contains(providerWire, "execute this input once") || !strings.Contains(providerWire, "execute only the later resumed input") {
		t.Fatalf("later CLOSE-won execution = providers:%d sequence:%d starts:%d wire:%s", laterStart.ProviderRequests, laterSequence, finalStarts, providerWire)
	}
}

func TestSubagentOpeningInputLeaseTakeoverFencesStaleRunner(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "lease_takeover")
	runtimeProcess := startAttachmentRecoveryRuntime(
		t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID,
		fixture.bindingID, 1, fixture.podUID,
	)
	client := dbconnect.NewClientForTesting(fixture.runtimeDB)
	queueStore := queue.NewPostgreSQLStore(client)
	baseQueue := tetralqueue.NewServer(queueStore, nil)
	takeoverQueue := &takeoverAfterLeaseQueueClient{
		QueueClient: baseQueue, store: queueStore, admin: fixture.admin, jobID: fixture.jobID,
	}
	countedSender := &countingAgentMailSender{RuntimeCommandSender: NewRuntimePodCommandClient(attachmentRuntimeTokenSource{})}
	runner := newSubagentRuntimeQueueRunner(
		t, fixture.runtimeDB, fixture.admin, runtimeProcess.port, fixture.sessionID, fixture.podUID,
		takeoverQueue, countedSender,
	)
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("stale runner after lease takeover = active:%t err:%v", active, err)
	}
	runtimeProcess.kill(t)
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
	if countedSender.agentMailCalls != 0 || inboxStatus != "queued" || queueStatus != queue.StatusLeased ||
		currentLease == "" || currentLease == takeoverQueue.staleLeaseToken || attempts != 2 || messages != 0 || starts != 0 {
		t.Fatalf("stale runner custody = Runtime:%d Inbox:%s Queue:%s leaseChanged:%t attempts:%d Messages:%d starts:%d",
			countedSender.agentMailCalls, inboxStatus, queueStatus, currentLease != takeoverQueue.staleLeaseToken, attempts, messages, starts)
	}
}

func TestOrdinaryAgentMailNPlusOneFinalizesWithoutFailingChild(t *testing.T) {
	fixture := newOpeningRuntimeFixture(t, "ordinary_n_plus_one")
	acceptingRuntime := startAttachmentRecoveryRuntime(t, fixture.bridgeAddress, "complete", fixture.sessionID, fixture.childID, fixture.bindingID, 1, fixture.podUID)
	runSubagentRuntimeQueueOnce(t, fixture.runtimeDB, fixture.admin, acceptingRuntime.port, fixture.sessionID, fixture.podUID)
	openingRun := acceptingRuntime.providerStart(t)
	waitForThreadRequestEnds(t, fixture.admin, fixture.sessionID, fixture.childID, 1)
	acceptingRuntime.kill(t)
	if openingRun.ProviderInvocations != 1 {
		t.Fatalf("opening control Provider calls = %d; want 1", openingRun.ProviderInvocations)
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
		Content: "ordinary follow-up that must not acquire opening lineage",
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
	var attempts, starts int
	if err := fixture.admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$3 AND id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND session_thread_id=$4 AND type='span.model_request_start')`,
		runtimeInputID, jobID, fixture.sessionID, fixture.childID).Scan(&inboxStatus, &queueStatus, &attempts, &childStatus, &starts); err != nil {
		t.Fatalf("read ordinary N+1 settlement: %v", err)
	}
	if countedSender.agentMailCalls != 0 || inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || attempts != maxAttempts+1 ||
		childStatus == "failed" || childStatus == "terminated" || childStatus == "closed_for_runtime" || starts != 1 {
		t.Fatalf("ordinary N+1 = Runtime:%d Inbox:%s Queue:%s attempts:%d child:%s starts:%d", countedSender.agentMailCalls, inboxStatus, queueStatus, attempts, childStatus, starts)
	}
}

type openingRuntimeFixture struct {
	runtimeDB, admin              *sql.DB
	bridgeAddress                 string
	sessionID, childID, bindingID string
	podUID, runtimeInputID, jobID string
}

func newOpeningRuntimeFixture(t *testing.T, suffix string) openingRuntimeFixture {
	t.Helper()
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_open_" + suffix
	parentID := "thr_parent_" + suffix
	bindingID := "bind_open_" + suffix
	podUID := "pod_open_" + suffix
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_parent_"+suffix, "evt_parent_"+suffix, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	store.RuntimeBindingTokenHMACKey = []byte("opening-loss-signing-key")
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
		t.Fatalf("read opening loss fixture: %v", err)
	}
	return openingRuntimeFixture{runtimeDB: runtimeDB, admin: admin, bridgeAddress: spawned.BridgeAddress,
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
	controlID := admitted.GetCommitted().GetControlOperationId()
	interruptRuntime, interruptPaths := startInterruptRuntimeComposition(
		t, t.TempDir(), bridgeAddress, sessionID, childID, bindingID, 1, podUID,
	)
	runQueueUntilInterruptSettled(t, runtimeDB, admin, interruptRuntime.port, sessionID, podUID)
	if err := os.WriteFile(interruptPaths.close, []byte("close"), 0o600); err != nil {
		t.Fatalf("release child-close Runtime composition: %v", err)
	}
	interruptRuntime.wait(t)
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

type lostAgentMailResponseSender struct {
	RuntimeCommandSender
	dropped bool
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

func (s *lostAgentMailResponseSender) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	response, err := s.RuntimeCommandSender.AcceptAgentMail(ctx, target, request)
	if err == nil && !s.dropped {
		s.dropped = true
		return nil, status.Error(codes.Unavailable, "fixture transport response lost")
	}
	return response, err
}

type lostAckQueueClient struct {
	QueueClient
	dropped bool
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

type retryObservingQueueClient struct {
	QueueClient
	retryCalls int
}

type takeoverAfterLeaseQueueClient struct {
	QueueClient
	store           *queue.PostgreSQLQueueStore
	admin           *sql.DB
	jobID           string
	staleLeaseToken string
	takenOver       bool
}

func (c *takeoverAfterLeaseQueueClient) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	response, err := c.QueueClient.Lease(ctx, request)
	if err != nil || c.takenOver || len(response.GetJobs()) == 0 {
		return response, err
	}
	c.takenOver = true
	c.staleLeaseToken = response.GetJobs()[0].GetLeaseToken()
	if _, err := c.admin.ExecContext(ctx, `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND lease_token=$2`, c.jobID, c.staleLeaseToken); err != nil {
		return nil, err
	}
	if reclaimed, err := c.store.ReclaimExpiredLeases(ctx, queue.ReclaimExpiredLeasesRequest{WorkspaceID: workspace.DefaultID}); err != nil || reclaimed != 1 {
		return nil, errors.New("fixture failed to reclaim stale Queue lease")
	}
	takeover, err := c.store.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "subagent-custody-takeover",
		MaxJobs: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(takeover) != 1 || takeover[0].ID != c.jobID || takeover[0].LeaseToken == c.staleLeaseToken {
		return nil, errors.New("fixture failed to install replacement Queue lease")
	}
	return response, nil
}

func (c *retryObservingQueueClient) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	c.retryCalls++
	return c.QueueClient.Retry(ctx, request)
}

type countingAgentMailSender struct {
	RuntimeCommandSender
	agentMailCalls int
}

func (s *countingAgentMailSender) AcceptAgentMail(ctx context.Context, target RuntimePodTarget, request *agentruntimev1.AcceptAgentMailRequest) (*agentruntimev1.AcceptAgentMailResponse, error) {
	s.agentMailCalls++
	return s.RuntimeCommandSender.AcceptAgentMail(ctx, target, request)
}

func (c *lostAckQueueClient) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	if !c.dropped {
		c.dropped = true
		return nil, errors.New("fixture Queue ACK response lost before transition")
	}
	return c.QueueClient.Ack(ctx, request)
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

func waitForThreadIdleEvents(t *testing.T, admin *sql.DB, sessionID, threadID string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
			  AND type='session.thread_status_idle' AND payload_json::jsonb #>> '{stop_reason,type}' <> 'closed_for_runtime'`,
			sessionID, threadID).Scan(&count); err == nil && count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Thread %s did not reach %d ordinary idle Events", threadID, want)
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
