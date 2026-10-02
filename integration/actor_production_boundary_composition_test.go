package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type lostACKSubagentBridge struct {
	bridgev1.AgentRuntimeBridgeServiceServer
	mu          sync.Mutex
	createCalls int
	requests    []*bridgev1.CreateSubagentThreadRequest
}

func (s *lostACKSubagentBridge) CreateSubagentThread(ctx context.Context, request *bridgev1.CreateSubagentThreadRequest) (*bridgev1.CreateSubagentThreadResponse, error) {
	response, err := s.AgentRuntimeBridgeServiceServer.CreateSubagentThread(ctx, request)
	if err != nil {
		return response, err
	}
	s.mu.Lock()
	s.createCalls++
	s.requests = append(s.requests, proto.Clone(request).(*bridgev1.CreateSubagentThreadRequest))
	call := s.createCalls
	s.mu.Unlock()
	if call == 1 {
		return nil, status.Error(codes.Unavailable, "sub-agent creation acknowledgement unavailable")
	}
	return response, nil
}

func (s *lostACKSubagentBridge) capturedRequests() []*bridgev1.CreateSubagentThreadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*bridgev1.CreateSubagentThreadRequest(nil), s.requests...)
}

func (s *lostACKSubagentBridge) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createCalls
}

type subagentProductionCompositionResult struct {
	ResultType          string            `json:"resultType"`
	ProviderInvocations int               `json:"providerInvocations"`
	ProviderContexts    []json.RawMessage `json:"providerContexts"`
	BridgeAddress       string            `json:"-"`
}

func contextEntrySequences(entries []bridgeRuntimeContextEntry) []int64 {
	sequences := make([]int64, 0, len(entries))
	for _, entry := range entries {
		sequences = append(sequences, entry.MessageSequence)
	}
	return sequences
}

func runSubagentProductionComposition(
	t *testing.T,
	bridgeServer bridgev1.AgentRuntimeBridgeServiceServer,
	sessionID, threadID, bindingID string,
	bindingGeneration int64,
	podUID, taskName, prompt, forkTurns string,
) subagentProductionCompositionResult {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for subagent ToolRunner composition: %v", err)
	}
	server := grpc.NewServer()
	bridgev1.RegisterAgentRuntimeBridgeServiceServer(server, bridgeServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": listener.Addr().String(), "workspaceId": "default",
		"sessionId": sessionID, "sessionThreadId": threadID,
		"bindingId": bindingID, "bindingGeneration": bindingGeneration, "targetPodUid": podUID, "runtimeProcessId": "process_" + podUID,
		"taskName": taskName, "prompt": prompt, "forkTurns": forkTurns,
	})
	if err != nil {
		t.Fatalf("encode subagent ToolRunner composition: %v", err)
	}
	inputPath := t.TempDir() + "/subagent-production.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write subagent ToolRunner composition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/subagent-production-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../services/agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run subagent ToolRunner composition: %v: %s", err, output)
	}
	var result subagentProductionCompositionResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode subagent ToolRunner composition: %v: %s", err, output)
	}
	result.BridgeAddress = listener.Addr().String()
	return result
}

func TestPostgreSQLThreadLoopToolRunnerCreatesOneAuthorizedSubagentAfterLostACK(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_subagent_toolrunner"
		threadID  = "thr_subagent_toolrunner"
		bindingID = "bind_subagent_toolrunner"
		podUID    = "pod_subagent_toolrunner"
		taskName  = "production-worker"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_subagent_toolrunner_user", "evt_subagent_toolrunner_user", 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json='{"parts":[{"type":"text","text":"start a worker"}]}'
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND sequence=1`, sessionID, threadID); err != nil {
		t.Fatalf("seed subagent ToolRunner user context: %v", err)
	}

	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("subagent-toolrunner-composition-signing-key")
	wrapped := &lostACKSubagentBridge{AgentRuntimeBridgeServiceServer: agentruntimebridge.NewBridgeAPIServer(store)}
	result := runSubagentProductionComposition(t, wrapped, sessionID, threadID, bindingID, 1, podUID, taskName, "complete the delegated task", "all")
	if result.ResultType != "observed" || result.ProviderInvocations != 3 || wrapped.calls() != 2 {
		t.Fatalf("subagent ToolRunner result = %+v create calls=%d", result, wrapped.calls())
	}
	requests := wrapped.capturedRequests()
	if len(requests) != 2 || !proto.Equal(requests[0], requests[1]) {
		t.Fatalf("lost-ack subagent declarations were not exact replay: %#v", requests)
	}
	declaration := requests[0]
	if declaration.GetTaskName() != taskName || declaration.GetAgentType() != "worker" ||
		declaration.GetInitialPrompt() != "complete the delegated task" ||
		len(declaration.GetParentMessageSequences()) != 1 || declaration.GetParentMessageSequences()[0] != 1 {
		t.Fatalf("Runtime-selected private subagent declaration = %#v", declaration)
	}

	var childID, prefixEntries, publicInput, receiptJSON string
	var children, createOperations, toolUses, toolResults, deliveries, openingSent, openingReceived, queuedJobs, reschedules int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT id FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT entries_json FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=(SELECT id FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent')),
		(SELECT payload_json::jsonb->'input' FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use' AND payload_json::jsonb->>'name'='spawn_agent'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use' AND payload_json::jsonb->>'name'='spawn_agent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'input_kind'='agent_mail'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_rescheduled'),
		(SELECT result_json FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn')`, sessionID, threadID).
		Scan(&childID, &prefixEntries, &publicInput, &children, &createOperations, &toolUses, &toolResults, &deliveries,
			&openingSent, &openingReceived, &queuedJobs, &reschedules, &receiptJSON); err != nil {
		t.Fatalf("read subagent ToolRunner durable census: %v", err)
	}
	if childID == "" || children != 1 || createOperations != 1 || toolUses != 1 || toolResults != 1 || deliveries != 1 || openingSent != 1 || openingReceived != 1 || queuedJobs != 1 || reschedules != 1 {
		t.Fatalf("subagent durable census child=%s children/ops/uses/results/deliveries/sent/received/queue=%d/%d/%d/%d/%d/%d/%d/%d",
			childID, children, createOperations, toolUses, toolResults, deliveries, openingSent, openingReceived, queuedJobs)
	}
	if receiptJSON != `{"child_thread_id":"`+childID+`"}` {
		t.Fatalf("minimal subagent replay receipt = %s", receiptJSON)
	}
	if !strings.Contains(prefixEntries, "start a worker") || strings.Contains(prefixEntries, "call_subagent_production") {
		t.Fatalf("subagent immutable prefix = %s", prefixEntries)
	}
	var persistedPrefix []bridgeRuntimeContextEntry
	if err := json.Unmarshal([]byte(prefixEntries), &persistedPrefix); err != nil {
		t.Fatalf("decode persisted subagent prefix: %v", err)
	}
	if !slices.Equal(contextEntrySequences(persistedPrefix), []int64{1}) {
		t.Fatalf("persisted subagent prefix sequences = %v; want [1]", contextEntrySequences(persistedPrefix))
	}
	canonicalPublicInput, err := runtimecontrol.CanonicalRunToolJSON(publicInput)
	if err != nil {
		t.Fatalf("canonicalize subagent public provider input: %v", err)
	}
	if canonicalPublicInput != `{"agent_type":"worker","fork_turns":"all","prompt":"complete the delegated task","task_name":"production-worker"}` {
		t.Fatalf("subagent public provider input = %s", publicInput)
	}

	coldRuntime := startAttachmentRecoveryRuntime(t, result.BridgeAddress, "complete", sessionID, childID, bindingID, 1, podUID)
	deliverAttachmentRuntimeInput(t, runtime, admin, coldRuntime.port, sessionID, "runtime-pod-0", podUID)
	providerStart := coldRuntime.providerStart(t)
	providerWire := string(providerStart.ProviderRequest)
	prefixOffset := strings.Index(providerWire, "start a worker")
	openingOffset := strings.Index(providerWire, "complete the delegated task")
	if providerStart.ProviderInvocations != 1 || providerStart.GatewayRequests != 1 || prefixOffset < 0 || openingOffset <= prefixOffset {
		t.Fatalf("cold child Provider request = invocations:%d gateway:%d prefix/opening:%d/%d wire:%s",
			providerStart.ProviderInvocations, providerStart.GatewayRequests, prefixOffset, openingOffset, providerWire)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var childEnds int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
			WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'`, sessionID, childID).Scan(&childEnds); err == nil && childEnds == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	coldRuntime.kill(t)
	var childEnds int
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end'),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND input_kind='agent_mail'),
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'session_thread_id'=$2)`, sessionID, childID).
		Scan(&childEnds, &inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read cold child delivery settlement: %v", err)
	}
	if childEnds != 1 || inboxStatus != "accepted" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("cold child settlement ends/Inbox/Queue = %d/%s/%s; want 1/accepted/acknowledged", childEnds, inboxStatus, queueStatus)
	}
}

func TestPostgreSQLChildControlExhaustionRejoinsParentToolResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_child_control_rejoin"
		parentID  = "thr_child_control_parent"
		bindingID = "bind_child_control_rejoin"
		podUID    = "pod_child_control_rejoin"
		taskName  = "child-control-target"
		spawnID   = "evt_child_control_spawn"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("child-control-rejoin-key")
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, parentID, "msg_child_control_prefix", "evt_child_control_prefix", 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, spawnID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"`+taskName+`","agent_type":"worker","fork_turns":"all"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true,model_request_id='mreq_child_control_spawn'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, spawnID); err != nil {
		t.Fatalf("authorize child control spawn source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, "mreq_child_control_spawn", spawnID, "call_child_control_spawn", "spawn_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, spawnID)
	created, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID), SourceToolUseEventId: spawnID,
		TaskName: taskName, AgentType: "worker", InitialPrompt: "perform child work", ParentMessageSequences: []int64{1},
	})
	if err != nil || created.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("create child control target = %#v/%v", created, err)
	}
	childID := created.GetCommitted().GetChildThreadId()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='running'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
		t.Fatalf("activate child control target: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for child control composition: %v", err)
	}
	server := grpc.NewServer()
	agentruntimebridge.RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	input, err := json.Marshal(map[string]any{
		"bridgeAddress": listener.Addr().String(), "workspaceId": "default", "sessionId": sessionID,
		"sessionThreadId": parentID, "bindingId": bindingID, "bindingGeneration": 1,
		"targetPodUid": podUID, "runtimeProcessId": "process_" + podUID, "taskName": taskName,
	})
	if err != nil {
		t.Fatalf("encode child control composition: %v", err)
	}
	inputPath := t.TempDir() + "/child-control.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write child control composition: %v", err)
	}
	var output bytes.Buffer
	command := exec.Command("bun", "packages/runtime-pod/test/fixtures/child-control-exhaustion-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../services/agent-runtime"
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatalf("start child control composition: %v", err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})

	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	deadline := time.Now().Add(10 * time.Second)
	for {
		var jobs int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM queue_jobs
			WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1
			  AND payload_json::jsonb->>'session_thread_id'=$2
			  AND payload_json::jsonb->>'input_kind'='interrupt_control'`, sessionID, childID).Scan(&jobs); err == nil && jobs == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Runtime did not create child interrupt custody: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET attempt_count=max_attempts,available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1
		  AND payload_json::jsonb->>'session_thread_id'=$2
		  AND payload_json::jsonb->>'input_kind'='interrupt_control'`, sessionID, childID); err != nil {
		t.Fatalf("advance child control to final delivery owner: %v", err)
	}
	deliverer := &postgresFinalizingDeliverer{store: jobrunner.NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)}
	runner := &jobrunner.JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: jobrunner.JobRunnerConfig{LeaseOwner: "child-control-rejoin", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run child control final owner: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("run child control composition: %v: %s", err, output.String())
	}
	var composed struct {
		ProviderInvocations int               `json:"providerInvocations"`
		ProviderContexts    []json.RawMessage `json:"providerContexts"`
	}
	if err := json.Unmarshal(output.Bytes(), &composed); err != nil {
		t.Fatalf("decode child control composition: %v: %s", err, output.String())
	}
	var childStatus string
	var toolUses, toolResults, parentEnds, completionMails int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='agent.tool_use' AND payload_json::jsonb->>'name'='interrupt_agent'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='agent.tool_result'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='span.model_request_end'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.thread_message_sent')`,
		sessionID, childID, parentID).Scan(&childStatus, &toolUses, &toolResults, &parentEnds, &completionMails); err != nil {
		t.Fatalf("read child control durable rejoin: %v", err)
	}
	if composed.ProviderInvocations != 2 || childStatus != "failed" || toolUses != 1 || toolResults != 1 || parentEnds != 2 || completionMails != 1 || deliverer.deliveries != 0 {
		t.Fatalf("child control rejoin = providers:%d child:%s uses/results/ends/mail/runtime:%d/%d/%d/%d/%d contexts:%s",
			composed.ProviderInvocations, childStatus, toolUses, toolResults, parentEnds, completionMails, deliverer.deliveries, output.String())
	}
}

func startActorProductionBridge(t *testing.T, runtime *sql.DB) bridgev1.AgentRuntimeBridgeServiceClient {
	t.Helper()
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("actor-production-composition-key")
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	agentruntimebridge.RegisterBridgeAPI(server, store)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///actor-production-composition",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial generated actor Bridge client: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return bridgev1.NewAgentRuntimeBridgeServiceClient(connection)
}

func assertRuntimeDirectContextComposition(t *testing.T, contextJSON string) {
	t.Helper()
	var direct map[string]json.RawMessage
	if err := json.Unmarshal([]byte(contextJSON), &direct); err != nil {
		t.Fatalf("decode generated LoadContext direct facts: %v", err)
	}
	if _, ok := direct["contextEntries"]; !ok {
		t.Fatalf("generated LoadContext omitted contextEntries: %s", contextJSON)
	}
	input, err := json.Marshal(map[string]any{"contextJson": contextJSON, "providerComposition": true})
	if err != nil {
		t.Fatalf("encode Runtime direct-context composition input: %v", err)
	}
	inputPath := t.TempDir() + "/runtime-direct-context.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write Runtime direct-context composition input: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/runtime-pod/test/fixtures/cold-checkpoint-composition.ts", inputPath) //nolint:gosec // Fixed repository fixture and test-owned input.
	command.Dir = "../services/agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compose generated LoadContext through Runtime parser/reducer: %v: %s", err, output)
	}
	var composed struct {
		Checkpoint json.RawMessage `json:"checkpoint"`
		NextStep   struct {
			Action string `json:"action"`
		} `json:"nextStep"`
		ProviderComposition struct {
			CarrierMessages []json.RawMessage `json:"carrierMessages"`
			Strategies      []struct {
				Validation struct {
					OK bool `json:"ok"`
				} `json:"validation"`
			} `json:"strategies"`
		} `json:"providerComposition"`
	}
	if err := json.Unmarshal(output, &composed); err != nil || len(composed.Checkpoint) == 0 || composed.NextStep.Action == "" {
		t.Fatalf("Runtime direct-context composition = %s err:%v", output, err)
	}
	if len(composed.ProviderComposition.Strategies) == 0 {
		t.Fatalf("Runtime provider composition omitted strategies: %s", output)
	}
	for _, strategy := range composed.ProviderComposition.Strategies {
		if !strategy.Validation.OK {
			t.Fatalf("Runtime provider composition rejected a strategy: %s", output)
		}
	}
}

func seedActorSourceEvent(t *testing.T, db *sql.DB, sessionID, threadID, eventID, eventType, payload string) {
	t.Helper()
	var sequence int64
	if err := db.QueryRowContext(context.Background(), `SELECT COALESCE(MAX(sequence),0)+1 FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&sequence); err != nil {
		t.Fatalf("allocate actor source event sequence: %v", err)
	}
	seedBridgeAPIEvent(t, db, "default", sessionID, threadID, eventID, sequence, eventType, payload)
}
