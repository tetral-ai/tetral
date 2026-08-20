package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type lostACKSubagentBridge struct {
	bridgev1.AgentRuntimeBridgeServiceServer
	mu          sync.Mutex
	createCalls int
}

func (s *lostACKSubagentBridge) CreateSubagentThread(ctx context.Context, request *bridgev1.CreateSubagentThreadRequest) (*bridgev1.CreateSubagentThreadResponse, error) {
	response, err := s.AgentRuntimeBridgeServiceServer.CreateSubagentThread(ctx, request)
	if err != nil {
		return response, err
	}
	s.mu.Lock()
	s.createCalls++
	call := s.createCalls
	s.mu.Unlock()
	if call == 1 {
		return nil, status.Error(codes.Unavailable, "sub-agent creation acknowledgement unavailable")
	}
	return response, nil
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
}

func runSubagentProductionComposition(
	t *testing.T,
	bridgeServer bridgev1.AgentRuntimeBridgeServiceServer,
	sessionID, threadID, bindingID string,
	bindingGeneration int64,
	podUID, taskName, prompt string,
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
		"bindingId": bindingID, "bindingGeneration": bindingGeneration, "targetPodUid": podUID,
		"taskName": taskName, "prompt": prompt,
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
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run subagent ToolRunner composition: %v: %s", err, output)
	}
	var result subagentProductionCompositionResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode subagent ToolRunner composition: %v: %s", err, output)
	}
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

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	wrapped := &lostACKSubagentBridge{AgentRuntimeBridgeServiceServer: BridgeAPIServer{store: store}}
	result := runSubagentProductionComposition(t, wrapped, sessionID, threadID, bindingID, 1, podUID, taskName, "complete the delegated task")
	if result.ResultType != "observed" || result.ProviderInvocations != 3 || wrapped.calls() != 2 {
		t.Fatalf("subagent ToolRunner result = %+v create calls=%d", result, wrapped.calls())
	}

	var childID, prefixEntries, publicInput, receiptJSON, inboxID string
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
		(SELECT result_json FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT runtime_input_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail')`, sessionID, threadID).
		Scan(&childID, &prefixEntries, &publicInput, &children, &createOperations, &toolUses, &toolResults, &deliveries,
			&openingSent, &openingReceived, &queuedJobs, &reschedules, &receiptJSON, &inboxID); err != nil {
		t.Fatalf("read subagent ToolRunner durable census: %v", err)
	}
	if childID == "" || children != 1 || createOperations != 1 || toolUses != 1 || toolResults != 1 || deliveries != 1 || openingSent != 1 || openingReceived != 1 || queuedJobs != 1 || reschedules != 1 {
		t.Fatalf("subagent durable census child=%s children/ops/uses/results/deliveries/sent/received/queue=%d/%d/%d/%d/%d/%d/%d/%d",
			childID, children, createOperations, toolUses, toolResults, deliveries, openingSent, openingReceived, queuedJobs)
	}
	if !strings.Contains(receiptJSON, childID) || !strings.Contains(receiptJSON, inboxID) || !strings.Contains(receiptJSON, "initial_event_id") {
		t.Fatalf("atomic subagent receipt/input identity = %s/%s", receiptJSON, inboxID)
	}
	if !strings.Contains(prefixEntries, "start a worker") || strings.Contains(prefixEntries, "call_subagent_production") {
		t.Fatalf("subagent immutable prefix = %s", prefixEntries)
	}
	canonicalPublicInput, err := canonicalRunToolJSON(publicInput)
	if err != nil {
		t.Fatalf("canonicalize subagent public provider input: %v", err)
	}
	if canonicalPublicInput != `{"agent_type":"worker","fork_turns":"all","prompt":"complete the delegated task","task_name":"production-worker"}` {
		t.Fatalf("subagent public provider input = %s", publicInput)
	}
}

func startActorProductionBridge(t *testing.T, runtime *sql.DB) bridgev1.AgentRuntimeBridgeServiceClient {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("actor-production-composition-key")
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	RegisterBridgeAPI(server, store)
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

func TestSubagentMailColdLoadCloseAndResumeAcrossGeneratedGRPCAndPostgreSQL(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_actor_production"
		parentID       = "thr_actor_production_parent"
		bindingID      = "bind_actor_production"
		podUID         = "pod_actor_production"
		spawnSourceID  = "evt_actor_production_spawn"
		spawnRequestID = "mreq_actor_production_spawn"
		taskName       = "durable-child"
		mailSourceID   = "evt_actor_production_mail"
		mailContent    = "inspect the target-owned durable envelope"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
		workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,created_at,updated_at
	) VALUES ('default',$1,$2,'msg_actor_production_user',1,'user',
		'{"parts":[{"type":"text","text":"safe parent prefix text"}]}',now(),now())`, sessionID, parentID); err != nil {
		t.Fatalf("seed stable parent prefix: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, spawnSourceID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"`+taskName+`","agent_type":"worker","fork_turns":"all"},"evaluated_permission":"ask"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, spawnRequestID, spawnSourceID); err != nil {
		t.Fatalf("authorize durable spawn source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, spawnRequestID, spawnSourceID, "call_actor_production_spawn", "spawn_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, spawnSourceID)

	client := startActorProductionBridge(t, runtime)
	parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	spawnRequest := &bridgev1.CreateSubagentThreadRequest{
		Scope: parentScope, SourceToolUseEventId: spawnSourceID, TaskName: taskName, AgentType: "worker", ForkTurns: "all", InitialPrompt: "inspect the target-owned durable envelope",
	}
	spawned, err := client.CreateSubagentThread(context.Background(), spawnRequest)
	if err != nil || spawned.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("create subagent through generated gRPC = %#v/%v", spawned, err)
	}
	childID := spawned.GetCommitted().GetChildThreadId()
	var prefixParent, prefixBoundary, prefixEntries string
	if err := admin.QueryRowContext(context.Background(), `SELECT parent_thread_id,parent_boundary_event_id,entries_json
		FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2`, sessionID, childID).
		Scan(&prefixParent, &prefixBoundary, &prefixEntries); err != nil {
		t.Fatalf("read Bridge-selected subagent prefix: %v", err)
	}
	if prefixParent != parentID || prefixBoundary != spawnSourceID || prefixEntries == "[]" {
		t.Fatalf("subagent prefix parent/boundary/entries = %s/%s/%s", prefixParent, prefixBoundary, prefixEntries)
	}
	if strings.Contains(prefixEntries, "call_actor_production_spawn") {
		t.Fatalf("live spawn draft leaked into child prefix: %s", prefixEntries)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json = jsonb_set(data_json::jsonb, '{parts}',
			(data_json::jsonb -> 'parts') || '[{"type":"text","text":"late parent growth"}]'::jsonb)::text,
			updated_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3`,
		sessionID, parentID, spawnRequestID); err != nil {
		t.Fatalf("grow parent Assistant after child creation: %v", err)
	}
	const repeatedSourceID = "evt_actor_production_spawn_repeated"
	const repeatedRequestID = "mreq_actor_production_spawn_repeated"
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, repeatedSourceID, 3, "agent.tool_use",
		`{"type":"agent.tool_use","name":"spawn_agent","input":{"task_name":"`+taskName+`","agent_type":"worker","fork_turns":"all"},"evaluated_permission":"ask"}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
		SET visibility='public',session_visible=true,model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$3`, sessionID, repeatedRequestID, repeatedSourceID); err != nil {
		t.Fatalf("authorize repeated durable spawn source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, repeatedRequestID, repeatedSourceID, "call_actor_production_spawn_repeated", "spawn_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, repeatedSourceID)
	if repeated, err := client.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: parentScope, SourceToolUseEventId: repeatedSourceID, TaskName: taskName, AgentType: "worker", ForkTurns: "all", InitialPrompt: "inspect the target-owned durable envelope",
	}); status.Code(err) != codes.AlreadyExists || repeated != nil {
		t.Fatalf("new Tool identity with repeated task name = %#v/%v; want AlreadyExists", repeated, err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_actor_production_spawn_end", 4, "span.model_request_end",
		`{"type":"span.model_request_end","model_request_id":"`+spawnRequestID+`","is_error":false}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET model_request_id=$2
		WHERE workspace_id='default' AND session_id=$1 AND event_id='evt_actor_production_spawn_end'`, sessionID, spawnRequestID); err != nil {
		t.Fatalf("seal durable spawn request after child creation: %v", err)
	}
	replayed, err := client.CreateSubagentThread(context.Background(), spawnRequest)
	if err != nil || replayed.GetDuplicate().GetChildThreadId() != childID {
		t.Fatalf("replay generated subagent creation = %#v/%v; want %s", replayed, err, childID)
	}
	var replayPrefixEntries string
	var children, operations, createdEvents, prefixes, runtimeInputs, queuedJobs int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT entries_json FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$3 AND role='subagent'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.thread_created' AND payload_json::jsonb->>'role'='subagent'),
		(SELECT count(*) FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1)`,
		sessionID, childID, parentID).Scan(&replayPrefixEntries, &children, &operations, &createdEvents, &prefixes, &runtimeInputs, &queuedJobs); err != nil {
		t.Fatalf("read replayed child creation census: %v", err)
	}
	if replayPrefixEntries != prefixEntries || children != 1 || operations != 1 || createdEvents != 1 || prefixes != 1 || runtimeInputs != 1 || queuedJobs != 1 {
		t.Fatalf("replayed child prefix/census = %s children:%d operations:%d events:%d prefixes:%d inbox:%d queue:%d",
			replayPrefixEntries, children, operations, createdEvents, prefixes, runtimeInputs, queuedJobs)
	}
	childScope := bridgeAPIScope(sessionID, childID, bindingID, 1, podUID)
	firstLoaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: childScope})
	if err != nil {
		t.Fatalf("cold-load child first provider context: %v", err)
	}
	prefixWires := runPendingPrefixProviderWireComposition(t, runRuntimeProviderComposition(t, firstLoaded.GetContextJson()))
	if len(prefixWires) != 3 {
		t.Fatalf("child first-request provider families = %d; want 3", len(prefixWires))
	}
	for _, wire := range prefixWires {
		if wire.Pathname == "" || wire.SafeTextCount != 1 || wire.ToolCallCount != 0 {
			t.Fatalf("child first-request provider wire = %+v", wire)
		}
	}

	seedActorSourceEvent(t, admin, sessionID, parentID, mailSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"send_message","input":{"task_name":"provider-owned-different","message":"provider-owned-different"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, mailSourceID); err != nil {
		t.Fatalf("authorize durable mail source: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, mailSourceID)
	deliveryID := agentMailDeliveryID(mailSourceID, childID)
	delivered, err := client.DeliverInterAgentMail(context.Background(), &bridgev1.DeliverInterAgentMailRequest{
		Scope: parentScope, DeliveryId: deliveryID, TargetThreadId: childID, SourceToolUseEventId: mailSourceID, Content: mailContent,
	})
	if err != nil || delivered.GetCommitted() == nil {
		t.Fatalf("deliver inter-agent mail through generated gRPC = %#v/%v", delivered, err)
	}
	loaded, err := client.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: childScope})
	if err != nil {
		t.Fatalf("cold-load target-owned mail through generated gRPC = %#v/%v", loaded, err)
	}
	if !strings.Contains(loaded.GetContextJson(), deliveryID) || !strings.Contains(loaded.GetContextJson(), mailContent) {
		t.Fatalf("cold target context does not contain durable delivery/content: %s", loaded.GetContextJson())
	}
	assertRuntimeDirectContextComposition(t, loaded.GetContextJson())

	closeSourceID := "evt_actor_production_close"
	seedActorSourceEvent(t, admin, sessionID, parentID, closeSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"close_agent","model_tool_call_id":"call_actor_production_close","input":{"task_name":"provider-owned-different"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true,model_request_id='mreq_actor_production_close',projection_json='{"model_tool_call_id":"call_actor_production_close"}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, closeSourceID); err != nil {
		t.Fatalf("authorize durable close source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, "mreq_actor_production_close", closeSourceID, "call_actor_production_close", "close_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, closeSourceID)
	closeAdmissionRequest := &bridgev1.AdmitChildInterruptRequest{
		Scope: parentScope, SourceToolUseEventId: closeSourceID, TargetChildThreadId: childID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	}
	admitted, err := client.AdmitChildInterrupt(context.Background(), closeAdmissionRequest)
	if err != nil || admitted.GetCommitted().GetControlOperationId() == "" {
		t.Fatalf("admit durable child close control = %#v/%v", admitted, err)
	}
	controlID := admitted.GetCommitted().GetControlOperationId()
	var interruptInputID, interruptEventIDsJSON string
	var interruptSequenceFrom, interruptSequenceTo int64
	interruptInputErr := admin.QueryRowContext(context.Background(), `SELECT runtime_input_id,event_ids_json,sequence_from,sequence_to FROM session_runtime_inbox
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND input_kind='interrupt_control'
		ORDER BY created_at DESC LIMIT 1`, sessionID, childID).Scan(&interruptInputID, &interruptEventIDsJSON, &interruptSequenceFrom, &interruptSequenceTo)
	if interruptInputErr != nil {
		t.Fatalf("read admitted child interrupt input: %v", interruptInputErr)
	}
	var interruptEventIDs []string
	if err := json.Unmarshal([]byte(interruptEventIDsJSON), &interruptEventIDs); err != nil {
		t.Fatalf("decode admitted child interrupt event IDs: %v", err)
	}
	var interruptQueueJob queue.Job
	interruptQueueJob.LeaseToken = "qlt_actor_production_interrupt"
	if err := admin.QueryRowContext(context.Background(), `UPDATE queue_jobs
		SET status='leased',leased_by='actor-production',lease_token=$2,leased_at=clock_timestamp(),
		    leased_until=clock_timestamp()+interval '1 minute',updated_at=clock_timestamp()
		WHERE workspace_id='default' AND kind='runtime_input' AND status='pending'
		  AND payload_json::jsonb->>'input_kind'='interrupt_control'
		  AND dedupe_key=$1
		RETURNING id,partition_key,dedupe_key`,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptInputID), interruptQueueJob.LeaseToken,
	).Scan(&interruptQueueJob.ID, &interruptQueueJob.PartitionKey, &interruptQueueJob.DedupeKey); err != nil {
		t.Fatalf("lease admitted child interrupt Queue authority: %v", err)
	}
	interruptQueueJob.WorkspaceID = workspace.DefaultID
	interruptQueueJob.Kind = queue.KindRuntimeInput
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	deliveryStore.TargetResolver = &recordingRuntimeTargetResolver{binding: runtimeBindingForDelivery{
		BindingID: bindingID, BindingGeneration: 1, Namespace: "engine", PodName: "runtime-actor-production",
		PodUID: podUID, PodIP: "127.0.0.1",
	}}
	plan, prepareErr := deliveryStore.PrepareRuntimeCommand(context.Background(), RuntimeJob{
		JobID: interruptQueueJob.ID, LeaseToken: interruptQueueJob.LeaseToken,
		Kind: queue.KindRuntimeInput, PartitionKey: interruptQueueJob.PartitionKey, DedupeKey: interruptQueueJob.DedupeKey,
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: childID,
		RuntimeInputID: interruptInputID, InputKind: "interrupt_control", EventIDs: interruptEventIDs,
		SequenceFrom: interruptSequenceFrom, SequenceTo: interruptSequenceTo,
	})
	if prepareErr != nil || plan.Interrupt == nil {
		t.Fatalf("prepare admitted child interrupt through production delivery store = %#v/%v", plan, prepareErr)
	}
	committedInterrupt, err := client.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: childScope, RuntimeInputId: interruptInputID, InterruptLeaseRef: bridgeInterruptLeaseRef(&interruptQueueJob),
	})
	if err != nil || committedInterrupt.GetCommitted().GetInterrupt() == nil {
		t.Fatalf("commit child interrupt through generated gRPC = %#v/%v", committedInterrupt, err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: interruptQueueJob.ID, LeaseToken: interruptQueueJob.LeaseToken,
	}); err != nil || !acked {
		t.Fatalf("ACK admitted child interrupt Queue authority = %t/%v", acked, err)
	}
	awaited, err := client.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(awaited.GetCompleted().GetTargets()) != 1 {
		t.Fatalf("await child interrupt through generated gRPC = %#v/%v", awaited, err)
	}
	closed, err := client.CloseChildControl(context.Background(), &bridgev1.CloseChildControlRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(closed.GetCommitted().GetChildren()) != 1 {
		t.Fatalf("close child through admitted operation = %#v/%v", closed, err)
	}
	closeReplay, err := client.CloseChildControl(context.Background(), &bridgev1.CloseChildControlRequest{Scope: parentScope, ControlOperationId: controlID})
	if err != nil || len(closeReplay.GetDuplicate().GetChildren()) != 1 {
		t.Fatalf("lost-ACK child close replay = %#v/%v", closeReplay, err)
	}
	settledClose, err := client.SettleToolResult(context.Background(), &bridgev1.SettleToolResultRequest{
		Scope: parentScope, Settlement: bridgeCompletedToolSettlementForTest(closeSourceID, "child closed"),
	})
	if err != nil || settledClose.GetCommitted() == nil {
		t.Fatalf("settle child close source through dedicated Tool result = %#v/%v", settledClose, err)
	}
	childAdmissionReplay, err := client.AdmitChildInterrupt(context.Background(), closeAdmissionRequest)
	if err != nil || childAdmissionReplay.GetDuplicate().GetControlOperationId() != controlID {
		t.Fatalf("lost-ACK child admission replay after route settlement = %#v/%v", childAdmissionReplay, err)
	}

	resumeSourceID := "evt_actor_production_resume"
	seedActorSourceEvent(t, admin, sessionID, parentID, resumeSourceID, "agent.tool_use",
		`{"type":"agent.tool_use","name":"resume_agent","model_tool_call_id":"call_actor_production_resume","input":{"task_name":"provider-owned-different"}}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET visibility='public',session_visible=true,model_request_id='mreq_actor_production_resume',projection_json='{"model_tool_call_id":"call_actor_production_resume"}'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, resumeSourceID); err != nil {
		t.Fatalf("authorize durable resume source: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, parentID, "mreq_actor_production_resume", resumeSourceID, "call_actor_production_resume", "resume_agent")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, parentID, resumeSourceID)
	resumeRequest := &bridgev1.MarkChildThreadActiveRequest{
		Scope: parentScope, SourceToolUseEventId: resumeSourceID, TargetChildThreadId: childID,
	}
	resumed, err := client.MarkChildThreadActive(context.Background(), resumeRequest)
	if err != nil || resumed.GetCommitted() == nil {
		t.Fatalf("resume child through generated gRPC = %#v/%v", resumed, err)
	}
	var finalStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&finalStatus); err != nil {
		t.Fatalf("read resumed child: %v", err)
	}
	if finalStatus != "idle" {
		t.Fatalf("resumed child status = %s; want idle", finalStatus)
	}
	settledResume, err := client.SettleToolResult(context.Background(), &bridgev1.SettleToolResultRequest{
		Scope: parentScope, Settlement: bridgeCompletedToolSettlementForTest(resumeSourceID, "child resumed"),
	})
	if err != nil || settledResume.GetCommitted() == nil {
		t.Fatalf("settle child resume source through dedicated Tool result = %#v/%v", settledResume, err)
	}
	resumeReplay, err := client.MarkChildThreadActive(context.Background(), resumeRequest)
	if err != nil || resumeReplay.GetDuplicate().GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
		t.Fatalf("lost-ACK child resume replay after route settlement = %#v/%v", resumeReplay, err)
	}
}

type pendingPrefixProviderWireComposition struct {
	Family        string `json:"family"`
	Pathname      string `json:"pathname"`
	SafeTextCount int    `json:"safeTextCount"`
	ToolCallCount int    `json:"toolCallCount"`
}

func runPendingPrefixProviderWireComposition(t *testing.T, requests []json.RawMessage) []pendingPrefixProviderWireComposition {
	t.Helper()
	input, err := json.Marshal(map[string]any{"requests": requests})
	if err != nil {
		t.Fatalf("encode pending-prefix provider requests: %v", err)
	}
	inputPath := t.TempDir() + "/pending-prefix-provider-requests.json"
	if err := os.WriteFile(inputPath, input, 0o600); err != nil {
		t.Fatalf("write pending-prefix provider requests: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "packages/provider-gateway/test/fixtures/pending-prefix-wire.ts", inputPath) //nolint:gosec // Fixed provider composition fixture and test-owned input.
	command.Dir = "../gateway"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run pending-prefix provider wire composition: %v: %s", err, output)
	}
	var result []pendingPrefixProviderWireComposition
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode pending-prefix provider wire composition: %v: %s", err, output)
	}
	return result
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
	command.Dir = "../agent-runtime"
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compose generated LoadContext through Runtime parser/reducer: %v: %s", err, output)
	}
	var composed struct {
		Checkpoint    json.RawMessage `json:"checkpoint"`
		ReducerAction struct {
			Action string `json:"action"`
		} `json:"reducerAction"`
		ProviderComposition struct {
			CarrierMessages []json.RawMessage `json:"carrierMessages"`
			Strategies      []struct {
				Validation struct {
					OK bool `json:"ok"`
				} `json:"validation"`
			} `json:"strategies"`
		} `json:"providerComposition"`
	}
	if err := json.Unmarshal(output, &composed); err != nil || len(composed.Checkpoint) == 0 || composed.ReducerAction.Action == "" {
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

func TestReviewerTrunkSuccessionAndSidecarReplayAcrossGeneratedGRPCAndPostgreSQL(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_production"
		parentID  = "thr_reviewer_production_parent"
		bindingID = "bind_reviewer_production"
		podUID    = "pod_reviewer_production"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	client := startActorProductionBridge(t, runtime)
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)

	firstRequest := &bridgev1.EnsureApprovalReviewerTrunkRequest{Scope: scope, EnsureOperationId: "ensure_reviewer_manager_1"}
	first, err := client.EnsureApprovalReviewerTrunk(context.Background(), firstRequest)
	if err != nil || first.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure first reviewer trunk = %#v/%v", first, err)
	}
	firstID := first.GetCommitted().GetReviewerThreadId()
	firstReplay, err := client.EnsureApprovalReviewerTrunk(context.Background(), firstRequest)
	if err != nil || firstReplay.GetDuplicate().GetReviewerThreadId() != firstID {
		t.Fatalf("replay first reviewer trunk = %#v/%v", firstReplay, err)
	}
	second, err := client.EnsureApprovalReviewerTrunk(context.Background(), &bridgev1.EnsureApprovalReviewerTrunkRequest{
		Scope: scope, EnsureOperationId: "ensure_reviewer_manager_2",
	})
	if err != nil || second.GetCommitted().GetReviewerThreadId() == "" || second.GetCommitted().GetReviewerThreadId() == firstID {
		t.Fatalf("new-manager reviewer trunk succession = %#v/%v", second, err)
	}
	secondID := second.GetCommitted().GetReviewerThreadId()
	var firstTrunk, secondTrunk bool
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT is_trunk FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT is_trunk FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3)`, sessionID, firstID, secondID).
		Scan(&firstTrunk, &secondTrunk); err != nil {
		t.Fatalf("read reviewer trunk succession: %v", err)
	}
	if firstTrunk || !secondTrunk {
		t.Fatalf("reviewer trunk flags old/new = %t/%t; want false/true", firstTrunk, secondTrunk)
	}

	sidecarRequest := &bridgev1.EnsureApprovalReviewerSidecarRequest{Scope: scope, ReviewId: "review_production_1"}
	sidecar, err := client.EnsureApprovalReviewerSidecar(context.Background(), sidecarRequest)
	if err != nil || sidecar.GetCommitted().GetReviewerThreadId() == "" {
		t.Fatalf("ensure review-keyed sidecar = %#v/%v", sidecar, err)
	}
	sidecarID := sidecar.GetCommitted().GetReviewerThreadId()
	sidecarReplay, err := client.EnsureApprovalReviewerSidecar(context.Background(), sidecarRequest)
	if err != nil || sidecarReplay.GetDuplicate().GetReviewerThreadId() != sidecarID {
		t.Fatalf("replay review-keyed sidecar = %#v/%v", sidecarReplay, err)
	}
	var prefixSource string
	if err := admin.QueryRowContext(context.Background(), `SELECT parent_thread_id FROM session_thread_context_prefixes
		WHERE workspace_id='default' AND session_id=$1 AND child_thread_id=$2`, sessionID, sidecarID).Scan(&prefixSource); err != nil {
		t.Fatalf("read sidecar durable prefix source: %v", err)
	}
	if prefixSource != secondID {
		t.Fatalf("sidecar prefix source = %s; want current trunk %s", prefixSource, secondID)
	}
	admitted, err := client.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || admitted.GetCommitted().GetRuntimeInputId() == "" {
		t.Fatalf("admit review-keyed sidecar input = %#v/%v", admitted, err)
	}
	reviewInputID := admitted.GetCommitted().GetRuntimeInputId()
	admissionReplay, err := client.AdmitApprovalReviewInput(context.Background(), &bridgev1.AdmitApprovalReviewInputRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || admissionReplay.GetDuplicate().GetRuntimeInputId() != reviewInputID {
		t.Fatalf("replay review-keyed input admission = %#v/%v", admissionReplay, err)
	}
	reviewerScope := bridgeAPIScope(sessionID, sidecarID, bindingID, 1, podUID)
	committedInput, err := client.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: reviewerScope, RuntimeInputId: reviewInputID,
		ApprovalReviewText: []string{"review the bounded approval evidence"},
	})
	if err != nil || committedInput.GetCommitted().GetContext() == nil {
		t.Fatalf("commit approval reviewer input through generated gRPC = %#v/%v", committedInput, err)
	}
	seedActorSourceEvent(t, admin, sessionID, sidecarID, "evt_reviewer_production_decision", "approval_review.decision",
		`{"type":"approval_review.decision","review_id":"`+sidecarRequest.GetReviewId()+`","decision":"approved"}`)
	closed, err := client.CloseApprovalReviewer(context.Background(), &bridgev1.CloseApprovalReviewerRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || closed.GetCommitted() == nil {
		t.Fatalf("close reviewer sidecar through generated gRPC = %#v/%v", closed, err)
	}
	closeReplay, err := client.CloseApprovalReviewer(context.Background(), &bridgev1.CloseApprovalReviewerRequest{
		Scope: scope, ReviewerThreadId: sidecarID, ReviewId: sidecarRequest.GetReviewId(),
	})
	if err != nil || closeReplay.GetDuplicate() == nil {
		t.Fatalf("lost-ACK reviewer close replay = %#v/%v", closeReplay, err)
	}

	var operationCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_bridge_operations
		WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread'`, sessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count reviewer ensure operations: %v", err)
	}
	if operationCount != 3 {
		t.Fatalf("durable reviewer ensure operation count = %d; want two trunks plus one sidecar", operationCount)
	}
}
