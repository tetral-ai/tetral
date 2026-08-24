package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLSessionInterruptBarrierDefersExactInflightCustody(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID        = "sesn_interrupt_barrier_inflight"
		threadID         = "thr_interrupt_barrier_inflight"
		inputID          = "rin_interrupt_barrier_inflight"
		eventID          = "evt_interrupt_barrier_inflight"
		interruptID      = "rin_interrupt_barrier_inflight_control"
		interruptEventID = "evt_interrupt_barrier_inflight_control"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 2, "user.message", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEventID, 3, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control", EventIDs: []string{interruptEventID}, SequenceFrom: 3, SequenceTo: 3})
	job := RuntimeJob{WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID, RuntimeInputID: inputID,
		InputKind: "messages", EventIDs: []string{eventID}, SequenceFrom: 2, SequenceTo: 2}
	seedRuntimeInboxBirthForJob(t, admin, job)
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{eventID}, "sequence_from": 2, "sequence_to": 2, "input_kind": "messages",
	})
	if err != nil {
		t.Fatalf("marshal runtime input: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	created, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue runtime input: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{WorkspaceID: workspace.DefaultID,
		Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "barrier-stale-owner", MaxJobs: 1, LeaseDuration: time.Minute, Now: now})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease runtime input = %#v/%v", leased, err)
	}
	lease := leased[0]
	job.JobID, job.LeaseToken, job.Kind, job.PartitionKey, job.DedupeKey = lease.ID, lease.LeaseToken, lease.Kind, lease.PartitionKey, lease.DedupeKey
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='delivering',
		binding_id='bind_barrier_deferred',binding_generation=1,target_pod_uid='pod_barrier_deferred'
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inputID); err != nil {
		t.Fatalf("bind input before barrier-stale response: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='committed',
		binding_id='bind_released_interrupt',binding_generation=1,target_pod_uid='pod_released_interrupt',committed_at=clock_timestamp()
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, interruptID); err != nil {
		t.Fatalf("commit interrupt Inbox before stale response finalization: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_bridge_operations (
			workspace_id,session_id,session_thread_id,operation,source_kind,idempotency_key,
			request_hash,ack_status,result_json,declaration_digest,receipt_json,created_at,updated_at
		) VALUES ('default',$1,$3,'commit_inputs','interrupt_control',$2,'released','committed','{}','released','{}',clock_timestamp(),clock_timestamp())`,
		sessionID, interruptID, threadID); err != nil {
		t.Fatalf("release interrupt barrier before stale response finalization: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	result, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale})
	if err != nil || result.Status != RuntimeDeliveryBarrierStale || !result.QueueLeaseSettled {
		t.Fatalf("finalize barrier stale = %#v/%v", result, err)
	}
	var queueStatus, inboxStatus string
	var attempts, messages, processed int
	var bindingID, leaseToken sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2 AND runtime_input_id=$3),
		(SELECT attempt_count FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT binding_id FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$2 AND runtime_input_id=$3),
		(SELECT lease_token FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND event_id=$4 AND processed_at IS NOT NULL)`,
		created.ID, sessionID, inputID, eventID).Scan(&queueStatus, &inboxStatus, &attempts, &bindingID, &leaseToken, &messages, &processed); err != nil {
		t.Fatalf("read barrier-stale custody: %v", err)
	}
	if queueStatus != queue.StatusPending || inboxStatus != "queued" || attempts != 0 || bindingID.Valid || leaseToken.Valid || messages != 0 || processed != 0 {
		t.Fatalf("barrier-stale custody = %s/%s attempts=%d binding=%v lease=%v messages=%d processed=%d",
			queueStatus, inboxStatus, attempts, bindingID, leaseToken, messages, processed)
	}
	replay, err := store.FinalizeRuntimeDelivery(context.Background(), job, RuntimeDeliveryResult{Status: RuntimeDeliveryBarrierStale})
	if err != nil || replay.Status != RuntimeDeliveryAuthorityLost || replay.QueueLeaseSettled {
		t.Fatalf("stale lease replay = %#v/%v; want authority loss without mutation", replay, err)
	}
}

func TestPostgreSQLSupersededInterruptSettlesItsExactQueueLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_superseded_interrupt_lease"
		threadID  = "thr_superseded_interrupt_lease"
		activeID  = "rin_superseded_interrupt_active"
		staleID   = "rin_superseded_interrupt_stale"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_superseded_interrupt_active", 1, "user.interrupt", `{}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_superseded_interrupt_stale", 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: activeID, InputKind: "interrupt_control", EventIDs: []string{"evt_superseded_interrupt_active"}, SequenceFrom: 1, SequenceTo: 1,
	})
	staleJob := RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: staleID, InputKind: "interrupt_control", EventIDs: []string{"evt_superseded_interrupt_stale"}, SequenceFrom: 2, SequenceTo: 2,
	}
	seedRuntimeInboxBirthForJob(t, admin, staleJob)
	activePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": activeID, "event_ids": []string{"evt_superseded_interrupt_active"},
		"sequence_from": 1, "sequence_to": 1, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal active interrupt payload: %v", err)
	}
	stalePayload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": staleID, "event_ids": []string{"evt_superseded_interrupt_stale"},
		"sequence_from": 2, "sequence_to": 2, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal stale interrupt payload: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Now().UTC()
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, activeID),
		PayloadVersion: 1, PayloadJSON: activePayload, Priority: 100, Now: now,
	}); err != nil {
		t.Fatalf("enqueue active interrupt: %v", err)
	}
	created, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, staleID),
		PayloadVersion: 1, PayloadJSON: stalePayload, Priority: 100, Now: now.Add(time.Microsecond),
	})
	if err != nil {
		t.Fatalf("enqueue stale interrupt: %v", err)
	}
	leaseToken, err := queue.NewLeaseToken()
	if err != nil {
		t.Fatalf("create superseded interrupt lease token: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs
		SET status='leased', leased_by='stale-interrupt-owner', lease_token=$2,
		    leased_at=clock_timestamp(), leased_until=clock_timestamp()+interval '1 minute', updated_at=clock_timestamp()
		WHERE workspace_id='default' AND id=$1`, created.ID, leaseToken); err != nil {
		t.Fatalf("install superseded interrupt lease: %v", err)
	}
	staleJob.JobID, staleJob.LeaseToken, staleJob.Kind = created.ID, leaseToken, created.Kind
	staleJob.PartitionKey, staleJob.DedupeKey = created.PartitionKey, created.DedupeKey
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	plan, err := store.PrepareRuntimeCommand(context.Background(), staleJob)
	if err != nil || !plan.StaleAccepted || !plan.QueueLeaseSettled {
		t.Fatalf("superseded interrupt plan = %#v/%v; want settled stale", plan, err)
	}
	var queueStatus, inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		created.ID, staleID).Scan(&queueStatus, &inboxStatus); err != nil {
		t.Fatalf("read stale interrupt custody: %v", err)
	}
	if queueStatus != queue.StatusCancelled || inboxStatus != "cancelled" {
		t.Fatalf("superseded interrupt custody = %s/%s; want cancelled/cancelled", queueStatus, inboxStatus)
	}
}

func TestPostgreSQLSessionInterruptBarrierRejectsLateMessageCommit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID   = "sesn_interrupt_barrier_message"
		threadID    = "thr_interrupt_barrier_message"
		bindingID   = "bind_interrupt_barrier_message"
		podUID      = "pod_interrupt_barrier_message"
		messageID   = "rin_interrupt_barrier_message"
		interruptID = "rin_interrupt_barrier_control"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIRuntimeInput(t, admin, "default", sessionID, threadID, messageID, bindingID, podUID, "evt_interrupt_barrier_message")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_barrier_control", 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_control"}, SequenceFrom: 2, SequenceTo: 2,
	})

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: messageID,
	})
	if err != nil || response.GetBarrierStale() == nil {
		t.Fatalf("late CommitInputs = %#v/%v; want barrier stale", response, err)
	}
	var messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages
		WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, threadID).Scan(&messages); err != nil {
		t.Fatalf("count late message projections: %v", err)
	}
	if messages != 0 {
		t.Fatalf("late message projections = %d; want 0", messages)
	}
}

func TestPostgreSQLSessionInterruptBarrierMakesLateToolSettlementStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_barrier_tool"
		threadID       = "thr_interrupt_barrier_tool"
		bindingID      = "bind_interrupt_barrier_tool"
		podUID         = "pod_interrupt_barrier_tool"
		requestID      = "mreq_interrupt_barrier_tool"
		toolUseID      = "evt_interrupt_barrier_tool_use"
		interruptID    = "rin_interrupt_barrier_tool_control"
		interruptEvent = "evt_interrupt_barrier_tool_control"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{},"evaluated_permission":"allow"}`)
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, requestID, toolUseID, "call_interrupt_barrier_tool", "exec_command")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{interruptEvent}, SequenceFrom: 2, SequenceTo: 2,
	})

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.SettleToolResult(context.Background(), bridgeToolSettlementRequestForTest(
		bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		&bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUseID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{}},
		},
	))
	if err != nil || response.GetStale() == nil {
		t.Fatalf("late Tool settlement = %#v/%v; want barrier stale", response, err)
	}
	var results int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type IN ('agent.tool_result','agent.mcp_tool_result')`, sessionID).Scan(&results); err != nil {
		t.Fatalf("count late Tool results: %v", err)
	}
	if results != 0 {
		t.Fatalf("late Tool results = %d; want 0", results)
	}
}

func TestPostgreSQLSessionInterruptBarrierRejectsSuccessorStartAndChildLifecycle(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_interrupt_barrier_lifecycle"
		threadID       = "thr_interrupt_barrier_lifecycle"
		bindingID      = "bind_interrupt_barrier_lifecycle"
		podUID         = "pod_interrupt_barrier_lifecycle"
		interruptID    = "rin_interrupt_barrier_lifecycle"
		interruptEvent = "evt_interrupt_barrier_lifecycle"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{interruptEvent}, SequenceFrom: 1, SequenceTo: 1,
	})
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	started, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_barrier_successor_start",
		ModelRequestId: "mreq_interrupt_barrier_successor", EventType: "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_interrupt_barrier_successor"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: requestKindAgentProviderRequest,
	})
	if err != nil || started.GetStale() == nil {
		t.Fatalf("successor Request Start = %#v/%v; want barrier stale", started, err)
	}
	created, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: scope, SourceToolUseEventId: "evt_interrupt_barrier_spawn", TaskName: "blocked", AgentType: "worker", InitialPrompt: "blocked first mail",
	})
	if err == nil || created != nil || !isSessionInterruptBarrierStaleError(err) {
		t.Fatalf("successor child lifecycle = %#v/%v; want private barrier-stale result", created, err)
	}

	var starts, threads int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_start'),
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&starts, &threads); err != nil {
		t.Fatalf("read blocked lifecycle facts: %v", err)
	}
	if starts != 0 || threads != 1 {
		t.Fatalf("blocked lifecycle facts = starts:%d threads:%d; want 0/1", starts, threads)
	}
}

func TestPostgreSQLSessionInterruptBarrierRejectsInternalToolRepair(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_barrier_internal_repair"
		threadID  = "thr_interrupt_barrier_internal_repair"
		bindingID = "bind_interrupt_barrier_internal_repair"
		podUID    = "pod_interrupt_barrier_internal_repair"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_barrier_internal_repair", 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_interrupt_barrier_internal_repair", InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_internal_repair"}, SequenceFrom: 1, SequenceTo: 1,
	})
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		ModelRequestId: "mreq_interrupt_barrier_internal_repair", ModelToolCallId: "call_interrupt_barrier_internal_repair",
		ToolName: "unknown_tool", CanonicalInputJson: `{}`,
		RepairKey: internalToolRepairKey("mreq_interrupt_barrier_internal_repair", "call_interrupt_barrier_internal_repair", "unknown_tool"),
		Error:     &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	}
	response, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).CommitInternalToolRepair(context.Background(), request)
	if err != nil || response.GetStale() == nil {
		t.Fatalf("internal Tool repair behind interrupt barrier = %#v/%v; want typed stale", response, err)
	}
	var events, messages, operations int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='commit_internal_tool_repair')`, sessionID).Scan(&events, &messages, &operations); err != nil {
		t.Fatalf("read blocked internal repair residue: %v", err)
	}
	if events != 0 || messages != 0 || operations != 0 {
		t.Fatalf("blocked internal repair residue = events:%d messages:%d operations:%d; want zero", events, messages, operations)
	}
}

func TestPostgreSQLColdLoadRemainsAvailableWhileInterruptBarrierIsActive(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_barrier_cold_load"
		threadID  = "thr_interrupt_barrier_cold_load"
		bindingID = "bind_interrupt_barrier_cold_load"
		podUID    = "pod_interrupt_barrier_cold_load"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_barrier_cold_message", 1, "user.message", `{"content":[{"type":"text","text":"durable before interrupt"}]}`)
	seedBridgeAPIProjectedUserMessage(t, admin, sessionID, threadID, "msg_interrupt_barrier_cold", "evt_interrupt_barrier_cold_message", 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, "evt_interrupt_barrier_cold_control", 2, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_interrupt_barrier_cold_control", InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_cold_control"}, SequenceFrom: 2, SequenceTo: 2,
	})
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("interrupt-barrier-cold-load-key")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
	})
	if err != nil || response.GetRuntimeBindingToken() == "" ||
		!strings.Contains(response.GetContextJson(), `"messageSequence":1`) ||
		!strings.Contains(response.GetContextJson(), `"type":"user.interrupt"`) {
		t.Fatalf("cold LoadContext during active interrupt barrier = %#v/%v; want durable message and interrupt custody", response, err)
	}
}
