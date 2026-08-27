package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLThreadInterruptBarrierDefersExactInflightCustody(t *testing.T) {
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

func TestPostgreSQLThreadInterruptBarrierRejectsLateMessageCommit(t *testing.T) {
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
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, "evt_interrupt_barrier_control", 2)

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

func TestPostgreSQLThreadInterruptBarrierMakesLateToolSettlementStale(t *testing.T) {
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
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, interruptEvent, 2)

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

func TestPostgreSQLToolSettlementAndInterruptBirthConvergeBothWinnerOrders(t *testing.T) {
	for _, settlementFirst := range []bool{true, false} {
		name := "interrupt_first"
		if settlementFirst {
			name = "tool_settlement_first"
		}
		t.Run(name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			suffix := "interrupt"
			if settlementFirst {
				suffix = "tool"
			}
			sessionID := "sesn_tool_interrupt_" + suffix
			threadID := "thr_tool_interrupt_" + suffix
			bindingID := "bind_tool_interrupt_" + suffix
			podUID := "pod_tool_interrupt_" + suffix
			modelRequestID := "mreq_tool_interrupt_" + suffix
			toolUseID := "evt_tool_interrupt_use_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
			bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			seedBridgeAPIRequestStart(t, bridgeStore, scope, "rwrite_tool_interrupt_start_"+suffix, modelRequestID, requestKindAgentProviderRequest, 0)
			sequence := nextBridgeAPIEventSequenceForTest(t, admin, sessionID, threadID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, sequence, "agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`)
			if _, err := admin.ExecContext(ctx, `UPDATE session_events SET model_request_id=$2,projection_json=$3
				WHERE workspace_id='default' AND event_id=$1`, toolUseID, modelRequestID, `{"model_tool_call_id":"call_`+suffix+`"}`); err != nil {
				t.Fatalf("stamp Tool Use provider identity: %v", err)
			}
			seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseID, "call_"+suffix, "Read")
			seedBridgeAPIAllowedToolRoute(t, admin, "default", sessionID, threadID, toolUseID)
			if accepted, err := bridgeStore.AcceptSandboxExecution(ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: toolUseID}); err != nil || accepted.GetCommitted() == nil {
				t.Fatalf("accept Tool execution = %#v/%v", accepted, err)
			}
			const terminalResult = `{"status":"success","result":{"content":"done"}}`
			if _, err := admin.ExecContext(ctx, `UPDATE session_runtime_tool_results
				SET execution_state='terminal_unconsumed',result_json=$2,result_digest=$3,updated_at=clock_timestamp()
				WHERE workspace_id='default' AND tool_use_event_id=$1`, toolUseID, terminalResult, sha256Hex(terminalResult)); err != nil {
				t.Fatalf("stage terminal Tool result: %v", err)
			}
			settleRequest := bridgeToolSettlementRequestForTest(scope, bridgeCompletedToolSettlementForTest(toolUseID, "done"))
			eventStore := sessionevent.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			eventService := sessionevent.NewService(eventStore)

			blocker, err := admin.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin Session race blocker: %v", err)
			}
			defer func() { _ = blocker.Rollback() }()
			var locked string
			if err := blocker.QueryRowContext(ctx, `SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&locked); err != nil {
				t.Fatalf("lock Session race owner: %v", err)
			}
			var blockerPID int
			if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
				t.Fatalf("read Session race blocker pid: %v", err)
			}
			type raceResult struct {
				kind       string
				settlement *bridgev1.SettleToolResultResponse
				interrupt  *sessionevent.AppendResult
				err        error
			}
			results := make(chan raceResult, 2)
			startSettlement := func() {
				go func() {
					response, err := bridgeStore.SettleToolResult(ctx, settleRequest)
					results <- raceResult{kind: "settlement", settlement: response, err: err}
				}()
			}
			startInterrupt := func() {
				go func() {
					response, err := eventService.AppendClientEvents(ctx, workspace.DefaultID, sessionID, "idem_tool_interrupt_"+suffix,
						sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserInterrupt}}})
					results <- raceResult{kind: "interrupt", interrupt: response, err: err}
				}()
			}
			if settlementFirst {
				startSettlement()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startInterrupt()
			} else {
				startInterrupt()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startSettlement()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
			if err := blocker.Commit(); err != nil {
				t.Fatalf("release Tool/interrupt race: %v", err)
			}
			var settlement *bridgev1.SettleToolResultResponse
			var interrupt *sessionevent.AppendResult
			for range 2 {
				outcome := <-results
				if outcome.err != nil {
					t.Fatalf("%s winner-order operation: %v", outcome.kind, outcome.err)
				}
				if outcome.kind == "settlement" {
					settlement = outcome.settlement
				} else {
					interrupt = outcome.interrupt
				}
			}
			if interrupt == nil || len(interrupt.Data) != 1 {
				t.Fatalf("interrupt birth = %#v; want one Event", interrupt)
			}
			if settlementFirst && settlement.GetCommitted() == nil {
				t.Fatalf("Tool-first settlement = %#v; want committed", settlement)
			}
			if !settlementFirst && settlement.GetStale() == nil {
				t.Fatalf("interrupt-first settlement = %#v; want typed stale", settlement)
			}

			queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			leased, err := queueStore.Lease(ctx, queue.LeaseRequest{
				WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "tool-interrupt-closeout",
				MaxJobs: 1, LeaseDuration: time.Minute,
			})
			if err != nil || len(leased) != 1 {
				t.Fatalf("lease interrupt closeout = %#v/%v", leased, err)
			}
			interruptJob, err := DecodeRuntimeJob(queueJobProto(leased[0]))
			if err != nil || interruptJob.InputKind != "interrupt_control" {
				t.Fatalf("decode interrupt closeout = %#v/%v", interruptJob, err)
			}
			if _, err := admin.ExecContext(ctx, `UPDATE session_runtime_inbox SET status='accepted',binding_id=$2,binding_generation=1,target_pod_uid=$3
				WHERE workspace_id='default' AND runtime_input_id=$1`, interruptJob.RuntimeInputID, bindingID, podUID); err != nil {
				t.Fatalf("accept interrupt closeout input: %v", err)
			}
			ended, err := bridgeStore.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_tool_interrupt_end_" + suffix, ModelRequestId: modelRequestID,
				FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
				ProviderContextRetention: &bridgev1.ProviderContextRetention{
					Disposition:     "interrupted",
					ToolUseEventIds: []string{toolUseID},
				},
				InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{
					RuntimeInputId: interruptJob.RuntimeInputID, InterruptLeaseRef: bridgeInterruptLeaseRef(leased[0]),
				},
			})
			if err != nil || ended.GetCommitted() == nil {
				t.Fatalf("interrupt Request End = %#v/%v; want committed", ended, err)
			}
			var resultEvents, requestEnds int
			if err := admin.QueryRowContext(ctx, `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$2),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND model_request_id=$3)`,
				sessionID, toolUseID, modelRequestID).Scan(&resultEvents, &requestEnds); err != nil {
				t.Fatalf("read converged Tool/interrupt facts: %v", err)
			}
			if resultEvents != 1 || requestEnds != 1 {
				t.Fatalf("converged Tool results/Request Ends = %d/%d; want 1/1", resultEvents, requestEnds)
			}
			var resultEventID, resultText string
			var isError bool
			if err := admin.QueryRowContext(ctx, `SELECT event_id,
				COALESCE((payload_json::jsonb->>'is_error')::boolean, false),
				COALESCE(payload_json::jsonb->'content'->0->>'text', '')
				FROM session_events
				WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_result'
				  AND payload_json::jsonb->>'tool_use_id'=$2`, sessionID, toolUseID).Scan(
				&resultEventID, &isError, &resultText,
			); err != nil {
				t.Fatalf("read converged Tool Result payload: %v", err)
			}
			if settlementFirst {
				if isError || resultText != "done" {
					t.Fatalf("Tool-first terminal payload = error:%t text:%q; want success done", isError, resultText)
				}
			} else if !isError {
				t.Fatal("interrupt-first terminal Tool Result is not an error")
			}
			var executionState, consumedEventID, consumptionReason string
			var resultJSON sql.NullString
			if err := admin.QueryRowContext(ctx, `SELECT execution_state, result_json,
				COALESCE(consumed_by_terminal_event_id, ''), COALESCE(consumption_reason, '')
				FROM session_runtime_tool_results
				WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`,
				sessionID, toolUseID,
			).Scan(&executionState, &resultJSON, &consumedEventID, &consumptionReason); err != nil {
				t.Fatalf("read converged executor custody: %v", err)
			}
			if executionState != "consumed" || resultJSON.Valid || consumedEventID != resultEventID || consumptionReason != "conversation_tool_result" {
				t.Fatalf("executor custody = state:%s result:%#v event:%s reason:%s; want consumed/null/%s/conversation_tool_result",
					executionState, resultJSON, consumedEventID, consumptionReason, resultEventID)
			}
		})
	}
}

func TestPostgreSQLThreadInterruptBarrierRejectsSuccessorStartAndChildLifecycle(t *testing.T) {
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
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, interruptEvent, 1)
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
	if err == nil || created != nil || !isThreadInterruptBarrierStaleError(err) {
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

func TestPostgreSQLThreadInterruptBarrierRejectsInternalToolRepair(t *testing.T) {
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
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, "rin_interrupt_barrier_internal_repair", "evt_interrupt_barrier_internal_repair", 1)
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
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, "rin_interrupt_barrier_cold_control", "evt_interrupt_barrier_cold_control", 2)
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

func TestPostgreSQLThreadInterruptBarrierIgnoresLockedTerminalHistory(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_interrupt_barrier_bounded_history"
		threadID  = "thr_interrupt_barrier_bounded_history"
		activeID  = "rin_interrupt_barrier_bounded_history_active"
		activeEvt = "evt_interrupt_barrier_bounded_history_active"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, activeEvt, 1, "user.interrupt", `{}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: activeID, InputKind: "interrupt_control", EventIDs: []string{activeEvt}, SequenceFrom: 1, SequenceTo: 1,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, activeID, activeEvt, 1)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		sequence_from, sequence_to, status, created_at, updated_at
	) SELECT 'default', $1, $2, 'rin_terminal_' || value, 'interrupt_control', '[]',
		value + 10, value + 10, 'cancelled', clock_timestamp(), clock_timestamp()
		FROM generate_series(1, 1000) AS value`, sessionID, threadID); err != nil {
		t.Fatalf("seed terminal interrupt history: %v", err)
	}
	terminalLock, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin terminal history lock: %v", err)
	}
	t.Cleanup(func() { _ = terminalLock.Rollback() })
	if _, err := terminalLock.ExecContext(context.Background(), `SELECT runtime_input_id
		FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id='rin_terminal_1' FOR UPDATE`); err != nil {
		t.Fatalf("lock terminal history row: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client := dbconnect.NewClientForTesting(runtime)
	var active bool
	if err := client.WithWorkspaceTx(ctx, "default", "bridge.interrupt_barrier_bounded_history", func(tx *dbconnect.Tx) error {
		var barrier threadInterruptBarrier
		var lookupErr error
		barrier, active, lookupErr = activeInterruptBarrierTx(ctx, tx, "default", sessionID, threadID)
		if lookupErr == nil && (!active || barrier.runtimeInputID != activeID) {
			return errors.New("active interrupt barrier was not selected")
		}
		return lookupErr
	}); err != nil {
		t.Fatalf("active barrier waited on terminal history: %v", err)
	}
}

func seedActiveInterruptQueueCustody(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	threadID string,
	runtimeInputID string,
	eventID string,
	sequence int64,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": runtimeInputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal interrupt Queue custody: %v", err)
	}
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payload, Priority: 100,
		MaxAttempts: queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("seed active interrupt Queue custody: %v", err)
	}
}
