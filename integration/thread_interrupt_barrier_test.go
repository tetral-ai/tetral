package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

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
	seedRuntimeInboxBirthForJob(t, admin, jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_control"}, SequenceFrom: 2, SequenceTo: 2,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, "evt_interrupt_barrier_control", 2)

	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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
	seedRuntimeInboxBirthForJob(t, admin, jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{interruptEvent}, SequenceFrom: 2, SequenceTo: 2,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, interruptEvent, 2)

	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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
			bridgeStore := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
			seedBridgeAPIRequestStart(t, bridgeStore, scope, "rwrite_tool_interrupt_start_"+suffix, modelRequestID, runtimecontrol.RequestKindAgentProviderRequest, 0)
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
				WHERE workspace_id='default' AND tool_use_event_id=$1`, toolUseID, terminalResult, runtimecontrol.Sha256Hex(terminalResult)); err != nil {
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
			interruptJob, err := jobrunner.DecodeRuntimeJob(queueJobProto(leased[0]))
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
	seedRuntimeInboxBirthForJob(t, admin, jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: interruptID, InputKind: "interrupt_control",
		EventIDs: []string{interruptEvent}, SequenceFrom: 1, SequenceTo: 1,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, interruptID, interruptEvent, 1)
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	started, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_interrupt_barrier_successor_start",
		ModelRequestId: "mreq_interrupt_barrier_successor", EventType: "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_interrupt_barrier_successor"}`,
		ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: runtimecontrol.RequestKindAgentProviderRequest,
	})
	if err != nil || started.GetStale() == nil {
		t.Fatalf("successor Request Start = %#v/%v; want barrier stale", started, err)
	}
	created, err := store.CreateSubagentThread(context.Background(), &bridgev1.CreateSubagentThreadRequest{
		Scope: scope, SourceToolUseEventId: "evt_interrupt_barrier_spawn", TaskName: "blocked", AgentType: "worker", InitialPrompt: "blocked first mail",
	})
	if err == nil || created != nil || !runtimecontrol.IsThreadInterruptBarrierStaleError(err) {
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
	seedRuntimeInboxBirthForJob(t, admin, jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_interrupt_barrier_internal_repair", InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_internal_repair"}, SequenceFrom: 1, SequenceTo: 1,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, "rin_interrupt_barrier_internal_repair", "evt_interrupt_barrier_internal_repair", 1)
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		ModelRequestId: "mreq_interrupt_barrier_internal_repair", ModelToolCallId: "call_interrupt_barrier_internal_repair",
		ToolName: "unknown_tool", CanonicalInputJson: `{}`,
		RepairKey: "internal_invalid_tool_3739ecf5202cc30f8a0b05a24f670596b528dd917bea6796f30e0b1323ea1789",
		Error:     &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
	}
	response, err := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).CommitInternalToolRepair(context.Background(), request)
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
	seedRuntimeInboxBirthForJob(t, admin, jobrunner.RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
		RuntimeInputID: "rin_interrupt_barrier_cold_control", InputKind: "interrupt_control",
		EventIDs: []string{"evt_interrupt_barrier_cold_control"}, SequenceFrom: 2, SequenceTo: 2,
	})
	seedActiveInterruptQueueCustody(t, runtime, sessionID, threadID, "rin_interrupt_barrier_cold_control", "evt_interrupt_barrier_cold_control", 2)
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
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
