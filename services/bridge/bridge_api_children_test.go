package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestThreadControlLogsUseBoundedLifecycleIdentity(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	scope := bridgeAPIScope("sesn_log", "thr_parent_log", "bind_log", 1, "pod_log")
	logThreadControlEvent(logger, "thread_interrupt_admitted", scope, "thr_root_log", "evt_source_log", 3, 0)
	logThreadControlEvent(logger, "thread_interrupt_completed", scope, "thr_root_log", "evt_source_log", 3, 2, 17)
	logThreadControlEvent(logger, "thread_interrupt_stale", scope, "thr_root_log", "evt_source_log", 3, 0)
	logThreadControlFailure(logger, scope, "evt_source_log", "closeout", status.Error(codes.FailedPrecondition, "raw payload must not be logged"))
	logThreadResumeRejected(logger, scope, "thr_target_log", status.Error(codes.FailedPrecondition, "raw checkpoint must not be logged"))
	text := output.String()
	for _, fragment := range []string{
		`"event.kind":"thread_interrupt_admitted"`, `"event.kind":"thread_interrupt_completed"`,
		`"event.kind":"thread_interrupt_stale"`, `"event.kind":"thread_control_failed"`,
		`"event.kind":"thread_resume_rejected"`, `"operation.id":"evt_source_log"`,
		`"thread.id":"thr_root_log"`, `"target.count":3`, `"projection.count":2`,
		`"outcome":"completed"`, `"duration.ms":17`, `"stale.reason":"binding_custody_changed"`,
		`"error.message_safe":"thread control failed"`, `"thread.id":"thr_target_log"`,
		`"stage":"validate_before_reactivate"`,
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("thread control logs missing %s: %s", fragment, text)
		}
	}
	if strings.Contains(text, "raw payload") || strings.Contains(text, "raw checkpoint") {
		t.Fatalf("thread control logs contain raw failure detail: %s", text)
	}
}

// This file owns the Bridge children protocol-family boundary.

func seedBridgeAPIChildLifecycleToolSource(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	threadID string,
	eventID string,
) *bridgev1.ChildLifecycleSource {
	t.Helper()
	toolName := "close_agent"
	if strings.Contains(eventID, "resume") {
		toolName = "resume_agent"
	}
	seedBridgeAPIEvent(
		t,
		db,
		"default",
		sessionID,
		threadID,
		eventID,
		nextBridgeAPIEventSequenceForTest(t, db, sessionID, threadID),
		"agent.tool_use",
		fmt.Sprintf(`{"type":"agent.tool_use","name":%q,"input":{},"evaluated_permission":"allow"}`, toolName),
	)
	if _, err := db.ExecContext(
		context.Background(),
		`UPDATE session_events
		    SET visibility = 'public'
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND event_id = $3`,
		sessionID,
		threadID,
		eventID,
	); err != nil {
		t.Fatalf("make child lifecycle source public: %v", err)
	}
	return &bridgev1.ChildLifecycleSource{
		Identity: &bridgev1.ChildLifecycleSource_SourceToolUseEventId{
			SourceToolUseEventId: eventID,
		},
	}
}

func prepareCompletedChildCloseForTest(
	t *testing.T,
	db *sql.DB,
	store *PostgreSQLBridgeAPIStore,
	request *bridgev1.MarkChildThreadClosedRequest,
) {
	t.Helper()
	sourceID := request.GetSource().GetSourceToolUseEventId()
	admitted, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: request.GetScope(), RootChildThreadId: request.GetChildThreadId(), SourceToolUseEventId: sourceID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
	})
	if err != nil {
		t.Fatalf("admit child close interrupt: %v", err)
	}
	for _, target := range admitted.GetTargets() {
		if target.GetRuntimeInputId() == "" {
			continue
		}
		if _, err := db.ExecContext(context.Background(), `UPDATE session_runtime_inbox
			SET status='accepted', binding_id=$3, binding_generation=$4, target_pod_uid=$5
			WHERE workspace_id=$1 AND runtime_input_id=$2`, request.GetScope().GetWorkspaceId(), target.GetRuntimeInputId(),
			request.GetScope().GetBinding().GetBindingId(), request.GetScope().GetBinding().GetBindingGeneration(), request.GetScope().GetBinding().GetTargetPodUid()); err != nil {
			t.Fatalf("accept child close interrupt fixture: %v", err)
		}
		if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope:          scopeForThread(request.GetScope(), target.GetChildThreadId()),
			RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
			EventIds:     []string{target.GetInterruptEventId()},
			SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
		}); err != nil {
			t.Fatalf("complete child close interrupt: %v", err)
		}
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
		Scope: request.GetScope(), RootChildThreadId: request.GetChildThreadId(), SourceToolUseEventId: sourceID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
		Targets: admitted.GetTargets(),
	}); err != nil {
		t.Fatalf("await child close interrupt: %v", err)
	}
	request.SourceToolUseEventId = &sourceID
	request.Targets = admitted.GetTargets()
}

func TestPostgreSQLBridgeAPIStoreInterruptClassifiesStartedAndBackgroundTools(t *testing.T) {
	tests := []struct {
		name, toolKind, executionState, backgroundKind, backgroundState, wantCode string
		pendingApproval                                                           bool
		providerReference, syntheticReceipt                                       bool
	}{
		{name: "pending approval before execution", pendingApproval: true, wantCode: "runtime_interrupted"},
		{name: "approved running execution", toolKind: "sandbox_tool", executionState: "running", pendingApproval: true, wantCode: "runtime_interrupted_outcome_unknown"},
		{name: "identified running execution", toolKind: "sandbox_tool", executionState: "running", providerReference: true, wantCode: "runtime_interrupted_outcome_unknown"},
		{name: "terminal execution before commit", toolKind: "sandbox_tool", executionState: "terminal_unconsumed", pendingApproval: true, wantCode: "runtime_interrupted_result_not_committed"},
		{name: "pending background poll", toolKind: "sandbox_background", backgroundKind: "poll", backgroundState: "pending", wantCode: "runtime_interrupted_outcome_unknown"},
		{name: "pending background stdin", toolKind: "sandbox_background", backgroundKind: "stdin", backgroundState: "pending", wantCode: "runtime_interrupted"},
		{name: "submitted background operation", toolKind: "sandbox_background", backgroundKind: "poll", backgroundState: "submitted", wantCode: "runtime_interrupted_outcome_unknown"},
		{name: "terminal background result", toolKind: "sandbox_background", backgroundKind: "poll", backgroundState: "terminal", wantCode: "runtime_interrupted_result_not_committed"},
		{name: "background tool without direct receipt", syntheticReceipt: true, wantCode: "runtime_interrupted_outcome_unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID, mainID, childID := "sesn_interrupt_class_"+suffix, "thr_interrupt_main_"+suffix, "thr_interrupt_child_"+suffix
			bindingID, podUID := "bind_interrupt_"+suffix, "pod_interrupt_"+suffix
			toolID, requestID := "evt_interrupt_tool_"+suffix, "mreq_interrupt_"+suffix
			seedBridgeAPISession(t, admin, "default", sessionID, mainID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_interrupt_source_"+suffix, 1, "agent.tool_use", `{"type":"agent.tool_use","name":"interrupt_agent","input":{},"evaluated_permission":"allow"}`)
			if _, err := admin.Exec(`UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND event_id=$1`, "evt_interrupt_source_"+suffix); err != nil {
				t.Fatalf("publish interrupt source: %v", err)
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, childID, toolID, 1, "agent.tool_use", `{"type":"agent.tool_use","name":"exec_command","input":{},"evaluated_permission":"allow"}`)
			if _, err := admin.Exec(`UPDATE session_events SET model_request_id=$2 WHERE workspace_id='default' AND event_id=$1`, toolID, requestID); err != nil {
				t.Fatalf("stamp tool request: %v", err)
			}
			seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, childID, requestID, toolID, "call_"+suffix, "exec_command")
			if test.pendingApproval {
				if _, err := admin.Exec(`INSERT INTO session_pending_tool_uses (workspace_id,session_id,session_thread_id,tool_use_event_id,model_tool_call_id,tool_name,input_json,status,created_at,updated_at) VALUES ('default',$1,$2,$3,$4,'exec_command','{}','pending',now(),now())`, sessionID, childID, toolID, "call_"+suffix); err != nil {
					t.Fatalf("seed pending approval: %v", err)
				}
			}
			switch test.toolKind {
			case "sandbox_tool":
				resultJSON, digest := any(nil), any(nil)
				if test.executionState == "terminal_unconsumed" {
					resultJSON, digest = `{"status":"completed"}`, "terminal_digest"
				}
				if _, err := admin.Exec(`INSERT INTO session_runtime_tool_results (workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,ack_status,result_json,result_digest,model_tool_call_id,execution_state,execution_attempt_generation,created_at,updated_at) VALUES ('default',$1,$2,$3,'sandbox_tool','hash','exec_command','{}','committed',$4,$5,$6,$7,1,now(),now())`, sessionID, childID, toolID, resultJSON, digest, "call_"+suffix, test.executionState); err != nil {
					t.Fatalf("seed sandbox execution: %v", err)
				}
				if test.providerReference {
					if _, err := admin.Exec(`UPDATE session_runtime_tool_results SET provider_command_reference_json='{"task_id":"provider-task"}' WHERE workspace_id='default' AND tool_use_event_id=$1`, toolID); err != nil {
						t.Fatalf("identify running provider command: %v", err)
					}
				}
			case "sandbox_background":
				resultJSON, digest := any(nil), any(nil)
				if test.backgroundState == "terminal" {
					resultJSON, digest = `{"status":"completed"}`, "background_terminal_digest"
				}
				if _, err := admin.Exec(`INSERT INTO session_runtime_tool_results (workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,ack_status,result_json,result_digest,background_operation_kind,background_operation_state,background_request_id,background_task_id,background_max_output_tokens,created_at,updated_at) VALUES ('default',$1,$2,$3,'sandbox_background','hash','exec_command','{}','committed',$4,$5,$6,$7,$8,$9,100,now(),now())`, sessionID, childID, toolID, resultJSON, digest, test.backgroundKind, test.backgroundState, "breq_"+suffix, "task_"+suffix); err != nil {
					t.Fatalf("seed background operation: %v", err)
				}
			}
			if test.syntheticReceipt {
				if _, err := admin.Exec(`INSERT INTO session_runtime_tool_results (workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,ack_status,background_operation_kind,background_operation_state,background_request_id,background_task_id,background_max_output_tokens,created_at,updated_at) VALUES ('default',$1,$2,$3,'sandbox_background','hash','CancelCommand','{}','committed','poll','submitted',$4,$5,100,now(),now())`, sessionID, childID, "synthetic_"+suffix, "breq_synthetic_"+suffix, "task_synthetic_"+suffix); err != nil {
					t.Fatalf("seed synthetic background receipt: %v", err)
				}
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
			if test.name == "approved running execution" {
				seedReadySandboxForSharedToolExecution(t, admin, "default", sessionID)
				providerHandle := "provider_" + sessionID
				if _, err := admin.Exec(`UPDATE session_runtime_tool_results SET authorized_provider_resource_id=$2 WHERE workspace_id='default' AND tool_use_event_id=$1`, toolID, providerHandle); err != nil {
					t.Fatalf("authorize running execution handle: %v", err)
				}
				if err := store.withScopeTx(context.Background(), scope, "test.interrupt.release", func(tx *dbconnect.Tx) error {
					_, _, err := sandboxrelease.EnsureTx(context.Background(), tx, "default", sessionID, sandboxrelease.SessionDelete, providerHandle, time.Now().UTC())
					return err
				}); err != nil {
					t.Fatalf("declare blocked release: %v", err)
				}
			}
			admitted, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{Scope: scope, RootChildThreadId: childID, SourceToolUseEventId: "evt_interrupt_source_" + suffix, Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT})
			if err != nil || len(admitted.GetTargets()) != 1 {
				t.Fatalf("AdmitChildInterrupt = %#v, %v", admitted, err)
			}
			target := admitted.GetTargets()[0]
			if _, err := admin.Exec(`UPDATE session_runtime_inbox SET status='accepted', binding_id=$2, binding_generation=1, target_pod_uid=$3 WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID, podUID); err != nil {
				t.Fatalf("accept interrupt fixture: %v", err)
			}
			commitRequest := &bridgev1.CommitInputsRequest{Scope: scopeForThread(scope, childID), RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control", EventIds: []string{target.GetInterruptEventId()}, SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence()}
			committed, err := store.CommitInputs(context.Background(), commitRequest)
			if err != nil {
				t.Fatalf("CommitInputs interrupt: %v", err)
			}
			projections := committed.GetDeclaration().GetReceipts()[0].GetInterruptToolProjections()
			if len(projections) != 1 || projections[0].GetToolUseEventId() != toolID {
				t.Fatalf("interrupt projections = %#v; want exactly the durable Tool Use", projections)
			}
			projection := projections[0]
			errorJSON := projection.GetError().GetErrorJson()
			if projection.GetCancelled() != nil {
				errorJSON = projection.GetCancelled().GetErrorJson()
			}
			if got := testJSONPathString(t, errorJSON, "type"); got != test.wantCode {
				t.Fatalf("interrupt Tool error type = %q; want %q", got, test.wantCode)
			}
			awaited, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
				Scope: scope, RootChildThreadId: childID, SourceToolUseEventId: "evt_interrupt_source_" + suffix,
				Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT, Targets: admitted.GetTargets(),
			})
			if err != nil || len(awaited.GetOutcomes()) != 1 || awaited.GetOutcomes()[0].GetOutcome() != bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_COMPLETED {
				t.Fatalf("AwaitChildInterrupt = %+v, %v; want one completed outcome", awaited, err)
			}
			replay, err := store.CommitInputs(context.Background(), commitRequest)
			if err != nil || replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
				t.Fatalf("CommitInputs replay = %+v, %v; want duplicate", replay, err)
			}
			if replayProjections := replay.GetDeclaration().GetReceipts()[0].GetInterruptToolProjections(); len(replayProjections) != 1 || !proto.Equal(replayProjections[0], projection) {
				t.Fatalf("CommitInputs replay projections = %+v; want original projection", replayProjections)
			}
			var resultEvents int
			if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$3`, sessionID, childID, toolID).Scan(&resultEvents); err != nil {
				t.Fatalf("count interrupt Tool Results: %v", err)
			}
			if resultEvents != 1 {
				t.Fatalf("interrupt Tool Result count = %d; want one", resultEvents)
			}
			if test.pendingApproval {
				var pendingStatus string
				if err := admin.QueryRow(`SELECT status FROM session_pending_tool_uses WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolID).Scan(&pendingStatus); err != nil {
					t.Fatalf("read interrupted pending approval: %v", err)
				}
				if pendingStatus != "cancelled" {
					t.Fatalf("pending approval status = %q; want cancelled", pendingStatus)
				}
			}
			if test.toolKind != "" {
				var executionState, backgroundState, resultJSON, consumedBy, consumptionReason, cancelState, providerReference sql.NullString
				if err := admin.QueryRow(`SELECT execution_state,background_operation_state,result_json,consumed_by_terminal_event_id,consumption_reason,cancel_state,provider_command_reference_json FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolID).Scan(&executionState, &backgroundState, &resultJSON, &consumedBy, &consumptionReason, &cancelState, &providerReference); err != nil {
					t.Fatalf("read interrupted execution: %v", err)
				}
				if resultJSON.Valid || !consumedBy.Valid || consumedBy.String != projection.GetResultEvent().GetEventId() || !consumptionReason.Valid || consumptionReason.String != "conversation_tool_result" {
					t.Fatalf("interrupted execution consumption = result %v event %v reason %v", resultJSON, consumedBy, consumptionReason)
				}
				if test.toolKind == "sandbox_tool" && (!executionState.Valid || executionState.String != "consumed") {
					t.Fatalf("sandbox execution state = %v; want consumed", executionState)
				}
				if test.toolKind == "sandbox_background" && (!backgroundState.Valid || backgroundState.String != "terminal") {
					t.Fatalf("background state = %v; want terminal", backgroundState)
				}
				if test.providerReference && (!cancelState.Valid || cancelState.String != "pending" || !providerReference.Valid) {
					t.Fatalf("identified cancellation tuple = state %v reference %v; want retained pending tuple", cancelState, providerReference)
				}
			}
			if test.name == "pending approval before execution" {
				var executionRows int
				if err := admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolID).Scan(&executionRows); err != nil {
					t.Fatalf("count not-started execution rows: %v", err)
				}
				if executionRows != 0 {
					t.Fatalf("not-started approval execution rows = %d; want zero", executionRows)
				}
			}
			var cancellationJobs int
			if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$1`, queue.KindSandboxToolCancel).Scan(&cancellationJobs); err != nil {
				t.Fatalf("count cancellation jobs: %v", err)
			}
			if cancellationJobs != map[bool]int{false: 0, true: 1}[test.providerReference] {
				t.Fatalf("cancellation jobs = %d; identified=%t", cancellationJobs, test.providerReference)
			}
			if test.syntheticReceipt {
				var syntheticState string
				var syntheticConsumed sql.NullString
				if err := admin.QueryRow(`SELECT background_operation_state,consumed_by_terminal_event_id FROM session_runtime_tool_results WHERE workspace_id='default' AND tool_use_event_id=$1`, "synthetic_"+suffix).Scan(&syntheticState, &syntheticConsumed); err != nil {
					t.Fatalf("read synthetic receipt: %v", err)
				}
				if syntheticState != "submitted" || syntheticConsumed.Valid {
					t.Fatalf("synthetic receipt = state %q consumed %v; want independently submitted", syntheticState, syntheticConsumed)
				}
			}
			if test.name == "approved running execution" {
				var releaseJobs int
				if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$1 AND status='pending'`, queue.KindSandboxRelease).Scan(&releaseJobs); err != nil {
					t.Fatalf("count release jobs: %v", err)
				}
				if releaseJobs != 1 {
					t.Fatalf("release jobs = %d; want one transactionally woken release", releaseJobs)
				}
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreOrdinaryToolResultWinsInterruptCensusRace(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_interrupt_result_race"
	const mainID = "thr_interrupt_result_race_main"
	const childID = "thr_interrupt_result_race_child"
	const bindingID = "bind_interrupt_result_race"
	const podUID = "pod_interrupt_result_race"
	const toolID = "evt_interrupt_result_race_tool"
	const modelRequestID = "mreq_interrupt_result_race"
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_interrupt_result_race_source", 1, "agent.tool_use", `{"type":"agent.tool_use","name":"interrupt_agent","input":{},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, toolID, 1, "agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.Exec(`UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND event_id='evt_interrupt_result_race_source'`); err != nil {
		t.Fatalf("publish interrupt source: %v", err)
	}
	if _, err := admin.Exec(`UPDATE session_events SET model_request_id=$2 WHERE workspace_id='default' AND event_id=$1`, toolID, modelRequestID); err != nil {
		t.Fatalf("stamp Tool request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, childID, modelRequestID, toolID, "call_interrupt_result_race", "Read")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	admitted, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, RootChildThreadId: childID, SourceToolUseEventId: "evt_interrupt_result_race_source",
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	})
	if err != nil || len(admitted.GetTargets()) != 1 {
		t.Fatalf("AdmitChildInterrupt = %#v, %v", admitted, err)
	}
	target := admitted.GetTargets()[0]
	if _, err := admin.Exec(`UPDATE session_runtime_inbox SET status='accepted', binding_id=$2, binding_generation=1, target_pod_uid=$3 WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID, podUID); err != nil {
		t.Fatalf("accept interrupt fixture: %v", err)
	}

	locker, lockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID)
	defer func() { _ = locker.Rollback() }()
	resultDone := make(chan error, 1)
	go func() {
		_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scopeForThread(scope, childID), RuntimeWriteId: "rwrite_interrupt_result_race",
			ModelRequestId: modelRequestID, EventType: "agent.tool_result",
			PayloadJson: `{"type":"agent.tool_result","tool_use_id":"` + toolID + `","content":[{"type":"text","text":"done"}]}`,
			Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolID, "done")},
		})
		resultDone <- err
	}()
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 1)
	type interruptResult struct {
		response *bridgev1.CommitInputsResponse
		err      error
	}
	interruptDone := make(chan interruptResult, 1)
	go func() {
		response, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope: scopeForThread(scope, childID), RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
			EventIds: []string{target.GetInterruptEventId()}, SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
		})
		interruptDone <- interruptResult{response: response, err: err}
	}()
	waitForPostgreSQLLockWaiters(t, admin, lockerPID, 2)
	if err := locker.Commit(); err != nil {
		t.Fatalf("release result/interrupt race fence: %v", err)
	}
	if err := <-resultDone; err != nil {
		t.Fatalf("ordinary Tool Result: %v", err)
	}
	interrupted := <-interruptDone
	if interrupted.err != nil {
		t.Fatalf("interrupt after Tool Result: %v", interrupted.err)
	}
	if projections := interrupted.response.GetDeclaration().GetReceipts()[0].GetInterruptToolProjections(); len(projections) != 0 {
		t.Fatalf("interrupt projected already-settled Tool: %#v", projections)
	}
	var resultEvents, interruptedResults int
	if err := admin.QueryRow(`SELECT count(*),count(*) FILTER (WHERE payload_json LIKE '%runtime_interrupted%') FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$3`, sessionID, childID, toolID).Scan(&resultEvents, &interruptedResults); err != nil {
		t.Fatalf("read converged Tool Result: %v", err)
	}
	if resultEvents != 1 || interruptedResults != 0 {
		t.Fatalf("converged Tool Results = %d interrupted=%d; want one ordinary result", resultEvents, interruptedResults)
	}
}

func TestPostgreSQLBridgeAPIStoreRejectsOverlappingChildControlAndReplaysFrozenCensus(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_child_control_overlap"
	const mainID = "thr_child_control_overlap_main"
	const parentID = "thr_child_control_overlap_parent"
	const childID = "thr_child_control_overlap_child"
	const bindingID = "bind_child_control_overlap"
	const podUID = "pod_child_control_overlap"
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_child_control_source_a", 1, "agent.tool_use", `{"type":"agent.tool_use","name":"close_agent","input":{},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "evt_child_control_source_b", 1, "agent.tool_use", `{"type":"agent.tool_use","name":"interrupt_agent","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.Exec(`UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND event_id IN ('evt_child_control_source_a','evt_child_control_source_b')`); err != nil {
		t.Fatalf("publish control sources: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	firstRequest := &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, RootChildThreadId: parentID, SourceToolUseEventId: "evt_child_control_source_a",
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
	}
	first, err := store.AdmitChildInterrupt(context.Background(), firstRequest)
	if err != nil || len(first.GetTargets()) != 2 {
		t.Fatalf("first control admission = %+v, %v; want frozen parent/child census", first, err)
	}
	var beforeEvents, beforeInbox, beforeQueue int
	if err := admin.QueryRow(`SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_interrupt_requested'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$2)`, sessionID, queue.KindRuntimeInput).Scan(&beforeEvents, &beforeInbox, &beforeQueue); err != nil {
		t.Fatalf("count first control state: %v", err)
	}
	_, err = store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: scopeForThread(scope, parentID), RootChildThreadId: childID, SourceToolUseEventId: "evt_child_control_source_b",
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_INTERRUPT,
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "child_control_in_progress") {
		t.Fatalf("overlapping control err = %v; want child_control_in_progress", err)
	}
	var afterEvents, afterInbox, afterQueue int
	if err := admin.QueryRow(`SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_interrupt_requested'),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='interrupt_control'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$2)`, sessionID, queue.KindRuntimeInput).Scan(&afterEvents, &afterInbox, &afterQueue); err != nil {
		t.Fatalf("count rejected control state: %v", err)
	}
	if afterEvents != beforeEvents || afterInbox != beforeInbox || afterQueue != beforeQueue {
		t.Fatalf("rejected overlap mutated event/inbox/Queue census: before %d/%d/%d after %d/%d/%d", beforeEvents, beforeInbox, beforeQueue, afterEvents, afterInbox, afterQueue)
	}
	replay, err := store.AdmitChildInterrupt(context.Background(), firstRequest)
	if err != nil || replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || !reflect.DeepEqual(first.GetTargets(), replay.GetTargets()) {
		t.Fatalf("same-source replay = %+v, %v; want exact frozen census", replay, err)
	}
}

func TestPostgreSQLBridgeAPIStoreChildControlDeliveryExhaustionPreventsClose(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_child_control_delivery_failure"
	const mainID = "thr_child_control_delivery_failure_main"
	const childID = "thr_child_control_delivery_failure_child"
	const bindingID = "bind_child_control_delivery_failure"
	const podUID = "pod_child_control_delivery_failure"
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_child_control_delivery_failure")
	client := dbconnect.NewClientForTesting(runtime)
	store := NewPostgreSQLBridgeAPIStore(client)
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	request := &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, RootChildThreadId: childID, SourceToolUseEventId: source.GetSourceToolUseEventId(),
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
	}
	admitted, err := store.AdmitChildInterrupt(context.Background(), request)
	if err != nil || len(admitted.GetTargets()) != 1 {
		t.Fatalf("AdmitChildInterrupt = %+v, %v; want one delivery", admitted, err)
	}
	queueStore := queue.NewPostgreSQLStore(client)
	now := time.Now().UTC().Add(time.Hour)
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-child-control-test",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease child control delivery = %d, %v; want one", len(leased), err)
	}
	runtimeJob, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode child control Queue job: %v", err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	plan, err := deliveryStore.PrepareRuntimeCommand(context.Background(), runtimeJob)
	if err != nil {
		t.Fatalf("prepare leased child control delivery: %v", err)
	}
	if _, err := deliveryStore.FinalizeRuntimeDelivery(context.Background(), runtimeJob, runtimeDeliveryResultWithAttemptedBinding(retryableExhaustionResult(), plan.Request)); err != nil {
		t.Fatalf("finalize child control delivery exhaustion: %v", err)
	}
	deadLettered, err := queueStore.DeadLetter(context.Background(), queue.DeadLetterRequest{
		WorkspaceID: workspace.DefaultID, JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken,
		ErrorKind: "runtime_delivery_exhausted", ErrorMessage: "runtime delivery exhausted", Now: now.Add(time.Second),
	})
	if err != nil || !deadLettered {
		t.Fatalf("dead-letter child control Queue lease = %v, %v; want true,nil", deadLettered, err)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRow(`SELECT inbox.status,q.status FROM session_runtime_inbox inbox JOIN queue_jobs q ON q.workspace_id=inbox.workspace_id AND q.dedupe_key='runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, admitted.GetTargets()[0].GetRuntimeInputId()).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read failed child control custody: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered {
		t.Fatalf("failed child control custody = inbox %q Queue %q; want dead_lettered/dead_lettered", inboxStatus, queueStatus)
	}
	awaited, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
		Scope: scope, RootChildThreadId: childID, SourceToolUseEventId: source.GetSourceToolUseEventId(),
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true, Targets: admitted.GetTargets(),
	})
	if err != nil || len(awaited.GetOutcomes()) != 1 || awaited.GetOutcomes()[0].GetOutcome() != bridgev1.ChildInterruptOutcome_CHILD_INTERRUPT_OUTCOME_DELIVERY_FAILED || awaited.GetOutcomes()[0].GetErrorCode() != "child_interrupt_delivery_failed" {
		t.Fatalf("AwaitChildInterrupt after delivery exhaustion = %+v, %v; want DELIVERY_FAILED", awaited, err)
	}
	sourceID := source.GetSourceToolUseEventId()
	_, err = store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope: scope, ChildThreadId: childID, Source: source, SourceToolUseEventId: &sourceID, Targets: admitted.GetTargets(),
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "child_close_delivery_failed") {
		t.Fatalf("MarkChildThreadClosed after delivery failure err = %v; want child_close_delivery_failed", err)
	}
	var childStatus string
	if err := admin.QueryRow(`SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID).Scan(&childStatus); err != nil {
		t.Fatalf("read child after delivery failure: %v", err)
	}
	if childStatus != "idle" {
		t.Fatalf("child status after delivery failure = %q; want original idle status", childStatus)
	}
}

func TestPostgreSQLBridgeAPIStoreChildControlFenceRejectsDirectedWorkUntilClose(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_child_control_work_fence"
	const mainID = "thr_child_control_work_fence_main"
	const childID = "thr_child_control_work_fence_child"
	const grandchildID = "thr_child_control_work_fence_grandchild"
	const bindingID = "bind_child_control_work_fence"
	const podUID = "pod_child_control_work_fence"
	const toolID = "evt_child_control_work_fence_tool"
	const modelRequestID = "mreq_child_control_work_fence"
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, grandchildID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, toolID, 1, "agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"ask"}`)
	if _, err := admin.Exec(`UPDATE session_events SET model_request_id=$2 WHERE workspace_id='default' AND event_id=$1`, toolID, modelRequestID); err != nil {
		t.Fatalf("stamp pending Tool request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, childID, modelRequestID, toolID, "call_work_fence", "Read")
	if _, err := admin.Exec(`INSERT INTO session_pending_tool_uses (workspace_id,session_id,session_thread_id,tool_use_event_id,model_tool_call_id,tool_name,input_json,status,created_at,updated_at) VALUES ('default',$1,$2,$3,'call_work_fence','Read','{}','pending',now(),now())`, sessionID, childID, toolID); err != nil {
		t.Fatalf("seed pending Tool confirmation: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_child_control_work_fence_source", 1, "agent.tool_use", `{"type":"agent.tool_use","name":"close_agent","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.Exec(`UPDATE session_events SET visibility='public' WHERE workspace_id='default' AND event_id='evt_child_control_work_fence_source'`); err != nil {
		t.Fatalf("publish close source: %v", err)
	}
	directMessageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_child_control_work_fence_direct", "direct message")
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_child_control_work_fence_sent", 2, "agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, "delivery_child_control_work_fence_direct", mainID, childID, "worker", "evt_child_control_work_fence_send_tool", directMessageJSON))
	seedAgentMailCustody(t, admin, sessionID, childID, "delivery_child_control_work_fence_direct", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	seedCompletionMailSentAt(t, admin, sessionID, childID, grandchildID, "delivery_child_control_work_fence_completion", 1, "2026-08-09T00:00:00Z")

	client := dbconnect.NewClientForTesting(runtime)
	store := NewPostgreSQLBridgeAPIStore(client)
	parentScope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	closeSource := &bridgev1.ChildLifecycleSource{Identity: &bridgev1.ChildLifecycleSource_SourceToolUseEventId{SourceToolUseEventId: "evt_child_control_work_fence_source"}}
	admitted, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
		Scope: parentScope, RootChildThreadId: childID, SourceToolUseEventId: closeSource.GetSourceToolUseEventId(),
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
	})
	if err != nil || len(admitted.GetTargets()) != 2 {
		t.Fatalf("AdmitChildInterrupt = %+v, %v; want frozen child subtree", admitted, err)
	}

	eventService := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	_, err = eventService.AppendClientEvents(context.Background(), workspace.DefaultID, sessionID, "idem_child_control_fenced_confirmation", sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{
		Type: sessionevent.EventTypeUserToolConfirmation, ToolUseID: toolID, Result: sessionevent.ToolConfirmationResultAllow,
	}}})
	var conflict *sessionevent.ConflictError
	if !errors.As(err, &conflict) || !strings.Contains(err.Error(), "closing") {
		t.Fatalf("Tool confirmation during child control err = %T %v; want closing conflict", err, err)
	}
	_, err = store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope: parentScope, ChildThreadId: childID, DeliveryId: "delivery_child_control_work_fence_direct",
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "target is closing") {
		t.Fatalf("direct agent mail during child control err = %v; want target closing", err)
	}
	_, err = store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope: scopeForThread(parentScope, childID), ChildThreadId: grandchildID,
	})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "target is closing") {
		t.Fatalf("completion mail during child control err = %v; want target closing", err)
	}
	var receivedEvents int
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'`, sessionID).Scan(&receivedEvents); err != nil {
		t.Fatalf("count fenced mail receives: %v", err)
	}
	if receivedEvents != 0 {
		t.Fatalf("fenced mail received events = %d; want zero", receivedEvents)
	}

	for _, target := range admitted.GetTargets() {
		if _, err := admin.Exec(`UPDATE session_runtime_inbox SET status='accepted',binding_id=$2,binding_generation=1,target_pod_uid=$3 WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID, podUID); err != nil {
			t.Fatalf("accept authorized close control for %s: %v", target.GetChildThreadId(), err)
		}
		if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
			Scope: scopeForThread(parentScope, target.GetChildThreadId()), RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
			EventIds: []string{target.GetInterruptEventId()}, SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
		}); err != nil {
			t.Fatalf("commit authorized close control for %s: %v", target.GetChildThreadId(), err)
		}
	}
	if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
		Scope: parentScope, RootChildThreadId: childID, SourceToolUseEventId: closeSource.GetSourceToolUseEventId(),
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true, Targets: admitted.GetTargets(),
	}); err != nil {
		t.Fatalf("await authorized close control: %v", err)
	}
	sourceToolID := closeSource.GetSourceToolUseEventId()
	if _, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope: parentScope, ChildThreadId: childID, Source: closeSource, SourceToolUseEventId: &sourceToolID, Targets: admitted.GetTargets(),
	}); err != nil {
		t.Fatalf("mark child closed: %v", err)
	}
	resumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_child_control_work_fence_resume")
	if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{Scope: parentScope, ChildThreadId: childID, Source: resumeSource}); err != nil {
		t.Fatalf("resume child after terminal control: %v", err)
	}
	const resumedDeliveryID = "delivery_child_control_work_fence_resumed"
	seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_child_control_work_fence_resumed_sent",
		nextBridgeAPIEventSequenceForTest(t, admin, sessionID, mainID), "agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, resumedDeliveryID, mainID, childID, "worker", "evt_child_control_work_fence_send_tool", directMessageJSON))
	seedAgentMailCustody(t, admin, sessionID, childID, resumedDeliveryID, time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC))
	resolved, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
		Scope: parentScope, ChildThreadId: childID, DeliveryId: resumedDeliveryID,
	})
	if err != nil || resolved.GetReceivedEventId() == "" {
		t.Fatalf("direct agent mail after control release = %+v, %v; want admitted delivery", resolved, err)
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadVisibilityFollowsRole(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_visibility", "thr_bridge_child_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_visibility", "bind_bridge_child_visibility", 1, "pod_uid_child_visibility")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_child_visibility", "thr_bridge_child_parent", "bind_bridge_child_visibility", 1, "pod_uid_child_visibility")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_visibility", "thr_bridge_child_parent", "evt_bridge_child_public_spawn", 1, "agent.tool_use", `{}`)
	prefixJSON := bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_visibility", "msg_bridge_child_seed", "seed context", "thr_bridge_child_parent", "evt_bridge_child_public_spawn", "all")

	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_parent",
		ChildThreadId:           "thr_bridge_child_public",
		Role:                    "subagent",
		TaskName:                "public child",
		AgentType:               "general",
		SourceToolUseEventId:    "evt_bridge_child_public_spawn",
		ForkTurns:               "all",
		ThreadContextPrefixJson: prefixJSON,
	}); err != nil {
		t.Fatalf("CreateChildThread subagent: %v", err)
	}
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
		ChildThreadId:  "thr_bridge_child_reviewer",
		Role:           "approval_reviewer",
		TaskName:       "reviewer child",
		IsTrunk:        true,
	}); err != nil {
		t.Fatalf("CreateChildThread reviewer: %v", err)
	}

	if got := bridgeThreadVisibility(t, admin, "thr_bridge_child_public"); got != "public" {
		t.Fatalf("subagent visibility = %q; want public", got)
	}
	if got := bridgeThreadVisibility(t, admin, "thr_bridge_child_reviewer"); got != "internal" {
		t.Fatalf("approval reviewer visibility = %q; want internal", got)
	}
	var reviewerIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_visibility'
		    AND id = 'thr_bridge_child_reviewer'`,
	).Scan(&reviewerIsTrunk); err != nil {
		t.Fatalf("read reviewer trunk flag: %v", err)
	}
	if !reviewerIsTrunk {
		t.Fatalf("approval reviewer is_trunk = false; want true")
	}
	successor, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
		ChildThreadId:  "thr_bridge_child_reviewer_successor",
		Role:           "approval_reviewer",
		TaskName:       "reviewer child successor",
		IsTrunk:        true,
	})
	if err != nil {
		t.Fatalf("CreateChildThread reviewer successor: %v", err)
	}
	if successor.GetChildThreadId() != "thr_bridge_child_reviewer_successor" {
		t.Fatalf("reviewer successor child id = %q; want thr_bridge_child_reviewer_successor", successor.GetChildThreadId())
	}
	var predecessorIsTrunk, successorIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND id = 'thr_bridge_child_reviewer'`,
	).Scan(&predecessorIsTrunk); err != nil {
		t.Fatalf("read reviewer predecessor trunk flag: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND id = 'thr_bridge_child_reviewer_successor'`,
	).Scan(&successorIsTrunk); err != nil {
		t.Fatalf("read reviewer successor trunk flag: %v", err)
	}
	if predecessorIsTrunk || !successorIsTrunk {
		t.Fatalf("reviewer succession flags = predecessor:%t successor:%t; want false/true", predecessorIsTrunk, successorIsTrunk)
	}
	var liveTrunks int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_visibility' AND parent_thread_id = 'thr_bridge_child_parent' AND role = 'approval_reviewer' AND is_trunk`,
	).Scan(&liveTrunks); err != nil {
		t.Fatalf("count live reviewer trunks: %v", err)
	}
	if liveTrunks != 1 {
		t.Fatalf("live reviewer trunks = %d; want 1", liveTrunks)
	}

	listed, err := store.ListChildThreads(context.Background(), &bridgev1.ListChildThreadsRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_child_parent",
	})
	if err != nil {
		t.Fatalf("ListChildThreads: %v", err)
	}
	if len(listed.GetThreadJson()) != 1 {
		t.Fatalf("listed child threads = %d; want only the subagent", len(listed.GetThreadJson()))
	}
	var listedThread struct {
		ID   string `json:"session_thread_id"`
		Role string `json:"role"`
	}
	if err := json.Unmarshal([]byte(listed.GetThreadJson()[0]), &listedThread); err != nil {
		t.Fatalf("decode listed child thread: %v", err)
	}
	if listedThread.ID != "thr_bridge_child_public" || listedThread.Role != "subagent" {
		t.Fatalf("listed child thread = %+v; want public subagent", listedThread)
	}
}

func TestValidateChildThreadRequestRejectsInvalidReviewerThreadContextPrefix(t *testing.T) {
	scope := bridgeAPIScope("sesn_bridge_reviewer_validation", "thr_bridge_reviewer_parent", "bind_bridge_reviewer_validation", 1, "pod_uid_reviewer_validation")
	reviewID := "arvw_bridge_reviewer_validation"
	request := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_reviewer_parent",
		ChildThreadId:           approvalReviewerSidecarThreadID(scope, "thr_bridge_reviewer_parent", reviewID),
		Role:                    "approval_reviewer",
		AgentType:               "approval_reviewer",
		ForkTurns:               "all",
		ThreadContextPrefixJson: bridgeReviewerThreadContextPrefixJSON(t, "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", reviewID, nil),
		ReviewerReviewId:        reviewID,
	}

	tests := []struct {
		name       string
		forkTurns  string
		prefixJSON string
	}{
		{
			name:       "zero fork turns",
			forkTurns:  "0",
			prefixJSON: `{"source_parent_thread_id":"thr_bridge_reviewer_parent","review_id":"arvw_bridge_reviewer_validation","fork_turns":"0","runtime_messages_snapshot":[]}`,
		},
		{
			name:       "trailing JSON value",
			forkTurns:  "all",
			prefixJSON: request.GetThreadContextPrefixJson() + `{}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
			candidate.ForkTurns = test.forkTurns
			candidate.ThreadContextPrefixJson = test.prefixJSON
			if err := validateChildThreadRequest(candidate, "approval_reviewer", "approval_reviewer", candidate.GetParentThreadId(), candidate.GetForkTurns()); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("validateChildThreadRequest err = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadEnforcesReviewerTrunkMarker(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reviewer_trunk", "bind_bridge_reviewer_trunk", 1, "pod_uid_reviewer_trunk")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent", "bind_bridge_reviewer_trunk", 1, "pod_uid_reviewer_trunk")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_reviewer_trunk", "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", 1, "user.message", `{"content":[{"type":"text","text":"parent"}]}`)

	trunkRequest := &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_reviewer_parent",
		ChildThreadId:  "thr_bridge_reviewer_trunk",
		Role:           "approval_reviewer",
		TaskName:       "reviewer trunk",
		IsTrunk:        true,
	}
	created, err := store.CreateChildThread(context.Background(), trunkRequest)
	if err != nil {
		t.Fatalf("CreateChildThread reviewer trunk: %v", err)
	}
	if created.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("reviewer trunk ack = %s; want committed", created.GetAck().GetStatus())
	}
	replay, err := store.CreateChildThread(context.Background(), trunkRequest)
	if err != nil {
		t.Fatalf("CreateChildThread reviewer trunk replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("reviewer trunk replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	var replayedIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_trunk'`,
	).Scan(&replayedIsTrunk); err != nil {
		t.Fatalf("read replayed reviewer trunk flag: %v", err)
	}
	if !replayedIsTrunk {
		t.Fatal("same-id reviewer trunk replay demoted the existing trunk")
	}

	secondTrunk := proto.Clone(trunkRequest).(*bridgev1.CreateChildThreadRequest)
	secondTrunk.ChildThreadId = "thr_bridge_reviewer_second_trunk"
	secondTrunk.TaskName = "second reviewer trunk"
	if _, err := store.CreateChildThread(context.Background(), secondTrunk); err != nil {
		t.Fatalf("CreateChildThread reviewer successor: %v", err)
	}
	var predecessorIsTrunk, successorIsTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_trunk'`,
	).Scan(&predecessorIsTrunk); err != nil {
		t.Fatalf("read reviewer predecessor trunk flag: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_trunk' AND id = 'thr_bridge_reviewer_second_trunk'`,
	).Scan(&successorIsTrunk); err != nil {
		t.Fatalf("read reviewer successor trunk flag: %v", err)
	}
	if predecessorIsTrunk || !successorIsTrunk {
		t.Fatalf("reviewer succession flags = predecessor:%t successor:%t; want false/true", predecessorIsTrunk, successorIsTrunk)
	}

	for _, reviewID := range []string{"arvw_bridge_reviewer_sidecar_a", "arvw_bridge_reviewer_sidecar_b"} {
		sidecar := proto.Clone(trunkRequest).(*bridgev1.CreateChildThreadRequest)
		sidecar.ChildThreadId = approvalReviewerSidecarThreadID(scope, "thr_bridge_reviewer_parent", reviewID)
		sidecar.TaskName = sidecar.ChildThreadId
		sidecar.IsTrunk = false
		sidecar.ReviewerReviewId = reviewID
		sidecar.ForkTurns = "all"
		sidecar.ThreadContextPrefixJson = bridgeReviewerThreadContextPrefixJSON(t, "thr_bridge_reviewer_parent", "evt_bridge_reviewer_parent_boundary", reviewID, nil)
		if _, err := store.CreateChildThread(context.Background(), sidecar); err != nil {
			t.Fatalf("CreateChildThread reviewer sidecar %q: %v", sidecar.ChildThreadId, err)
		}

		var isTrunk bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT is_trunk
			   FROM session_threads
			  WHERE workspace_id = 'default'
			    AND session_id = 'sesn_bridge_reviewer_trunk'
			    AND id = $1`,
			sidecar.ChildThreadId,
		).Scan(&isTrunk); err != nil {
			t.Fatalf("read reviewer sidecar %q trunk flag: %v", sidecar.ChildThreadId, err)
		}
		if isTrunk {
			t.Fatalf("reviewer sidecar %q is_trunk = true; want false", sidecar.ChildThreadId)
		}
	}

	invalidSubagent := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_reviewer_parent",
		ChildThreadId:           "thr_bridge_invalid_subagent_trunk",
		Role:                    "subagent",
		TaskName:                "invalid subagent trunk",
		AgentType:               "general",
		SourceToolUseEventId:    "evt_bridge_invalid_subagent_trunk",
		ForkTurns:               "all",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(t, "sesn_bridge_reviewer_trunk", "msg_bridge_invalid_subagent_trunk", "seed context", "thr_bridge_reviewer_parent", "evt_bridge_invalid_subagent_trunk", "all"),
		IsTrunk:                 true,
	}
	if _, err := store.CreateChildThread(context.Background(), invalidSubagent); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("subagent is_trunk err = %v; want InvalidArgument", err)
	}
}

func TestValidateApprovalReviewerSidecarCloseRequiresOutcomeOrCancelledRequestAndQuiescence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_close_proof"
		parentID  = "thr_reviewer_close_parent"
		reviewID  = "arvw_reviewer_close"
		requestID = "mreq_reviewer_close"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	scope := bridgeAPIScope(sessionID, parentID, "bind_reviewer_close", 1, "pod_reviewer_close")
	sidecarID := approvalReviewerSidecarThreadID(scope, parentID, reviewID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, sidecarID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_start", 1, "span.model_request_start",
		`{"type":"span.model_request_start","request_kind":"approval_reviewer"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id=$4
		  WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		"default", sessionID, "evt_reviewer_start", requestID); err != nil {
		t.Fatalf("stamp reviewer request start: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	validate := func() error {
		return store.withScopeTx(context.Background(), scope, "test.reviewer_close_proof", func(tx *dbconnect.Tx) error {
			return validateSettledApprovalReviewerCloseTx(context.Background(), tx, scope, sidecarID, reviewID)
		})
	}
	if err := validate(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("open request close err = %v; want FailedPrecondition", err)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_cancelled", 2, "span.model_request_end",
		`{"type":"span.model_request_end","request_kind":"approval_reviewer","is_error":true,"error_kind":"runtime_interrupted","finish_reason":"cancelled"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id=$4
		  WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		"default", sessionID, "evt_reviewer_cancelled", requestID); err != nil {
		t.Fatalf("stamp reviewer request end: %v", err)
	}
	if err := validate(); err != nil {
		t.Fatalf("cancelled quiescent reviewer close: %v", err)
	}
}

func TestValidateApprovalReviewerSidecarCloseRequiresOneOutcomeOnTheExecutingThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_reviewer_outcome_close"
		parentID  = "thr_reviewer_outcome_parent"
		reviewID  = "arvw_reviewer_outcome"
		requestID = "mreq_reviewer_outcome"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	scope := bridgeAPIScope(sessionID, parentID, "bind_reviewer_outcome", 1, "pod_reviewer_outcome")
	sidecarID := approvalReviewerSidecarThreadID(scope, parentID, reviewID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, parentID, sidecarID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_outcome_start", 1, "span.model_request_start",
		`{"type":"span.model_request_start","request_kind":"approval_reviewer"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id=$4 WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		"default", sessionID, "evt_reviewer_outcome_start", requestID); err != nil {
		t.Fatalf("stamp reviewer request start: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_outcome_decision", 2, "approval_review.decision",
		`{"type":"approval_review.decision","review_id":"`+reviewID+`"}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	validate := func() error {
		return store.withScopeTx(context.Background(), scope, "test.reviewer_outcome_close", func(tx *dbconnect.Tx) error {
			return validateSettledApprovalReviewerCloseTx(context.Background(), tx, scope, sidecarID, reviewID)
		})
	}
	if err := validate(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("outcome with open request close err = %v; want FailedPrecondition", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_outcome_end", 3, "span.model_request_end",
		`{"type":"span.model_request_end","request_kind":"approval_reviewer","is_error":false,"finish_reason":"stop"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id=$4 WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		"default", sessionID, "evt_reviewer_outcome_end", requestID); err != nil {
		t.Fatalf("stamp reviewer request end: %v", err)
	}
	if err := validate(); err != nil {
		t.Fatalf("quiescent single-outcome reviewer close: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, sidecarID, "evt_reviewer_outcome_failure", 4, "approval_review.failure",
		`{"type":"approval_review.failure","review_id":"`+reviewID+`"}`)
	if err := validate(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("competing reviewer outcomes close err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadConcurrentReviewerTrunkReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reviewer_replay_race", "thr_bridge_reviewer_replay_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reviewer_replay_race", "bind_bridge_reviewer_replay_race", 1, "pod_uid_reviewer_replay_race")
	storeA := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	storeB := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_reviewer_replay_race", "thr_bridge_reviewer_replay_parent", "bind_bridge_reviewer_replay_race", 1, "pod_uid_reviewer_replay_race")
	request := &bridgev1.CreateChildThreadRequest{
		Scope:          scope,
		ParentThreadId: "thr_bridge_reviewer_replay_parent",
		ChildThreadId:  "thrd_aprv_replay_race",
		Role:           "approval_reviewer",
		TaskName:       "reviewer trunk",
		IsTrunk:        true,
	}
	blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR UPDATE`,
		"default",
		"sesn_bridge_reviewer_replay_race",
		"thr_bridge_reviewer_replay_parent",
	)

	type createResult struct {
		response *bridgev1.CreateChildThreadResponse
		err      error
	}
	results := make(chan createResult, 2)
	for _, store := range []*PostgreSQLBridgeAPIStore{storeA, storeB} {
		go func() {
			response, err := store.CreateChildThread(context.Background(), proto.Clone(request).(*bridgev1.CreateChildThreadRequest))
			results <- createResult{response: response, err: err}
		}()
	}
	waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release reviewer parent lock: %v", err)
	}

	statuses := map[bridgev1.BridgeWriteStatus]int{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("CreateChildThread concurrent reviewer replay: %v", result.err)
		}
		statuses[result.response.GetAck().GetStatus()]++
	}
	if statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED] != 1 ||
		statuses[bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE] != 1 {
		t.Fatalf("concurrent reviewer replay statuses = %v; want one committed and one duplicate", statuses)
	}
	var isTrunk bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT is_trunk FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_reviewer_replay_race' AND id = 'thrd_aprv_replay_race'`,
	).Scan(&isTrunk); err != nil {
		t.Fatalf("read concurrent reviewer trunk flag: %v", err)
	}
	if !isTrunk {
		t.Fatal("concurrent same-id replay demoted the reviewer trunk")
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadCommitsCreatedEventAndContextPrefix(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_seed", "bind_bridge_child_seed", 1, "pod_uid_child_seed")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("child-seed-load-context-test-key")
	scope := bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "bind_bridge_child_seed", 1, "pod_uid_child_seed")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_spawn", 1, "agent.tool_use", `{}`)
	prefixJSON := bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_seed", "msg_bridge_child_seed_parent", "parent context", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_spawn", "all")
	request := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_seed_parent",
		ChildThreadId:           "thr_bridge_child_seed_worker",
		Role:                    "subagent",
		TaskName:                "seeded worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_seed_spawn",
		ForkTurns:               "all",
		ThreadContextPrefixJson: prefixJSON,
	}
	missingSource := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	missingSource.ChildThreadId = "thr_bridge_child_seed_missing_source"
	missingSource.SourceToolUseEventId = ""
	if _, err := store.CreateChildThread(context.Background(), missingSource); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing source_tool_use_event_id err = %v; want InvalidArgument", err)
	}
	invalidForkTurns := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	invalidForkTurns.ChildThreadId = "thr_bridge_child_seed_bad_fork"
	invalidForkTurns.SourceToolUseEventId = "evt_bridge_child_seed_bad_fork"
	invalidForkTurns.ForkTurns = "0"
	if _, err := store.CreateChildThread(context.Background(), invalidForkTurns); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid fork_turns err = %v; want InvalidArgument", err)
	}
	forbiddenRouting := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	forbiddenRouting.ChildThreadId = "thr_bridge_child_seed_routing"
	forbiddenRouting.ThreadContextPrefixJson = strings.Replace(
		prefixJSON,
		`"role":"user"`,
		`"role":"user","providerId":"openai","modelId":"gpt-5.5"`,
		1,
	)
	if _, err := store.CreateChildThread(context.Background(), forbiddenRouting); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("fork prefix routing metadata err = %v; want InvalidArgument", err)
	}

	response, err := store.CreateChildThread(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateChildThread: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || response.GetChildThreadId() != "thr_bridge_child_seed_worker" {
		t.Fatalf("CreateChildThread response = %+v; want committed child id", response)
	}
	replay, err := store.CreateChildThread(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateChildThread replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetChildThreadId() != response.GetChildThreadId() {
		t.Fatalf("CreateChildThread replay = %+v; want duplicate same child", replay)
	}

	var parentThreadID string
	var role string
	var visibility string
	var statusValue string
	var agentType string
	var taskName string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT parent_thread_id, role, visibility, status, agent_type, task_name
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND id = 'thr_bridge_child_seed_worker'`).Scan(&parentThreadID, &role, &visibility, &statusValue, &agentType, &taskName); err != nil {
		t.Fatalf("read child thread: %v", err)
	}
	if parentThreadID != "thr_bridge_child_seed_parent" || role != "subagent" || visibility != "public" || statusValue != "idle" || agentType != "worker" || taskName != "seeded worker" {
		t.Fatalf("child row = parent=%q role=%q visibility=%q status=%q agentType=%q taskName=%q; want seeded public worker",
			parentThreadID, role, visibility, statusValue, agentType, taskName)
	}

	var createdEventID string
	var createdVisibility string
	var createdSessionVisible bool
	var createdPayloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, visibility, session_visible, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND session_thread_id = 'thr_bridge_child_seed_worker'
		    AND type = 'session.thread_created'`).Scan(&createdEventID, &createdVisibility, &createdSessionVisible, &createdPayloadJSON); err != nil {
		t.Fatalf("read thread_created event: %v", err)
	}
	if createdVisibility != "public" || !createdSessionVisible ||
		testJSONPathString(t, createdPayloadJSON, "session_thread_id") != "thr_bridge_child_seed_worker" ||
		testJSONPathString(t, createdPayloadJSON, "parent_thread_id") != "thr_bridge_child_seed_parent" ||
		testJSONPathString(t, createdPayloadJSON, "agent_type") != "worker" ||
		testJSONPathString(t, createdPayloadJSON, "task_name") != "seeded worker" {
		t.Fatalf("thread_created event projection = visibility %s sessionVisible %v payload %s; want public child metadata", createdVisibility, createdSessionVisible, createdPayloadJSON)
	}
	var streamThreadID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT session_thread_id
		   FROM session_event_stream_changes
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		createdEventID,
	).Scan(&streamThreadID); err != nil {
		t.Fatalf("read thread_created stream change: %v", err)
	}
	if streamThreadID != "thr_bridge_child_seed_worker" {
		t.Fatalf("thread_created stream thread = %q; want child thread", streamThreadID)
	}

	var prefixEntriesJSON string
	var prefixBoundaryEventID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT entries_json, parent_boundary_event_id
		   FROM session_thread_context_prefixes
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_seed'
		    AND child_thread_id = 'thr_bridge_child_seed_worker'`).Scan(&prefixEntriesJSON, &prefixBoundaryEventID); err != nil {
		t.Fatalf("read thread context prefix: %v", err)
	}
	if prefixBoundaryEventID != "evt_bridge_child_seed_spawn" || !strings.Contains(prefixEntriesJSON, "parent context") {
		t.Fatalf("thread context prefix boundary=%q entries=%s; want durable parent boundary and snapshot", prefixBoundaryEventID, prefixEntriesJSON)
	}
	childContext, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_worker", "bind_bridge_child_seed", 1, "pod_uid_child_seed"),
		RuntimeInputId: "rin_bridge_child_seed_load",
		SequenceFrom:   1,
		SequenceTo:     1,
	})
	if err != nil {
		t.Fatalf("LoadContext child context prefix: %v", err)
	}
	var childContextPayload struct {
		Messages            []map[string]any `json:"messages"`
		ThreadContextPrefix *struct {
			ParentBoundaryEventID string           `json:"parentBoundaryEventId"`
			Entries               []map[string]any `json:"entries"`
		} `json:"threadContextPrefix"`
	}
	if err := json.Unmarshal([]byte(childContext.GetContextJson()), &childContextPayload); err != nil {
		t.Fatalf("decode child context: %v", err)
	}
	if len(childContextPayload.Messages) != 0 {
		t.Fatalf("child context messages = %s; want no sequenced fork message", childContext.GetContextJson())
	}
	if childContextPayload.ThreadContextPrefix == nil ||
		childContextPayload.ThreadContextPrefix.ParentBoundaryEventID != "evt_bridge_child_seed_spawn" ||
		len(childContextPayload.ThreadContextPrefix.Entries) != 1 ||
		childContextPayload.ThreadContextPrefix.Entries[0]["id"] != "msg_bridge_child_seed_parent" {
		t.Fatalf("child context = %s; want separate parent context prefix", childContext.GetContextJson())
	}

	emptyPrefixJSON := `{"source_parent_thread_id":"thr_bridge_child_seed_parent","parent_boundary_event_id":"evt_bridge_child_seed_empty_spawn","source_tool_use_event_id":"evt_bridge_child_seed_empty_spawn","fork_turns":"none","runtime_messages_snapshot":[]}`
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_seed", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_empty_spawn", 2, "agent.tool_use", `{}`)
	emptySeedRequest := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	emptySeedRequest.ChildThreadId = "thr_bridge_child_seed_empty"
	emptySeedRequest.TaskName = "empty seed worker"
	emptySeedRequest.SourceToolUseEventId = "evt_bridge_child_seed_empty_spawn"
	emptySeedRequest.ForkTurns = "none"
	emptySeedRequest.ThreadContextPrefixJson = emptyPrefixJSON
	if _, err := store.CreateChildThread(context.Background(), emptySeedRequest); err != nil {
		t.Fatalf("CreateChildThread empty context prefix: %v", err)
	}
	emptyChildContext, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_child_seed", "thr_bridge_child_seed_empty", "bind_bridge_child_seed", 1, "pod_uid_child_seed"),
		RuntimeInputId: "rin_bridge_child_seed_empty_load",
		SequenceFrom:   1,
		SequenceTo:     1,
	})
	if err != nil {
		t.Fatalf("LoadContext empty child context prefix: %v", err)
	}
	var emptyChildContextPayload struct {
		Messages            []map[string]any `json:"messages"`
		ThreadContextPrefix *struct {
			Entries []map[string]any `json:"entries"`
		} `json:"threadContextPrefix"`
	}
	if err := json.Unmarshal([]byte(emptyChildContext.GetContextJson()), &emptyChildContextPayload); err != nil {
		t.Fatalf("decode empty child context: %v", err)
	}
	if len(emptyChildContextPayload.Messages) != 0 {
		t.Fatalf("empty child context = %s; want no sequenced fork message", emptyChildContext.GetContextJson())
	}
	if emptyChildContextPayload.ThreadContextPrefix == nil || len(emptyChildContextPayload.ThreadContextPrefix.Entries) != 0 {
		t.Fatalf("empty child context = %s; want empty separate prefix", emptyChildContext.GetContextJson())
	}

	conflict := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	conflict.ChildThreadId = "thr_bridge_child_seed_other"
	if _, err := store.CreateChildThread(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting source tool use err = %v; want AlreadyExists", err)
	}
	duplicateTask := proto.Clone(request).(*bridgev1.CreateChildThreadRequest)
	duplicateTask.ChildThreadId = "thr_bridge_child_seed_duplicate_task"
	duplicateTask.SourceToolUseEventId = "evt_bridge_child_seed_other_spawn"
	duplicateTask.ThreadContextPrefixJson = bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_seed", "msg_bridge_child_seed_parent", "parent context", "thr_bridge_child_seed_parent", "evt_bridge_child_seed_other_spawn", "all")
	if _, err := store.CreateChildThread(context.Background(), duplicateTask); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate task_name err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCreateChildThreadConcurrentDuplicateTaskName(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_race", "bind_bridge_child_race", 1, "pod_uid_child_race")
	storeA := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	storeB := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_child_race", "thr_bridge_child_race_parent", "bind_bridge_child_race", 1, "pod_uid_child_race")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent", "evt_bridge_child_race_a", 1, "agent.tool_use", `{}`)
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_race", "thr_bridge_child_race_parent", "evt_bridge_child_race_b", 2, "agent.tool_use", `{}`)
	requestA := &bridgev1.CreateChildThreadRequest{
		Scope:                   scope,
		ParentThreadId:          "thr_bridge_child_race_parent",
		ChildThreadId:           "thr_bridge_child_race_a",
		Role:                    "subagent",
		TaskName:                "same worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_race_a",
		ForkTurns:               "none",
		ThreadContextPrefixJson: `{"source_parent_thread_id":"thr_bridge_child_race_parent","parent_boundary_event_id":"evt_bridge_child_race_a","source_tool_use_event_id":"evt_bridge_child_race_a","fork_turns":"none","runtime_messages_snapshot":[]}`,
	}
	requestB := proto.Clone(requestA).(*bridgev1.CreateChildThreadRequest)
	requestB.ChildThreadId = "thr_bridge_child_race_b"
	requestB.SourceToolUseEventId = "evt_bridge_child_race_b"
	requestB.ThreadContextPrefixJson = `{"source_parent_thread_id":"thr_bridge_child_race_parent","parent_boundary_event_id":"evt_bridge_child_race_b","source_tool_use_event_id":"evt_bridge_child_race_b","fork_turns":"none","runtime_messages_snapshot":[]}`

	start := make(chan struct{})
	var wg sync.WaitGroup
	type createResult struct {
		response *bridgev1.CreateChildThreadResponse
		err      error
	}
	results := make(chan createResult, 2)
	for _, item := range []struct {
		store   *PostgreSQLBridgeAPIStore
		request *bridgev1.CreateChildThreadRequest
	}{
		{store: storeA, request: requestA},
		{store: storeB, request: requestB},
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			response, err := item.store.CreateChildThread(context.Background(), item.request)
			results <- createResult{response: response, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	committed := 0
	alreadyExists := 0
	for result := range results {
		if result.err != nil {
			if status.Code(result.err) == codes.AlreadyExists {
				alreadyExists++
				continue
			}
			t.Fatalf("CreateChildThread concurrent err = %v; want committed or AlreadyExists", result.err)
		}
		if result.response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
			t.Fatalf("CreateChildThread concurrent response = %+v; want committed", result.response)
		}
		committed++
	}
	if committed != 1 || alreadyExists != 1 {
		t.Fatalf("concurrent create committed=%d alreadyExists=%d; want 1/1", committed, alreadyExists)
	}
	var childCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_race'
		    AND parent_thread_id = 'thr_bridge_child_race_parent'
		    AND role = 'subagent'
		    AND task_name = 'same worker'`).Scan(&childCount); err != nil {
		t.Fatalf("count child threads: %v", err)
	}
	if childCount != 1 {
		t.Fatalf("childCount = %d; want durable task_name uniqueness", childCount)
	}
}

func TestPostgreSQLBridgeAPIStoreChildThreadStatusEventsStayThreadScoped(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_status", "thr_bridge_child_status_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_status", "bind_bridge_child_status", 1, "pod_uid_child_status")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND id = 'sesn_bridge_child_status'`); err != nil {
		t.Fatalf("seed running public session: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_parent'`); err != nil {
		t.Fatalf("seed running main thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, status_event_id, idle_since, cleanup_after,
			cleanup_enqueued_at, cleanup_claimed_at, cleanup_job_id,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_child_status', 'running', 'evt_child_status_session_running_sentinel', NULL, NULL,
			'2026-01-01T00:00:10Z', '2026-01-01T00:00:11Z', 'qjob_child_status_cleanup_sentinel',
			'bind_child_status_runtime_sentinel', 41, '2026-01-01T00:00:05Z', '2026-01-01T00:00:12Z'
		)`); err != nil {
		t.Fatalf("seed running runtime status sentinel: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	clockNow := time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC)
	store.Clock = func() time.Time { return clockNow }
	parentScope := bridgeAPIScope("sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1, "pod_uid_child_status")
	childScope := bridgeAPIScope("sesn_bridge_child_status", "thr_bridge_child_status_worker", "bind_bridge_child_status", 1, "pod_uid_child_status")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_status", "thr_bridge_child_status_parent", "evt_bridge_child_status_spawn", 1, "agent.tool_use", `{}`)

	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                   parentScope,
		ParentThreadId:          "thr_bridge_child_status_parent",
		ChildThreadId:           "thr_bridge_child_status_worker",
		Role:                    "subagent",
		TaskName:                "status worker",
		AgentType:               "worker",
		SourceToolUseEventId:    "evt_bridge_child_status_spawn",
		ForkTurns:               "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(t, "sesn_bridge_child_status", "msg_bridge_child_status_seed", "seed", "thr_bridge_child_status_parent", "evt_bridge_child_status_spawn", "none"),
	}); err != nil {
		t.Fatalf("CreateChildThread: %v", err)
	}
	running, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          childScope,
		RuntimeWriteId: "rwrite_bridge_child_status_running",
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent child running: %v", err)
	}
	runningReplay, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          childScope,
		RuntimeWriteId: "rwrite_bridge_child_status_running",
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent child running replay: %v", err)
	}
	if runningReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		runningReplay.GetEventId() != running.GetEventId() ||
		runningReplay.GetSequence() != running.GetSequence() {
		t.Fatalf("child running replay = %+v; want duplicate for first event", runningReplay)
	}
	var runningEventCount, runningOperationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
		    (SELECT count(*)
		       FROM session_events
		      WHERE workspace_id = 'default'
		        AND session_id = 'sesn_bridge_child_status'
		        AND session_thread_id = 'thr_bridge_child_status_worker'
		        AND type = 'session.thread_status_running'
		        AND runtime_write_id = 'rwrite_bridge_child_status_running'),
		    (SELECT count(*)
		       FROM session_bridge_operations
		      WHERE workspace_id = 'default'
		        AND session_id = 'sesn_bridge_child_status'
		        AND session_thread_id = 'thr_bridge_child_status_worker'
		        AND operation = 'write_event'
		        AND source_kind = 'session.thread_status_running'
		        AND idempotency_key = 'rwrite_bridge_child_status_running')`,
	).Scan(&runningEventCount, &runningOperationCount); err != nil {
		t.Fatalf("count child running replay rows: %v", err)
	}
	if runningEventCount != 1 || runningOperationCount != 1 {
		t.Fatalf("child running replay rows = event %d operation %d; want 1/1", runningEventCount, runningOperationCount)
	}
	finishIdleRequest := &bridgev1.FinishIdleRequest{
		Scope:          childScope,
		DurableTurnId:  running.GetEventId(),
		StopReasonJson: `{"type":"end_turn"}`,
	}
	finishIdleResponse, err := finishIdleWithStagedCaptureForTest(t, admin, store, finishIdleRequest)
	if err != nil {
		t.Fatalf("FinishIdle child: %v", err)
	}
	if finishIdleResponse.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("FinishIdle child ack = %s; want committed", finishIdleResponse.GetAck().GetStatus())
	}
	finishIdleReplay, err := store.FinishIdle(context.Background(), finishIdleRequest)
	if err != nil {
		t.Fatalf("FinishIdle child replay: %v", err)
	}
	if finishIdleReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("FinishIdle child replay ack = %s; want duplicate", finishIdleReplay.GetAck().GetStatus())
	}
	var runningType string
	var runningPayload string
	var runningThreadID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT type, payload_json, session_thread_id
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND event_id = $1`,
		running.GetEventId(),
	).Scan(&runningType, &runningPayload, &runningThreadID); err != nil {
		t.Fatalf("read child running event: %v", err)
	}
	if runningType != "session.thread_status_running" ||
		runningThreadID != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, runningPayload, "session_thread_id") != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, runningPayload, "task_name") != "status worker" {
		t.Fatalf("child running event = type %q thread %q payload %s; want thread-scoped running", runningType, runningThreadID, runningPayload)
	}
	var idleEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND runtime_write_id = $1`,
		running.GetEventId(),
	).Scan(&idleEventCount); err != nil {
		t.Fatalf("count child idle events: %v", err)
	}
	if idleEventCount != 1 {
		t.Fatalf("child idle event count after replay = %d; want 1", idleEventCount)
	}
	var idleEventID string
	var idlePayload string
	var idleVisibility string
	var idleSessionVisible bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json, visibility, session_visible
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND runtime_write_id = $1`,
		running.GetEventId(),
	).Scan(&idleEventID, &idlePayload, &idleVisibility, &idleSessionVisible); err != nil {
		t.Fatalf("read child idle event: %v", err)
	}
	if idleVisibility != "public" || !idleSessionVisible ||
		testJSONPathString(t, idlePayload, "session_thread_id") != "thr_bridge_child_status_worker" ||
		testJSONPathString(t, idlePayload, "task_name") != "status worker" ||
		testJSONPathString(t, idlePayload, "stop_reason.type") != "end_turn" {
		t.Fatalf("child idle event = visibility %q sessionVisible=%v payload %s; want public session-visible thread-scoped idle", idleVisibility, idleSessionVisible, idlePayload)
	}
	var idleStreamCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes c
		   JOIN session_events e
		     ON e.workspace_id = c.workspace_id
		    AND e.session_id = c.session_id
		    AND e.event_id = c.event_id
		  WHERE c.workspace_id = 'default'
		    AND c.session_id = 'sesn_bridge_child_status'
		    AND c.event_id = $1
		    AND c.session_thread_id = 'thr_bridge_child_status_worker'
		    AND c.revision = 1
		    AND c.visibility = 'public'
		    AND c.session_visible = true
		    AND c.stream_position = e.latest_stream_position`, idleEventID).Scan(&idleStreamCount); err != nil {
		t.Fatalf("count matching child idle stream changes: %v", err)
	}
	if idleStreamCount != 1 {
		t.Fatalf("matching child idle stream changes after replay = %d; want 1", idleStreamCount)
	}
	var threadStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_worker'`).Scan(&threadStatus); err != nil {
		t.Fatalf("read child thread status: %v", err)
	}
	if threadStatus != "idle" {
		t.Fatalf("child thread status = %q; want idle after FinishIdle", threadStatus)
	}
	var sessionIdleEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND type = 'session.status_idle'`).Scan(&sessionIdleEventCount); err != nil {
		t.Fatalf("count session idle events: %v", err)
	}
	if sessionIdleEventCount != 0 {
		t.Fatalf("session.status_idle event count after child FinishIdle = %d; want 0", sessionIdleEventCount)
	}
	var finishIdleOperationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND operation = 'finish_idle'
		    AND source_kind = 'turn_closeout'
		    AND idempotency_key = $1
		    AND runtime_input_id = $1
		    AND ack_status = 'committed'`, running.GetEventId()).Scan(&finishIdleOperationCount); err != nil {
		t.Fatalf("count child finish_idle operations: %v", err)
	}
	if finishIdleOperationCount != 1 {
		t.Fatalf("child finish_idle operation count after replay = %d; want 1", finishIdleOperationCount)
	}
	assertBridgeAPIChildFinishIdlePreservesSessionState(t, admin, "sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1)
	firstCloseSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_close_first",
	)
	clockNow = time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	firstCloseRequest := &bridgev1.MarkChildThreadClosedRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		Source:        firstCloseSource,
	}
	prepareCompletedChildCloseForTest(t, admin, store, firstCloseRequest)
	if _, err := store.MarkChildThreadClosed(context.Background(), firstCloseRequest); err != nil {
		t.Fatalf("MarkChildThreadClosed: %v", err)
	}
	if _, err := store.MarkChildThreadClosed(context.Background(), firstCloseRequest); err != nil {
		t.Fatalf("MarkChildThreadClosed replay: %v", err)
	}
	var closedStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND id = 'thr_bridge_child_status_worker'`).Scan(&closedStatus); err != nil {
		t.Fatalf("read closed child thread status: %v", err)
	}
	if closedStatus != "closed_for_runtime" {
		t.Fatalf("child thread status after close = %q; want closed_for_runtime", closedStatus)
	}
	var closeIdleCount int
	var closeIdlePayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(payload_json)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`).Scan(&closeIdleCount, &closeIdlePayload); err != nil {
		t.Fatalf("read close idle event: %v", err)
	}
	if closeIdleCount != 1 || testJSONPathString(t, closeIdlePayload, "stop_reason.type") != "closed_for_runtime" {
		t.Fatalf("close idle event count/payload = %d/%s; want one closed_for_runtime idle event", closeIdleCount, closeIdlePayload)
	}
	if testJSONPathString(t, closeIdlePayload, "task_name") != "status worker" {
		t.Fatalf("close idle task_name = %s; want callable status worker", closeIdlePayload)
	}
	resumeSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_resume",
	)
	clockNow = time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC)
	if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		Source:        resumeSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadActive after close: %v", err)
	}
	secondCloseSource := seedBridgeAPIChildLifecycleToolSource(
		t,
		admin,
		"sesn_bridge_child_status",
		"thr_bridge_child_status_parent",
		"evt_bridge_child_status_close_second",
	)
	clockNow = time.Date(2026, 1, 1, 0, 3, 0, 0, time.UTC)
	secondCloseRequest := &bridgev1.MarkChildThreadClosedRequest{
		Scope:         parentScope,
		ChildThreadId: "thr_bridge_child_status_worker",
		Source:        secondCloseSource,
	}
	prepareCompletedChildCloseForTest(t, admin, store, secondCloseRequest)
	if _, err := store.MarkChildThreadClosed(context.Background(), secondCloseRequest); err != nil {
		t.Fatalf("MarkChildThreadClosed after resume: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND session_thread_id = 'thr_bridge_child_status_worker'
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`).Scan(&closeIdleCount); err != nil {
		t.Fatalf("read close idle event count after resume: %v", err)
	}
	if closeIdleCount != 2 {
		t.Fatalf("close idle event count after resume = %d; want a new closed_for_runtime idle event", closeIdleCount)
	}
	assertBridgeAPIChildFinishIdlePreservesSessionState(t, admin, "sesn_bridge_child_status", "thr_bridge_child_status_parent", "bind_bridge_child_status", 1)
	var sessionLevelStatusEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_status'
		    AND type IN ('session.status_running', 'session.status_idle')`).Scan(&sessionLevelStatusEventCount); err != nil {
		t.Fatalf("read session-level status count: %v", err)
	}
	if sessionLevelStatusEventCount != 0 {
		t.Fatalf("session-level status event count = %d; want only thread status events for child", sessionLevelStatusEventCount)
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadClosedCascadesAcrossDescendants(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID    = "sesn_bridge_close_tree"
		mainID       = "thr_bridge_close_tree_main"
		childID      = "thr_bridge_close_tree_child"
		grandchildID = "thr_bridge_close_tree_grandchild"
		bindingID    = "bind_bridge_close_tree"
		podUID       = "pod_bridge_close_tree"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, grandchildID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 6, 0, time.UTC) }
	closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_close_tree_command")
	closeRequest := &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		Source:        closeSource,
	}
	prepareCompletedChildCloseForTest(t, admin, store, closeRequest)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, childID, "rin_close_tree_message", "messages", `["evt_close_tree_message"]`, "accepted", bindingID, podUID, 1, 1)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: childID,
		RuntimeInputID: "rin_close_tree_queued", InputKind: "messages",
		EventIDs: []string{"evt_close_tree_queued"}, SequenceFrom: 2, SequenceTo: 2,
	})
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, grandchildID, "task_notification:close_tree", "task_notification", `[]`, "accepted", bindingID, podUID, 0, 0)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	for _, input := range []runtimePodLostAcceptedInput{
		{SessionThreadID: childID, RuntimeInputID: "rin_close_tree_message", InputKind: "messages", EventIDsJSON: `["evt_close_tree_message"]`, SequenceFrom: sql.NullInt64{Int64: 1, Valid: true}, SequenceTo: sql.NullInt64{Int64: 1, Valid: true}},
		{SessionThreadID: childID, RuntimeInputID: "rin_close_tree_queued", InputKind: "messages", EventIDsJSON: `["evt_close_tree_queued"]`, SequenceFrom: sql.NullInt64{Int64: 2, Valid: true}, SequenceTo: sql.NullInt64{Int64: 2, Valid: true}},
		{SessionThreadID: grandchildID, RuntimeInputID: "task_notification:close_tree", InputKind: "task_notification", EventIDsJSON: `[]`},
	} {
		request, err := lostRuntimeInputEnqueueRequest("default", sessionID, input, store.Clock())
		if err != nil {
			t.Fatalf("build close-time Queue custody: %v", err)
		}
		if _, err := queueStore.Enqueue(context.Background(), request); err != nil {
			t.Fatalf("enqueue close-time Queue custody: %v", err)
		}
	}
	response, err := store.MarkChildThreadClosed(context.Background(), closeRequest)
	if err != nil {
		t.Fatalf("MarkChildThreadClosed tree: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("close tree ack = %s; want committed", response.GetAck().GetStatus())
	}
	for runtimeInputID, wantStatus := range map[string]string{
		"rin_close_tree_message":       "cancelled",
		"rin_close_tree_queued":        "cancelled",
		"task_notification:close_tree": "parked",
	} {
		var inboxStatus, queueStatus string
		if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status, job.status
			FROM session_runtime_inbox inbox
			JOIN queue_jobs job
			  ON job.workspace_id = inbox.workspace_id
			 AND job.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
			WHERE inbox.workspace_id = 'default' AND inbox.runtime_input_id = $1`, runtimeInputID).Scan(&inboxStatus, &queueStatus); err != nil {
			t.Fatalf("read child-close input %s: %v", runtimeInputID, err)
		}
		if inboxStatus != wantStatus || queueStatus != queue.StatusCancelled {
			t.Fatalf("child-close input %s = inbox %q / Queue %q; want %q / cancelled", runtimeInputID, inboxStatus, queueStatus, wantStatus)
		}
	}
	if len(response.GetDeclaration().GetReceipts()) != 2 {
		t.Fatalf("close tree receipts = %d; want one per target", len(response.GetDeclaration().GetReceipts()))
	}
	receiptTargets := map[string]string{}
	for _, receipt := range response.GetDeclaration().GetReceipts() {
		if len(receipt.GetChildLifecycle()) != 1 {
			t.Fatalf("close tree receipt %q lifecycle stamps = %d; want 1", receipt.GetSessionThreadId(), len(receipt.GetChildLifecycle()))
		}
		stamp := receipt.GetChildLifecycle()[0]
		if stamp.GetChildThreadId() != receipt.GetSessionThreadId() {
			t.Fatalf("close tree receipt target = %q/%q; want matching thread scope", receipt.GetSessionThreadId(), stamp.GetChildThreadId())
		}
		if stamp.GetEffectiveAt() != "2026-01-01T00:00:06Z" {
			t.Fatalf("close tree receipt effective_at = %q; want original declaration bytes", stamp.GetEffectiveAt())
		}
		receiptTargets[receipt.GetSessionThreadId()] = stamp.GetDisposition().String()
	}
	if receiptTargets[childID] != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED.String() ||
		receiptTargets[grandchildID] != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED.String() {
		t.Fatalf("close tree receipt targets = %#v; want both target-scoped closed receipts", receiptTargets)
	}
	var operationScopes []string
	operationRows, err := admin.QueryContext(context.Background(),
		`SELECT session_thread_id
		   FROM session_bridge_operations
		  WHERE workspace_id='default'
		    AND session_id=$1
		    AND operation='mark_child_thread_closed'
		  ORDER BY session_thread_id`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("read close tree declaration operations: %v", err)
	}
	defer func() { _ = operationRows.Close() }()
	for operationRows.Next() {
		var threadID string
		if err := operationRows.Scan(&threadID); err != nil {
			t.Fatalf("scan close tree declaration operation: %v", err)
		}
		operationScopes = append(operationScopes, threadID)
	}
	if err := operationRows.Err(); err != nil {
		t.Fatalf("iterate close tree declaration operations: %v", err)
	}
	if len(operationScopes) != 2 || operationScopes[0] != childID || operationScopes[1] != grandchildID {
		t.Fatalf("close tree operation scopes = %v; want [%s %s]", operationScopes, childID, grandchildID)
	}
	replay, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		Source:        closeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadClosed tree replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replay.GetDeclaration()) {
		t.Fatalf("close tree replay = %+v; want duplicate with exact stored receipts", replay)
	}
	rows, err := admin.QueryContext(context.Background(),
		`SELECT id, status FROM session_threads
		  WHERE workspace_id='default' AND session_id=$1 AND id IN ($2, $3)
		  ORDER BY id`,
		sessionID, childID, grandchildID,
	)
	if err != nil {
		t.Fatalf("read closed child tree: %v", err)
	}
	defer func() { _ = rows.Close() }()
	statuses := map[string]string{}
	for rows.Next() {
		var threadID, threadStatus string
		if err := rows.Scan(&threadID, &threadStatus); err != nil {
			t.Fatalf("scan closed child tree: %v", err)
		}
		statuses[threadID] = threadStatus
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate closed child tree: %v", err)
	}
	if statuses[childID] != "closed_for_runtime" || statuses[grandchildID] != "closed_for_runtime" {
		t.Fatalf("closed child tree statuses = %#v; want both closed_for_runtime", statuses)
	}
	var closedEvents, completionMail int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT
		   count(*) FILTER (WHERE type='session.thread_status_idle' AND payload_json::jsonb #>> '{stop_reason,type}'='closed_for_runtime'),
		   count(*) FILTER (WHERE type='agent.thread_message_sent')
		 FROM session_events
		 WHERE workspace_id='default' AND session_id=$1`,
		sessionID,
	).Scan(&closedEvents, &completionMail); err != nil {
		t.Fatalf("count closed child tree events: %v", err)
	}
	if closedEvents != 2 || completionMail != 0 {
		t.Fatalf("closed child tree events/mail = %d/%d; want 2/0", closedEvents, completionMail)
	}
	if _, err := store.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope:          bridgeAPIScope(sessionID, grandchildID, bindingID, 1, podUID),
		DurableTurnId:  "evt_bridge_close_tree_grandchild_running",
		StopReasonJson: `{"type":"end_turn"}`,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late FinishIdle after descendant close = %v; want FailedPrecondition", err)
	}
	var lateStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		grandchildID,
	).Scan(&lateStatus); err != nil {
		t.Fatalf("read descendant status after late FinishIdle: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
		sessionID,
	).Scan(&completionMail); err != nil {
		t.Fatalf("count completion mail after late FinishIdle: %v", err)
	}
	if lateStatus != "closed_for_runtime" || completionMail != 0 {
		t.Fatalf("late FinishIdle status/mail = %q/%d; want closed_for_runtime/0", lateStatus, completionMail)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'idle', closed_at = NULL
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("reopen child before topology-change replay: %v", err)
	}
	const laterChildID = "thr_bridge_close_tree_later_child"
	seedBridgeAPIChildThread(t, admin, "default", sessionID, childID, laterChildID)
	replayAfterTopologyChange, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: childID,
		Source:        closeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadClosed replay after topology change: %v", err)
	}
	if replayAfterTopologyChange.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replayAfterTopologyChange.GetDeclaration()) {
		t.Fatalf("close tree replay after topology change = %+v; want original duplicate receipt set", replayAfterTopologyChange)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'closed_for_runtime', closed_at = '2026-01-01T00:00:05Z'
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("restore closed parent after topology-change replay: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "sevt_bridge_close_tree_late_spawn", nextBridgeAPIEventSequenceForTest(t, admin, sessionID, childID), "agent.tool_use", `{}`)
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ParentThreadId:       childID,
		ChildThreadId:        "thr_bridge_close_tree_late_child",
		Role:                 "subagent",
		TaskName:             "late-child",
		AgentType:            "worker",
		SourceToolUseEventId: "sevt_bridge_close_tree_late_spawn",
		ForkTurns:            "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
			t,
			sessionID,
			"msg_bridge_close_tree_late_seed",
			"late seed",
			childID,
			"sevt_bridge_close_tree_late_spawn",
			"none",
		),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create below already-closed parent err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadClosedPreservesTerminalTargetsInFrozenSubtree(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID         = "sesn_bridge_close_terminal_tree"
		mainID            = "thr_bridge_close_terminal_tree_main"
		failedRootID      = "thr_bridge_close_terminal_tree_failed"
		terminatedChildID = "thr_bridge_close_terminal_tree_terminated"
		runningGrandID    = "thr_bridge_close_terminal_tree_running"
		closedSiblingID   = "thr_bridge_close_terminal_tree_closed"
		bindingID         = "bind_bridge_close_terminal_tree"
		podUID            = "pod_bridge_close_terminal_tree"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, failedRootID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, failedRootID, terminatedChildID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, terminatedChildID, runningGrandID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, failedRootID, closedSiblingID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = CASE id
		      WHEN $2 THEN 'failed'
		      WHEN $3 THEN 'terminated'
		      WHEN $4 THEN 'running'
		      WHEN $5 THEN 'closed_for_runtime'
		    END,
		        closed_at = CASE WHEN id = $5 THEN '2026-01-01T00:00:01Z'::timestamptz ELSE NULL END
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id IN ($2, $3, $4, $5)`,
		sessionID,
		failedRootID,
		terminatedChildID,
		runningGrandID,
		closedSiblingID,
	); err != nil {
		t.Fatalf("seed mixed child subtree statuses: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 6, 0, time.UTC) }
	closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_close_terminal_tree")
	request := &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: failedRootID,
		Source:        closeSource,
	}
	prepareCompletedChildCloseForTest(t, admin, store, request)
	response, err := store.MarkChildThreadClosed(context.Background(), request)
	if err != nil {
		t.Fatalf("MarkChildThreadClosed mixed terminal tree: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("close mixed terminal tree ack = %s; want committed", response.GetAck().GetStatus())
	}
	wantDispositions := map[string]bridgev1.ChildLifecycleDisposition{
		failedRootID:      bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED,
		terminatedChildID: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED,
		runningGrandID:    bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED,
		closedSiblingID:   bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED,
	}
	gotDispositions := make(map[string]bridgev1.ChildLifecycleDisposition, len(wantDispositions))
	for _, receipt := range response.GetDeclaration().GetReceipts() {
		if len(receipt.GetChildLifecycle()) != 1 {
			t.Fatalf("close mixed terminal tree receipt %q lifecycle stamps = %d; want 1", receipt.GetSessionThreadId(), len(receipt.GetChildLifecycle()))
		}
		gotDispositions[receipt.GetSessionThreadId()] = receipt.GetChildLifecycle()[0].GetDisposition()
	}
	if !reflect.DeepEqual(gotDispositions, wantDispositions) {
		t.Fatalf("close mixed terminal tree dispositions = %#v; want %#v", gotDispositions, wantDispositions)
	}
	rows, err := admin.QueryContext(context.Background(),
		`SELECT id, status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id IN ($2, $3, $4, $5)`,
		sessionID,
		failedRootID,
		terminatedChildID,
		runningGrandID,
		closedSiblingID,
	)
	if err != nil {
		t.Fatalf("read mixed terminal tree statuses: %v", err)
	}
	defer func() { _ = rows.Close() }()
	gotStatuses := map[string]string{}
	for rows.Next() {
		var threadID, threadStatus string
		if err := rows.Scan(&threadID, &threadStatus); err != nil {
			t.Fatalf("scan mixed terminal tree status: %v", err)
		}
		gotStatuses[threadID] = threadStatus
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate mixed terminal tree statuses: %v", err)
	}
	wantStatuses := map[string]string{
		failedRootID:      "failed",
		terminatedChildID: "terminated",
		runningGrandID:    "closed_for_runtime",
		closedSiblingID:   "closed_for_runtime",
	}
	if !reflect.DeepEqual(gotStatuses, wantStatuses) {
		t.Fatalf("close mixed terminal tree statuses = %#v; want %#v", gotStatuses, wantStatuses)
	}
	var closedEvents int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND type = 'session.thread_status_idle'
		    AND payload_json::jsonb #>> '{stop_reason,type}' = 'closed_for_runtime'`,
		sessionID,
	).Scan(&closedEvents); err != nil {
		t.Fatalf("count mixed terminal tree close events: %v", err)
	}
	if closedEvents != 1 {
		t.Fatalf("mixed terminal tree close events = %d; want only the running descendant transition", closedEvents)
	}
	replay, err := store.MarkChildThreadClosed(context.Background(), request)
	if err != nil {
		t.Fatalf("MarkChildThreadClosed mixed terminal tree replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(response.GetDeclaration(), replay.GetDeclaration()) {
		t.Fatalf("close mixed terminal tree replay = %+v; want exact duplicate declaration", replay)
	}
	const escapedChildID = "thr_bridge_close_terminal_tree_escaped"
	seedBridgeAPIEvent(t, admin, "default", sessionID, failedRootID, "sevt_bridge_close_terminal_tree_late_spawn", 2, "agent.tool_use", `{}`)
	if _, err := store.CreateChildThread(context.Background(), &bridgev1.CreateChildThreadRequest{
		Scope:                bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ParentThreadId:       failedRootID,
		ChildThreadId:        escapedChildID,
		Role:                 "subagent",
		TaskName:             "escaped-child",
		AgentType:            "worker",
		SourceToolUseEventId: "sevt_bridge_close_terminal_tree_late_spawn",
		ForkTurns:            "none",
		ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
			t,
			sessionID,
			"msg_bridge_close_terminal_tree_late_seed",
			"late seed",
			failedRootID,
			"sevt_bridge_close_terminal_tree_late_spawn",
			"none",
		),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create below preserved failed root err = %v; want FailedPrecondition", err)
	}
	var escapedChildCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_threads
		  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID,
		escapedChildID,
	).Scan(&escapedChildCount); err != nil {
		t.Fatalf("count child below preserved failed root: %v", err)
	}
	if escapedChildCount != 0 {
		t.Fatalf("children created below preserved failed root = %d; want 0", escapedChildCount)
	}
}

func TestPostgreSQLBridgeAPIStoreCloseAndConcurrentChildCreateSerializeAtTheParent(t *testing.T) {
	type operationResult struct {
		operation string
		err       error
	}
	for _, test := range []struct {
		name        string
		createFirst bool
	}{
		{name: "create first is included in close", createFirst: true},
		{name: "close first rejects create", createFirst: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_bridge_close_create_" + suffix
			mainID := "thr_bridge_close_create_main_" + suffix
			parentID := "thr_bridge_close_create_parent_" + suffix
			createdID := "thr_bridge_close_create_new_" + suffix
			bindingID := "bind_bridge_close_create_" + suffix
			podUID := "pod_bridge_close_create_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, mainID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "sevt_bridge_close_create_"+suffix, 1, "agent.tool_use", `{}`)
			seedBridgeAPIEvent(t, admin, "default", sessionID, parentID, "sevt_bridge_close_create_result_"+suffix, 2, "agent.tool_result", fmt.Sprintf(`{"type":"agent.tool_result","tool_use_id":%q,"content":[]}`, "sevt_bridge_close_create_"+suffix))
			scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
			createRequest := &bridgev1.CreateChildThreadRequest{
				Scope:                scope,
				ParentThreadId:       parentID,
				ChildThreadId:        createdID,
				Role:                 "subagent",
				TaskName:             "concurrent-child",
				AgentType:            "worker",
				SourceToolUseEventId: "sevt_bridge_close_create_" + suffix,
				ForkTurns:            "none",
				ThreadContextPrefixJson: bridgeThreadContextPrefixJSON(
					t,
					sessionID,
					"msg_bridge_close_create_seed_"+suffix,
					"concurrent seed",
					parentID,
					"sevt_bridge_close_create_"+suffix,
					"none",
				),
			}
			closeRequest := &bridgev1.MarkChildThreadClosedRequest{
				Scope:         scope,
				ChildThreadId: parentID,
				Source: seedBridgeAPIChildLifecycleToolSource(
					t,
					admin,
					sessionID,
					mainID,
					"evt_bridge_close_create_command_"+suffix,
				),
			}
			blocker, blockerPID := lockPostgreSQLFinalizationFence(t, admin,
				`SELECT id FROM session_threads
				  WHERE workspace_id=$1 AND session_id=$2 AND id=$3
				  FOR UPDATE`,
				"default",
				sessionID,
				parentID,
			)
			results := make(chan operationResult, 2)
			startCreate := func() {
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				go func() {
					_, err := store.CreateChildThread(context.Background(), createRequest)
					results <- operationResult{operation: "create", err: err}
				}()
			}
			startClose := func() {
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				go func() {
					sourceID := closeRequest.GetSource().GetSourceToolUseEventId()
					admitted, err := store.AdmitChildInterrupt(context.Background(), &bridgev1.AdmitChildInterruptRequest{
						Scope: scope, RootChildThreadId: parentID, SourceToolUseEventId: sourceID,
						Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true,
					})
					if err == nil {
						for _, target := range admitted.GetTargets() {
							if target.GetRuntimeInputId() == "" {
								continue
							}
							_, err = admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox
								SET status='accepted', binding_id=$2, binding_generation=$3, target_pod_uid=$4
								WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID, int64(1), podUID)
							if err != nil {
								break
							}
							_, err = store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
								Scope:          scopeForThread(scope, target.GetChildThreadId()),
								RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
								EventIds:     []string{target.GetInterruptEventId()},
								SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
							})
							if err != nil {
								break
							}
						}
					}
					if err == nil {
						closeRequest.SourceToolUseEventId = &sourceID
						closeRequest.Targets = admitted.GetTargets()
						_, err = store.MarkChildThreadClosed(context.Background(), closeRequest)
					}
					results <- operationResult{operation: "close", err: err}
				}()
			}
			if test.createFirst {
				startCreate()
			} else {
				startClose()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
			if test.createFirst {
				startClose()
			} else {
				startCreate()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
			if err := blocker.Commit(); err != nil {
				t.Fatalf("release close/create parent lock: %v", err)
			}

			outcomes := map[string]error{}
			for range 2 {
				outcome := <-results
				outcomes[outcome.operation] = outcome.err
			}
			if outcomes["close"] != nil {
				t.Fatalf("concurrent close: %v", outcomes["close"])
			}
			if test.createFirst {
				if outcomes["create"] != nil {
					t.Fatalf("create-first concurrent create: %v", outcomes["create"])
				}
				var parentStatus, createdStatus string
				if err := admin.QueryRowContext(context.Background(),
					`SELECT parent.status, child.status
					   FROM session_threads parent
					   JOIN session_threads child
					     ON child.workspace_id=parent.workspace_id
					    AND child.session_id=parent.session_id
					    AND child.parent_thread_id=parent.id
					  WHERE parent.workspace_id='default'
					    AND parent.session_id=$1
					    AND parent.id=$2
					    AND child.id=$3`,
					sessionID,
					parentID,
					createdID,
				).Scan(&parentStatus, &createdStatus); err != nil {
					t.Fatalf("read create-first close result: %v", err)
				}
				if parentStatus != "closed_for_runtime" || createdStatus != "closed_for_runtime" {
					t.Fatalf("create-first statuses = %q/%q; want both closed_for_runtime", parentStatus, createdStatus)
				}
				return
			}
			if status.Code(outcomes["create"]) != codes.FailedPrecondition {
				t.Fatalf("close-first concurrent create err = %v; want FailedPrecondition", outcomes["create"])
			}
			var createdCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_threads
				  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
				sessionID,
				createdID,
			).Scan(&createdCount); err != nil {
				t.Fatalf("count close-first child: %v", err)
			}
			if createdCount != 0 {
				t.Fatalf("close-first created rows = %d; want 0", createdCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadActiveOnlyReopensClosedThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_child_resume_guard"
		mainID    = "thr_bridge_child_resume_guard_main"
		childID   = "thr_bridge_child_resume_guard_child"
		bindingID = "bind_bridge_child_resume_guard"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_bridge_child_resume_guard")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'running' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child running: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_uid_bridge_child_resume_guard")
	firstResumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_child_resume_guard_first")
	activeResponse, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope: scope, ChildThreadId: childID,
		Source: firstResumeSource,
	})
	if err != nil {
		t.Fatalf("MarkChildThreadActive running child: %v", err)
	}
	if len(activeResponse.GetDeclaration().GetReceipts()) != 1 ||
		activeResponse.GetDeclaration().GetReceipts()[0].GetSessionThreadId() != childID ||
		activeResponse.GetDeclaration().GetReceipts()[0].GetChildLifecycle()[0].GetDisposition() != bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE {
		t.Fatalf("already-active receipt = %+v; want one target-scoped ALREADY_ACTIVE receipt", activeResponse.GetDeclaration())
	}
	var childStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID).Scan(&childStatus); err != nil {
		t.Fatalf("read running child status: %v", err)
	}
	if childStatus != "running" {
		t.Fatalf("running child status after resume = %q; want running", childStatus)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status = 'closed_for_runtime' WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID); err != nil {
		t.Fatalf("mark child closed_for_runtime: %v", err)
	}
	secondResumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_child_resume_guard_second")
	if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
		Scope: scope, ChildThreadId: childID,
		Source: secondResumeSource,
	}); err != nil {
		t.Fatalf("MarkChildThreadActive closed child: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
		sessionID, childID).Scan(&childStatus); err != nil {
		t.Fatalf("read reopened child status: %v", err)
	}
	if childStatus != "idle" {
		t.Fatalf("closed child status after resume = %q; want idle", childStatus)
	}
}

func TestPostgreSQLTaskNotificationAndChildCloseSerializeAtSessionBoundary(t *testing.T) {
	for _, admissionFirst := range []bool{false, true} {
		name := "notification_commit_first"
		if admissionFirst {
			name = "close_admission_first"
		}
		t.Run(name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			const (
				sessionID = "sesn_notification_close_race"
				parentID  = "thr_notification_close_parent"
				childID   = "thr_notification_close_child"
				bindingID = "bind_notification_close"
				taskID    = "task_notification_close"
				inputID   = "task_notification:task_notification_close"
			)
			seedBridgeAPISession(t, admin, "default", sessionID, parentID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_notification_close")
			seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, childID, bindingID, taskID, "evt_task_source")
			resultJSON := `{"task_id":"task_notification_close","source_tool_use_event_id":"evt_task_source","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
			settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", resultJSON)
			seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, childID, inputID, bindingID, "pod_notification_close")
			closeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_notification_close")
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			parentScope := bridgeAPIScope(sessionID, parentID, bindingID, 1, "pod_notification_close")
			childScope := scopeForThread(parentScope, childID)
			deliveredResultJSON, err := canonicalTaskNotificationPayloadJSON(taskID, "evt_task_source", "completed", resultJSON)
			if err != nil {
				t.Fatalf("build delivered task notification: %v", err)
			}
			commitRequest := bridgeTaskNotificationRequestForTest(t, childScope, inputID, taskID, deliveredResultJSON)
			admitRequest := &bridgev1.AdmitChildInterruptRequest{
				Scope: parentScope, RootChildThreadId: childID,
				SourceToolUseEventId: closeSource.GetSourceToolUseEventId(),
				Action:               bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
				IncludeDescendants:   true,
			}

			blocker, err := admin.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatalf("begin Session lock: %v", err)
			}
			defer func() { _ = blocker.Rollback() }()
			var locked string
			if err := blocker.QueryRow(`SELECT id FROM sessions WHERE workspace_id='default' AND id=$1 FOR UPDATE`, sessionID).Scan(&locked); err != nil {
				t.Fatalf("lock Session: %v", err)
			}
			var blockerPID int
			if err := blocker.QueryRow(`SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
				t.Fatalf("read blocker pid: %v", err)
			}
			type outcome struct {
				kind     string
				commit   *bridgev1.CommitTaskNotificationResultResponse
				admitted *bridgev1.AdmitChildInterruptResponse
				err      error
			}
			results := make(chan outcome, 2)
			startCommit := func() {
				go func() {
					response, err := store.CommitTaskNotificationResult(context.Background(), commitRequest)
					results <- outcome{kind: "commit", commit: response, err: err}
				}()
			}
			startAdmission := func() {
				go func() {
					response, err := store.AdmitChildInterrupt(context.Background(), admitRequest)
					results <- outcome{kind: "admit", admitted: response, err: err}
				}()
			}
			if admissionFirst {
				startAdmission()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startCommit()
			} else {
				startCommit()
				waitForPostgreSQLLockWaiters(t, admin, blockerPID, 1)
				startAdmission()
			}
			waitForPostgreSQLLockWaiters(t, admin, blockerPID, 2)
			if err := blocker.Commit(); err != nil {
				t.Fatalf("release Session lock: %v", err)
			}
			var committed *bridgev1.CommitTaskNotificationResultResponse
			var admitted *bridgev1.AdmitChildInterruptResponse
			for range 2 {
				result := <-results
				if result.err != nil {
					t.Fatalf("%s race operation: %v", result.kind, result.err)
				}
				if result.kind == "commit" {
					committed = result.commit
				} else {
					admitted = result.admitted
				}
			}
			if admitted == nil || len(admitted.GetTargets()) != 1 {
				t.Fatalf("close admission = %#v; want one target", admitted)
			}
			if !admissionFirst {
				if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
					t.Fatalf("notification-first ack = %#v; want committed", committed.GetAck())
				}
				var inboxStatus string
				var notificationEvents, notificationMessages, rejectedOperations int
				if err := admin.QueryRow(`SELECT
					(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
					(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='runtime_notification'),
					(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2 AND kind='runtime_notification'),
					(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$2
					  AND operation='commit_task_notification_result' AND ack_status='rejected')`, inputID, sessionID,
				).Scan(&inboxStatus, &notificationEvents, &notificationMessages, &rejectedOperations); err != nil {
					t.Fatalf("read notification-first facts: %v", err)
				}
				if inboxStatus != "committed" || notificationEvents != 1 || notificationMessages != 1 || rejectedOperations != 0 {
					t.Fatalf("notification-first facts = Inbox:%s Events:%d Messages:%d rejectedOps:%d", inboxStatus, notificationEvents, notificationMessages, rejectedOperations)
				}
				return
			}
			if committed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || committed.GetAck().GetErrorCode() != "task_notification_deferred" {
				t.Fatalf("admission-first notification ack = %#v; want deferred", committed.GetAck())
			}
			target := admitted.GetTargets()[0]
			if _, err := admin.Exec(`UPDATE session_runtime_inbox
				SET status='accepted', binding_id=$2, binding_generation=1, target_pod_uid='pod_notification_close'
				WHERE workspace_id='default' AND runtime_input_id=$1`, target.GetRuntimeInputId(), bindingID); err != nil {
				t.Fatalf("accept close control: %v", err)
			}
			if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
				Scope: childScope, RuntimeInputId: target.GetRuntimeInputId(), InputKind: "interrupt_control",
				EventIds: []string{target.GetInterruptEventId()}, SequenceFrom: target.GetInterruptEventSequence(), SequenceTo: target.GetInterruptEventSequence(),
			}); err != nil {
				t.Fatalf("commit close control: %v", err)
			}
			if _, err := store.AwaitChildInterrupt(context.Background(), &bridgev1.AwaitChildInterruptRequest{
				Scope: parentScope, RootChildThreadId: childID, SourceToolUseEventId: closeSource.GetSourceToolUseEventId(),
				Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE, IncludeDescendants: true, Targets: admitted.GetTargets(),
			}); err != nil {
				t.Fatalf("await close control: %v", err)
			}
			closeSourceID := closeSource.GetSourceToolUseEventId()
			if _, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
				Scope: parentScope, ChildThreadId: childID, Source: closeSource,
				SourceToolUseEventId: &closeSourceID, Targets: admitted.GetTargets(),
			}); err != nil {
				t.Fatalf("mark child closed: %v", err)
			}
			resumeSource := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, parentID, "evt_notification_resume")
			if _, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
				Scope: parentScope, ChildThreadId: childID, Source: resumeSource,
			}); err != nil {
				t.Fatalf("resume child: %v", err)
			}
			var inboxStatus string
			var queuedJobs, notificationEvents, notificationMessages, rejectedOperations int
			if err := admin.QueryRow(`SELECT
				(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
				(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND kind=$2 AND payload_json::jsonb ->> 'runtime_input_id'=$1),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$3 AND type='runtime_notification'),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$3 AND kind='runtime_notification'),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$3
				  AND operation='commit_task_notification_result' AND ack_status='rejected')`, inputID, queue.KindRuntimeInput, sessionID,
			).Scan(&inboxStatus, &queuedJobs, &notificationEvents, &notificationMessages, &rejectedOperations); err != nil {
				t.Fatalf("read resumed notification facts: %v", err)
			}
			if inboxStatus != "queued" || queuedJobs != 1 || notificationEvents != 0 || notificationMessages != 0 || rejectedOperations != 0 {
				t.Fatalf("resumed notification = Inbox:%s jobs:%d Events:%d Messages:%d rejectedOps:%d", inboxStatus, queuedJobs, notificationEvents, notificationMessages, rejectedOperations)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreMarkChildThreadActivePreservesTerminalThread(t *testing.T) {
	for _, testCase := range []struct {
		status      string
		disposition bridgev1.ChildLifecycleDisposition
	}{
		{status: "failed", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED},
		{status: "terminated", disposition: bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED},
	} {
		t.Run(testCase.status, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_resume_" + testCase.status
			mainID := "thr_bridge_resume_" + testCase.status + "_main"
			childID := "thr_bridge_resume_" + testCase.status + "_child"
			bindingID := "bind_bridge_resume_" + testCase.status
			seedBridgeAPISession(t, admin, "default", sessionID, mainID)
			seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_bridge_resume_terminal")
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_threads
				    SET status = $3
				  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
				sessionID,
				childID,
				testCase.status,
			); err != nil {
				t.Fatalf("mark child %s: %v", testCase.status, err)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_resume_"+testCase.status)
			response, err := store.MarkChildThreadActive(context.Background(), &bridgev1.MarkChildThreadActiveRequest{
				Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_bridge_resume_terminal"),
				ChildThreadId: childID,
				Source:        source,
			})
			if err != nil {
				t.Fatalf("MarkChildThreadActive %s child: %v", testCase.status, err)
			}
			stamps := response.GetDeclaration().GetReceipts()[0].GetChildLifecycle()
			if len(stamps) != 1 || stamps[0].GetDisposition() != testCase.disposition {
				t.Fatalf("resume %s disposition = %+v; want %s", testCase.status, stamps, testCase.disposition)
			}
			var childStatus string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_threads
				  WHERE workspace_id = 'default' AND session_id = $1 AND id = $2`,
				sessionID,
				childID,
			).Scan(&childStatus); err != nil {
				t.Fatalf("read %s child status: %v", testCase.status, err)
			}
			if childStatus != testCase.status {
				t.Fatalf("resume %s child status = %q; want unchanged", testCase.status, childStatus)
			}
			var statusEvents int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default'
				    AND session_id = $1
				    AND session_thread_id = $2
				    AND type = 'session.thread_status_idle'`,
				sessionID,
				childID,
			).Scan(&statusEvents); err != nil {
				t.Fatalf("count resume %s status events: %v", testCase.status, err)
			}
			if statusEvents != 0 {
				t.Fatalf("resume %s status events = %d; want 0", testCase.status, statusEvents)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReviewerLifecycleSourceRequiresReviewerThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_reviewer_lifecycle_role"
		mainID    = "thr_bridge_reviewer_lifecycle_role_main"
		reviewID  = "arvw_bridge_reviewer_lifecycle_role"
		bindingID = "bind_bridge_reviewer_lifecycle_role"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, "pod_uid_bridge_reviewer_lifecycle_role")
	childID := approvalReviewerSidecarThreadID(scope, mainID, reviewID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_bridge_reviewer_lifecycle_role")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	_, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         scope,
		ChildThreadId: childID,
		Source:        &bridgev1.ChildLifecycleSource{Identity: &bridgev1.ChildLifecycleSource_ReviewerReviewId{ReviewerReviewId: reviewID}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("reviewer lifecycle source on subagent role err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreToolLifecycleSourceRequiresDirectSubagentChild(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_tool_lifecycle_owner"
		mainID    = "thr_bridge_tool_lifecycle_owner_main"
		parentID  = "thr_bridge_tool_lifecycle_owner_parent"
		targetID  = "thr_bridge_tool_lifecycle_owner_target"
		bindingID = "bind_bridge_tool_lifecycle_owner"
		podUID    = "pod_bridge_tool_lifecycle_owner"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, targetID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	source := seedBridgeAPIChildLifecycleToolSource(t, admin, sessionID, mainID, "evt_bridge_tool_lifecycle_owner")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))

	_, err := store.MarkChildThreadClosed(context.Background(), &bridgev1.MarkChildThreadClosedRequest{
		Scope:         bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		ChildThreadId: targetID,
		Source:        source,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("tool lifecycle source on another parent's child err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreSharedChildStatusWriterPreservesClosedThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_closed_wins"
		mainID    = "thr_bridge_closed_wins_main"
		childID   = "thr_bridge_closed_wins_child"
		bindingID = "bind_bridge_closed_wins"
		podUID    = "pod_bridge_closed_wins"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads SET status='closed_for_runtime'
		  WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		childID,
	); err != nil {
		t.Fatalf("close child fixture: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	childScope := bridgeAPIScope(sessionID, childID, bindingID, 1, podUID)
	if err := store.withScopeTx(context.Background(), childScope, "test.closed_wins", func(tx *dbconnect.Tx) error {
		return updateChildThreadStatusTx(
			context.Background(),
			tx,
			childScope,
			"running",
			time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC),
		)
	}); err != nil {
		t.Fatalf("shared status write after close: %v", err)
	}
	var got string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2`,
		sessionID,
		childID,
	).Scan(&got); err != nil {
		t.Fatalf("read child status: %v", err)
	}
	if got != "closed_for_runtime" {
		t.Fatalf("child status after shared writer = %q; want closed_for_runtime", got)
	}
}
