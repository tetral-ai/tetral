package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

// This file owns the Bridge tasks protocol-family boundary.

func settleBridgeAPIBackgroundTask(t *testing.T, admin *sql.DB, sessionID string, taskID string, terminalStatus string, resultJSON string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status=$3, terminal_result_json=$4, terminal_result_digest=$5,
		    terminal_at='2026-01-01T00:00:30Z', next_poll_at=NULL,
		    reconcile_generation=reconcile_generation+1, updated_at='2026-01-01T00:00:30Z'
		WHERE workspace_id='default' AND session_id=$1 AND task_id=$2 AND status='running'`,
		sessionID, taskID, terminalStatus, resultJSON, bridgeRequestHash(resultJSON)); err != nil {
		t.Fatalf("settle background task: %v", err)
	}
}

func TestPostgreSQLBridgeAPIStoreSendCommandInputReplayReusesWriteSequence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "default"
		sessionID      = "sesn_bridge_stdin_replay"
		threadID       = "thr_bridge_stdin_replay"
		bindingID      = "bind_bridge_stdin_replay"
		taskID         = "task_bridge_stdin_replay"
		toolUseEventID = "evt_bridge_stdin_replay"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_stdin_replay")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_bridge_stdin_replay","chars":"hello\n"},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, taskID, "evt_source_stdin_replay")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_stdin_replay")
	request := &bridgev1.SendCommandInputRequest{
		Scope: scope, TaskId: taskID, ToolUseEventId: toolUseEventID,
		OperationId: "cmdop_bridge_stdin_replay", InputJson: `{"chars":"hello\n","session_id":"task_bridge_stdin_replay"}`,
	}

	type callResult struct {
		response *bridgev1.SendCommandInputResponse
		err      error
	}
	firstDone := make(chan callResult, 1)
	go func() {
		response, err := store.SendCommandInput(context.Background(), request)
		firstDone <- callResult{response: response, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3 AND background_operation_state='pending'`,
			workspaceID, sessionID, toolUseEventID).Scan(&count); err != nil {
			t.Fatalf("read accepted stdin operation: %v", err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stdin operation was not accepted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	terminalJSON := `{"status":"accepted"}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_tool_results
		SET background_operation_state='terminal', result_json=$4, result_digest='digest', updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`, workspaceID, sessionID, toolUseEventID, terminalJSON); err != nil {
		t.Fatalf("settle accepted stdin operation: %v", err)
	}
	first := <-firstDone
	if first.err != nil || first.response.GetCommitted().GetResultJson() != terminalJSON {
		t.Fatalf("first SendCommandInput = response %+v err %v; want committed result", first.response, first.err)
	}
	replay, err := store.SendCommandInput(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetResultJson() != terminalJSON {
		t.Fatalf("replay SendCommandInput = response %+v err %v; want original result", replay, err)
	}
	var taskSequence int64
	if err := admin.QueryRowContext(context.Background(), `SELECT stdin_write_sequence FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`, workspaceID, sessionID, taskID).Scan(&taskSequence); err != nil {
		t.Fatalf("read task write sequence: %v", err)
	}
	if taskSequence != 1 {
		t.Fatalf("task write sequence = %d; want one allocation across replay", taskSequence)
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultReplaysConsumedTerminalReceipt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID     = "default"
		sessionID       = "sesn_bridge_poll_consumed"
		threadID        = "thr_bridge_poll_consumed"
		bindingID       = "bind_bridge_poll_consumed"
		taskID          = "task_bridge_poll_consumed"
		toolUseEventID  = "evt_bridge_poll_consumed"
		terminalEventID = "evt_bridge_poll_terminal"
		terminalJSON    = `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_poll_consumed")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_bridge_poll_consumed","chars":""},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, taskID, toolUseEventID)
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", terminalJSON)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, terminalEventID, 2, "agent.tool_result", `{"tool_use_id":"`+toolUseEventID+`"}`)
	requestID := "cmdop_bridge_poll_consumed"
	inputJSON, err := marshalBridgeJSON(map[string]any{"session_id": taskID, "chars": ""})
	if err != nil {
		t.Fatalf("marshal poll input: %v", err)
	}
	canonicalInput, inputHash, err := canonicalBackgroundCommandInput(inputJSON)
	if err != nil {
		t.Fatalf("canonical poll input: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_digest,
		consumed_by_terminal_event_id, consumption_reason,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, created_at, updated_at
	) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'write_stdin',$6,'committed',$7,$8,
		'conversation_tool_result','poll','terminal',$9,$10,0,now(),now())`,
		workspaceID, sessionID, threadID, toolUseEventID, inputHash, canonicalInput,
		bridgeRequestHash(terminalJSON), terminalEventID, requestID, taskID); err != nil {
		t.Fatalf("seed consumed terminal poll receipt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	response, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{
		Scope:  bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_poll_consumed"),
		TaskId: taskID, ToolUseEventId: toolUseEventID, OperationId: requestID,
	})
	if err != nil || response.GetCompleted().GetResultJson() != terminalJSON {
		t.Fatalf("ReadCommandResult consumed replay = response %+v err %v; want stored terminal result", response, err)
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultSurvivesConsumptionWhileWaiting(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID     = "default"
		sessionID       = "sesn_bridge_poll_wait_consumed"
		threadID        = "thr_bridge_poll_wait_consumed"
		bindingID       = "bind_bridge_poll_wait_consumed"
		taskID          = "task_bridge_poll_wait_consumed"
		toolUseEventID  = "evt_bridge_poll_wait_consumed"
		terminalEventID = "evt_bridge_poll_wait_terminal"
		terminalJSON    = `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_poll_wait_consumed")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_bridge_poll_wait_consumed","chars":""},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, taskID, toolUseEventID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	type callResult struct {
		response *bridgev1.ReadCommandResultResponse
		err      error
	}
	done := make(chan callResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		response, err := store.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{
			Scope:  bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_poll_wait_consumed"),
			TaskId: taskID, ToolUseEventId: toolUseEventID, OperationId: "cmdop_bridge_poll_wait_consumed",
		})
		done <- callResult{response: response, err: err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3
			  AND background_operation_state='pending'`, workspaceID, sessionID, toolUseEventID).Scan(&count); err != nil {
			t.Fatalf("read pending poll receipt: %v", err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("poll receipt was not accepted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	tx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin terminal consumption: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status='completed', terminal_result_json=$4, terminal_result_digest=$5,
		    terminal_at=now(), next_poll_at=NULL, updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3`,
		workspaceID, sessionID, taskID, terminalJSON, bridgeRequestHash(terminalJSON)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("settle background task: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json, created_at, updated_at
	) VALUES ($1,$2,$3,$4,2,'agent.tool_result',$5,now(),now())`,
		workspaceID, sessionID, threadID, terminalEventID, `{"tool_use_id":"`+toolUseEventID+`"}`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("seed terminal event: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(), `UPDATE session_runtime_tool_results
		SET background_operation_state='terminal', result_json=NULL, result_digest=$4,
		    consumed_by_terminal_event_id=$5, consumption_reason='conversation_tool_result', updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`,
		workspaceID, sessionID, toolUseEventID, bridgeRequestHash(terminalJSON), terminalEventID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("consume terminal poll receipt: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit terminal consumption: %v", err)
	}
	result := <-done
	if result.err != nil || result.response.GetCompleted().GetResultJson() != terminalJSON {
		t.Fatalf("ReadCommandResult waiting consumption = response %+v err %v; want stored terminal result", result.response, result.err)
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultRejectsReceiptWithoutTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "default"
		sessionID      = "sesn_bridge_poll_missing_task"
		threadID       = "thr_bridge_poll_missing_task"
		bindingID      = "bind_bridge_poll_missing_task"
		toolUseEventID = "evt_bridge_poll_missing_task"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_poll_missing_task")
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, created_at, updated_at
	) VALUES ($1,$2,$3,$4,'sandbox_background','poll_hash','write_stdin','{}','committed',
		'poll','pending','req_missing_task','task_missing',0,now(),now())`,
		workspaceID, sessionID, threadID, toolUseEventID); err != nil {
		t.Fatalf("seed background receipt without task: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_, err := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime)).waitForBackgroundResult(
		ctx,
		bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_poll_missing_task"),
		toolUseEventID,
	)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("waitForBackgroundResult missing task error = %T %v; want sql.ErrNoRows", err, err)
	}
}

func TestPostgreSQLBridgeAPIStoreCancelCommandKeepsAnIndependentReceipt(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID    = "default"
		sessionID      = "sesn_bridge_cancel_receipt"
		threadID       = "thr_bridge_cancel_receipt"
		bindingID      = "bind_bridge_cancel_receipt"
		taskID         = "task_bridge_cancel_receipt"
		toolUseEventID = "evt_bridge_cancel_receipt"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_cancel_receipt")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"sleep 60"},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, taskID, toolUseEventID)
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, created_at, updated_at
	) VALUES ($1,$2,$3,$4,'sandbox_background','poll_hash','write_stdin','{}','committed',
		'poll','pending','req_poll',$5,0,now(),now())`, workspaceID, sessionID, threadID, toolUseEventID, taskID); err != nil {
		t.Fatalf("seed poll receipt: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_cancel_receipt")
	request := &bridgev1.CancelCommandRequest{
		Scope: scope, TaskId: taskID, ToolUseEventId: toolUseEventID,
		OperationId: "cmdop_bridge_cancel_receipt", Reason: "runtime_interrupted",
	}
	type callResult struct {
		response *bridgev1.CancelCommandResponse
		err      error
	}
	done := make(chan callResult, 1)
	go func() {
		response, err := store.CancelCommand(context.Background(), request)
		done <- callResult{response: response, err: err}
	}()

	requestID := request.GetOperationId()
	receiptID := backgroundCommandReceiptID(requestID)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_use_event_id=$4 AND background_operation_kind='cancel'
			  AND background_request_id=$5 AND background_operation_state='pending'`,
			workspaceID, sessionID, threadID, receiptID, requestID).Scan(&count); err != nil {
			t.Fatalf("read cancel receipt: %v", err)
		}
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cancel receipt was not accepted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var pollState string
	if err := admin.QueryRowContext(context.Background(), `SELECT background_operation_state
		FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND tool_use_event_id=$4`, workspaceID, sessionID, threadID, toolUseEventID).Scan(&pollState); err != nil {
		t.Fatalf("read original poll receipt: %v", err)
	}
	if pollState != "pending" {
		t.Fatalf("original poll state = %q; want pending", pollState)
	}
	terminalJSON := `{"status":"cancelled"}`
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_tool_results
		SET background_operation_state='terminal', result_json=$5, result_digest='digest', updated_at=now()
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4`,
		workspaceID, sessionID, threadID, receiptID, terminalJSON); err != nil {
		t.Fatalf("settle cancel receipt: %v", err)
	}
	first := <-done
	if first.err != nil || first.response.GetCommitted().GetResultJson() != terminalJSON {
		t.Fatalf("CancelCommand = response %+v err %v", first.response, first.err)
	}
	replay, err := store.CancelCommand(context.Background(), request)
	if err != nil || replay.GetDuplicate().GetResultJson() != terminalJSON {
		t.Fatalf("replay CancelCommand = response %+v err %v", replay, err)
	}
	var receipts int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`, workspaceID, sessionID, threadID).Scan(&receipts); err != nil {
		t.Fatalf("count background receipts: %v", err)
	}
	if receipts != 2 {
		t.Fatalf("background receipts = %d; want poll and cancel", receipts)
	}
}

func TestPostgreSQLBridgeAPIStoreBackgroundCommandsRejectUnrelatedSameThreadAuthority(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_bridge_command_authority"
		threadID    = "thr_bridge_command_authority"
		bindingID   = "bind_bridge_command_authority"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, bindingID, 1, "pod_uid_command_authority")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_command_poll", 1, "agent.tool_use",
		`{"name":"write_stdin","input":{"session_id":"task_command_expected","chars":""},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_command_send", 2, "agent.tool_use",
		`{"name":"write_stdin","input":{"session_id":"task_command_expected","chars":"hello"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_command_exec", 3, "agent.tool_use",
		`{"name":"exec_command","input":{"cmd":"sleep 60"},"evaluated_permission":"allow"}`)
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_command_other_exec", 4, "agent.tool_use",
		`{"name":"exec_command","input":{"cmd":"sleep 30"},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, "task_command_expected", "evt_command_exec")
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, "task_command_other", "evt_command_other_exec")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_command_authority")
	if _, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope: scope, TaskId: "task_command_other", ToolUseEventId: "evt_command_poll", OperationId: "cmdop_wrong_task",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("read with unrelated task error = %v; want FailedPrecondition", err)
	}
	if _, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope: scope, TaskId: "task_command_expected", ToolUseEventId: "evt_command_send", OperationId: "cmdop_wrong_input",
		InputJson: `{"session_id":"task_command_expected","chars":"different"}`,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("send with divergent durable input error = %v; want FailedPrecondition", err)
	}
	if _, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope: scope, TaskId: "task_command_other", ToolUseEventId: "evt_command_exec", OperationId: "cmdop_wrong_cancel",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("cancel with unrelated source Tool error = %v; want FailedPrecondition", err)
	}
	var count int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_tool_results
		WHERE workspace_id=$1 AND session_id=$2 AND background_request_id LIKE 'cmdop_wrong_%'`, workspaceID, sessionID).Scan(&count); err != nil {
		t.Fatalf("count rejected background receipts: %v", err)
	}
	if count != 0 {
		t.Fatalf("rejected background receipts = %d; want none", count)
	}
}

func TestCanonicalTaskNotificationPayloadRejectsNullRequiredStreamFields(t *testing.T) {
	_, err := canonicalTaskNotificationPayloadJSON(
		"task_bridge_null",
		"sevt_tool_null",
		"completed",
		`{"status":"completed","stdout":{"text":null,"truncated":false}}`,
	)
	if err == nil {
		t.Fatal("canonical task notification payload accepted null stdout.text; want schema error")
	}
}

func TestCanonicalTaskNotificationPayloadRequiresBothStreams(t *testing.T) {
	for _, resultJSON := range []string{
		`{"status":"completed","stderr":{"text":"","truncated":false}}`,
		`{"status":"completed","stdout":{"text":"","truncated":false}}`,
	} {
		if _, err := canonicalTaskNotificationPayloadJSON("task_bridge_stream", "sevt_tool_stream", "completed", resultJSON); err == nil {
			t.Fatalf("canonical task notification payload accepted missing required stream: %s", resultJSON)
		}
	}
}

func TestRuntimeTaskNotificationPayloadAcceptsSandboxFailureEnvelope(t *testing.T) {
	payload, err := runtimeTaskNotificationPayloadJSON(&RuntimeTaskNotificationPlan{
		TaskID: "task_failed_delivery", SourceToolUseEventID: "evt_failed_delivery",
	}, "failed", `{"status":"failed","error":{"kind":"sandbox_provider_unavailable","message":"provider unavailable"},"result":{"stdout":{"text":"","truncated":false},"stderr":{"text":"provider unavailable","truncated":false}}}`)
	if err != nil {
		t.Fatalf("runtimeTaskNotificationPayloadJSON: %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
		Stderr struct {
			Text      string `json:"text"`
			Truncated bool   `json:"truncated"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("decode task failure payload: %v", err)
	}
	if decoded.Status != "failed" || decoded.Stderr.Text != "provider unavailable" || decoded.Stderr.Truncated {
		t.Fatalf("task failure payload = %s", payload)
	}
}

func TestCanonicalTaskNotificationPayloadFitsRuntimeRail(t *testing.T) {
	resultJSON := fmt.Sprintf(
		`{"status":"completed","stdout":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":5000},"stderr":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":6000}}`,
		strings.Repeat("out-\u754c", 10240),
		strings.Repeat("err-\u754c", 10240),
	)

	payloadJSON, err := canonicalTaskNotificationPayloadJSON("task_bridge_large", "sevt_tool_large", "completed", resultJSON)
	if err != nil {
		t.Fatalf("canonicalTaskNotificationPayloadJSON: %v", err)
	}
	if len([]byte(payloadJSON)) > runtimeTaskNotificationPayloadMaxBytes {
		t.Fatalf("canonical payload bytes = %d; want <= %d", len([]byte(payloadJSON)), runtimeTaskNotificationPayloadMaxBytes)
	}
	var payload struct {
		Stdout struct {
			Text          string `json:"text"`
			Truncated     bool   `json:"truncated"`
			OriginalBytes int64  `json:"original_bytes"`
			OriginalLines int64  `json:"original_lines"`
		} `json:"stdout"`
		Stderr struct {
			Text          string `json:"text"`
			Truncated     bool   `json:"truncated"`
			OriginalBytes int64  `json:"original_bytes"`
			OriginalLines int64  `json:"original_lines"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode canonical payload: %v", err)
	}
	if !utf8.ValidString(payload.Stdout.Text) || !utf8.ValidString(payload.Stderr.Text) {
		t.Fatal("canonical payload split a UTF-8 sequence")
	}
	if !payload.Stdout.Truncated || !payload.Stderr.Truncated {
		t.Fatalf("truncated flags = stdout:%t stderr:%t; want both true", payload.Stdout.Truncated, payload.Stderr.Truncated)
	}
	if payload.Stdout.OriginalBytes != 51200 || payload.Stdout.OriginalLines != 5000 || payload.Stderr.OriginalBytes != 51200 || payload.Stderr.OriginalLines != 6000 {
		t.Fatalf("original totals changed during fit: stdout=%+v stderr=%+v", payload.Stdout, payload.Stderr)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitTaskNotificationProjectsRuntimeNotification(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify")
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", "task_bridge_notify", "sevt_tool_notify")
	storedResultJSON := `{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":null}`
	settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_notify", "task_bridge_notify", "expired", storedResultJSON)
	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "task_notification:task_bridge_notify", "bind_bridge_task_notify", "pod_uid_task_notify")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope:          bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		RuntimeInputId: "task_notification:task_bridge_notify",
		Disposition:    bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	}
	response, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult: %v", err)
	}
	if response.GetCommitted() == nil || len(response.GetCommitted().GetAssignedContextSequences()) != 1 {
		t.Fatalf("outcome = %#v; want committed runtime-notification delta", response)
	}
	replay, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult replay: %v", err)
	}
	if replay.GetDuplicate() == nil ||
		len(replay.GetDuplicate().GetAssignedContextSequences()) != 1 ||
		replay.GetDuplicate().GetAssignedContextSequences()[0] != response.GetCommitted().GetAssignedContextSequences()[0] {
		t.Fatalf("task notification lost-ACK replay diverged: committed=%#v replay=%#v", response, replay)
	}

	var taskStatus string
	var terminalEventID sql.NullString
	var inboxStatus string
	var notificationMessages int
	var notificationEvents int
	var notificationDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND task_id = 'task_bridge_notify'`).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read task settlement: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'task_notification:task_bridge_notify'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationMessages); err != nil {
		t.Fatalf("read runtime notification messages: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationDataJSON); err != nil {
		t.Fatalf("read runtime notification data: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND type = 'runtime_notification'
		    AND visibility = 'internal'`).Scan(&notificationEvents); err != nil {
		t.Fatalf("read runtime notification events: %v", err)
	}
	if taskStatus != "expired" || !terminalEventID.Valid || inboxStatus != "committed" || notificationMessages != 1 || notificationEvents != 1 {
		t.Fatalf("settlement status=%q terminal=%v inbox=%q messages=%d events=%d; want expired/event/committed/1/1",
			taskStatus, terminalEventID.Valid, inboxStatus, notificationMessages, notificationEvents)
	}
	var notificationMessage struct {
		Role   string `json:"role"`
		Origin string `json:"origin"`
		Parts  []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(notificationDataJSON), &notificationMessage); err != nil {
		t.Fatalf("decode runtime notification message: %v", err)
	}
	if notificationMessage.Role != "user" || notificationMessage.Origin != "runtime" || len(notificationMessage.Parts) != 1 ||
		notificationMessage.Parts[0].Type != "text" ||
		!strings.Contains(notificationMessage.Parts[0].Text, `"task_id":"task_bridge_notify"`) ||
		strings.Contains(notificationMessage.Parts[0].Text, `"output_paths"`) ||
		strings.Contains(notificationMessage.Parts[0].Text, `/tmp/tetral-runtime/tasks/`) ||
		strings.Contains(notificationDataJSON, "provider_") {
		t.Fatalf("runtime notification data = %s; want runtime-origin RuntimeMessage without task output paths or provider metadata", notificationDataJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreTaskNotificationStaleSettlementHasStableEvidence(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_stale_evidence"
		threadID  = "thr_task_stale_evidence"
		bindingID = "bind_task_stale_evidence"
		podUID    = "pod_task_stale_evidence"
		taskID    = "task_stale_evidence"
		inputID   = "task_notification:task_stale_evidence"
		sourceID  = "sevt_task_stale_source"
		terminal  = "sevt_task_stale_terminal"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, terminal, 20, "runtime_notification", `{}`)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks SET terminal_event_id=$3
		WHERE workspace_id='default' AND session_id=$1 AND task_id=$2`, sessionID, taskID, terminal); err != nil {
		t.Fatalf("seed prior terminal notification: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := store.CommitTaskNotificationResult(context.Background(), request)
		if err != nil || response.GetStale() == nil {
			t.Fatalf("stale settlement attempt %d = %#v/%v", attempt+1, response, err)
		}
	}
	var inboxStatus string
	var operations, notificationMessages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$2
		  AND operation=$3 AND ack_status='rejected' AND runtime_input_id=$1),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2 AND kind='runtime_notification')`,
		inputID, sessionID, bridgeOpCommitTaskNotificationResult,
	).Scan(&inboxStatus, &operations, &notificationMessages); err != nil {
		t.Fatalf("read stale settlement evidence: %v", err)
	}
	if inboxStatus != "committed" || operations != 1 || notificationMessages != 0 {
		t.Fatalf("stale settlement = Inbox:%s operations:%d messages:%d; want committed/1/0", inboxStatus, operations, notificationMessages)
	}
}

func TestPostgreSQLBridgeAPIStoreRejectsInvalidTaskNotificationSourceEventPerInput(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_invalid_source"
		threadID  = "thr_task_invalid_source"
		bindingID = "bind_task_invalid_source"
		podUID    = "pod_task_invalid_source"
		taskID    = "task_invalid_source"
		inputID   = "task_notification:task_invalid_source"
		sourceID  = "sevt_task_invalid_source"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET type='agent.message'
		WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, sourceID); err != nil {
		t.Fatalf("change task source event type: %v", err)
	}
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := store.CommitTaskNotificationResult(context.Background(), request)
		if err != nil || response.GetRejected().GetReason() != bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID {
			t.Fatalf("invalid-source settlement attempt %d = %#v/%v", attempt+1, response, err)
		}
	}
	var inboxStatus string
	var operations, events, messages int
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$1),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$2 AND operation=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$2 AND type='runtime_notification'),
		(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$2 AND kind='runtime_notification')`,
		inputID, sessionID, bridgeOpCommitTaskNotificationResult,
	).Scan(&inboxStatus, &operations, &events, &messages); err != nil {
		t.Fatalf("read invalid-source settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || operations != 1 || events != 0 || messages != 0 {
		t.Fatalf("invalid-source settlement = Inbox:%s operations:%d events:%d messages:%d", inboxStatus, operations, events, messages)
	}
}

func TestPostgreSQLRuntimeDeliveryReplayKeepsGenuineTaskNotificationExhaustionTerminal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_task_exhaustion", "thr_task_exhaustion")
	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_task_exhaustion",
		SessionThreadID: "thr_task_exhaustion", RuntimeInputID: "task_notification:task_exhaustion", InputKind: "task_notification",
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='dead_lettered'
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, job.SessionID, job.RuntimeInputID); err != nil {
		t.Fatalf("terminalize task notification Inbox: %v", err)
	}
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)

	replayed, found, err := store.ReplayRuntimeDeliveryFinalization(context.Background(), job)
	if err != nil || !found || replayed.Status != RuntimeDeliveryRejected || replayed.Retryable || replayed.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("genuine task notification exhaustion replay = %#v/%t/%v", replayed, found, err)
	}
}

type taskNotificationReplayOnlyDeliverer struct {
	store      *PostgreSQLRuntimeDeliveryStore
	deliveries int
}

func (d *taskNotificationReplayOnlyDeliverer) DeliverRuntimeJob(context.Context, RuntimeJob) (RuntimeDeliveryResult, error) {
	d.deliveries++
	return RuntimeDeliveryResult{}, errors.New("Runtime must not be contacted after durable rejection")
}

func (d *taskNotificationReplayOnlyDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return d.store.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func TestPostgreSQLJobRunnerReclaimsRejectedTaskNotificationAndACKsWithoutRuntime(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_rejection_reclaim"
		threadID  = "thr_task_rejection_reclaim"
		bindingID = "bind_task_rejection_reclaim"
		podUID    = "pod_task_rejection_reclaim"
		taskID    = "task_rejection_reclaim"
		inputID   = "task_notification:task_rejection_reclaim"
	)
	now := time.Now().UTC()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_task_rejection_reclaim")
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET type='agent.message'
		WHERE workspace_id='default' AND session_id=$1 AND event_id='sevt_task_rejection_reclaim'`, sessionID); err != nil {
		t.Fatalf("corrupt durable task source: %v", err)
	}
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, threadID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue job: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue task notification Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "rejection-before-crash",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease task notification Queue job = %#v/%v", leased, err)
	}
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	}
	bridgeStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := bridgeStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil || response.GetRejected().GetReason() != bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID {
		t.Fatalf("commit durable task notification rejection = %#v/%v", response, err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE queue_jobs SET leased_until=clock_timestamp()-interval '1 second'
		WHERE workspace_id='default' AND id=$1 AND status='leased'`, queued.ID); err != nil {
		t.Fatalf("expire rejected task notification lease: %v", err)
	}
	if reclaimed, err := queueStore.ReclaimExpiredLeases(context.Background(), queue.ReclaimExpiredLeasesRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
	}); err != nil || reclaimed != 1 {
		t.Fatalf("reclaim rejected task notification lease = %d/%v; want one", reclaimed, err)
	}
	deliverer := &taskNotificationReplayOnlyDeliverer{store: NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)}
	runner := &JobRunner{
		Queue: tetralqueue.NewServer(queueStore, nil), Workspaces: staticWorkspaceLister{workspace.DefaultID}, Deliverer: deliverer,
		Config: JobRunnerConfig{LeaseOwner: "rejection-after-crash", MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if active, err := runner.RunOnceWithActivity(context.Background()); err != nil || !active {
		t.Fatalf("replay rejected task notification after reclaim = active:%t err:%v", active, err)
	}
	var queueStatus, inboxStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM queue_jobs WHERE workspace_id='default' AND id=$1),
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND runtime_input_id=$2)`,
		queued.ID, inputID,
	).Scan(&queueStatus, &inboxStatus); err != nil {
		t.Fatalf("read reclaimed rejection custody: %v", err)
	}
	if queueStatus != queue.StatusAcknowledged || inboxStatus != "dead_lettered" || deliverer.deliveries != 0 {
		t.Fatalf("reclaimed rejection = Queue:%s Inbox:%s Runtime calls:%d", queueStatus, inboxStatus, deliverer.deliveries)
	}
}

func TestPostgreSQLTaskNotificationRejectionBeforeAcceptanceFinalizationACKsOwnedQueueLease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_rejection_acceptance"
		threadID  = "thr_task_rejection_acceptance"
		bindingID = "bind_task_rejection_acceptance"
		podUID    = "pod_task_rejection_acceptance"
		taskID    = "task_rejection_acceptance"
		inputID   = "task_notification:task_rejection_acceptance"
	)
	now := time.Now().UTC()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_task_rejection_acceptance")
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_events SET type='agent.message'
		WHERE workspace_id='default' AND session_id=$1 AND event_id='sevt_task_rejection_acceptance'`, sessionID); err != nil {
		t.Fatalf("corrupt durable task source: %v", err)
	}
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, threadID, taskID, now)
	if err != nil {
		t.Fatalf("build task notification Queue job: %v", err)
	}
	queued, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue task notification Queue job: %v", err)
	}
	leased, err := queueStore.Lease(context.Background(), queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "task-rejection-acceptance",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != queued.ID {
		t.Fatalf("lease task notification Queue job = %#v/%v", leased, err)
	}
	job, err := DecodeRuntimeJob(queueJobProto(leased[0]))
	if err != nil {
		t.Fatalf("decode task notification Queue job: %v", err)
	}
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), RuntimeInputId: inputID,
		Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_COMMIT,
	}
	apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := apiStore.CommitTaskNotificationResult(context.Background(), request)
	if err != nil || response.GetRejected().GetReason() != bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID {
		t.Fatalf("commit terminal notification rejection = %#v/%v", response, err)
	}
	deliveryStore := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	attemptedBinding := RuntimeAttemptedBinding{
		BindingID: bindingID, Generation: 1, TargetPodUID: podUID,
	}
	if settled, err := deliveryStore.MarkRuntimeInputAccepted(context.Background(), job, attemptedBinding); err != nil || settled {
		t.Fatalf("MarkRuntimeInputAccepted after rejection = settled:%t err:%v; want replayed terminal Inbox", settled, err)
	}
	if acked, err := queueStore.Ack(context.Background(), queue.AckRequest{
		WorkspaceID: workspace.DefaultID, JobID: leased[0].ID, LeaseToken: leased[0].LeaseToken, Now: now.Add(2 * time.Second),
	}); err != nil || !acked {
		t.Fatalf("ACK rejection Queue lease = %t/%v", acked, err)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.id=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID, queued.ID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read converged rejected custody: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusAcknowledged {
		t.Fatalf("rejected custody = Inbox:%s Queue:%s", inboxStatus, queueStatus)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitTaskNotificationRequiresSettlementFences(t *testing.T) {
	t.Run("Inbox identity cannot settle another background task", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const (
			sessionID = "sesn_bridge_task_identity_fence"
			threadID  = "thr_bridge_task_identity_fence"
			bindingID = "bind_bridge_task_identity_fence"
			podUID    = "pod_uid_task_identity_fence"
			inboxID   = "task_notification:task_identity_inbox"
			taskID    = "task_identity_other"
		)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_tool_identity_fence")
		storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
		settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
		seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inboxID, bindingID, podUID)
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), inboxID,
		))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("CommitTaskNotificationResult mismatched identities = %#v/%v; want FailedPrecondition", response, err)
		}
		var inboxStatus string
		var terminalEventID sql.NullString
		var notificationEvents, notificationMessages, operations int
		if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_runtime_inbox
			WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, sessionID, inboxID).Scan(&inboxStatus); err != nil {
			t.Fatalf("read Inbox after identity mismatch: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(), `SELECT terminal_event_id FROM session_background_tasks
			WHERE workspace_id='default' AND session_id=$1 AND task_id=$2`, sessionID, taskID).Scan(&terminalEventID); err != nil {
			t.Fatalf("read task after identity mismatch: %v", err)
		}
		if err := admin.QueryRowContext(context.Background(), `SELECT
			(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='runtime_notification'),
			(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='runtime_notification'),
			(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$2)`,
			sessionID, bridgeOpCommitTaskNotificationResult).Scan(&notificationEvents, &notificationMessages, &operations); err != nil {
			t.Fatalf("read durable effects after identity mismatch: %v", err)
		}
		if inboxStatus != "accepted" || terminalEventID.Valid || notificationEvents != 0 || notificationMessages != 0 || operations != 0 {
			t.Fatalf("identity mismatch changed durable facts: Inbox=%s terminal=%v events=%d messages=%d operations=%d",
				inboxStatus, terminalEventID.Valid, notificationEvents, notificationMessages, operations)
		}
		replay, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), inboxID,
		))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("CommitTaskNotificationResult identity mismatch replay = %#v/%v; want FailedPrecondition", replay, err)
		}
	})

	t.Run("runtime inbox target pod mismatch is not deliverable", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence")
		seedBridgeAPINotifiableBackgroundTask(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", "task_inbox_fence", "sevt_tool_inbox")
		settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_inbox_fence", "task_inbox_fence", "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "task_notification:task_inbox_fence", "bind_bridge_task_inbox_fence", "pod_uid_other")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		_, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t,
			bridgeAPIScope("sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence"),
			"task_notification:task_inbox_fence",
		))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("inbox pod mismatch err = %v; want FailedPrecondition", err)
		}
	})
}

func TestPostgreSQLBridgeAPIStoreTaskNotificationRejectsEveryRuntimeScopeMismatch(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_scope_fence"
		threadID  = "thr_task_scope_fence"
		bindingID = "bind_task_scope_fence"
		podUID    = "pod_task_scope_fence"
		taskID    = "task_scope_fence"
		inputID   = "task_notification:task_scope_fence"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, "sevt_task_scope_fence")
	storedResult := `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", storedResult)
	seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, inputID, bindingID, podUID)
	valid := bridgeTaskNotificationRequestForTest(t, bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID), inputID)
	wrongCaller := internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
		ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
		KubernetesPodUID: "pod_task_scope_other",
	})
	tests := []struct {
		name   string
		ctx    context.Context
		mutate func(*bridgev1.CommitTaskNotificationResultRequest)
	}{
		{name: "caller", ctx: wrongCaller, mutate: func(*bridgev1.CommitTaskNotificationResultRequest) {}},
		{name: "workspace", ctx: context.Background(), mutate: func(request *bridgev1.CommitTaskNotificationResultRequest) {
			request.Scope.WorkspaceId = "workspace_other"
		}},
		{name: "session", ctx: context.Background(), mutate: func(request *bridgev1.CommitTaskNotificationResultRequest) {
			request.Scope.SessionId = "sesn_task_scope_other"
		}},
		{name: "thread", ctx: context.Background(), mutate: func(request *bridgev1.CommitTaskNotificationResultRequest) {
			request.Scope.SessionThreadId = "thr_task_scope_other"
		}},
		{name: "binding generation", ctx: context.Background(), mutate: func(request *bridgev1.CommitTaskNotificationResultRequest) {
			request.Scope.Binding.BindingGeneration = 2
		}},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := proto.Clone(valid).(*bridgev1.CommitTaskNotificationResultRequest)
			test.mutate(request)
			if response, err := store.CommitTaskNotificationResult(test.ctx, request); err == nil {
				t.Fatalf("scope mismatch response = %#v; want rejection", response)
			}
			var inboxStatus string
			var terminalEventID sql.NullString
			var notificationEvents, notificationMessages, operations int
			if err := admin.QueryRowContext(context.Background(), `SELECT
				(SELECT status FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2),
				(SELECT terminal_event_id FROM session_background_tasks WHERE workspace_id='default' AND session_id=$1 AND task_id=$3),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='runtime_notification'),
				(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND kind='runtime_notification'),
				(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation=$4)`,
				sessionID, inputID, taskID, bridgeOpCommitTaskNotificationResult,
			).Scan(&inboxStatus, &terminalEventID, &notificationEvents, &notificationMessages, &operations); err != nil {
				t.Fatalf("read durable facts after scope rejection: %v", err)
			}
			if inboxStatus != "accepted" || terminalEventID.Valid || notificationEvents != 0 || notificationMessages != 0 || operations != 0 {
				t.Fatalf("scope rejection changed durable facts: Inbox=%s terminal=%t Events=%d Messages=%d operations=%d",
					inboxStatus, terminalEventID.Valid, notificationEvents, notificationMessages, operations)
			}
		})
	}
}

func TestPostgreSQLCommitTaskNotificationDeferredReceiptLeavesQueueCustodyToJobRunner(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_task_deferred_receipt"
		parentID  = "thr_task_deferred_parent"
		childID   = "thr_task_deferred_child"
		bindingID = "bind_task_deferred"
		podUID    = "pod_task_deferred"
		taskID    = "task_deferred_receipt"
		inputID   = "task_notification:task_deferred_receipt"
	)
	now := time.Date(2026, 8, 11, 8, 0, 0, 0, time.UTC)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, parentID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPINotifiableBackgroundTask(t, admin, "default", sessionID, childID, bindingID, taskID, "evt_task_deferred_source")
	resultJSON := `{"task_id":"task_deferred_receipt","source_tool_use_event_id":"evt_task_deferred_source","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`
	settleBridgeAPIBackgroundTask(t, admin, sessionID, taskID, "completed", resultJSON)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads SET status='closed_for_runtime'
		WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
		t.Fatalf("close notification target: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,
		binding_id,binding_generation,target_pod_uid,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'task_notification','[]','parked',$4,1,$5,$6,$6)`, sessionID, childID, inputID, bindingID, podUID, now); err != nil {
		t.Fatalf("seed parked notification after close admission: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueue, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(workspace.DefaultID, sessionID, childID, taskID, now)
	if err != nil {
		t.Fatalf("build notification Queue custody: %v", err)
	}
	queuedJob, err := queueStore.Enqueue(context.Background(), enqueue)
	if err != nil {
		t.Fatalf("enqueue notification Queue custody: %v", err)
	}

	response, err := store.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{
		Scope: bridgeAPIScope(sessionID, childID, bindingID, 1, podUID), RuntimeInputId: inputID,
		Disposition: bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_DEFER,
	})
	if err != nil {
		t.Fatalf("commit deferred task notification: %v", err)
	}
	if response.GetDeferred() == nil {
		t.Fatalf("deferred notification outcome = %#v; want deferred", response)
	}
	var inboxStatus, queueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT inbox.status,job.status
		FROM session_runtime_inbox inbox JOIN queue_jobs job ON job.workspace_id=inbox.workspace_id AND job.id=$2
		WHERE inbox.workspace_id='default' AND inbox.runtime_input_id=$1`, inputID, queuedJob.ID).Scan(&inboxStatus, &queueStatus); err != nil {
		t.Fatalf("read deferred custody: %v", err)
	}
	if inboxStatus != "parked" || queueStatus != queue.StatusPending {
		t.Fatalf("deferred custody = Inbox %q / Queue %q; want parked / pending", inboxStatus, queueStatus)
	}
}
