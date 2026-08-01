package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
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
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseEventID, 1, "agent.tool_use", `{"name":"write_stdin","input":{},"evaluated_permission":"allow"}`)
	seedBridgeAPIBackgroundTask(t, admin, workspaceID, sessionID, threadID, bindingID, taskID, "evt_source_stdin_replay")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_stdin_replay")
	scope.RequestId = "req_bridge_stdin_replay"
	request := &bridgev1.SendCommandInputRequest{
		Scope: scope, TaskId: taskID, ToolUseEventId: toolUseEventID,
		InputJson: `{"chars":"hello\n"}`,
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
	if first.err != nil || first.response.GetWriteSeq() != 1 || first.response.GetResultJson() != terminalJSON {
		t.Fatalf("first SendCommandInput = response %+v err %v; want sequence 1", first.response, first.err)
	}
	replay, err := store.SendCommandInput(context.Background(), request)
	if err != nil || replay.GetWriteSeq() != 1 || replay.GetResultJson() != terminalJSON {
		t.Fatalf("replay SendCommandInput = response %+v err %v; want original sequence 1", replay, err)
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
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_notify", "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", "task_bridge_notify", "sevt_tool_notify")
	resultJSON := `{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":null}`
	settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_notify", "task_bridge_notify", "expired", resultJSON)
	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "rin_bridge_task_notify", "bind_bridge_task_notify", "pod_uid_task_notify")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	request := bridgeTaskNotificationRequestForTest(
		t,
		bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		"rin_bridge_task_notify",
		"task_bridge_notify",
		resultJSON,
	)
	response, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	replay, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 ||
		len(replay.GetDeclaration().GetReceipts()) != 1 ||
		!proto.Equal(response.GetDeclaration().GetReceipts()[0], replay.GetDeclaration().GetReceipts()[0]) {
		t.Fatalf("task notification replay receipts diverged: committed=%#v replay=%#v", response.GetDeclaration(), replay.GetDeclaration())
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
		    AND runtime_input_id = 'rin_bridge_task_notify'`).Scan(&inboxStatus); err != nil {
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
	receipt := response.GetDeclaration().GetReceipts()[0]
	if len(receipt.GetEvents()) != 1 || len(receipt.GetMessages()) != 1 ||
		receipt.GetEvents()[0].GetEventId() != terminalEventID.String ||
		receipt.GetMessages()[0].GetOwningEventId() != terminalEventID.String {
		t.Fatalf("task notification receipt = %#v; want one event and its loop-authored message", receipt)
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

	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "rin_bridge_task_notify_late", "bind_bridge_task_notify", "pod_uid_task_notify")
	lateRequest := bridgeTaskNotificationRequestForTest(
		t,
		bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		"rin_bridge_task_notify_late",
		"task_bridge_notify",
		`{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"late","truncated":false},"stderr":{"text":"","truncated":false}}`,
	)
	late, err := store.CommitTaskNotificationResult(context.Background(), lateRequest)
	if err != nil {
		t.Fatalf("late CommitTaskNotificationResult: %v", err)
	}
	if late.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || late.GetAck().GetErrorCode() != "task_notification_stale" {
		t.Fatalf("late task notification ack = %#v; want stale rejected", late.GetAck())
	}
	if late.GetDeclaration() != nil {
		t.Fatalf("late task notification declaration = %#v; want no stale projection", late.GetDeclaration())
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationMessages); err != nil {
		t.Fatalf("read runtime notification messages after late notification: %v", err)
	}
	if notificationMessages != 1 {
		t.Fatalf("late notification wrote %d runtime notification messages; want still 1", notificationMessages)
	}
	replayedLate, err := store.CommitTaskNotificationResult(context.Background(), lateRequest)
	if err != nil {
		t.Fatalf("late CommitTaskNotificationResult replay: %v", err)
	}
	if replayedLate.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || replayedLate.GetAck().GetErrorCode() != "task_notification_stale" {
		t.Fatalf("late task notification replay ack = %#v; want stale rejected", replayedLate.GetAck())
	}
	if replayedLate.GetDeclaration() != nil {
		t.Fatalf("late task notification replay declaration = %#v; want no stale projection", replayedLate.GetDeclaration())
	}
}

func TestPostgreSQLBridgeAPIStoreCommitTaskNotificationRequiresSettlementFences(t *testing.T) {
	t.Run("source tool use mismatch is stale without settlement", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_source_fence", "bind_bridge_task_source_fence", 1, "pod_uid_task_source_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_source_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "bind_bridge_task_source_fence", "task_source_fence", "sevt_tool_original")
		settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_source_fence", "task_source_fence", "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "rin_task_source_fence", "bind_bridge_task_source_fence", "pod_uid_task_source_fence")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t,
			bridgeAPIScope("sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "bind_bridge_task_source_fence", 1, "pod_uid_task_source_fence"),
			"rin_task_source_fence",
			"task_source_fence",
			`{"task_id":"task_source_fence","source_tool_use_event_id":"sevt_tool_other","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("CommitTaskNotificationResult source mismatch = %#v/%v; want FailedPrecondition", response, err)
		}
	})

	t.Run("released launch sandbox still permits terminal settlement", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", 1, "pod_uid_task_sandbox_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_sandbox_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", "task_sandbox_fence", "sevt_tool_sandbox")
		settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_sandbox_fence", "task_sandbox_fence", "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "rin_task_sandbox_fence", "bind_bridge_task_sandbox_fence", "pod_uid_task_sandbox_fence")
		if _, err := admin.ExecContext(context.Background(), `UPDATE sandboxes SET status = 'released' WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_sandbox_fence'`); err != nil {
			t.Fatalf("release sandbox: %v", err)
		}

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t,
			bridgeAPIScope("sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", 1, "pod_uid_task_sandbox_fence"),
			"rin_task_sandbox_fence",
			"task_sandbox_fence",
			`{"task_id":"task_sandbox_fence","source_tool_use_event_id":"sevt_tool_sandbox","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		))
		if err != nil {
			t.Fatalf("CommitTaskNotificationResult sandbox mismatch: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
			len(response.GetDeclaration().GetReceipts()) != 1 {
			t.Fatalf("released sandbox response = %#v; want committed terminal receipt", response)
		}
		var taskStatus string
		var terminalEventID sql.NullString
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_sandbox_fence' AND task_id = 'task_sandbox_fence'`,
		).Scan(&taskStatus, &terminalEventID); err != nil {
			t.Fatalf("read released sandbox terminal task: %v", err)
		}
		if taskStatus != "completed" || !terminalEventID.Valid {
			t.Fatalf("released sandbox terminal task = %q/%v; want completed durable event", taskStatus, terminalEventID)
		}
	})

	t.Run("runtime inbox target pod mismatch is not deliverable", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_inbox_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", "task_inbox_fence", "sevt_tool_inbox")
		settleBridgeAPIBackgroundTask(t, admin, "sesn_bridge_task_inbox_fence", "task_inbox_fence", "completed", `{"status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "rin_task_inbox_fence", "bind_bridge_task_inbox_fence", "pod_uid_other")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		_, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t,
			bridgeAPIScope("sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence"),
			"rin_task_inbox_fence",
			"task_inbox_fence",
			`{"task_id":"task_inbox_fence","source_tool_use_event_id":"sevt_tool_inbox","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		))
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("inbox pod mismatch err = %v; want FailedPrecondition", err)
		}
	})
}
