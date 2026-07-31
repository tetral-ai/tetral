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
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tasks protocol-family boundary.

func TestValidateSandboxBackgroundTaskBoundsRecoveryMetadataObject(t *testing.T) {
	base := SandboxBackgroundTask{TaskID: "task_metadata", ProviderSessionID: "provider_session", ProviderCommandID: "provider_command"}
	valid := base
	valid.ProviderCommandMetadataJSON = "{\"x\":\"" + strings.Repeat("a", 4088) + "\"}"
	if err := validateSandboxBackgroundTask(valid); err != nil {
		t.Fatalf("4096-byte metadata rejected: %v", err)
	}
	for _, metadata := range []string{
		"{\"x\":\"" + strings.Repeat("a", 4089) + "\"}",
		`[]`,
		`null`,
		`{"x":`,
	} {
		candidate := base
		candidate.ProviderCommandMetadataJSON = metadata
		if err := validateSandboxBackgroundTask(candidate); err == nil {
			t.Fatalf("metadata %q accepted; want object/4KiB rejection", metadata[:min(len(metadata), 32)])
		}
	}
}

func TestPostgreSQLBridgeAPIStoreCommandCleanupWinnerDefeatsLateHelperPayload(t *testing.T) {
	for _, operation := range []string{"poll", "stdin", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_cleanup_winner_" + operation
			threadID := "thr_bridge_cleanup_winner_" + operation
			bindingID := "bind_bridge_cleanup_winner_" + operation
			taskID := "task_bridge_cleanup_winner_" + operation
			sourceID := "evt_bridge_cleanup_winner_" + operation
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_"+operation)
			seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)

			executor := &recordingSandboxToolExecutor{
				commandResult: SandboxCommandResult{ResultJSON: `{"status":"success","result":{"stdout":{"text":"loser"}}}`, TerminalStatus: "completed"},
				inputResult:   SandboxCommandResult{ResultJSON: `{"status":"accepted","loser":true}`},
				cancelResult:  SandboxCommandResult{ResultJSON: `{"status":"cancelled","loser":true}`, TerminalStatus: "cancelled"},
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 31, 0, time.UTC) }
			store.SandboxToolExecutor = executor
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+operation)
			scope.RequestId = "req_cleanup_winner_" + operation
			winnerJSON := `{"status":"cancelled","result":{"stdout":{"text":"winner","truncated":false},"stderr":{"text":"","truncated":false}}}`
			cleanupWin := func() {
				if err := store.withScopeTx(context.Background(), scope, "test.cleanup_winner", func(tx *dbconnect.Tx) error {
					settled, _, err := settleBackgroundTaskTx(context.Background(), tx, scope, taskID, sourceID, "cancelled_by_cleanup", winnerJSON, store.now())
					if err == nil && !settled {
						return errors.New("cleanup did not win terminal CAS")
					}
					return err
				}); err != nil {
					t.Fatalf("settle cleanup winner: %v", err)
				}
			}
			switch operation {
			case "poll":
				executor.onRead = func(SandboxCommandReference) { cleanupWin() }
			case "stdin":
				executor.onInput = func(SandboxCommandInput) { cleanupWin() }
			case "cancel":
				executor.onCancel = func(SandboxCommandCancel) { cleanupWin() }
			}

			call := func() (string, error) {
				switch operation {
				case "poll":
					response, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: taskID, ToolUseEventId: "evt_poll_cleanup_winner"})
					return response.GetResultJson(), err
				case "stdin":
					response, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: taskID, InputJson: `{"chars":"late\n"}`})
					return response.GetResultJson(), err
				default:
					response, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{Scope: scope, TaskId: taskID, ToolUseEventId: "evt_cancel_cleanup_winner", Reason: "late"})
					return response.GetResultJson(), err
				}
			}
			first, err := call()
			if err != nil || !strings.Contains(first, `"text":"winner"`) || strings.Contains(first, "loser") {
				t.Fatalf("%s cleanup race result = %s err=%v; want durable winner without losing helper payload", operation, first, err)
			}
			replay, err := call()
			if err != nil || replay != first {
				t.Fatalf("%s cleanup replay = %s err=%v; want durable winner", operation, replay, err)
			}
			helperCalls := len(executor.reads) + len(executor.inputs) + len(executor.cancels)
			if helperCalls != 1 {
				t.Fatalf("%s helper calls = %d; want one losing attempt and zero on replay", operation, helperCalls)
			}
			var statusValue string
			var eventID sql.NullString
			var eventCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = $1 AND task_id = $2`, sessionID, taskID,
			).Scan(&statusValue, &eventID); err != nil {
				t.Fatalf("read cleanup winner task: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`, sessionID,
			).Scan(&eventCount); err != nil {
				t.Fatalf("count cleanup winner events: %v", err)
			}
			if statusValue != "cancelled_by_cleanup" || !eventID.Valid || eventCount != 1 {
				t.Fatalf("%s durable winner = %q/%v events=%d; want one cleanup terminal", operation, statusValue, eventID, eventCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandReplayRequiresBackgroundTaskRow(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_read_fence", "thr_bridge_read_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_read_fence", "bind_bridge_read_fence", 1, "pod_uid_read_fence")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_read_fence", "thr_bridge_read_fence", "bind_bridge_read_fence", 1, "pod_uid_read_fence")
	scope.RequestId = "req_bridge_read_fence"
	taskID := "task_bridge_read_fence"
	sourceToolUseEventID := "evt_bridge_read_fence"
	_, key, payloadHashPart := readCommandResultOwnerIdentity(sourceToolUseEventID, taskID, false, 0)
	requestHash := bridgeRequestHash(bridgeOpReadCommandResult, key, payloadHashPart)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
			request_hash, ack_status, result_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $4, $5, $6, 'committed', $7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		"default",
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		bridgeOpReadCommandResult,
		key,
		requestHash,
		`{"status":"success","result":{"task_id":"task_bridge_read_fence"}}`,
	); err != nil {
		t.Fatalf("seed read command operation: %v", err)
	}

	_, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          scope,
		TaskId:         taskID,
		ToolUseEventId: sourceToolUseEventID,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ReadCommandResult replay without task row error = %v; want NotFound", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommandFollowUpsUseStoredRecoveryIdentity(t *testing.T) {
	tests := []struct {
		name      string
		requestID string
		call      func(context.Context, *PostgreSQLBridgeAPIStore, *bridgev1.RuntimeScope) (string, error)
		calls     func(*recordingSandboxToolExecutor) int
	}{
		{
			name:      "read",
			requestID: "req_cmd_gate_read",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{
					Scope:          scope,
					TaskId:         "task_bridge_cmd_gate",
					ToolUseEventId: "evt_bridge_cmd_gate_read",
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.reads) },
		},
		{
			name:      "stdin",
			requestID: "req_cmd_gate_stdin",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.SendCommandInput(ctx, &bridgev1.SendCommandInputRequest{
					Scope:     scope,
					TaskId:    "task_bridge_cmd_gate",
					InputJson: `{"chars":"hello\n"}`,
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.inputs) },
		},
		{
			name:      "cancel",
			requestID: "req_cmd_gate_cancel",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.CancelCommand(ctx, &bridgev1.CancelCommandRequest{
					Scope:  scope,
					TaskId: "task_bridge_cmd_gate",
					Reason: "user_cancel",
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.cancels) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_cmd_gate_" + tc.name
			threadID := "thr_bridge_cmd_gate_" + tc.name
			bindingID := "bind_bridge_cmd_gate_" + tc.name
			preparationID := "prep_bridge_cmd_gate_" + tc.name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_cmd_gate_"+tc.name)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:44:30Z")
			seedBridgeAPIResourceRootsJSON(t, admin, "default", sessionID, preparationID, `[{"path":"/workspace/data/report.csv","mode":"read"}]`)
			seedBridgeAPIResourceCredentialExpiresAt(t, admin, "default", sessionID, preparationID, "2026-01-01T01:00:00Z")
			seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, "task_bridge_cmd_gate", "evt_tool_cmd_gate")

			executor := &recordingSandboxToolExecutor{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC) }
			store.SandboxToolExecutor = executor

			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_cmd_gate_"+tc.name)
			scope.RequestId = tc.requestID
			resultJSON, err := tc.call(context.Background(), store, scope)
			if err != nil {
				t.Fatalf("%s follow-up: %v", tc.name, err)
			}
			if got := tc.calls(executor); got != 1 {
				t.Fatalf("helper %s calls = %d; want stored running-task recovery despite preparation rotation", tc.name, got)
			}

			replay, err := tc.call(context.Background(), store, scope)
			if err != nil {
				t.Fatalf("%s replay: %v", tc.name, err)
			}
			if replay != resultJSON || tc.calls(executor) != 1 {
				t.Fatalf("%s replay result=%s calls=%d; want durable operation replay without another helper call", tc.name, replay, tc.calls(executor))
			}
			if tc.name == "stdin" {
				var metadata string
				if err := admin.QueryRowContext(context.Background(),
					`SELECT provider_command_metadata_json
					   FROM session_background_tasks
					  WHERE workspace_id = 'default'
					    AND session_id = $1
					    AND task_id = 'task_bridge_cmd_gate'`,
					sessionID,
				).Scan(&metadata); err != nil {
					t.Fatalf("read stdin metadata: %v", err)
				}
				if strings.Contains(metadata, "stdin_write_seq") {
					t.Fatalf("provider metadata = %s; want no write_seq allocated while readiness is gated", metadata)
				}
			}
		})
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
	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "rin_bridge_task_notify", "bind_bridge_task_notify", "pod_uid_task_notify")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	request := bridgeTaskNotificationRequestForTest(
		t,
		bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		"rin_bridge_task_notify",
		"task_bridge_notify",
		`{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":null}`,
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
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "rin_task_source_fence", "bind_bridge_task_source_fence", "pod_uid_task_source_fence")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), bridgeTaskNotificationRequestForTest(
			t,
			bridgeAPIScope("sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "bind_bridge_task_source_fence", 1, "pod_uid_task_source_fence"),
			"rin_task_source_fence",
			"task_source_fence",
			`{"task_id":"task_source_fence","source_tool_use_event_id":"sevt_tool_other","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		))
		if err != nil {
			t.Fatalf("CommitTaskNotificationResult source mismatch: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
			response.GetAck().GetErrorCode() != "task_notification_stale" {
			t.Fatalf("source mismatch ack = %#v; want stale rejected", response.GetAck())
		}
		assertBackgroundTaskStillRunningWithoutNotification(t, admin, "sesn_bridge_task_source_fence", "task_source_fence")
	})

	t.Run("released launch sandbox still permits terminal settlement", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", 1, "pod_uid_task_sandbox_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_sandbox_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", "task_sandbox_fence", "sevt_tool_sandbox")
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
		assertBackgroundTaskStillRunningWithoutNotification(t, admin, "sesn_bridge_task_inbox_fence", "task_inbox_fence")
	})
}
