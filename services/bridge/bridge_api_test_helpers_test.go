package agentruntimebridge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file owns shared Bridge API store test fixtures and assertions.

func assertCommitInputsConflictDidNotAdvance(t *testing.T, admin *sql.DB, sessionID string, runtimeInputID string, eventIDs []string) {
	t.Helper()
	var inboxStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = $1`, runtimeInputID).Scan(&inboxStatus); err != nil {
		t.Fatalf("read conflicting inbox status: %v", err)
	}
	var processedCount int
	var messageCount int
	var operationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND event_id = ANY($1) AND processed_at IS NOT NULL`, eventIDs).Scan(&processedCount); err != nil {
		t.Fatalf("count processed conflicting events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND source_event_id = ANY($1)`, eventIDs).Scan(&messageCount); err != nil {
		t.Fatalf("count conflicting message projections: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = $1 AND operation = 'commit_inputs'`, sessionID).Scan(&operationCount); err != nil {
		t.Fatalf("count conflicting commit operations: %v", err)
	}
	if inboxStatus != "accepted" || processedCount != 0 || messageCount != 0 || operationCount != 0 {
		t.Fatalf("conflicting commit advanced inbox=%q processed=%d messages=%d operations=%d; want accepted/0/0/0", inboxStatus, processedCount, messageCount, operationCount)
	}
}

func createBridgeTransientAttachmentForTest(t *testing.T, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope, runtimeWriteID string, sourceToolUseEventID string, data []byte) *bridgev1.TransientAttachmentRef {
	t.Helper()
	create := transientAttachmentCreate{
		Scope:                scope,
		SourceToolUseEventID: sourceToolUseEventID,
		Data:                 data,
		Mime:                 "image/png",
		Filename:             runtimeWriteID + ".png",
		SourcePath:           "sandbox:" + runtimeWriteID + ".png",
		Detail:               "auto",
	}
	pending, err := store.uploadTransientAttachment(context.Background(), create)
	if err != nil {
		t.Fatalf("upload transient attachment %s: %v", runtimeWriteID, err)
	}
	now := store.now()
	if err := store.withScopeTx(context.Background(), scope, "test.create_transient_attachment", func(tx *dbconnect.Tx) error {
		return insertTransientAttachmentTx(context.Background(), tx, create, pending.Attachment, pending.BlobPointer, now)
	}); err != nil {
		_ = store.AttachmentBlobStore.Delete(context.Background(), pending.BlobPointer)
		t.Fatalf("insert transient attachment %s: %v", runtimeWriteID, err)
	}
	return pending.Attachment
}

func bridgeTransientAttachmentStatus(t *testing.T, db *sql.DB, attachmentRef string) string {
	t.Helper()
	var statusValue string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_transient_attachments
		  WHERE workspace_id = 'default'
		    AND attachment_ref = $1`,
		attachmentRef,
	).Scan(&statusValue); err != nil {
		t.Fatalf("read transient attachment %s status: %v", attachmentRef, err)
	}
	return statusValue
}

func deleteCleanupRuntimeJob(sessionID string, deleteCleanupID string) RuntimeJob {
	return RuntimeJob{JobID: "qjob_" + sessionID, LeaseToken: "lease_" + sessionID, Kind: queue.KindSessionDeleteCleanup,
		WorkspaceID: "default", SessionID: sessionID, RuntimeInputID: "session_delete_cleanup:" + deleteCleanupID,
		CleanupJobID: deleteCleanupID, DeleteCleanupID: deleteCleanupID, CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION}
}

func finishBridgeDeleteFixtureIdle(t *testing.T, runtime *sql.DB, sessionID string, threadID string, bindingID string, generation int64, podUID string) {
	t.Helper()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	store.OutputCapturer = outputcapture.NewCapturer(blob.NewFakeBlobStore(), &recordingOutputScanner{})
	if _, err := store.FinishIdle(context.Background(), &bridgev1.FinishIdleRequest{
		Scope: bridgeAPIScope(sessionID, threadID, bindingID, generation, podUID), RuntimeWriteId: "rwrite_idle_" + sessionID,
		IdleSince: "2026-01-01T00:00:45Z", StopReasonJson: `{"type":"end_turn"}`,
	}); err != nil {
		t.Fatalf("FinishIdle fixture: %v", err)
	}
}

func assertDeleteCleanupSettlementCounts(t *testing.T, db *sql.DB, sessionID string, wantBindings int, wantSettledTasks int, wantSettledWaits int) {
	t.Helper()
	var bindings, settledTasks, settledWaits int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_bindings WHERE session_id=$1`, sessionID).Scan(&bindings); err != nil {
		t.Fatalf("count bindings: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_background_tasks WHERE session_id=$1 AND status='cancelled_by_cleanup' AND terminal_event_id IS NOT NULL`, sessionID).Scan(&settledTasks); err != nil {
		t.Fatalf("count settled tasks: %v", err)
	}
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM session_pending_tool_uses WHERE session_id=$1 AND status='expired' AND result_event_id IS NOT NULL`, sessionID).Scan(&settledWaits); err != nil {
		t.Fatalf("count settled waits: %v", err)
	}
	if bindings != wantBindings || settledTasks != wantSettledTasks || settledWaits != wantSettledWaits {
		t.Fatalf("bindings/tasks/waits=%d/%d/%d; want %d/%d/%d", bindings, settledTasks, settledWaits, wantBindings, wantSettledTasks, wantSettledWaits)
	}
}

func testPostgreSQLRunToolTerminalIdentityFencing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_tool_identity", "prep_bridge_tool_identity")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_tool_identity", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{execution: SandboxToolExecution{ResultJSON: `{"status":"success","result":{"exit_code":0,"stdout":"ok"}}`}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor
	canonicalInput := `{"cmd":"printf '<>&'","workdir":"/workspace"}`
	request := &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_tool_identity", "thr_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity"),
		ToolUseEventId:      "evt_tool_identity",
		NormalizedInputHash: sha256Hex(canonicalInput),
		ToolName:            "exec_command",
		InputJson:           canonicalInput,
	}
	first, err := store.RunTool(context.Background(), request)
	if err != nil {
		t.Fatalf("RunTool terminal: %v", err)
	}
	reordered := proto.Clone(request).(*bridgev1.RunToolRequest)
	reordered.Scope.RequestId = "req_bridge_tool_identity_replay"
	reordered.InputJson = "{ \"workdir\" : \"/workspace\", \"cmd\" : \"printf '<>&'\" }"
	replay, err := store.RunTool(context.Background(), reordered)
	if err != nil {
		t.Fatalf("RunTool canonical replay: %v", err)
	}
	if first.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		first.GetResultJson() != replay.GetResultJson() ||
		first.GetBackgroundTaskStarted() != replay.GetBackgroundTaskStarted() ||
		first.GetTaskId() != replay.GetTaskId() || len(executor.invocations) != 1 {
		t.Fatalf("terminal first/replay/calls = %+v / %+v / %d; want field-identical logical replay and one helper call", first, replay, len(executor.invocations))
	}

	for name, mutate := range map[string]func(*bridgev1.RunToolRequest){
		"hash": func(conflict *bridgev1.RunToolRequest) { conflict.NormalizedInputHash = "different_hash" },
		"name": func(conflict *bridgev1.RunToolRequest) { conflict.ToolName = "Bash" },
		"payload_reusing_hash": func(conflict *bridgev1.RunToolRequest) {
			conflict.InputJson = `{"cmd":"printf different","workdir":"/workspace"}`
		},
	} {
		t.Run(name+" conflict", func(t *testing.T) {
			conflict := proto.Clone(request).(*bridgev1.RunToolRequest)
			conflict.Scope.RequestId = "req_bridge_tool_identity_conflict_" + name
			mutate(conflict)
			if _, err := store.RunTool(context.Background(), conflict); status.Code(err) != codes.AlreadyExists && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("RunTool %s conflict error = %v; want fatal identity rejection", name, err)
			}
			if len(executor.invocations) != 1 {
				t.Fatalf("helper calls after %s conflict = %d; want one", name, len(executor.invocations))
			}
		})
	}
	if _, err := store.ClaimMcpToolResult(context.Background(), &bridgev1.ClaimMcpToolResultRequest{
		Scope: request.GetScope(), ToolUseEventId: request.GetToolUseEventId(), NormalizedInputHash: request.GetNormalizedInputHash(),
		McpServerName: "github", ToolName: "create_issue", InputJson: request.GetInputJson(),
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("cross-kind MCP claim error = %v; want AlreadyExists", err)
	}
	if len(executor.invocations) != 1 {
		t.Fatalf("helper calls after cross-kind conflict = %d; want one", len(executor.invocations))
	}
	var rowCount int
	var claimStatus, claimOwner, claimLease sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(mcp_claim_status), max(mcp_claim_owner_request_id), max(mcp_claim_lease_expires_at)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_identity' AND tool_use_event_id = 'evt_tool_identity'`,
	).Scan(&rowCount, &claimStatus, &claimOwner, &claimLease); err != nil {
		t.Fatalf("read terminal settlement row: %v", err)
	}
	if rowCount != 1 || claimStatus.Valid || claimOwner.Valid || claimLease.Valid {
		t.Fatalf("terminal rows/claims = %d/%+v/%+v/%+v; want one row and all MCP claim fields NULL", rowCount, claimStatus, claimOwner, claimLease)
	}
}

func assertBackgroundTaskStillRunningWithoutNotification(t *testing.T, db *sql.DB, sessionID string, taskID string) {
	t.Helper()
	var taskStatus string
	var terminalEventID sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND task_id = $2`,
		sessionID,
		taskID,
	).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read background task fence state: %v", err)
	}
	var notificationMessages int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND kind = 'runtime_notification'`,
		sessionID,
	).Scan(&notificationMessages); err != nil {
		t.Fatalf("read notification count after fenced settlement: %v", err)
	}
	if taskStatus != "running" || terminalEventID.Valid || notificationMessages != 0 {
		t.Fatalf("background task status=%q terminal=%v notifications=%d; want running/no terminal/no notifications",
			taskStatus, terminalEventID.Valid, notificationMessages)
	}
}

func assertNoSessionPrepareJobsForSession(t *testing.T, db *sql.DB, workspaceID string, sessionID string) {
	t.Helper()
	var prepareJobs int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND partition_key = $3`,
		workspaceID,
		queue.KindSessionPrepare,
		queue.FormatSessionPartitionKey(workspace.ID(workspaceID), sessionID),
	).Scan(&prepareJobs); err != nil {
		t.Fatalf("count session_prepare jobs for %s: %v", sessionID, err)
	}
	if prepareJobs != 0 {
		t.Fatalf("session_prepare jobs for %s = %d; want none", sessionID, prepareJobs)
	}
}

func assertNoRuntimeInboxRow(t *testing.T, db *sql.DB, runtimeInputID string) {
	t.Helper()
	var rows int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = $1`,
		runtimeInputID,
	).Scan(&rows); err != nil {
		t.Fatalf("count runtime inbox rows for %s: %v", runtimeInputID, err)
	}
	if rows != 0 {
		t.Fatalf("runtime inbox rows for %s = %d; want 0 before readiness gate succeeds", runtimeInputID, rows)
	}
}

func sourceFunctionBody(t *testing.T, source string, signature string) string {
	t.Helper()
	start := strings.Index(source, signature)
	if start < 0 {
		t.Fatalf("%s not found", signature)
	}
	body := source[start:]
	if next := strings.Index(body[len(signature):], "\nfunc "); next >= 0 {
		body = body[:len(signature)+next]
	}
	return body
}

func bridgeAPIScope(sessionID string, threadID string, bindingID string, generation int64, podUID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		RequestId:       "req_" + sessionID,
		WorkspaceId:     "default",
		SessionId:       sessionID,
		SessionThreadId: threadID,
		Binding:         &bridgev1.RuntimeBindingRef{BindingId: bindingID, BindingGeneration: generation, TargetPodUid: podUID},
	}
}

func bridgeInternalToolRepairMessageJSON(t *testing.T, sessionID string, messageID string, partID string, toolCallID string, toolName string, message string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":        messageID,
		"sessionId": sessionID,
		"role":      "assistant",
		"origin":    "agent",
		"sequence":  float64(2),
		"status":    "completed",
		"createdAt": "2026-01-01T00:00:00Z",
		"parts": []map[string]any{
			{
				"id":          partID,
				"sessionId":   sessionID,
				"messageId":   messageID,
				"sequence":    float64(0),
				"createdAt":   "2026-01-01T00:00:00Z",
				"type":        "tool",
				"toolCallId":  toolCallID,
				"toolName":    toolName,
				"completedAt": "2026-01-01T00:00:00Z",
				"state": map[string]any{
					"status": "error",
					"input": map[string]any{
						"value":     map[string]any{"q": "x"},
						"preview":   `{"q":"x"}`,
						"truncated": false,
					},
					"error": map[string]any{
						"type":      "provider_tool_protocol_error",
						"message":   message,
						"retryable": false,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal internal repair message: %v", err)
	}
	return string(raw)
}

func bridgeInternalToolCallMessageJSON(t *testing.T, sessionID string, messageID string, partID string, toolCallID string, toolName string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":        messageID,
		"sessionId": sessionID,
		"role":      "assistant",
		"origin":    "agent",
		"sequence":  float64(1),
		"status":    "completed",
		"createdAt": "2026-01-01T00:00:00Z",
		"parts": []map[string]any{
			{
				"id":         partID,
				"sessionId":  sessionID,
				"messageId":  messageID,
				"sequence":   float64(0),
				"createdAt":  "2026-01-01T00:00:00Z",
				"type":       "tool",
				"toolCallId": toolCallID,
				"toolName":   toolName,
				"state": map[string]any{
					"status": "running",
					"input": map[string]any{
						"value":     map[string]any{"q": "x"},
						"preview":   `{"q":"x"}`,
						"truncated": false,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal internal tool call message: %v", err)
	}
	return string(raw)
}

func testJSONPathString(t *testing.T, raw string, path string) string {
	t.Helper()
	value := testJSONPathValue(t, raw, path)
	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("JSON path %s = %#v; want string", path, value)
	}
	return stringValue
}

func testJSONPathInt(t *testing.T, raw string, path string) int64 {
	t.Helper()
	value := testJSONPathValue(t, raw, path)
	switch typed := value.(type) {
	case float64:
		return int64(typed)
	case int64:
		return typed
	case int:
		return int64(typed)
	default:
		t.Fatalf("JSON path %s = %#v; want number", path, value)
		return 0
	}
}

func assertNoTaskOutputPaths(t *testing.T, raw string) {
	t.Helper()
	if strings.Contains(raw, `"output_paths"`) || strings.Contains(raw, "/tmp/tetral-runtime/tasks/") {
		t.Fatalf("task notification surface contains internal output paths: %s", raw)
	}
}

func testJSONPathValue(t *testing.T, raw string, path string) any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("parse JSON %s: %v", path, err)
	}
	var current any = payload
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("JSON path %s segment %s entered non-object %#v", path, segment, current)
		}
		current, ok = object[segment]
		if !ok {
			t.Fatalf("JSON path %s missing segment %s in %s", path, segment, raw)
		}
	}
	return current
}

func bridgeRuntimeUserMessageJSON(t *testing.T, sessionID string, messageID string, text string) string {
	t.Helper()
	return bridgeRuntimeMessageJSON(t, sessionID, messageID, text, "user")
}

func bridgeAcceptedMessageSnapshotJSON(t *testing.T, sessionID string, messageID string, text string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"messages": []json.RawMessage{json.RawMessage(bridgeRuntimeUserMessageJSON(t, sessionID, messageID, text))},
	})
	if err != nil {
		t.Fatalf("marshal accepted message snapshot: %v", err)
	}
	return string(raw)
}

func bridgeAcceptedMessageDeliveryPayload(t *testing.T, runtime *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, eventIDs []string, sequenceFrom int64, sequenceTo int64) string {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtime)
	var payloadJSON string
	if err := client.WithWorkspaceTx(context.Background(), workspaceID, "agentruntimebridge.test_accepted_message_delivery_payload", func(tx *dbconnect.Tx) error {
		var err error
		payloadJSON, err = acceptedMessageCommandPayloadTx(context.Background(), tx, RuntimeJob{
			Kind:            queue.KindRuntimeInput,
			WorkspaceID:     workspaceID,
			SessionID:       sessionID,
			SessionThreadID: threadID,
			RuntimeInputID:  runtimeInputID,
			EventIDs:        eventIDs,
			SequenceFrom:    sequenceFrom,
			SequenceTo:      sequenceTo,
			InputKind:       "messages",
		})
		return err
	}); err != nil {
		t.Fatalf("build accepted message delivery payload: %v", err)
	}
	return payloadJSON
}

func assertBridgeRuntimeUserProjection(t *testing.T, raw string, sessionID string, text string) {
	t.Helper()
	var message struct {
		ID        string `json:"id"`
		SessionID string `json:"sessionId"`
		Role      string `json:"role"`
		Origin    string `json:"origin"`
		Sequence  int64  `json:"sequence"`
		Status    string `json:"status"`
		Parts     []struct {
			ID          string `json:"id"`
			SessionID   string `json:"sessionId"`
			MessageID   string `json:"messageId"`
			Sequence    int64  `json:"sequence"`
			Type        string `json:"type"`
			Text        string `json:"text"`
			Status      string `json:"status"`
			Truncated   bool   `json:"truncated"`
			CompletedAt string `json:"completedAt"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatalf("unmarshal projected user RuntimeMessage: %v", err)
	}
	if message.ID == "" || message.SessionID != sessionID || message.Role != "user" || message.Origin != "user" || message.Sequence != 0 || message.Status != "completed" || len(message.Parts) != 1 {
		t.Fatalf("projected user RuntimeMessage = %+v; want completed user message", message)
	}
	part := message.Parts[0]
	if part.ID == "" || part.SessionID != sessionID || part.MessageID != message.ID || part.Sequence != 0 || part.Type != "text" || part.Text != text || part.Status != "completed" || part.Truncated || part.CompletedAt == "" {
		t.Fatalf("projected user RuntimePart = %+v; want completed text part", part)
	}
}

func bridgeRuntimeNotificationMessageJSON(t *testing.T, sessionID string, messageID string, text string) string {
	t.Helper()
	return bridgeRuntimeMessageJSON(t, sessionID, messageID, text, "runtime")
}

func bridgeRuntimeMessageJSON(t *testing.T, sessionID string, messageID string, text string, origin string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id":        messageID,
		"sessionId": sessionID,
		"role":      "user",
		"origin":    origin,
		"sequence":  0,
		"status":    "completed",
		"createdAt": "2026-01-01T00:00:00Z",
		"updatedAt": "2026-01-01T00:00:00Z",
		"parts": []map[string]any{
			{
				"id":          messageID + "_text",
				"sessionId":   sessionID,
				"messageId":   messageID,
				"sequence":    0,
				"createdAt":   "2026-01-01T00:00:00Z",
				"updatedAt":   "2026-01-01T00:00:00Z",
				"type":        "text",
				"text":        text,
				"truncated":   false,
				"status":      "completed",
				"completedAt": "2026-01-01T00:00:00Z",
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal runtime user message: %v", err)
	}
	return string(raw)
}

func bridgeForkSeedJSON(t *testing.T, sessionID string, messageID string, text string, parentThreadID string, sourceToolUseEventID string, forkTurns string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"source_parent_thread_id":   parentThreadID,
		"source_tool_use_event_id":  sourceToolUseEventID,
		"fork_turns":                forkTurns,
		"runtime_messages_snapshot": []json.RawMessage{json.RawMessage(bridgeRuntimeUserMessageJSON(t, sessionID, messageID, text))},
	})
	if err != nil {
		t.Fatalf("marshal fork seed: %v", err)
	}
	return string(raw)
}

func bridgeReviewerForkSeedJSON(t *testing.T, parentThreadID string, reviewID string, messages []json.RawMessage) string {
	t.Helper()
	if messages == nil {
		messages = []json.RawMessage{}
	}
	raw, err := json.Marshal(map[string]any{
		"source_parent_thread_id":   parentThreadID,
		"review_id":                 reviewID,
		"fork_turns":                "all",
		"runtime_messages_snapshot": messages,
	})
	if err != nil {
		t.Fatalf("marshal reviewer fork seed: %v", err)
	}
	return string(raw)
}

func bridgeInterAgentMessageJSON(t *testing.T, deliveryID string, sourceThreadID string, sourceToolUseEventID string, messageJSON string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"delivery_id":              deliveryID,
		"source_thread_id":         sourceThreadID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		t.Fatalf("marshal inter-agent message: %v", err)
	}
	return string(raw)
}

func bridgeInterAgentMessagePresentationJSON(
	t *testing.T,
	deliveryID string,
	sourceThreadID string,
	sourceToolUseEventID string,
	messageJSON string,
	presentation string,
) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"delivery_id":              deliveryID,
		"source_thread_id":         sourceThreadID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
		"presentation":             presentation,
	})
	if err != nil {
		t.Fatalf("marshal inter-agent message presentation: %v", err)
	}
	return string(raw)
}

func bridgeInterAgentSentEventJSON(t *testing.T, deliveryID string, sourceThreadID string, targetThreadID string, targetTaskName string, sourceToolUseEventID string, messageJSON string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"type":                     "agent.thread_message_sent",
		"delivery_id":              deliveryID,
		"source_thread_id":         sourceThreadID,
		"target_thread_id":         targetThreadID,
		"target_task_name":         targetTaskName,
		"source_tool_use_event_id": sourceToolUseEventID,
		"message":                  json.RawMessage(messageJSON),
	})
	if err != nil {
		t.Fatalf("marshal inter-agent sent event: %v", err)
	}
	return string(raw)
}

func bridgeRuntimeMessageWithPublicContentJSON(t *testing.T, sessionID string, messageID string) string {
	t.Helper()
	var message map[string]any
	if err := json.Unmarshal([]byte(bridgeRuntimeNotificationMessageJSON(t, sessionID, messageID, "repair text")), &message); err != nil {
		t.Fatalf("decode Runtime message fixture: %v", err)
	}
	message["content"] = []map[string]any{
		{"type": "text", "text": "first public block"},
		{"type": "text", "text": "second public block"},
	}
	raw, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal Runtime message with public content: %v", err)
	}
	return string(raw)
}

func readBridgeEventPayloadByID(t *testing.T, db *sql.DB, sessionID string, eventID string) string {
	t.Helper()
	var payload string
	if err := db.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND event_id = $2`,
		sessionID,
		eventID,
	).Scan(&payload); err != nil {
		t.Fatalf("read Bridge event payload %s: %v", eventID, err)
	}
	return payload
}

func assertDurableInterAgentOrderedPublicContentPreservesRuntimeMessage(t *testing.T, raw string) {
	t.Helper()
	var payload struct {
		Message struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			Parts []json.RawMessage `json:"parts"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode ordered durable inter-agent payload: %v", err)
	}
	if len(payload.Message.Content) != 2 ||
		payload.Message.Content[0].Type != "text" || payload.Message.Content[0].Text != "first public block" ||
		payload.Message.Content[1].Type != "text" || payload.Message.Content[1].Text != "second public block" {
		t.Fatalf("durable ordered public content = %+v; want original two-block order", payload.Message.Content)
	}
	if len(payload.Message.Parts) != 1 {
		t.Fatalf("durable repair parts = %d; want original Runtime repair part retained", len(payload.Message.Parts))
	}
}

func assertProjectedSentInterAgentEvent(t *testing.T, raw []byte, targetThreadID string, targetTaskName string, wantTaskName bool) {
	t.Helper()
	var event map[string]json.RawMessage
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("decode projected sent event: %v", err)
	}
	wantFieldCount := 5
	if wantTaskName {
		wantFieldCount++
	}
	if len(event) != wantFieldCount || string(event["to_session_thread_id"]) != strconv.Quote(targetThreadID) ||
		string(event["content"]) != `[{"text":"first public block","type":"text"},{"text":"second public block","type":"text"}]` {
		t.Fatalf("projected sent event has wrong exact fields/values: %s", raw)
	}
	if wantTaskName {
		if string(event["to_agent_name"]) != strconv.Quote(targetTaskName) {
			t.Fatalf("projected to_agent_name = %s; want %q: %s", event["to_agent_name"], targetTaskName, raw)
		}
	} else if _, exists := event["to_agent_name"]; exists {
		t.Fatalf("projected primary target retained to_agent_name: %s", raw)
	}
}

func assertRejectedSentInterAgentWriteHasNoDurableSideEffects(t *testing.T, db *sql.DB, sessionID string, runtimeWriteID string) {
	t.Helper()
	var eventCount int
	var streamChangeCount int
	var bridgeOperationCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*)
			   FROM session_events
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND runtime_write_id = $2),
			(SELECT count(*)
			   FROM session_event_stream_changes
			  WHERE workspace_id = 'default'
			    AND session_id = $1),
			(SELECT count(*)
			   FROM session_bridge_operations
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND (idempotency_key = $2 OR runtime_write_id = $2))`,
		sessionID,
		runtimeWriteID,
	).Scan(&eventCount, &streamChangeCount, &bridgeOperationCount); err != nil {
		t.Fatalf("read malformed sent-event durable side effects for %s: %v", runtimeWriteID, err)
	}
	if eventCount != 0 || streamChangeCount != 0 || bridgeOperationCount != 0 {
		t.Fatalf(
			"malformed sent-event durable rows for %s = events %d stream changes %d bridge operations %d; want all zero",
			runtimeWriteID,
			eventCount,
			streamChangeCount,
			bridgeOperationCount,
		)
	}
}

func assertRejectedReceivedInterAgentCommitHasNoDurableSideEffects(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	targetThreadID string,
	runtimeInputID string,
	deliveryID string,
) {
	t.Helper()
	var eventCount int
	var messageCount int
	var streamChangeCount int
	var bridgeOperationCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT
			(SELECT count(*)
			   FROM session_events
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND session_thread_id = $2
			    AND type = 'agent.thread_message_received'
			    AND payload_json::jsonb ->> 'delivery_id' = $4),
			(SELECT count(*)
			   FROM session_messages
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND session_thread_id = $2),
			(SELECT count(*)
			   FROM session_event_stream_changes
			  WHERE workspace_id = 'default'
			    AND session_id = $1),
			(SELECT count(*)
			   FROM session_bridge_operations
			  WHERE workspace_id = 'default'
			    AND session_id = $1
			    AND (idempotency_key = $3 OR runtime_input_id = $3))`,
		sessionID,
		targetThreadID,
		runtimeInputID,
		deliveryID,
	).Scan(&eventCount, &messageCount, &streamChangeCount, &bridgeOperationCount); err != nil {
		t.Fatalf("read malformed received-message durable side effects for %s: %v", runtimeInputID, err)
	}
	if eventCount != 0 || messageCount != 0 || streamChangeCount != 0 || bridgeOperationCount != 0 {
		t.Fatalf(
			"malformed received-message durable rows for %s = events %d messages %d stream changes %d bridge operations %d; want all zero",
			runtimeInputID,
			eventCount,
			messageCount,
			streamChangeCount,
			bridgeOperationCount,
		)
	}
	var inboxStatus string
	var inboxUpdatedAt string
	var inboxCommittedAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, updated_at, committed_at
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = $1`,
		runtimeInputID,
	).Scan(&inboxStatus, &inboxUpdatedAt, &inboxCommittedAt); err != nil {
		t.Fatalf("read accepted inbox after malformed received message for %s: %v", runtimeInputID, err)
	}
	if inboxStatus != "accepted" || inboxUpdatedAt != "2026-01-01T00:00:00Z" || inboxCommittedAt.Valid {
		t.Fatalf(
			"accepted inbox after malformed received message %s = status %q updated_at %q committed_at %v; want unchanged accepted row",
			runtimeInputID,
			inboxStatus,
			inboxUpdatedAt,
			inboxCommittedAt,
		)
	}
}

func assertDurableInterAgentPublicContentPreservesRuntimeMessage(t *testing.T, raw string, wantText string) {
	t.Helper()
	var payload struct {
		Message struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			Parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode durable inter-agent payload: %v", err)
	}
	if len(payload.Message.Content) != 1 || payload.Message.Content[0].Type != "text" || payload.Message.Content[0].Text != wantText {
		t.Fatalf("durable public message content = %+v; want ordered text %q", payload.Message.Content, wantText)
	}
	if len(payload.Message.Parts) != 1 || payload.Message.Parts[0].Type != "text" || payload.Message.Parts[0].Text != wantText {
		t.Fatalf("durable repair message parts = %+v; want original runtime text %q retained", payload.Message.Parts, wantText)
	}
}

func memoryCreateInputJSON(t *testing.T, path string, content string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"action":  "create",
		"path":    path,
		"content": content,
	})
	if err != nil {
		t.Fatalf("marshal memory create input: %v", err)
	}
	return string(raw)
}

func memoryReplaceInputJSON(t *testing.T, path string, oldText string, newText string, replaceAll bool) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"action":      "replace",
		"path":        path,
		"old_text":    oldText,
		"new_text":    newText,
		"replace_all": replaceAll,
	})
	if err != nil {
		t.Fatalf("marshal memory replace input: %v", err)
	}
	return string(raw)
}

func memoryDeleteInputJSON(t *testing.T, path string, expectedText string) string {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"action":        "delete",
		"path":          path,
		"expected_text": expectedText,
	})
	if err != nil {
		t.Fatalf("marshal memory delete input: %v", err)
	}
	return string(raw)
}

func verifyRuntimeBindingTokenForTest(t *testing.T, token string, key []byte) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "rtbt_v1" {
		t.Fatalf("runtime binding token = %q; want rtbt_v1 token", token)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(parts[1]))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if parts[2] != expected {
		t.Fatalf("runtime binding token signature mismatch")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode runtime binding token payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(body, &claims); err != nil {
		t.Fatalf("parse runtime binding token payload: %v", err)
	}
	return claims
}

func bridgeThreadVisibility(t *testing.T, db *sql.DB, threadID string) string {
	t.Helper()
	var visibility string
	if err := db.QueryRowContext(context.Background(), `SELECT visibility FROM session_threads WHERE id = $1`, threadID).Scan(&visibility); err != nil {
		t.Fatalf("read thread visibility: %v", err)
	}
	return visibility
}

func seedBridgeAPISession(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	now := "2026-01-01T00:00:00Z"
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, $2) ON CONFLICT (id) DO NOTHING`, []any{workspaceID, now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, $2, $2, 1, $3, $3)`, []any{workspaceID, agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, $2, $3, 1, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $4, $5)`, []any{workspaceID, agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', $3, $3)`, []any{workspaceID, environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, installed_tools_json, created_at, updated_at) VALUES ($1, $2, $3, 'session', 'idle', 'active', $4, 1, $5, '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}', $6, $6)`, []any{workspaceID, sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ($1, $2, $3, 'main', 'public', 'idle', $4, $4, $4)`, []any{workspaceID, threadID, sessionID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed bridge api session statement %q: %v", statement.query, err)
		}
	}
}

func seedBridgeAPIInternalToolCallMessage(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, messageID string, partID string, toolCallID string, toolName string, sequence int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		messageID,
		sequence,
		bridgeInternalToolCallMessageJSON(t, sessionID, messageID, partID, toolCallID, toolName),
	); err != nil {
		t.Fatalf("seed bridge api internal tool call message: %v", err)
	}
}

func seedBridgeAPIAgentConfig(t *testing.T, db *sql.DB, workspaceID string, sessionID string, configJSON string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE agent_versions
		    SET config_json = $3
		  WHERE workspace_id = $1
		    AND agent_id = $2
		    AND version = 1`,
		workspaceID,
		"agent_"+sessionID,
		configJSON,
	)
	if err != nil {
		t.Fatalf("seed bridge api agent config: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("seed bridge api agent config affected %d rows; want 1", affected)
	}
}

func seedBridgeAPIInternalReviewerThread(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, reviewerThreadID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'approval_reviewer', 'internal', 'idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		reviewerThreadID,
		sessionID,
		parentThreadID,
	); err != nil {
		t.Fatalf("seed bridge api internal reviewer thread: %v", err)
	}
}

func seedBridgeAPIChildThread(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, childThreadID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			task_name, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'subagent', 'public', 'idle',
			$5, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		childThreadID,
		sessionID,
		parentThreadID,
		"task_"+childThreadID,
	); err != nil {
		t.Fatalf("seed bridge api child thread: %v", err)
	}
}

func seedBridgeAPIChildFinishIdleFailureFixture(t *testing.T, db *sql.DB, suffix string) {
	t.Helper()
	sessionID := "sesn_bridge_child_finish_idle_" + suffix
	mainThreadID := "thr_bridge_child_finish_idle_main_" + suffix
	childThreadID := "thr_bridge_child_finish_idle_" + suffix
	seedBridgeAPISession(t, db, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, db, "default", sessionID, mainThreadID, childThreadID)
	seedBridgeAPIEvent(t, db, "default", sessionID, childThreadID, "evt_bridge_child_created_"+suffix, 1, "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":"`+mainThreadID+`","source_tool_use_event_id":"sevt_bridge_child_spawn_`+suffix+`"}`)
	seedBridgeAPIRuntimeBinding(t, db, "default", sessionID, "bind_bridge_child_finish_idle_"+suffix, 1, "pod_uid_child_finish_idle_"+suffix)
	seedBridgeAPIPreparationReady(t, db, "default", sessionID, "prep_bridge_child_finish_idle_"+suffix)
	seedBridgeAPIActiveSandbox(t, db, "default", sessionID, "2026-01-01T00:00:00Z")
	if _, err := db.ExecContext(context.Background(),
		`UPDATE sessions
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND id = $1`, sessionID); err != nil {
		t.Fatalf("seed child FinishIdle failure session running: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'running'
		  WHERE workspace_id = 'default'
		    AND session_id = $1`, sessionID); err != nil {
		t.Fatalf("seed child FinishIdle failure threads running: %v", err)
	}
}

func bridgeAPIChildFinishIdleFailureRequest(suffix string) *bridgev1.FinishIdleRequest {
	return &bridgev1.FinishIdleRequest{
		Scope: bridgeAPIScope(
			"sesn_bridge_child_finish_idle_"+suffix,
			"thr_bridge_child_finish_idle_"+suffix,
			"bind_bridge_child_finish_idle_"+suffix,
			1,
			"pod_uid_child_finish_idle_"+suffix,
		),
		RuntimeWriteId: "rwrite_bridge_child_finish_idle_" + suffix,
		IdleSince:      "2026-01-01T00:00:45Z",
		StopReasonJson: `{"type":"end_turn"}`,
	}
}

func assertBridgeAPIChildFinishIdleFailedClosed(t *testing.T, db *sql.DB, suffix string) {
	t.Helper()
	sessionID := "sesn_bridge_child_finish_idle_" + suffix
	childThreadID := "thr_bridge_child_finish_idle_" + suffix
	runtimeWriteID := "rwrite_bridge_child_finish_idle_" + suffix
	var idleEventCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND type IN ('session.thread_status_idle', 'session.status_idle')`, sessionID).Scan(&idleEventCount); err != nil {
		t.Fatalf("count child idle projections after capture failure: %v", err)
	}
	var streamChangeCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_event_stream_changes c
		   JOIN session_events e
		     ON e.workspace_id = c.workspace_id
		    AND e.session_id = c.session_id
		    AND e.event_id = c.event_id
		  WHERE e.workspace_id = 'default'
		    AND e.session_id = $1
		    AND e.runtime_write_id = $2`, sessionID, runtimeWriteID).Scan(&streamChangeCount); err != nil {
		t.Fatalf("count child idle stream changes after capture failure: %v", err)
	}
	var operationCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND session_thread_id = $2
		    AND operation = 'finish_idle'
		    AND idempotency_key = $3`, sessionID, childThreadID, runtimeWriteID).Scan(&operationCount); err != nil {
		t.Fatalf("count child finish_idle operations after capture failure: %v", err)
	}
	var outputCaptureCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_output_captures
		  WHERE workspace_id = 'default'
		    AND session_id = $1`, sessionID).Scan(&outputCaptureCount); err != nil {
		t.Fatalf("count child output captures after capture failure: %v", err)
	}
	var childStatus string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND id = $2`, sessionID, childThreadID).Scan(&childStatus); err != nil {
		t.Fatalf("read child status after capture failure: %v", err)
	}
	if idleEventCount != 0 || streamChangeCount != 0 || operationCount != 0 || outputCaptureCount != 0 || childStatus != "running" {
		t.Fatalf("child capture failure state = idleEvents %d streamChanges %d operations %d outputCaptures %d status %q; want 0/0/0/0/running",
			idleEventCount, streamChangeCount, operationCount, outputCaptureCount, childStatus)
	}
	var sessionStatus string
	var mainThreadStatus string
	if err := db.QueryRowContext(context.Background(),
		`SELECT s.status, st.status
		   FROM sessions s
		   JOIN session_threads st
		     ON st.workspace_id = s.workspace_id
		    AND st.session_id = s.id
		    AND st.id = s.main_thread_id
		  WHERE s.workspace_id = 'default'
		    AND s.id = $1`, sessionID).Scan(&sessionStatus, &mainThreadStatus); err != nil {
		t.Fatalf("read session state after child capture failure: %v", err)
	}
	if sessionStatus != "running" || mainThreadStatus != "running" {
		t.Fatalf("session/main status after child capture failure = %q/%q; want running/running", sessionStatus, mainThreadStatus)
	}
}

func assertBridgeAPIChildFinishIdlePreservesSessionState(t *testing.T, db *sql.DB, sessionID string, mainThreadID string, bindingID string, bindingGeneration int64) {
	t.Helper()
	var sessionStatus string
	var mainThreadStatus string
	if err := db.QueryRowContext(context.Background(),
		`SELECT s.status, st.status
		   FROM sessions s
		   JOIN session_threads st
		     ON st.workspace_id = s.workspace_id
		    AND st.session_id = s.id
		    AND st.id = $2
		  WHERE s.workspace_id = 'default'
		    AND s.id = $1`, sessionID, mainThreadID).Scan(&sessionStatus, &mainThreadStatus); err != nil {
		t.Fatalf("read protected session/main thread status: %v", err)
	}
	if sessionStatus != "running" || mainThreadStatus != "running" {
		t.Fatalf("protected session/main thread status = %q/%q; want running/running", sessionStatus, mainThreadStatus)
	}
	var runtimeBindingID string
	var runtimeBindingGeneration int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT binding_id, binding_generation
		   FROM session_runtime_bindings
		  WHERE workspace_id = 'default'
		    AND session_id = $1`, sessionID).Scan(&runtimeBindingID, &runtimeBindingGeneration); err != nil {
		t.Fatalf("read protected runtime binding: %v", err)
	}
	if runtimeBindingID != bindingID || runtimeBindingGeneration != bindingGeneration {
		t.Fatalf("protected runtime binding = %q/%d; want %q/%d", runtimeBindingID, runtimeBindingGeneration, bindingID, bindingGeneration)
	}
	var runtimeStatus string
	var statusEventID string
	var idleSince sql.NullString
	var cleanupAfter sql.NullString
	var cleanupEnqueuedAt string
	var cleanupClaimedAt string
	var cleanupJobID string
	var statusBindingID string
	var statusBindingGeneration int64
	var statusUpdatedAt string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, status_event_id, idle_since, cleanup_after,
		        cleanup_enqueued_at, cleanup_claimed_at, cleanup_job_id,
		        binding_id, binding_generation, updated_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = $1`, sessionID).Scan(
		&runtimeStatus,
		&statusEventID,
		&idleSince,
		&cleanupAfter,
		&cleanupEnqueuedAt,
		&cleanupClaimedAt,
		&cleanupJobID,
		&statusBindingID,
		&statusBindingGeneration,
		&statusUpdatedAt,
	); err != nil {
		t.Fatalf("read protected runtime status sentinel: %v", err)
	}
	if runtimeStatus != "running" ||
		statusEventID != "evt_child_status_session_running_sentinel" ||
		idleSince.Valid ||
		cleanupAfter.Valid ||
		cleanupEnqueuedAt != "2026-01-01T00:00:10Z" ||
		cleanupClaimedAt != "2026-01-01T00:00:11Z" ||
		cleanupJobID != "qjob_child_status_cleanup_sentinel" ||
		statusBindingID != "bind_child_status_runtime_sentinel" ||
		statusBindingGeneration != 41 ||
		statusUpdatedAt != "2026-01-01T00:00:12Z" {
		t.Fatalf("protected runtime status changed = status %q event %q idleSince=%v cleanupAfter=%v enqueued %q claimed %q job %q binding %q/%d updated %q",
			runtimeStatus,
			statusEventID,
			idleSince,
			cleanupAfter,
			cleanupEnqueuedAt,
			cleanupClaimedAt,
			cleanupJobID,
			statusBindingID,
			statusBindingGeneration,
			statusUpdatedAt,
		)
	}
}

func seedBridgeAPIRuntimeBinding(t *testing.T, db *sql.DB, workspaceID string, sessionID string, bindingID string, generation int64, podUID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral-agent-runtime', 'runtime-pod-0', $5, '10.0.0.10', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, bindingID, generation, podUID); err != nil {
		t.Fatalf("seed runtime binding: %v", err)
	}
}

func seedBridgeAPIRuntimeInput(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, bindingID string, podUID string, eventID string) {
	t.Helper()
	seedBridgeAPIEvent(t, db, workspaceID, sessionID, threadID, eventID, 1, "user.message", `{"content":[{"type":"text","text":"hello"}]}`)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'messages', $5, 1, 1, 'delivering', $6, 1, $7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, runtimeInputID, `["`+eventID+`"]`, bindingID, podUID); err != nil {
		t.Fatalf("seed runtime inbox: %v", err)
	}
}

func seedBridgeAPIRuntimeInbox(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, inputKind string, eventsJSON string, status string, bindingID string, podUID string, sequenceFrom int64, sequenceTo int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, runtimeInputID, inputKind, eventsJSON, sequenceFrom, sequenceTo, status, bindingID, podUID); err != nil {
		t.Fatalf("seed runtime inbox: %v", err)
	}
}

func seedBridgeAPIEvent(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64, eventType string, payloadJSON string) {
	t.Helper()
	var preparationAttemptID sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT (
		    SELECT preparation_attempt_id
		      FROM session_preparations
		     WHERE workspace_id = $1
		       AND session_id = $2
		     ORDER BY created_at DESC, preparation_attempt_id DESC
		     LIMIT 1
		)`,
		workspaceID,
		sessionID,
	).Scan(&preparationAttemptID); err != nil {
		t.Fatalf("read bridge api event birth: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			preparation_attempt_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, eventID, sequence, eventType, payloadJSON, preparationAttemptID); err != nil {
		t.Fatalf("seed bridge api event: %v", err)
	}
}

func seedBridgeAPIStreamChange(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, revision int64, visibility string, sessionVisible bool) int64 {
	t.Helper()
	var streamPosition int64
	if err := db.QueryRowContext(context.Background(),
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, '2026-01-01T00:00:00Z')
		RETURNING stream_position`,
		workspaceID, sessionID, eventID, threadID, revision, visibility, sessionVisible).Scan(&streamPosition); err != nil {
		t.Fatalf("seed bridge api stream change: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET latest_stream_position = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		workspaceID, sessionID, eventID, streamPosition); err != nil {
		t.Fatalf("seed bridge api stream latest position: %v", err)
	}
	return streamPosition
}

func seedBridgeAPITaskNotificationInbox(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, bindingID string, podUID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, status, binding_id, binding_generation, target_pod_uid, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'task_notification', '[]', 'accepted', $5, 1, $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, runtimeInputID, bindingID, podUID); err != nil {
		t.Fatalf("seed task notification inbox: %v", err)
	}
}

func seedBridgeAPIBackgroundTask(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, bindingID string, taskID string, sourceToolUseEventID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_background_tasks (
			workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
			binding_id, sandbox_id, provider_session_id, provider_command_id,
			provider_command_metadata_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'provider_session_notify', 'provider_command_notify', '{}', 'running', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, taskID, sourceToolUseEventID, bindingID, "sandbox_"+sessionID); err != nil {
		t.Fatalf("seed background task: %v", err)
	}
}

func seedBridgeAPIPendingApproval(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, toolUseEventID string, sequence int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.tool_use', $6, 'public', true, 'mrq_pending_approval', $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		toolUseEventID,
		sequence,
		`{"type":"agent.tool_use","name":"dangerous_tool","input":{},"evaluated_permission":"ask"}`,
	); err != nil {
		t.Fatalf("seed pending approval tool event: %v", err)
	}
	seedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, toolUseEventID, 1, "public", true)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, kind, input_json, status, expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'toolu_cleanup_wait', 'dangerous_tool', 'approval', '{}', 'pending',
			'2026-01-01T00:30:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		toolUseEventID,
	); err != nil {
		t.Fatalf("seed pending approval row: %v", err)
	}
}

func setBridgeAPIPendingApprovalStatus(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, toolUseEventID string, status string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = $5,
		        updated_at = '2026-01-01T00:00:01Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4`,
		workspaceID,
		sessionID,
		threadID,
		toolUseEventID,
		status,
	); err != nil {
		t.Fatalf("set pending approval status: %v", err)
	}
}

func seedBridgeAPIUserMessageEvent(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'user.message', $6, 'public', true, $6, '2026-01-01T00:31:00Z', '2026-01-01T00:31:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		eventID,
		sequence,
		`{"type":"user.message","content":[{"type":"text","text":"next turn"}]}`,
	); err != nil {
		t.Fatalf("seed post-claim user message: %v", err)
	}
	seedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, eventID, 1, "public", true)
}

func seedBridgeAPIToolConfirmationEvent(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64, toolUseEventID string, decision string) {
	t.Helper()
	payload, err := json.Marshal(map[string]string{
		"type":        "user.tool_confirmation",
		"tool_use_id": toolUseEventID,
		"result":      decision,
	})
	if err != nil {
		t.Fatalf("marshal tool confirmation payload: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'user.tool_confirmation', $6, 'public', true, $6, '2026-01-01T00:31:05Z', '2026-01-01T00:31:05Z')`,
		workspaceID,
		sessionID,
		threadID,
		eventID,
		sequence,
		string(payload),
	); err != nil {
		t.Fatalf("seed tool confirmation event: %v", err)
	}
	seedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, eventID, 1, "public", true)
}

func seedBridgeAPIWritableMemoryStore(t *testing.T, db *sql.DB, workspaceID string, sessionID string, storeID string) {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	resourceID := "res_" + strings.TrimPrefix(storeID, "memstore_")
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		 VALUES ($1, $2, $2, $3, $3)`,
		workspaceID, storeID, now); err != nil {
		t.Fatalf("seed memory store: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', $4, $4)`,
		workspaceID, sessionID, resourceID, now); err != nil {
		t.Fatalf("seed session resource: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_memory_store_resources (
			workspace_id, session_id, resource_id, memory_store_id, access, name, mount_path
		) VALUES ($1, $2, $3, $4, 'read_write', 'memory', $5)`,
		workspaceID, sessionID, resourceID, storeID, "/mnt/memory/"+strings.TrimPrefix(storeID, "memstore_")); err != nil {
		t.Fatalf("seed writable memory resource: %v", err)
	}
}

func seedBridgeAPIMemoryStoreBinding(t *testing.T, db *sql.DB, workspaceID string, sessionID string, storeID string, access string, mountPath string) {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	resourceID := "res_" + strings.TrimPrefix(storeID, "memstore_") + "_" + strings.ReplaceAll(access, "_", "") + "_" + strings.Trim(strings.ReplaceAll(mountPath, "/", "_"), "_")
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', $4, $4)`,
		workspaceID, sessionID, resourceID, now); err != nil {
		t.Fatalf("seed extra memory session resource: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_memory_store_resources (
			workspace_id, session_id, resource_id, memory_store_id, access, name, mount_path
		) VALUES ($1, $2, $3, $4, $5, 'memory', $6)`,
		workspaceID, sessionID, resourceID, storeID, access, mountPath); err != nil {
		t.Fatalf("seed extra memory resource binding: %v", err)
	}
}

func seedBridgeAPIDetachedMemoryStoreBinding(t *testing.T, db *sql.DB, workspaceID string, sessionID string, storeID string, access string, mountPath string) {
	t.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	resourceID := "res_detached_" + strings.TrimPrefix(storeID, "memstore_") + "_" + strings.ReplaceAll(access, "_", "") + "_" + strings.Trim(strings.ReplaceAll(mountPath, "/", "_"), "_")
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		 VALUES ($1, $2, $2, $3, $3)
		 ON CONFLICT (workspace_id, memory_store_id) DO NOTHING`,
		workspaceID, storeID, now); err != nil {
		t.Fatalf("seed detached memory store: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, detached_at, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', $4, $4, $4)`,
		workspaceID, sessionID, resourceID, now); err != nil {
		t.Fatalf("seed detached memory session resource: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_memory_store_resources (
			workspace_id, session_id, resource_id, memory_store_id, access, name, mount_path
		) VALUES ($1, $2, $3, $4, $5, 'memory', $6)`,
		workspaceID, sessionID, resourceID, storeID, access, mountPath); err != nil {
		t.Fatalf("seed detached memory resource binding: %v", err)
	}
}

func seedBridgeAPIFileSessionResource(t *testing.T, db *sql.DB, workspaceID string, sessionID string, resourceID string) {
	t.Helper()
	now := "2026-01-01T00:00:00Z"
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'file', $4, $4)`,
		workspaceID,
		sessionID,
		resourceID,
		now,
	); err != nil {
		t.Fatalf("seed file session resource: %v", err)
	}
}

func seedBridgeAPIMemory(t *testing.T, db *sql.DB, workspaceID string, storeID string, memoryID string, path string, content string) {
	t.Helper()
	now := "2026-01-01T00:00:00Z"
	versionID := memoryID + "_ver"
	hash := sha256Hex(content)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed memory tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memories (
			workspace_id, memory_store_id, memory_id, current_version_id, path,
			content_sha256, content_size_bytes, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
		workspaceID, storeID, memoryID, versionID, path, hash, len([]byte(content)), now); err != nil {
		t.Fatalf("seed memory head: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, 'session_actor', 'sesn_seed')`,
		workspaceID, storeID, memoryID, versionID, path, content, hash, len([]byte(content)), now); err != nil {
		t.Fatalf("seed memory version: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed memory tx: %v", err)
	}
}

func seedBridgeAPIMemoryIdentities(t *testing.T, db *sql.DB, storeID string, count int) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin bridge memory identity seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memories (workspace_id, memory_store_id, memory_id, current_version_id, path, content_sha256, content_size_bytes, created_at, updated_at)
		 SELECT 'default', $1, 'mem_bridge_quota_identity_' || g, 'memver_bridge_quota_identity_' || g,
		        '/quota-identity-' || g || '.md', 'sha', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		   FROM generate_series(1, $2) AS g`,
		storeID, count); err != nil {
		t.Fatalf("seed bridge memory identities: %v", err)
	}
	if _, err := tx.ExecContext(context.Background(),
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) SELECT 'default', $1, 'mem_bridge_quota_identity_' || g, 'memver_bridge_quota_identity_' || g,
		         'created', '/quota-identity-' || g || '.md', 'x', 'sha', 1,
		         '2026-01-01T00:00:00Z', 'session_actor', 'sesn_bridge_quota'
		    FROM generate_series(1, $2) AS g`,
		storeID, count); err != nil {
		t.Fatalf("seed bridge memory identity versions: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit bridge memory identity seed: %v", err)
	}
}

func seedBridgeAPIAdditionalMemoryVersions(t *testing.T, db *sql.DB, storeID string, memoryID string, count int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) SELECT 'default', $1, $2, 'memver_bridge_quota_' || g, 'modified', '/quota.md', 'x',
		         'sha', 1, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_bridge_quota'
		    FROM generate_series(1, $3) AS g`,
		storeID, memoryID, count); err != nil {
		t.Fatalf("seed bridge memory versions: %v", err)
	}
}

func seedBridgeAPIRetainedMemoryPayload(t *testing.T, db *sql.DB, storeID string, memoryID string, bytes int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO memory_versions (
			workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
			content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
		) VALUES ('default', $1, $2, 'memver_bridge_retained_quota', 'modified', '/quota.md', repeat('x', $3),
		          'sha', $3, '2026-01-01T00:00:00Z', 'session_actor', 'sesn_bridge_quota')`,
		storeID, memoryID, bytes); err != nil {
		t.Fatalf("seed bridge retained memory payload: %v", err)
	}
}

func countBridgeAPIMemories(t *testing.T, db *sql.DB, storeID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM memories WHERE workspace_id = 'default' AND memory_store_id = $1`, storeID).Scan(&count); err != nil {
		t.Fatalf("count bridge memories: %v", err)
	}
	return count
}

func assertBridgeAPIMemoryHead(t *testing.T, db *sql.DB, storeID string, path string, content string) {
	t.Helper()
	var gotPath, gotContent string
	if err := db.QueryRowContext(context.Background(),
		`SELECT m.path, v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default' AND m.memory_store_id = $1 AND m.deleted_at IS NULL`, storeID).Scan(&gotPath, &gotContent); err != nil {
		t.Fatalf("read bridge memory head: %v", err)
	}
	if gotPath != path || gotContent != content {
		t.Fatalf("memory head = path %q content %q; want path %q content %q", gotPath, gotContent, path, content)
	}
}

func assertNoBridgeAPIRuntimeToolResult(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = $1 AND tool_use_event_id = $2`,
		sessionID, toolUseEventID).Scan(&count); err != nil {
		t.Fatalf("count runtime tool results: %v", err)
	}
	if count != 0 {
		t.Fatalf("runtime tool result rows after quota rejection = %d; want 0", count)
	}
}

func seedBridgeAPIPreparationReady(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string) {
	t.Helper()
	environmentID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at, ready_at
		) VALUES ($1, $2, $3, $4, 1, $5, 'ready', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, preparationID, environmentID, "sandbox_"+sessionID); err != nil {
		t.Fatalf("seed ready preparation: %v", err)
	}
}

func seedBridgeAPIPreparationFailed(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, failureReason string) {
	t.Helper()
	environmentID := "env_" + sessionID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, failure_stage, last_error_kind, failure_reason, retryable,
			created_at, updated_at, failed_at
		) VALUES ($1, $2, $3, $4, 1, $5, 'failed', 'resource_preparation', $6, $6, false,
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, preparationID, environmentID, "sandbox_"+sessionID, failureReason); err != nil {
		t.Fatalf("seed failed preparation: %v", err)
	}
}

func seedBridgeAPIResourceCredentialExpiresAt(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, expiresAt string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET resource_cred_expires_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		workspaceID, sessionID, preparationID, expiresAt,
	)
	if err != nil {
		t.Fatalf("seed resource credential expiry: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("seed resource credential expiry affected %d rows; want 1", affected)
	}
}

func seedBridgeAPIResourceRootsJSON(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, rootsJSON string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET resource_roots_json = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		workspaceID, sessionID, preparationID, rootsJSON,
	)
	if err != nil {
		t.Fatalf("seed resource roots json: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("seed resource roots json affected %d rows; want 1", affected)
	}
}

func seedBridgeAPIActiveSandbox(t *testing.T, db *sql.DB, workspaceID string, sessionID string, refreshedAt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_sandbox_id,
			machine_was_usable, created_at, updated_at, status_refreshed_at
		) VALUES ($1, $2, $3, 'active', 'tetral', $4, TRUE, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', $5)`,
		workspaceID, "sandbox_"+sessionID, sessionID, "provider_"+sessionID, refreshedAt); err != nil {
		t.Fatalf("seed active sandbox: %v", err)
	}
}

func setBridgeAPISandboxStatus(t *testing.T, db *sql.DB, workspaceID string, sessionID string, status string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(),
		`UPDATE sandboxes
		    SET status = $3
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		workspaceID,
		sessionID,
		status,
	)
	if err != nil {
		t.Fatalf("set sandbox status %s: %v", status, err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("set sandbox status %s affected %d rows; want 1", status, affected)
	}
}

type recordingOutputScanner struct {
	targets   []outputcapture.SandboxOutputTarget
	files     []outputcapture.SandboxOutputFile
	truncated bool
	err       error
}

func (s *recordingOutputScanner) ScanOutputs(_ context.Context, target outputcapture.SandboxOutputTarget) (outputcapture.SandboxOutputScan, error) {
	s.targets = append(s.targets, target)
	if s.err != nil {
		return outputcapture.SandboxOutputScan{}, s.err
	}
	return outputcapture.SandboxOutputScan{Files: s.files, Truncated: s.truncated}, nil
}

type blockingOutputScanner struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	mu          sync.Mutex
	targets     []outputcapture.SandboxOutputTarget
}

func newBlockingOutputScanner() *blockingOutputScanner {
	return &blockingOutputScanner{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingOutputScanner) ScanOutputs(ctx context.Context, target outputcapture.SandboxOutputTarget) (outputcapture.SandboxOutputScan, error) {
	s.mu.Lock()
	s.targets = append(s.targets, target)
	s.mu.Unlock()
	s.enterOnce.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return outputcapture.SandboxOutputScan{}, nil
	case <-ctx.Done():
		return outputcapture.SandboxOutputScan{}, ctx.Err()
	}
}

func (s *blockingOutputScanner) Release() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *blockingOutputScanner) CallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.targets)
}

type recordingMemoryProjectionRefresher struct {
	calls      []MemoryProjectionRefresh
	deadlines  []time.Time
	observedAt []time.Time
	err        error
}

func (r *recordingMemoryProjectionRefresher) RefreshMemoryProjection(ctx context.Context, refresh MemoryProjectionRefresh) error {
	copied := MemoryProjectionRefresh{
		Target:     refresh.Target,
		MountPaths: append([]string(nil), refresh.MountPaths...),
		Ops:        append([]MemoryProjectionOp(nil), refresh.Ops...),
	}
	r.calls = append(r.calls, copied)
	r.observedAt = append(r.observedAt, time.Now())
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Time{}
	}
	r.deadlines = append(r.deadlines, deadline)
	return r.err
}

type blockingMemoryProjectionRefresher struct {
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

func newBlockingMemoryProjectionRefresher(err error) *blockingMemoryProjectionRefresher {
	return &blockingMemoryProjectionRefresher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     err,
	}
}

func (r *blockingMemoryProjectionRefresher) RefreshMemoryProjection(context.Context, MemoryProjectionRefresh) error {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.err
}

func tryMemoryStoreMutationLockWithTimeout(db *sql.DB, storeID string, timeout time.Duration) error {
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, "SELECT set_config('lock_timeout', $1, true)", strconv.FormatInt(timeout.Milliseconds(), 10)+"ms"); err != nil {
		return err
	}
	if err := storage.AcquireMemoryStoreMutationLock(ctx, tx, "default", storeID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func seedPendingMemoryProjectionResult(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, toolUseID string, inputHash string, inputJSON string, resultJSON string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			memory_projection_state, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'memory', $5, 'memory', $6, 'committed', $7, $8, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		toolUseID,
		inputHash,
		inputJSON,
		resultJSON,
		memoryProjectionStatePending,
	); err != nil {
		t.Fatalf("seed pending memory projection result: %v", err)
	}
}

func assertMemoryProjectionOps(t *testing.T, got []MemoryProjectionOp, want []MemoryProjectionOp) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("memory projection ops = %+v; want %+v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("memory projection op[%d] = %+v; want %+v; full ops %+v", index, got[index], want[index], got)
		}
	}
}

type beforePutBlobStore struct {
	inner     blob.BlobStore
	beforePut func(key string)
}

type failDeleteBlobStore struct {
	inner     blob.BlobStore
	mu        sync.Mutex
	remaining int
}

func (s *failDeleteBlobStore) allowDeletes() {
	s.mu.Lock()
	s.remaining = 0
	s.mu.Unlock()
}

func (s *failDeleteBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *failDeleteBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *failDeleteBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *failDeleteBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *failDeleteBlobStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	if s.remaining > 0 {
		s.remaining--
		s.mu.Unlock()
		return errors.New("injected loser delete failure")
	}
	s.mu.Unlock()
	return s.inner.Delete(ctx, key)
}

func (s *failDeleteBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

type countingGetBlobStore struct {
	inner    blob.BlobStore
	getCalls int
}

func (s *countingGetBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	return s.inner.Put(ctx, key, content, size)
}

func (s *countingGetBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.getCalls++
	return s.inner.Get(ctx, key)
}

func (s *countingGetBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *countingGetBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *countingGetBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *countingGetBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

func (s *beforePutBlobStore) Put(ctx context.Context, key string, content io.Reader, size int64) error {
	if s.beforePut != nil {
		s.beforePut(key)
	}
	return s.inner.Put(ctx, key, content, size)
}

func (s *beforePutBlobStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}

func (s *beforePutBlobStore) HeadObject(ctx context.Context, key string) (blob.ObjectMetadata, error) {
	return s.inner.HeadObject(ctx, key)
}

func (s *beforePutBlobStore) CopyObject(ctx context.Context, sourceKey string, destinationKey string) error {
	return s.inner.CopyObject(ctx, sourceKey, destinationKey)
}

func (s *beforePutBlobStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *beforePutBlobStore) DeletePrefix(ctx context.Context, prefix string) error {
	return s.inner.DeletePrefix(ctx, prefix)
}

func waitForSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type recordingSandboxReleaseClient struct {
	requests      []SandboxReleaseRequest
	result        SandboxReleaseResult
	err           error
	beforeRelease func(SandboxReleaseRequest) error
}

func (c *recordingSandboxReleaseClient) ReleaseSandbox(_ context.Context, request SandboxReleaseRequest) (SandboxReleaseResult, error) {
	c.requests = append(c.requests, request)
	if c.beforeRelease != nil {
		if err := c.beforeRelease(request); err != nil {
			return SandboxReleaseResult{}, err
		}
	}
	if c.err != nil {
		return SandboxReleaseResult{}, c.err
	}
	if c.result.Status == "" {
		return SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"}, nil
	}
	return c.result, nil
}

type recordingRuntimeTargetResolver struct {
	jobs    []RuntimeJob
	binding runtimeBindingForDelivery
	err     error
}

func (r *recordingRuntimeTargetResolver) ResolveRuntimeTarget(_ context.Context, _ *dbconnect.Tx, job RuntimeJob) (runtimeBindingForDelivery, error) {
	r.jobs = append(r.jobs, job)
	if r.err != nil {
		return runtimeBindingForDelivery{}, r.err
	}
	return r.binding, nil
}

func capturedOutputFile(sourcePath string, body string) outputcapture.SandboxOutputFile {
	return outputcapture.SandboxOutputFile{
		SourcePath: sourcePath,
		Kind:       "regular",
		LinkCount:  1,
		SizeBytes:  int64(len(body)),
		SHA256:     sha256Hex(body),
		MIMEType:   "text/plain",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(body)), nil
		},
	}
}

type recordingSandboxToolExecutor struct {
	invocations   []SandboxToolInvocation
	healthChecks  []SandboxToolTarget
	execution     SandboxToolExecution
	reads         []SandboxCommandReference
	inputs        []SandboxCommandInput
	commandResult SandboxCommandResult
	inputResult   SandboxCommandResult
	cancelResult  SandboxCommandResult
	cancels       []SandboxCommandCancel
	onRun         func(SandboxToolInvocation)
	onRead        func(SandboxCommandReference)
	onInput       func(SandboxCommandInput)
	onCancel      func(SandboxCommandCancel)
	err           error
	healthErr     error
	commandErr    error
}

func (e *recordingSandboxToolExecutor) CheckHealth(_ context.Context, target SandboxToolTarget) error {
	e.healthChecks = append(e.healthChecks, target)
	return e.healthErr
}

func (e *recordingSandboxToolExecutor) RunTool(_ context.Context, invocation SandboxToolInvocation) (SandboxToolExecution, error) {
	e.invocations = append(e.invocations, invocation)
	if e.onRun != nil {
		e.onRun(invocation)
	}
	if e.err != nil {
		return SandboxToolExecution{}, e.err
	}
	return e.execution, nil
}

func (e *recordingSandboxToolExecutor) ReadCommandResult(_ context.Context, reference SandboxCommandReference) (SandboxCommandResult, error) {
	e.reads = append(e.reads, reference)
	if e.onRead != nil {
		e.onRead(reference)
	}
	if e.commandErr != nil {
		return SandboxCommandResult{}, e.commandErr
	}
	if e.commandResult.ResultJSON == "" {
		return SandboxCommandResult{ResultJSON: `{"status":"running"}`}, nil
	}
	return e.commandResult, nil
}

func (e *recordingSandboxToolExecutor) SendCommandInput(_ context.Context, input SandboxCommandInput) (SandboxCommandResult, error) {
	e.inputs = append(e.inputs, input)
	if e.onInput != nil {
		e.onInput(input)
	}
	if e.inputResult.ResultJSON != "" || e.inputResult.TerminalStatus != "" {
		return e.inputResult, nil
	}
	return SandboxCommandResult{ResultJSON: `{"status":"accepted"}`}, nil
}

func (e *recordingSandboxToolExecutor) CancelCommand(_ context.Context, cancel SandboxCommandCancel) (SandboxCommandResult, error) {
	e.cancels = append(e.cancels, cancel)
	if e.onCancel != nil {
		e.onCancel(cancel)
	}
	if e.cancelResult.ResultJSON != "" || e.cancelResult.TerminalStatus != "" {
		return e.cancelResult, nil
	}
	return SandboxCommandResult{ResultJSON: `{"status":"cancelled"}`, TerminalStatus: "cancelled"}, nil
}

type recordingTaskNotificationResultReader struct {
	reads      []recordedTaskNotificationRead
	resultJSON string
	err        error
}

type recordedTaskNotificationRead struct {
	scope                *bridgev1.RuntimeScope
	taskID               string
	sourceToolUseEventID string
}

func (r *recordingTaskNotificationResultReader) ReadTaskNotificationResult(_ context.Context, scope *bridgev1.RuntimeScope, taskID string, sourceToolUseEventID string) (string, error) {
	r.reads = append(r.reads, recordedTaskNotificationRead{scope: scope, taskID: taskID, sourceToolUseEventID: sourceToolUseEventID})
	if r.err != nil {
		return "", r.err
	}
	return r.resultJSON, nil
}

type recordingMCPManifestLister struct {
	requests []MCPManifestListRequest
	results  []MCPManifestListResult
	err      error
}

func (l *recordingMCPManifestLister) ListMCPTools(_ context.Context, request MCPManifestListRequest) (MCPManifestListResult, error) {
	l.requests = append(l.requests, request)
	if l.err != nil {
		return MCPManifestListResult{}, l.err
	}
	if len(l.results) == 0 {
		return MCPManifestListResult{}, nil
	}
	result := l.results[0]
	l.results = l.results[1:]
	return result, nil
}

func assertRuntimeInputRepairQueueJob(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, eventIDs []string, sequenceFrom int64, sequenceTo int64, inputKind string, priority int) {
	t.Helper()
	var payload string
	var partitionKey string
	var statusValue string
	var priorityValue int
	if err := db.QueryRowContext(context.Background(),
		`SELECT payload_json, partition_key, status, priority
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND dedupe_key = $3`,
		workspaceID,
		queue.KindRuntimeInput,
		queue.FormatRuntimeInputDedupeKey(workspace.ID(workspaceID), sessionID, runtimeInputID),
	).Scan(&payload, &partitionKey, &statusValue, &priorityValue); err != nil {
		t.Fatalf("read runtime input repair queue job: %v", err)
	}
	if want := queue.FormatSessionPartitionKey(workspace.ID(workspaceID), sessionID); partitionKey != want {
		t.Fatalf("runtime input repair partition = %q; want %q", partitionKey, want)
	}
	if statusValue != "pending" || priorityValue != priority {
		t.Fatalf("runtime input repair queue state = %s priority %d; want pending priority %d", statusValue, priorityValue, priority)
	}
	var parsed struct {
		WorkspaceID          string   `json:"workspace_id"`
		SessionID            string   `json:"session_id"`
		SessionThreadID      string   `json:"session_thread_id"`
		RuntimeInputID       string   `json:"runtime_input_id"`
		EventIDs             []string `json:"event_ids"`
		SequenceFrom         int64    `json:"sequence_from"`
		SequenceTo           int64    `json:"sequence_to"`
		InputKind            string   `json:"input_kind"`
		PreparationAttemptID string   `json:"preparation_attempt_id"`
	}
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("parse runtime input repair payload: %v", err)
	}
	if parsed.WorkspaceID != workspaceID ||
		parsed.SessionID != sessionID ||
		parsed.SessionThreadID != threadID ||
		parsed.RuntimeInputID != runtimeInputID ||
		parsed.SequenceFrom != sequenceFrom ||
		parsed.SequenceTo != sequenceTo ||
		parsed.InputKind != inputKind ||
		parsed.PreparationAttemptID == "" ||
		len(parsed.EventIDs) != len(eventIDs) {
		t.Fatalf("runtime input repair payload = %s; want canonical runtime input identity", payload)
	}
	for index := range eventIDs {
		if parsed.EventIDs[index] != eventIDs[index] {
			t.Fatalf("runtime input repair event_ids = %v; want %v", parsed.EventIDs, eventIDs)
		}
	}
}

func assertNoRuntimeInputRepairQueueJob(t *testing.T, db *sql.DB, workspaceID string, sessionID string, runtimeInputID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND dedupe_key = $3`,
		workspaceID,
		queue.KindRuntimeInput,
		queue.FormatRuntimeInputDedupeKey(workspace.ID(workspaceID), sessionID, runtimeInputID),
	).Scan(&count); err != nil {
		t.Fatalf("count runtime input repair queue job: %v", err)
	}
	if count != 0 {
		t.Fatalf("runtime input repair queue jobs = %d; want 0", count)
	}
}

func assertRuntimeMCPManifestQueueJob(t *testing.T, db *sql.DB, workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64) {
	t.Helper()
	var payload string
	var partitionKey string
	var statusValue string
	var payloadVersion int
	if err := db.QueryRowContext(context.Background(),
		`SELECT payload_json, partition_key, status, payload_version
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND payload_json::jsonb ->> 'session_id' = $3
		    AND payload_json::jsonb ->> 'mcp_server_name' = $4
		    AND (payload_json::jsonb ->> 'manifest_generation')::bigint = $5`,
		workspaceID,
		queue.KindRuntimeConfigUpdate,
		sessionID,
		mcpServerName,
		manifestGeneration,
	).Scan(&payload, &partitionKey, &statusValue, &payloadVersion); err != nil {
		t.Fatalf("read runtime MCP manifest queue job: %v", err)
	}
	if want := queue.FormatSessionPartitionKey(workspace.ID(workspaceID), sessionID); partitionKey != want {
		t.Fatalf("runtime MCP manifest queue partition = %q; want %q", partitionKey, want)
	}
	if statusValue != "pending" || payloadVersion != 2 {
		t.Fatalf("runtime MCP manifest queue status/version = %q/%d; want pending/2", statusValue, payloadVersion)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("parse runtime MCP manifest payload: %v", err)
	}
	want := map[string]any{
		"workspace_id":        workspaceID,
		"session_id":          sessionID,
		"mcp_server_name":     mcpServerName,
		"manifest_generation": float64(manifestGeneration),
	}
	if !reflect.DeepEqual(parsed, want) {
		t.Fatalf("runtime MCP manifest payload = %#v; want refs only %#v", parsed, want)
	}
}

func assertNoRuntimeMCPManifestQueueJob(t *testing.T, db *sql.DB, workspaceID string, sessionID string, mcpServerName string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND payload_json::jsonb ->> 'session_id' = $3
		    AND payload_json::jsonb ->> 'mcp_server_name' = $4`,
		workspaceID,
		queue.KindRuntimeConfigUpdate,
		sessionID,
		mcpServerName,
	).Scan(&count); err != nil {
		t.Fatalf("count runtime MCP manifest queue job: %v", err)
	}
	if count != 0 {
		t.Fatalf("runtime MCP manifest queue jobs = %d; want 0", count)
	}
}

func assertMemoryResultStatus(t *testing.T, raw string, want string) {
	t.Helper()
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("result JSON %q: %v", raw, err)
	}
	if payload.Status != want {
		t.Fatalf("memory result status = %q; want %q in %s", payload.Status, want, raw)
	}
}

func assertMemoryToolErrorCode(t *testing.T, raw string, want string) {
	t.Helper()
	var payload struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("result JSON %q: %v", raw, err)
	}
	if payload.Status != "tool_error" || payload.ErrorCode != want {
		t.Fatalf("memory result = status %q error %q; want tool_error/%s in %s", payload.Status, payload.ErrorCode, want, raw)
	}
}

func assertMemoryToolError(t *testing.T, raw string, wantCode string, wantReread bool) {
	t.Helper()
	var payload struct {
		Status         string `json:"status"`
		ErrorCode      string `json:"error_code"`
		RereadRequired bool   `json:"reread_required"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("result JSON %q: %v", raw, err)
	}
	if payload.Status != "tool_error" || payload.ErrorCode != wantCode || payload.RereadRequired != wantReread {
		t.Fatalf("memory result = status %q error %q reread %v; want tool_error/%s/%v in %s", payload.Status, payload.ErrorCode, payload.RereadRequired, wantCode, wantReread, raw)
	}
}

type memoryPathConflictWireHead struct {
	MemoryID string `json:"memory_id"`
	Path     string `json:"path"`
}

func assertMemoryPathConflictResult(t *testing.T, raw string, wantConflicts []memoryPathConflictWireHead, wantTotal int, wantTruncated bool) {
	t.Helper()
	var payload struct {
		Conflicts          []memoryPathConflictWireHead `json:"conflicts"`
		ConflictTotal      int                          `json:"conflict_total"`
		ConflictsTruncated bool                         `json:"conflicts_truncated"`
		ConflictingPaths   json.RawMessage              `json:"conflicting_paths"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("path conflict result JSON %q: %v", raw, err)
	}
	if payload.ConflictTotal != wantTotal || payload.ConflictsTruncated != wantTruncated || len(payload.Conflicts) != len(wantConflicts) {
		t.Fatalf("path conflict metadata = conflicts %+v total %d truncated %v; want %+v/%d/%v in %s",
			payload.Conflicts, payload.ConflictTotal, payload.ConflictsTruncated, wantConflicts, wantTotal, wantTruncated, raw)
	}
	for index := range wantConflicts {
		if payload.Conflicts[index] != wantConflicts[index] {
			t.Fatalf("path conflict[%d] = %+v; want %+v in %s", index, payload.Conflicts[index], wantConflicts[index], raw)
		}
	}
	if payload.ConflictingPaths != nil {
		t.Fatalf("path conflict returned legacy conflicting_paths in %s", raw)
	}
}

func assertMemoryProjectionRefreshed(t *testing.T, raw string, want bool) {
	t.Helper()
	var payload struct {
		ProjectionRefreshed bool `json:"projection_refreshed"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("result JSON %q: %v", raw, err)
	}
	if payload.ProjectionRefreshed != want {
		t.Fatalf("projection_refreshed = %v; want %v in %s", payload.ProjectionRefreshed, want, raw)
	}
}

func assertRuntimeToolErrorCode(t *testing.T, raw string, want string) {
	t.Helper()
	assertRuntimeToolErrorCodeWithRetryable(t, raw, want, true)
}

func assertRuntimeToolErrorCodeWithRetryable(t *testing.T, raw string, want string, retryable bool) {
	t.Helper()
	var payload struct {
		Status    string `json:"status"`
		ErrorCode string `json:"error_code"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("result JSON %q: %v", raw, err)
	}
	if payload.Status != "runtime_error" || payload.ErrorCode != want || payload.Retryable != retryable {
		t.Fatalf("tool result = status %q error %q retryable %v; want runtime_error/%s retryable=%v in %s", payload.Status, payload.ErrorCode, payload.Retryable, want, retryable, raw)
	}
}

func assertToolResultRuntimeMessage(t *testing.T, raw string, wantCallID string, wantName string, wantToolUseEventID string, wantState string, wantOutput string) {
	t.Helper()
	var message struct {
		Role   string `json:"role"`
		Origin string `json:"origin"`
		Status string `json:"status"`
		Parts  []struct {
			Type           string `json:"type"`
			ToolCallID     string `json:"toolCallId"`
			ToolName       string `json:"toolName"`
			ToolUseEventID string `json:"toolUseEventId"`
			State          struct {
				Status string `json:"status"`
				Input  struct {
					Value map[string]any `json:"value"`
				} `json:"input"`
				Output struct {
					Text string `json:"text"`
				} `json:"output"`
			} `json:"state"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		t.Fatalf("parse runtime message: %v\n%s", err, raw)
	}
	if message.Role != "assistant" || message.Origin != "agent" || message.Status != "completed" || len(message.Parts) != 1 {
		t.Fatalf("runtime message role/origin/status/parts = %q/%q/%q/%d; want assistant/agent/completed/1 in %s",
			message.Role, message.Origin, message.Status, len(message.Parts), raw)
	}
	part := message.Parts[0]
	if part.Type != "tool" || part.ToolCallID != wantCallID || part.ToolName != wantName ||
		part.ToolUseEventID != wantToolUseEventID || part.State.Status != wantState ||
		part.State.Output.Text != wantOutput || part.State.Input.Value["q"] != "x" {
		t.Fatalf("runtime tool part = type %q call %q name %q event %q state %q output %q input %#v; want tool/%s/%s/%s/%s/%s in %s",
			part.Type, part.ToolCallID, part.ToolName, part.ToolUseEventID, part.State.Status,
			part.State.Output.Text, part.State.Input.Value, wantCallID, wantName, wantToolUseEventID, wantState, wantOutput, raw)
	}
}

func assertNoRuntimeToolResult(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND tool_use_event_id = $2`,
		sessionID,
		toolUseEventID,
	).Scan(&count); err != nil {
		t.Fatalf("count runtime tool results: %v", err)
	}
	if count != 0 {
		t.Fatalf("runtime tool result count for %s/%s = %d; want none", sessionID, toolUseEventID, count)
	}
}

func assertStoredMemoryResultStatus(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string, want string) {
	t.Helper()
	assertMemoryResultStatus(t, storedMemoryResultJSON(t, db, sessionID, toolUseEventID), want)
}

func storedMemoryResultJSON(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string) string {
	t.Helper()
	var resultJSON string
	if err := db.QueryRowContext(context.Background(),
		`SELECT result_json
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND tool_use_event_id = $2`,
		sessionID,
		toolUseEventID,
	).Scan(&resultJSON); err != nil {
		t.Fatalf("read stored memory result: %v", err)
	}
	return resultJSON
}

func assertMemoryProjectionState(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string, want string) {
	t.Helper()
	var state sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT memory_projection_state
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND tool_use_event_id = $2`,
		sessionID,
		toolUseEventID,
	).Scan(&state); err != nil {
		t.Fatalf("read memory projection state: %v", err)
	}
	if !state.Valid || state.String != want {
		t.Fatalf("memory projection state = %v; want %q", state, want)
	}
}

func assertMemoryProjectionStateNull(t *testing.T, db *sql.DB, sessionID string, toolUseEventID string) {
	t.Helper()
	var state sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT memory_projection_state
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND tool_use_event_id = $2`,
		sessionID,
		toolUseEventID,
	).Scan(&state); err != nil {
		t.Fatalf("read memory projection state: %v", err)
	}
	if state.Valid {
		t.Fatalf("memory projection state = %q; want NULL", state.String)
	}
}

func assertMemoryDeleted(t *testing.T, db *sql.DB, storeID string, path string) {
	t.Helper()
	var deletedAt sql.NullString
	var contentSHA sql.NullString
	var contentSize sql.NullInt64
	var operation string
	if err := db.QueryRowContext(context.Background(),
		`SELECT m.deleted_at, m.content_sha256, m.content_size_bytes, v.operation
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = $1
		    AND m.path = $2`,
		storeID,
		path,
	).Scan(&deletedAt, &contentSHA, &contentSize, &operation); err != nil {
		t.Fatalf("read deleted memory: %v", err)
	}
	if !deletedAt.Valid || contentSHA.Valid || contentSize.Valid || operation != "deleted" {
		t.Fatalf("deleted memory state deleted=%v sha=%v size=%v op=%q; want deleted/null/null/deleted", deletedAt, contentSHA, contentSize, operation)
	}
}

func assertMemoryCurrentPathAndContent(t *testing.T, db *sql.DB, storeID string, memoryID string, wantPath string, wantContent string) {
	t.Helper()
	var pathValue string
	var content string
	if err := db.QueryRowContext(context.Background(),
		`SELECT m.path, v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = $1
		    AND m.memory_id = $2
		    AND m.deleted_at IS NULL`,
		storeID,
		memoryID,
	).Scan(&pathValue, &content); err != nil {
		t.Fatalf("read current memory: %v", err)
	}
	if pathValue != wantPath || content != wantContent {
		t.Fatalf("memory %s path/content = %q/%q; want %q/%q", memoryID, pathValue, content, wantPath, wantContent)
	}
}

func assertMemoryCurrentPathContentAndOperation(t *testing.T, db *sql.DB, storeID string, memoryID string, wantPath string, wantContent string, wantOperation string) {
	t.Helper()
	var pathValue string
	var content string
	var operation string
	if err := db.QueryRowContext(context.Background(),
		`SELECT m.path, v.content, v.operation
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = $1
		    AND m.memory_id = $2
		    AND m.deleted_at IS NULL`,
		storeID,
		memoryID,
	).Scan(&pathValue, &content, &operation); err != nil {
		t.Fatalf("read current memory operation: %v", err)
	}
	if pathValue != wantPath || content != wantContent || operation != wantOperation {
		t.Fatalf("memory %s path/content/operation = %q/%q/%q; want %q/%q/%q", memoryID, pathValue, content, operation, wantPath, wantContent, wantOperation)
	}
}

func equalStringSlices(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func assertBridgeResourceStillAttached(t *testing.T, db *sql.DB, sessionID string, resourceID string) {
	t.Helper()
	var detachedAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT detached_at
		   FROM session_resources
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND resource_id = $2`,
		sessionID,
		resourceID,
	).Scan(&detachedAt); err != nil {
		t.Fatalf("read detached resource: %v", err)
	}
	if detachedAt.Valid {
		t.Fatalf("resource %s detached_at = %v; want ordinary idle cleanup to keep session resource attached", resourceID, detachedAt)
	}
}

func assertBridgeResourcePrefixGCMarkerAbsent(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_resource_prefix_gc
		  WHERE workspace_id = 'default'
		    AND session_id = $1`,
		sessionID,
	).Scan(&count); err != nil {
		t.Fatalf("read resource prefix gc marker: %v", err)
	}
	if count != 0 {
		t.Fatalf("resource prefix gc marker count after ordinary idle cleanup = %d; want none", count)
	}
}

func assertSessionPrepareRequeued(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string) {
	t.Helper()
	assertSessionPrepareRequeuedWithCredentialExpiry(t, db, workspaceID, sessionID, preparationID, "")
}

func assertSessionPrepareRequeuedForCredentialRotation(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string) {
	t.Helper()
	assertSessionPrepareRequeuedWithCredentialExpiry(t, db, workspaceID, sessionID, preparationID, "")
}

func assertSessionPrepareRequeuedPreservingCredential(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, wantExpiresAt string) {
	t.Helper()
	assertSessionPrepareRequeuedWithCredentialExpiry(t, db, workspaceID, sessionID, preparationID, wantExpiresAt)
}

func assertSessionPrepareRequeuedWithCredentialExpiry(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string, wantExpiresAt string) {
	t.Helper()
	var latestPreparationID string
	var status string
	var readyAt sql.NullString
	var resourceCredExpiresAt sql.NullString
	var resourceRootsJSON sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id, status, ready_at, resource_cred_expires_at, resource_roots_json
			   FROM session_preparations
			  WHERE workspace_id = $1 AND session_id = $2 AND superseded_at IS NULL
			  ORDER BY created_at DESC, preparation_attempt_id DESC
			  LIMIT 1`,
		workspaceID, sessionID,
	).Scan(&latestPreparationID, &status, &readyAt, &resourceCredExpiresAt, &resourceRootsJSON); err != nil {
		t.Fatalf("read preparation after stale requeue: %v", err)
	}
	if latestPreparationID == preparationID {
		t.Fatalf("latest preparation attempt = %q; want fresh attempt superseding %q", latestPreparationID, preparationID)
	}
	var supersededAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT superseded_at
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		workspaceID,
		sessionID,
		preparationID,
	).Scan(&supersededAt); err != nil {
		t.Fatalf("read superseded preparation: %v", err)
	}
	if !supersededAt.Valid || supersededAt.String == "" {
		t.Fatalf("old preparation superseded_at = %v; want non-null", supersededAt)
	}
	if status != "pending" || readyAt.Valid || resourceRootsJSON.Valid {
		t.Fatalf("preparation status=%q ready_at=%v resource_cred_expires_at=%v resource_roots_json=%v; want pending without ready metadata", status, readyAt, resourceCredExpiresAt, resourceRootsJSON)
	}
	if wantExpiresAt == "" {
		if resourceCredExpiresAt.Valid {
			t.Fatalf("resource_cred_expires_at = %v; want NULL after requeue", resourceCredExpiresAt)
		}
	} else if !resourceCredExpiresAt.Valid || resourceCredExpiresAt.String != wantExpiresAt {
		t.Fatalf("resource_cred_expires_at = %v; want preserved credential expiry %q", resourceCredExpiresAt, wantExpiresAt)
	}
	var payload string
	var queueStatus string
	var maxAttempts int
	if err := db.QueryRowContext(context.Background(),
		`SELECT payload_json, status, max_attempts
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND dedupe_key = $3
		    AND status = 'pending'`,
		workspaceID,
		queue.KindSessionPrepare,
		queue.FormatSessionPrepareDedupeKey(workspace.ID(workspaceID), sessionID, latestPreparationID),
	).Scan(&payload, &queueStatus, &maxAttempts); err != nil {
		t.Fatalf("read requeued session_prepare job: %v", err)
	}
	if queueStatus != "pending" || !strings.Contains(payload, latestPreparationID) || !strings.Contains(payload, sessionID) {
		t.Fatalf("session_prepare job status=%q payload=%s; want pending payload for preparation", queueStatus, payload)
	}
	if strings.Contains(payload, "rotate_resource_credential") {
		t.Fatalf("session_prepare payload contains forbidden rotate control field: %s", payload)
	}
	if maxAttempts != queue.DefaultMaxAttempts {
		t.Fatalf("session_prepare max_attempts = %d; want preserved fixed budget %d", maxAttempts, queue.DefaultMaxAttempts)
	}
}

func assertSessionPreparationReady(t *testing.T, db *sql.DB, workspaceID string, sessionID string, preparationID string) {
	t.Helper()
	var status string
	var readyAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, ready_at
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		workspaceID, sessionID, preparationID,
	).Scan(&status, &readyAt); err != nil {
		t.Fatalf("read preparation ready state: %v", err)
	}
	if status != "ready" || !readyAt.Valid {
		t.Fatalf("preparation status=%q ready_at=%v; want ready with ready_at", status, readyAt)
	}
	var prepareJobs int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND dedupe_key = $3`,
		workspaceID,
		queue.KindSessionPrepare,
		queue.FormatSessionPrepareDedupeKey(workspace.ID(workspaceID), sessionID, preparationID),
	).Scan(&prepareJobs); err != nil {
		t.Fatalf("count session_prepare jobs: %v", err)
	}
	if prepareJobs != 0 {
		t.Fatalf("session_prepare jobs = %d; want none", prepareJobs)
	}
}

func countMemoryVersions(t *testing.T, db *sql.DB, storeID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM memory_versions WHERE workspace_id = 'default' AND memory_store_id = $1`,
		storeID,
	).Scan(&count); err != nil {
		t.Fatalf("count memory versions: %v", err)
	}
	return count
}
