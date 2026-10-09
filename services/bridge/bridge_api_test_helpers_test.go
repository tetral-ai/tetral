package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func repoRootFromBridgeTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "../.."))
}

func bridgeAgentMailCommitRequestForTest(
	t *testing.T,
	db *sql.DB,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	deliveryID string,
	sourceThreadID string,
	sourceToolUseEventID string,
	messageJSON string,
) *bridgev1.CommitInputsRequest {
	t.Helper()
	eventID := runtimecontrol.StableRuntimeID(
		"agent_mail_received_event",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		deliveryID,
	)
	var existing int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID,
	).Scan(&existing); err != nil {
		t.Fatalf("find admitted agent mail event: %v", err)
	}
	var sequence int64
	if existing == 0 {
		publicMessage, err := runtimecontrol.ValidatedPublicInterAgentMessageJSON(json.RawMessage(messageJSON))
		if err != nil {
			t.Fatalf("normalize admitted agent mail message: %v", err)
		}
		var sourceTaskName sql.NullString
		if err := db.QueryRowContext(context.Background(),
			`SELECT CASE WHEN role='main' THEN NULL ELSE task_name END
			   FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), sourceThreadID,
		).Scan(&sourceTaskName); err != nil {
			t.Fatalf("read agent mail source task name: %v", err)
		}
		if err := db.QueryRowContext(context.Background(),
			`SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_events
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&sequence); err != nil {
			t.Fatalf("allocate agent mail event sequence: %v", err)
		}
		payload, err := json.Marshal(map[string]any{
			"type":                     "agent.thread_message_received",
			"delivery_id":              deliveryID,
			"source_thread_id":         sourceThreadID,
			"source_task_name":         runtimecontrol.NullableJSONString(sourceTaskName),
			"source_tool_use_event_id": sourceToolUseEventID,
			"message":                  publicMessage,
		})
		if err != nil {
			t.Fatalf("marshal admitted agent mail event: %v", err)
		}
		seedBridgeAPIEvent(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence, "agent.thread_message_received", string(payload))
		sessionfixture.SeedBridgeAPIStreamChange(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, 1, "public", true)
	} else if err := db.QueryRowContext(context.Background(),
		`SELECT sequence FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID,
	).Scan(&sequence); err != nil {
		t.Fatalf("read admitted agent mail event sequence: %v", err)
	}
	var inboxExists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2)`,
		scope.GetWorkspaceId(), runtimeInputID,
	).Scan(&inboxExists); err != nil {
		t.Fatalf("find admitted agent mail inbox: %v", err)
	}
	if !inboxExists {
		sessionfixture.SeedBridgeAPIRuntimeInbox(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), runtimeInputID, "agent_mail", fmt.Sprintf("[%q]", eventID), "accepted", scope.GetBinding().GetBindingId(), scope.GetBinding().GetTargetPodUid(), sequence, sequence)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_runtime_inbox
		    SET status='accepted',
		        event_ids_json=$3,
		        sequence_from=$4,
		        sequence_to=$4,
		        binding_id=$5,
		        binding_generation=$6,
		        target_pod_uid=$7,
		        updated_at=now()
		  WHERE workspace_id=$1
		    AND runtime_input_id=$2
		    AND status IN ('queued', 'delivering', 'accepted')`,
		scope.GetWorkspaceId(), runtimeInputID, fmt.Sprintf("[%q]", eventID), sequence,
		scope.GetBinding().GetBindingId(), scope.GetBinding().GetBindingGeneration(), scope.GetBinding().GetTargetPodUid(),
	); err != nil {
		t.Fatalf("align admitted agent mail inbox: %v", err)
	}
	return &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: runtimeInputID,
	}
}

func bridgeMCPToolDeclarationForTest(modelToolCallID, toolName, serverName, inputJSON, permission string) *bridgev1.RuntimeToolDeclaration {
	return &bridgev1.RuntimeToolDeclaration{
		EventKind:                bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_MCP,
		ModelToolCallId:          modelToolCallID,
		ToolName:                 toolName,
		PublicExecutionInputJson: inputJSON,
		EvaluatedPermission:      permission,
		RouteCapability:          "mcp_execute",
		McpServerName:            bridgeString(serverName),
	}
}

func bridgeSignedReasoningToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission string) *bridgev1.RuntimeToolDeclaration {
	declaration := sessionfixture.BridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, permission)
	declaration.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{
		Text:                 "provider-declared reasoning",
		ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"sig_provider_context"}}`),
	}}
	return declaration
}

type panicSlogHandler struct{}

func (panicSlogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (panicSlogHandler) Handle(context.Context, slog.Record) error { panic("logger failed") }

func (panicSlogHandler) WithAttrs([]slog.Attr) slog.Handler { return panicSlogHandler{} }

func (panicSlogHandler) WithGroup(string) slog.Handler { return panicSlogHandler{} }

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

func seedBridgeAPIOpenDurableTurn(
	t *testing.T,
	db *sql.DB,
	scope *bridgev1.RuntimeScope,
	durableTurnID string,
) {
	t.Helper()
	var exists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events
			 WHERE workspace_id=$1
			   AND session_id=$2
			   AND session_thread_id=$3
			   AND event_id=$4
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		durableTurnID,
	).Scan(&exists); err != nil {
		t.Fatalf("inspect open durable turn: %v", err)
	}
	if exists {
		return
	}
	var role string
	if err := db.QueryRowContext(context.Background(),
		`SELECT role
		   FROM session_threads
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&role); err != nil {
		t.Fatalf("read durable turn thread role: %v", err)
	}
	var sequence int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence); err != nil {
		t.Fatalf("allocate durable turn fixture sequence: %v", err)
	}
	eventType := "session.thread_status_running"
	if role == "main" {
		eventType = "session.status_running"
	}
	seedBridgeAPIEvent(
		t,
		db,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		durableTurnID,
		sequence,
		eventType,
		`{"type":"`+eventType+`"}`,
	)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status='running'
		  WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	); err != nil {
		t.Fatalf("mark durable turn thread running: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE sessions
		    SET status='running'
		  WHERE workspace_id=$1 AND id=$2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	); err != nil {
		t.Fatalf("mark durable turn session running: %v", err)
	}
}

func testPostgreSQLAcceptSandboxExecutionIdentityFencing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity")
	sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity", "thr_bridge_tool_identity_other")
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_identity_other", "thr_bridge_tool_identity_foreign")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_identity_other", "bind_bridge_tool_identity_foreign", 1, "pod_uid_tool_identity_foreign")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity", "evt_tool_identity", 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"printf '<>&'","workdir":"/workspace"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET model_request_id = 'mreq_tool_identity',
		        projection_json = jsonb_build_object(
		          'model_tool_call_id', 'call_tool_identity',
		          'tool_name', payload_json::jsonb ->> 'name',
		          'provider_input', payload_json::jsonb -> 'input',
		          'canonical_execution_input', payload_json::jsonb -> 'input'
		        )
		  WHERE workspace_id = 'default' AND event_id = 'evt_tool_identity'`); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity", "evt_tool_identity")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	request := &bridgev1.AcceptSandboxExecutionRequest{
		Scope:          sessionfixture.BridgeAPIScope("sesn_bridge_tool_identity", "thr_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity"),
		ToolUseEventId: "evt_tool_identity",
	}
	first, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution: %v", err)
	}
	replay, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution replay: %v", err)
	}
	if first.GetCommitted() == nil || replay.GetDuplicate() == nil {
		t.Fatalf("accept first/replay = %+v / %+v; want committed then duplicate", first, replay)
	}

	for _, test := range []struct {
		name     string
		wantCode codes.Code
		mutate   func(*bridgev1.AcceptSandboxExecutionRequest)
	}{
		{name: "other_thread", wantCode: codes.FailedPrecondition, mutate: func(conflict *bridgev1.AcceptSandboxExecutionRequest) {
			conflict.Scope.SessionThreadId = "thr_bridge_tool_identity_other"
		}},
		{name: "other_session", wantCode: codes.FailedPrecondition, mutate: func(conflict *bridgev1.AcceptSandboxExecutionRequest) {
			conflict.Scope.SessionId = "sesn_bridge_tool_identity_other"
			conflict.Scope.SessionThreadId = "thr_bridge_tool_identity_foreign"
			conflict.Scope.Binding = &bridgev1.RuntimeBindingRef{
				BindingId: "bind_bridge_tool_identity_foreign", BindingGeneration: 1,
				TargetPodUid: "pod_uid_tool_identity_foreign", RuntimeProcessId: "process_pod_uid_tool_identity_foreign",
			}
		}},
	} {
		t.Run(test.name+" conflict", func(t *testing.T) {
			conflict := proto.Clone(request).(*bridgev1.AcceptSandboxExecutionRequest)
			test.mutate(conflict)
			if _, err := store.AcceptSandboxExecution(context.Background(), conflict); status.Code(err) != test.wantCode {
				t.Fatalf("AcceptSandboxExecution %s conflict error = %v; want %s", test.name, err, test.wantCode)
			}
		})
	}
	var rowCount int
	var claimStatus, claimOwner, claimLease sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(mcp_claim_status), max(mcp_claim_id), max(mcp_claim_lease_expires_at)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_identity' AND tool_use_event_id = 'evt_tool_identity'`,
	).Scan(&rowCount, &claimStatus, &claimOwner, &claimLease); err != nil {
		t.Fatalf("read terminal settlement row: %v", err)
	}
	if rowCount != 1 || claimStatus.Valid || claimOwner.Valid || claimLease.Valid {
		t.Fatalf("accepted rows/claims = %d/%+v/%+v/%+v; want one row and all MCP claim fields NULL", rowCount, claimStatus, claimOwner, claimLease)
	}
}

func seedBridgeAPIRequestStart(
	t *testing.T,
	store *PostgreSQLBridgeAPIStore,
	scope *bridgev1.RuntimeScope,
	writeID string,
	modelRequestID string,
	requestKind string,
	messageBoundary int64,
	consumedFileAttachments ...*bridgev1.FileAttachmentPair,
) *bridgev1.WriteEventResponse {
	t.Helper()
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:                         scope,
		RuntimeWriteId:                writeID,
		ModelRequestId:                modelRequestID,
		EventType:                     "span.model_request_start",
		PayloadJson:                   fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q}`, modelRequestID),
		ContextThroughMessageSequence: sessionfixture.BridgeAPIInt64(messageBoundary),
		RequestKind:                   requestKind,
		ConsumedFileAttachments:       consumedFileAttachments,
	})
	if err != nil {
		t.Fatalf("seed request start: %v", err)
	}
	return response
}

func assertBridgeUserContextProjection(t *testing.T, raw string, text string) {
	t.Helper()
	parts, err := runtimecontrol.DecodeStoredRuntimeContextParts(raw)
	if err != nil || len(parts) != 1 {
		t.Fatalf("decode projected user context: parts=%d err=%v raw=%s", len(parts), err, raw)
	}
	var part map[string]any
	if err := json.Unmarshal(parts[0], &part); err != nil {
		t.Fatalf("decode projected user text: %v", err)
	}
	if len(part) != 2 || part["type"] != "text" || part["text"] != text {
		t.Fatalf("projected user context part = %#v; want exact text %q", part, text)
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

func seedBridgeAPIToolDeclarationProjection(
	t *testing.T,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	threadID string,
	toolUseEventID string,
	modelToolCallID string,
	toolName string,
	inputJSON string,
	routeCapability string,
) {
	t.Helper()
	canonicalInput, _, err := runtimecontrol.CanonicalRunToolInput(inputJSON)
	if err != nil {
		t.Fatalf("canonicalize seeded Tool declaration input: %v", err)
	}
	projectionJSON, err := runtimecontrol.MarshalJSON(map[string]any{
		"event_type":                "agent.tool_use",
		"evaluated_permission":      "allow",
		"model_tool_call_id":        modelToolCallID,
		"tool_name":                 toolName,
		"provider_input":            json.RawMessage(canonicalInput),
		"canonical_execution_input": json.RawMessage(canonicalInput),
		"route_capability":          routeCapability,
		"state":                     "running",
	})
	if err != nil {
		t.Fatalf("marshal seeded Tool declaration projection: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events
		SET model_request_id=$5, projection_json=$6, model_tool_call_id=$7
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4 AND type='agent.tool_use'`,
		workspaceID, sessionID, threadID, toolUseEventID, "mreq_"+toolUseEventID, projectionJSON, modelToolCallID); err != nil {
		t.Fatalf("seed Tool declaration projection: %v", err)
	}
}

func seedBridgeAPIChildFinishIdleFailureFixture(t *testing.T, db *sql.DB, suffix string) {
	t.Helper()
	sessionID := "sesn_bridge_child_finish_idle_" + suffix
	mainThreadID := "thr_bridge_child_finish_idle_main_" + suffix
	childThreadID := "thr_bridge_child_finish_idle_" + suffix
	sessionfixture.SeedBridgeAPISession(t, db, "default", sessionID, mainThreadID)
	sessionfixture.SeedBridgeAPIChildThread(t, db, "default", sessionID, mainThreadID, childThreadID)
	seedBridgeAPIEvent(t, db, "default", sessionID, childThreadID, "evt_bridge_child_created_"+suffix, 1, "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":"`+mainThreadID+`","source_tool_use_event_id":"sevt_bridge_child_spawn_`+suffix+`"}`)
	seedBridgeAPIRuntimeBinding(t, db, "default", sessionID, "bind_bridge_child_finish_idle_"+suffix, 1, "pod_uid_child_finish_idle_"+suffix)
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
	seedBridgeAPIOpenDurableTurn(
		t,
		db,
		sessionfixture.BridgeAPIScope(
			sessionID,
			childThreadID,
			"bind_bridge_child_finish_idle_"+suffix,
			1,
			"pod_uid_child_finish_idle_"+suffix,
		),
		"evt_bridge_child_finish_idle_running_"+suffix,
	)
}

func bridgeAPIChildFinishIdleFailureRequest(suffix string) *bridgev1.FinishIdleRequest {
	scope := sessionfixture.BridgeAPIScope(
		"sesn_bridge_child_finish_idle_"+suffix,
		"thr_bridge_child_finish_idle_"+suffix,
		"bind_bridge_child_finish_idle_"+suffix,
		1,
		"pod_uid_child_finish_idle_"+suffix,
	)
	durableTurnID := "evt_bridge_child_finish_idle_running_" + suffix
	return &bridgev1.FinishIdleRequest{
		Scope:              scope,
		DurableTurnId:      durableTurnID,
		StopReasonJson:     `{"type":"end_turn"}`,
		CompletionMailText: bridgeString(completionMailEnvelope("main", "task_"+"thr_bridge_child_finish_idle_"+suffix, "completed")),
	}
}

func seedBridgeAPIRuntimeBinding(t *testing.T, db *sql.DB, workspaceID string, sessionID string, bindingID string, generation int64, podUID string) {
	t.Helper()
	processIdentity := runtimecontrol.ProcessIdentity{Namespace: "tetral-agent-runtime", PodUID: podUID, ID: "process_" + podUID}
	registered, err := runtimecontrol.RegisterProcess(context.Background(), dbconnect.NewClientForTesting(db), processIdentity)
	if err != nil {
		t.Fatalf("register fixture Runtime process: %v", err)
	}
	if _, _, err := runtimecontrol.ReportProcess(context.Background(), dbconnect.NewClientForTesting(db), processIdentity, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatalf("promote fixture Runtime process: %v", err)
	}

	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, runtime_process_id, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral-agent-runtime', 'runtime-pod-0', $5, '10.0.0.10', $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, bindingID, generation, podUID, processIdentity.ID); err != nil {
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

func seedBridgeAPIEvent(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64, eventType string, payloadJSON string) {
	t.Helper()
	relation := sessionfixture.ToolEventRelationForTest(t, db, workspaceID, eventID, eventType, payloadJSON)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, model_request_id, model_tool_call_id, tool_use_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, eventID, sequence, eventType, payloadJSON,
		relation.ModelRequestID, relation.ModelToolCallID, relation.ToolUseEventID); err != nil {
		t.Fatalf("seed bridge api event: %v", err)
	}
}

func seedBridgeAPIChildLifecycleToolSource(t *testing.T, db *sql.DB, sessionID string, parentID string, sourceID string) string {
	t.Helper()
	var taskName string
	if err := db.QueryRowContext(context.Background(), `SELECT task_name FROM session_threads
		WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'
		ORDER BY created_at,id LIMIT 1`, sessionID, parentID).Scan(&taskName); err != nil {
		t.Fatalf("read child task for lifecycle source: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"type": "agent.tool_use", "name": "close_agent", "input": map[string]any{"task_name": taskName},
	})
	if err != nil {
		t.Fatalf("marshal child lifecycle source: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,visibility,session_visible,
		model_request_id,model_tool_call_id,created_at,updated_at
	) SELECT 'default',$1,$2,$3,COALESCE(max(sequence),0)+1,'agent.tool_use',$4,'public',true,
		'mreq_' || $3,'call_' || $3,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'
	FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, parentID, sourceID, string(payload)); err != nil {
		t.Fatalf("seed child lifecycle Tool Use: %v", err)
	}
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, db, "default", sessionID, parentID, sourceID)
	return sourceID
}

func seedBridgeAPIBackgroundTask(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, bindingID string, taskID string, sourceToolUseEventID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, created_at, updated_at
		) SELECT $1, $2, $3, $4,
			COALESCE((SELECT MAX(sequence) + 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2), 1),
			'span.tool_use', '{}',
			'internal', false, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		WHERE NOT EXISTS (
			SELECT 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND event_id=$4
		)`,
		workspaceID, sessionID, threadID, sourceToolUseEventID); err != nil {
		t.Fatalf("seed background task source Tool Use: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_background_tasks (
			workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
			binding_id, sandbox_id, provider_session_id, provider_command_id,
			provider_command_metadata_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, 'provider_session_notify', 'provider_command_notify', '{}', 'running', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, taskID, sourceToolUseEventID, bindingID, "sandbox_"+sessionID); err != nil {
		t.Fatalf("seed background task: %v", err)
	}
}

func seedBridgeAPINotifiableBackgroundTask(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, bindingID string, taskID string, sourceToolUseEventID string) {
	t.Helper()
	seedBridgeAPIBackgroundTask(t, db, workspaceID, sessionID, threadID, bindingID, taskID, sourceToolUseEventID)
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events
		SET type='agent.tool_use',
		    payload_json='{"type":"agent.tool_use","name":"exec_command","input":{},"evaluated_permission":"allow"}',
		    model_request_id='mreq_' || event_id, model_tool_call_id='call_' || event_id
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4`,
		workspaceID, sessionID, threadID, sourceToolUseEventID); err != nil {
		t.Fatalf("mark background task source Tool Use: %v", err)
	}
	sessionfixture.SeedBridgeAPIDurableToolMessage(t, db, workspaceID, sessionID, threadID,
		"mreq_"+sourceToolUseEventID, sourceToolUseEventID, "call_"+sourceToolUseEventID, "exec_command")
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, model_request_id, tool_use_event_id, projection_json, created_at, updated_at
	) SELECT $1, $2, $3, 'evt_result_' || $4,
		COALESCE((SELECT MAX(sequence) + 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2), 1),
		'agent.tool_result', jsonb_build_object('type','agent.tool_result','tool_use_id',$4,'content',jsonb_build_array(jsonb_build_object('type','text','text','Background command accepted.'))),
		'internal', false, 'mreq_' || $4, $4,
		jsonb_build_object(
			'model_tool_call_id','call_' || $4,'tool_name','exec_command',
			'provider_input','{}'::jsonb,'canonical_execution_input','{}'::jsonb,'state','completed',
			'output',jsonb_build_object('text','Background command accepted.','truncated',false)
		),
		'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
	WHERE NOT EXISTS (SELECT 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND event_id='evt_result_' || $4)`,
		workspaceID, sessionID, threadID, sourceToolUseEventID); err != nil {
		t.Fatalf("seed background task source Tool Result: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `UPDATE session_messages
		SET data_json = jsonb_set(
			data_json::jsonb,
			'{parts}',
			(data_json::jsonb -> 'parts') || jsonb_build_array(jsonb_build_object(
				'type', 'tool_result',
				'modelToolCallId', 'call_' || $4,
				'result', jsonb_build_object(
					'type', 'completed',
					'output', jsonb_build_object('text', 'Background command accepted.')
				)
			))
		)::text
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND source_event_id=$4`,
		workspaceID, sessionID, threadID, sourceToolUseEventID); err != nil {
		t.Fatalf("seed background task durable Tool Result context: %v", err)
	}
}

func seedBridgeAPIPendingApproval(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, toolUseEventID string, sequence int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, model_tool_call_id, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.tool_use', $6, 'public', true, 'mrq_pending_approval',
			'toolu_cleanup_wait', '{"model_tool_call_id":"toolu_cleanup_wait"}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		toolUseEventID,
		sequence,
		`{"type":"agent.tool_use","name":"dangerous_tool","input":{},"evaluated_permission":"ask"}`,
	); err != nil {
		t.Fatalf("seed pending approval tool event: %v", err)
	}
	sessionfixture.SeedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, toolUseEventID, 1, "public", true)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'toolu_cleanup_wait', 'dangerous_tool', '{}', 'pending',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		sessionID,
		threadID,
		toolUseEventID,
	); err != nil {
		t.Fatalf("seed pending approval row: %v", err)
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

func seedBridgeAPIMemory(t *testing.T, db *sql.DB, workspaceID string, storeID string, memoryID string, path string, content string) {
	t.Helper()
	now := "2026-01-01T00:00:00Z"
	versionID := memoryID + "_ver"
	hash := runtimecontrol.Sha256Hex(content)
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

type recordingMCPManifestLister struct {
	requests []mcpmanifest.ListRequest
	results  []mcpmanifest.ListResult
	err      error
}

func (l *recordingMCPManifestLister) ListMCPTools(_ context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	l.requests = append(l.requests, request)
	if l.err != nil {
		return mcpmanifest.ListResult{}, l.err
	}
	if len(l.results) == 0 {
		return mcpmanifest.ListResult{}, nil
	}
	result := l.results[0]
	l.results = l.results[1:]
	return result, nil
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

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func mustLeaseBridgeQueueJob(t *testing.T, store *queue.PostgreSQLQueueStore, request queue.LeaseRequest) *queue.Job {
	t.Helper()
	jobs, err := store.Lease(context.Background(), request)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("lease Queue job = %#v/%v; want exactly one", jobs, err)
	}
	return jobs[0]
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

func enqueueInterruptExhaustionJob(
	t *testing.T,
	store *queue.PostgreSQLQueueStore,
	sessionID string,
	threadID string,
	inputID string,
	inputKind string,
	eventID string,
	sequence int64,
	maxAttempts int,
	now time.Time,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": inputKind,
	})
	if err != nil {
		t.Fatalf("marshal runtime input %s: %v", inputID, err)
	}
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: maxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue runtime input %s: %v", inputID, err)
	}
}
