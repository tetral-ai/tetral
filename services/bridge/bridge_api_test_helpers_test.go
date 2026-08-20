package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This file owns shared Bridge API store test fixtures and assertions.

func bridgeInterruptLeaseRef(job *queue.Job) *bridgev1.InterruptLeaseRef {
	if job == nil {
		return nil
	}
	return &bridgev1.InterruptLeaseRef{
		JobId: job.ID, LeaseToken: job.LeaseToken, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	}
}

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
	eventID := stableRuntimeID(
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
		publicMessage, err := validatedPublicInterAgentMessageJSON(json.RawMessage(messageJSON))
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
			"source_task_name":         nullableJSONString(sourceTaskName),
			"source_tool_use_event_id": sourceToolUseEventID,
			"message":                  publicMessage,
		})
		if err != nil {
			t.Fatalf("marshal admitted agent mail event: %v", err)
		}
		seedBridgeAPIEvent(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence, "agent.thread_message_received", string(payload))
		seedBridgeAPIStreamChange(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, 1, "public", true)
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
		seedBridgeAPIRuntimeInbox(t, db, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), runtimeInputID, "agent_mail", fmt.Sprintf("[%q]", eventID), "accepted", scope.GetBinding().GetBindingId(), scope.GetBinding().GetTargetPodUid(), sequence, sequence)
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

func bridgeTextContextDeltaForTest(text string) *bridgev1.RuntimeContextDelta {
	return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: text}}}}}
}

func bridgeToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission, routeCapability string) *bridgev1.RuntimeToolDeclaration {
	return &bridgev1.RuntimeToolDeclaration{
		EventKind:                bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_TOOL,
		ModelToolCallId:          modelToolCallID,
		ToolName:                 toolName,
		PublicExecutionInputJson: inputJSON,
		EvaluatedPermission:      permission,
		RouteCapability:          routeCapability,
	}
}

func bridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, permission string) *bridgev1.RuntimeToolDeclaration {
	routeCapability := "sandbox_execute"
	switch toolName {
	case "memory":
		routeCapability = "memory_execute"
	case "spawn_agent":
		routeCapability = "child_create"
	case "send_message", "send_input":
		routeCapability = "child_message"
	case "wait", "wait_agent", "wait_threads":
		routeCapability = "child_wait"
	case "interrupt_agent":
		routeCapability = "child_interrupt"
	case "close_agent":
		routeCapability = "child_close"
	case "resume_agent":
		routeCapability = "child_resume"
	case "list_agents":
		routeCapability = "child_list"
	case "background_command":
		routeCapability = "background_command"
	case "write_stdin":
		routeCapability = "background_command"
	case "web", "web_search", "web_fetch":
		routeCapability = "web_execute"
	}
	return bridgeToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission, routeCapability)
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
	declaration := bridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, permission)
	declaration.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{
		Text:                 "provider-declared reasoning",
		ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"sig_provider_context"}}`),
	}}
	return declaration
}

type panicSlogHandler struct{}

func (panicSlogHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panicSlogHandler) Handle(context.Context, slog.Record) error { panic("logger failed") }
func (panicSlogHandler) WithAttrs([]slog.Attr) slog.Handler        { return panicSlogHandler{} }
func (panicSlogHandler) WithGroup(string) slog.Handler             { return panicSlogHandler{} }

func bridgeCompletedToolSettlementForTest(toolUseEventID, textValue string) *bridgev1.RuntimeToolSettlement {
	return &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome: &bridgev1.RuntimeToolSettlement_Completed{
			Completed: &bridgev1.RuntimeToolCompleted{
				OutputJson: fmt.Sprintf(`{"text":%q,"truncated":false}`, textValue),
			},
		},
	}
}

func bridgeErrorToolSettlementForTest(toolUseEventID, message string) *bridgev1.RuntimeToolSettlement {
	return &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome: &bridgev1.RuntimeToolSettlement_Error{
			Error: &bridgev1.RuntimeToolError{
				ErrorJson: fmt.Sprintf(`{"type":"tool_error","message":%q}`, message),
			},
		},
	}
}

func bridgeToolSettlementRequestForTest(
	scope *bridgev1.RuntimeScope,
	settlement *bridgev1.RuntimeToolSettlement,
) *bridgev1.SettleToolResultRequest {
	return &bridgev1.SettleToolResultRequest{Scope: scope, Settlement: settlement}
}

func bridgeRequireToolSettlementOutcomeForTest(
	t *testing.T,
	response *bridgev1.SettleToolResultResponse,
	want string,
) {
	t.Helper()
	if response == nil {
		t.Fatalf("Tool settlement response is nil; want %s", want)
	}
	got := ""
	switch response.GetOutcome().(type) {
	case *bridgev1.SettleToolResultResponse_Committed:
		got = "committed"
	case *bridgev1.SettleToolResultResponse_Duplicate:
		got = "duplicate"
	case *bridgev1.SettleToolResultResponse_Stale:
		got = "stale"
	default:
		t.Fatalf("Tool settlement response has no closed outcome: %#v", response)
	}
	if got != want {
		t.Fatalf("Tool settlement outcome = %s; want %s", got, want)
	}
}

func bridgeTaskNotificationRequestForTest(t *testing.T, scope *bridgev1.RuntimeScope, runtimeInputID string) *bridgev1.CommitTaskNotificationResultRequest {
	t.Helper()
	return &bridgev1.CommitTaskNotificationResultRequest{
		Scope: scope, RuntimeInputId: runtimeInputID,
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

func nextBridgeAPIEventSequenceForTest(t *testing.T, db *sql.DB, sessionID string, threadID string) int64 {
	t.Helper()
	var sequence int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2`,
		sessionID,
		threadID,
	).Scan(&sequence); err != nil {
		t.Fatalf("allocate event fixture sequence: %v", err)
	}
	return sequence
}

func bridgeAPIFinishIdleRequest(
	t *testing.T,
	db *sql.DB,
	scope *bridgev1.RuntimeScope,
	durableTurnID string,
	stopReasonJSON string,
) *bridgev1.FinishIdleRequest {
	t.Helper()
	seedBridgeAPIOpenDurableTurn(t, db, scope, durableTurnID)
	return &bridgev1.FinishIdleRequest{
		Scope:          scope,
		DurableTurnId:  durableTurnID,
		StopReasonJson: stopReasonJSON,
	}
}

func seedReadySandboxForSharedToolExecution(t *testing.T, db *sql.DB, workspaceID string, sessionID string) {
	t.Helper()
	environmentID := "env_" + sessionID
	if _, err := db.Exec(`UPDATE environments SET current_generation=1 WHERE workspace_id=$1 AND id=$2`, workspaceID, environmentID); err != nil {
		t.Fatalf("set shared-tool environment generation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO environment_artifacts (
		workspace_id, environment_id, generation, status, provider, provider_artifact_ref,
		normalized_config_hash, artifact_input_hash, runtime_network_policy_json, packages_json,
		created_at, updated_at
	) VALUES ($1, $2, 1, 'ready', 'daytona', 'artifact_shared_tool_execution',
		'config_hash', 'artifact_hash', '{"type":"unrestricted"}', '{}', clock_timestamp(), clock_timestamp())`, workspaceID, environmentID); err != nil {
		t.Fatalf("seed shared-tool environment artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_credential_expires_at, resource_roots_json, provider_metadata_json,
		helper_verified_at, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 1, 'daytona', $5, 1, 1,
		clock_timestamp()+interval '2 hours', '[]', '{}', clock_timestamp(), clock_timestamp(), clock_timestamp())`,
		workspaceID, sessionID, "sbox_"+sessionID, environmentID, "provider_"+sessionID); err != nil {
		t.Fatalf("seed ready shared-tool Sandbox binding: %v", err)
	}
}

func testPostgreSQLAcceptSandboxExecutionIdentityFencing(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity", "thr_bridge_tool_identity_other")
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_identity_other", "thr_bridge_tool_identity_foreign")
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
	seedBridgeAPIAllowedToolRoute(t, admin, "default", "sesn_bridge_tool_identity", "thr_bridge_tool_identity", "evt_tool_identity")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	request := &bridgev1.AcceptSandboxExecutionRequest{
		Scope:          bridgeAPIScope("sesn_bridge_tool_identity", "thr_bridge_tool_identity", "bind_bridge_tool_identity", 1, "pod_uid_tool_identity"),
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
				TargetPodUid: "pod_uid_tool_identity_foreign",
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

func bridgeAPIScope(sessionID string, threadID string, bindingID string, generation int64, podUID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     "default",
		SessionId:       sessionID,
		SessionThreadId: threadID,
		Binding:         &bridgev1.RuntimeBindingRef{BindingId: bindingID, BindingGeneration: generation, TargetPodUid: podUID},
	}
}

func bridgeAPIInt64(value int64) *int64 {
	return &value
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
		ContextThroughMessageSequence: bridgeAPIInt64(messageBoundary),
		RequestKind:                   requestKind,
		ConsumedFileAttachments:       consumedFileAttachments,
	})
	if err != nil {
		t.Fatalf("seed request start: %v", err)
	}
	return response
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

func assertBridgeUserContextProjection(t *testing.T, raw string, text string) {
	t.Helper()
	parts, err := decodeStoredRuntimeContextParts(raw)
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

func bridgePublicMessageJSONForTest(t *testing.T, text string) string {
	t.Helper()
	raw, err := publicAgentMailMessageJSON(text)
	if err != nil {
		t.Fatalf("marshal public message content: %v", err)
	}
	return raw
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

func assertDurableInterAgentPublicContent(t *testing.T, raw string, wantText string) {
	t.Helper()
	var payload struct {
		Message struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode durable inter-agent payload: %v", err)
	}
	if len(payload.Message.Content) != 1 || payload.Message.Content[0].Type != "text" || payload.Message.Content[0].Text != wantText {
		t.Fatalf("durable public message content = %+v; want ordered text %q", payload.Message.Content, wantText)
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

func seedBridgeAPIDurableToolMessage(
	t *testing.T,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	threadID string,
	modelRequestID string,
	toolUseEventID string,
	toolCallID string,
	toolName string,
) {
	t.Helper()
	var messageSequence int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		workspaceID, sessionID, threadID,
	).Scan(&messageSequence); err != nil {
		t.Fatalf("allocate durable tool message sequence: %v", err)
	}
	messageID := "msg_" + toolUseEventID
	timestamp := "2026-01-01T00:00:00Z"
	dataJSON, err := json.Marshal(map[string]any{
		"parts": []map[string]any{{
			"type":            "tool_call",
			"modelToolCallId": toolCallID,
			"toolName":        toolName,
			"canonicalInput":  map[string]any{},
		}},
	})
	if err != nil {
		t.Fatalf("marshal durable tool message: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, model_request_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, $7, $8, $9, $9)`,
		workspaceID,
		sessionID,
		threadID,
		messageID,
		messageSequence,
		string(dataJSON),
		toolUseEventID,
		modelRequestID,
		timestamp,
	); err != nil {
		t.Fatalf("seed durable tool message: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET model_request_id = $4,
		        projection_json = COALESCE(projection_json, '{}')::jsonb || jsonb_build_object(
		          'event_type', type,
		          'evaluated_permission', COALESCE(payload_json::jsonb ->> 'evaluated_permission','allow'),
		          'model_tool_call_id', $5::text,
		          'tool_name', $6::text,
		          'provider_input', payload_json::jsonb -> 'input',
		          'canonical_execution_input', payload_json::jsonb -> 'input',
		          'route_capability', CASE WHEN type='agent.mcp_tool_use' THEN 'mcp_execute' ELSE $7::text END,
		          'mcp_server_name', payload_json::jsonb ->> 'mcp_server_name',
		          'state', 'running'
		        )
		  WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3`,
		workspaceID,
		sessionID,
		toolUseEventID,
		modelRequestID,
		toolCallID,
		toolName,
		bridgeToolDeclarationWithRouteForTest(toolCallID, toolName, `{}`, "allow").GetRouteCapability(),
	); err != nil {
		t.Fatalf("seed durable Tool Use identity: %v", err)
	}
}

func seedBridgeAPIAllowedToolRoute(
	t *testing.T,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	threadID string,
	toolUseEventID string,
) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `UPDATE session_events AS event
		SET model_request_id=COALESCE(NULLIF(event.model_request_id,''),'mreq_' || event.event_id),
		    projection_json=jsonb_build_object(
		      'event_type', event.type,
		      'evaluated_permission', COALESCE(NULLIF(event.projection_json::jsonb ->> 'evaluated_permission',''),event.payload_json::jsonb ->> 'evaluated_permission','allow'),
		      'model_tool_call_id', COALESCE(NULLIF(event.projection_json::jsonb ->> 'model_tool_call_id',''),'call_' || event.event_id),
		      'tool_name', COALESCE(NULLIF(event.projection_json::jsonb ->> 'tool_name',''),event.payload_json::jsonb ->> 'name'),
		      'provider_input', COALESCE(event.projection_json::jsonb -> 'provider_input',event.payload_json::jsonb -> 'input'),
		      'canonical_execution_input', COALESCE(event.projection_json::jsonb -> 'canonical_execution_input',event.payload_json::jsonb -> 'input'),
		      'route_capability', COALESCE(NULLIF(event.projection_json::jsonb ->> 'route_capability',''),CASE
		        WHEN event.type='agent.mcp_tool_use' THEN 'mcp_execute'
		        WHEN event.payload_json::jsonb ->> 'name'='memory' THEN 'memory_execute'
		        WHEN event.payload_json::jsonb ->> 'name' IN ('web_search','web_fetch') THEN 'web_execute'
		        WHEN event.payload_json::jsonb ->> 'name'='write_stdin' THEN 'background_command'
		        WHEN event.payload_json::jsonb ->> 'name'='spawn_agent' THEN 'child_create'
		        WHEN event.payload_json::jsonb ->> 'name' IN ('send_message','send_input') THEN 'child_message'
		        WHEN event.payload_json::jsonb ->> 'name' IN ('wait','wait_agent','wait_threads') THEN 'child_wait'
		        WHEN event.payload_json::jsonb ->> 'name'='interrupt_agent' THEN 'child_interrupt'
		        WHEN event.payload_json::jsonb ->> 'name'='close_agent' THEN 'child_close'
		        WHEN event.payload_json::jsonb ->> 'name'='resume_agent' THEN 'child_resume'
		        WHEN event.payload_json::jsonb ->> 'name'='list_agents' THEN 'child_list'
		        ELSE 'sandbox_execute' END),
		      'mcp_server_name', COALESCE(NULLIF(event.projection_json::jsonb ->> 'mcp_server_name',''),event.payload_json::jsonb ->> 'mcp_server_name'),
		      'state', 'running'
		    )
		WHERE event.workspace_id=$1 AND event.session_id=$2 AND event.session_thread_id=$3 AND event.event_id=$4
		  AND event.type IN ('agent.tool_use','agent.mcp_tool_use')`,
		workspaceID, sessionID, threadID, toolUseEventID); err != nil {
		t.Fatalf("seed allowed Tool declaration projection: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, decision, created_at, updated_at
		)
		SELECT e.workspace_id, e.session_id, e.session_thread_id, e.event_id,
		       COALESCE(e.projection_json::jsonb ->> 'model_tool_call_id', 'call_' || e.event_id),
		       COALESCE(e.projection_json::jsonb ->> 'tool_name', e.payload_json::jsonb ->> 'name'),
		       COALESCE(e.projection_json::jsonb -> 'canonical_execution_input', e.payload_json::jsonb -> 'input')::text,
		       'resolving', 'allow', clock_timestamp(), clock_timestamp()
		  FROM session_events e
		 WHERE e.workspace_id=$1 AND e.session_id=$2 AND e.session_thread_id=$3 AND e.event_id=$4`,
		workspaceID, sessionID, threadID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed allowed Tool route: %v", err)
	}
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
	canonicalInput, _, err := canonicalRunToolInput(inputJSON)
	if err != nil {
		t.Fatalf("canonicalize seeded Tool declaration input: %v", err)
	}
	projectionJSON, err := marshalBridgeJSON(map[string]any{
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
		SET model_request_id=$5, projection_json=$6
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4 AND type='agent.tool_use'`,
		workspaceID, sessionID, threadID, toolUseEventID, "mreq_"+toolUseEventID, projectionJSON); err != nil {
		t.Fatalf("seed Tool declaration projection: %v", err)
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
			is_trunk, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'approval_reviewer', 'internal', 'idle',
			true, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
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
		bridgeAPIScope(
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
	scope := bridgeAPIScope(
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
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral-agent-runtime', 'runtime-pod-0', $5, '10.0.0.10', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, bindingID, generation, podUID); err != nil {
		t.Fatalf("seed runtime binding: %v", err)
	}
}

func seedRuntimePodLostStatusFence(t *testing.T, db *sql.DB, sessionID string, bindingID string, generation int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, binding_id, binding_generation, created_at, updated_at
		) VALUES ('default', $1, 'running', $2, $3, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, bindingID, generation); err != nil {
		t.Fatalf("seed runtime pod-loss status: %v", err)
	}
}

func runtimePodLostBinding(sessionID string, bindingID string, generation int64) runtimeBindingForDelivery {
	return runtimeBindingForDelivery{
		BindingID:         bindingID,
		BindingGeneration: generation,
		Namespace:         "tetral-agent-runtime",
		PodName:           "runtime-pod-0",
		PodUID:            "pod_uid_" + sessionID,
		PodIP:             "10.0.0.10",
	}
}

func assertRuntimePodLostRetryableError(t *testing.T, err error, kind string) {
	t.Helper()
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != kind || !prepareErr.retryable {
		t.Fatalf("repair error = %#v; want retryable %q", err, kind)
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

func seedRuntimeInboxBirthForJob(t *testing.T, db *sql.DB, job RuntimeJob) {
	t.Helper()
	eventIDs := job.EventIDs
	if eventIDs == nil {
		eventIDs = []string{}
	}
	eventIDsJSON, err := json.Marshal(eventIDs)
	if err != nil {
		t.Fatalf("marshal Runtime Inbox birth events: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,rejection_reason_code,
		event_ids_json,sequence_from,sequence_to,status,created_at,updated_at
	) VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),$7,NULLIF($8,0),NULLIF($9,0),'queued','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, job.RuntimeInputID, job.InputKind,
		job.RejectionReasonCode, string(eventIDsJSON), job.SequenceFrom, job.SequenceTo,
	); err != nil {
		t.Fatalf("seed Runtime Inbox birth: %v", err)
	}
}

func seedAgentMailCustody(t *testing.T, db *sql.DB, sessionID string, targetThreadID string, deliveryID string, now time.Time) {
	t.Helper()
	runtimeInputID := completionRuntimeInputID(deliveryID)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_runtime_inbox (
		workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,status,created_at,updated_at
	) VALUES ('default',$1,$2,$3,'agent_mail','[]','queued',$4,$4)`,
		sessionID, targetThreadID, runtimeInputID, now,
	); err != nil {
		t.Fatalf("seed agent-mail Inbox custody: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": targetThreadID,
		"runtime_input_id": runtimeInputID, "event_ids": []string{}, "sequence_from": 0,
		"sequence_to": 0, "input_kind": "agent_mail",
	})
	if err != nil {
		t.Fatalf("marshal agent-mail Queue custody: %v", err)
	}
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	if _, err := queueStore.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.ID("default"), Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.ID("default"), sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.ID("default"), sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: queue.DefaultMaxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("seed agent-mail Queue custody: %v", err)
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
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, threadID, eventID, sequence, eventType, payloadJSON); err != nil {
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
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,visibility,session_visible,created_at,updated_at
	) SELECT 'default',$1,$2,$3,COALESCE(max(sequence),0)+1,'agent.tool_use',$4,'public',true,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z'
	FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2`, sessionID, parentID, sourceID, string(payload)); err != nil {
		t.Fatalf("seed child lifecycle Tool Use: %v", err)
	}
	seedBridgeAPIAllowedToolRoute(t, db, "default", sessionID, parentID, sourceID)
	return sourceID
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
		    payload_json='{"type":"agent.tool_use","name":"exec_command","input":{},"evaluated_permission":"allow"}'
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4`,
		workspaceID, sessionID, threadID, sourceToolUseEventID); err != nil {
		t.Fatalf("mark background task source Tool Use: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, db, workspaceID, sessionID, threadID,
		"mreq_"+sourceToolUseEventID, sourceToolUseEventID, "call_"+sourceToolUseEventID, "exec_command")
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, model_request_id, projection_json, created_at, updated_at
	) SELECT $1, $2, $3, 'evt_result_' || $4,
		COALESCE((SELECT MAX(sequence) + 1 FROM session_events WHERE workspace_id=$1 AND session_id=$2), 1),
		'agent.tool_result', jsonb_build_object('type','agent.tool_result','tool_use_event_id',$4,'content',jsonb_build_array(jsonb_build_object('type','text','text','Background command accepted.'))),
		'internal', false, 'mreq_' || $4,
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
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.tool_use', $6, 'public', true, 'mrq_pending_approval',
			'{"model_tool_call_id":"toolu_cleanup_wait"}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
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
