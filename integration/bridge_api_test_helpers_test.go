package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
)

func repoRootFromBridgeTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, ".."))
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

func bridgeSignedReasoningToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission string) *bridgev1.RuntimeToolDeclaration {
	declaration := sessionfixture.BridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, permission)
	declaration.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{
		Text:                 "provider-declared reasoning",
		ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"sig_provider_context"}}`),
	}}
	return declaration
}

// Seed an attachment and bytes as fixture input; the assertions exercise real
// Bridge resolution and Runner cleanup through their supported operations.
func createBridgeTransientAttachmentForTest(t *testing.T, admin *sql.DB, store *agentruntimebridge.PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope, runtimeWriteID string, sourceToolUseEventID string, data []byte) *bridgev1.TransientAttachmentRef {
	t.Helper()
	attachment := &bridgev1.TransientAttachmentRef{AttachmentRef: "att_fixture_" + runtimeWriteID, Mime: "image/png", Filename: runtimeWriteID + ".png", SourcePath: "sandbox:" + runtimeWriteID + ".png", Detail: "auto"}
	pointer := "transient-attachments/" + scope.GetWorkspaceId() + "/" + scope.GetSessionId() + "/" + attachment.GetAttachmentRef()
	if err := store.AttachmentBlobStore.Put(context.Background(), pointer, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("seed attachment bytes: %v", err)
	}
	digest := sha256.Sum256(data)
	metadata, err := json.Marshal(map[string]any{"filename": attachment.Filename, "source_path": attachment.SourcePath, "page_range": "", "detail": "auto", "size_bytes": len(data), "sha256": hex.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if store.Clock != nil {
		now = store.Clock()
	}
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_transient_attachments(workspace_id,attachment_ref,session_id,session_thread_id,source_tool_use_event_id,blob_pointer,mime,metadata_json,status,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,'image/png',$7,'active',$8::timestamptz+interval '15 minutes',$8,$8)`, scope.GetWorkspaceId(), attachment.AttachmentRef, scope.GetSessionId(), scope.GetSessionThreadId(), sourceToolUseEventID, pointer, string(metadata), now); err != nil {
		t.Fatalf("seed attachment index: %v", err)
	}
	return attachment
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

func seedBridgeAPIRequestStart(
	t *testing.T,
	store *agentruntimebridge.PostgreSQLBridgeAPIStore,
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

func assertNoTaskOutputPaths(t *testing.T, raw string) {
	t.Helper()
	if strings.Contains(raw, `"output_paths"`) || strings.Contains(raw, "/tmp/tetral-runtime/tasks/") {
		t.Fatalf("task notification surface contains internal output paths: %s", raw)
	}
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
	processID := seedFixtureRuntimeProcess(t, dbconnect.NewClientForTesting(db), "tetral-agent-runtime", podUID)

	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_bindings (
			workspace_id, session_id, binding_id, binding_generation, agent_runtime_namespace,
			agent_runtime_pod_name, agent_runtime_pod_uid, agent_runtime_pod_ip, runtime_process_id, bound_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tetral-agent-runtime', 'runtime-pod-0', $5, '10.0.0.10', $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, sessionID, bindingID, generation, podUID, processID); err != nil {
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

func seedRuntimeInboxBirthForJob(t *testing.T, db *sql.DB, job jobrunner.RuntimeJob) {
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
	sessionfixture.SeedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, eventID, 1, "public", true)
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
	sessionfixture.SeedBridgeAPIStreamChange(t, db, workspaceID, sessionID, threadID, eventID, 1, "public", true)
}

type recordingRuntimeTargetResolver struct {
	jobs    []jobrunner.RuntimeJob
	binding runtimecontrol.Binding
	err     error
}

func (r *recordingRuntimeTargetResolver) ResolveRuntimeTarget(_ context.Context, _ *dbconnect.Tx, job jobrunner.RuntimeJob) (runtimecontrol.Binding, error) {
	r.jobs = append(r.jobs, job)
	if r.err != nil {
		return runtimecontrol.Binding{}, r.err
	}
	return r.binding, nil
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

func bridgeString(value string) *string { return &value }

func walkPostgreSQLPlan(node map[string]any, visit func(map[string]any)) {
	visit(node)
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		childNode, _ := child.(map[string]any)
		if childNode != nil {
			walkPostgreSQLPlan(childNode, visit)
		}
	}
}

func seedBridgeAPIFileAttachment(t *testing.T, db *sql.DB, blobStore blob.BlobStore, fileID, filename, mime, body string) {
	t.Helper()
	objectID := "fobj_" + fileID
	blobKey := "files/default/" + objectID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ('default', $1, $2, $3, $4, '2026-01-01T00:00:00Z')`,
		objectID, blobKey, len(body), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed file object %s: %v", fileID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ('default', $1, $2, $3, $4, TRUE, '2026-01-01T00:00:00Z')`,
		fileID, objectID, filename, mime); err != nil {
		t.Fatalf("seed file %s: %v", fileID, err)
	}
	if err := blobStore.Put(context.Background(), blobKey, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("seed file blob %s: %v", fileID, err)
	}
}

func seedBridgeAPIProjectedUserMessage(t *testing.T, db *sql.DB, sessionID, threadID, messageID, sourceEventID string, sequence int64) {
	t.Helper()
	dataJSON := `{"parts":[]}`
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $4, 'user', $5, $6, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, messageID, sequence, dataJSON, sourceEventID); err != nil {
		t.Fatalf("seed projected user message %s: %v", sourceEventID, err)
	}
}

// seedFixtureRuntimeProcess performs the actual ordered registration handshake
// for a fixture Pod before placement or a seeded binding takes custody.
func seedFixtureRuntimeProcess(t *testing.T, client *dbconnect.Client, namespace, podUID string) string {
	t.Helper()
	identity := runtimecontrol.ProcessIdentity{Namespace: namespace, PodUID: podUID, ID: "process_" + podUID}
	registered, err := runtimecontrol.RegisterProcess(context.Background(), client, identity)
	if err != nil {
		t.Fatalf("register fixture Runtime process: %v", err)
	}
	if _, _, err := runtimecontrol.ReportProcess(context.Background(), client, identity, registered.RegistrationReceipt, runtimecontrol.ProcessAccepting); err != nil {
		t.Fatalf("promote fixture Runtime process: %v", err)
	}
	return identity.ID
}
