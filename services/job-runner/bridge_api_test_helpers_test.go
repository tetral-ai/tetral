package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

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

func bridgePublicMessageJSONForTest(t *testing.T, text string) string {
	t.Helper()
	raw, err := runtimecontrol.PublicAgentMailMessageJSON(text)
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

func runtimePodLostBinding(sessionID string, bindingID string, generation int64) runtimecontrol.Binding {
	return runtimecontrol.Binding{
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
	var prepareErr runtimecontrol.PreparationError
	if !errors.As(err, &prepareErr) || prepareErr.Kind != kind || !prepareErr.Retryable {
		t.Fatalf("repair error = %#v; want retryable %q", err, kind)
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
	runtimeInputID := runtimecontrol.CompletionRuntimeInputID(deliveryID)
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

type recordingRuntimeTargetResolver struct {
	jobs    []RuntimeJob
	binding runtimecontrol.Binding
	err     error
}

func (r *recordingRuntimeTargetResolver) ResolveRuntimeTarget(_ context.Context, _ *dbconnect.Tx, job RuntimeJob) (runtimecontrol.Binding, error) {
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
