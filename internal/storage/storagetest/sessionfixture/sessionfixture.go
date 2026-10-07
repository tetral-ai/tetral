// Package sessionfixture seeds Session, Thread, Tool, Runtime delivery and
// Session resource rows and builds Bridge API request values for the Bridge,
// Job Runner and cross-service integration tests. Helpers write synthetic rows
// through the caller's database handle and fail the test on error; this
// package is imported only by tests.
//
// Fixtures that add session_events rows stay in each test package's _test.go
// files, because the repository checks every non-test source that adds
// session_events rows as a public event producer that must stamp processed_at.
package sessionfixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// BridgeToolDeclarationForTest returns a built-in Tool declaration with an
// explicit route capability.
func BridgeToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission, routeCapability string) *bridgev1.RuntimeToolDeclaration {
	return &bridgev1.RuntimeToolDeclaration{
		EventKind:                bridgev1.RuntimeToolEventKind_RUNTIME_TOOL_EVENT_KIND_TOOL,
		ModelToolCallId:          modelToolCallID,
		ToolName:                 toolName,
		PublicExecutionInputJson: inputJSON,
		EvaluatedPermission:      permission,
		RouteCapability:          routeCapability,
	}
}

// BridgeToolDeclarationWithRouteForTest returns a built-in Tool declaration
// whose route capability is selected from toolName; unlisted tools use
// sandbox_execute.
func BridgeToolDeclarationWithRouteForTest(modelToolCallID, toolName, inputJSON, permission string) *bridgev1.RuntimeToolDeclaration {
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
	return BridgeToolDeclarationForTest(modelToolCallID, toolName, inputJSON, permission, routeCapability)
}

// BridgeAPIScope returns a default-workspace Runtime scope whose binding names
// the Runtime process "process_"+podUID.
func BridgeAPIScope(sessionID string, threadID string, bindingID string, generation int64, podUID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     "default",
		SessionId:       sessionID,
		SessionThreadId: threadID,
		Binding:         &bridgev1.RuntimeBindingRef{BindingId: bindingID, BindingGeneration: generation, TargetPodUid: podUID, RuntimeProcessId: "process_" + podUID},
	}
}

// JSONPathValue decodes raw as a JSON object and returns the value at the
// dot-separated path, failing the test when a segment is missing or crosses a
// non-object value.
func JSONPathValue(t *testing.T, raw string, path string) any {
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

// JSONPathString returns the string at path in the JSON object raw and fails the test
// when that value is not a string.
func JSONPathString(t *testing.T, raw string, path string) string {
	t.Helper()
	value := JSONPathValue(t, raw, path)
	stringValue, ok := value.(string)
	if !ok {
		t.Fatalf("JSON path %s = %#v; want string", path, value)
	}
	return stringValue
}

// BridgePublicMessageJSONForTest returns the public agent-mail message content
// JSON for text.
func BridgePublicMessageJSONForTest(t *testing.T, text string) string {
	t.Helper()
	raw, err := runtimecontrol.PublicAgentMailMessageJSON(text)
	if err != nil {
		t.Fatalf("marshal public message content: %v", err)
	}
	return raw
}

// BridgeAPIInt64 returns a pointer to value.
func BridgeAPIInt64(value int64) *int64 {
	return &value
}

// BridgeTextContextDeltaForTest returns a Runtime context delta with one text part.
func BridgeTextContextDeltaForTest(text string) *bridgev1.RuntimeContextDelta {
	return &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: text}}}}}
}

// BridgeCompletedToolSettlementForTest returns a completed settlement for toolUseEventID
// whose output is the untruncated text textValue.
func BridgeCompletedToolSettlementForTest(toolUseEventID, textValue string) *bridgev1.RuntimeToolSettlement {
	return &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome: &bridgev1.RuntimeToolSettlement_Completed{
			Completed: &bridgev1.RuntimeToolCompleted{
				OutputJson: fmt.Sprintf(`{"text":%q,"truncated":false}`, textValue),
			},
		},
	}
}

// BridgeErrorToolSettlementForTest returns a tool_error settlement for toolUseEventID
// carrying message.
func BridgeErrorToolSettlementForTest(toolUseEventID, message string) *bridgev1.RuntimeToolSettlement {
	return &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome: &bridgev1.RuntimeToolSettlement_Error{
			Error: &bridgev1.RuntimeToolError{
				ErrorJson: fmt.Sprintf(`{"type":"tool_error","message":%q}`, message),
			},
		},
	}
}

// BridgeToolSettlementRequestForTest returns a SettleToolResult request for settlement
// in scope.
func BridgeToolSettlementRequestForTest(
	scope *bridgev1.RuntimeScope,
	settlement *bridgev1.RuntimeToolSettlement,
) *bridgev1.SettleToolResultRequest {
	return &bridgev1.SettleToolResultRequest{Scope: scope, Settlement: settlement}
}

// BridgeRequireToolSettlementOutcomeForTest fails the test unless response has the
// outcome want: committed, duplicate or stale.
func BridgeRequireToolSettlementOutcomeForTest(
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

// BridgeTaskNotificationRequestForTest returns a CommitTaskNotificationResult request
// for runtimeInputID in scope.
func BridgeTaskNotificationRequestForTest(t *testing.T, scope *bridgev1.RuntimeScope, runtimeInputID string) *bridgev1.CommitTaskNotificationResultRequest {
	t.Helper()
	return &bridgev1.CommitTaskNotificationResultRequest{
		Scope: scope, RuntimeInputId: runtimeInputID,
	}
}

// BridgeInterruptLeaseRef returns the interrupt lease reference of job, or nil when job
// is nil.
func BridgeInterruptLeaseRef(job *queue.Job) *bridgev1.InterruptLeaseRef {
	if job == nil {
		return nil
	}
	return &bridgev1.InterruptLeaseRef{
		JobId: job.ID, LeaseToken: job.LeaseToken, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	}
}

// BridgeInterAgentMessageJSON returns the JSON of an inter-agent message that delivers
// messageJSON from sourceThreadID.
func BridgeInterAgentMessageJSON(t *testing.T, deliveryID string, sourceThreadID string, sourceToolUseEventID string, messageJSON string) string {
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

// BridgeInterAgentSentEventJSON returns the payload JSON of an
// agent.thread_message_sent event from sourceThreadID to targetThreadID.
func BridgeInterAgentSentEventJSON(t *testing.T, deliveryID string, sourceThreadID string, targetThreadID string, targetTaskName string, sourceToolUseEventID string, messageJSON string) string {
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

// RuntimePodLostBinding returns the Runtime binding that pod-loss fixtures use for
// sessionID, with fixed Pod placement values.
func RuntimePodLostBinding(sessionID string, bindingID string, generation int64) runtimecontrol.Binding {
	return runtimecontrol.Binding{
		BindingID:         bindingID,
		BindingGeneration: generation,
		Namespace:         "tetral-agent-runtime",
		PodName:           "runtime-pod-0",
		PodUID:            "pod_uid_" + sessionID,
		RuntimeProcessID:  "process_pod_uid_" + sessionID,
		PodIP:             "10.0.0.10",
	}
}

// SeedBridgeAPISession inserts the workspace when absent, then an agent, its
// version 1, an environment, an idle Session and its public main Thread. The
// agent, version and environment IDs are derived from sessionID.
func SeedBridgeAPISession(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string) {
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

// SeedBridgeAPIDurableToolMessage inserts the assistant message carrying the
// tool call for toolUseEventID and stamps that event with its model request and
// running declaration projection.
func SeedBridgeAPIDurableToolMessage(
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
		BridgeToolDeclarationWithRouteForTest(toolCallID, toolName, `{}`, "allow").GetRouteCapability(),
	); err != nil {
		t.Fatalf("seed durable Tool Use identity: %v", err)
	}
}

// SeedBridgeAPIAllowedToolRoute projects a Tool Use event as an allowed
// running declaration and inserts its resolving pending Tool Use row.
func SeedBridgeAPIAllowedToolRoute(
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

// SeedBridgeAPIAgentConfig replaces the configuration of the version 1 agent
// that SeedBridgeAPISession created for sessionID.
func SeedBridgeAPIAgentConfig(t *testing.T, db *sql.DB, workspaceID string, sessionID string, configJSON string) {
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

// SeedBridgeAPIInternalReviewerThread inserts an idle internal
// approval-reviewer Thread under parentThreadID.
func SeedBridgeAPIInternalReviewerThread(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, reviewerThreadID string) {
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

// SeedBridgeAPIChildThread inserts an idle public subagent Thread under
// parentThreadID with task name "task_"+childThreadID.
func SeedBridgeAPIChildThread(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, childThreadID string) {
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

// SeedBridgeAPIRuntimeInbox inserts a Runtime Inbox row targeted at bindingID
// generation 1 on podUID.
func SeedBridgeAPIRuntimeInbox(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, inputKind string, eventsJSON string, status string, bindingID string, podUID string, sequenceFrom int64, sequenceTo int64) {
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

// NextBridgeAPIEventSequenceForTest returns the next session_events sequence of
// a default-workspace Thread.
func NextBridgeAPIEventSequenceForTest(t *testing.T, db *sql.DB, sessionID string, threadID string) int64 {
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

// SeedBridgeAPIStreamChange inserts a stream change for eventID, points the
// event's latest_stream_position at it and returns that stream position.
func SeedBridgeAPIStreamChange(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, revision int64, visibility string, sessionVisible bool) int64 {
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

// SetBridgeAPIPendingApprovalStatus sets the status of the pending Tool Use
// toolUseEventID.
func SetBridgeAPIPendingApprovalStatus(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, toolUseEventID string, status string) {
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

// SeedBridgeAPITaskNotificationInbox inserts an accepted task_notification Runtime
// Inbox row targeted at bindingID generation 1 on podUID.
func SeedBridgeAPITaskNotificationInbox(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, bindingID string, podUID string) {
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

// SeedAgentMailCustody gives a default-workspace agent-mail delivery its queued Runtime
// Inbox row and its Runtime input Queue job, enqueued through the Queue store.
func SeedAgentMailCustody(t *testing.T, db *sql.DB, sessionID string, targetThreadID string, deliveryID string, now time.Time) {
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

// SeedRuntimePodLostStatusFence inserts a running default-workspace Runtime status
// for bindingID at generation.
func SeedRuntimePodLostStatusFence(t *testing.T, db *sql.DB, sessionID string, bindingID string, generation int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, binding_id, binding_generation, created_at, updated_at
		) VALUES ('default', $1, 'running', $2, $3, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, bindingID, generation); err != nil {
		t.Fatalf("seed runtime pod-loss status: %v", err)
	}
}

// SeedBridgeAPIWritableMemoryStore inserts a memory store and attaches it to the
// Session as a read_write resource mounted under /mnt/memory.
func SeedBridgeAPIWritableMemoryStore(t *testing.T, db *sql.DB, workspaceID string, sessionID string, storeID string) {
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

// SeedReadySandboxForSharedToolExecution gives the Session's environment a ready
// generation 1 artifact and a ready Sandbox binding for shared Tool execution.
func SeedReadySandboxForSharedToolExecution(t *testing.T, db *sql.DB, workspaceID string, sessionID string) {
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

// BridgeTransientAttachmentStatus returns the status of a default-workspace transient
// attachment.
func BridgeTransientAttachmentStatus(t *testing.T, db *sql.DB, attachmentRef string) string {
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

// AssertRuntimeMCPManifestQueueJob requires the pending version 2 Runtime
// configuration update Queue job for the MCP server manifest generation, in the
// Session partition, with a payload of identifying references only.
func AssertRuntimeMCPManifestQueueJob(t *testing.T, db *sql.DB, workspaceID string, sessionID string, mcpServerName string, manifestGeneration int64) {
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
