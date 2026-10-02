package jobrunner

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestRuntimeRepairOpenRequestDetectionScopesEndsToTheirThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_runtime_repair_thread_scope"
		mainThreadID   = "thr_runtime_repair_thread_scope_main"
		childThreadID  = "thr_runtime_repair_thread_scope_child"
		modelRequestID = "mreq_runtime_repair_shared"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_repair_main_start', 1, 'span.model_request_start', '{}',
		 'internal', false, $4, '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $3, 'evt_repair_child_start', 1, 'span.model_request_start', '{}',
		 'internal', false, $4, '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $3, 'evt_repair_child_end', 2, 'span.model_request_end', '{}',
		 'internal', false, $4, '{}', now(), now())`,
		sessionID, mainThreadID, childThreadID, modelRequestID,
	); err != nil {
		t.Fatalf("seed same-request-id sibling events: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_thread_scope", func(tx *dbconnect.Tx) error {
		starts, err := runtimecontrol.RuntimePodLostOpenRequestStartsTx(context.Background(), tx, "default", sessionID, []string{mainThreadID, childThreadID})
		if err != nil {
			return err
		}
		if len(starts) != 1 || starts[0].SessionThreadID != mainThreadID || starts[0].EventID != "evt_repair_main_start" {
			t.Fatalf("pod-loss open starts = %+v; want only main-thread start", starts)
		}
		return nil
	}); err != nil {
		t.Fatalf("query pod-loss open starts: %v", err)
	}
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.termination_thread_scope", func(tx *dbconnect.Tx) error {
		starts, err := runtimecontrol.RuntimeTerminationOpenRequestStartsTx(
			context.Background(),
			tx,
			bridgeAPIScope(sessionID, mainThreadID, "bind_unused", 1, "pod_unused"),
		)
		if err != nil {
			return err
		}
		if len(starts) != 1 || starts[0].SessionThreadID != mainThreadID || starts[0].EventID != "evt_repair_main_start" {
			t.Fatalf("runtime-termination open starts = %+v; want main-thread start", starts)
		}
		return nil
	}); err != nil {
		t.Fatalf("query runtime-termination open starts: %v", err)
	}
}

func TestRuntimePodLossSettlesMCPToolNamedLikeSubAgentToolWithoutConnectorReplay(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_mcp_pod_loss"
		threadID       = "thr_mcp_pod_loss"
		modelRequestID = "mreq_mcp_pod_loss"
		toolUseEventID = "evt_mcp_pod_loss_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_mcp_pod_loss", 1, "pod_mcp_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_mcp_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_mcp_pod_loss_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_mcp_pod_loss","request_kind":"agent_provider_request"}',
		 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', $1, $2, $4, 2, 'agent.mcp_tool_use',
		 '{"type":"agent.mcp_tool_use","name":"spawn_agent","mcp_server_name":"github","input":{"q":"x"},"evaluated_permission":"allow"}',
		 'public', true, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, threadID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed MCP pod-loss events: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseEventID, "call_mcp_pod_loss", "spawn_agent")
	binding := runtimecontrol.Binding{BindingID: "bind_mcp_pod_loss", BindingGeneration: 1, PodUID: "pod_mcp_pod_loss"}
	repaired, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("repair MCP pod loss: %v", err)
	}
	if repaired < 2 {
		t.Fatalf("repaired facts = %d; want at least Request End plus MCP Tool Result", repaired)
	}
	var resultEventID, payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.mcp_tool_result'
		    AND payload_json::jsonb ->> 'mcp_tool_use_id' = $2`,
		sessionID, toolUseEventID,
	).Scan(&resultEventID, &payloadJSON); err != nil {
		t.Fatalf("read MCP pod-loss result: %v", err)
	}
	if !strings.Contains(payloadJSON, `"reason":"runtime_pod_lost"`) || !strings.Contains(payloadJSON, `"is_error":true`) {
		t.Fatalf("MCP pod-loss result = %s; want terminal runtime_pod_lost error", payloadJSON)
	}
	var messageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND message_id = $2`,
		sessionID, "msg_"+toolUseEventID,
	).Scan(&messageJSON); err != nil {
		t.Fatalf("read repaired MCP message: %v", err)
	}
	assertPodLossDurableToolErrorContext(t, messageJSON, "call_mcp_pod_loss")
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 6, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("replay MCP pod-loss repair: %v", err)
	}
	var resultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.mcp_tool_result'
		    AND payload_json::jsonb ->> 'mcp_tool_use_id' = $2`,
		sessionID, toolUseEventID,
	).Scan(&resultCount); err != nil {
		t.Fatalf("count MCP pod-loss results: %v", err)
	}
	if resultCount != 1 {
		t.Fatalf("MCP pod-loss result count = %d; want exactly one", resultCount)
	}
}

func assertPodLossDurableToolErrorContext(t *testing.T, raw, modelToolCallID string) {
	t.Helper()
	stored, err := runtimecontrol.DecodeRuntimeDeclarationObject(raw)
	if err != nil {
		t.Fatalf("decode repaired Tool context: %v", err)
	}
	parts, ok := stored["parts"].([]any)
	if !ok {
		t.Fatalf("repaired Tool context parts = %#v; want an array", stored["parts"])
	}
	var resultPart map[string]any
	for _, candidate := range parts {
		part, candidateOK := candidate.(map[string]any)
		if candidateOK && part["type"] == "tool_result" && part["modelToolCallId"] == modelToolCallID {
			resultPart = part
			break
		}
	}
	if resultPart == nil || len(resultPart) != 3 || resultPart["type"] != "tool_result" || resultPart["modelToolCallId"] != modelToolCallID {
		t.Fatalf("repaired Tool result = %#v; want exact narrow result identity", resultPart)
	}
	outcome, ok := resultPart["result"].(map[string]any)
	if !ok || len(outcome) != 2 || outcome["type"] != "error" {
		t.Fatalf("repaired Tool outcome = %#v; want exact error outcome", resultPart["result"])
	}
	toolError, ok := outcome["error"].(map[string]any)
	if !ok || len(toolError) != 3 || toolError["type"] != "runtime_pod_lost" ||
		toolError["message"] != "Tool result unavailable because the runtime pod was lost." || toolError["retryable"] != false {
		t.Fatalf("repaired Tool error = %#v; want non-retryable pod-loss error", outcome["error"])
	}
}

func TestRuntimePodLossDetectsInternalApprovalReviewerToolUse(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_reviewer_pod_loss"
		mainThreadID   = "thr_reviewer_pod_loss_main"
		reviewerID     = "thr_reviewer_pod_loss"
		modelRequestID = "mreq_reviewer_pod_loss"
		toolUseEventID = "evt_reviewer_pod_loss_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIInternalReviewerThread(t, admin, "default", sessionID, mainThreadID, reviewerID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_reviewer_pod_loss_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_reviewer_pod_loss","request_kind":"approval_reviewer"}',
		 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"approval_reviewer"}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', $1, $2, $4, 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}',
		 'internal', false, $3, '{}',
		 '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, reviewerID, modelRequestID, toolUseEventID,
	); err != nil {
		t.Fatalf("seed reviewer pod-loss events: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, reviewerID, modelRequestID,
		toolUseEventID, "call_reviewer_pod_loss", "Read",
	)

	client := dbconnect.NewClientForTesting(runtime)
	var orphans []runtimecontrol.OrphanToolUse
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.reviewer_pod_loss", func(tx *dbconnect.Tx) error {
		var err error
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID, []string{reviewerID})
		return err
	}); err != nil {
		t.Fatalf("load reviewer pod-loss Tool Uses: %v", err)
	}
	if len(orphans) != 1 || orphans[0].EventID != toolUseEventID || orphans[0].SessionThreadID != reviewerID {
		t.Fatalf("reviewer orphan Tool Uses = %+v; want internal reviewer Read", orphans)
	}
}

func TestRuntimePodLossOrphanDetectionKeepsToolFamilyAndThreadClosed(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_closed_result_identity"
		threadID  = "thr_pod_loss_closed_result_identity"
		otherID   = "thr_pod_loss_closed_result_other"
		toolUseID = "evt_pod_loss_closed_result_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			agent_type, title, task_name, is_trunk, created_at, last_active_at, updated_at
		) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'idle',
			'worker', 'worker', 'worker', false, NOW(), NOW(), NOW())`,
		otherID, sessionID, threadID,
	); err != nil {
		t.Fatalf("seed sibling thread: %v", err)
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, toolUseID, 1, "agent.tool_use",
		`{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_closed_result_identity'
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND event_id = $3`,
		sessionID, threadID, toolUseID,
	); err != nil {
		t.Fatalf("stamp Tool Use request identity: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, "mreq_closed_result_identity", toolUseID, "call_closed_result_identity", "Read")
	seedBridgeAPIEvent(t, admin, "default", sessionID, otherID, "evt_pod_loss_wrong_family_result", 1, "agent.mcp_tool_result",
		`{"type":"agent.mcp_tool_result","mcp_tool_use_id":"`+toolUseID+`","is_error":false}`)

	client := dbconnect.NewClientForTesting(runtime)
	var orphans []runtimecontrol.OrphanToolUse
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.pod_loss_closed_result_identity", func(tx *dbconnect.Tx) error {
		var err error
		orphans, err = runtimePodLostOrphanToolUsesTx(context.Background(), tx, "default", sessionID, []string{threadID})
		return err
	}); err != nil {
		t.Fatalf("load orphan Tool Uses: %v", err)
	}
	if len(orphans) != 1 || orphans[0].EventID != toolUseID || orphans[0].SessionThreadID != threadID {
		t.Fatalf("orphan Tool Uses = %+v; want ordinary Tool Use preserved across cross-Thread MCP result", orphans)
	}
}

func TestRuntimePodLossUsesDurablePrivateRequestKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_compaction_pod_loss"
		threadID       = "thr_compaction_pod_loss"
		modelRequestID = "mreq_compaction_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_compaction_pod_loss", 1, "pod_compaction_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_compaction_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES (
			'default', $1, $2, 'evt_compaction_pod_loss_start', 1, 'span.model_request_start',
			'{"type":"span.model_request_start","model_request_id":"mreq_compaction_pod_loss"}',
			'internal', false, $3,
			'{"context_through_message_sequence":0,"request_kind":"compaction_summary"}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`,
		sessionID, threadID, modelRequestID,
	); err != nil {
		t.Fatalf("seed compaction Request Start: %v", err)
	}
	binding := runtimecontrol.Binding{
		BindingID: "bind_compaction_pod_loss", BindingGeneration: 1, PodUID: "pod_compaction_pod_loss",
	}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("repair compaction pod loss: %v", err)
	}
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = $3 AND type = 'span.model_request_end'`,
		sessionID, threadID, modelRequestID,
	).Scan(&payloadJSON); err != nil {
		t.Fatalf("read repaired compaction Request End: %v", err)
	}
	if !strings.Contains(payloadJSON, `"request_kind":"compaction_summary"`) {
		t.Fatalf("repaired Request End = %s; want durable compaction request kind", payloadJSON)
	}
}

func TestRuntimePodLossPreservesEveryPendingApprovalExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID             = "sesn_pod_loss_multiple_approvals"
		threadID              = "thr_pod_loss_multiple_approvals"
		siblingThreadID       = "thr_pod_loss_sibling_approval"
		idleSiblingThreadID   = "thr_pod_loss_idle_sibling"
		modelRequestID        = "mreq_pod_loss_multiple_approvals"
		siblingModelRequestID = "mreq_pod_loss_sibling_approval"
		bindingID             = "bind_pod_loss_multiple_approvals"
	)
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, siblingThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, idleSiblingThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET status = 'idle', running_since = NULL
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID,
	); err != nil {
		t.Fatalf("seed idle Runtime status with durable pending work: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_multiple_start', 1, 'span.model_request_start',
		 $4, 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_tool_pending', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_tool_resolving', 3, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/b.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_multiple_end', 4, 'span.model_request_end',
		 $5, 'internal', false, $3, '{}', now(), now())`,
		sessionID,
		threadID,
		modelRequestID,
		`{"type":"span.model_request_start","model_request_id":"`+modelRequestID+`"}`,
		`{"type":"span.model_request_end","model_request_id":"`+modelRequestID+`","model_request_start_id":"evt_pod_loss_multiple_start","finish_reason":"tool_calls","is_error":false}`,
	); err != nil {
		t.Fatalf("seed multiple pending approvals request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, threadID, modelRequestID,
		"evt_pod_loss_multiple_tool_pending", "tool-call-pod-loss-multiple-pending", "Write",
	)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages
		    SET data_json = jsonb_set(
				data_json::jsonb,
				'{parts}',
				(data_json::jsonb -> 'parts') || $4::jsonb
			)::text
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND model_request_id = $3 AND kind = 'assistant'`,
		sessionID,
		threadID,
		modelRequestID,
		`[{"type":"tool_call","modelToolCallId":"tool-call-pod-loss-multiple-resolving","toolName":"Write","canonicalInput":{}}]`,
	); err != nil {
		t.Fatalf("append second durable tool part: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET projection_json = jsonb_build_object(
		      'model_tool_call_id', 'tool-call-pod-loss-multiple-resolving',
		      'tool_name', 'Write',
		      'provider_input', payload_json::jsonb -> 'input',
		      'canonical_execution_input', payload_json::jsonb -> 'input'
		    )
		  WHERE workspace_id='default' AND session_id=$1
		    AND event_id='evt_pod_loss_multiple_tool_resolving'`,
		sessionID,
	); err != nil {
		t.Fatalf("seed second durable Tool Use identity: %v", err)
	}
	for _, tool := range []struct {
		eventID    string
		toolCallID string
		status     string
		path       string
	}{
		{eventID: "evt_pod_loss_multiple_tool_pending", toolCallID: "tool-call-pod-loss-multiple-pending", status: "pending", path: "src/a.ts"},
		{eventID: "evt_pod_loss_multiple_tool_resolving", toolCallID: "tool-call-pod-loss-multiple-resolving", status: "resolving", path: "src/b.ts"},
	} {
		if _, err := admin.ExecContext(context.Background(),
			`INSERT INTO session_pending_tool_uses (
				workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
				tool_name, input_json, status, created_at, updated_at
			) VALUES ('default', $1, $2, $3, $4, 'Write', $5, $6, now(), now())`,
			sessionID, threadID, tool.eventID, tool.toolCallID,
			`{"file_path":"`+tool.path+`"}`, tool.status,
		); err != nil {
			t.Fatalf("seed %s approval: %v", tool.status, err)
		}
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_sibling_start', 1, 'span.model_request_start',
		 $4, 'internal', false, $3,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_sibling_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/sibling.ts"},"evaluated_permission":"ask"}',
		 'public', true, $3, '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_sibling_end', 3, 'span.model_request_end',
		 $5, 'internal', false, $3, '{}', now(), now())`,
		sessionID,
		siblingThreadID,
		siblingModelRequestID,
		`{"type":"span.model_request_start","model_request_id":"`+siblingModelRequestID+`"}`,
		`{"type":"span.model_request_end","model_request_id":"`+siblingModelRequestID+`","model_request_start_id":"evt_pod_loss_sibling_start","finish_reason":"tool_calls","is_error":false}`,
	); err != nil {
		t.Fatalf("seed sibling approval request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, siblingThreadID, siblingModelRequestID,
		"evt_pod_loss_sibling_tool", "tool-call-pod-loss-sibling", "Write",
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ('default', $1, $2, 'evt_pod_loss_sibling_tool', 'tool-call-pod-loss-sibling',
			'Write', '{"file_path":"src/sibling.ts"}', 'pending', now(), now())`,
		sessionID,
		siblingThreadID,
	); err != nil {
		t.Fatalf("seed sibling approval: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_pod_loss_idle_start', 1, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_pod_loss_idle"}',
		 'internal', false, 'mreq_pod_loss_idle',
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_tool', 2, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}',
		 'public', true, 'mreq_pod_loss_idle', '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_result', 3, 'agent.tool_result',
		 '{"type":"agent.tool_result","tool_use_event_id":"evt_pod_loss_idle_tool","content":[{"type":"text","text":"done"}]}',
		 'public', true, 'mreq_pod_loss_idle', '{}', now(), now()),
		('default', $1, $2, 'evt_pod_loss_idle_end', 4, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mreq_pod_loss_idle","model_request_start_id":"evt_pod_loss_idle_start","finish_reason":"stop","is_error":false}',
		 'internal', false, 'mreq_pod_loss_idle', '{}', now(), now())`,
		sessionID,
		idleSiblingThreadID,
	); err != nil {
		t.Fatalf("seed idle sibling terminal history: %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := runRuntimePodLostRepairTransaction(
			context.Background(), runtime, sessionID, binding,
			time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
		); err != nil {
			t.Fatalf("repair multiple approvals attempt %d: %v", attempt, err)
		}
	}

	var terminalResults int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'reason' = 'runtime_pod_lost'`,
		sessionID, threadID,
	).Scan(&terminalResults); err != nil {
		t.Fatalf("count multiple approval pod-loss results: %v", err)
	}
	if terminalResults != 0 {
		t.Fatalf("multiple approval pod-loss results = %d; want none", terminalResults)
	}
	var cancelledAndLinked int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND status = 'cancelled' AND result_event_id IS NOT NULL`,
		sessionID, threadID,
	).Scan(&cancelledAndLinked); err != nil {
		t.Fatalf("count cancelled multiple approvals: %v", err)
	}
	if cancelledAndLinked != 0 {
		t.Fatalf("cancelled and linked approvals = %d; want none", cancelledAndLinked)
	}
	var siblingApprovalStatus string
	var siblingResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT p.status,
		        (SELECT count(*) FROM session_events result
		          WHERE result.workspace_id = p.workspace_id
		            AND result.session_id = p.session_id
		            AND result.session_thread_id = p.session_thread_id
		            AND result.type = 'agent.tool_result'
		            AND result.payload_json::jsonb ->> 'tool_use_event_id' = p.tool_use_event_id)
		   FROM session_pending_tool_uses p
		  WHERE p.workspace_id = 'default' AND p.session_id = $1 AND p.session_thread_id = $2
		    AND p.tool_use_event_id = 'evt_pod_loss_sibling_tool'`,
		sessionID,
		siblingThreadID,
	).Scan(&siblingApprovalStatus, &siblingResultCount); err != nil {
		t.Fatalf("read sibling approval: %v", err)
	}
	if siblingApprovalStatus != "pending" || siblingResultCount != 0 {
		t.Fatalf("sibling approval = %q/results %d; want pending/0", siblingApprovalStatus, siblingResultCount)
	}
	var idleSiblingStatus string
	var idleSiblingRepairEventCount int
	var idleSiblingHistoryCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status,
		        (SELECT count(*) FROM session_events event
		          WHERE event.workspace_id = thread.workspace_id
		            AND event.session_id = thread.session_id
		            AND event.session_thread_id = thread.id
		            AND event.type IN ('session.error', 'session.thread_status_idle')),
		        (SELECT count(*) FROM session_events event
		          WHERE event.workspace_id = thread.workspace_id
		            AND event.session_id = thread.session_id
		            AND event.session_thread_id = thread.id)
		   FROM session_threads thread
		  WHERE thread.workspace_id = 'default' AND thread.session_id = $1 AND thread.id = $2`,
		sessionID,
		idleSiblingThreadID,
	).Scan(&idleSiblingStatus, &idleSiblingRepairEventCount, &idleSiblingHistoryCount); err != nil {
		t.Fatalf("read idle sibling after pod loss: %v", err)
	}
	if idleSiblingStatus != "idle" || idleSiblingRepairEventCount != 0 || idleSiblingHistoryCount != 4 {
		t.Fatalf("idle sibling = %q/repair events %d/history %d; want idle/0/4", idleSiblingStatus, idleSiblingRepairEventCount, idleSiblingHistoryCount)
	}
}

func TestRuntimePodLossRejectsMissingPrivateRequestKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_missing_kind_pod_loss"
		threadID  = "thr_missing_kind_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_missing_kind_pod_loss", 1, "pod_missing_kind_pod_loss")
	seedRuntimePodLostStatusFence(t, admin, sessionID, "bind_missing_kind_pod_loss", 1)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES (
			'default', $1, $2, 'evt_missing_kind_pod_loss_start', 1, 'span.model_request_start',
			'{"type":"span.model_request_start","model_request_id":"mreq_missing_kind_pod_loss"}',
			'internal', false, 'mreq_missing_kind_pod_loss', '{}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`, sessionID, threadID,
	); err != nil {
		t.Fatalf("seed malformed Request Start: %v", err)
	}
	_, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID,
		runtimecontrol.Binding{BindingID: "bind_missing_kind_pod_loss", BindingGeneration: 1, PodUID: "pod_missing_kind_pod_loss"},
		time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing private request kind err = %v; want FailedPrecondition", err)
	}
}
