package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

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

func bridgeAcceptedMessageDeliveryPayload(t *testing.T, runtime *sql.DB, workspaceID string, sessionID string, threadID string, runtimeInputID string, eventIDs []string, sequenceFrom int64, sequenceTo int64) string {
	t.Helper()
	client := dbconnect.NewClientForTesting(runtime)
	var payloadJSON string
	if err := client.WithWorkspaceTx(context.Background(), workspaceID, "jobrunner.test_accepted_message_delivery_payload", func(tx *dbconnect.Tx) error {
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
	sessionfixture.SeedBridgeAPIDurableToolMessage(t, db, workspaceID, sessionID, threadID,
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
