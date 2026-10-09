package jobrunner

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

// seedRunnerToolTurn records one open request whose Tool Use has a durable
// Tool Call and no result.
func seedRunnerToolTurn(t *testing.T, admin *sql.DB, sessionID, threadID, modelRequestID, toolUseEventID, eventType, payloadJSON string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, model_request_id, projection_json, model_tool_call_id, latest_stream_position, created_at, updated_at
	) VALUES
	('default', $1, $2, $5, 1, 'span.model_request_start', '{}', 'internal', false, $3,
	 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', NULL, 1, now(), now()),
	('default', $1, $2, $4, 2, $6, $7, 'public', true, $3, '{}', 'call_' || $4, 2, now(), now())`,
		sessionID, threadID, modelRequestID, toolUseEventID, "evt_start_"+toolUseEventID, eventType, payloadJSON); err != nil {
		t.Fatalf("seed Tool turn: %v", err)
	}
	sessionfixture.SeedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, threadID, modelRequestID, toolUseEventID, "call_"+toolUseEventID, "Read")
}

func runnerResultWriteID(t *testing.T, admin *sql.DB, eventID string) string {
	t.Helper()
	var writeID sql.NullString
	if err := admin.QueryRowContext(context.Background(), `SELECT runtime_write_id FROM session_events WHERE event_id=$1`, eventID).Scan(&writeID); err != nil {
		t.Fatalf("read result writer identity: %v", err)
	}
	return writeID.String
}

func requireOneRunnerResult(t *testing.T, facts map[string]sessionfixture.ToolRelationFact, toolUseEventID string) sessionfixture.ToolRelationFact {
	t.Helper()
	var found []sessionfixture.ToolRelationFact
	for _, fact := range facts {
		if fact.ToolUseEventID == toolUseEventID {
			found = append(found, fact)
		}
	}
	if len(found) != 1 {
		t.Fatalf("results referencing %s = %+v; want exactly one", toolUseEventID, found)
	}
	return found[0]
}

// The Runner's terminal Tool result writers derive the relation column, public
// payload key and projection from the one Tool Use they settle, under the real
// Job Runner role.
func TestPostgreSQLRunnerToolRelationWritersDeriveOneFact(t *testing.T) {
	for _, family := range []struct {
		name, eventType, payload, resultType string
	}{
		{"pod loss ordinary", "agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{"path":"a.txt"},"evaluated_permission":"allow"}`, "agent.tool_result"},
		{"pod loss MCP", "agent.mcp_tool_use", `{"type":"agent.mcp_tool_use","name":"Read","mcp_server_name":"github","input":{"q":"x"},"evaluated_permission":"allow"}`, "agent.mcp_tool_result"},
	} {
		t.Run(family.name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			runner := storagetest.OpenWorkloadDB(t, admin, "job_runner").DB
			const (
				sessionID = "sesn_runner_relation_pod_loss"
				threadID  = "thr_runner_relation_pod_loss"
				toolUseID = "evt_runner_relation_pod_loss_tool"
			)
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runner_relation", 1, "pod_runner_relation")
			sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, "bind_runner_relation", 1)
			seedRunnerToolTurn(t, admin, sessionID, threadID, "mreq_runner_relation", toolUseID, family.eventType, family.payload)
			if _, err := runRuntimePodLostRepairTransaction(context.Background(), runner, sessionID,
				runtimecontrol.Binding{BindingID: "bind_runner_relation", BindingGeneration: 1, PodUID: "pod_runner_relation"},
				time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)); err != nil {
				t.Fatalf("repair lost Pod: %v", err)
			}
			facts := sessionfixture.RequireToolRelationFactsForTest(t, admin, "default", sessionID)
			result := requireOneRunnerResult(t, facts, toolUseID)
			if result.EventType != family.resultType || runnerResultWriteID(t, admin, result.EventID) != "rwrite_runtime_pod_lost_tool_"+toolUseID {
				t.Fatalf("Pod-loss result = %+v; want the shared terminal writer's %s", result, family.resultType)
			}
		})
	}

	t.Run("cleanup", func(t *testing.T) {
		_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		runner := storagetest.OpenWorkloadDB(t, admin, "job_runner").DB
		const (
			sessionID = "sesn_runner_relation_cleanup"
			threadID  = "thr_runner_relation_cleanup"
			toolUseID = "evt_runner_relation_cleanup_tool"
		)
		sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_runner_cleanup", 1, "pod_runner_cleanup")
		seedRunnerToolTurn(t, admin, sessionID, threadID, "mreq_runner_cleanup", toolUseID, "agent.tool_use",
			`{"type":"agent.tool_use","name":"Read","input":{"path":"a.txt"},"evaluated_permission":"allow"}`)
		if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
			workspace_id,session_id,session_thread_id,tool_use_event_id,tool_kind,normalized_input_hash,tool_name,input_json,
			ack_status,model_tool_call_id,execution_state,execution_attempt_generation,created_at,updated_at
		) VALUES ('default',$1,$2,$3,'sandbox_tool',$4,'Read','{"path":"a.txt"}','committed',$5,'pending',1,now(),now())`,
			sessionID, threadID, toolUseID, runtimecontrol.Sha256Hex(`{"path":"a.txt"}`), "call_"+toolUseID); err != nil {
			t.Fatalf("seed pending Sandbox execution: %v", err)
		}
		if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_pending_tool_uses (
			workspace_id,session_id,session_thread_id,tool_use_event_id,model_tool_call_id,tool_name,input_json,status,created_at,updated_at
		) VALUES ('default',$1,$2,$3,$4,'Read','{"path":"a.txt"}','cancelled',now(),now())`, sessionID, threadID, toolUseID, "call_"+toolUseID); err != nil {
			t.Fatalf("seed cancelled route: %v", err)
		}
		claim := cleanupSessionClaim{
			WorkspaceID: "default", SessionID: sessionID, IdleStreamPosition: 2,
			BindingID: "bind_runner_cleanup", BindingGeneration: 1, PodUID: "pod_runner_cleanup", RuntimeProcessID: "process_pod_runner_cleanup",
		}
		if err := dbconnect.NewClientForTesting(runner).WithWorkspaceTx(context.Background(), "default", "test.runner_relation_cleanup", func(tx *dbconnect.Tx) error {
			return expireCleanupSandboxExecutionsTx(context.Background(), tx, claim, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC))
		}); err != nil {
			t.Fatalf("expire cleanup Sandbox execution: %v", err)
		}
		facts := sessionfixture.RequireToolRelationFactsForTest(t, admin, "default", sessionID)
		result := requireOneRunnerResult(t, facts, toolUseID)
		var executionState, consumedBy string
		if err := admin.QueryRowContext(context.Background(), `SELECT execution_state, consumed_by_terminal_event_id FROM session_runtime_tool_results
			WHERE workspace_id='default' AND session_id=$1 AND tool_use_event_id=$2`, sessionID, toolUseID).Scan(&executionState, &consumedBy); err != nil {
			t.Fatal(err)
		}
		if result.EventType != "agent.tool_result" || executionState != "consumed" || consumedBy != result.EventID {
			t.Fatalf("cleanup result = %+v; execution %s consumed by %s", result, executionState, consumedBy)
		}
	})
}
