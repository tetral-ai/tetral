package agentruntimebridge

import (
	"context"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestPostgreSQLRuntimePodLossInterruptFenceMatrix(t *testing.T) {
	for _, role := range []string{"main", "child"} {
		for _, state := range []string{"before_snapshot_ack", "snapshot_acked", "committed_input_above", "committed_inter_agent_above"} {
			t.Run(role+"/"+state, func(t *testing.T) {
				runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				suffix := role + "_" + state
				sessionID := "sesn_pod_loss_interrupt_" + suffix
				mainThreadID := "thrd_pod_loss_interrupt_main_" + suffix
				targetThreadID := mainThreadID
				seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
				if role == "child" {
					targetThreadID = "thrd_pod_loss_interrupt_child_" + suffix
					if _, err := admin.ExecContext(context.Background(),
						`INSERT INTO session_threads (
							workspace_id, id, session_id, parent_thread_id, role, visibility, status,
							task_name, created_at, last_active_at, updated_at
						) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'running', $1, $4, $4, $4)`,
						targetThreadID, sessionID, mainThreadID, "2026-01-01T00:00:00Z"); err != nil {
						t.Fatalf("seed child thread: %v", err)
					}
				}
				bindingID := "bind_pod_loss_interrupt_" + suffix
				binding := runtimePodLostReleaseFenceBinding(sessionID, bindingID, 1)
				seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
				runtimeStatus := "running"
				if role == "child" {
					runtimeStatus = "idle"
				}
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_runtime_status (
						workspace_id, session_id, status, binding_id, binding_generation, created_at, updated_at
					) VALUES ('default', $1, $2, $3, 1, $4, $4)`,
					sessionID, runtimeStatus, bindingID, "2026-01-01T00:00:00Z"); err != nil {
					t.Fatalf("seed runtime status: %v", err)
				}
				seedBridgeAPIEvent(t, admin, "default", sessionID, targetThreadID, "sevt_interrupt_"+suffix, 2, "user.interrupt", `{}`)
				if state != "before_snapshot_ack" {
					if _, err := admin.ExecContext(context.Background(),
						`UPDATE session_events SET processed_at = $3
						  WHERE workspace_id = 'default' AND session_id = $1 AND event_id = $2`,
						sessionID, "sevt_interrupt_"+suffix, "2026-01-01T00:00:01Z"); err != nil {
						t.Fatalf("mark interrupt processed: %v", err)
					}
				}
				if state == "committed_input_above" {
					seedBridgeAPIEvent(t, admin, "default", sessionID, targetThreadID, "sevt_above_"+suffix, 3, "user.message", `{}`)
					if _, err := admin.ExecContext(context.Background(),
						`UPDATE session_events SET processed_at = $3
						  WHERE workspace_id = 'default' AND session_id = $1 AND event_id = $2`,
						sessionID, "sevt_above_"+suffix, "2026-01-01T00:00:02Z"); err != nil {
						t.Fatalf("mark above-fence input processed: %v", err)
					}
				}
				if state == "committed_inter_agent_above" {
					seedBridgeAPIEvent(t, admin, "default", sessionID, targetThreadID, "sevt_inter_agent_above_"+suffix, 3, "agent.thread_message_received", `{}`)
				}
				otherThreadID := "thrd_pod_loss_interrupt_other_" + suffix
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_threads (
						workspace_id, id, session_id, parent_thread_id, role, visibility, status,
						task_name, created_at, last_active_at, updated_at
					) VALUES ('default', $1, $2, $3, 'subagent', 'public', 'idle', $1, $4, $4, $4)`,
					otherThreadID, sessionID, mainThreadID, "2026-01-01T00:00:00Z"); err != nil {
					t.Fatalf("seed other thread: %v", err)
				}
				seedBridgeAPIEvent(t, admin, "default", sessionID, otherThreadID, "sevt_other_"+suffix, 99, "user.message", `{}`)
				if _, err := admin.ExecContext(context.Background(),
					`UPDATE session_events SET processed_at = $3
					  WHERE workspace_id = 'default' AND session_id = $1 AND event_id = $2`,
					sessionID, "sevt_other_"+suffix, "2026-01-01T00:00:03Z"); err != nil {
					t.Fatalf("mark other-thread input processed: %v", err)
				}

				now := time.Date(2026, 1, 1, 0, 0, 4, 0, time.UTC)
				for attempt := 0; attempt < 2; attempt++ {
					if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, sessionID, binding, now); err != nil {
						t.Fatalf("repair attempt %d: %v", attempt+1, err)
					}
				}

				var errorCount, endTurnCount, exhaustedCount, completionMailCount, completionJobCount int
				idleType := "session.status_idle"
				if role == "child" {
					idleType = "session.thread_status_idle"
				}
				if err := admin.QueryRowContext(context.Background(),
					`SELECT
					   count(*) FILTER (WHERE type = 'session.error'),
					   count(*) FILTER (WHERE type = $3 AND payload_json::jsonb #>> '{stop_reason,type}' = 'end_turn'),
					   count(*) FILTER (WHERE type = $3 AND payload_json::jsonb #>> '{stop_reason,type}' = 'retries_exhausted')
					 FROM session_events
					 WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2`,
					sessionID, targetThreadID, idleType).Scan(&errorCount, &endTurnCount, &exhaustedCount); err != nil {
					t.Fatalf("read settlement facts: %v", err)
				}
				if state == "snapshot_acked" {
					if errorCount != 0 || endTurnCount != 1 || exhaustedCount != 0 {
						t.Fatalf("settlement error/end_turn/exhausted = %d/%d/%d; want 0/1/0", errorCount, endTurnCount, exhaustedCount)
					}
				} else if errorCount != 1 || endTurnCount != 0 || exhaustedCount != 1 {
					t.Fatalf("settlement error/end_turn/exhausted = %d/%d/%d; want 1/0/1", errorCount, endTurnCount, exhaustedCount)
				}
				if err := admin.QueryRowContext(context.Background(),
					`SELECT count(*) FROM session_events
					  WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent'`,
					sessionID,
				).Scan(&completionMailCount); err != nil {
					t.Fatalf("count pod-loss completion mail: %v", err)
				}
				if err := admin.QueryRowContext(context.Background(),
					`SELECT count(*) FROM queue_jobs
					  WHERE workspace_id='default' AND payload_json::jsonb ->> 'input_kind'='agent_mail'
					    AND payload_json::jsonb ->> 'session_id'=$1`,
					sessionID,
				).Scan(&completionJobCount); err != nil {
					t.Fatalf("count pod-loss completion jobs: %v", err)
				}
				if completionMailCount != 0 || completionJobCount != 0 {
					t.Fatalf("pod-loss completion rows = mail %d job %d; want zero", completionMailCount, completionJobCount)
				}
			})
		}
	}
}
