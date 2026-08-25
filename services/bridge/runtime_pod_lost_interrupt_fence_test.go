package agentruntimebridge

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLRuntimePodLossInterruptFenceMatrix(t *testing.T) {
	for _, role := range []string{"main", "child"} {
		for _, state := range []string{"before_snapshot_ack", "snapshot_acked", "committed_input_above", "committed_inter_agent_above", "orphan_inter_agent_above"} {
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
				binding := runtimePodLostBinding(sessionID, bindingID, 1)
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
				if state == "committed_inter_agent_above" || state == "orphan_inter_agent_above" {
					deliveryID := "delivery_inter_agent_above_" + suffix
					receivedEventID := "sevt_inter_agent_above_" + suffix
					seedBridgeAPIEvent(t, admin, "default", sessionID, targetThreadID, receivedEventID, 3, "agent.thread_message_received", `{"delivery_id":"`+deliveryID+`"}`)
					if state == "committed_inter_agent_above" {
						seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, targetThreadID, "agent_mail:"+deliveryID, "agent_mail", `[`+fmt.Sprintf("%q", receivedEventID)+`]`, "committed", bindingID, binding.PodUID, 3, 3)
					}
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
				if state == "snapshot_acked" || state == "orphan_inter_agent_above" {
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
					t.Fatalf("pod-loss completion rows = mail %d job %d; want 0/0", completionMailCount, completionJobCount)
				}
			})
		}
	}
}

func TestPostgreSQLRuntimePodLossRepairsSiblingWithoutClosingInterruptedThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_pod_loss_mixed_interrupt"
		mainThreadID    = "thrd_pod_loss_mixed_main"
		interruptedID   = "thrd_pod_loss_mixed_interrupted"
		siblingID       = "thrd_pod_loss_mixed_sibling"
		bindingID       = "bind_pod_loss_mixed_interrupt"
		interruptInput  = "rin_pod_loss_mixed_interrupt"
		interruptEvent  = "evt_pod_loss_mixed_interrupt"
		interruptedReq  = "mreq_pod_loss_mixed_interrupted"
		interruptedTool = "evt_pod_loss_mixed_interrupted_tool"
		siblingReq      = "mreq_pod_loss_mixed_sibling"
		siblingTool     = "evt_pod_loss_mixed_sibling_tool"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, interruptedID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, siblingID)
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
		SET status='running' WHERE workspace_id='default' AND session_id=$1 AND id IN ($2,$3)`,
		sessionID, interruptedID, siblingID); err != nil {
		t.Fatalf("seed mixed pod-loss Threads: %v", err)
	}
	for _, request := range []struct {
		threadID, requestID, startID, toolID, callID string
	}{
		{interruptedID, interruptedReq, "evt_pod_loss_mixed_interrupted_start", interruptedTool, "call_pod_loss_mixed_interrupted"},
		{siblingID, siblingReq, "evt_pod_loss_mixed_sibling_start", siblingTool, "call_pod_loss_mixed_sibling"},
	} {
		seedBridgeAPIEvent(t, admin, "default", sessionID, request.threadID, request.startID, 1,
			"span.model_request_start", `{"type":"span.model_request_start","model_request_id":"`+request.requestID+`","request_kind":"agent_provider_request"}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
			SET visibility='internal', session_visible=false, model_request_id=$3,
			    projection_json='{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}'
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
			sessionID, request.startID, request.requestID); err != nil {
			t.Fatalf("seed mixed pod-loss Request Start: %v", err)
		}
		seedBridgeAPIEvent(t, admin, "default", sessionID, request.threadID, request.toolID, 2,
			"agent.tool_use", `{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}`)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_events
			SET visibility='public', session_visible=true, model_request_id=$3
			WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`,
			sessionID, request.toolID, request.requestID); err != nil {
			t.Fatalf("seed mixed pod-loss Tool Use: %v", err)
		}
		seedBridgeAPIDurableToolMessage(t, admin, "default", sessionID, request.threadID, request.requestID, request.toolID, request.callID, "Read")
	}
	seedBridgeAPIEvent(t, admin, "default", sessionID, interruptedID, interruptEvent, 3, "user.interrupt", `{"type":"user.interrupt"}`)
	seedRuntimeInboxBirthForJob(t, admin, RuntimeJob{
		WorkspaceID: "default", SessionID: sessionID, SessionThreadID: interruptedID,
		RuntimeInputID: interruptInput, InputKind: "interrupt_control", EventIDs: []string{interruptEvent}, SequenceFrom: 3, SequenceTo: 3,
	})
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	enqueueInterruptExhaustionJob(t, queueStore, sessionID, interruptedID, interruptInput, "interrupt_control", interruptEvent, 3, queue.DefaultMaxAttempts, time.Now().UTC())

	now := time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC)
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	if _, err := store.mutateLostRuntimeBinding(context.Background(), "default", sessionID, binding, now, false); err != nil {
		t.Fatalf("repair mixed pod-loss Threads: %v", err)
	}
	var interruptedEnds, interruptedResults, siblingEnds, siblingResults, bindings int
	var interruptedStatus, siblingStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='span.model_request_end' AND model_request_id=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id'=$5),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='span.model_request_end' AND model_request_id=$6),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_event_id'=$7),
		(SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
		(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3)`,
		sessionID, interruptedID, siblingID, interruptedReq, interruptedTool, siblingReq, siblingTool,
	).Scan(&interruptedEnds, &interruptedResults, &siblingEnds, &siblingResults, &bindings, &interruptedStatus, &siblingStatus); err != nil {
		t.Fatalf("read mixed pod-loss settlement: %v", err)
	}
	if interruptedEnds != 0 || interruptedResults != 0 || siblingEnds != 1 || siblingResults != 1 || bindings != 0 ||
		interruptedStatus != "running" || siblingStatus != "idle" {
		t.Fatalf("mixed pod-loss settlement = interrupted end/result/status %d/%d/%s, sibling %d/%d/%s, bindings %d",
			interruptedEnds, interruptedResults, interruptedStatus, siblingEnds, siblingResults, siblingStatus, bindings)
	}
	var interruptQueueStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM queue_jobs
		WHERE workspace_id='default' AND dedupe_key=$1`,
		queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, interruptInput),
	).Scan(&interruptQueueStatus); err != nil {
		t.Fatalf("read interrupted Thread Queue custody: %v", err)
	}
	if interruptQueueStatus != queue.StatusPending {
		t.Fatalf("interrupted Thread Queue custody = %s; want pending for interrupt owner", interruptQueueStatus)
	}
}
