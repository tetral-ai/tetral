package jobrunner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

func TestPodLossPreservesFinishedMainTurnWhileChildRuns(t *testing.T) {
	for _, tc := range []struct{ name, childID string }{
		{"child sorts first", "thrd_child"},
		{"child sorts last", "thrd_zz_child"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			workloads := storagetest.OpenWorkloadDB(t, admin, "job_runner")
			const (
				sessionID = "sesn_podloss_child"
				mainID    = "thrd_podloss_main"
				bindingID = "bind_podloss"
				childReq  = "mreq_podloss_child"
			)
			childID := tc.childID
			ctx := context.Background()
			sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, mainID)
			sessionfixture.SeedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
			binding := sessionfixture.RuntimePodLostBinding(sessionID, bindingID, 1)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, binding.PodUID)
			if _, err := admin.ExecContext(ctx, `UPDATE session_runtime_status SET binding_id=$2, binding_generation=1 WHERE workspace_id='default' AND session_id=$1`, sessionID, bindingID); err != nil {
				t.Fatal(err)
			}
			// The main thread's own turn: running, then idle with end_turn.
			seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_main_running", 1, "session.status_running", `{"type":"session.status_running"}`)
			seedBridgeAPIEvent(t, admin, "default", sessionID, mainID, "evt_main_idle", 2, "session.status_idle", `{"type":"session.status_idle","stop_reason":{"type":"end_turn"}}`)
			// The child runs, mid model request.
			if _, err := admin.ExecContext(ctx, `UPDATE session_threads SET status='running' WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, childID); err != nil {
				t.Fatal(err)
			}
			seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_child_start", 1,
				"span.model_request_start", `{"type":"span.model_request_start","model_request_id":"`+childReq+`","request_kind":"agent_provider_request"}`)
			if _, err := admin.ExecContext(ctx, `UPDATE session_events
				SET visibility='internal', session_visible=false, model_request_id=$3,
				    projection_json='{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}'
				WHERE workspace_id='default' AND session_id=$1 AND event_id=$2`, sessionID, "evt_child_start", childReq); err != nil {
				t.Fatal(err)
			}
			client := dbconnect.NewClientForTesting(workloads.DB)
			{
				if err := client.WithWorkspaceTx(ctx, "default", "test.refresh", func(tx *dbconnect.Tx) error {
					if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, "default", sessionID); err != nil {
						return err
					}
					return runtimecontrol.RefreshSessionStatusTx(ctx, tx, "default", sessionID, time.Now().UTC())
				}); err != nil {
					t.Fatalf("refresh: %v", err)
				}
			}
			var srsBefore string
			if err := admin.QueryRowContext(ctx, `SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1`, sessionID).Scan(&srsBefore); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 8, 24, 21, 0, 0, 0, time.UTC)
			store := NewPostgreSQLRuntimeDeliveryStore(client, 9090, KubernetesRuntimeTargetResolver{GetPod: fixtureAbsentRuntimePod, Snapshot: func() enginekubernetes.BindingVisibilitySnapshot {
				return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
			}})
			if _, err := store.mutateLostRuntimeBinding(ctx, "default", sessionID, binding, now, false); err != nil {
				t.Fatalf("repair: %v", err)
			}
			var mainErrors, primaryErrors, mainIdles, childErrors, aggregateIdles int
			var mainLatestIdle, sessionIdle, mainStatus, childStatus, srsAfter string
			if err := admin.QueryRowContext(ctx, `SELECT
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.error'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.error' AND session_visible),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.status_idle'),
				(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$3 AND type='session.error'),
				(SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='session.status_idle' ORDER BY sequence DESC LIMIT 1),
				COALESCE((SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id IS NULL AND type='session.status_idle' ORDER BY sequence DESC LIMIT 1),''),
				(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$2),
				(SELECT status FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND id=$3),
				(SELECT status FROM session_runtime_status WHERE workspace_id='default' AND session_id=$1),
 (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id IS NULL AND type='session.status_idle')`,
				sessionID, mainID, childID).Scan(&mainErrors, &primaryErrors, &mainIdles, &childErrors, &mainLatestIdle, &sessionIdle, &mainStatus, &childStatus, &srsAfter, &aggregateIdles); err != nil {
				t.Fatal(err)
			}
			t.Logf("srs before=%s after=%s; main thread: session.error=%d status_idle=%d latest idle=%s status=%s; child: session.error=%d status=%s; primary-stream session.error=%d; Session status_idle=%s",
				srsBefore, srsAfter, mainErrors, mainIdles, mainLatestIdle, mainStatus, childErrors, childStatus, primaryErrors, sessionIdle)
			var idle struct {
				StopReason struct {
					Type string `json:"type"`
				} `json:"stop_reason"`
			}
			if err := json.Unmarshal([]byte(sessionIdle), &idle); err != nil {
				t.Fatal(err)
			}
			if srsBefore != "running" || srsAfter != "idle" || aggregateIdles != 1 || idle.StopReason.Type != "end_turn" || childErrors != 1 || childStatus != "idle" {
				t.Errorf("aggregate %s -> %s, idle events %d reason %s, child errors %d status %s", srsBefore, srsAfter, aggregateIdles, idle.StopReason.Type, childErrors, childStatus)
			}

			if mainErrors != 0 || mainIdles != 1 {
				t.Errorf("main thread whose turn had ended was settled as lost: session.error=%d status_idle=%d (want 0 and 1)", mainErrors, mainIdles)
			}
		})
	}
}
