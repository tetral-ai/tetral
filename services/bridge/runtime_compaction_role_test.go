package agentruntimebridge

import (
	"context"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLBridgeCompactionRoleCommitsCheckpointAndConsumesPrefix(t *testing.T) {
	for _, child := range []bool{false, true} {
		name := "main_without_prefix"
		if child {
			name = "child_with_prefix"
		}
		t.Run(name, func(t *testing.T) {
			_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			seedBridgeAPISession(t, admin, "default", "sesn_compaction_role", "thr_main")
			seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_compaction_role", "bind_compaction_role", 1, "pod_compaction_role")
			workload := storagetest.OpenWorkloadDB(t, admin, "bridge")
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(workload.DB))
			ctx := context.Background()
			scope := bridgeAPIScope("sesn_compaction_role", "thr_main", "bind_compaction_role", 1, "pod_compaction_role")
			running, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rw_running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
			if err != nil || running.GetCommitted() == nil {
				t.Fatalf("start turn: %v, %v", running, err)
			}
			var prefix *bridgev1.PrefixConsumptionDraft
			if child {
				seedBridgeAPIChildThread(t, admin, "default", scope.SessionId, "thr_main", "thr_child")
				if _, err := admin.Exec(`INSERT INTO session_thread_context_prefixes
					(workspace_id,session_id,child_thread_id,parent_thread_id,parent_boundary_event_id,entries_json,created_at)
					VALUES ('default',$1,'thr_child','thr_main',$2,'[]',now())`, scope.SessionId, running.GetCommitted().GetEventId()); err != nil {
					t.Fatal(err)
				}
				scope.SessionThreadId = "thr_child"
				prefix = &bridgev1.PrefixConsumptionDraft{ChildThreadId: "thr_child", ParentBoundaryEventId: running.GetCommitted().GetEventId()}
				if _, err := store.WriteEvent(ctx, &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "rw_child_running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`}); err != nil {
					t.Fatal(err)
				}
			}
			seedBridgeAPIRequestStart(t, store, scope, "rw_start", "mreq_compaction_role", requestKindCompactionSummary, 0)
			boundary := int64(0)
			request := &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rw_end", ModelRequestId: "mreq_compaction_role", FinishReason: "end_turn", UsageJson: `{}`,
				ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "compacted"},
				CompactionContext:        bridgeTextContextDeltaForTest("retained summary"), CompactedThroughMessageSequence: &boundary,
				CompactionEventPayloadJson: `{"type":"agent.thread_context_compacted"}`, PrefixConsumption: prefix,
			}
			workload.RequirePrivilege(t, "session_thread_context_prefixes", "UPDATE", func() error {
				_, err := store.WriteRequestEnd(ctx, request)
				return err
			})
			var count int
			if err := admin.QueryRow(`SELECT count(*) FROM session_messages WHERE kind='compaction'`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed compaction retained %d checkpoints: %v", count, err)
			}
			response, err := store.WriteRequestEnd(ctx, request)
			if err != nil || response.GetCommitted() == nil {
				t.Fatalf("compaction: %v, %v", response, err)
			}
			if replay, err := store.WriteRequestEnd(ctx, request); err != nil || replay.GetDuplicate() == nil {
				t.Fatalf("compaction replay: %v, %v", replay, err)
			}
			if err := admin.QueryRow(`SELECT count(*) FROM session_messages WHERE kind='compaction' AND sequence=1`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("compaction checkpoints = %d: %v", count, err)
			}
			if child {
				if err := admin.QueryRow(`SELECT count(*) FROM session_thread_context_prefixes p
					JOIN session_messages m ON m.workspace_id=p.workspace_id AND m.message_id=p.consumed_by_checkpoint_message_id
					WHERE p.child_thread_id='thr_child' AND m.session_thread_id='thr_child' AND m.kind='compaction' AND m.sequence=1`).Scan(&count); err != nil || count != 1 {
					t.Fatalf("consumed prefix checkpoints = %d: %v", count, err)
				}
			}
		})
	}
}
