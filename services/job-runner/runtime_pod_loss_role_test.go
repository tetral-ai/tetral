package jobrunner

import (
	"context"
	"testing"

	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// This is an installed-role transaction regression, not a physical Pod-loss
// test. The seeded binding supplies the exact already-confirmed lost custody;
// the owning fenced repair must atomically close its still-open request.
func TestPostgreSQLInstalledJobRunnerPodLossRecordsTerminalUsage(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	serving := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	fixture := seedRuntimePodLostDeliveryFixture(t, admin, 181, "Write", "idle", false, false, false, false, true)
	store := runtimePodLossSweepStore(t, serving.DB, nil, func() enginekubernetes.BindingVisibilitySnapshot {
		return enginekubernetes.NewBindingVisibilitySnapshotForTest(true, nil)
	})
	if err := store.repairLostRuntimeBinding(context.Background(), "default", fixture.sessionID, fixture.binding, fixture.now); err != nil {
		t.Fatalf("installed job_runner fenced open-request repair: %v", err)
	}
	var ends, usage, bindings int
	var input, output, total int64
	if err := admin.QueryRowContext(context.Background(), `SELECT
  (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='span.model_request_end' AND model_request_id=$2 AND payload_json::jsonb->>'error_kind'='runtime_pod_lost'),
  (SELECT count(*) FROM request_usage_details WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2),
  (SELECT count(*) FROM session_runtime_bindings WHERE workspace_id='default' AND session_id=$1)`, fixture.sessionID, fixture.modelRequestID).Scan(&ends, &usage, &bindings); err != nil {
		t.Fatal(err)
	}
	if ends != 1 || usage != 1 || bindings != 0 {
		t.Fatalf("terminal repair effects end=%d usage=%d bindings=%d", ends, usage, bindings)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT input_total_tokens,output_total_tokens,total_tokens FROM request_usage_details WHERE workspace_id='default' AND session_id=$1 AND model_request_id=$2`, fixture.sessionID, fixture.modelRequestID).Scan(&input, &output, &total); err != nil {
		t.Fatal(err)
	}
	if input != 0 || output != 0 || total != 0 {
		t.Fatalf("loss-repair invented usage %d/%d/%d", input, output, total)
	}
	assertRuntimePodLostRetryableError(t, store.repairLostRuntimeBinding(context.Background(), "default", fixture.sessionID, fixture.binding, fixture.now), "runtime_pod_lost_claim_stale")
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM request_usage_details u JOIN session_events e ON e.workspace_id=u.workspace_id AND e.session_id=u.session_id AND e.model_request_id=u.model_request_id AND e.runtime_write_id=u.runtime_write_id AND e.type='span.model_request_end' WHERE u.workspace_id='default' AND u.session_id=$1 AND u.model_request_id=$2`, fixture.sessionID, fixture.modelRequestID).Scan(&usage); err != nil || usage != 1 {
		t.Fatalf("replay changed original terminal End/usage identity: %d/%v", usage, err)
	}
	// The second fresh scope proves both added privileges are causally required
	// and that failing after the End append rolls back the entire repair.
	second := seedRuntimePodLostDeliveryFixture(t, admin, 182, "Write", "idle", false, false, false, false, true)
	snapshot := func() string {
		t.Helper()
		var value string
		if err := admin.QueryRowContext(context.Background(), `SELECT jsonb_build_object(
		 'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY sequence) FROM session_events e WHERE workspace_id='default' AND session_id=$1),
		 'stream_changes',(SELECT jsonb_agg(to_jsonb(c) ORDER BY stream_position) FROM session_event_stream_changes c WHERE workspace_id='default' AND session_id=$1),
		 'messages',(SELECT jsonb_agg(to_jsonb(m) ORDER BY sequence) FROM session_messages m WHERE workspace_id='default' AND session_id=$1),
		 'routes',(SELECT jsonb_agg(to_jsonb(p) ORDER BY tool_use_event_id) FROM session_pending_tool_uses p WHERE workspace_id='default' AND session_id=$1),
		 'bindings',(SELECT jsonb_agg(to_jsonb(b)) FROM session_runtime_bindings b WHERE workspace_id='default' AND session_id=$1),
		 'runtime_status',(SELECT jsonb_agg(to_jsonb(s) ORDER BY session_id) FROM session_runtime_status s WHERE workspace_id='default' AND session_id=$1),
		 'usage',(SELECT jsonb_agg(to_jsonb(u) ORDER BY model_request_id,runtime_write_id) FROM request_usage_details u WHERE workspace_id='default' AND session_id=$1)
		)::text`, second.sessionID).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := snapshot()
	for _, privilege := range []string{"INSERT", "SELECT"} {
		// SELECT is needed by the explicit ON CONFLICT key target; there is
		// no UPDATE or DELETE requirement for the zero-usage audit append.
		serving.RequirePrivilege(t, "request_usage_details", privilege, func() error {
			return store.repairLostRuntimeBinding(context.Background(), "default", second.sessionID, second.binding, second.now)
		})
		if snapshot() != before {
			t.Fatalf("missing usage %s leaked partial repair rows", privilege)
		}
	}
	if err := store.repairLostRuntimeBinding(context.Background(), "default", second.sessionID, second.binding, second.now); err != nil {
		t.Fatalf("restored installed-role repair: %v", err)
	}
}
