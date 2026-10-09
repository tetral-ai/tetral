package eventstream_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Under the real Event Stream role, one statement reports each requested
// Session's lifecycle and the greater of its newest change of any visibility
// and its highest feed watermark; a missing Session is reported as missing.
func TestSessionSignalsReportLifecycleAndTheHighestPosition(t *testing.T) {
	f := newRetentionReaderFixture(t)
	ctx := context.Background()
	seedEventStreamSession(t, f.admin, "default", "sesn_signal_pruned", "thr_signal_pruned")
	for _, session := range []string{"sesn_signal_live", "sesn_signal_deleted"} {
		if _, err := f.admin.Exec(`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at)
			SELECT workspace_id, $1, 'thr_' || $1, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at FROM sessions WHERE id = 'sesn_signal_pruned'`, session); err != nil {
			t.Fatal(err)
		}
		if _, err := f.admin.Exec(`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at)
			VALUES ('default', 'thr_' || $1, $1, 'main', 'public', 'idle', now(), now(), now())`, session); err != nil {
			t.Fatal(err)
		}
	}
	f.seedFreshChange(t, "sesn_signal_live", "thr_sesn_signal_live", "evt_signal_public", 1)
	seedEventStreamEvent(t, f.admin, "default", "sesn_signal_live", "thr_sesn_signal_live", "evt_signal_internal", 2, "agent.thinking", `{}`, "internal", false, "2026-01-01T00:00:00Z")
	var internal int64
	if err := f.admin.QueryRow(`INSERT INTO session_event_stream_changes (workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at)
		VALUES ('default', 'sesn_signal_live', 'evt_signal_internal', 'thr_sesn_signal_live', 1, 'internal', false, clock_timestamp()) RETURNING stream_position`).Scan(&internal); err != nil {
		t.Fatal(err)
	}
	retained := f.seedFreshChange(t, "sesn_signal_pruned", "thr_signal_pruned", "evt_signal_kept", 1)
	pruned := f.seedChange(t, "sesn_signal_pruned", "thr_child", "evt_signal_pruned", false, "public")
	f.pruneAll(t)
	if got := f.watermark(t, "sesn_signal_pruned", "thread:thr_child"); got != pruned || pruned <= retained {
		t.Fatalf("child watermark = %d; want %d above the retained %d", got, pruned, retained)
	}
	if _, err := f.admin.Exec(`UPDATE sessions SET lifecycle_state = 'deleted' WHERE id = 'sesn_signal_deleted'`); err != nil {
		t.Fatal(err)
	}
	signals, err := f.reader.ReadSessionSignals(ctx, workspace.DefaultID, []string{"sesn_signal_live", "sesn_signal_pruned", "sesn_signal_deleted", "sesn_signal_missing"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]eventstream.SessionSignal{}
	for _, signal := range signals {
		got[signal.SessionID] = signal
	}
	want := map[string]eventstream.SessionSignal{
		"sesn_signal_live":    {SessionID: "sesn_signal_live", Exists: true, LifecycleState: "active", Position: internal},
		"sesn_signal_pruned":  {SessionID: "sesn_signal_pruned", Exists: true, LifecycleState: "active", Position: pruned},
		"sesn_signal_deleted": {SessionID: "sesn_signal_deleted", Exists: true, LifecycleState: "deleted"},
		"sesn_signal_missing": {SessionID: "sesn_signal_missing"},
	}
	if len(signals) != len(want) || !reflect.DeepEqual(got, want) {
		t.Fatalf("signals = %+v; want %+v", signals, want)
	}
}

// Under the real Event Stream role, with sequential scans and sorts disabled,
// the signal statement seeks each Session's newest change through the change
// primary key and its highest watermark through the watermark index, both
// under their Limit.
func TestSessionSignalsSeekTheChangeKeyAndTheWatermarkIndex(t *testing.T) {
	f := newRetentionReaderFixture(t)
	seedEventStreamSession(t, f.admin, "default", "sesn_signal_plan", "thr_signal_plan")
	f.seedChange(t, "sesn_signal_plan", "thr_signal_plan", "evt_signal_plan_a", true, "public")
	f.seedChange(t, "sesn_signal_plan", "thr_child", "evt_signal_plan_b", false, "public")
	f.pruneAll(t)
	f.seedFreshChange(t, "sesn_signal_plan", "thr_signal_plan", "evt_signal_plan_c", 1)
	if _, err := f.admin.Exec(`ANALYZE session_event_stream_changes; ANALYZE session_event_feed_retention; ANALYZE sessions`); err != nil {
		t.Fatal(err)
	}
	var role string
	if err := f.workload.DB.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	plan := explainAs(t, f.admin, role, eventstream.SessionSignalsQueryForTest, "default", []string{"sesn_signal_plan"})
	found := map[string]bool{}
	var sorts, seqScans int
	var walk func(node map[string]any, underLimit bool)
	walk = func(node map[string]any, underLimit bool) {
		nodeType, _ := node["Node Type"].(string)
		relation, _ := node["Relation Name"].(string)
		switch {
		case nodeType == "Limit":
			underLimit = true
		case nodeType == "Sort" || nodeType == "Incremental Sort":
			sorts++
		case nodeType == "Seq Scan" && (relation == "session_event_stream_changes" || relation == "session_event_feed_retention"):
			seqScans++
		case (nodeType == "Index Scan" || nodeType == "Index Only Scan") && underLimit:
			index, _ := node["Index Name"].(string)
			found[index] = true
		}
		children, _ := node["Plans"].([]any)
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				walk(childNode, underLimit)
			}
		}
	}
	walk(plan, false)
	if !found["session_event_stream_changes_pkey"] || !found["idx_session_event_feed_retention_pruned"] || sorts != 0 || seqScans != 0 {
		t.Fatalf("signal plan: indexes under Limit %v, sorts %d, seq scans %d", found, sorts, seqScans)
	}
}
