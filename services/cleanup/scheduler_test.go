package tetralcleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestSchedulerClaimsOnlyDueBoundIdleRowsAndEnqueuesCleanupJobs(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedCleanupSession(t, admin, "sesn_due")
	seedCleanupSession(t, admin, "sesn_due_second")
	seedCleanupSession(t, admin, "sesn_unbound")
	seedCleanupSession(t, admin, "sesn_future")
	seedCleanupSession(t, admin, "sesn_existing")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = 'default' AND id = 'sesn_due'`); err != nil {
		t.Fatalf("mark due session deleted: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, cleanup_after, cleanup_job_id, binding_id, binding_generation, created_at, updated_at
		) VALUES
			('default', 'sesn_due', 'idle', '2026-01-01T00:00:00Z', NULL, 'bind_due', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			('default', 'sesn_due_second', 'idle', '2026-01-01T00:00:00Z', NULL, 'bind_due_second', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			('default', 'sesn_unbound', 'idle', '2026-01-01T00:00:00Z', NULL, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			('default', 'sesn_future', 'idle', '2026-01-01T01:00:00Z', NULL, 'bind_future', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
			('default', 'sesn_existing', 'idle', '2026-01-01T00:00:00Z', 'cleanup_existing', 'bind_existing', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
	ids := 0
	scheduler := NewScheduler(dbconnect.NewClientForTesting(runtime))
	scheduler.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC) }
	scheduler.IDStrategy = func(prefix string) string {
		ids++
		return prefix + strconv.Itoa(ids)
	}

	claimed, err := scheduler.ClaimDue(context.Background(), ClaimDueRequest{WorkspaceID: workspace.DefaultID, Limit: 10})
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claimed) != 2 ||
		claimed[0].SessionID != "sesn_due" || claimed[0].CleanupJobID != "cleanup_1" || claimed[0].QueueJobID != "qjob_2" ||
		claimed[1].SessionID != "sesn_due_second" || claimed[1].CleanupJobID != "cleanup_3" || claimed[1].QueueJobID != "qjob_4" {
		t.Fatalf("claimed = %#v; want two due cleanup jobs in semantic order", claimed)
	}

	var cleanupJobID string
	var cleanupEnqueuedAt sql.NullString
	var cleanupClaimedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_job_id, cleanup_enqueued_at, cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_due'`).Scan(&cleanupJobID, &cleanupEnqueuedAt, &cleanupClaimedAt); err != nil {
		t.Fatalf("read cleanup status: %v", err)
	}
	if cleanupJobID != "cleanup_1" || !cleanupEnqueuedAt.Valid || cleanupClaimedAt.Valid {
		t.Fatalf("cleanup markers = %q/%v/%v; want job/enqueued/unclaimed", cleanupJobID, cleanupEnqueuedAt, cleanupClaimedAt)
	}
	var kind string
	var partitionKey string
	var dedupeKey string
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT kind, partition_key, dedupe_key, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND id = 'qjob_2'`).Scan(&kind, &partitionKey, &dedupeKey, &payloadJSON); err != nil {
		t.Fatalf("read queue job: %v", err)
	}
	if kind != "cleanup_session" || partitionKey != "session:default:sesn_due" || dedupeKey != "cleanup_session:default:sesn_due:cleanup_1" {
		t.Fatalf("queue identity = %s/%s/%s", kind, partitionKey, dedupeKey)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["workspace_id"] != "default" || payload["session_id"] != "sesn_due" || payload["cleanup_job_id"] != "cleanup_1" {
		t.Fatalf("payload = %#v", payload)
	}
	var firstPartitionJobs int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND id IN ('qjob_2', 'qjob_4')
		    AND queue_partition_sequence = 1`,
	).Scan(&firstPartitionJobs); err != nil {
		t.Fatalf("read cleanup partition sequences: %v", err)
	}
	if firstPartitionJobs != 2 {
		t.Fatalf("cleanup jobs with first partition sequence = %d; want 2", firstPartitionJobs)
	}

	replay, err := scheduler.ClaimDue(context.Background(), ClaimDueRequest{WorkspaceID: workspace.DefaultID, Limit: 10})
	if err != nil {
		t.Fatalf("ClaimDue replay: %v", err)
	}
	if len(replay) != 0 {
		t.Fatalf("replay claimed = %#v; want none", replay)
	}
}

func TestCleanupWorkloadStaysWithinSchedulerBoundary(t *testing.T) {
	for _, path := range []string{
		"scheduler.go",
		filepath.Join("cmd", "tetral-cleanup", "main.go"),
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		text := string(body)
		for _, forbidden := range []string{"internal/" + "blob", "Blob" + "Store", "Delete" + "Prefix", "Collect" + "ResourcePrefixes"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s contains %q; cleanup workload must stay Postgres/Queue-only", path, forbidden)
			}
		}
	}
}

func TestSchedulerMetricsCollectorReportsSafeCounters(t *testing.T) {
	metrics := NewSchedulerMetrics()
	metrics.ObserveClaimDue(2, 25*time.Millisecond)
	metrics.ObserveClaimDue(-1, -time.Second)

	collector := metrics.Collector()
	samples, err := collector(context.Background())
	if err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	got := map[string]float64{}
	for _, sample := range samples {
		got[sample.Name] = sample.Value
		if len(sample.Labels) != 0 {
			t.Fatalf("cleanup scheduler metric %s has labels %#v; want no user/session labels", sample.Name, sample.Labels)
		}
	}
	want := map[string]float64{
		"tetral_cleanup_claim_due_runs_total":        2,
		"tetral_cleanup_jobs_claimed_total":          2,
		"tetral_cleanup_claim_due_duration_ms_total": 25,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("metric %s = %v; want %v in %#v", name, got[name], value, got)
		}
	}
}

func seedCleanupSession(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	now := "2026-01-01T00:00:00Z"
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	threadID := "thr_" + sessionID
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ('default', 'workspace', 'default', $1) ON CONFLICT (id) DO NOTHING`, []any{now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ('default', $1, $1, 1, $2, $2)`, []any{agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ('default', $1, $2, 1, '{}', $3, $4)`, []any{agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ('default', $1, $1, '{}', $2, $2)`, []any{environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at) VALUES ('default', $1, $2, 'session', 'idle', 'active', $3, 1, $4, $5, $5)`, []any{sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ('default', $1, $2, 'main', 'public', 'idle', $3, $3, $3)`, []any{threadID, sessionID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed cleanup session %s: %v", sessionID, err)
		}
	}
}
