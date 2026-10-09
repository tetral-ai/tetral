package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

var jobRunnerRetentionKinds = []string{KindRuntimeInput, KindRuntimeRecovery, KindRuntimeConfigUpdate, KindCleanupSession, KindSessionDeleteCleanup}

var nonRunnerRetentionKinds = []string{
	KindEnvironmentBuild, KindEnvironmentReadyFanout,
	KindSandboxToolExecute, KindSandboxActivate, KindSandboxMaterialize, KindSandboxRelease, KindSandboxToolCancel,
	KindSandboxOutputCapture, KindSandboxOutputCaptureCleanup, KindSandboxMemoryProjection,
	KindSandboxBackgroundCommand, KindSandboxBackgroundReconcile,
}

// Under the real Queue role, each Runner kind's acknowledged and cancelled rows
// are deleted exactly 24 hours after their terminal timestamp and dead-lettered
// rows exactly 7 days after; a microsecond younger they stay. Sandbox and
// Environment kinds, nonterminal Runner rows and partition counters are never
// touched, and a Runner row whose terminal timestamp is NULL stays and is
// counted.
func TestJobRunnerTerminalRetentionBoundariesAndAllowlist(t *testing.T) {
	admin, store, _ := newJobRunnerRetentionFixture(t)
	now := jobRunnerRetentionNow(t, admin)
	ages := map[string]time.Duration{StatusAcknowledged: JobRunnerTerminalRetentionAge, StatusCancelled: JobRunnerTerminalRetentionAge, StatusDeadLettered: JobRunnerDeadLetterRetentionAge}
	expired := map[string]bool{}
	for _, kind := range jobRunnerRetentionKinds {
		for status, age := range ages {
			boundary := now.Add(-age)
			expired[seedRetentionJob(t, admin, kind, status, &boundary)] = true
			younger := boundary.Add(time.Microsecond)
			seedRetentionJob(t, admin, kind, status, &younger)
		}
		seedRetentionJob(t, admin, kind, StatusPending, nil)
		seedRetentionJob(t, admin, kind, StatusLeased, nil)
	}
	ancient := now.Add(-30 * 24 * time.Hour)
	for _, kind := range nonRunnerRetentionKinds {
		for status := range ages {
			seedRetentionJob(t, admin, kind, status, &ancient)
		}
	}
	for status := range ages {
		seedRetentionJob(t, admin, KindRuntimeInput, status, nil)
	}
	if _, err := admin.Exec(`INSERT INTO queue_partition_counters (workspace_id, partition_key, last_sequence, created_at, updated_at)
		SELECT DISTINCT workspace_id, partition_key, 1, now(), now() FROM queue_jobs`); err != nil {
		t.Fatal(err)
	}
	counters := countRows(t, admin, `queue_partition_counters`)
	before := retainedJobIDs(t, admin)

	result, err := store.PruneJobRunnerTerminalJobs(context.Background(), JobRunnerTerminalRetentionRequest{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != len(expired) || result.Malformed != 3 || result.ExhaustedStates != 0 {
		t.Fatalf("retention = %+v; want %d deleted, 3 malformed, no exhausted state", result, len(expired))
	}
	after := retainedJobIDs(t, admin)
	for id := range before {
		if expired[id] == after[id] {
			t.Fatalf("job %s: expired %t but retained %t", id, expired[id], after[id])
		}
	}
	if got := countRows(t, admin, `queue_partition_counters`); got != counters {
		t.Fatalf("partition counters = %d; want %d untouched", got, counters)
	}
}

// Each terminal state deletes at most 256 rows per pass. A state whose page
// takes exactly its last 256 eligible rows is not exhausted; with one more row
// the probe after the page finds it and the state counts as exhausted, and the
// next pass finishes the remainder. A state-local error does not stop the
// remaining states.
func TestJobRunnerTerminalRetentionCapsEachStateAndContinuesAfterAStateError(t *testing.T) {
	admin, store, _ := newJobRunnerRetentionFixture(t)
	now := jobRunnerRetentionNow(t, admin)
	old := now.Add(-8 * 24 * time.Hour)
	for range JobRunnerRetentionStateLimit {
		seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &old)
	}
	// A younger acknowledged row and an old Sandbox row are not eligible.
	younger, ancient := now.Add(-time.Hour), now.Add(-30*24*time.Hour)
	seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &younger)
	seedRetentionJob(t, admin, KindSandboxToolExecute, StatusAcknowledged, &ancient)
	result, err := store.PruneJobRunnerTerminalJobs(context.Background(), JobRunnerTerminalRetentionRequest{Now: now})
	if err != nil || result.Deleted != JobRunnerRetentionStateLimit || result.ExhaustedStates != 0 {
		t.Fatalf("exactly one full page = %+v/%v; want 256 deleted and no exhausted state", result, err)
	}
	for range JobRunnerRetentionStateLimit + 1 {
		seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &old)
	}
	seedRetentionJob(t, admin, KindCleanupSession, StatusCancelled, &old)
	seedRetentionJob(t, admin, KindRuntimeRecovery, StatusDeadLettered, &old)

	result, err = store.PruneJobRunnerTerminalJobs(context.Background(), JobRunnerTerminalRetentionRequest{Now: now})
	if err != nil || result.Deleted != JobRunnerRetentionStateLimit+2 || result.ExhaustedStates != 1 {
		t.Fatalf("first pass = %+v/%v; want 256 acknowledged plus one of each other state and one exhausted state", result, err)
	}
	result, err = store.PruneJobRunnerTerminalJobs(context.Background(), JobRunnerTerminalRetentionRequest{Now: now})
	if err != nil || result.Deleted != 1 || result.ExhaustedStates != 0 {
		t.Fatalf("second pass = %+v/%v; want the remaining row and no exhausted state", result, err)
	}

	// A test trigger fails deletion of one cancelled fixture row; the
	// dead-lettered state is still pruned in the same pass.
	marker := seedRetentionJob(t, admin, KindCleanupSession, StatusCancelled, &old)
	seedRetentionJob(t, admin, KindRuntimeRecovery, StatusDeadLettered, &old)
	if _, err := admin.Exec(fmt.Sprintf(`CREATE FUNCTION public.fail_retention_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
		IF OLD.id = %s THEN RAISE EXCEPTION 'fixture retention failure' USING ERRCODE = 'P0001'; END IF; RETURN OLD; END $$;
		CREATE TRIGGER fail_retention_fixture BEFORE DELETE ON queue_jobs FOR EACH ROW EXECUTE FUNCTION public.fail_retention_fixture()`, quotePGLiteral(marker))); err != nil {
		t.Fatal(err)
	}
	result, err = store.PruneJobRunnerTerminalJobs(context.Background(), JobRunnerTerminalRetentionRequest{Now: now})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" || result.Deleted != 1 {
		t.Fatalf("pass with a failing cancelled row = %+v/%v; want the dead-lettered row deleted and the fixture error", result, err)
	}
	if !retainedJobIDs(t, admin)[marker] {
		t.Fatal("failing state deleted its row")
	}
}

// The Queue-only function rejects unknown states and invalid arguments, clamps
// its limit, cannot be executed by any other workload, gives nothing to a
// serving role that sets its purpose flag, resists search-path shadowing, and
// deletes nothing when its definer is not the table owner.
func TestJobRunnerRetentionFunctionBoundary(t *testing.T) {
	ctx := context.Background()
	admin, _, w := newJobRunnerRetentionFixture(t)
	now := jobRunnerRetentionNow(t, admin)
	old := now.Add(-8 * 24 * time.Hour)
	seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &old)
	seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &old)
	cutoff := now.Add(-JobRunnerTerminalRetentionAge)
	call := func(db *sql.DB, args ...any) (int, error) {
		var deleted, malformed int
		err := db.QueryRowContext(ctx, `SELECT deleted_count, malformed_count FROM public.tetral_prune_job_runner_jobs($1, $2, $3)`, args...).Scan(&deleted, &malformed)
		return deleted, err
	}
	for name, args := range map[string][]any{
		"unknown state": {"pending", cutoff, 1},
		"null state":    {nil, cutoff, 1},
		"null cutoff":   {StatusAcknowledged, nil, 1},
		"null limit":    {StatusAcknowledged, cutoff, nil},
	} {
		if _, err := call(w.DB, args...); jobRunnerSQLState(err) != "22023" {
			t.Fatalf("%s: %v; want invalid_parameter_value", name, err)
		}
	}
	if deleted, err := call(w.DB, StatusAcknowledged, cutoff, -5); err != nil || deleted != 0 {
		t.Fatalf("negative limit = %d/%v; want clamped to 0", deleted, err)
	}
	// A caller cutoff later than the database clock minus the state's age is
	// capped there: a day-old acknowledged row and a six-day-old dead-lettered
	// row stay even for a cutoff of now.
	dayOld, sixDaysOld := now.Add(-23*time.Hour), now.Add(-6*24*time.Hour)
	youngAcknowledged := seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &dayOld)
	youngDeadLettered := seedRetentionJob(t, admin, KindRuntimeInput, StatusDeadLettered, &sixDaysOld)
	for _, state := range []string{StatusAcknowledged, StatusDeadLettered} {
		if _, err := call(w.DB, state, now.Add(time.Hour), 256); err != nil {
			t.Fatal(err)
		}
	}
	if retained := retainedJobIDs(t, admin); !retained[youngAcknowledged] || !retained[youngDeadLettered] {
		t.Fatalf("a future cutoff deleted rows younger than their state's age: acknowledged kept %t, dead-lettered kept %t", retained[youngAcknowledged], retained[youngDeadLettered])
	}
	seedRetentionJob(t, admin, KindRuntimeInput, StatusAcknowledged, &old)
	contract, err := database.LoadRoleContract()
	if err != nil {
		t.Fatal(err)
	}
	for _, workload := range contract.WorkloadNames() {
		if workload == "queue" {
			continue
		}
		if _, err := call(w.OpenWorkload(t, workload, nil), StatusAcknowledged, cutoff, 256); jobRunnerSQLState(err) != "42501" {
			t.Fatalf("%s executed Runner retention: %v", workload, err)
		}
	}
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.retention_maintenance', 'true', true)`); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM queue_jobs`).Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("Queue with a spoofed retention flag saw %d jobs (%v)", visible, err)
	}
	_ = tx.Rollback()

	var sandboxRole string
	if err := w.OpenWorkload(t, "sandbox", nil).QueryRow(`SELECT current_user`).Scan(&sandboxRole); err != nil {
		t.Fatal(err)
	}
	//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
	if _, err := admin.Exec(`CREATE SCHEMA retention_shadow;
		CREATE FUNCTION retention_shadow.never(timestamptz, timestamptz) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE OPERATOR retention_shadow.<= (LEFTARG = timestamptz, RIGHTARG = timestamptz, FUNCTION = retention_shadow.never);
		GRANT USAGE ON SCHEMA retention_shadow TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	shadowed, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadowed.ExecContext(ctx, `SET LOCAL search_path = retention_shadow, public, pg_catalog`); err != nil {
		t.Fatal(err)
	}
	var deleted, malformed int
	if err := shadowed.QueryRowContext(ctx, `SELECT deleted_count, malformed_count FROM public.tetral_prune_job_runner_jobs($1, $2, 1)`, StatusAcknowledged, cutoff).Scan(&deleted, &malformed); err != nil || deleted != 1 {
		t.Fatalf("shadowed search path = %d/%v; want one deletion", deleted, err)
	}
	if err := shadowed.Commit(); err != nil {
		t.Fatal(err)
	}

	var ownerMatches, definer, fixedPath, publicExecute bool
	if err := admin.QueryRow(`SELECT pg_get_userbyid(p.proowner) = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.queue_jobs'::regclass),
		       p.prosecdef, COALESCE('search_path=pg_catalog' = ANY(p.proconfig), false),
		       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')
		  FROM pg_proc p WHERE p.oid = 'public.tetral_prune_job_runner_jobs(text,timestamptz,integer)'::regprocedure`).Scan(&ownerMatches, &definer, &fixedPath, &publicExecute); err != nil {
		t.Fatal(err)
	}
	if !ownerMatches || !definer || !fixedPath || publicExecute {
		t.Fatalf("owner=table owner %t definer %t fixed path %t public %t", ownerMatches, definer, fixedPath, publicExecute)
	}
	//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
	if _, err := admin.Exec(`ALTER FUNCTION public.tetral_prune_job_runner_jobs(text,timestamptz,integer) OWNER TO ` + pgx.Identifier{sandboxRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if deleted, err := call(w.DB, StatusAcknowledged, cutoff, 256); err != nil || deleted != 0 {
		t.Fatalf("non-owner definer = %d/%v; want nothing deleted", deleted, err)
	}
}

// With sequential scans and sorts disabled and a generic plan, each terminal
// state's candidate page reads its partial age index under the Limit.
func TestJobRunnerRetentionReadsTheTerminalAgeIndexes(t *testing.T) {
	admin, _, w := newJobRunnerRetentionFixture(t)
	now := jobRunnerRetentionNow(t, admin)
	old := now.Add(-8 * 24 * time.Hour)
	for status := range map[string]bool{StatusAcknowledged: true, StatusCancelled: true, StatusDeadLettered: true} {
		for range 3 {
			seedRetentionJob(t, admin, KindRuntimeInput, status, &old)
		}
	}
	if _, err := admin.Exec(`ANALYZE queue_jobs`); err != nil {
		t.Fatal(err)
	}
	var role string
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	for _, status := range JobRunnerTerminalStates {
		plans := nestedPlansAs(t, admin, role, `SELECT * FROM public.tetral_prune_job_runner_jobs($1, $2, 1)`, status, now)
		index := "idx_queue_jobs_job_runner_" + status + "_retention"
		found, probed := false, false
		for _, plan := range plans {
			if ok, sorts, seqScans := limitScansIndex(plan, index); ok {
				if sorts != 0 || seqScans != 0 {
					t.Fatalf("%s: Limit subtree has %d sorts and %d sequential scans", status, sorts, seqScans)
				}
				found = true
			}
			probed = probed || probeScansIndex(plan, index)
		}
		if !found || !probed {
			t.Fatalf("%s: %s serves the locked page %t and the more_remaining probe %t in %d plans", status, index, found, probed, len(plans))
		}
	}
}

func newJobRunnerRetentionFixture(t *testing.T) (*sql.DB, *PostgreSQLQueueStore, *storagetest.WorkloadDB) {
	t.Helper()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	w := storagetest.OpenWorkloadDB(t, admin, "queue")
	return admin, NewPostgreSQLStore(dbconnect.NewClientForTesting(w.DB)), w
}

func jobRunnerRetentionNow(t *testing.T, admin *sql.DB) time.Time {
	t.Helper()
	var now time.Time
	if err := admin.QueryRow(`SELECT date_trunc('second', clock_timestamp())`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

var retentionJobSequence struct {
	sync.Mutex
	next int
}

// seedRetentionJob inserts one job of kind in status. A terminal status gets
// terminalAt in its own timestamp column; NULL leaves that column empty.
func seedRetentionJob(t *testing.T, admin *sql.DB, kind, status string, terminalAt *time.Time) string {
	t.Helper()
	retentionJobSequence.Lock()
	retentionJobSequence.next++
	id := fmt.Sprintf("qjob_retention_%d", retentionJobSequence.next)
	retentionJobSequence.Unlock()
	var at any
	if terminalAt != nil {
		at = *terminalAt
	}
	if _, err := admin.Exec(`INSERT INTO queue_jobs (id, workspace_id, kind, partition_key, queue_partition_sequence, status, payload_json,
			lease_token, leased_by, leased_at, leased_until, available_at, created_at, updated_at, acknowledged_at, cancelled_at, dead_lettered_at)
		VALUES ($1, 'ws_retention', $2, 'partition:' || $1, 1, $3, '{}',
			CASE WHEN $3 = 'leased' THEN 'qlt_fixture' END, CASE WHEN $3 = 'leased' THEN 'fixture' END,
			CASE WHEN $3 = 'leased' THEN now() END, CASE WHEN $3 = 'leased' THEN now() END,
			now(), now(), now(),
			CASE WHEN $3 = 'acknowledged' THEN $4::timestamptz END,
			CASE WHEN $3 = 'cancelled' THEN $4::timestamptz END,
			CASE WHEN $3 = 'dead_lettered' THEN $4::timestamptz END)`, id, kind, status, at); err != nil {
		t.Fatalf("seed %s %s job: %v", kind, status, err)
	}
	return id
}

func retainedJobIDs(t *testing.T, admin *sql.DB) map[string]bool {
	t.Helper()
	rows, err := admin.Query(`SELECT id FROM queue_jobs`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	ids := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	return ids
}

func countRows(t *testing.T, admin *sql.DB, table string) int {
	t.Helper()
	var count int
	if err := admin.QueryRow(`SELECT count(*) FROM ` + pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func quotePGLiteral(value string) string { return "'" + strings.ReplaceAll(value, "'", "''") + "'" }

func jobRunnerSQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// nestedPlansAs runs statement as role with auto_explain logging each nested
// statement's generic plan, and returns those plans.
func nestedPlansAs(t *testing.T, admin *sql.DB, role, statement string, args ...any) []map[string]any {
	t.Helper()
	ctx := context.Background()
	config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var notices []string
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, notice.Message)
		mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `LOAD 'auto_explain'`); err != nil {
		t.Fatalf("load auto_explain: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, setting := range []string{
		`SET LOCAL auto_explain.log_min_duration = 0`,
		`SET LOCAL auto_explain.log_nested_statements = on`,
		`SET LOCAL auto_explain.log_format = 'json'`,
		`SET LOCAL auto_explain.log_level = 'notice'`,
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_sort = off`,
		`SET LOCAL plan_cache_mode = force_generic_plan`,
		`SET LOCAL ROLE ` + pgx.Identifier{role}.Sanitize(),
	} {
		if _, err := tx.Exec(ctx, setting); err != nil {
			t.Fatalf("%s: %v", setting, err)
		}
	}
	if _, err := tx.Exec(ctx, statement, args...); err != nil {
		t.Fatalf("explain %q: %v", statement, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var plans []map[string]any
	for _, message := range notices {
		start := strings.Index(message, "{")
		if start < 0 {
			continue
		}
		var document struct {
			Plan map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(message[start:]), &document); err == nil && document.Plan != nil {
			plans = append(plans, document.Plan)
		}
	}
	return plans
}

// probeScansIndex reports whether the plan is a top-level Limit that reads
// index by an age condition without locking or sorting: the more_remaining
// probe, as opposed to the locked page or the IS NULL integrity count.
func probeScansIndex(plan map[string]any, index string) bool {
	if nodeType, _ := plan["Node Type"].(string); nodeType != "Limit" {
		return false
	}
	// The only other child is the policy's owner lookup InitPlan.
	var child map[string]any
	children, _ := plan["Plans"].([]any)
	for _, candidate := range children {
		if node, ok := candidate.(map[string]any); ok && node["Parent Relationship"] == "Outer" {
			child = node
		}
	}
	childType, _ := child["Node Type"].(string)
	cond, _ := child["Index Cond"].(string)
	return (childType == "Index Scan" || childType == "Index Only Scan") && child["Index Name"] == index &&
		strings.Contains(cond, "<=") && !strings.Contains(cond, "IS NULL")
}

// limitScansIndex reports whether some Limit's subtree locks rows it scans
// through index (the candidate page), with the Sort and queue_jobs Seq Scan
// nodes found inside that subtree.
func limitScansIndex(node map[string]any, index string) (bool, int, int) {
	if nodeType, _ := node["Node Type"].(string); nodeType == "Limit" {
		found, locks, sorts, seqScans := false, false, 0, 0
		var walk func(map[string]any)
		walk = func(current map[string]any) {
			currentType, _ := current["Node Type"].(string)
			relation, _ := current["Relation Name"].(string)
			switch {
			case currentType == "LockRows":
				locks = true
			case currentType == "Sort" || currentType == "Incremental Sort":
				sorts++
			case currentType == "Seq Scan" && relation == "queue_jobs":
				seqScans++
			case (currentType == "Index Scan" || currentType == "Index Only Scan") && current["Index Name"] == index:
				found = true
			}
			children, _ := current["Plans"].([]any)
			for _, child := range children {
				if childNode, ok := child.(map[string]any); ok {
					walk(childNode)
				}
			}
		}
		walk(node)
		if found && locks {
			return true, sorts, seqScans
		}
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			if found, sorts, seqScans := limitScansIndex(childNode, index); found {
				return true, sorts, seqScans
			}
		}
	}
	return false, 0, 0
}
