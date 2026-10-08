package tetralcleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// tetral_cleanup_due_sessions is Cleanup's only cross-Workspace read. The real
// Cleanup role pages through every Workspace in key order; its own direct
// reads, a spoofed purpose flag, a shadowed search path or a function owned by
// anyone but the table owner reveal nothing, other workloads cannot execute it,
// and Cleanup can only read and update the cursor row.
func TestCleanupDiscoveryFunctionBoundary(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	past := dbTime(t, admin, "clock_timestamp() - interval '1 hour'")
	seedDueSessions(t, admin, "default", past, "sesn_disc_a")
	seedDueSessions(t, admin, "ws_cleanup_other", past, "sesn_disc_b")
	seedDueSessions(t, admin, "ws_cleanup_other", past.Add(time.Second), "sesn_disc_c")
	cutoff := dbTime(t, admin, "clock_timestamp()")
	var cleanupRole string
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&cleanupRole); err != nil {
		t.Fatal(err)
	}

	t.Run("RealCleanupRolePages", func(t *testing.T) {
		for _, step := range []struct {
			afterAt any
			afterID any
			limit   int
			want    string
		}{
			{nil, nil, 100, "default/sesn_disc_a ws_cleanup_other/sesn_disc_b ws_cleanup_other/sesn_disc_c"},
			{nil, nil, 1, "default/sesn_disc_a"},
			{past, "sesn_disc_a", 1, "ws_cleanup_other/sesn_disc_b"},
			{past, "sesn_disc_b", 100, "ws_cleanup_other/sesn_disc_c"},
			{past.Add(time.Second), "sesn_disc_c", 100, ""},
		} {
			if got := discoverAs(t, w.DB, cutoff, step.afterAt, step.afterID, step.limit); got != step.want {
				t.Fatalf("page after %v/%v limit %d = %q; want %q", step.afterAt, step.afterID, step.limit, got, step.want)
			}
		}
		if got := discoverAs(t, w.DB, past.Add(-time.Second), nil, nil, 100); got != "" {
			t.Fatalf("page before every due time = %q; want empty", got)
		}
	})

	t.Run("InvalidArguments", func(t *testing.T) {
		future := cutoff.Add(time.Hour)
		for name, args := range map[string][]any{
			"future cutoff":    {future, nil, nil, 1},
			"missing cutoff":   {nil, nil, nil, 1},
			"time without id":  {cutoff, past, nil, 1},
			"id without time":  {cutoff, nil, "sesn_disc_a", 1},
			"empty session id": {cutoff, past, "", 1},
			"zero limit":       {cutoff, nil, nil, 0},
			"limit above cap":  {cutoff, nil, nil, 101},
			"null limit":       {cutoff, nil, nil, nil},
			"negative limit":   {cutoff, past, "sesn_disc_a", -1},
		} {
			_, err := w.DB.ExecContext(ctx, `SELECT * FROM public.tetral_cleanup_due_sessions($1, $2, $3, $4)`, args...)
			if code := sqlState(err); code != "22023" {
				t.Fatalf("%s: SQLSTATE %q (%v); want invalid_parameter_value", name, code, err)
			}
		}
	})

	t.Run("DirectReadsAndSpoofedFlag", func(t *testing.T) {
		tx, err := w.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		visible := func() int {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM session_runtime_status`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			return count
		}
		if count := visible(); count != 0 {
			t.Fatalf("direct global SELECT saw %d rows", count)
		}
		if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.cleanup_discovery', 'true', true)`); err != nil {
			t.Fatal(err)
		}
		if count := visible(); count != 0 {
			t.Fatalf("spoofed discovery flag saw %d rows", count)
		}
		var returned int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM public.tetral_cleanup_due_sessions($1, NULL, NULL, 100)`, cutoff).Scan(&returned); err != nil || returned != 3 {
			t.Fatalf("function rows = %d/%v; want 3", returned, err)
		}
		if count := visible(); count != 0 {
			t.Fatalf("flag left by the function let Cleanup see %d rows", count)
		}
		if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.workspace_id', 'default', true)`); err != nil {
			t.Fatal(err)
		}
		if count := visible(); count != 1 {
			t.Fatalf("Workspace-scoped SELECT with the flag saw %d rows; want only its own", count)
		}
	})

	t.Run("OtherWorkloadsCannotExecute", func(t *testing.T) {
		contract, err := database.LoadRoleContract()
		if err != nil {
			t.Fatal(err)
		}
		for _, workload := range contract.WorkloadNames() {
			if workload == "cleanup" {
				continue
			}
			_, err := w.OpenWorkload(t, workload, nil).ExecContext(ctx, `SELECT * FROM public.tetral_cleanup_due_sessions($1, NULL, NULL, 100)`, cutoff)
			if code := sqlState(err); code != "42501" {
				t.Fatalf("%s executed discovery: SQLSTATE %q (%v)", workload, code, err)
			}
		}
	})

	t.Run("SearchPathShadowing", func(t *testing.T) {
		// A not-yet-due row would appear if the shadow operator replaced the
		// due comparison; the shadow view would replace the table. PUBLIC
		// usage makes the shadow schema visible to the definer as well.
		seedDueSessions(t, admin, "ws_cleanup_other", cutoff.Add(time.Hour), "sesn_disc_future")
		//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
		if _, err := admin.Exec(`CREATE SCHEMA cleanup_shadow;
			CREATE VIEW cleanup_shadow.session_runtime_status AS
			  SELECT 'ws_shadow'::text workspace_id, 'sesn_shadow'::text session_id, 'idle'::text status,
			         NULL::text cleanup_job_id, 'bind_shadow'::text binding_id, '2000-01-01T00:00:00Z'::timestamptz cleanup_after;
			CREATE FUNCTION cleanup_shadow.always_due(timestamptz, timestamptz) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT true';
			CREATE OPERATOR cleanup_shadow.<= (LEFTARG = timestamptz, RIGHTARG = timestamptz, FUNCTION = cleanup_shadow.always_due);
			GRANT USAGE ON SCHEMA cleanup_shadow TO PUBLIC;
			GRANT SELECT ON cleanup_shadow.session_runtime_status TO ` + pgx.Identifier{cleanupRole}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		tx, err := w.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = cleanup_shadow, public, pg_catalog`); err != nil {
			t.Fatal(err)
		}
		rows, err := tx.QueryContext(ctx, `SELECT session_id FROM public.tetral_cleanup_due_sessions($1, NULL, NULL, 100)`, cutoff)
		if err != nil {
			t.Fatal(err)
		}
		var sessions []string
		for rows.Next() {
			var sessionID string
			if err := rows.Scan(&sessionID); err != nil {
				t.Fatal(err)
			}
			sessions = append(sessions, sessionID)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if strings.Join(sessions, " ") != "sesn_disc_a sesn_disc_b sesn_disc_c" {
			t.Fatalf("shadowed search path changed discovery: %v", sessions)
		}
	})

	t.Run("CursorMutationBoundary", func(t *testing.T) {
		for _, statement := range []string{
			`INSERT INTO cleanup_schedule_cursor (singleton) VALUES (true)`,
			`DELETE FROM cleanup_schedule_cursor`,
		} {
			if _, err := w.DB.ExecContext(ctx, statement); sqlState(err) != "42501" {
				t.Fatalf("Cleanup %q: %v; want insufficient_privilege", statement, err)
			}
		}
		if _, err := w.DB.ExecContext(ctx, `UPDATE cleanup_schedule_cursor SET after_session_id = 'sesn_partial'`); sqlState(err) != "23514" {
			t.Fatalf("partial cursor position: %v; want check_violation", err)
		}
		contract, err := database.LoadRoleContract()
		if err != nil {
			t.Fatal(err)
		}
		for _, workload := range contract.WorkloadNames() {
			if workload == "cleanup" {
				continue
			}
			db := w.OpenWorkload(t, workload, nil)
			for _, statement := range []string{
				`SELECT owner_generation FROM cleanup_schedule_cursor`,
				`UPDATE cleanup_schedule_cursor SET owner_generation = owner_generation + 1`,
			} {
				if _, err := db.ExecContext(ctx, statement); sqlState(err) != "42501" {
					t.Fatalf("%s %q: %v; want insufficient_privilege", workload, statement, err)
				}
			}
		}
	})

	t.Run("FunctionOwnershipAndCatalogPosture", func(t *testing.T) {
		var ownerMatches, definer, fixedPath, publicExecute bool
		if err := admin.QueryRow(`SELECT pg_get_userbyid(p.proowner) = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.session_runtime_status'::regclass),
			       p.prosecdef, COALESCE('search_path=pg_catalog' = ANY(p.proconfig), false),
			       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')
			  FROM pg_proc p WHERE p.oid = 'public.tetral_cleanup_due_sessions(timestamptz,timestamptz,text,integer)'::regprocedure`).Scan(&ownerMatches, &definer, &fixedPath, &publicExecute); err != nil {
			t.Fatal(err)
		}
		if !ownerMatches || !definer || !fixedPath || publicExecute {
			t.Fatalf("function owner=table owner %t definer %t fixed path %t public %t", ownerMatches, definer, fixedPath, publicExecute)
		}
		// A function owned by any role other than the table's owner fails the
		// owner predicate and discovers nothing.
		//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
		if _, err := admin.Exec(`ALTER FUNCTION public.tetral_cleanup_due_sessions(timestamptz,timestamptz,text,integer) OWNER TO ` + pgx.Identifier{cleanupRole}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		if got := discoverAs(t, w.DB, cutoff, nil, nil, 100); got != "" {
			t.Fatalf("wrongly owned function discovered %q", got)
		}
	})
}

// Under the real Cleanup role, with sequential scans and sorts disabled and a
// generic plan, both discovery statements read the global due index in order
// under their Limit; the continuation is an index condition, not a filter.
func TestCleanupDiscoveryUsesGlobalDueIndex(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "cleanup")
	past := dbTime(t, admin, "clock_timestamp() - interval '1 hour'")
	seedDueSessions(t, admin, "default", past, "sesn_plan_a", "sesn_plan_b", "sesn_plan_c")
	seedDueSessions(t, admin, "ws_cleanup_other", past, "sesn_plan_d", "sesn_plan_e")
	if _, err := admin.Exec(`UPDATE session_runtime_status SET status = 'running' WHERE session_id = 'sesn_plan_e'; ANALYZE session_runtime_status`); err != nil {
		t.Fatal(err)
	}
	var cleanupRole string
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&cleanupRole); err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	var notices noticeLog
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) { notices.add(notice.Message) }
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
	for _, statement := range []string{
		`SET LOCAL auto_explain.log_min_duration = 0`,
		`SET LOCAL auto_explain.log_nested_statements = on`,
		`SET LOCAL auto_explain.log_format = 'json'`,
		`SET LOCAL auto_explain.log_level = 'notice'`,
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_sort = off`,
		`SET LOCAL plan_cache_mode = force_generic_plan`,
		`SET LOCAL ROLE ` + pgx.Identifier{cleanupRole}.Sanitize(),
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	cutoff := dbTime(t, admin, "clock_timestamp()")
	for _, page := range []struct {
		name    string
		afterAt any
		afterID any
		rows    int
	}{
		{"first", nil, nil, 4},
		{"continuation", past, "sesn_plan_b", 2},
	} {
		notices.reset()
		var rows int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.tetral_cleanup_due_sessions($1, $2, $3, 100)`, cutoff, page.afterAt, page.afterID).Scan(&rows); err != nil || rows != page.rows {
			t.Fatalf("%s page rows = %d/%v; want %d", page.name, rows, err, page.rows)
		}
		plan := notices.discoveryPlan(t, page.afterID != nil)
		evidence := discoveryPlanEvidence{}
		walkPlan(plan, nil, &evidence)
		if !evidence.indexUnderLimit || evidence.sorts != 0 || evidence.seqScans != 0 {
			t.Fatalf("%s page plan = %+v\n%s", page.name, evidence, notices.raw())
		}
		if page.afterID != nil && !strings.Contains(evidence.indexCond, "session_id") {
			t.Fatalf("continuation is not an index condition: %q\n%s", evidence.indexCond, notices.raw())
		}
	}
}

type discoveryPlanEvidence struct {
	indexUnderLimit bool
	indexCond       string
	sorts           int
	seqScans        int
}

func walkPlan(node map[string]any, parent map[string]any, evidence *discoveryPlanEvidence) {
	nodeType, _ := node["Node Type"].(string)
	relation, _ := node["Relation Name"].(string)
	switch {
	case nodeType == "Sort" || nodeType == "Incremental Sort":
		evidence.sorts++
	case nodeType == "Seq Scan" && relation == "session_runtime_status":
		evidence.seqScans++
	case (nodeType == "Index Scan" || nodeType == "Index Only Scan") && node["Index Name"] == "idx_session_runtime_status_cleanup_global_due":
		if parentType, _ := parent["Node Type"].(string); parentType == "Limit" {
			evidence.indexUnderLimit = true
			evidence.indexCond, _ = node["Index Cond"].(string)
		}
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkPlan(childNode, node, evidence)
		}
	}
}

type noticeLog struct {
	mu       sync.Mutex
	messages []string
}

func (n *noticeLog) add(message string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = append(n.messages, message)
}

func (n *noticeLog) reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = nil
}

func (n *noticeLog) raw() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Join(n.messages, "\n---\n")
}

// discoveryPlan returns the nested plan auto_explain logged for the function's
// first-page or continuation read of session_runtime_status.
func (n *noticeLog) discoveryPlan(t *testing.T, continuation bool) map[string]any {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, message := range n.messages {
		start := strings.Index(message, "{")
		if start < 0 {
			continue
		}
		var document struct {
			QueryText string         `json:"Query Text"`
			Plan      map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(message[start:]), &document); err != nil {
			continue
		}
		if strings.Contains(document.QueryText, "public.session_runtime_status") && strings.Contains(document.QueryText, "($2, $3)") == continuation {
			return document.Plan
		}
	}
	t.Fatalf("no nested discovery plan in notices:\n%s", strings.Join(n.messages, "\n---\n"))
	return nil
}

func discoverAs(t *testing.T, db *sql.DB, cutoff time.Time, afterAt, afterID any, limit int) string {
	t.Helper()
	rows, err := db.Query(`SELECT workspace_id, session_id FROM public.tetral_cleanup_due_sessions($1, $2, $3, $4)`, cutoff, afterAt, afterID, limit)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var page []string
	for rows.Next() {
		var workspaceID, sessionID string
		if err := rows.Scan(&workspaceID, &sessionID); err != nil {
			t.Fatal(err)
		}
		page = append(page, workspaceID+"/"+sessionID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(page, " ")
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
