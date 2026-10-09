package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

// The two binding discovery functions are the Runner's only cross-Workspace
// reads. Under the real Job Runner role they page raw binding membership in key
// order with the exact active predicate; its own direct reads, a spoofed purpose
// flag, a shadowed search path or a function owned by anyone but the table owner
// reveal nothing, and other workloads cannot execute them.
func TestJobRunnerBindingDiscoveryFunctionBoundary(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	var runnerRole string
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&runnerRole); err != nil {
		t.Fatal(err)
	}
	if upper, ok := bindingUpperAs(t, w.DB); ok {
		t.Fatalf("upper of an empty binding table = %q; want none", upper)
	}
	if got := bindingPageAs(t, w.DB, nil, nil, "default", "sesn_zz", 128); got != "" {
		t.Fatalf("page of an empty binding table = %q; want empty", got)
	}

	seedDiscoveryBinding(t, admin, "default", "sesn_disc_a", "running", 0)
	seedDiscoveryBinding(t, admin, "default", "sesn_disc_b", "idle", 0)
	seedDiscoveryBinding(t, admin, "default", "sesn_disc_c", "idle", 0)
	if _, err := admin.Exec(`UPDATE sessions SET status='rescheduling' WHERE id='sesn_disc_c'`); err != nil {
		t.Fatal(err)
	}
	seedDiscoveryBinding(t, admin, "default", "sesn_disc_d", "idle", 0)
	seedDiscoveryAcceptedInbox(t, admin, "sesn_disc_d", "")
	seedDiscoveryBinding(t, admin, "default", "sesn_disc_e", "idle", 0)
	seedDiscoveryAcceptedInbox(t, admin, "sesn_disc_e", "pod-uid-other")
	seedDiscoveryBinding(t, admin, "default", "sesn_disc_f", "running", 1)
	seedDiscoveryBinding(t, admin, "ws_jr_disc_other", "sesn_disc_g", "running", 0)
	seedDiscoveryBinding(t, admin, "ws_jr_disc_other", "sesn_disc_h", "", 0)

	t.Run("RealRunnerRolePages", func(t *testing.T) {
		upper, ok := bindingUpperAs(t, w.DB)
		if !ok || upper != "ws_jr_disc_other/sesn_disc_h" {
			t.Fatalf("upper = %q/%t; want the last binding key", upper, ok)
		}
		want := "default/sesn_disc_a:active default/sesn_disc_b:inactive default/sesn_disc_c:active default/sesn_disc_d:active " +
			"default/sesn_disc_e:inactive default/sesn_disc_f:inactive ws_jr_disc_other/sesn_disc_g:active ws_jr_disc_other/sesn_disc_h:inactive"
		if got := bindingPageAs(t, w.DB, nil, nil, "ws_jr_disc_other", "sesn_disc_h", 128); got != want {
			t.Fatalf("whole page = %q; want %q", got, want)
		}
		if got := bindingPageAs(t, w.DB, nil, nil, "default", "sesn_disc_c", 128); got != "default/sesn_disc_a:active default/sesn_disc_b:inactive default/sesn_disc_c:active" {
			t.Fatalf("page bounded by upper = %q", got)
		}
		keys := strings.Fields(want)
		var afterWorkspace, afterSession any
		for index, key := range append(keys, "") {
			got := bindingPageAs(t, w.DB, afterWorkspace, afterSession, "ws_jr_disc_other", "sesn_disc_h", 1)
			if got != key {
				t.Fatalf("page %d after %v/%v = %q; want %q", index, afterWorkspace, afterSession, got, key)
			}
			if key != "" {
				identity := strings.SplitN(strings.SplitN(key, ":", 2)[0], "/", 2)
				afterWorkspace, afterSession = identity[0], identity[1]
			}
		}
	})

	t.Run("InvalidArguments", func(t *testing.T) {
		for name, args := range map[string][]any{
			"workspace without session": {"default", nil, "default", "sesn_zz", 1},
			"session without workspace": {nil, "sesn_disc_a", "default", "sesn_zz", 1},
			"empty after workspace":     {"", "sesn_disc_a", "default", "sesn_zz", 1},
			"missing upper":             {nil, nil, nil, nil, 1},
			"empty upper session":       {nil, nil, "default", "", 1},
			"zero limit":                {nil, nil, "default", "sesn_zz", 0},
			"limit above cap":           {nil, nil, "default", "sesn_zz", 129},
			"null limit":                {nil, nil, "default", "sesn_zz", nil},
		} {
			_, err := w.DB.ExecContext(ctx, `SELECT * FROM public.tetral_job_runner_binding_page($1, $2, $3, $4, $5)`, args...)
			if code := discoverySQLState(err); code != "22023" {
				t.Fatalf("%s: SQLSTATE %q (%v); want invalid_parameter_value", name, code, err)
			}
		}
	})

	t.Run("ProcessAbsenceKeepsTheRawRow", func(t *testing.T) {
		var constraint string
		if err := admin.QueryRow(`SELECT conname FROM pg_constraint
			WHERE conrelid='session_runtime_bindings'::regclass AND confrelid='runtime_processes'::regclass`).Scan(&constraint); err != nil {
			t.Fatal(err)
		}
		if _, err := admin.Exec(`ALTER TABLE session_runtime_bindings DROP CONSTRAINT ` + pgx.Identifier{constraint}.Sanitize() + `;
			UPDATE session_runtime_bindings SET runtime_process_id='process_never_registered' WHERE session_id='sesn_disc_a'`); err != nil {
			t.Fatal(err)
		}
		var registered bool
		if err := w.DB.QueryRow(`SELECT process_registered FROM public.tetral_job_runner_binding_page(NULL, NULL, 'default', 'sesn_disc_a', 1)`).Scan(&registered); err != nil || registered {
			t.Fatalf("unregistered process row = %t/%v; want returned with process_registered=false", registered, err)
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
			if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM session_runtime_bindings) + (SELECT count(*) FROM session_runtime_status)
				+ (SELECT count(*) FROM sessions) + (SELECT count(*) FROM session_runtime_inbox)`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			return count
		}
		if count := visible(); count != 0 {
			t.Fatalf("direct global SELECT saw %d rows", count)
		}
		if _, err := tx.ExecContext(ctx, `SELECT set_config('tetral.runner_discovery', 'true', true)`); err != nil {
			t.Fatal(err)
		}
		if count := visible(); count != 0 {
			t.Fatalf("spoofed discovery flag saw %d rows", count)
		}
		var returned int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM public.tetral_job_runner_binding_page(NULL, NULL, 'ws_jr_disc_other', 'sesn_disc_h', 128)`).Scan(&returned); err != nil || returned != 8 {
			t.Fatalf("function rows = %d/%v; want 8", returned, err)
		}
		if count := visible(); count != 0 {
			t.Fatalf("flag left by the function let the Runner see %d rows", count)
		}
	})

	t.Run("OtherWorkloadsCannotExecute", func(t *testing.T) {
		contract, err := database.LoadRoleContract()
		if err != nil {
			t.Fatal(err)
		}
		for _, workload := range contract.WorkloadNames() {
			if workload == "job_runner" {
				continue
			}
			db := w.OpenWorkload(t, workload, nil)
			for _, statement := range []string{
				`SELECT * FROM public.tetral_job_runner_binding_upper()`,
				`SELECT * FROM public.tetral_job_runner_binding_page(NULL, NULL, 'default', 'sesn_zz', 1)`,
			} {
				if _, err := db.ExecContext(ctx, statement); discoverySQLState(err) != "42501" {
					t.Fatalf("%s executed %q: %v", workload, statement, err)
				}
			}
		}
	})

	t.Run("SearchPathShadowing", func(t *testing.T) {
		//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
		if _, err := admin.Exec(`CREATE SCHEMA runner_shadow;
			CREATE VIEW runner_shadow.session_runtime_bindings AS
			  SELECT 'ws_shadow'::text workspace_id, 'sesn_shadow'::text session_id;
			GRANT USAGE ON SCHEMA runner_shadow TO PUBLIC;
			GRANT SELECT ON runner_shadow.session_runtime_bindings TO ` + pgx.Identifier{runnerRole}.Sanitize()); err != nil {
			t.Fatal(err)
		}
		tx, err := w.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `SET LOCAL search_path = runner_shadow, public, pg_catalog`); err != nil {
			t.Fatal(err)
		}
		var upper string
		if err := tx.QueryRowContext(ctx, `SELECT workspace_id || '/' || session_id FROM public.tetral_job_runner_binding_upper()`).Scan(&upper); err != nil || upper != "ws_jr_disc_other/sesn_disc_h" {
			t.Fatalf("shadowed search path upper = %q/%v", upper, err)
		}
	})

	t.Run("FunctionOwnershipAndCatalogPosture", func(t *testing.T) {
		for _, function := range []string{"public.tetral_job_runner_binding_upper()", "public.tetral_job_runner_binding_page(text,text,text,text,integer)"} {
			var ownerMatches, definer, fixedPath, publicExecute bool
			if err := admin.QueryRow(`SELECT pg_get_userbyid(p.proowner) = (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = 'public.session_runtime_bindings'::regclass),
				       p.prosecdef, COALESCE('search_path=pg_catalog' = ANY(p.proconfig), false),
				       EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')
				  FROM pg_proc p WHERE p.oid = $1::regprocedure`, function).Scan(&ownerMatches, &definer, &fixedPath, &publicExecute); err != nil {
				t.Fatal(err)
			}
			if !ownerMatches || !definer || !fixedPath || publicExecute {
				t.Fatalf("%s owner=table owner %t definer %t fixed path %t public %t", function, ownerMatches, definer, fixedPath, publicExecute)
			}
			// Owned by any role other than the table owner, the function fails
			// the owner predicate and discovers nothing.
			//nolint:gosec // G202: only the quoted installer-owned role identifier is concatenated.
			if _, err := admin.Exec(`ALTER FUNCTION ` + function + ` OWNER TO ` + pgx.Identifier{runnerRole}.Sanitize()); err != nil {
				t.Fatal(err)
			}
		}
		if upper, ok := bindingUpperAs(t, w.DB); ok {
			t.Fatalf("wrongly owned upper discovered %q", upper)
		}
		if got := bindingPageAs(t, w.DB, nil, nil, "ws_jr_disc_other", "sesn_disc_h", 128); got != "" {
			t.Fatalf("wrongly owned page discovered %q", got)
		}
	})
}

// Under the real Job Runner role, with sequential scans and sorts disabled and
// a generic plan, the binding page applies its LIMIT to a primary-key range
// scan inside the materialized raw CTE, and each joined fact is an index probe
// per raw row, so eligibility never widens the rows read.
func TestJobRunnerBindingPageLimitsRawRowsBeforeEligibility(t *testing.T) {
	ctx := context.Background()
	admin := storagetest.NewPostgreSQLAdminDB(t)
	w := storagetest.OpenWorkloadDB(t, admin, "job_runner")
	for index := 0; index < 40; index++ {
		status := "idle"
		if index%3 == 0 {
			status = "running"
		}
		seedDiscoveryBinding(t, admin, "default", fmt.Sprintf("sesn_plan_%02d", index), status, 0)
	}
	if _, err := admin.Exec(`ANALYZE session_runtime_bindings; ANALYZE session_runtime_status; ANALYZE sessions; ANALYZE session_runtime_inbox; ANALYZE runtime_processes`); err != nil {
		t.Fatal(err)
	}
	var runnerRole string
	if err := w.DB.QueryRow(`SELECT current_user`).Scan(&runnerRole); err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(storagetest.AdminDatabaseURL(t, admin))
	if err != nil {
		t.Fatal(err)
	}
	var notices discoveryNoticeLog
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
		`SET LOCAL auto_explain.log_analyze = on`,
		`SET LOCAL auto_explain.log_nested_statements = on`,
		`SET LOCAL auto_explain.log_format = 'json'`,
		`SET LOCAL auto_explain.log_level = 'notice'`,
		`SET LOCAL enable_seqscan = off`,
		`SET LOCAL enable_sort = off`,
		`SET LOCAL plan_cache_mode = force_generic_plan`,
		`SET LOCAL ROLE ` + pgx.Identifier{runnerRole}.Sanitize(),
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, page := range []struct {
		name                         string
		afterWorkspace, afterSession any
		rows                         int
	}{
		{"first", nil, nil, 8},
		{"continuation", "default", "sesn_plan_09", 8},
	} {
		notices.reset()
		var rows int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.tetral_job_runner_binding_page($1, $2, 'default', 'sesn_plan_39', 8)`, page.afterWorkspace, page.afterSession).Scan(&rows); err != nil || rows != page.rows {
			t.Fatalf("%s page rows = %d/%v; want %d", page.name, rows, err, page.rows)
		}
		plan := notices.pagePlan(t, page.afterSession != nil)
		evidence := bindingPagePlanEvidence{probes: map[string]float64{}}
		walkBindingPagePlan(plan, nil, &evidence)
		if !evidence.rawLimitOnPrimaryKey || evidence.rawRowsRead > 8 || evidence.sorts != 0 || evidence.seqScans != 0 {
			t.Fatalf("%s page plan = %+v\n%s", page.name, evidence, notices.raw())
		}
		if page.afterSession != nil && strings.Count(evidence.rawIndexCond, "ROW(workspace_id, session_id)") != 2 {
			t.Fatalf("%s page bounds are not index conditions: %q\n%s", page.name, evidence.rawIndexCond, notices.raw())
		}
		for _, relation := range []string{"session_runtime_status", "sessions", "runtime_processes"} {
			if loops, ok := evidence.probes[relation]; !ok || loops > 8 {
				t.Fatalf("%s page joins %s with %v index probes; want one per raw row\n%s", page.name, relation, loops, notices.raw())
			}
		}
	}
}

type bindingPagePlanEvidence struct {
	rawLimitOnPrimaryKey bool
	rawRowsRead          float64
	rawIndexCond         string
	probes               map[string]float64
	sorts                int
	seqScans             int
}

func walkBindingPagePlan(node map[string]any, parent map[string]any, evidence *bindingPagePlanEvidence) {
	nodeType, _ := node["Node Type"].(string)
	relation, _ := node["Relation Name"].(string)
	switch {
	case nodeType == "Sort" || nodeType == "Incremental Sort":
		evidence.sorts++
	case nodeType == "Seq Scan":
		evidence.seqScans++
	case strings.HasPrefix(nodeType, "Index") && relation == "session_runtime_bindings":
		if parentType, _ := parent["Node Type"].(string); parentType == "Limit" && node["Index Name"] == "session_runtime_bindings_pkey" {
			evidence.rawLimitOnPrimaryKey = true
			evidence.rawIndexCond, _ = node["Index Cond"].(string)
			evidence.rawRowsRead, _ = node["Actual Rows"].(float64)
		}
	case strings.HasPrefix(nodeType, "Index") && relation != "":
		loops, _ := node["Actual Loops"].(float64)
		evidence.probes[relation] = max(evidence.probes[relation], loops)
	}
	children, _ := node["Plans"].([]any)
	for _, child := range children {
		if childNode, ok := child.(map[string]any); ok {
			walkBindingPagePlan(childNode, node, evidence)
		}
	}
}

type discoveryNoticeLog struct {
	mu       sync.Mutex
	messages []string
}

func (n *discoveryNoticeLog) add(message string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = append(n.messages, message)
}

func (n *discoveryNoticeLog) reset() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.messages = nil
}

func (n *discoveryNoticeLog) raw() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Join(n.messages, "\n---\n")
}

// pagePlan returns the nested plan auto_explain logged for the function's first
// or continuation page statement.
func (n *discoveryNoticeLog) pagePlan(t *testing.T, continuation bool) map[string]any {
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
		if strings.Contains(document.QueryText, "WITH raw AS MATERIALIZED") && strings.Contains(document.QueryText, "($1, $2)") == continuation {
			return document.Plan
		}
	}
	t.Fatalf("no nested binding page plan in notices:\n%s", strings.Join(n.messages, "\n---\n"))
	return nil
}

// seedDiscoveryBinding creates a Session with a binding and, unless
// runtimeStatus is empty, a runtime status row; generationOffset makes the
// status point at another binding generation.
func seedDiscoveryBinding(t *testing.T, admin *sql.DB, workspaceID string, sessionID string, runtimeStatus string, generationOffset int64) {
	t.Helper()
	sessionfixture.SeedBridgeAPISession(t, admin, workspaceID, sessionID, "thrd_"+sessionID)
	var generation int64
	if err := admin.QueryRow(`SELECT nextval('session_runtime_binding_generation_seq')`).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_"+sessionID, generation, "pod-uid-"+sessionID)
	if runtimeStatus == "" {
		return
	}
	if _, err := admin.Exec(`INSERT INTO session_runtime_status (workspace_id, session_id, status, binding_id, binding_generation, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, clock_timestamp(), clock_timestamp())`,
		workspaceID, sessionID, runtimeStatus, "bind_"+sessionID, generation+generationOffset); err != nil {
		t.Fatalf("seed discovery runtime status: %v", err)
	}
}

// seedDiscoveryAcceptedInbox accepts one input for the Session's binding;
// podUID overrides the target Pod.
func seedDiscoveryAcceptedInbox(t *testing.T, admin *sql.DB, sessionID string, podUID string) {
	t.Helper()
	if podUID == "" {
		podUID = "pod-uid-" + sessionID
	}
	if _, err := admin.Exec(`INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json,
		sequence_from, sequence_to, status, binding_id, binding_generation, target_pod_uid, created_at, updated_at
	) SELECT b.workspace_id, b.session_id, 'thrd_' || b.session_id, 'rin_' || b.session_id, 'messages', '[]',
	         1, 1, 'accepted', b.binding_id, b.binding_generation, $2, clock_timestamp(), clock_timestamp()
	    FROM session_runtime_bindings b WHERE b.session_id = $1`, sessionID, podUID); err != nil {
		t.Fatalf("seed accepted Inbox: %v", err)
	}
}

func bindingUpperAs(t *testing.T, db *sql.DB) (string, bool) {
	t.Helper()
	var workspaceID, sessionID string
	err := db.QueryRow(`SELECT workspace_id, session_id FROM public.tetral_job_runner_binding_upper()`).Scan(&workspaceID, &sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("binding upper: %v", err)
	}
	return workspaceID + "/" + sessionID, true
}

func bindingPageAs(t *testing.T, db *sql.DB, afterWorkspace, afterSession any, upperWorkspace, upperSession string, limit int) string {
	t.Helper()
	rows, err := db.Query(`SELECT workspace_id, session_id, active, process_registered, is_current, phase
		FROM public.tetral_job_runner_binding_page($1, $2, $3, $4, $5)`, afterWorkspace, afterSession, upperWorkspace, upperSession, limit)
	if err != nil {
		t.Fatalf("binding page: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var page []string
	for rows.Next() {
		var workspaceID, sessionID, phase string
		var active, registered, current bool
		if err := rows.Scan(&workspaceID, &sessionID, &active, &registered, &current, &phase); err != nil {
			t.Fatal(err)
		}
		if !registered || !current || phase != "accepting" {
			t.Fatalf("%s/%s process facts = %t/%t/%q; want the registered accepting process", workspaceID, sessionID, registered, current, phase)
		}
		state := "inactive"
		if active {
			state = "active"
		}
		page = append(page, workspaceID+"/"+sessionID+":"+state)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(page, " ")
}

func discoverySQLState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
