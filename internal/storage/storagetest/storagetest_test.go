package storagetest

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const sensitiveSentinel = "do-not-leak-this-helper"

func TestNewPostgreSQLDBProvidesIsolatedSchemas(t *testing.T) {
	dbA := NewPostgreSQLDB(t)
	dbB := NewPostgreSQLDB(t)

	var schemaA, schemaB string
	if err := dbA.QueryRow(`SELECT current_schema()`).Scan(&schemaA); err != nil {
		t.Fatalf("dbA current_schema(): %v", err)
	}
	if err := dbB.QueryRow(`SELECT current_schema()`).Scan(&schemaB); err != nil {
		t.Fatalf("dbB current_schema(): %v", err)
	}
	if schemaA == schemaB {
		t.Fatalf("expected distinct schema names per helper call; got %q twice", schemaA)
	}
	for _, name := range []string{schemaA, schemaB} {
		if !strings.HasPrefix(name, "tetral_test_") {
			t.Errorf("schema %q must start with tetral_test_", name)
		}
	}
}

func TestNewPostgreSQLDBHidesRowsAcrossSchemas(t *testing.T) {
	// Schema isolation is independent of RLS. Drive the inserts and
	// reads through admin connections to bypass FORCE RLS — the
	// assertion here is that two helper schemas truly are different
	// schemas, not that RLS works.
	_, adminA := NewPostgreSQLDBWithAdmin(t)
	_, adminB := NewPostgreSQLDBWithAdmin(t)

	if _, err := adminA.Exec(
		`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at) VALUES ('default', 'vlt_isolation', 'A', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("insert into A: %v", err)
	}
	var n int
	if err := adminB.QueryRow(`SELECT count(*) FROM vaults WHERE id = 'vlt_isolation'`).Scan(&n); err != nil {
		t.Fatalf("query B: %v", err)
	}
	if n != 0 {
		t.Errorf("vlt_isolation visible in schema B; expected isolation, got %d row(s)", n)
	}
}

func TestNewPostgreSQLDBInitializesSchemaIntoIsolatedSchemaNotPublic(t *testing.T) {
	db := NewPostgreSQLDB(t)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	if schema == "public" {
		t.Fatalf("helper schema must not be public; got %q", schema)
	}
	// All schema-owned tables must live in the helper schema, none in public
	// (the helper must not pollute the shared default schema).
	var helperCount int
	if err := db.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type='BASE TABLE'`,
		schema).Scan(&helperCount); err != nil {
		t.Fatal(err)
	}
	expectedCount := expectedCurrentBaseTableCount(t)
	if helperCount != expectedCount {
		t.Errorf("helper schema %q has %d base tables; want %d", schema, helperCount, expectedCount)
	}
}

func TestNewPostgreSQLDBInstallsDefaultWorkspaceOnlyAsTestFixture(t *testing.T) {
	db := NewPostgreSQLDB(t)
	var name string
	if err := db.QueryRow(`SELECT name FROM workspaces WHERE id = $1`, string(workspace.DefaultID)).Scan(&name); err != nil {
		t.Fatalf("read test default workspace fixture: %v", err)
	}
	if name != "Default Test Fixture" {
		t.Fatalf("default workspace fixture name = %q; want test-only fixture", name)
	}
}

func expectedCurrentBaseTableCount(t *testing.T) int {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	storageDir := filepath.Join(filepath.Dir(thisFile), "..")
	var count int
	for _, name := range []string{"postgresql_schema.go", "postgresql_migrator.go"} {
		body, err := os.ReadFile(filepath.Join(storageDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		count += strings.Count(string(body), "CREATE TABLE IF NOT EXISTS ")
		count += strings.Count(string(body), "CREATE TABLE tetral_schema_migrations (")
	}
	return count
}

func TestNewPostgreSQLDBLeavesPublicFoundationTablesUnchanged(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := openBasePostgreSQLDBForInspection(ctx, t)
	defer func() { _ = base.Close() }()

	before := publicFoundationTables(t, base)
	t.Run("setup", func(inner *testing.T) {
		db := NewPostgreSQLDB(inner)
		afterSetup := publicFoundationTables(inner, db)
		if !equalOrderedStrings(afterSetup, before) {
			inner.Fatalf("public foundation tables changed after helper setup: before=%v after_setup=%v", before, afterSetup)
		}
	})
	afterCleanup := publicFoundationTables(t, base)
	if !equalOrderedStrings(afterCleanup, before) {
		t.Fatalf("public foundation tables changed after helper cleanup: before=%v after_cleanup=%v", before, afterCleanup)
	}
}

func TestNewPostgreSQLDBDropsSchemaAfterCleanup(t *testing.T) {
	// Capture the schema name from a sub-test, let the sub-test's
	// t.Cleanup run, then assert in the parent that the schema is gone.
	var capturedSchema string
	t.Run("inner", func(inner *testing.T) {
		db := NewPostgreSQLDB(inner)
		if err := db.QueryRow(`SELECT current_schema()`).Scan(&capturedSchema); err != nil {
			inner.Fatalf("capture schema: %v", err)
		}
	})
	if capturedSchema == "" {
		t.Fatal("inner test failed to capture schema name")
	}
	// Open a fresh helper connection, then verify the prior helper
	// schema no longer exists. This keeps TETRAL_TEST_DATABASE_URL
	// parsing inside NewPostgreSQLDB's sanitized boundary.
	db := NewPostgreSQLDB(t)
	var exists bool
	if err := db.QueryRowContext(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)`, capturedSchema).Scan(&exists); err != nil {
		t.Fatalf("check schema existence: %v", err)
	}
	if exists {
		t.Errorf("schema %q must be dropped during t.Cleanup; still present", capturedSchema)
	}
}

func TestNewPostgreSQLDBSafeUnderParallel(t *testing.T) {
	// Each subtest inserts a row with the SAME id through its admin
	// connection. If isolation failed the second insert would
	// PK-collide. The shared sentinel id makes failure deterministic
	// rather than statistical.
	const sharedID = "vlt_parallel_isolation"
	for i := 0; i < 4; i++ {
		i := i
		t.Run("parallel_"+itoa(i), func(p *testing.T) {
			p.Parallel()
			_, admin := NewPostgreSQLDBWithAdmin(p)
			if _, err := admin.Exec(
				`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at) VALUES ('default', $1, $2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
				sharedID, "name-"+itoa(i)); err != nil {
				p.Fatalf("insert in parallel test %d: %v", i, err)
			}
			var n int
			if err := admin.QueryRow(`SELECT count(*) FROM vaults WHERE id = $1`, sharedID).Scan(&n); err != nil {
				p.Fatal(err)
			}
			if n != 1 {
				p.Errorf("parallel test %d sees %d row(s); want 1", i, n)
			}
		})
	}
}

// TestNewPostgreSQLDBReturnsNonSuperuserRuntimeRole pins the runtime-role split:
// the *sql.DB returned by NewPostgreSQLDB must
// authenticate as a non-superuser, NOBYPASSRLS role so RLS proofs
// running against it actually exercise the policies.
func TestNewPostgreSQLDBReturnsNonSuperuserRuntimeRole(t *testing.T) {
	db := NewPostgreSQLDB(t)
	var role string
	var isSuperuser, bypassRLS bool
	if err := db.QueryRow(
		`SELECT current_user::text, rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user`,
	).Scan(&role, &isSuperuser, &bypassRLS); err != nil {
		t.Fatalf("query current role: %v", err)
	}
	if isSuperuser {
		t.Errorf("runtime role %q is superuser; helper must hand out a non-superuser role", role)
	}
	if bypassRLS {
		t.Errorf("runtime role %q has BYPASSRLS; helper must hand out a NOBYPASSRLS role", role)
	}
}

// TestNewPostgreSQLDBWithAdminPinsBothToSameSchema pins that the
// admin and runtime DBs returned by NewPostgreSQLDBWithAdmin are
// pinned to the same per-test schema; otherwise RLS proofs that
// seed cross-workspace data through admin would not be visible to
// the runtime read path.
func TestNewPostgreSQLDBWithAdminPinsBothToSameSchema(t *testing.T) {
	runtime, admin := NewPostgreSQLDBWithAdmin(t)
	var runtimeSchema, adminSchema string
	if err := runtime.QueryRow(`SELECT current_schema()`).Scan(&runtimeSchema); err != nil {
		t.Fatalf("runtime current_schema: %v", err)
	}
	if err := admin.QueryRow(`SELECT current_schema()`).Scan(&adminSchema); err != nil {
		t.Fatalf("admin current_schema: %v", err)
	}
	if runtimeSchema != adminSchema {
		t.Errorf("runtime schema %q differs from admin schema %q", runtimeSchema, adminSchema)
	}
}

func TestOpenIsolatedPostgreSQLDBReportsMissingTestDatabaseURL(t *testing.T) {
	t.Setenv("TETRAL_TEST_DATABASE_URL", "")
	ctx := context.Background()
	_, _, err := openIsolatedPostgreSQLDB(ctx)
	if err == nil {
		t.Fatal("expected missing-config error, got nil")
	}
	if !strings.Contains(err.Error(), "TETRAL_TEST_DATABASE_URL") {
		t.Errorf("error must name TETRAL_TEST_DATABASE_URL, got %q", err.Error())
	}
}

func TestOpenIsolatedPostgreSQLDBRedactsMalformedDSN(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"userinfo_password", "postgres://%zz:" + sensitiveSentinel + "@host/dbname"},
		{"sslpassword_query", "postgres://%zz@host/dbname?sslpassword=" + sensitiveSentinel},
		{"passfile_query", "postgres://%zz@host/dbname?passfile=" + sensitiveSentinel},
		{"application_name_query", "postgres://%zz@host/dbname?application_name=" + sensitiveSentinel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TETRAL_TEST_DATABASE_URL", tc.dsn)
			ctx := context.Background()
			_, _, err := openIsolatedPostgreSQLDB(ctx)
			if err == nil {
				t.Fatal("expected error for malformed DSN, got nil")
			}
			msg := err.Error()
			if strings.Contains(msg, sensitiveSentinel) {
				t.Errorf("helper error leaked sentinel password: %q", msg)
			}
			if strings.Contains(msg, tc.dsn) {
				t.Errorf("helper error leaked raw DSN: %q", msg)
			}
		})
	}
}

func TestOpenIsolatedPostgreSQLDBSchemaNameIsHelperGenerated(t *testing.T) {
	// Schema name must be helper-generated; the env var name and any
	// test-controllable strings must not flow into it.
	ctx := context.Background()
	cleanup, schemaName, err := newSchemaForInspection(ctx, t)
	if err != nil {
		t.Fatalf("newSchemaForInspection: %v", err)
	}
	defer cleanup()
	if !strings.HasPrefix(schemaName, "tetral_test_") {
		t.Errorf("schema name %q must use tetral_test_ prefix", schemaName)
	}
	for _, r := range schemaName {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if !ok {
			t.Errorf("schema name %q contains non-[a-z0-9_] character %q", schemaName, r)
			break
		}
	}
}

// newSchemaForInspection wraps openIsolatedPostgreSQLDB so the test can
// observe the generated schema name + base-DB cleanup hook without
// going through NewPostgreSQLDB (which would attach to t.Cleanup).
func newSchemaForInspection(ctx context.Context, t *testing.T) (func(), string, error) {
	t.Helper()
	db, cleanup, err := openIsolatedPostgreSQLDB(ctx)
	if err != nil {
		return nil, "", err
	}
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		_ = cleanup()
		return nil, "", err
	}
	return func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Errorf("cleanup: %v", cleanupErr)
		}
	}, schema, nil
}

// itoa is a tiny test-side strconv.Itoa replacement to keep the import
// list minimal.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	negative := i < 0
	if negative {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// TestNewPostgreSQLDBPoolSizeIsParallelSafe pins that the helper does
// not hand out a pool whose connections all share a single
// per-connection state — every fresh connection from the pool must
// observe the helper schema in search_path.
func TestNewPostgreSQLDBPoolSizeIsParallelSafe(t *testing.T) {
	db := NewPostgreSQLDB(t)
	expected := ""
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&expected); err != nil {
		t.Fatal(err)
	}

	// Open many concurrent queries to force pool growth. Each must
	// land on a connection whose runtime search_path includes the
	// helper schema first.
	var wg sync.WaitGroup
	mismatchMu := &sync.Mutex{}
	var mismatch error
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var got string
			if err := db.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&got); err != nil {
				mismatchMu.Lock()
				mismatch = errors.Join(mismatch, err)
				mismatchMu.Unlock()
				return
			}
			if got != expected {
				mismatchMu.Lock()
				mismatch = errors.Join(mismatch, errors.New("connection observed schema "+got+", expected "+expected))
				mismatchMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if mismatch != nil {
		t.Fatalf("pool connections did not all see the helper schema: %v", mismatch)
	}
}

func openBasePostgreSQLDBForInspection(ctx context.Context, t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.OpenPostgreSQLDatabase(ctx, EnvTestDatabaseURL, os.Getenv(EnvTestDatabaseURL))
	if err != nil {
		t.Fatalf("open base PostgreSQL DB for public-schema inspection: %v", err)
	}
	return db
}

func publicFoundationTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT table_name
		 FROM information_schema.tables
		 WHERE table_schema = 'public'
		   AND table_type = 'BASE TABLE'
		   AND table_name IN (
		       'agent_versions',
		       'agents',
		       'credentials',
		       'environments',
		       'session_file_resources',
		       'session_github_repository_resources',
		       'session_memory_store_resources',
		       'session_resources',
		       'sessions',
		       'vaults'
		   )
		 ORDER BY table_name`)
	if err != nil {
		t.Fatalf("query public foundation tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan public foundation table: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("public foundation table rows: %v", err)
	}
	sort.Strings(names)
	return names
}

func equalOrderedStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
