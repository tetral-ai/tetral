package storagetest

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const sensitiveSentinel = "do-not-leak-this-helper"

func TestBaselineDigestBindsEveryCopiedAndReappliedInput(t *testing.T) {
	base := baselineInputs{
		helperFormat:       "format",
		schemaChecksum:     "schema",
		postgresqlContract: "postgresql-contract",
		roleContract:       "role-contract",
		seed:               "seed",
		cloneRole:          "role-flags",
		cloneGrants:        "grants",
		owner:              "owner",
		serverVersion:      "server-version",
		provenance:         "provenance",
	}
	want := digestBaselineInputs(base)
	tests := map[string]func(*baselineInputs){
		"helper format":       func(inputs *baselineInputs) { inputs.helperFormat += "-changed" },
		"schema checksum":     func(inputs *baselineInputs) { inputs.schemaChecksum += "-changed" },
		"PostgreSQL contract": func(inputs *baselineInputs) { inputs.postgresqlContract += "-changed" },
		"role contract":       func(inputs *baselineInputs) { inputs.roleContract += "-changed" },
		"seed":                func(inputs *baselineInputs) { inputs.seed += "-changed" },
		"clone role":          func(inputs *baselineInputs) { inputs.cloneRole += "-changed" },
		"clone grants":        func(inputs *baselineInputs) { inputs.cloneGrants += "-changed" },
		"owner":               func(inputs *baselineInputs) { inputs.owner += "-changed" },
		"server version":      func(inputs *baselineInputs) { inputs.serverVersion += "-changed" },
		"provenance":          func(inputs *baselineInputs) { inputs.provenance += "-changed" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			inputs := base
			mutate(&inputs)
			if got := digestBaselineInputs(inputs); got == want {
				t.Fatalf("baseline digest did not bind %s", name)
			}
		})
	}
}

func TestNewPostgreSQLDBProvidesIsolatedDatabasesAndRoles(t *testing.T) {
	dbA := NewPostgreSQLDB(t)
	dbB := NewPostgreSQLDB(t)

	var databaseA, databaseB, roleA, roleB string
	if err := dbA.QueryRow(`SELECT current_database(), current_user`).Scan(&databaseA, &roleA); err != nil {
		t.Fatalf("dbA identity: %v", err)
	}
	if err := dbB.QueryRow(`SELECT current_database(), current_user`).Scan(&databaseB, &roleB); err != nil {
		t.Fatalf("dbB identity: %v", err)
	}
	if databaseA == databaseB {
		t.Fatalf("expected distinct databases per helper call; got %q twice", databaseA)
	}
	if roleA == roleB {
		t.Fatalf("expected distinct runtime roles per helper call; got %q twice", roleA)
	}
	for _, name := range []string{databaseA, databaseB} {
		if !strings.HasPrefix(name, "tetral_test_") {
			t.Errorf("database %q must start with tetral_test_", name)
		}
	}
	for _, name := range []string{roleA, roleB} {
		if !strings.HasPrefix(name, "tetral_test_role_") {
			t.Errorf("role %q must start with tetral_test_role_", name)
		}
	}
}

func TestRuntimeCloneHandleSuppliesRealBunReadiness(t *testing.T) {
	db, admin := NewPostgreSQLDBWithAdmin(t)
	dsn := RuntimeDatabaseURL(t, db)
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))
	if output, err := runBunReadiness(root, dsn); err != nil {
		t.Fatalf("Bun readiness through clone handle failed: %v\n%s", err, output)
	}
	var role string
	if err := db.QueryRow(`SELECT current_user`).Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("ALTER ROLE " + pgx.Identifier{role}.Sanitize() + " BYPASSRLS"); err != nil {
		t.Fatal(err)
	}
	output, err := runBunReadiness(root, dsn)
	if _, restoreErr := admin.Exec("ALTER ROLE " + pgx.Identifier{role}.Sanitize() + " NOBYPASSRLS"); restoreErr != nil {
		t.Fatal(restoreErr)
	}
	if err == nil {
		t.Fatal("Bun readiness accepted a BYPASSRLS clone role")
	}
	for _, secret := range []string{dsn, role} {
		if strings.Contains(string(output), secret) {
			t.Fatal("Bun readiness failure disclosed database identity")
		}
	}
}

func runBunReadiness(root, dsn string) ([]byte, error) {
	command := exec.Command("bun", "packages/schema/test/fixtures/live-readiness.ts")
	command.Dir = filepath.Join(root, "services", "gateway")
	command.Env = append(os.Environ(), "TETRAL_TEST_RUNTIME_DATABASE_URL="+dsn)
	return command.CombinedOutput()
}

func TestNewPostgreSQLDBHidesRowsAcrossDatabases(t *testing.T) {
	// Database isolation is independent of RLS. Drive the inserts and
	// reads through admin connections to bypass FORCE RLS — the
	// assertion here is that two helper databases have no shared
	// writable state, not that RLS works.
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

func TestNewPostgreSQLDBInitializesCanonicalPublicSchemaInsideClone(t *testing.T) {
	db := NewPostgreSQLDB(t)
	var schema string
	if err := db.QueryRow(`SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatalf("current_schema: %v", err)
	}
	if schema != "public" {
		t.Fatalf("clone must expose the canonical public schema; got %q", schema)
	}
	// The clone is a private database, so its canonical schema can use the same
	// public name as production without polluting the control database.
	var helperCount int
	if err := db.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type='BASE TABLE'`,
		schema).Scan(&helperCount); err != nil {
		t.Fatal(err)
	}
	expectedCount := expectedCurrentBaseTableCount(t)
	if helperCount != expectedCount {
		t.Errorf("clone schema %q has %d base tables; want %d", schema, helperCount, expectedCount)
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

func TestTemplateCloneMatchesHelperBootstrapContract(t *testing.T) {
	runtimeDB, adminDB := NewPostgreSQLDBWithAdmin(t)
	var databaseName, runtimeRole, adminRole string
	if err := runtimeDB.QueryRow(`SELECT current_database(), current_user`).Scan(&databaseName, &runtimeRole); err != nil {
		t.Fatal(err)
	}
	if err := adminDB.QueryRow(`SELECT current_user`).Scan(&adminRole); err != nil {
		t.Fatal(err)
	}
	var publicConnect, runtimeConnect, superuser, bypassRLS, schemaUsage, schemaCreate bool
	if err := adminDB.QueryRow(`SELECT
		has_database_privilege('public', current_database(), 'CONNECT'),
		has_database_privilege($1, current_database(), 'CONNECT'),
		(SELECT rolsuper FROM pg_roles WHERE rolname=$1),
		(SELECT rolbypassrls FROM pg_roles WHERE rolname=$1),
		has_schema_privilege($1, 'public', 'USAGE'),
		has_schema_privilege($1, 'public', 'CREATE')`, runtimeRole).Scan(
		&publicConnect, &runtimeConnect, &superuser, &bypassRLS, &schemaUsage, &schemaCreate,
	); err != nil {
		t.Fatal(err)
	}
	if publicConnect || !runtimeConnect || superuser || bypassRLS || !schemaUsage || schemaCreate {
		t.Fatalf("clone boundary database=%s public_connect=%v runtime_connect=%v superuser=%v bypass_rls=%v schema_usage=%v schema_create=%v",
			databaseName, publicConnect, runtimeConnect, superuser, bypassRLS, schemaUsage, schemaCreate)
	}
	var unownedTables, missingTablePrivileges, missingSequencePrivileges int
	if err := adminDB.QueryRow(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname='public' AND c.relkind IN ('r','p') AND pg_get_userbyid(c.relowner)<>$1`, adminRole).Scan(&unownedTables); err != nil {
		t.Fatal(err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname='public' AND c.relkind IN ('r','p') AND NOT has_table_privilege($1, c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')`, runtimeRole).Scan(&missingTablePrivileges); err != nil {
		t.Fatal(err)
	}
	if err := adminDB.QueryRow(`SELECT count(*) FROM information_schema.sequences
		WHERE sequence_schema='public' AND NOT has_sequence_privilege($1, format('%I.%I', sequence_schema, sequence_name), 'USAGE')`, runtimeRole).Scan(&missingSequencePrivileges); err != nil {
		t.Fatal(err)
	}
	if unownedTables != 0 || missingTablePrivileges != 0 || missingSequencePrivileges != 0 {
		t.Fatalf("helper bootstrap drift: unowned_tables=%d missing_table_privileges=%d missing_sequence_privileges=%d", unownedTables, missingTablePrivileges, missingSequencePrivileges)
	}
	var workspaceName string
	if err := runtimeDB.QueryRow(`SELECT name FROM workspaces WHERE id=$1`, string(workspace.DefaultID)).Scan(&workspaceName); err != nil {
		t.Fatal(err)
	}
	if workspaceName != "Default Test Fixture" {
		t.Fatalf("template seed name = %q; want Default Test Fixture", workspaceName)
	}
}

func TestConnectedTemplateRefusesClone(t *testing.T) {
	source := NewPostgreSQLDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sourceConn, err := source.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sourceConn.Close() }()
	var sourceDatabase string
	if err := sourceConn.QueryRowContext(ctx, "SELECT current_database()").Scan(&sourceDatabase); err != nil {
		t.Fatal(err)
	}
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	suffix, err := randomHex(5)
	if err != nil {
		t.Fatal(err)
	}
	target := clonePrefix + "connected_" + suffix
	if err := createDatabaseClone(ctx, control, target, sourceDatabase); err == nil {
		_, _ = control.ExecContext(context.Background(), "DROP DATABASE "+target+" WITH (FORCE)")
		t.Fatal("PostgreSQL cloned a template with an active connection")
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
		_ = NewPostgreSQLDB(inner)
		afterSetup := publicFoundationTables(inner, base)
		if !equalOrderedStrings(afterSetup, before) {
			inner.Fatalf("public foundation tables changed after helper setup: before=%v after_setup=%v", before, afterSetup)
		}
	})
	afterCleanup := publicFoundationTables(t, base)
	if !equalOrderedStrings(afterCleanup, before) {
		t.Fatalf("public foundation tables changed after helper cleanup: before=%v after_cleanup=%v", before, afterCleanup)
	}
}

func TestNewPostgreSQLDBDropsDatabaseAndRoleAfterCleanup(t *testing.T) {
	// Capture the clone identity from a sub-test, let t.Cleanup run, then
	// inspect the control catalog and prove both capabilities are gone.
	var capturedDatabase, capturedRole string
	t.Run("inner", func(inner *testing.T) {
		db := NewPostgreSQLDB(inner)
		if err := db.QueryRow(`SELECT current_database(), current_user`).Scan(&capturedDatabase, &capturedRole); err != nil {
			inner.Fatalf("capture clone identity: %v", err)
		}
	})
	if capturedDatabase == "" || capturedRole == "" {
		t.Fatal("inner test failed to capture clone identity")
	}
	control := openBasePostgreSQLDBForInspection(context.Background(), t)
	defer func() { _ = control.Close() }()
	var databaseExists, roleExists bool
	if err := control.QueryRowContext(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, capturedDatabase).Scan(&databaseExists); err != nil {
		t.Fatalf("check database existence: %v", err)
	}
	if err := control.QueryRowContext(context.Background(),
		`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)`, capturedRole).Scan(&roleExists); err != nil {
		t.Fatalf("check role existence: %v", err)
	}
	if databaseExists || roleExists {
		t.Errorf("cleanup left capabilities: database_exists=%v role_exists=%v", databaseExists, roleExists)
	}
}

func TestCleanupRecoversRegisteredCloneCreatedBeforeDatabaseAuthoritySeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	if err := ensureRegistry(ctx, control); err != nil {
		t.Fatal(err)
	}
	runID, err := ensureProcessRun(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := randomHex(6)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := reserveClone(ctx, control, runID, "crash-proof", clonePrefix+runID[:8]+"_"+suffix, rolePrefix+runID[:8]+"_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err := createCloneRole(ctx, control, registration, "test-password"); err != nil {
		t.Fatal(err)
	}
	registration.phase = clonePhaseRole
	if err := createDatabaseClone(ctx, control, registration.database, ""); err != nil {
		t.Fatal(err)
	}

	if err := cleanupRegisteredClone(ctx, control, registration); err != nil {
		t.Fatalf("recover unsealed registered clone: %v", err)
	}
	assertCloneCapabilitiesAbsent(t, control, registration)
}

func TestCleanupResumesAfterCapabilityRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	provisioned, err := provisionDatabase(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = provisioned.runtime.Close()
	_ = provisioned.admin.Close()
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	registration := registrationForDatabase(t, control, provisioned.handle.database)
	cleanupPhase := cloneCleanupPrefix + registration.phase
	if _, err := control.ExecContext(ctx, "UPDATE "+cloneRegistryTable+" SET phase=$2 WHERE database_name=$1", registration.database, cleanupPhase); err != nil {
		t.Fatal(err)
	}
	registration.phase = cleanupPhase
	if _, err := control.ExecContext(ctx, "ALTER ROLE "+registration.role+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := control.ExecContext(ctx, "REVOKE CONNECT ON DATABASE "+registration.database+" FROM "+registration.role); err != nil {
		t.Fatal(err)
	}
	if err := cleanupRegisteredClone(ctx, control, registration); err != nil {
		t.Fatalf("resume cleanup after revocation: %v", err)
	}
	assertCloneCapabilitiesAbsent(t, control, registration)
}

func TestExpiredRunLeaseCannotBeRevivedByLateHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	if err := ensureRegistry(ctx, control); err != nil {
		t.Fatal(err)
	}
	runID, err := randomHex(16)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.ExecContext(ctx, "INSERT INTO "+runRegistryTable+" (run_id, owner_pid, heartbeat_at, expires_at) VALUES ($1,$2,clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 minute')", runID, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = control.ExecContext(context.Background(), "DELETE FROM "+runRegistryTable+" WHERE run_id=$1", runID)
	}()
	if err := refreshRunLease(ctx, control, runID); err == nil {
		t.Fatal("late heartbeat revived an expired run lease")
	}
	var expired bool
	if err := control.QueryRowContext(ctx, "SELECT expires_at < clock_timestamp() FROM "+runRegistryTable+" WHERE run_id=$1", runID).Scan(&expired); err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("late heartbeat changed the expired lease")
	}
}

func TestTemplateNameCollisionFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	digest := strings.Repeat("a", 64)
	name := templatePrefix + digest[:20]
	_, _ = control.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := control.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = control.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	}()
	if _, err := ensureTemplate(ctx, control, config, digest); err == nil {
		t.Fatal("unowned template-name collision was accepted")
	}
	var exists bool
	if err := control.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("unowned template-name collision was deleted")
	}
}

func TestExpiredCloneCleanupHasOneConcurrentOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	config, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	if err := ensureRegistry(ctx, control); err != nil {
		t.Fatal(err)
	}
	runID, err := randomHex(16)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := randomHex(6)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := reserveClone(ctx, control, runID, "concurrent-cleanup", clonePrefix+runID[:8]+"_"+suffix, rolePrefix+runID[:8]+"_"+suffix)
	if err != nil {
		t.Fatal(err)
	}

	errors := make(chan error, 2)
	for range 2 {
		go func() {
			errors <- cleanupExpiredResources(ctx, control, config)
		}()
	}
	for range 2 {
		if err := <-errors; err != nil {
			t.Fatalf("concurrent expired cleanup: %v", err)
		}
	}
	var remains bool
	if err := control.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+cloneRegistryTable+" WHERE database_name=$1)", registration.database).Scan(&remains); err != nil {
		t.Fatal(err)
	}
	if remains {
		t.Fatal("concurrent expired cleanup left the clone registration")
	}
}

func TestCleanupRejectsCloneWhoseDurableAuthorityDoesNotMatchRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	provisioned, err := provisionDatabase(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	controlConfig, err := parseControlConfig()
	if err != nil {
		t.Fatal(err)
	}
	control := openPool(controlConfig)
	defer func() { _ = control.Close() }()
	registration := registrationForDatabase(t, control, provisioned.handle.database)
	if _, err := control.ExecContext(ctx, "COMMENT ON DATABASE "+registration.database+" IS 'not-owned-by-storagetest'"); err != nil {
		t.Fatal(err)
	}
	if err := cleanupRegisteredClone(ctx, control, registration); err == nil {
		t.Fatal("cleanup accepted a database whose authority comment did not match its registry")
	}
	var exists bool
	if err := control.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", registration.database).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("authority mismatch deleted the database")
	}
	if _, err := control.ExecContext(ctx, "COMMENT ON DATABASE "+registration.database+" IS "+quoteLiteral(registration.databaseNote)); err != nil {
		t.Fatal(err)
	}
	if err := provisioned.cleanup(); err != nil {
		t.Fatal(err)
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

// TestNewPostgreSQLDBWithAdminPinsBothToSameDatabase pins that the
// admin and runtime DBs returned by NewPostgreSQLDBWithAdmin are
// pinned to the same per-test schema; otherwise RLS proofs that
// seed cross-workspace data through admin would not be visible to
// the runtime read path.
func TestNewPostgreSQLDBWithAdminPinsBothToSameDatabase(t *testing.T) {
	runtime, admin := NewPostgreSQLDBWithAdmin(t)
	var runtimeDatabase, adminDatabase string
	if err := runtime.QueryRow(`SELECT current_database()`).Scan(&runtimeDatabase); err != nil {
		t.Fatalf("runtime current_database: %v", err)
	}
	if err := admin.QueryRow(`SELECT current_database()`).Scan(&adminDatabase); err != nil {
		t.Fatalf("admin current_database: %v", err)
	}
	if runtimeDatabase != adminDatabase {
		t.Errorf("runtime database %q differs from admin database %q", runtimeDatabase, adminDatabase)
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

func TestOpenIsolatedPostgreSQLDBNameIsHelperGenerated(t *testing.T) {
	// Database name must be helper-generated; the env var name and any
	// test-controllable strings must not flow into it.
	ctx := context.Background()
	cleanup, databaseName, err := newDatabaseForInspection(ctx, t)
	if err != nil {
		t.Fatalf("newDatabaseForInspection: %v", err)
	}
	defer cleanup()
	if !strings.HasPrefix(databaseName, "tetral_test_") {
		t.Errorf("database name %q must use tetral_test_ prefix", databaseName)
	}
	for _, r := range databaseName {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if !ok {
			t.Errorf("database name %q contains non-[a-z0-9_] character %q", databaseName, r)
			break
		}
	}
}

// newDatabaseForInspection wraps openIsolatedPostgreSQLDB so the test can
// observe the generated database name + cleanup hook without
// going through NewPostgreSQLDB (which would attach to t.Cleanup).
func newDatabaseForInspection(ctx context.Context, t *testing.T) (func(), string, error) {
	t.Helper()
	db, cleanup, err := openIsolatedPostgreSQLDB(ctx)
	if err != nil {
		return nil, "", err
	}
	var database string
	if err := db.QueryRow(`SELECT current_database()`).Scan(&database); err != nil {
		_ = cleanup()
		return nil, "", err
	}
	return func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Errorf("cleanup: %v", cleanupErr)
		}
	}, database, nil
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
// not hand out a pool whose connections can drift to another clone.
func TestNewPostgreSQLDBPoolSizeIsParallelSafe(t *testing.T) {
	db := NewPostgreSQLDB(t)
	expected := ""
	if err := db.QueryRow(`SELECT current_database()`).Scan(&expected); err != nil {
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
			if err := db.QueryRowContext(ctx, `SELECT current_database()`).Scan(&got); err != nil {
				mismatchMu.Lock()
				mismatch = errors.Join(mismatch, err)
				mismatchMu.Unlock()
				return
			}
			if got != expected {
				mismatchMu.Lock()
				mismatch = errors.Join(mismatch, errors.New("connection observed database "+got+", expected "+expected))
				mismatchMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if mismatch != nil {
		t.Fatalf("pool connections did not all see the owning clone: %v", mismatch)
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

func registrationForDatabase(t *testing.T, control *sql.DB, database string) cloneRegistration {
	t.Helper()
	var registration cloneRegistration
	if err := control.QueryRow(`SELECT database_name, role_name, run_id, baseline_digest, phase, database_owner, database_comment, role_comment FROM `+cloneRegistryTable+` WHERE database_name=$1`, database).Scan(
		&registration.database, &registration.role, &registration.runID, &registration.baseline, &registration.phase, &registration.databaseOwner, &registration.databaseNote, &registration.roleNote,
	); err != nil {
		t.Fatal(err)
	}
	return registration
}

func assertCloneCapabilitiesAbsent(t *testing.T, control *sql.DB, registration cloneRegistration) {
	t.Helper()
	var databaseExists, roleExists, registryExists bool
	if err := control.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1), EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$2), EXISTS(SELECT 1 FROM `+cloneRegistryTable+` WHERE database_name=$1)`, registration.database, registration.role).Scan(&databaseExists, &roleExists, &registryExists); err != nil {
		t.Fatal(err)
	}
	if databaseExists || roleExists || registryExists {
		t.Fatalf("clone cleanup left database=%v role=%v registry=%v", databaseExists, roleExists, registryExists)
	}
}
