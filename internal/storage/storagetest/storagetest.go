// Package storagetest provides reusable PostgreSQL test helpers for
// Engine tests. NewPostgreSQLDB(t) returns a *sql.DB pinned to a
// unique schema with the Engine PostgreSQL schema already initialized. After
// initialization, the helper installs the legacy `workspace.DefaultID` row as a
// test fixture only; production schema initialization does not seed a hidden
// workspace. The schema is dropped during t.Cleanup. Tests use a non-superuser
// runtime role so RLS proofs run against a role that is actually subject to
// workspace_isolation, plus an admin helper for tests that need to seed
// cross-workspace data while RLS is enforced on the runtime connection.
//
// Configuration is read from TETRAL_TEST_DATABASE_URL. When the
// variable is unset the helper fails the test loudly with a message
// naming the env var; CI provides the value through the engine-ci.yml
// workflow.
package storagetest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// EnvTestDatabaseURL is the name of the environment variable the
// helper reads its base PostgreSQL DSN from.
const EnvTestDatabaseURL = "TETRAL_TEST_DATABASE_URL"

// schemaPrefix anchors helper-generated schema names. Identifier
// safety is guaranteed by construction: the suffix is hex of
// cryptographically random bytes so the full name only contains
// [a-z0-9_].
const schemaPrefix = "tetral_test_"

// helperPoolMaxOpen caps the per-test connection pool. Tests typically
// need a handful of connections; capping keeps PostgreSQL load
// predictable when many parallel tests run concurrently.
const helperPoolMaxOpen = 4

// runtimeRoleName is the non-superuser PostgreSQL role tests use to
// exercise Engine business connections under RLS. The role is
// created once per test process by the helper and reused across
// every per-test schema. The role name uses only [a-z0-9_] so it
// never needs identifier quoting.
const runtimeRoleName = "tetral_runtime_test"

// runtimeRolePassword is the fixed test password for runtimeRoleName.
// It is a test-only secret that lives nowhere near production
// credentials; CI rotates it implicitly because every CI run boots a
// fresh PostgreSQL container.
const runtimeRolePassword = "tetral_runtime_test_pw" //nolint:gosec // G101: test-only role password, never used in production

// runtimeRoleOnce ensures the helper creates/refreshes the runtime
// role only once per process even when many tests call NewPostgreSQLDB
// in parallel.
var (
	runtimeRoleOnce sync.Once
	runtimeRoleErr  error
)

// NewPostgreSQLDB provisions an isolated PostgreSQL schema for the
// caller, runs the Engine PostgreSQL schema initialization against it,
// grants the helper runtime role privileges on the schema, and
// returns a *sql.DB whose connections authenticate as the
// non-superuser runtime role with search_path pinned to that schema.
// The schema is dropped (with CASCADE) during t.Cleanup. Missing
// TETRAL_TEST_DATABASE_URL fails the test with a clear message; raw
// DSN secrets are never echoed in helper error text.
//
// Tests that need to seed cross-workspace rows under FORCE RLS — for
// example RLS proofs that insert under workspace A and prove
// workspace B cannot see them — must use NewPostgreSQLDBWithAdmin or
// NewPostgreSQLAdminDB instead, because the runtime role is subject
// to workspace_isolation.
func NewPostgreSQLDB(t testing.TB) *sql.DB {
	t.Helper()
	runtime, _ := newPostgreSQLDBPair(t, false)
	return runtime
}

// NewPostgreSQLDBWithAdmin provisions an isolated PostgreSQL schema
// and returns BOTH the runtime role *sql.DB and an admin (superuser)
// *sql.DB pinned to the same schema. The runtime DB is subject to
// FORCE RLS; the admin DB bypasses RLS as a superuser and is the
// allowed setup path for cross-workspace seeding in tests. Both DBs
// share cleanup via t.Cleanup.
func NewPostgreSQLDBWithAdmin(t testing.TB) (runtime, admin *sql.DB) {
	t.Helper()
	return newPostgreSQLDBPair(t, true)
}

// NewEmptyPostgreSQLAdminDB provisions an isolated schema without applying or
// stamping the Engine schema. It is reserved for migration ownership tests
// that must exercise empty and pre-registry states.
func NewEmptyPostgreSQLAdminDB(t testing.TB) *sql.DB {
	t.Helper()
	_, admin := newPostgreSQLDBPairWithInitialization(t, true, false)
	return admin
}

// NewPostgreSQLAdminDB provisions an isolated PostgreSQL schema and
// returns only the admin (superuser) *sql.DB pinned to that schema.
// Used by schema-shape tests that intentionally insert through admin
// because their assertions are about catalog or constraint behavior
// rather than RLS.
func NewPostgreSQLAdminDB(t testing.TB) *sql.DB {
	t.Helper()
	_, admin := newPostgreSQLDBPair(t, true)
	return admin
}

func newPostgreSQLDBPair(t testing.TB, withAdmin bool) (runtime, admin *sql.DB) {
	return newPostgreSQLDBPairWithInitialization(t, withAdmin, true)
}

func newPostgreSQLDBPairWithInitialization(t testing.TB, withAdmin bool, initialize bool) (runtime, admin *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime, admin, cleanup, err := openIsolatedPostgreSQLDBWithAdminAndInitialization(ctx, withAdmin, initialize)
	if err != nil {
		t.Fatalf("storagetest.NewPostgreSQLDB: %v", err)
	}
	t.Cleanup(func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			t.Errorf("storagetest.NewPostgreSQLDB cleanup: %v", cleanupErr)
		}
	})
	return runtime, admin
}

// openIsolatedPostgreSQLDB is preserved for the existing
// missing-config and malformed-DSN tests that need to drive the
// helper without aborting the parent test. It returns the runtime DB
// only; tests that need admin access should call
// openIsolatedPostgreSQLDBWithAdmin.
func openIsolatedPostgreSQLDB(ctx context.Context) (*sql.DB, func() error, error) {
	runtime, _, cleanup, err := openIsolatedPostgreSQLDBWithAdmin(ctx, false)
	return runtime, cleanup, err
}

// openIsolatedPostgreSQLDBWithAdmin is the shared lower-level entry
// point. When withAdmin is true the second return is an admin
// (superuser) *sql.DB pinned to the same helper schema; otherwise it
// is nil.
//
// The cleanup function must be called exactly once. It closes the
// runtime DB, the admin DB if any, drops the helper schema with
// CASCADE through a separate base connection, closes that connection,
// and returns any DROP SCHEMA error so the caller can surface it.
func openIsolatedPostgreSQLDBWithAdmin(ctx context.Context, withAdmin bool) (runtime, admin *sql.DB, cleanup func() error, err error) {
	return openIsolatedPostgreSQLDBWithAdminAndInitialization(ctx, withAdmin, true)
}

func openIsolatedPostgreSQLDBWithAdminAndInitialization(ctx context.Context, withAdmin bool, initialize bool) (runtime, admin *sql.DB, cleanup func() error, err error) {
	dsn := os.Getenv(EnvTestDatabaseURL)
	if dsn == "" {
		return nil, nil, nil, &MissingTestDSNError{EnvVarName: EnvTestDatabaseURL}
	}

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		// Do not surface pgx's parse error directly: it embeds the
		// raw DSN and would echo userinfo/query secrets. The typed
		// error degrades to host=<unknown> database=<unknown>.
		return nil, nil, nil, &MalformedTestDSNError{EnvVarName: EnvTestDatabaseURL}
	}

	if err := ensureRuntimeRole(ctx, cfg); err != nil {
		return nil, nil, nil, err
	}

	schemaName, err := generateSchemaName()
	if err != nil {
		return nil, nil, nil, err
	}

	baseDB := sql.OpenDB(stdlib.GetConnector(*cfg))
	if _, err := baseDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		_ = baseDB.Close()
		return nil, nil, nil, &SchemaSetupError{Stage: "create_schema", Schema: schemaName}
	}

	// Initialize the schema using the admin connection pinned to the
	// helper schema (ALTER ROLE ... ENABLE/FORCE RLS works for any
	// role that owns the table; the admin/superuser used by tests
	// owns the tables it creates).
	adminCfg := cfg.Copy()
	if adminCfg.RuntimeParams == nil {
		adminCfg.RuntimeParams = map[string]string{}
	}
	adminCfg.RuntimeParams["search_path"] = schemaName + ",pg_catalog"
	adminDB := sql.OpenDB(stdlib.GetConnector(*adminCfg))
	adminDB.SetMaxOpenConns(helperPoolMaxOpen)
	adminDB.SetMaxIdleConns(helperPoolMaxOpen)

	if initialize {
		if err := storage.MigrateSchema(ctx, adminDB); err != nil {
			_ = adminDB.Close()
			_, _ = baseDB.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
			_ = baseDB.Close()
			return nil, nil, nil, err
		}
		if err := seedTestDefaultWorkspace(ctx, adminDB); err != nil {
			_ = adminDB.Close()
			_, _ = baseDB.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
			_ = baseDB.Close()
			return nil, nil, nil, &SchemaSetupError{Stage: "seed_test_default_workspace", Schema: schemaName}
		}
	}

	// Grant the runtime role privileges on the helper schema so it can
	// connect, look up the schema in search_path, and exercise the
	// workspace-owned tables under RLS.
	grantStmts := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", schemaName, runtimeRoleName),
		fmt.Sprintf("GRANT ALL ON ALL TABLES IN SCHEMA %s TO %s", schemaName, runtimeRoleName),
		fmt.Sprintf("GRANT ALL ON ALL SEQUENCES IN SCHEMA %s TO %s", schemaName, runtimeRoleName),
	}
	for _, stmt := range grantStmts {
		if _, err := adminDB.ExecContext(ctx, stmt); err != nil {
			_ = adminDB.Close()
			_, _ = baseDB.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
			_ = baseDB.Close()
			return nil, nil, nil, &SchemaSetupError{Stage: "grant_runtime_role", Schema: schemaName}
		}
	}

	// Build the runtime DSN: same host/database as admin but
	// authenticate as runtimeRoleName with runtimeRolePassword. pgx
	// honors per-config user/password without re-parsing the DSN.
	runtimeCfg := cfg.Copy()
	if runtimeCfg.RuntimeParams == nil {
		runtimeCfg.RuntimeParams = map[string]string{}
	}
	runtimeCfg.RuntimeParams["search_path"] = schemaName + ",pg_catalog"
	runtimeCfg.User = runtimeRoleName
	runtimeCfg.Password = runtimeRolePassword
	runtimeDB := sql.OpenDB(stdlib.GetConnector(*runtimeCfg))
	runtimeDB.SetMaxOpenConns(helperPoolMaxOpen)
	runtimeDB.SetMaxIdleConns(helperPoolMaxOpen)

	if err := runtimeDB.PingContext(ctx); err != nil {
		_ = runtimeDB.Close()
		_ = adminDB.Close()
		_, _ = baseDB.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName))
		_ = baseDB.Close()
		return nil, nil, nil, &SchemaSetupError{Stage: "ping_runtime_role", Schema: schemaName}
	}

	cleanup = func() error {
		_ = runtimeDB.Close()
		if adminDB != nil {
			_ = adminDB.Close()
		}
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		var dropErr error
		if _, err := baseDB.ExecContext(dropCtx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schemaName)); err != nil {
			dropErr = &SchemaCleanupError{Schema: schemaName}
		}
		_ = baseDB.Close()
		return dropErr
	}

	if !withAdmin {
		// The caller did not request admin access; close the admin DB
		// inside cleanup but stop exposing it as a return.
		return runtimeDB, nil, cleanup, nil
	}
	return runtimeDB, adminDB, cleanup, nil
}

func seedTestDefaultWorkspace(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', 'Default Test Fixture', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspace.DefaultID),
	)
	return err
}

// runtimeRoleAdvisoryLockKey is the cross-process serialization key
// used to guard CREATE ROLE / ALTER ROLE on the runtime test role.
// PostgreSQL session-level advisory locks are cluster-wide, so two
// `go test` packages that race on the same role catalog row both
// queue on this key and avoid the "tuple concurrently updated"
// SQLSTATE XX000 that bare DO $$...$$ blocks surface under contention.
// The literal value is a stable arbitrary int64; only this helper
// uses it.
const runtimeRoleAdvisoryLockKey int64 = 0x7465_7472_616c_5254 // "tetralRT"

// ensureRuntimeRole creates or refreshes the runtime role
// (NOSUPERUSER, NOBYPASSRLS, LOGIN with the fixed test password).
// Idempotent within a single process via runtimeRoleOnce, and
// idempotent across processes via a PostgreSQL session-level advisory
// lock that serializes the CREATE/ALTER role catalog mutation. Without
// the cross-process lock, parallel `go test` packages racing on the
// same role row produce sporadic SQLSTATE XX000 ("tuple concurrently
// updated") errors.
func ensureRuntimeRole(ctx context.Context, baseCfg *pgx.ConnConfig) error {
	runtimeRoleOnce.Do(func() {
		baseDB := sql.OpenDB(stdlib.GetConnector(*baseCfg))
		defer func() { _ = baseDB.Close() }()
		// pin one connection so the session-level advisory lock and
		// the role catalog mutation use the same backend.
		conn, connErr := baseDB.Conn(ctx)
		if connErr != nil {
			runtimeRoleErr = &RuntimeRoleSetupError{Cause: connErr.Error()}
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", runtimeRoleAdvisoryLockKey); err != nil {
			runtimeRoleErr = &RuntimeRoleSetupError{Cause: err.Error()}
			return
		}
		defer func() { _, _ = conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", runtimeRoleAdvisoryLockKey) }()
		// The role name and password are package-level constants and
		// not user input, so embedding them in the DDL is safe. The
		// DO $$ ... $$ block keeps creation/refresh atomic, and the
		// surrounding advisory lock stops catalog races between
		// concurrent test processes.
		stmt := fmt.Sprintf(
			`DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN
					CREATE ROLE %s LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '%s';
				ELSE
					ALTER ROLE %s LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD '%s';
				END IF;
			END $$;`,
			runtimeRoleName, runtimeRoleName, runtimeRolePassword,
			runtimeRoleName, runtimeRolePassword,
		)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			runtimeRoleErr = &RuntimeRoleSetupError{Cause: err.Error()}
		}
	})
	return runtimeRoleErr
}

// generateSchemaName produces a per-call unique schema name. The name
// uses a fixed prefix, a UTC timestamp for human-readable triage, and
// 8 cryptographically random bytes (16 hex chars) for collision
// safety. All characters are in [a-z0-9_] so identifier quoting is
// unnecessary.
func generateSchemaName() (string, error) {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", &SchemaSetupError{Stage: "random_suffix"}
	}
	ts := time.Now().UTC().Format("20060102_150405")
	return schemaPrefix + ts + "_" + hex.EncodeToString(raw[:]), nil
}

// MissingTestDSNError indicates TETRAL_TEST_DATABASE_URL was unset
// when a helper that requires it was called. Error() text is
// public-safe and names the env var so the developer can fix it
// locally; CI provides the value automatically.
type MissingTestDSNError struct {
	EnvVarName string
}

func (e *MissingTestDSNError) Error() string {
	return fmt.Sprintf("storagetest: %s is not set; provide a real PostgreSQL test DSN (CI provides it automatically)", e.EnvVarName)
}

// MalformedTestDSNError indicates the configured
// TETRAL_TEST_DATABASE_URL failed pgx.ParseConfig. The raw pgx error
// is intentionally not embedded in Error() because it can echo the
// DSN, including userinfo and query-string secrets.
type MalformedTestDSNError struct {
	EnvVarName string
}

func (e *MalformedTestDSNError) Error() string {
	return fmt.Sprintf("storagetest: %s is malformed (host=<unknown> database=<unknown>); raw error suppressed to avoid echoing DSN secrets", e.EnvVarName)
}

// SchemaSetupError indicates that schema provisioning before
// initialization failed. Stage names the failed step (random_suffix,
// create_schema, grant_runtime_role, ping_runtime_role) and never
// includes raw SQL or driver state.
type SchemaSetupError struct {
	Stage  string
	Schema string
}

func (e *SchemaSetupError) Error() string {
	if e.Schema == "" {
		return fmt.Sprintf("storagetest: schema setup failed at stage %q", e.Stage)
	}
	return fmt.Sprintf("storagetest: schema setup failed at stage %q for schema %q", e.Stage, e.Schema)
}

// SchemaCleanupError indicates DROP SCHEMA ... CASCADE failed during
// helper teardown. The raw driver error is intentionally not echoed —
// callers receive only the schema name so they can investigate
// manually.
type SchemaCleanupError struct {
	Schema string
}

func (e *SchemaCleanupError) Error() string {
	return fmt.Sprintf("storagetest: failed to drop helper schema %q during cleanup", e.Schema)
}

// RuntimeRoleSetupError indicates the helper failed to create or
// refresh the non-superuser runtime role. The cause is summarized so
// developers can recover, but raw driver state is not embedded.
type RuntimeRoleSetupError struct {
	Cause string
}

func (e *RuntimeRoleSetupError) Error() string {
	return fmt.Sprintf("storagetest: failed to create or refresh runtime role %q: %s", runtimeRoleName, e.Cause)
}
