package dbconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const sensitivePlainDSNSentinel = "do-not-leak-dbconnect"

func requirePostgreSQLTestDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("TETRAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TETRAL_TEST_DATABASE_URL is required: set it to a real PostgreSQL test DSN. CI provides it automatically.")
	}
	return dsn
}

func newTestClient(db *sql.DB) *Client {
	return &Client{
		db:         db,
		provider:   ProviderPlainDSN,
		descriptor: Descriptor{Host: "test", Database: "test"},
	}
}

func assertDiagnostic(t *testing.T, err error, phase Phase, kind Kind) *DiagnosticError {
	t.Helper()
	if err == nil {
		t.Fatal("expected diagnostic error, got nil")
	}
	var diagnostic *DiagnosticError
	if !errors.As(err, &diagnostic) {
		t.Fatalf("expected DiagnosticError, got %T (%v)", err, err)
	}
	if diagnostic.Provider != ProviderPlainDSN {
		t.Fatalf("provider = %q; want %q", diagnostic.Provider, ProviderPlainDSN)
	}
	if diagnostic.Phase != phase || diagnostic.Kind != kind {
		t.Fatalf("phase/kind = %s/%s; want %s/%s", diagnostic.Phase, diagnostic.Kind, phase, kind)
	}
	return diagnostic
}

func assertPublicSafe(t *testing.T, err error, forbidden ...string) {
	t.Helper()
	text := err.Error()
	for _, token := range forbidden {
		if token != "" && strings.Contains(text, token) {
			t.Fatalf("diagnostic Error() leaked %q in %q", token, text)
		}
	}
	if strings.Contains(text, "postgres://") || strings.Contains(text, "postgresql://") {
		t.Fatalf("diagnostic Error() leaked connection string in %q", text)
	}
}

func sensitiveDSNCases(host string) []string {
	return []string{
		"postgres://tetral:" + sensitivePlainDSNSentinel + "@" + host + "/tetral?connect_timeout=1",
		"postgres://tetral@" + host + "/tetral?sslpassword=" + sensitivePlainDSNSentinel + "&connect_timeout=1",
		"postgres://tetral@" + host + "/tetral?passfile=" + sensitivePlainDSNSentinel + "&connect_timeout=1",
		"postgres://tetral:" + sensitivePlainDSNSentinel + "@" + host + "/tetral?sslpassword=" + sensitivePlainDSNSentinel + "&application_name=" + sensitivePlainDSNSentinel + "&connect_timeout=1",
	}
}

func TestOpenPlainDSNEmptyAndMalformedAreDiagnostics(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		dsn  string
	}{
		{name: "empty", dsn: ""},
		{name: "malformed", dsn: "postgres://tetral:" + sensitivePlainDSNSentinel + "@%zz/tetral"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenPlainDSN(ctx, "TETRAL_DATABASE_URL", tc.dsn)
			diagnostic := assertDiagnostic(t, err, PhaseParseConfig, KindInvalidConfig)
			if diagnostic.Descriptor.Host != UnknownDescriptorField || diagnostic.Descriptor.Database != UnknownDescriptorField {
				t.Fatalf("descriptor = %+v; want unknown fields", diagnostic.Descriptor)
			}
			assertPublicSafe(t, err, tc.dsn, sensitivePlainDSNSentinel)
		})
	}
}

func TestOpenPlainDSNRedactsUnreachableDSN(t *testing.T) {
	for _, dsn := range sensitiveDSNCases("127.0.0.1:1") {
		t.Run("redacted", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := OpenPlainDSN(ctx, "TETRAL_DATABASE_URL", dsn)
			diagnostic := assertDiagnostic(t, err, PhasePing, KindEndpointUnreachable)
			if diagnostic.Descriptor.Host != "127.0.0.1" {
				t.Fatalf("descriptor host = %q; want parsed host", diagnostic.Descriptor.Host)
			}
			if diagnostic.Descriptor.Database != "tetral" {
				t.Fatalf("descriptor database = %q; want tetral", diagnostic.Descriptor.Database)
			}
			assertPublicSafe(t, err, dsn, sensitivePlainDSNSentinel)
			if diagnostic.Unwrap() == nil {
				t.Fatal("unreachable diagnostic must preserve original cause")
			}
		})
	}
}

func TestOpenPlainDSNContextCancellationAndTimeoutDiagnostics(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)

	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	_, err := OpenPlainDSN(canceled, "TETRAL_DATABASE_URL", dsn)
	diagnostic := assertDiagnostic(t, err, PhasePing, KindCanceled)
	if !errors.Is(diagnostic, context.Canceled) {
		t.Fatalf("diagnostic must unwrap context.Canceled, got %v", diagnostic.Unwrap())
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	_, err = OpenPlainDSN(expired, "TETRAL_DATABASE_URL", dsn)
	diagnostic = assertDiagnostic(t, err, PhasePing, KindTimeout)
	if !errors.Is(diagnostic, context.DeadlineExceeded) {
		t.Fatalf("diagnostic must unwrap context deadline, got %v", diagnostic.Unwrap())
	}
}

func TestOpenPlainDSNSuccessAndFromEnv(t *testing.T) {
	dsn := requirePostgreSQLTestDSN(t)
	t.Setenv("TETRAL_DATABASE_URL", dsn)
	t.Setenv("TETRAL_TEST_DATABASE_URL", "postgres://tetral:must-not-be-used@127.0.0.1:1/tetral")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := OpenPlainDSN(ctx, "TETRAL_DATABASE_URL", dsn)
	if err != nil {
		t.Fatalf("OpenPlainDSN: %v", err)
	}
	defer func() { _ = result.Client.Close() }()
	if result.Provider != ProviderPlainDSN || result.Client == nil || result.RawDatabaseForExcludedStores == nil {
		t.Fatalf("unexpected open result: %+v", result)
	}
	expectedConfig, err := configurePlainDSN(dsn)
	if err != nil {
		t.Fatalf("configure expected DSN: %v", err)
	}
	expectedDescriptor := descriptorFromConfig(expectedConfig)
	if result.Descriptor != expectedDescriptor {
		t.Fatalf("open descriptor = %+v; want %+v", result.Descriptor, expectedDescriptor)
	}
	if result.Client.descriptor != expectedDescriptor {
		t.Fatalf("client descriptor = %+v; want %+v", result.Client.descriptor, expectedDescriptor)
	}
	if err := result.Client.Ping(ctx); err != nil {
		t.Fatalf("Ping after open: %v", err)
	}

	envResult, err := OpenPlainDSNFromEnv(ctx)
	if err != nil {
		t.Fatalf("OpenPlainDSNFromEnv: %v", err)
	}
	defer func() { _ = envResult.Client.Close() }()
	if err := envResult.Client.Ping(ctx); err != nil {
		t.Fatalf("Ping env result: %v", err)
	}
	if envResult.Descriptor != expectedDescriptor {
		t.Fatalf("env descriptor = %+v; want %+v", envResult.Descriptor, expectedDescriptor)
	}
}

func TestOpenPlainDSNFromEnvDoesNotFallbackToTestVariable(t *testing.T) {
	t.Setenv("TETRAL_DATABASE_URL", "")
	t.Setenv("TETRAL_TEST_DATABASE_URL", requirePostgreSQLTestDSN(t))

	_, err := OpenPlainDSNFromEnv(context.Background())
	_ = assertDiagnostic(t, err, PhaseParseConfig, KindInvalidConfig)
	assertPublicSafe(t, err, "TETRAL_TEST_DATABASE_URL")
}

func TestConfigurePlainDSNPreservesSecureSSLMode(t *testing.T) {
	for _, sslmode := range []string{"require", "verify-full"} {
		t.Run(sslmode, func(t *testing.T) {
			cfg, err := configurePlainDSN("postgres://tetral@example.invalid/tetral?sslmode=" + sslmode)
			if err != nil {
				t.Fatalf("configurePlainDSN: %v", err)
			}
			if cfg.TLSConfig == nil {
				t.Fatalf("sslmode=%s produced nil TLSConfig; must not downgrade TLS", sslmode)
			}
		})
	}
}

func TestDiagnosticErrorRedactsAndUnwraps(t *testing.T) {
	cause := errors.New("secret cause remains internal")
	diagnostic := &DiagnosticError{
		Provider:   ProviderPlainDSN,
		Descriptor: Descriptor{Host: "private-db.internal.example", Database: "tenant_control_plane"},
		Phase:      PhaseRuntimeQuery,
		Kind:       KindInternalError,
		Operation:  "skill.create_version",
		Message:    "query failed",
		Cause:      cause,
	}
	if !errors.Is(diagnostic, cause) {
		t.Fatalf("DiagnosticError must unwrap original cause")
	}
	assertPublicSafe(t, diagnostic,
		"secret", "SELECT", "INSERT", sensitivePlainDSNSentinel, "postgres://tetral:password@localhost/db",
		"private-db.internal.example", "tenant_control_plane",
	)
}

func TestDiagnosticErrorTextExcludesDescriptorAcrossPhases(t *testing.T) {
	descriptor := Descriptor{Host: "private-pg.example.internal", Database: "customer_control_plane"}
	for _, phase := range []Phase{
		PhaseParseConfig,
		PhaseOpenConnection,
		PhasePing,
		PhaseMigrateSchema,
		PhaseVerifySchema,
		PhaseVerifyRuntimeRole,
		PhaseRuntimeQuery,
	} {
		t.Run(string(phase), func(t *testing.T) {
			diagnostic := &DiagnosticError{
				Provider:   ProviderPlainDSN,
				Descriptor: descriptor,
				Phase:      phase,
				Kind:       KindInternalError,
				Operation:  "dbconnect.probe",
				Cause:      errors.New("driver detail"),
			}
			assertPublicSafe(t, diagnostic, descriptor.Host, descriptor.Database, "driver detail")
			if diagnostic.Descriptor != descriptor {
				t.Fatalf("descriptor field = %+v; want %+v", diagnostic.Descriptor, descriptor)
			}
		})
	}
}

func TestStartupHelpersMapStorageErrorsToDiagnostics(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	if err := runtimeDB.Close(); err != nil {
		t.Fatalf("close runtime db: %v", err)
	}
	client := newTestClient(runtimeDB)
	err := client.MigrateSchema(context.Background())
	diagnostic := assertDiagnostic(t, err, PhaseMigrateSchema, KindSchemaMigrationFailed)
	var migrationErr *storage.SchemaMigrationError
	if !errors.As(diagnostic, &migrationErr) {
		t.Fatalf("diagnostic must unwrap SchemaMigrationError, got %T", diagnostic.Unwrap())
	}

	emptyAdmin := storagetest.NewEmptyPostgreSQLAdminDB(t)
	emptyClient := newTestClient(emptyAdmin)
	err = emptyClient.VerifySchema(context.Background())
	diagnostic = assertDiagnostic(t, err, PhaseVerifySchema, KindSchemaVerificationFailed)
	if !errors.As(diagnostic, &migrationErr) || migrationErr.Kind != storage.SchemaErrorMissing {
		t.Fatalf("diagnostic must unwrap missing SchemaMigrationError, got %#v", migrationErr)
	}

	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	adminClient := newTestClient(admin)
	err = adminClient.VerifyRuntimeRole(context.Background())
	diagnostic = assertDiagnostic(t, err, PhaseVerifyRuntimeRole, KindRuntimeRoleInvalid)
	var roleErr *storage.RuntimeRoleError
	if !errors.As(diagnostic, &roleErr) {
		t.Fatalf("diagnostic must unwrap RuntimeRoleError, got %T", diagnostic.Unwrap())
	}
}

func TestClientExecQueryQueryRowAndRowsLifecycle(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)
	ctx := context.Background()

	result, err := client.Exec(ctx, "dbconnect.insert_workspace",
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z') ON CONFLICT DO NOTHING`,
		"workspace_dbconnect")
	if err != nil {
		t.Fatalf("Exec insert workspace: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if affected != 1 {
		t.Fatalf("RowsAffected = %d; want 1", affected)
	}

	rows, err := client.Query(ctx, "dbconnect.list_workspaces", `SELECT id FROM workspaces WHERE id = $1`, "workspace_dbconnect")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected one workspace row")
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Rows.Scan: %v", err)
	}
	if id != "workspace_dbconnect" {
		t.Fatalf("id = %q", id)
	}
	if rows.Next() {
		t.Fatal("expected only one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err: %v", err)
	}

	err = client.QueryRow(ctx, "dbconnect.no_rows", `SELECT id FROM workspaces WHERE id = 'missing'`).Scan(&id)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("QueryRow no rows err = %T %v; want sql.ErrNoRows", err, err)
	}
}

func TestRowsScanAndIterationErrorsArePreserved(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	rows, err := client.Query(context.Background(), "dbconnect.scan_error", `SELECT 'not-an-int'`)
	if err != nil {
		t.Fatalf("Query scan error: %v", err)
	}
	if !rows.Next() {
		t.Fatal("expected conversion row")
	}
	var number int
	err = rows.Scan(&number)
	if err == nil {
		t.Fatal("expected scan conversion error")
	}
	if strings.Contains(err.Error(), "dbconnect") {
		t.Fatalf("scan conversion error should be original driver error, got %v", err)
	}
	_ = rows.Close()

	iterCtx, cancel := context.WithCancel(context.Background())
	rows, err = client.Query(iterCtx, "dbconnect.iteration_error", `SELECT generate_series(1, 100000)`)
	if err != nil {
		t.Fatalf("Query iteration error: %v", err)
	}
	if !rows.Next() {
		t.Fatal("expected first generated row")
	}
	cancel()
	for rows.Next() {
	}
	err = rows.Err()
	if err == nil {
		t.Fatal("expected context cancellation from Rows.Err")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Rows.Err = %T %v; want context.Canceled reachable", err, err)
	}
	_ = rows.Close()
}

func TestWithTxCommitsAndRollsBack(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(admin)
	ctx := context.Background()

	if err := client.WithTx(ctx, "dbconnect.commit", nil, func(tx *Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO workspaces (id, type, name, created_at)
			 VALUES ('workspace_tx_commit', 'workspace', 'commit', '2026-01-01T00:00:00Z')`)
		return err
	}); err != nil {
		t.Fatalf("WithTx commit: %v", err)
	}
	assertWorkspaceCount(t, admin, "workspace_tx_commit", 1)

	sentinel := errors.New("rollback sentinel")
	err := client.WithTx(ctx, "dbconnect.rollback", nil, func(tx *Tx) error {
		if _, execErr := tx.Exec(ctx,
			`INSERT INTO workspaces (id, type, name, created_at)
			 VALUES ('workspace_tx_rollback', 'workspace', 'rollback', '2026-01-01T00:00:00Z')`); execErr != nil {
			return execErr
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx rollback err = %v; want sentinel", err)
	}
	assertWorkspaceCount(t, admin, "workspace_tx_rollback", 0)
}

func TestWithTxQueryUsesTransactionRowsLifecycle(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(admin)
	ctx := context.Background()

	err := client.WithTx(ctx, "dbconnect.query_tx", nil, func(tx *Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO workspaces (id, type, name, created_at)
			 VALUES ('workspace_tx_query', 'workspace', 'query-tx', '2026-01-01T00:00:00Z')`); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT id, name FROM workspaces WHERE id = $1 ORDER BY id ASC`,
			"workspace_tx_query")
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		if !rows.Next() {
			return errors.New("tx.Query returned no rows for row inserted in same transaction")
		}
		var idValue, name string
		if err := rows.Scan(&idValue, &name); err != nil {
			return err
		}
		if idValue != "workspace_tx_query" || name != "query-tx" {
			return errors.New("tx.Query returned unexpected row contents")
		}
		if rows.Next() {
			return errors.New("tx.Query returned extra rows")
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatalf("WithTx tx.Query: %v", err)
	}
	assertWorkspaceCount(t, admin, "workspace_tx_query", 1)
}

func TestClientCloseIsIdempotentAndPreventsLaterDispatch(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping before close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
	if err := client.Ping(context.Background()); err == nil {
		t.Fatal("Ping after Close reported success")
	}
	if _, err := client.Exec(context.Background(), "dbconnect.after_close", `SELECT 1`); err == nil {
		t.Fatal("Exec after Close reported success")
	}
	if _, err := client.Query(context.Background(), "dbconnect.after_close", `SELECT 1`); err == nil {
		t.Fatal("Query after Close reported success")
	}
}

func TestBusinessErrorsRemainVisible(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(admin)
	ctx := context.Background()

	if _, err := client.Exec(ctx, "dbconnect.create_unique_probe", `CREATE TEMP TABLE unique_probe (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create temp table: %v", err)
	}
	if _, err := client.Exec(ctx, "dbconnect.insert_unique_probe", `INSERT INTO unique_probe (id) VALUES (1)`); err != nil {
		t.Fatalf("insert first row: %v", err)
	}
	_, err := client.Exec(ctx, "dbconnect.insert_unique_probe", `INSERT INTO unique_probe (id) VALUES (1)`)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("unique violation = %T %v; want pgconn.PgError 23505 visible", err, err)
	}
	var diagnostic *DiagnosticError
	if errors.As(err, &diagnostic) {
		t.Fatalf("business PgError must not be wrapped as diagnostic: %+v", diagnostic)
	}
}

func TestRuntimeOperationalErrorsClassifyConservatively(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)

	canceled, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled()
	_, err := client.Exec(canceled, "dbconnect.canceled", `SELECT 1`)
	diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, KindCanceled)
	if !errors.Is(diagnostic, context.Canceled) {
		t.Fatalf("canceled diagnostic must unwrap context.Canceled, got %v", diagnostic.Unwrap())
	}

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	_, err = client.Exec(expired, "dbconnect.timeout", `SELECT 1`)
	diagnostic = assertDiagnostic(t, err, PhaseRuntimeQuery, KindTimeout)
	if !errors.Is(diagnostic, context.DeadlineExceeded) {
		t.Fatalf("timeout diagnostic must unwrap context deadline, got %v", diagnostic.Unwrap())
	}

	err = client.classifyRuntimeError("dbconnect.bad_conn", driver.ErrBadConn)
	diagnostic = assertDiagnostic(t, err, PhaseRuntimeQuery, KindEndpointUnreachable)
	if !errors.Is(diagnostic, driver.ErrBadConn) {
		t.Fatalf("bad connection diagnostic must unwrap driver.ErrBadConn, got %v", diagnostic.Unwrap())
	}
}

func TestInvalidOperationLabelsAreRejectedSafelyAcrossDispatchPaths(t *testing.T) {
	db := storagetest.NewPostgreSQLDB(t)
	client := newTestClient(db)
	for _, operation := range []string{"", "SELECT * FROM secrets", "agent.create.workspace_123", "Agent.Create", "agent/create"} {
		t.Run(operation, func(t *testing.T) {
			_, err := client.Exec(context.Background(), operation, `SELECT 1`)
			assertInvalidOperationDiagnostic(t, err, operation)

			_, err = client.Query(context.Background(), operation, `SELECT 1`)
			assertInvalidOperationDiagnostic(t, err, operation)

			err = client.QueryRow(context.Background(), operation, `SELECT 1`).Scan(new(int))
			assertInvalidOperationDiagnostic(t, err, operation)

			err = client.WithTx(context.Background(), operation, nil, func(_ *Tx) error {
				t.Fatal("WithTx callback must not run for invalid operation")
				return nil
			})
			assertInvalidOperationDiagnostic(t, err, operation)

			err = client.WithWorkspaceTx(context.Background(), string(workspaceIDForInvalidOperationTest), operation, func(_ *Tx) error {
				t.Fatal("WithWorkspaceTx callback must not run for invalid operation")
				return nil
			})
			assertInvalidOperationDiagnostic(t, err, operation)

			err = client.WithWorkspaceTxAndCleanup(context.Background(), string(workspaceIDForInvalidOperationTest), operation, func(_ *Tx) error {
				t.Fatal("WithWorkspaceTxAndCleanup callback must not run for invalid operation")
				return nil
			}, func() {
				t.Fatal("cleanup must not run for invalid operation")
			})
			assertInvalidOperationDiagnostic(t, err, operation)
		})
	}
}

const workspaceIDForInvalidOperationTest = "default"

func assertInvalidOperationDiagnostic(t *testing.T, err error, operation string) {
	t.Helper()
	diagnostic := assertDiagnostic(t, err, PhaseRuntimeQuery, KindInternalError)
	assertPublicSafe(t, diagnostic, operation, "SELECT * FROM secrets", "workspace_123")
}

func TestWithWorkspaceTxScopesRollsBackAndDoesNotLeak(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(runtimeDB)
	ctx := context.Background()
	seedWorkspaceAndVault(t, admin, "workspace_dbconnect_a", "vlt_dbconnect_a")

	err := client.WithWorkspaceTx(ctx, "workspace_dbconnect_a", "dbconnect.scope_workspace", func(tx *Tx) error {
		var setting string
		if err := tx.QueryRow(ctx, `SELECT current_setting('tetral.workspace_id', true)`).Scan(&setting); err != nil {
			return err
		}
		if setting != "workspace_dbconnect_a" {
			t.Fatalf("tetral.workspace_id = %q; want workspace_dbconnect_a", setting)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM vaults WHERE id = $1`, "vlt_dbconnect_a").Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("visible vault count = %d; want 1", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithWorkspaceTx scope: %v", err)
	}
	for probe := 0; probe < 4; probe++ {
		var setting string
		if err := runtimeDB.QueryRowContext(ctx, `SELECT current_setting('tetral.workspace_id', true)`).Scan(&setting); err != nil {
			t.Fatalf("post-tx setting probe: %v", err)
		}
		if setting != "" {
			t.Fatalf("workspace setting leaked after transaction: %q", setting)
		}
	}

	sentinel := errors.New("workspace rollback")
	err = client.WithWorkspaceTx(ctx, "workspace_dbconnect_a", "dbconnect.rollback_workspace", func(tx *Tx) error {
		if _, execErr := tx.Exec(ctx,
			`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
			 VALUES ('workspace_dbconnect_a', 'vlt_dbconnect_rollback', 'rollback', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); execErr != nil {
			return execErr
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("rollback err = %v; want sentinel", err)
	}
	assertVaultCount(t, admin, "vlt_dbconnect_rollback", 0)
	for probe := 0; probe < 4; probe++ {
		var setting string
		if err := runtimeDB.QueryRowContext(ctx, `SELECT current_setting('tetral.workspace_id', true)`).Scan(&setting); err != nil {
			t.Fatalf("post-rollback setting probe: %v", err)
		}
		if setting != "" {
			t.Fatalf("workspace setting leaked after rollback: %q", setting)
		}
	}
}

func TestWithWorkspaceReadOnlyTxScopesAndRejectsWrites(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(runtimeDB)
	ctx := context.Background()
	seedWorkspaceAndVault(t, admin, "workspace_dbconnect_readonly", "vlt_dbconnect_readonly")

	err := client.WithWorkspaceReadOnlyTx(ctx, "workspace_dbconnect_readonly", "dbconnect.readonly_workspace", func(tx *Tx) error {
		var setting string
		if err := tx.QueryRow(ctx, `SELECT current_setting('tetral.workspace_id', true)`).Scan(&setting); err != nil {
			return err
		}
		if setting != "workspace_dbconnect_readonly" {
			t.Fatalf("tetral.workspace_id = %q; want workspace_dbconnect_readonly", setting)
		}
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM vaults WHERE id = $1`, "vlt_dbconnect_readonly").Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			t.Fatalf("visible vault count = %d; want 1", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithWorkspaceReadOnlyTx: %v", err)
	}

	err = client.WithWorkspaceReadOnlyTx(ctx, "workspace_dbconnect_readonly", "dbconnect.readonly_reject_write", func(tx *Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
			 VALUES ('workspace_dbconnect_readonly', 'vlt_dbconnect_readonly_write', 'blocked', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`)
		return err
	})
	if err == nil {
		t.Fatal("read-only workspace transaction accepted a write")
	}
	assertVaultCount(t, admin, "vlt_dbconnect_readonly_write", 0)
	for probe := 0; probe < 4; probe++ {
		var setting string
		if err := runtimeDB.QueryRowContext(ctx, `SELECT current_setting('tetral.workspace_id', true)`).Scan(&setting); err != nil {
			t.Fatalf("post-readonly setting probe: %v", err)
		}
		if setting != "" {
			t.Fatalf("workspace setting leaked after read-only transaction: %q", setting)
		}
	}
}

func TestWithWorkspaceReadOnlyRepeatableReadTxUsesFrozenReadOnlySnapshot(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(runtimeDB)
	ctx := context.Background()
	seedWorkspaceAndVault(t, admin, "workspace_dbconnect_frozen", "vlt_dbconnect_frozen")

	err := client.WithWorkspaceReadOnlyRepeatableReadTx(ctx, "workspace_dbconnect_frozen", "dbconnect.frozen_workspace", func(tx *Tx) error {
		var isolation string
		var readOnly bool
		if err := tx.QueryRow(ctx, `SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')::boolean`).Scan(&isolation, &readOnly); err != nil {
			return err
		}
		if isolation != "repeatable read" || !readOnly {
			t.Fatalf("transaction mode = %q/%t; want repeatable read/read-only", isolation, readOnly)
		}
		var before int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM vaults`).Scan(&before); err != nil {
			return err
		}
		seedWorkspaceAndVault(t, admin, "workspace_dbconnect_frozen", "vlt_dbconnect_frozen_after_snapshot")
		var after int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM vaults`).Scan(&after); err != nil {
			return err
		}
		if before != 1 || after != before {
			t.Fatalf("repeatable-read vault counts = %d then %d; want frozen 1/1", before, after)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WithWorkspaceReadOnlyRepeatableReadTx: %v", err)
	}
}

func TestWithWorkspaceTxAndCleanupSemantics(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := newTestClient(runtimeDB)
	ctx := context.Background()
	seedWorkspaceAndVault(t, admin, "workspace_cleanup", "vlt_cleanup_seed")

	cleanupCalls := 0
	sentinel := errors.New("callback failure")
	err := client.WithWorkspaceTxAndCleanup(ctx, "workspace_cleanup", "dbconnect.cleanup_callback",
		func(_ *Tx) error { return sentinel },
		func() { cleanupCalls++ },
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback err = %v; want sentinel", err)
	}
	if cleanupCalls != 0 {
		t.Fatalf("cleanup calls on callback failure = %d; want 0", cleanupCalls)
	}

	err = client.WithWorkspaceTxAndCleanup(ctx, "workspace_cleanup", "dbconnect.cleanup_success",
		func(tx *Tx) error {
			_, execErr := tx.Exec(ctx,
				`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
				 VALUES ('workspace_cleanup', 'vlt_cleanup_success', 'success', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`)
			return execErr
		},
		func() { cleanupCalls++ },
	)
	if err != nil {
		t.Fatalf("cleanup success transaction: %v", err)
	}
	if cleanupCalls != 0 {
		t.Fatalf("cleanup calls on commit success = %d; want 0", cleanupCalls)
	}

	if _, err := admin.ExecContext(ctx, `CREATE TABLE cleanup_probe (
		id INT PRIMARY KEY,
		other_id INT NOT NULL,
		FOREIGN KEY (other_id) REFERENCES cleanup_probe(id) DEFERRABLE INITIALLY DEFERRED
	)`); err != nil {
		t.Fatalf("create cleanup probe: %v", err)
	}
	var runtimeRole string
	if err := runtimeDB.QueryRowContext(ctx, `SELECT current_user`).Scan(&runtimeRole); err != nil {
		t.Fatalf("read clone runtime role: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `GRANT INSERT, SELECT, REFERENCES ON cleanup_probe TO `+pgx.Identifier{runtimeRole}.Sanitize()); err != nil {
		t.Fatalf("grant cleanup probe: %v", err)
	}
	err = client.WithWorkspaceTxAndCleanup(ctx, "workspace_cleanup", "dbconnect.cleanup_commit_failure",
		func(tx *Tx) error {
			_, execErr := tx.Exec(ctx, `INSERT INTO cleanup_probe (id, other_id) VALUES (1, 999)`)
			return execErr
		},
		func() { cleanupCalls++ },
	)
	if err == nil {
		t.Fatal("expected deferred constraint commit failure")
	}
	if cleanupCalls != 1 {
		t.Fatalf("cleanup calls after commit failure = %d; want 1", cleanupCalls)
	}
}

func TestTransactionCodeUsesDatabaseSQLTransactions(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(thisFile)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dbconnect dir: %v", err)
	}
	var source strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		body, err := os.ReadFile(path) //nolint:gosec // test reads local source
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		source.Write(body)
		assertNoTransactionLifecycleSQLLiterals(t, path)
	}
	allProductionSource := source.String()
	for _, required := range []string{".BeginTx(", ".Commit(", ".Rollback("} {
		if !strings.Contains(allProductionSource, required) {
			t.Fatalf("dbconnect production source missing database/sql transaction call %s", required)
		}
	}
}

func assertNoTransactionLifecycleSQLLiterals(t *testing.T, path string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		lit, ok := node.(*ast.BasicLit)
		if !ok {
			return true
		}
		value, err := literalString(lit)
		if err != nil {
			return true
		}
		normalized := strings.ToLower(strings.TrimSpace(value))
		for _, forbidden := range []string{"begin", "commit", "rollback"} {
			if normalized == forbidden || strings.HasPrefix(normalized, forbidden+" ") || strings.HasPrefix(normalized, forbidden+";") {
				t.Fatalf("%s contains hand-written transaction lifecycle SQL literal %q", path, value)
			}
		}
		return true
	})
}

func literalString(lit *ast.BasicLit) (string, error) {
	if lit.Kind != token.STRING {
		return "", errors.New("not a string literal")
	}
	return strconv.Unquote(lit.Value)
}

func assertWorkspaceCount(t *testing.T, db *sql.DB, workspaceID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM workspaces WHERE id = $1`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count workspace %s: %v", workspaceID, err)
	}
	if count != want {
		t.Fatalf("workspace %s count = %d; want %d", workspaceID, count, want)
	}
}

func assertVaultCount(t *testing.T, db *sql.DB, vaultID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM vaults WHERE id = $1`, vaultID).Scan(&count); err != nil {
		t.Fatalf("count vault %s: %v", vaultID, err)
	}
	if count != want {
		t.Fatalf("vault %s count = %d; want %d", vaultID, count, want)
	}
}

func seedWorkspaceAndVault(t *testing.T, admin *sql.DB, workspaceID string, vaultID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z') ON CONFLICT DO NOTHING`,
		workspaceID); err != nil {
		t.Fatalf("seed workspace %s: %v", workspaceID, err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
		 VALUES ($1, $2, $2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID, vaultID); err != nil {
		t.Fatalf("seed vault %s: %v", vaultID, err)
	}
}
