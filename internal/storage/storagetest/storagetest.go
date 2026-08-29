// Package storagetest provisions isolated PostgreSQL databases for Engine
// tests. Ordinary tests clone one immutable, schema-checksum-bound template;
// migration tests receive an empty database. Every clone has a unique login,
// private CONNECT capability, bounded cleanup, and runtime/admin pools derived
// from the same in-memory clone handle.
package storagetest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/database"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/catalogtest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	EnvTestDatabaseURL = "TETRAL_TEST_DATABASE_URL"
	EnvTestRunID       = "TETRAL_TEST_RUN_ID"
	EnvTestProvenance  = "TETRAL_TEST_POSTGRES_PROVENANCE"

	helperPoolMaxOpen = 4
	helperFormat      = "postgresql-clone-v1"
	templatePrefix    = "tetral_test_template_"
	clonePrefix       = "tetral_test_"
	rolePrefix        = "tetral_test_role_"
	registryLease     = 10 * time.Minute
	heartbeatInterval = 30 * time.Second
	testSeedName      = "Default Test Fixture"
	testSeedCreatedAt = "2026-01-01T00:00:00Z"
	cloneRoleFlags    = "LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT"
	testSeedStatement = "INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $2, $3) ON CONFLICT (id) DO NOTHING"

	templateAdvisoryLockKey int64 = 0x7465_7472_616c_5450 // "tetralTP"
	registryAdvisoryLockKey int64 = 0x7465_7472_616c_5247 // "tetralRG"
)

type baselineInputs struct {
	helperFormat       string
	schemaChecksum     string
	postgresqlContract string
	roleContract       string
	seed               string
	cloneRole          string
	cloneGrants        string
	owner              string
	serverVersion      string
	provenance         string
}

const (
	runRegistryTable   = "tetral_test_control_runs"
	cloneRegistryTable = "tetral_test_clone_registry"
)

var (
	processRunOnce sync.Once
	processRunID   string
	processRunErr  error
	processStop    chan struct{}
	templateOnce   sync.Once
	templateName   string
	templateDigest string
	templateErr    error

	handleMu sync.RWMutex
	handles  = map[*sql.DB]*cloneHandle{}

	heartbeatMu  sync.Mutex
	heartbeatErr error
)

type cloneHandle struct {
	database      string
	runtimeRole   string
	runtimeConfig *pgx.ConnConfig
	adminConfig   *pgx.ConnConfig
	runtimeURL    string
	adminURL      string
}

type cloneRegistration struct {
	database      string
	role          string
	runID         string
	baseline      string
	phase         string
	databaseOwner string
	databaseNote  string
	roleNote      string
}

type cleanupExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const (
	clonePhaseReserved = "reserved"
	clonePhaseRole     = "role_ready"
	clonePhaseDatabase = "database_ready"
	clonePhaseReady    = "ready"
	cloneCleanupPrefix = "cleanup_"
)

type provisionedDatabase struct {
	runtime *sql.DB
	admin   *sql.DB
	handle  *cloneHandle
	cleanup func() error
}

// NewPostgreSQLDB returns a runtime-role pool for one private database cloned
// from the immutable Engine test template. The role is LOGIN, NOSUPERUSER, and
// NOBYPASSRLS, so FORCE-RLS tests exercise the production security posture.
func NewPostgreSQLDB(t testing.TB) *sql.DB {
	t.Helper()
	runtime, _ := newPostgreSQLDBPair(t, false)
	return runtime
}

// NewPostgreSQLDBWithAdmin returns runtime and migration-owner pools for the
// same private clone. The admin pool exists only for setup that must cross RLS.
func NewPostgreSQLDBWithAdmin(t testing.TB) (runtime, admin *sql.DB) {
	t.Helper()
	return newPostgreSQLDBPair(t, true)
}

// NewPostgreSQLAdminDB returns the admin pool for one initialized clone.
func NewPostgreSQLAdminDB(t testing.TB) *sql.DB {
	t.Helper()
	_, admin := newPostgreSQLDBPair(t, true)
	return admin
}

// PrepareTemplate initializes the same immutable template used by ordinary
// tests and returns its content digest. Repository test infrastructure calls it
// once before package execution so template setup has its own measured phase.
func PrepareTemplate(ctx context.Context, controlDSN string) (string, error) {
	config, err := pgx.ParseConfig(controlDSN)
	if err != nil {
		return "", &DatabaseSetupError{Stage: "parse_control_dsn"}
	}
	control := openPool(config)
	defer func() { _ = control.Close() }()
	if err := control.PingContext(ctx); err != nil {
		return "", &DatabaseSetupError{Stage: "connect_control_database"}
	}
	_, digest, err := ensureProcessTemplate(ctx, control, config)
	return digest, err
}

// NewEmptyPostgreSQLAdminDB creates an independent empty database. Migration
// tests intentionally bypass template reuse so they exercise the production
// migrator from the exact pre-migration state they declare.
func NewEmptyPostgreSQLAdminDB(t testing.TB) *sql.DB {
	t.Helper()
	_, admin := newPostgreSQLDBPairWithInitialization(t, true, false)
	return admin
}

// OpenRuntimeRoleDBWithTracer derives a secondary runtime pool from source's
// clone handle. It never reparses the cluster-control DSN, so database, role,
// credential, and connection identity cannot drift from the owning clone.
func OpenRuntimeRoleDBWithTracer(t testing.TB, source *sql.DB, tracer pgx.QueryTracer) *sql.DB {
	t.Helper()
	handleMu.RLock()
	handle := handles[source]
	handleMu.RUnlock()
	if handle == nil {
		t.Fatal("storagetest: source database is not owned by a live clone handle")
		return nil
	}
	config := handle.runtimeConfig.Copy()
	config.Tracer = tracer
	db := openPool(config)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		t.Fatal("storagetest: open traced runtime-role database")
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// RuntimeDatabaseURL returns the runtime credential derived from source's
// clone handle for a test-owned child process. Callers must pass it only through
// the child environment; it is never a diagnostic or result-artifact value.
func RuntimeDatabaseURL(t testing.TB, source *sql.DB) string {
	t.Helper()
	handleMu.RLock()
	handle := handles[source]
	handleMu.RUnlock()
	if handle == nil {
		t.Fatal("storagetest: source database is not owned by a live clone handle")
		return ""
	}
	return handle.runtimeURL
}

// AdminDatabaseURL returns the migration-capable credential from source's
// clone handle for a test-owned child process. It is intentionally separate
// from RuntimeDatabaseURL so compositions cannot accidentally promote a
// serving-role connection into a schema-owner connection.
func AdminDatabaseURL(t testing.TB, source *sql.DB) string {
	t.Helper()
	handleMu.RLock()
	handle := handles[source]
	handleMu.RUnlock()
	if handle == nil {
		t.Fatal("storagetest: source database is not owned by a live clone handle")
		return ""
	}
	return handle.adminURL
}

func newPostgreSQLDBPair(t testing.TB, withAdmin bool) (runtime, admin *sql.DB) {
	return newPostgreSQLDBPairWithInitialization(t, withAdmin, true)
}

func newPostgreSQLDBPairWithInitialization(t testing.TB, withAdmin, initialize bool) (runtime, admin *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	provisioned, err := provisionDatabase(ctx, initialize)
	if err != nil {
		t.Fatalf("storagetest.NewPostgreSQLDB: %v", err)
	}
	registerHandle(provisioned.runtime, provisioned.handle)
	registerHandle(provisioned.admin, provisioned.handle)
	t.Cleanup(func() {
		unregisterHandle(provisioned.runtime)
		unregisterHandle(provisioned.admin)
		if cleanupErr := provisioned.cleanup(); cleanupErr != nil {
			t.Errorf("storagetest.NewPostgreSQLDB cleanup: %v", cleanupErr)
		}
		if heartbeatErr := processHeartbeatError(); heartbeatErr != nil {
			t.Errorf("storagetest PostgreSQL run heartbeat: %v", heartbeatErr)
		}
	})
	if !withAdmin {
		return provisioned.runtime, nil
	}
	return provisioned.runtime, provisioned.admin
}

// openIsolatedPostgreSQLDB remains a narrow error-returning entry point for
// helper contract tests. Ordinary production tests use NewPostgreSQLDB.
func openIsolatedPostgreSQLDB(ctx context.Context) (*sql.DB, func() error, error) {
	provisioned, err := provisionDatabase(ctx, true)
	if err != nil {
		return nil, nil, err
	}
	registerHandle(provisioned.runtime, provisioned.handle)
	return provisioned.runtime, func() error {
		unregisterHandle(provisioned.runtime)
		return provisioned.cleanup()
	}, nil
}

func provisionDatabase(ctx context.Context, initialize bool) (*provisionedDatabase, error) {
	if err := processHeartbeatError(); err != nil {
		return nil, &DatabaseSetupError{Stage: "run_heartbeat_failed"}
	}
	controlConfig, err := parseControlConfig()
	if err != nil {
		return nil, err
	}
	runID, err := ensureProcessRun(ctx, controlConfig)
	if err != nil {
		return nil, err
	}

	controlDB := openPool(controlConfig)
	if err := controlDB.PingContext(ctx); err != nil {
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "connect_control_database"}
	}
	cloneTemplate := ""
	baselineDigest := "empty"
	if initialize {
		cloneTemplate, baselineDigest, err = ensureProcessTemplate(ctx, controlDB, controlConfig)
		if err != nil {
			_ = controlDB.Close()
			return nil, err
		}
	}

	cloneID, err := randomHex(6)
	if err != nil {
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "random_clone_identity"}
	}
	databaseName := clonePrefix + runID[:8] + "_" + cloneID
	roleName := rolePrefix + runID[:8] + "_" + cloneID
	password, err := randomHex(24)
	if err != nil {
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "random_clone_credential", Database: databaseName}
	}

	registration, err := reserveClone(ctx, controlDB, runID, baselineDigest, databaseName, roleName)
	if err != nil {
		_ = controlDB.Close()
		return nil, err
	}
	if err := createCloneRole(ctx, controlDB, registration, password); err != nil {
		_ = removeCloneRegistration(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, err
	}
	registration.phase = clonePhaseRole
	if err := createDatabaseClone(ctx, controlDB, databaseName, cloneTemplate); err != nil {
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, err
	}
	if err := grantCloneDatabasePrivileges(ctx, controlDB, registration); err != nil {
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, err
	}
	registration.phase = clonePhaseDatabase

	adminConfig := controlConfig.Copy()
	adminConfig.Database = databaseName
	adminDB := openPool(adminConfig)
	if err := adminDB.PingContext(ctx); err != nil {
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "connect_clone_admin", Database: databaseName}
	}
	if err := grantClonePrivileges(ctx, controlDB, adminDB, registration); err != nil {
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, err
	}
	registration.phase = clonePhaseReady
	if err := updateClonePhase(ctx, controlDB, registration); err != nil {
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, err
	}

	runtimeConfig := adminConfig.Copy()
	runtimeConfig.User = roleName
	runtimeConfig.Password = password
	runtimeURL, err := connectionURLWithIdentity(adminConfig.ConnString(), databaseName, roleName, password)
	if err != nil {
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "derive_clone_runtime_url", Database: databaseName}
	}
	adminURL, err := connectionURLWithIdentity(controlConfig.ConnString(), databaseName, controlConfig.User, controlConfig.Password)
	if err != nil {
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "derive_clone_admin_url", Database: databaseName}
	}
	runtimeDB := openPool(runtimeConfig)
	if err := runtimeDB.PingContext(ctx); err != nil {
		_ = runtimeDB.Close()
		_ = adminDB.Close()
		_ = cleanupRegisteredClone(context.Background(), controlDB, registration)
		_ = controlDB.Close()
		return nil, &DatabaseSetupError{Stage: "connect_clone_runtime", Database: databaseName}
	}

	handle := &cloneHandle{
		database:      databaseName,
		runtimeRole:   roleName,
		runtimeConfig: runtimeConfig,
		adminConfig:   adminConfig,
		runtimeURL:    runtimeURL,
		adminURL:      adminURL,
	}
	cleanup := func() error {
		_ = runtimeDB.Close()
		_ = adminDB.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err := cleanupRegisteredClone(cleanupCtx, controlDB, registration)
		_ = controlDB.Close()
		return err
	}
	return &provisionedDatabase{runtime: runtimeDB, admin: adminDB, handle: handle, cleanup: cleanup}, nil
}

func connectionURLWithIdentity(source, databaseName, user, password string) (string, error) {
	parsed, err := url.Parse(source)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Host == "" {
		return "", fmt.Errorf("PostgreSQL test DSN must use URL form")
	}
	parsed.User = url.UserPassword(user, password)
	parsed.Path = "/" + databaseName
	return parsed.String(), nil
}

func parseControlConfig() (*pgx.ConnConfig, error) {
	dsn := os.Getenv(EnvTestDatabaseURL)
	if dsn == "" {
		return nil, &MissingTestDSNError{EnvVarName: EnvTestDatabaseURL}
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, &MalformedTestDSNError{EnvVarName: EnvTestDatabaseURL}
	}
	return config, nil
}

func openPool(config *pgx.ConnConfig) *sql.DB {
	db := sql.OpenDB(stdlib.GetConnector(*config))
	db.SetMaxOpenConns(helperPoolMaxOpen)
	db.SetMaxIdleConns(helperPoolMaxOpen)
	return db
}

func ensureProcessRun(ctx context.Context, config *pgx.ConnConfig) (string, error) {
	processRunOnce.Do(func() {
		processRunID = os.Getenv(EnvTestRunID)
		if processRunID == "" {
			processRunID, processRunErr = randomHex(16)
			if processRunErr != nil {
				processRunErr = &DatabaseSetupError{Stage: "random_run_identity"}
				return
			}
		} else if !safeHexIdentity(processRunID, 32) {
			processRunErr = &DatabaseSetupError{Stage: "validate_run_identity"}
			return
		}
		processStop = make(chan struct{})
		db := openPool(config)
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			processRunErr = &DatabaseSetupError{Stage: "connect_run_registry"}
			return
		}
		if err := ensureRegistry(ctx, db); err != nil {
			_ = db.Close()
			processRunErr = err
			return
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO "+runRegistryTable+" (run_id, owner_pid, heartbeat_at, expires_at) VALUES ($1, $2, clock_timestamp(), clock_timestamp() + $3::interval) ON CONFLICT (run_id) DO UPDATE SET owner_pid = EXCLUDED.owner_pid, heartbeat_at = EXCLUDED.heartbeat_at, expires_at = EXCLUDED.expires_at",
			processRunID, os.Getpid(), postgresInterval(registryLease),
		); err != nil {
			_ = db.Close()
			processRunErr = &DatabaseSetupError{Stage: "register_test_run"}
			return
		}
		if err := cleanupExpiredResources(ctx, db, config); err != nil {
			_ = db.Close()
			processRunErr = err
			return
		}
		go heartbeatRun(db, processRunID, processStop)
	})
	return processRunID, processRunErr
}

func ensureProcessTemplate(ctx context.Context, controlDB *sql.DB, controlConfig *pgx.ConnConfig) (string, string, error) {
	templateOnce.Do(func() {
		templateDigest, templateErr = testBaselineDigest(ctx, controlDB)
		if templateErr != nil {
			return
		}
		templateName, templateErr = ensureTemplate(ctx, controlDB, controlConfig, templateDigest)
	})
	return templateName, templateDigest, templateErr
}

// CloseRun revokes every database capability registered to one runner-owned
// run. It is the normal runner teardown owner after package processes have
// exited or been killed. Direct go test processes continue to use per-test
// cleanup and expiry-based bootstrap recovery.
func CloseRun(ctx context.Context, controlDSN, runID string) error {
	if !safeHexIdentity(runID, 32) {
		return &DatabaseCleanupError{Stage: "validate_run_identity"}
	}
	config, err := pgx.ParseConfig(controlDSN)
	if err != nil {
		return &MalformedTestDSNError{EnvVarName: EnvTestDatabaseURL}
	}
	db := openPool(config)
	defer func() { _ = db.Close() }()
	if err := ensureRegistry(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, "UPDATE "+runRegistryTable+" SET expires_at=clock_timestamp()-interval '1 second' WHERE run_id=$1", runID); err != nil {
		return &DatabaseCleanupError{Stage: "expire_test_run"}
	}
	return cleanupExpiredResources(ctx, db, config)
}

func heartbeatRun(db *sql.DB, runID string, stop <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	defer func() { _ = db.Close() }()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := refreshRunLease(ctx, db, runID)
			cancel()
			if err != nil {
				recordHeartbeatError(err)
				return
			}
		case <-stop:
			return
		}
	}
}

func refreshRunLease(ctx context.Context, db *sql.DB, runID string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return &DatabaseSetupError{Stage: "connect_run_heartbeat"}
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", registryAdvisoryLockKey); err != nil {
		return &DatabaseSetupError{Stage: "lock_run_heartbeat"}
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", registryAdvisoryLockKey)
	}()
	result, err := conn.ExecContext(ctx,
		"UPDATE "+runRegistryTable+" SET heartbeat_at = clock_timestamp(), expires_at = clock_timestamp() + $2::interval WHERE run_id = $1 AND expires_at >= clock_timestamp()",
		runID, postgresInterval(registryLease),
	)
	if err != nil {
		return &DatabaseSetupError{Stage: "refresh_run_heartbeat"}
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		return &DatabaseSetupError{Stage: "lost_run_heartbeat_lease"}
	}
	return nil
}

func recordHeartbeatError(err error) {
	heartbeatMu.Lock()
	heartbeatErr = errors.Join(heartbeatErr, err)
	heartbeatMu.Unlock()
}

func processHeartbeatError() error {
	heartbeatMu.Lock()
	defer heartbeatMu.Unlock()
	return heartbeatErr
}

func ensureRegistry(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return &DatabaseSetupError{Stage: "lock_registry"}
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", registryAdvisoryLockKey); err != nil {
		return &DatabaseSetupError{Stage: "lock_registry"}
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", registryAdvisoryLockKey)
	}()
	statements := []string{
		"CREATE TABLE IF NOT EXISTS " + runRegistryTable + " (run_id text PRIMARY KEY, owner_pid integer NOT NULL, heartbeat_at timestamptz NOT NULL, expires_at timestamptz NOT NULL)",
		"CREATE TABLE IF NOT EXISTS " + cloneRegistryTable + " (database_name text PRIMARY KEY, run_id text NOT NULL, baseline_digest text NOT NULL, role_name text NOT NULL, phase text NOT NULL DEFAULT 'reserved', database_owner text NOT NULL, database_comment text NOT NULL, role_comment text NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp())",
		"ALTER TABLE " + cloneRegistryTable + " ADD COLUMN IF NOT EXISTS phase text NOT NULL DEFAULT 'reserved'",
		"ALTER TABLE " + cloneRegistryTable + " ADD COLUMN IF NOT EXISTS database_owner text NOT NULL DEFAULT ''",
		"ALTER TABLE " + cloneRegistryTable + " ADD COLUMN IF NOT EXISTS database_comment text NOT NULL DEFAULT ''",
		"ALTER TABLE " + cloneRegistryTable + " ADD COLUMN IF NOT EXISTS role_comment text NOT NULL DEFAULT ''",
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return &DatabaseSetupError{Stage: "create_registry"}
		}
	}
	return nil
}

func cleanupExpiredResources(ctx context.Context, db *sql.DB, _ *pgx.ConnConfig) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return &DatabaseSetupError{Stage: "lock_expired_cleanup"}
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", registryAdvisoryLockKey); err != nil {
		return &DatabaseSetupError{Stage: "lock_expired_cleanup"}
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", registryAdvisoryLockKey)
	}()

	rows, err := conn.QueryContext(ctx,
		"SELECT c.database_name, c.role_name, c.run_id, c.baseline_digest, c.phase, c.database_owner, c.database_comment, c.role_comment FROM "+cloneRegistryTable+" c LEFT JOIN "+runRegistryTable+" r ON r.run_id = c.run_id WHERE r.run_id IS NULL OR r.expires_at < clock_timestamp()",
	)
	if err != nil {
		return &DatabaseSetupError{Stage: "list_expired_clones"}
	}
	var expired []cloneRegistration
	for rows.Next() {
		var item cloneRegistration
		if err := rows.Scan(&item.database, &item.role, &item.runID, &item.baseline, &item.phase, &item.databaseOwner, &item.databaseNote, &item.roleNote); err != nil {
			_ = rows.Close()
			return &DatabaseSetupError{Stage: "scan_expired_clones"}
		}
		expired = append(expired, item)
	}
	if err := rows.Close(); err != nil {
		return &DatabaseSetupError{Stage: "close_expired_clone_rows"}
	}
	for _, item := range expired {
		if !safeGeneratedName(item.database, clonePrefix) || !safeGeneratedName(item.role, rolePrefix) {
			continue
		}
		if err := cleanupRegisteredClone(ctx, conn, item); err != nil {
			return err
		}
	}
	_, _ = conn.ExecContext(ctx, "DELETE FROM "+runRegistryTable+" WHERE expires_at < clock_timestamp() AND NOT EXISTS (SELECT 1 FROM "+cloneRegistryTable+" c WHERE c.run_id = "+runRegistryTable+".run_id)")
	return nil
}

func testBaselineDigest(ctx context.Context, db *sql.DB) (string, error) {
	var serverVersion, owner string
	if err := db.QueryRowContext(ctx, "SELECT current_setting('server_version_num'), current_user").Scan(&serverVersion, &owner); err != nil {
		return "", &DatabaseSetupError{Stage: "read_server_version"}
	}
	return digestBaselineInputs(baselineInputs{
		helperFormat:       helperFormat,
		schemaChecksum:     storage.PostgreSQLSchemaVersionOneChecksum,
		postgresqlContract: database.PostgreSQLContractDigest(),
		roleContract:       database.RoleContractDigest(),
		seed:               strings.Join([]string{testSeedStatement, string(workspace.DefaultID), testSeedName, testSeedCreatedAt}, "\x00"),
		cloneRole:          cloneRoleFlags,
		cloneGrants:        strings.Join(append(cloneDatabaseGrantStatements("$database", "$role", "$comment"), cloneSchemaGrantStatements("$role")...), "\n"),
		owner:              owner,
		serverVersion:      serverVersion,
		provenance:         testPostgreSQLProvenance(),
	}), nil
}

func digestBaselineInputs(inputs baselineInputs) string {
	seed := strings.Join([]string{
		inputs.helperFormat, inputs.schemaChecksum, inputs.postgresqlContract,
		inputs.roleContract, inputs.seed, inputs.cloneRole, inputs.cloneGrants,
		inputs.owner, inputs.serverVersion, inputs.provenance,
	}, "\n")
	digest := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(digest[:])
}

func testPostgreSQLProvenance() string {
	if declared := os.Getenv(EnvTestProvenance); declared != "" {
		digest := sha256.Sum256([]byte(declared))
		return "runner:" + hex.EncodeToString(digest[:])
	}
	config, err := parseControlConfig()
	if err != nil {
		return "external:unavailable"
	}
	identity := strings.Join([]string{config.Host, strconv.Itoa(int(config.Port)), config.Database}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return "external:" + hex.EncodeToString(digest[:])
}

func ensureTemplate(ctx context.Context, controlDB *sql.DB, controlConfig *pgx.ConnConfig, digest string) (string, error) {
	name := templatePrefix + digest[:20]
	finalPrefix := "tetral-test-template:" + digest + ":"
	legacyComment := "tetral-test-template:" + digest
	buildingComment := "tetral-test-template-building:" + digest
	conn, err := controlDB.Conn(ctx)
	if err != nil {
		return "", &DatabaseSetupError{Stage: "lock_template"}
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", templateAdvisoryLockKey); err != nil {
		return "", &DatabaseSetupError{Stage: "lock_template"}
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", templateAdvisoryLockKey)
	}()

	var exists, isTemplate, allowsConnections, publicConnect, unexpectedConnect bool
	var comment, owner string
	if err := conn.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1),
		       COALESCE((SELECT d.datistemplate FROM pg_database d WHERE d.datname=$1), false),
		       COALESCE((SELECT d.datallowconn FROM pg_database d WHERE d.datname=$1), false),
		       COALESCE((SELECT shobj_description(d.oid, 'pg_database') FROM pg_database d WHERE d.datname=$1), ''),
		       COALESCE((SELECT pg_get_userbyid(d.datdba) FROM pg_database d WHERE d.datname=$1), ''),
		       COALESCE((SELECT has_database_privilege('public', d.oid, 'CONNECT') FROM pg_database d WHERE d.datname=$1), false),
		       COALESCE((SELECT EXISTS (
		         SELECT 1 FROM aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a
		          WHERE a.privilege_type='CONNECT' AND a.grantee<>d.datdba
		       ) FROM pg_database d WHERE d.datname=$1), false)`, name,
	).Scan(&exists, &isTemplate, &allowsConnections, &comment, &owner, &publicConnect, &unexpectedConnect); err != nil {
		return "", &DatabaseSetupError{Stage: "inspect_template"}
	}
	metadataOwned := owner == controlConfig.User && !publicConnect && !unexpectedConnect
	if exists && isTemplate && !allowsConnections && metadataOwned && strings.HasPrefix(comment, finalPrefix) {
		expectedState := strings.TrimPrefix(comment, finalPrefix)
		actualState, err := templateCloneStateDigest(ctx, conn, controlConfig, name)
		if err == nil && actualState == expectedState {
			return name, nil
		}
	}
	if exists {
		ownedIncomplete := metadataOwned && (comment == buildingComment || comment == legacyComment || strings.HasPrefix(comment, finalPrefix))
		if !ownedIncomplete {
			return "", &DatabaseSetupError{Stage: "template_name_collision"}
		}
		if isTemplate || !allowsConnections {
			if _, err := conn.ExecContext(ctx, "ALTER DATABASE "+name+" WITH ALLOW_CONNECTIONS true IS_TEMPLATE false"); err != nil {
				return "", &DatabaseSetupError{Stage: "unseal_incomplete_template"}
			}
		}
		if _, err := conn.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)"); err != nil {
			return "", &DatabaseSetupError{Stage: "remove_incomplete_template"}
		}
	}
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		return "", &DatabaseSetupError{Stage: "create_template"}
	}
	for _, statement := range []string{
		"REVOKE CONNECT ON DATABASE " + name + " FROM PUBLIC",
		"COMMENT ON DATABASE " + name + " IS " + quoteLiteral(buildingComment),
	} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			_, _ = conn.ExecContext(context.Background(), "DROP DATABASE "+name+" WITH (FORCE)")
			return "", &DatabaseSetupError{Stage: "claim_template"}
		}
	}
	templateConfig := controlConfig.Copy()
	templateConfig.Database = name
	templateDB := openPool(templateConfig)
	if err := storage.MigrateSchema(ctx, templateDB); err != nil {
		_ = templateDB.Close()
		_, _ = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		return "", err
	}
	if err := seedTestDefaultWorkspace(ctx, templateDB); err != nil {
		_ = templateDB.Close()
		_, _ = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		return "", &DatabaseSetupError{Stage: "seed_template"}
	}
	stateDigest, err := templateStateDigest(ctx, templateDB)
	if err != nil {
		_ = templateDB.Close()
		_, _ = conn.ExecContext(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		return "", &DatabaseSetupError{Stage: "digest_template_state"}
	}
	_ = templateDB.Close()
	statements := []string{
		"COMMENT ON DATABASE " + name + " IS " + quoteLiteral(finalPrefix+stateDigest),
		"ALTER DATABASE " + name + " WITH ALLOW_CONNECTIONS false IS_TEMPLATE true",
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ALTER DATABASE "+name+" WITH ALLOW_CONNECTIONS true IS_TEMPLATE false")
			_, _ = conn.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			return "", &DatabaseSetupError{Stage: "seal_template"}
		}
	}
	return name, nil
}

func templateCloneStateDigest(ctx context.Context, control cleanupExecutor, controlConfig *pgx.ConnConfig, template string) (string, error) {
	suffix, err := randomHex(6)
	if err != nil {
		return "", err
	}
	probe := clonePrefix + "template_probe_" + suffix
	if _, err := control.ExecContext(ctx, "CREATE DATABASE "+probe+" TEMPLATE "+template); err != nil {
		return "", err
	}
	defer func() { _, _ = control.ExecContext(context.Background(), "DROP DATABASE "+probe+" WITH (FORCE)") }()
	config := controlConfig.Copy()
	config.Database = probe
	db := openPool(config)
	defer func() { _ = db.Close() }()
	if err := db.PingContext(ctx); err != nil {
		return "", err
	}
	return templateStateDigest(ctx, db)
}

func templateStateDigest(ctx context.Context, db *sql.DB) (string, error) {
	catalog, err := catalogtest.Snapshot(ctx, db)
	if err != nil {
		return "", err
	}
	var workspaceType, workspaceName string
	var workspaceCreated time.Time
	if err := db.QueryRowContext(ctx, "SELECT type, name, created_at FROM workspaces WHERE id=$1", workspace.DefaultID).Scan(&workspaceType, &workspaceName, &workspaceCreated); err != nil {
		return "", err
	}
	rows, err := db.QueryContext(ctx, "SELECT version, checksum FROM tetral_schema_migrations ORDER BY version")
	if err != nil {
		return "", err
	}
	var migrations [][2]string
	for rows.Next() {
		var version int
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			_ = rows.Close()
			return "", err
		}
		migrations = append(migrations, [2]string{strconv.Itoa(version), checksum})
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		Catalog    json.RawMessage `json:"catalog"`
		Workspace  [3]string       `json:"workspace"`
		Migrations [][2]string     `json:"migrations"`
	}{
		Catalog:    catalog,
		Workspace:  [3]string{workspaceType, workspaceName, workspaceCreated.UTC().Format(time.RFC3339Nano)},
		Migrations: migrations,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func createCloneRole(ctx context.Context, db *sql.DB, registration cloneRegistration, password string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return &DatabaseSetupError{Stage: "begin_clone_role", Database: registration.database}
	}
	defer func() { _ = tx.Rollback() }()
	statement := fmt.Sprintf("CREATE ROLE %s %s PASSWORD %s", registration.role, cloneRoleFlags, quoteLiteral(password))
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return &DatabaseSetupError{Stage: "create_clone_role", Database: registration.database}
	}
	if _, err := tx.ExecContext(ctx, "COMMENT ON ROLE "+registration.role+" IS "+quoteLiteral(registration.roleNote)); err != nil {
		return &DatabaseSetupError{Stage: "comment_clone_role", Database: registration.database}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+cloneRegistryTable+" SET phase=$4 WHERE database_name=$1 AND run_id=$2 AND role_name=$3 AND phase=$5", registration.database, registration.runID, registration.role, clonePhaseRole, clonePhaseReserved); err != nil {
		return &DatabaseSetupError{Stage: "record_clone_role", Database: registration.database}
	}
	if err := tx.Commit(); err != nil {
		return &DatabaseSetupError{Stage: "commit_clone_role", Database: registration.database}
	}
	return nil
}

func createDatabaseClone(ctx context.Context, db *sql.DB, databaseName, templateName string) error {
	// PostgreSQL does not parameterize identifiers; pgx.Identifier quotes both generated names.
	//nolint:gosec
	statement := "CREATE DATABASE " + pgx.Identifier{databaseName}.Sanitize()
	if templateName != "" {
		statement += " TEMPLATE " + pgx.Identifier{templateName}.Sanitize()
	}
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return &DatabaseSetupError{Stage: "create_clone_database", Database: databaseName}
	}
	return nil
}

func grantCloneDatabasePrivileges(ctx context.Context, controlDB *sql.DB, registration cloneRegistration) error {
	databaseName := registration.database
	roleName := registration.role
	tx, err := controlDB.BeginTx(ctx, nil)
	if err != nil {
		return &DatabaseSetupError{Stage: "begin_clone_database_grants", Database: databaseName}
	}
	defer func() { _ = tx.Rollback() }()
	statements := cloneDatabaseGrantStatements(databaseName, roleName, registration.databaseNote)
	statements = append(statements, "UPDATE "+cloneRegistryTable+" SET phase="+quoteLiteral(clonePhaseDatabase)+" WHERE database_name="+quoteLiteral(databaseName)+" AND run_id="+quoteLiteral(registration.runID)+" AND role_name="+quoteLiteral(roleName)+" AND phase="+quoteLiteral(clonePhaseRole))
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return &DatabaseSetupError{Stage: "grant_clone_privileges", Database: databaseName}
		}
	}
	if err := tx.Commit(); err != nil {
		return &DatabaseSetupError{Stage: "commit_clone_database_grants", Database: databaseName}
	}
	return nil
}

func grantClonePrivileges(ctx context.Context, _ *sql.DB, adminDB *sql.DB, registration cloneRegistration) error {
	databaseName := registration.database
	roleName := registration.role
	statements := cloneSchemaGrantStatements(roleName)
	for _, statement := range statements {
		if _, err := adminDB.ExecContext(ctx, statement); err != nil {
			return &DatabaseSetupError{Stage: "grant_clone_privileges", Database: databaseName}
		}
	}
	return nil
}

func cloneDatabaseGrantStatements(databaseName, roleName, comment string) []string {
	return []string{
		"REVOKE CONNECT ON DATABASE " + databaseName + " FROM PUBLIC",
		"GRANT CONNECT ON DATABASE " + databaseName + " TO " + roleName,
		"COMMENT ON DATABASE " + databaseName + " IS " + quoteLiteral(comment),
	}
}

func cloneSchemaGrantStatements(roleName string) []string {
	return []string{
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"GRANT USAGE ON SCHEMA public TO " + roleName,
		"GRANT ALL ON ALL TABLES IN SCHEMA public TO " + roleName,
		"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO " + roleName,
	}
}

func reserveClone(ctx context.Context, db *sql.DB, runID, digest, databaseName, roleName string) (cloneRegistration, error) {
	var owner string
	if err := db.QueryRowContext(ctx, "SELECT current_user").Scan(&owner); err != nil {
		return cloneRegistration{}, &DatabaseSetupError{Stage: "read_clone_owner", Database: databaseName}
	}
	registration := cloneRegistration{
		database: databaseName, role: roleName, runID: runID, baseline: digest, phase: clonePhaseReserved, databaseOwner: owner,
		databaseNote: "tetral-test-clone:" + runID + ":" + digest + ":" + roleName,
		roleNote:     "tetral-test-role:" + runID + ":" + databaseName,
	}
	_, err := db.ExecContext(ctx,
		"INSERT INTO "+cloneRegistryTable+" (database_name, run_id, baseline_digest, role_name, database_owner, database_comment, role_comment) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		registration.database, registration.runID, registration.baseline, registration.role, registration.databaseOwner, registration.databaseNote, registration.roleNote,
	)
	if err != nil {
		return cloneRegistration{}, &DatabaseSetupError{Stage: "reserve_clone", Database: databaseName}
	}
	return registration, nil
}

func cleanupRegisteredClone(ctx context.Context, db cleanupExecutor, registration cloneRegistration) error {
	if !safeGeneratedName(registration.database, clonePrefix) || !safeGeneratedName(registration.role, rolePrefix) {
		return &DatabaseCleanupError{Database: registration.database, Stage: "validate_identity"}
	}
	authorized, err := cloneCleanupAuthorized(ctx, db, registration)
	if err != nil || !authorized {
		return &DatabaseCleanupError{Database: registration.database, Stage: "verify_authority"}
	}
	if !strings.HasPrefix(registration.phase, cloneCleanupPrefix) {
		cleanupPhase := cloneCleanupPrefix + registration.phase
		result, err := db.ExecContext(ctx,
			"UPDATE "+cloneRegistryTable+" SET phase=$6 WHERE database_name=$1 AND run_id=$2 AND role_name=$3 AND baseline_digest=$4 AND phase=$5",
			registration.database, registration.runID, registration.role, registration.baseline, registration.phase, cleanupPhase,
		)
		if err != nil {
			return &DatabaseCleanupError{Database: registration.database, Stage: "begin_cleanup"}
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return &DatabaseCleanupError{Database: registration.database, Stage: "claim_cleanup"}
		}
		registration.phase = cleanupPhase
	}
	databaseName, roleName := registration.database, registration.role
	var databaseExists, roleExists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1), EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$2)", databaseName, roleName).Scan(&databaseExists, &roleExists); err != nil {
		return &DatabaseCleanupError{Database: databaseName, Stage: "inspect_resources"}
	}
	var statements []struct{ stage, sql string }
	if roleExists {
		statements = append(statements, struct{ stage, sql string }{"revoke_login", "ALTER ROLE " + roleName + " NOLOGIN"})
	}
	if databaseExists {
		if roleExists {
			statements = append(statements, struct{ stage, sql string }{"revoke_connect", "REVOKE CONNECT ON DATABASE " + databaseName + " FROM " + roleName})
		}
		statements = append(statements,
			struct{ stage, sql string }{"revoke_public_connect", "REVOKE CONNECT ON DATABASE " + databaseName + " FROM PUBLIC"},
			struct{ stage, sql string }{"terminate_sessions", "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = " + quoteLiteral(databaseName) + " AND pid <> pg_backend_pid()"},
			struct{ stage, sql string }{"drop_database", "DROP DATABASE " + databaseName},
		)
	}
	if roleExists {
		statements = append(statements, struct{ stage, sql string }{"drop_role", "DROP ROLE " + roleName})
	}
	var cleanupErr error
	for _, item := range statements {
		if _, err := db.ExecContext(ctx, item.sql); err != nil {
			cleanupErr = errors.Join(cleanupErr, &DatabaseCleanupError{Database: databaseName, Stage: item.stage})
		}
	}
	if cleanupErr == nil {
		cleanupErr = removeCloneRegistration(ctx, db, registration)
	}
	return cleanupErr
}

func removeCloneRegistration(ctx context.Context, db cleanupExecutor, registration cloneRegistration) error {
	result, err := db.ExecContext(ctx,
		"DELETE FROM "+cloneRegistryTable+" WHERE database_name = $1 AND run_id = $2 AND role_name = $3 AND baseline_digest = $4 AND phase = $5 AND database_owner = $6 AND database_comment = $7 AND role_comment = $8",
		registration.database, registration.runID, registration.role, registration.baseline, registration.phase, registration.databaseOwner, registration.databaseNote, registration.roleNote,
	)
	if err != nil {
		return &DatabaseCleanupError{Database: registration.database, Stage: "remove_registry"}
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return &DatabaseCleanupError{Database: registration.database, Stage: "settle_registry"}
	}
	return nil
}

func cloneCleanupAuthorized(ctx context.Context, db cleanupExecutor, registration cloneRegistration) (bool, error) {
	var registryMatch bool
	err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM "+cloneRegistryTable+" WHERE database_name=$1 AND run_id=$2 AND role_name=$3 AND baseline_digest=$4 AND phase=$5 AND database_owner=$6 AND database_comment=$7 AND role_comment=$8)",
		registration.database, registration.runID, registration.role, registration.baseline, registration.phase, registration.databaseOwner, registration.databaseNote, registration.roleNote,
	).Scan(&registryMatch)
	if err != nil || !registryMatch {
		return false, err
	}

	originalPhase := strings.TrimPrefix(registration.phase, cloneCleanupPrefix)
	cleanupStarted := originalPhase != registration.phase
	if originalPhase != clonePhaseReserved && originalPhase != clonePhaseRole && originalPhase != clonePhaseDatabase && originalPhase != clonePhaseReady {
		return false, nil
	}
	var databaseExists, databaseSealed, databaseUnsealed, databaseCleanupOwned, databaseRoleCleanupOwned bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1),
		       COALESCE((SELECT shobj_description(d.oid, 'pg_database')=$2
		                       AND pg_get_userbyid(d.datdba)=$3
		                       AND NOT has_database_privilege('public', d.datname, 'CONNECT')
		                       AND has_database_privilege($4, d.datname, 'CONNECT')
		                       AND NOT EXISTS (
		                         SELECT 1 FROM aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a
		                          WHERE a.privilege_type='CONNECT'
		                            AND a.grantee NOT IN (d.datdba, (SELECT oid FROM pg_roles WHERE rolname=$4))
		                       )
		                  FROM pg_database d WHERE d.datname=$1), false),
			       COALESCE((SELECT shobj_description(d.oid, 'pg_database') IS NULL
		                       AND pg_get_userbyid(d.datdba)=$3
		                       AND has_database_privilege('public', d.datname, 'CONNECT')
		                       AND NOT EXISTS (
		                         SELECT 1 FROM aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a
		                          WHERE a.grantee <> d.datdba AND a.grantee <> 0
		                       )
			                  FROM pg_database d WHERE d.datname=$1), false),
			       COALESCE((SELECT shobj_description(d.oid, 'pg_database')=$2
			                       AND pg_get_userbyid(d.datdba)=$3
			                       AND NOT has_database_privilege('public', d.datname, 'CONNECT')
			                       AND NOT EXISTS (
			                         SELECT 1 FROM aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a
			                          WHERE a.privilege_type='CONNECT'
			                            AND a.grantee NOT IN (d.datdba, (SELECT oid FROM pg_roles WHERE rolname=$4))
			                       )
			                  FROM pg_database d WHERE d.datname=$1), false),
			       COALESCE((SELECT shobj_description(d.oid, 'pg_database') IS NULL
			                       AND pg_get_userbyid(d.datdba)=$3
			                       AND NOT EXISTS (
			                         SELECT 1 FROM aclexplode(COALESCE(d.datacl, acldefault('d', d.datdba))) a
			                          WHERE a.grantee NOT IN (d.datdba, 0, (SELECT oid FROM pg_roles WHERE rolname=$4))
			                       )
			                  FROM pg_database d WHERE d.datname=$1), false)`,
		registration.database, registration.databaseNote, registration.databaseOwner, registration.role,
	).Scan(&databaseExists, &databaseSealed, &databaseUnsealed, &databaseCleanupOwned, &databaseRoleCleanupOwned)
	if err != nil {
		return false, err
	}
	var roleExists, roleReady, roleOwned bool
	err = db.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=$1),
			       COALESCE((SELECT shobj_description(r.oid, 'pg_authid')=$2
			                       AND r.rolcanlogin AND NOT r.rolsuper AND NOT r.rolbypassrls
			                  FROM pg_roles r WHERE r.rolname=$1), false),
			       COALESCE((SELECT shobj_description(r.oid, 'pg_authid')=$2
			                       AND NOT r.rolsuper AND NOT r.rolbypassrls
			                  FROM pg_roles r WHERE r.rolname=$1), false)`,
		registration.role, registration.roleNote,
	).Scan(&roleExists, &roleReady, &roleOwned)
	if err != nil {
		return false, err
	}
	if cleanupStarted {
		switch originalPhase {
		case clonePhaseReserved:
			return !databaseExists && !roleExists, nil
		case clonePhaseRole:
			return (!roleExists || roleOwned) && (!databaseExists || databaseUnsealed || databaseSealed || databaseCleanupOwned || databaseRoleCleanupOwned), nil
		case clonePhaseDatabase, clonePhaseReady:
			return (!roleExists || roleOwned) && (!databaseExists || databaseSealed || databaseCleanupOwned), nil
		}
	}
	switch originalPhase {
	case clonePhaseReserved:
		return !databaseExists && !roleExists, nil
	case clonePhaseRole:
		return roleExists && roleReady && (!databaseExists || databaseUnsealed), nil
	case clonePhaseDatabase, clonePhaseReady:
		return databaseExists && databaseSealed && roleExists && roleReady, nil
	default:
		return false, nil
	}
}

func updateClonePhase(ctx context.Context, db *sql.DB, registration cloneRegistration) error {
	result, err := db.ExecContext(ctx,
		"UPDATE "+cloneRegistryTable+" SET phase=$4 WHERE database_name=$1 AND run_id=$2 AND role_name=$3 AND phase=$5",
		registration.database, registration.runID, registration.role, registration.phase, clonePhaseDatabase,
	)
	if err != nil {
		return &DatabaseSetupError{Stage: "record_clone_ready", Database: registration.database}
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return &DatabaseSetupError{Stage: "settle_clone_ready", Database: registration.database}
	}
	return nil
}

func seedTestDefaultWorkspace(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, testSeedStatement, workspace.DefaultID, testSeedName, testSeedCreatedAt)
	return err
}

func registerHandle(db *sql.DB, handle *cloneHandle) {
	if db == nil {
		return
	}
	handleMu.Lock()
	handles[db] = handle
	handleMu.Unlock()
}

func unregisterHandle(db *sql.DB) {
	if db == nil {
		return
	}
	handleMu.Lock()
	delete(handles, db)
	handleMu.Unlock()
}

func randomHex(byteCount int) (string, error) {
	raw := make([]byte, byteCount)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func safeGeneratedName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) || len(name) <= len(prefix) || len(name) > 63 {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func safeHexIdentity(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func postgresInterval(duration time.Duration) string {
	return strconv.FormatInt(int64(duration/time.Second), 10) + " seconds"
}

type MissingTestDSNError struct{ EnvVarName string }

func (e *MissingTestDSNError) Error() string {
	return fmt.Sprintf("storagetest: %s is not set; provide a real PostgreSQL administrative test DSN (CI provides it automatically)", e.EnvVarName)
}

type MalformedTestDSNError struct{ EnvVarName string }

func (e *MalformedTestDSNError) Error() string {
	return fmt.Sprintf("storagetest: %s is malformed; raw error suppressed", e.EnvVarName)
}

type DatabaseSetupError struct {
	Stage    string
	Database string
}

func (e *DatabaseSetupError) Error() string {
	if e.Database == "" {
		return fmt.Sprintf("storagetest: database setup failed at stage %q", e.Stage)
	}
	return fmt.Sprintf("storagetest: database setup failed at stage %q for database %q", e.Stage, e.Database)
}

type DatabaseCleanupError struct {
	Database string
	Stage    string
}

func (e *DatabaseCleanupError) Error() string {
	return fmt.Sprintf("storagetest: database cleanup failed at stage %q for database %q", e.Stage, e.Database)
}
