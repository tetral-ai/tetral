package storage_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/catalogtest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const (
	stageABaselineCommit = "0a82afb360959147cb0f6f13e7095d410a612c44"
	stageABaselineTree   = "767408b04eab8e7a11f09814ad2a164a0de2dd47"
)

// Upgrade a database created by the historical source through the real migrator.
// Fresh and upgraded catalogs must match exactly, including column positions,
// constraints, dependencies, RLS and privileges. The old helper snapshot is
// independently extended by the explicit Git identity delta below.
func TestGitIdentityMigrationPreservesDataAndMatchesFreshCatalog(t *testing.T) {
	controlDSN := os.Getenv(storagetest.EnvTestDatabaseURL)
	if controlDSN == "" {
		t.Skip("TETRAL_TEST_DATABASE_URL is required for migration catalog equivalence")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	assertGitObject(ctx, t, stageABaselineCommit+"^{commit}", stageABaselineCommit)
	assertGitObject(ctx, t, stageABaselineCommit+"^{tree}", stageABaselineTree)
	repositoryRootCommand := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel")
	repositoryRootOutput, err := repositoryRootCommand.Output()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot := strings.TrimSpace(string(repositoryRootOutput))

	baselineRoot := filepath.Join(t.TempDir(), "baseline")
	if err := os.MkdirAll(baselineRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(t.TempDir(), "baseline.tar")
	archiveFile, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive := exec.CommandContext(ctx, "git", "archive", stageABaselineCommit)
	archive.Dir = repositoryRoot
	archive.Stdout = archiveFile
	if err := archive.Run(); err != nil {
		_ = archiveFile.Close()
		t.Fatal(err)
	}
	if err := archiveFile.Close(); err != nil {
		t.Fatal(err)
	}
	tar := exec.CommandContext(ctx, "tar", "-xf", archivePath, "-C", baselineRoot)
	if output, err := tar.CombinedOutput(); err != nil {
		t.Fatalf("extract exact baseline archive: %v\n%s", err, output)
	}
	if _, err := os.Stat(filepath.Join(baselineRoot, "go.mod")); err != nil {
		t.Fatal(err)
	}
	copyCatalogHelper(t, baselineRoot)

	baselineDSN, baselineCleanup := freshCatalogDatabase(ctx, t, controlDSN)
	defer baselineCleanup()
	currentDSN, currentCleanup := freshCatalogDatabase(ctx, t, controlDSN)
	defer currentCleanup()
	baselinePath := filepath.Join(t.TempDir(), "baseline-catalog.json")
	baselineHelperPath := filepath.Join(t.TempDir(), "baseline-helper.json")
	writeBaselineSnapshotTest(t, baselineRoot)
	command := exec.CommandContext(ctx, "go", "test", "./internal/storage", "-run", "^TestWriteStageABaselineCatalog$", "-count=1")
	command.Dir = baselineRoot
	command.Env = append(os.Environ(),
		"TETRAL_STAGE_A_CATALOG_DSN="+baselineDSN,
		"TETRAL_STAGE_A_CATALOG_OUTPUT="+baselinePath,
		"TETRAL_STAGE_A_HELPER_OUTPUT="+baselineHelperPath,
		storagetest.EnvTestDatabaseURL+"="+controlDSN,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("construct exact baseline catalog: %v\n%s", err, output)
	}

	currentDB := openCatalogDatabase(t, currentDSN)
	defer func() { _ = currentDB.Close() }()
	if err := storage.MigrateSchema(ctx, currentDB); err != nil {
		t.Fatalf("construct current catalog: %v", err)
	}
	currentSnapshot, err := catalogtest.Snapshot(ctx, currentDB)
	if err != nil {
		t.Fatal(err)
	}
	baselineDB := openCatalogDatabase(t, baselineDSN)
	defer func() { _ = baselineDB.Close() }()
	seedGitIdentityMigrationData(t, baselineDB)
	beforeData := gitIdentityMigrationData(t, baselineDB)
	var oldChecksum string
	var oldAppliedAt time.Time
	if err := baselineDB.QueryRowContext(ctx, `SELECT checksum, applied_at FROM tetral_schema_migrations WHERE version = 1`).Scan(&oldChecksum, &oldAppliedAt); err != nil {
		t.Fatal(err)
	}
	if oldChecksum != storage.PostgreSQLSchemaVersionOneChecksum {
		t.Fatal("historical database does not match immutable V1")
	}
	assertSchemaErrorKind(t, storage.VerifySchema(ctx, baselineDB), storage.SchemaErrorBehind)
	// Fail the second ALTER after the first has added columns. A failed upgrade
	// must leave neither partial columns nor a V2 stamp, and must release its lock.
	if _, err := baselineDB.ExecContext(ctx, `ALTER TABLE session_github_repository_resources ADD CONSTRAINT session_github_repository_git_identity_shape CHECK (true)`); err != nil {
		t.Fatal(err)
	}
	assertSchemaErrorKind(t, storage.MigrateSchema(ctx, baselineDB), storage.SchemaErrorApply)
	var columns, stamps int
	if err := baselineDB.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'session_github_repository_resources' AND column_name IN ('git_identity_name', 'git_identity_email')`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if err := baselineDB.QueryRowContext(ctx, `SELECT count(*) FROM tetral_schema_migrations`).Scan(&stamps); err != nil {
		t.Fatal(err)
	}
	if columns != 0 || stamps != 1 {
		t.Fatalf("failed V2 left columns=%d stamps=%d; want 0/1", columns, stamps)
	}
	if _, err := baselineDB.ExecContext(ctx, `ALTER TABLE session_github_repository_resources DROP CONSTRAINT session_github_repository_git_identity_shape`); err != nil {
		t.Fatal(err)
	}
	if err := storage.MigrateSchema(ctx, baselineDB); err != nil {
		t.Fatalf("upgrade historical database: %v", err)
	}
	if err := storage.VerifySchema(ctx, baselineDB); err != nil {
		t.Fatalf("verify upgraded database: %v", err)
	}
	if afterData := gitIdentityMigrationData(t, baselineDB); beforeData != afterData {
		t.Fatal("upgrade changed existing workspace, Session, or repository data")
	}
	var unchangedStamp, defaultIdentities bool
	if err := baselineDB.QueryRowContext(ctx, `SELECT checksum = $1 AND applied_at = $2 FROM tetral_schema_migrations WHERE version = 1`, oldChecksum, oldAppliedAt).Scan(&unchangedStamp); err != nil {
		t.Fatal(err)
	}
	if err := baselineDB.QueryRowContext(ctx, `SELECT count(*) = 2 AND bool_and(git_identity_name IS NULL AND git_identity_email IS NULL) FROM session_github_repository_resources`).Scan(&defaultIdentities); err != nil {
		t.Fatal(err)
	}
	if !unchangedStamp || !defaultIdentities {
		t.Fatalf("V1 history preserved=%t, old repositories retain default identity=%t", unchangedStamp, defaultIdentities)
	}
	baselineSnapshot, err := catalogtest.Snapshot(ctx, baselineDB)
	if err != nil {
		t.Fatal(err)
	}
	if string(baselineSnapshot) != string(currentSnapshot) {
		t.Fatalf("upgraded catalog differs from fresh V1 + V2: %s", firstSnapshotDifference(baselineSnapshot, currentSnapshot))
	}

	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	currentHelperSnapshot, err := catalogtest.HelperSnapshot(ctx, runtimeDB, adminDB)
	if err != nil {
		t.Fatal(err)
	}
	baselineHelperSnapshot, err := os.ReadFile(baselineHelperPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(baselineHelperSnapshot) != string(currentHelperSnapshot) {
		t.Fatalf("storage-test catalog, seed, runtime-role, or privileges differ from Stage A plus Git identity: %s", firstSnapshotDifference(baselineHelperSnapshot, currentHelperSnapshot))
	}
}

func seedGitIdentityMigrationData(t *testing.T, db *sql.DB) {
	t.Helper()
	// Two tenants with real foreign keys, metadata, checkout and token bytes.
	// Only fixture values are synthetic; neither constraints nor RLS are removed.
	statements := []string{
		`INSERT INTO workspaces (id, name, created_at) VALUES ($1, $1, NOW())`,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $1 || '-env', 'environment', '{}', NOW(), NOW())`,
		`INSERT INTO agents (workspace_id, id, name, created_at, updated_at) VALUES ($1, $1 || '-agent', 'agent', NOW(), NOW())`,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, created_at) VALUES ($1, $1 || '-version', $1 || '-agent', 1, '{}', NOW())`,
		`INSERT INTO sessions (workspace_id, id, type, status, metadata_json, agent_id, agent_version_id, agent_version, environment_id, created_at, updated_at) VALUES ($1, $1 || '-session', 'agent', 'idle', '{"keep":"original"}', $1 || '-agent', $1 || '-version', 1, $1 || '-env', NOW(), NOW())`,
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at) VALUES ($1, $1 || '-session', 'repository', 'github_repository', NOW(), NOW())`,
		`INSERT INTO session_github_repository_resources (workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref, authorization_token_encrypted) VALUES ($1, $1 || '-session', 'repository', 'https://github.com/example/repository.git', '/workspace/repository', 'branch', 'main', decode('010203', 'hex'))`,
	}
	for _, workspace := range []string{"migration-a", "migration-b"} {
		for _, statement := range statements {
			if _, err := db.ExecContext(context.Background(), statement, workspace); err != nil {
				t.Fatalf("seed historical database: %v", err)
			}
		}
	}
}

func gitIdentityMigrationData(t *testing.T, db *sql.DB) string {
	t.Helper()
	var snapshot strings.Builder
	for _, table := range []string{"workspaces", "environments", "agents", "agent_versions", "sessions", "session_resources", "session_github_repository_resources"} {
		var rows string
		// Compare all original columns, including IDs, timestamps and token bytes.
		//nolint:gosec // Fixed fixture table names, quoted as PostgreSQL identifiers.
		query := fmt.Sprintf(`SELECT COALESCE(jsonb_agg(value ORDER BY value::text), '[]')::text FROM (SELECT to_jsonb(r) - 'git_identity_name' - 'git_identity_email' AS value FROM %s r) original_rows`, pgx.Identifier{table}.Sanitize())
		if err := db.QueryRowContext(context.Background(), query).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		snapshot.WriteString(table + ":" + rows + "\n")
	}
	return snapshot.String()
}

func firstSnapshotDifference(before, after []byte) string {
	beforeLines := strings.Split(string(before), "\n")
	afterLines := strings.Split(string(after), "\n")
	limit := min(len(beforeLines), len(afterLines))
	for index := 0; index < limit; index++ {
		if beforeLines[index] != afterLines[index] {
			start := max(0, index-3)
			return "line " + strconv.Itoa(index+1) + " baseline=" + strings.Join(beforeLines[start:index+1], " | ") + " current=" + strings.Join(afterLines[start:index+1], " | ")
		}
	}
	return "line counts baseline=" + strconv.Itoa(len(beforeLines)) + " current=" + strconv.Itoa(len(afterLines))
}

func assertGitObject(ctx context.Context, t *testing.T, object, want string) {
	t.Helper()
	command := exec.CommandContext(ctx, "git", "rev-parse", object)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("resolve %s: %v", object, err)
	}
	if got := strings.TrimSpace(string(output)); got != want {
		t.Fatalf("%s = %s; want %s", object, got, want)
	}
}

func copyCatalogHelper(t *testing.T, root string) {
	t.Helper()
	source, err := os.ReadFile(filepath.Join("catalogtest", "catalog.go"))
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "internal", "storage", "catalogtest")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	// The destination is rooted in the test-owned temporary checkout.
	//nolint:gosec
	if err := os.WriteFile(filepath.Join(directory, "catalog.go"), source, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeBaselineSnapshotTest(t *testing.T, root string) {
	t.Helper()
	const source = `package storage_test

import (
  "context"
  "database/sql"
  "os"
  "testing"
  _ "github.com/jackc/pgx/v5/stdlib"
  "github.com/tetral-ai/tetral/internal/storage"
  "github.com/tetral-ai/tetral/internal/storage/catalogtest"
  "github.com/tetral-ai/tetral/internal/storage/storagetest"
)

func TestWriteStageABaselineCatalog(t *testing.T) {
  db, err := sql.Open("pgx", os.Getenv("TETRAL_STAGE_A_CATALOG_DSN"))
  if err != nil { t.Fatal(err) }
  defer db.Close()
  if err := storage.MigrateSchema(context.Background(), db); err != nil { t.Fatal(err) }
  snapshot, err := catalogtest.Snapshot(context.Background(), db)
  if err != nil { t.Fatal(err) }
  if err := os.WriteFile(os.Getenv("TETRAL_STAGE_A_CATALOG_OUTPUT"), snapshot, 0600); err != nil { t.Fatal(err) }
  runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
  applyExpectedGitIdentityDelta(t, adminDB)
  helperSnapshot, err := catalogtest.HelperSnapshot(context.Background(), runtimeDB, adminDB)
  if err != nil { t.Fatal(err) }
  if err := os.WriteFile(os.Getenv("TETRAL_STAGE_A_HELPER_OUTPUT"), helperSnapshot, 0600); err != nil { t.Fatal(err) }
}

// This fixture states the intended delta independently of the current DDL.
// Do not derive it from storage's current schema or migration constants.
func applyExpectedGitIdentityDelta(t *testing.T, db *sql.DB) {
  t.Helper()
  statements := []string{
    "ALTER TABLE session_github_repository_resources ADD COLUMN git_identity_name TEXT, ADD COLUMN git_identity_email TEXT",
    "ALTER TABLE session_github_repository_resources ADD CONSTRAINT session_github_repository_git_identity_shape CHECK ((git_identity_name IS NULL AND git_identity_email IS NULL) OR (git_identity_name IS NOT NULL AND git_identity_name <> '' AND git_identity_email IS NOT NULL AND git_identity_email <> ''))",
    "INSERT INTO tetral_schema_migrations (version, checksum) VALUES (2, '36b50e4c53b62e8a7b38b8d91b3128400ff06394bf71dcd3e1d992df32b55458')",
  }
  for _, statement := range statements {
    if _, err := db.ExecContext(context.Background(), statement); err != nil { t.Fatal(err) }
  }
}
`
	if err := os.WriteFile(filepath.Join(root, "internal", "storage", "stage_a_catalog_test.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func freshCatalogDatabase(ctx context.Context, t *testing.T, controlDSN string) (string, func()) {
	t.Helper()
	config, err := pgx.ParseConfig(controlDSN)
	if err != nil {
		t.Fatal(err)
	}
	control := stdlib.OpenDB(*config)
	suffixBytes := make([]byte, 8)
	if _, err := rand.Read(suffixBytes); err != nil {
		t.Fatal(err)
	}
	name := "tetral_catalog_" + hex.EncodeToString(suffixBytes)
	if _, err := control.ExecContext(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		_ = control.Close()
		t.Fatal(err)
	}
	// ConnConfig.ConnString returns its original input; changing Database on
	// the parsed config does not change the DSN passed to the baseline process.
	dsn := controlDSN + " dbname=" + name
	if strings.HasPrefix(controlDSN, "postgres://") || strings.HasPrefix(controlDSN, "postgresql://") {
		databaseURL, err := url.Parse(controlDSN)
		if err != nil {
			t.Fatal(err)
		}
		databaseURL.Path = "/" + name
		databaseURL.RawPath = ""
		query := databaseURL.Query()
		query.Set("dbname", name)
		databaseURL.RawQuery = query.Encode()
		dsn = databaseURL.String()
	}
	cleanup := func() {
		_, _ = control.ExecContext(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
		_, _ = control.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
		_ = control.Close()
	}
	probe := openCatalogDatabase(t, dsn)
	var actualDatabase string
	err = probe.QueryRowContext(ctx, "SELECT current_database()").Scan(&actualDatabase)
	_ = probe.Close()
	if err != nil || actualDatabase != name {
		cleanup()
		t.Fatalf("catalog DSN selected database %q; want %q (query error: %v)", actualDatabase, name, err)
	}
	return dsn, cleanup
}

func openCatalogDatabase(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return stdlib.OpenDB(*config)
}
