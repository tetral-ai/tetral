package storage_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

func TestVersionOneCatalogMatchesExactStageABaseline(t *testing.T) {
	controlDSN := os.Getenv(storagetest.EnvTestDatabaseURL)
	if controlDSN == "" {
		t.Skip("TETRAL_TEST_DATABASE_URL is required for Version 1 catalog equivalence")
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
	baselineSnapshot, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(baselineSnapshot) != string(currentSnapshot) {
		t.Fatal("fresh Version 1 PostgreSQL catalog differs from the exact Stage A baseline")
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
		t.Fatalf("storage-test seed, runtime-role, or object privileges differ from the exact Stage A baseline: %s", firstSnapshotDifference(baselineHelperSnapshot, currentHelperSnapshot))
	}
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
  helperSnapshot, err := catalogtest.HelperSnapshot(context.Background(), runtimeDB, adminDB)
  if err != nil { t.Fatal(err) }
  if err := os.WriteFile(os.Getenv("TETRAL_STAGE_A_HELPER_OUTPUT"), helperSnapshot, 0600); err != nil { t.Fatal(err) }
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
	databaseConfig := config.Copy()
	databaseConfig.Database = name
	dsn := databaseConfig.ConnString()
	cleanup := func() {
		_, _ = control.ExecContext(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1`, name)
		_, _ = control.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize())
		_ = control.Close()
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
