package storage_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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

// Keep the historical catalog as an independent oracle, extended only by the
// explicit Git identity delta below. All other schema and helper facts must
// remain identical, including constraints, dependencies, RLS and privileges.
func TestVersionOneCatalogMatchesStageABaselineWithGitIdentity(t *testing.T) {
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
	baselineSnapshot = canonicalCatalogSnapshot(t, baselineSnapshot, true)
	currentSnapshot = canonicalCatalogSnapshot(t, currentSnapshot, false)
	if string(baselineSnapshot) != string(currentSnapshot) {
		t.Fatalf("fresh Version 1 catalog differs from Stage A plus Git identity: %s", firstSnapshotDifference(baselineSnapshot, currentSnapshot))
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
	baselineHelperSnapshot = canonicalCatalogSnapshot(t, baselineHelperSnapshot, true)
	currentHelperSnapshot = canonicalCatalogSnapshot(t, currentHelperSnapshot, false)
	if string(baselineHelperSnapshot) != string(currentHelperSnapshot) {
		t.Fatalf("storage-test catalog, seed, runtime-role, or privileges differ from Stage A plus Git identity: %s", firstSnapshotDifference(baselineHelperSnapshot, currentHelperSnapshot))
	}
}

func canonicalCatalogSnapshot(t *testing.T, body []byte, stageA bool) []byte {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if stageA {
		catalog := decoded
		if helper, ok := decoded.(map[string]any); ok {
			catalog = helper["catalog"]
		}
		// ALTER TABLE appends the expected columns at positions 9 and 10;
		// the new bootstrap places them before the old token column at 8.
		// Account only for this declared insertion, preserving every other
		// column's ordinal and every column's type/default/nullability.
		moved := 0
		for _, section := range catalog.([]any) {
			query := section.(map[string]any)
			if query["name"] != "columns" {
				continue
			}
			rows := query["rows"].([]any)
			var identityRows []any
			var identityIndexes []int
			for index, item := range rows {
				row := item.([]any)
				if row[0] != "session_github_repository_resources" {
					continue
				}
				var before, after string
				switch row[2] {
				case "authorization_token_encrypted":
					before, after = "8", "10"
				case "git_identity_name":
					before, after = "9", "8"
				case "git_identity_email":
					before, after = "10", "9"
				default:
					continue
				}
				if row[1] != before {
					t.Fatalf("Stage A column %s ordinal = %v; want %s", row[2], row[1], before)
				}
				row[1] = after
				moved++
				identityRows = append(identityRows, row)
				identityIndexes = append(identityIndexes, index)
			}
			sort.Slice(identityRows, func(i, j int) bool {
				left, right := identityRows[i].([]any), identityRows[j].([]any)
				leftOrdinal, _ := strconv.Atoi(left[1].(string))
				rightOrdinal, _ := strconv.Atoi(right[1].(string))
				return leftOrdinal < rightOrdinal
			})
			for index, position := range identityIndexes {
				rows[position] = identityRows[index]
			}
		}
		if moved != 3 {
			t.Fatalf("Stage A Git identity column insertion changed %d ordinals; want 3", moved)
		}
	}
	canonical, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return canonical
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
  applyExpectedGitIdentityDelta(t, db)
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
    "UPDATE tetral_schema_migrations SET checksum = '6f1ec030d986cec0ae83cc9a5abc818045b5d3a388a9434483d05a5bcdd9fc44' WHERE version = 1 AND checksum = 'd42f4f8936525f02525b621e943d9ad98a91c6d8a76ca11a309c62dee496ade6'",
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
