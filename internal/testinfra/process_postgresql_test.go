package testinfra

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const (
	cloneFixtureEnv     = "TETRAL_TESTINFRA_CLONE_FIXTURE"
	bootstrapFixtureEnv = "TETRAL_TESTINFRA_BOOTSTRAP_FIXTURE"
)

type cloneFixtureIdentity struct {
	Database   string `json:"database"`
	Role       string `json:"role"`
	RuntimeURL string `json:"runtime_url"`
}

func TestRunnerPostgreSQLCloneFixture(t *testing.T) {
	if os.Getenv(cloneFixtureEnv) != "1" {
		return
	}
	db := storagetest.NewPostgreSQLDB(t)
	var identity cloneFixtureIdentity
	if err := db.QueryRow(`SELECT current_database(), current_user`).Scan(&identity.Database, &identity.Role); err != nil {
		t.Fatal(err)
	}
	identity.RuntimeURL = storagetest.RuntimeDatabaseURL(t, db)
	body, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	// The parent runner supplies a test-owned temporary artifact path.
	//nolint:gosec
	if err := os.WriteFile(os.Getenv("TETRAL_TESTINFRA_CLONE_IDENTITY"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestRunnerPostgreSQLBootstrapFixture(t *testing.T) {
	if os.Getenv(bootstrapFixtureEnv) != "1" {
		return
	}
	_ = storagetest.NewPostgreSQLDB(t)
}

func TestRunStepRevokesPostgreSQLCloneAfterTimeout(t *testing.T) {
	dsn := os.Getenv(storagetest.EnvTestDatabaseURL)
	if dsn == "" {
		t.Fatal("TETRAL_TEST_DATABASE_URL is required")
	}
	root := repositoryRoot(t)
	outputDir := t.TempDir()
	identityPath := filepath.Join(outputDir, "clone.json")
	t.Setenv(cloneFixtureEnv, "1")
	t.Setenv("TETRAL_TESTINFRA_CLONE_IDENTITY", identityPath)
	manager := &dependencyManager{environment: os.Environ(), postgresDSN: dsn}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := runStep(ctx, root, "fixture", commandSpec{
			Arguments: []string{"go", "test", "./internal/testinfra", "-run", "^TestRunnerPostgreSQLCloneFixture$", "-count=1"},
		}, manager, outputDir)
		result <- err
	}()
	identity := readCloneFixtureIdentity(t, identityPath)
	cancel()
	err := <-result
	if err == nil {
		t.Fatal("fixture process unexpectedly completed")
	}
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close() }()
	var databaseExists, roleExists, registryExists bool
	if err := control.QueryRow(`SELECT
		EXISTS(SELECT 1 FROM pg_database WHERE datname=$1),
		EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$2),
		EXISTS(SELECT 1 FROM tetral_test_clone_registry WHERE database_name=$1)`, identity.Database, identity.Role).Scan(&databaseExists, &roleExists, &registryExists); err != nil {
		t.Fatal(err)
	}
	if databaseExists || roleExists || registryExists {
		t.Fatalf("runner cleanup left database=%v role=%v registry=%v", databaseExists, roleExists, registryExists)
	}
}

func readCloneFixtureIdentity(t *testing.T, path string) cloneFixtureIdentity {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		body, err := os.ReadFile(path)
		if err == nil {
			var identity cloneFixtureIdentity
			if json.Unmarshal(body, &identity) == nil && identity.Database != "" && identity.Role != "" {
				return identity
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("clone fixture did not publish identity: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory || strings.TrimSpace(directory) == "" {
			t.Fatal("repository root not found")
		}
		directory = parent
	}
}
