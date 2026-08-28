//go:build unix

package testinfra

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

const detachedCloneFixtureEnv = "TETRAL_TESTINFRA_DETACHED_CLONE_FIXTURE"

func TestRunnerDetachedBunCloneFixture(t *testing.T) {
	if os.Getenv(detachedCloneFixtureEnv) != "1" {
		return
	}
	db := storagetest.NewPostgreSQLDB(t)
	var identity cloneFixtureIdentity
	if err := db.QueryRow(`SELECT current_database(), current_user`).Scan(&identity.Database, &identity.Role); err != nil {
		t.Fatal(err)
	}
	identity.RuntimeURL = storagetest.RuntimeDatabaseURL(t, db)
	marker := os.Getenv("TETRAL_TESTINFRA_DETACHED_MARKER")
	script := "const sql = new Bun.SQL({ url: process.env.TETRAL_TEST_RUNTIME_DATABASE_URL, max: 1 });\n" +
		"await sql`SELECT 1`;\n" +
		"await Bun.write(process.env.TETRAL_TEST_DETACHED_MARKER + \".ready\", \"ready\");\n" +
		"for (;;) {\n" +
		"  await Bun.sleep(50);\n" +
		"  try { await sql`SELECT 1`; }\n" +
		"  catch { await Bun.write(process.env.TETRAL_TEST_DETACHED_MARKER + \".revoked\", \"revoked\"); if (process.env.TETRAL_TEST_DETACHED_STAY_ALIVE !== \"1\") process.exit(0); }\n" +
		"}"
	command := exec.Command("bun", "-e", script)
	command.Dir = filepath.Join(repositoryRoot(t), "services", "gateway")
	command.Env = append(os.Environ(),
		"TETRAL_TEST_RUNTIME_DATABASE_URL="+identity.RuntimeURL,
		"TETRAL_TEST_DETACHED_MARKER="+marker,
	)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	registry := os.Getenv(descendantRegistryEnv)
	if registry == "" {
		t.Fatal("detached descendant registry is required")
	}
	registration := []byte(fmt.Sprintf("%d:%s\n", command.Process.Pid, processStartIdentity(command.Process.Pid)))
	// The parent runner supplies a test-owned temporary registry path.
	//nolint:gosec
	if err := os.WriteFile(registry, registration, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		// The marker path is created inside the test-owned temporary directory.
		//nolint:gosec
		if _, err := os.Stat(marker + ".ready"); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("detached Bun probe exited before readiness: %v: %s", err, output.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached Bun probe did not become ready: %s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	body, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	// The parent runner supplies a test-owned temporary artifact path.
	//nolint:gosec
	if err := os.WriteFile(os.Getenv("TETRAL_TESTINFRA_CLONE_IDENTITY"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TETRAL_TESTINFRA_DETACHED_PARENT_SUCCEEDS") == "1" {
		return
	}
	select {}
}

func TestRunStepRevokesDetachedBunCloneCapability(t *testing.T) {
	dsn := requiredControlDSN(t)
	root := repositoryRoot(t)
	outputDir := t.TempDir()
	identityPath := filepath.Join(outputDir, "detached-clone.json")
	marker := filepath.Join(outputDir, "detached")
	t.Setenv(detachedCloneFixtureEnv, "1")
	t.Setenv("TETRAL_TESTINFRA_CLONE_IDENTITY", identityPath)
	t.Setenv("TETRAL_TESTINFRA_DETACHED_MARKER", marker)
	manager := &dependencyManager{environment: os.Environ(), postgresDSN: dsn}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result := make(chan error, 1)
	go func() {
		_, err := runStep(ctx, root, "fixture", commandSpec{
			Arguments: []string{"go", "test", "./internal/testinfra", "-run", "^TestRunnerDetachedBunCloneFixture$", "-count=1"},
		}, manager, outputDir)
		result <- err
	}()
	identity := readCloneFixtureIdentity(t, identityPath)
	cancel()
	if err := <-result; err == nil {
		t.Fatal("detached fixture process unexpectedly completed")
	}
	waitForFile(t, marker+".revoked", 10*time.Second)

	concurrent := storagetest.NewPostgreSQLDB(t)
	var concurrentDatabase string
	if err := concurrent.QueryRow(`SELECT current_database()`).Scan(&concurrentDatabase); err != nil {
		t.Fatal(err)
	}
	config, err := pgx.ParseConfig(identity.RuntimeURL)
	if err != nil {
		t.Fatal(err)
	}
	config.Database = concurrentDatabase
	connection, err := pgx.ConnectConfig(context.Background(), config)
	if err == nil {
		_ = connection.Close(context.Background())
		t.Fatal("retired clone credential connected to a concurrent clone")
	}
}

func TestPassingPackageWithLiveDetachedDescendantIsApparatusFailure(t *testing.T) {
	dsn := requiredControlDSN(t)
	root := repositoryRoot(t)
	outputDir := t.TempDir()
	identityPath := filepath.Join(outputDir, "detached-pass-clone.json")
	marker := filepath.Join(outputDir, "detached-pass")
	t.Setenv(detachedCloneFixtureEnv, "1")
	t.Setenv("TETRAL_TESTINFRA_CLONE_IDENTITY", identityPath)
	t.Setenv("TETRAL_TESTINFRA_DETACHED_MARKER", marker)
	t.Setenv("TETRAL_TESTINFRA_DETACHED_PARENT_SUCCEEDS", "1")
	t.Setenv("TETRAL_TEST_DETACHED_STAY_ALIVE", "1")
	manager := &dependencyManager{environment: os.Environ(), postgresDSN: dsn}
	step, err := runStep(context.Background(), root, "fixture", commandSpec{
		Arguments: []string{"go", "test", "./internal/testinfra", "-run", "^TestRunnerDetachedBunCloneFixture$", "-count=1"},
	}, manager, outputDir)
	if err == nil || step.Status != "apparatus-failed" {
		t.Fatalf("live detached descendant result = %s/%v; want apparatus failure", step.Status, err)
	}
	identity := readCloneFixtureIdentity(t, identityPath)
	if connection, err := pgx.Connect(context.Background(), identity.RuntimeURL); err == nil {
		_ = connection.Close(context.Background())
		t.Fatal("retired detached descendant credential still connected")
	}
}

func TestNextBootstrapRecoversDeadRunWithoutDeletingActiveOrUnownedDatabase(t *testing.T) {
	dsn := requiredControlDSN(t)
	root := repositoryRoot(t)
	directory := t.TempDir()
	identityPath := filepath.Join(directory, "dead-run.json")
	runID, err := randomIdentity(16)
	if err != nil {
		t.Fatal(err)
	}
	environment := withoutEnvironmentVariable(os.Environ(), storagetest.EnvTestRunID)
	environment = withoutEnvironmentVariable(environment, cloneFixtureEnv)
	environment = append(environment,
		storagetest.EnvTestRunID+"="+runID,
		cloneFixtureEnv+"=1",
		"TETRAL_TESTINFRA_CLONE_IDENTITY="+identityPath,
	)
	command := exec.Command("go", "test", "./internal/testinfra", "-run", "^TestRunnerPostgreSQLCloneFixture$", "-count=1")
	command.Dir = root
	command.Env = environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	identity := readCloneFixtureIdentity(t, identityPath)
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = control.Close() }()

	active := storagetest.NewPostgreSQLDB(t)
	var activeDatabase string
	if err := active.QueryRow(`SELECT current_database()`).Scan(&activeDatabase); err != nil {
		t.Fatal(err)
	}
	assertDatabaseExists(t, control, identity.Database, true)
	assertDatabaseExists(t, control, activeDatabase, true)

	unownedSuffix, err := randomIdentity(5)
	if err != nil {
		t.Fatal(err)
	}
	unowned := "tetral_test_unowned_" + unownedSuffix
	if _, err := control.Exec("CREATE DATABASE " + pgx.Identifier{unowned}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = control.Exec("DROP DATABASE IF EXISTS " + pgx.Identifier{unowned}.Sanitize() + " WITH (FORCE)")
	}()

	_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	_ = command.Wait()
	if _, err := control.Exec(`UPDATE tetral_test_control_runs SET expires_at=clock_timestamp()-interval '1 second' WHERE run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	nextRunID, err := randomIdentity(16)
	if err != nil {
		t.Fatal(err)
	}
	nextEnvironment := withoutEnvironmentVariable(os.Environ(), storagetest.EnvTestRunID)
	nextEnvironment = withoutEnvironmentVariable(nextEnvironment, cloneFixtureEnv)
	nextEnvironment = append(nextEnvironment,
		storagetest.EnvTestRunID+"="+nextRunID,
		bootstrapFixtureEnv+"=1",
	)
	nextBootstrap := exec.Command("go", "test", "./internal/testinfra", "-run", "^TestRunnerPostgreSQLBootstrapFixture$", "-count=1")
	nextBootstrap.Dir = root
	nextBootstrap.Env = nextEnvironment
	if output, err := nextBootstrap.CombinedOutput(); err != nil {
		t.Fatalf("next PostgreSQL bootstrap failed: %v: %s", err, output)
	}
	assertDatabaseExists(t, control, identity.Database, false)
	assertDatabaseExists(t, control, activeDatabase, true)
	assertDatabaseExists(t, control, unowned, true)
}

func requiredControlDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv(storagetest.EnvTestDatabaseURL)
	if dsn == "" {
		t.Fatal("TETRAL_TEST_DATABASE_URL is required")
	}
	return dsn
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", filepath.Base(path))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertDatabaseExists(t *testing.T, db *sql.DB, name string, want bool) {
	t.Helper()
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)`, name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Fatalf("database %s exists=%v; want %v", name, exists, want)
	}
}
