package testinfra

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFastPlanStartsNoDurableDependency(t *testing.T) {
	root := repositoryRootForTest(t)
	plan, err := BuildPlan(root, ProfileFast, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Dependencies) != 0 {
		t.Fatalf("Fast dependencies = %v; want none", plan.Dependencies)
	}
	calls := map[string]int{}
	starters := dependencyStarters{
		postgresql: func(context.Context, *dependencyManager) error { calls["postgresql"]++; return nil },
		minio:      func(context.Context, *dependencyManager) error { calls["minio"]++; return nil },
		docker:     func(context.Context) error { calls["docker"]++; return nil },
	}
	manager, err := startDependenciesWith(context.Background(), plan.Dependencies, nil, starters)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 || len(manager.containers) != 0 || len(manager.evidence) != 0 {
		t.Fatalf("Fast dependency startup had side effects: calls=%v containers=%v evidence=%v", calls, manager.containers, manager.evidence)
	}
}

func TestNextRunnerRemovesDependencyContainerOwnedByDeadProcess(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name, err := dependencyContainerName("orphan-proof")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runQuiet(context.Background(), "docker", "rm", "-f", name) }()
	if err := runQuiet(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"--label", "tetral.test.owner=testinfra",
		"--label", "tetral.test.owner-pid=999999999",
		"--label", "tetral.test.owner-start=dead",
		postgresImage, "sh", "-c", "sleep 300"); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		t.Fatal(err)
	}
	output, err := commandOutput(ctx, "docker", "ps", "-aq", "--filter", "name="+name)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output) != "" {
		t.Fatal("dead runner's dependency container remained")
	}
}

func TestNextRunnerPreservesDependencyContainerOwnedByLiveProcess(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	name, err := dependencyContainerName("active-proof")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runQuiet(context.Background(), "docker", "rm", "-f", name) }()
	pid, started := currentProcessIdentity()
	if err := runQuiet(ctx, "docker", "run", "-d", "--rm", "--name", name,
		"--label", "tetral.test.owner=testinfra",
		"--label", "tetral.test.owner-pid="+strconv.Itoa(pid),
		"--label", "tetral.test.owner-start="+started,
		postgresImage, "sh", "-c", "sleep 300"); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		t.Fatal(err)
	}
	output, err := commandOutput(ctx, "docker", "ps", "-q", "--filter", "name="+name)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output) == "" {
		t.Fatal("active runner's dependency container was removed")
	}
}

func TestRunnerOwnedPostgreSQLIsHostReadyBeforeStartupReturns(t *testing.T) {
	if os.Getenv("TETRAL_TEST_DOCKER_AVAILABLE") == "" {
		t.Skip("requires runner-owned Docker dependency")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	manager := &dependencyManager{environment: os.Environ()}
	if err := manager.startPostgreSQL(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := manager.stopBounded(); err != nil {
			t.Errorf("stop PostgreSQL dependency: %v", err)
		}
	}()
	if err := verifyPostgreSQLConnection(ctx, manager.postgresDSN); err != nil {
		t.Fatalf("PostgreSQL dependency was not reachable through its published DSN: %v", err)
	}
}
