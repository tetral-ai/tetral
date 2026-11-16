package testinfra

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestDockerResourcesOwnAndCleanFailedFixture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	r, err := NewDockerResources(ctx, t, "ownership-proof")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := r.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	network, err := r.Network(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.Run(ctx, ContainerSpec{Image: postgresImage, Network: network, Ports: []int{5432}, Entrypoint: "sh", Command: []string{"-c", "sleep 300"}})
	if err != nil {
		t.Fatal(err)
	}
	address, err := c.Address(ctx, 5432)
	if err != nil || !strings.HasPrefix(address, "127.0.0.1:") || c.ImageID == "" {
		t.Fatalf("fixture address=%q identity=%q error=%v", address, c.ImageID, err)
	}
	// A failed removal must not be hidden merely because the daemon is alive.
	// A resource that remains present exhausts the caller's shorter join bound.
	blocked, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	removed, removalErr := waitDockerResourceRemoved(blocked, "container", "name", "^"+c.Name+"$")
	stop()
	if removed || !errors.Is(removalErr, context.DeadlineExceeded) {
		t.Fatalf("present resource falsely joined: removed=%t error=%v", removed, removalErr)
	}
	if err := cleanupOrphanedDependencyContainers(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOrphanedDependencyNetworks(ctx); err != nil {
		t.Fatal(err)
	}
	if output, err := c.Exec(ctx, "printf", "still-owned"); err != nil || output != "still-owned" {
		t.Fatalf("live fixture was not preserved: %q %v", output, err)
	}
	for _, spec := range []ContainerSpec{
		{Image: postgresImage, NetworkContainer: &DockerContainer{Name: c.Name}},
		{Image: postgresImage, NetworkContainer: c, Network: network},
		{Image: postgresImage, NetworkContainer: c, Aliases: []string{"sidecar"}},
		{Image: postgresImage, NetworkContainer: c, Ports: []int{9090}},
	} {
		if _, err := r.Run(ctx, spec); err == nil {
			t.Fatal("shared namespace accepted an unowned container or incompatible network options")
		}
	}
	sidecar, err := r.Run(ctx, ContainerSpec{Image: postgresImage, NetworkContainer: c, Entrypoint: "sh", Command: []string{"-c", "sleep 300"}})
	if err != nil {
		t.Fatal(err)
	}
	mainNamespace, err := c.Exec(ctx, "readlink", "/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	sidecarNamespace, err := sidecar.Exec(ctx, "readlink", "/proc/self/ns/net")
	if err != nil || mainNamespace == "" || sidecarNamespace != mainNamespace {
		t.Fatalf("sidecar did not share the owned network namespace: %q %q %v", mainNamespace, sidecarNamespace, err)
	}
	// Docker creates container metadata even when its executable cannot start.
	// Cleanup must also own that partial resource, without the returned handle.
	if _, err := r.Run(ctx, ContainerSpec{Image: postgresImage, Network: network, Entrypoint: "/missing-fixture-executable"}); err == nil {
		t.Fatal("invalid fixture executable unexpectedly started")
	}
	// The image declares a volume, so Docker gives every container an
	// anonymous one, the partial container included.
	owned, err := dockerOutput(ctx, "container", "ls", "--all", "-q", "--filter", "label=tetral.test.run-id="+r.runID)
	if err != nil {
		t.Fatal(err)
	}
	var volumes []string
	for _, container := range strings.Fields(owned) {
		names, err := dockerOutput(ctx, "container", "inspect", "--format", `{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}`, container)
		if err != nil {
			t.Fatal(err)
		}
		volumes = append(volumes, strings.Fields(names)...)
	}
	if len(volumes) == 0 {
		t.Fatal("fixture containers have no anonymous volume to clean")
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	remaining, err := dockerOutput(ctx, "volume", "ls", "-q")
	if err != nil {
		t.Fatal(err)
	}
	for _, volume := range volumes {
		if slices.Contains(strings.Fields(remaining), volume) {
			t.Fatalf("fixture left anonymous volume %s", volume)
		}
	}
	for _, kind := range []string{"container", "network"} {
		args := []string{kind, "ls", "-q", "--filter", "label=tetral.test.run-id=" + r.runID}
		if kind == "container" {
			args = append(args, "--all")
		}
		output, err := dockerOutput(ctx, args...)
		if err != nil || output != "" {
			t.Fatalf("fixture left %s resources: %q %v", kind, output, err)
		}
	}
	if _, err := r.Network(ctx); err == nil {
		t.Fatal("closed fixture admitted a network")
	}
}

func TestDockerResourcesCollectDeadOwnerNetwork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	name, err := dependencyContainerName("dead-network-proof")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = runQuiet(cleanup, "docker", "network", "rm", name)
	})
	if err := runQuiet(ctx, "docker", "network", "create", "--label", "tetral.test.owner=testinfra", "--label", "tetral.test.owner-pid=999999999", "--label", "tetral.test.owner-start=dead", name); err != nil {
		t.Fatal(err)
	}
	if err := cleanupOrphanedDependencyNetworks(ctx); err != nil {
		t.Fatal(err)
	}
	output, err := dockerOutput(ctx, "network", "ls", "-q", "--filter", "name=^"+name+"$")
	if err != nil || output != "" {
		t.Fatalf("dead fixture network remains: %q %v", output, err)
	}
}

type fixtureTestOutcome struct {
	name   string
	failed bool
}

func (o fixtureTestOutcome) Name() string { return o.name }
func (o fixtureTestOutcome) Failed() bool { return o.failed }

func TestFixtureKeepsItsContainersLogsWhenItsTestFailed(t *testing.T) {
	for _, row := range []struct {
		name     string
		failed   bool
		wantLogs bool
	}{
		{name: "a failed test keeps each container's log under its name", failed: true, wantLogs: true},
		{name: "a passed test keeps none", failed: false},
	} {
		t.Run(row.name, func(t *testing.T) {
			evidence := t.TempDir()
			t.Setenv(EnvTestEvidenceDir, evidence)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			r, err := NewDockerResources(ctx, fixtureTestOutcome{name: "TestOwner/case_one", failed: row.failed}, "log-proof")
			if err != nil {
				t.Fatal(err)
			}
			c, err := r.Run(ctx, ContainerSpec{Image: postgresImage, Entrypoint: "sh", Command: []string{"-c", "echo fixture-log-line; echo fixture-error-line >&2; sleep 300"}})
			if err != nil {
				_ = r.Close(context.Background())
				t.Fatal(err)
			}
			for logs, _ := c.Logs(ctx); !strings.Contains(logs, "fixture-error-line"); logs, _ = c.Logs(ctx) {
				if ctx.Err() != nil {
					t.Fatalf("the container never logged its lines: %q", logs)
				}
				time.Sleep(50 * time.Millisecond)
			}
			if err := r.Close(ctx); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(evidence, "container-logs", "TestOwner_case_one", "postgres-"+c.Name+".log")
			body, err := os.ReadFile(path) //nolint:gosec // The path is beneath this test's directory.
			if row.wantLogs && (err != nil || !strings.Contains(string(body), "fixture-log-line") || !strings.Contains(string(body), "fixture-error-line")) {
				t.Fatalf("log %s = %q, %v; want both lines", path, body, err)
			}
			if _, statErr := os.Stat(filepath.Join(evidence, "container-logs")); !row.wantLogs && !os.IsNotExist(statErr) {
				t.Fatalf("a passed test kept container logs: %v", statErr)
			}
		})
	}
}
