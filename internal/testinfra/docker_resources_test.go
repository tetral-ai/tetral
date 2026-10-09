package testinfra

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDockerResourcesOwnAndCleanFailedFixture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	r, err := NewDockerResources(ctx, "ownership-proof")
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
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
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
