package testinfra

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPinnedTransportImagesRunCanonicalRuntimes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	manager := &dependencyManager{root: repositoryRootForTest(t)}
	resources, err := NewDockerResources(ctx, "pinned-runtime-proof")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		if err := resources.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	for _, tc := range []struct{ name, executable string }{
		{"envoy", "/usr/local/bin/envoy"}, {"bun-image", "/usr/local/bin/bun"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := manager.preparePinnedImage(ctx, tc.name); err != nil {
				t.Fatal(err)
			}
			identity := manager.evidence[len(manager.evidence)-1].Identity
			container, err := resources.Run(ctx, ContainerSpec{Image: identity, Entrypoint: tc.executable, Command: []string{"--version"}})
			if err != nil {
				t.Fatal(err)
			}
			if code, err := container.Wait(ctx); err != nil || code != 0 {
				t.Fatalf("canonical runtime exit=%d error=%v", code, err)
			}
			version, err := container.Logs(ctx)
			if err != nil || strings.TrimSpace(version) == "" {
				t.Fatalf("canonical runtime omitted version: %v", err)
			}
			if tc.name == "bun-image" {
				// The production image tag and executed version must agree.
				if !strings.Contains(identity, ":"+strings.TrimSpace(version)+"-") {
					t.Fatalf("Bun executable version %q differs from canonical image %s", version, identity)
				}
			}
			t.Logf("canonical=%s image=%s version=%s", identity, container.ImageID, strings.TrimSpace(version))
		})
	}
}
