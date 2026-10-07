package testinfra

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPublicStreamingChangesSelectActualProtocolConsumers(t *testing.T) {
	root := serviceSelectionFixture(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{
		"services/event-stream/preview_hub.go", "internal/eventstream/request_final_messages.go",
		"internal/eventwire/preview_event.go", "services/gateway/packages/provider-gateway/src/providers/preview-publisher.ts",
		"services/gateway/packages/protocol/src/preview-limits.json", "services/agent-runtime/packages/runtime-pod/src/gateway-client.ts",
		"integration/public_streaming_test_support_test.go", "integration/testdata/public-streaming.json",
		"integration/preview_tls_test.go", "integration/static/public_preview_sdk_projection_test.go",
	} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selected, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("known preview owner fell back to Full: %s", revision.FullFallbackCause)
			}
			groups := map[string]Selection{}
			for _, selection := range selected {
				groups[selection.Group] = selection
			}
			for _, owner := range []string{"go", "runtime", "gateway"} {
				if _, ok := groups[owner]; !ok {
					t.Errorf("missing %s evidence", owner)
				}
			}
			if !slices.Contains(groups["go"].Packages, "github.com/tetral-ai/tetral/integration") {
				t.Errorf("missing real SDK streaming compositions: %v", groups["go"].Packages)
			}
		})
	}
	for _, suffix := range []string{"Identity", "BrokerFailure", "Ordering", "RequestClosure", "Subscribers", "Visibility", "Backpressure", "EndPublication", "Recovery", "ListCursors"} {
		name := "TestPostgreSQLPublicStreaming" + suffix
		found := false
		for _, declared := range inventory.GoTests {
			if declared.Name != name {
				continue
			}
			found = true
			for _, dependency := range []string{"postgresql", "minio", "nats", "bun-workspaces", "sdk"} {
				if !slices.Contains(declared.Dependencies, dependency) {
					t.Errorf("%s lacks %s", name, dependency)
				}
			}
		}
		if !found {
			t.Errorf("Full inventory lacks %s", name)
		}
	}
}

func TestNATSDependencyRejectsMissingStarterAndFixture(t *testing.T) {
	t.Setenv(EnvNATSFixture, "")
	if _, err := LoadNATSFixture(); err == nil {
		t.Fatal("missing mandatory broker fixture accepted")
	}
	if _, err := startDependenciesWith(t.Context(), []string{"nats"}, nil, dependencyStarters{}); err == nil {
		t.Fatal("missing broker starter accepted")
	}
}

// Local brokers project their client policy from the NATS release values, an
// input that Go import traversal cannot see. A values-only change must still
// run the live ACL proof and the broker-backed integration compositions,
// alongside the rendered release checks.
func TestNATSReleaseValuesSelectProjectedBrokerCompositions(t *testing.T) {
	root := serviceSelectionFixture(t)
	directory := filepath.Join(root, "internal", "testinfra")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, directory, "owner.go", "package owner\n")
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{"deploy/nats/values.yaml", "deploy/nats/values-hardened.yaml"} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("NATS release values unexpectedly fell back to Full: %s", revision.FullFallbackCause)
			}
			groups := map[string]Selection{}
			for _, selection := range selections {
				groups[selection.Group] = selection
			}
			for _, owner := range []string{"internal/testinfra", "integration"} {
				if !slices.Contains(groups["go"].Packages, "github.com/tetral-ai/tetral/"+owner) {
					t.Errorf("%s omitted %s: %v", changed, owner, groups["go"].Packages)
				}
			}
			if _, ok := groups["deployment"]; !ok {
				t.Errorf("%s omitted the rendered release checks", changed)
			}
		})
	}
}
