package testinfra

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSeparatedServiceAffectedSelectionIncludesBothOwnersAndConsumers(t *testing.T) {
	root := serviceSelectionFixture(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{
		"internal/runtimeconfig/config.go",
		"internal/mcpmanifest/manifest.go",
		"internal/runtimecontrol/control.go",
		"internal/internalgrpc/auth/config.go",
		"services/bridge/api.go",
		"services/bridge/cmd/bridge-api/main.go",
		"services/job-runner/runtime_delivery.go",
		"services/job-runner/cmd/job-runner/main.go",
		"services/bridge/k8s/deployment.yaml",
		"services/job-runner/k8s/deployment.yaml",
		"services/web-connector/authz.go",
		"services/web-connector/k8s/deployment.yaml",
		"services/agent-runtime/k8s/deployment.yaml",
		"services/gateway/k8s/provider-gateway/configmap.yaml",
		"services/gateway/k8s/mcp-connector/configmap.yaml",
		"services/agent-runtime/packages/runtime-pod/src/auth.ts",
		"services/agent-runtime/packages/protocol/src/binding.ts",
		"services/agent-runtime/proto/tetral/agent_runtime/v1/agent_runtime.proto",
		"services/gateway/packages/provider-gateway/src/config.ts",
		"services/gateway/packages/mcp-connector/src/auth.ts",
		"services/gateway/packages/protocol/src/binding-token.ts",
		"services/gateway/proto/tetral/provider_gateway/v1/provider_gateway.proto",
		"integration/testdata/service-identity.ts",
	} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("known service contract unexpectedly fell back to Full: %s", revision.FullFallbackCause)
			}
			groups := map[string]Selection{}
			for _, selection := range selections {
				groups[selection.Group] = selection
			}
			for _, group := range []string{"go", "runtime", "gateway"} {
				if _, ok := groups[group]; !ok {
					t.Errorf("missing %s consumer for %s", group, changed)
				}
			}
			for _, owner := range []string{"services/bridge", "services/job-runner", "integration"} {
				if !slices.Contains(groups["go"].Packages, "github.com/tetral-ai/tetral/"+owner) {
					t.Errorf("%s omitted %s: %v", changed, owner, groups["go"].Packages)
				}
			}
			if strings.Contains(changed, "/k8s/") {
				if _, ok := groups["deployment"]; !ok {
					t.Error("service-owned manifest omitted deployment evidence")
				}
			}
			if strings.Contains(changed, "/proto/") {
				if _, ok := groups["protocol"]; !ok {
					t.Error("service-owned protocol omitted generation and compatibility evidence")
				}
			}
		})
	}
}

func TestSeparatedServiceManifestSelectionExecutesRawAndHelmInvariants(t *testing.T) {
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{
		"services/bridge/k8s/deployment.yaml",
		"services/job-runner/k8s/deployment.yaml",
		"services/gateway/k8s/provider-gateway/deployment.yaml",
		"services/gateway/k8s/mcp-connector/deployment.yaml",
		"services/web-connector/k8s/deployment.yaml",
	} {
		found := false
		for _, group := range inventory.MatchPath(changed) {
			found = found || group.ID == "deployment"
		}
		if !found {
			t.Errorf("%s omitted deployment evidence", changed)
		}
	}
	commands, err := commandsForSelection(Plan{}, Selection{Group: "deployment"}, t.TempDir(), t.TempDir(), DependencyAuditChanged)
	if err != nil {
		t.Fatal(err)
	}
	var raw, rendered, lint bool
	for _, command := range commands {
		if len(command.Arguments) >= 2 && slices.Equal(command.Arguments[:2], []string{"go", "test"}) {
			raw = raw || slices.Contains(command.Arguments, "./deploy/kubernetes")
			rendered = rendered || slices.Contains(command.Arguments, "./deploy/helm")
		}
		lint = lint || slices.Equal(command.Arguments, []string{"helm", "lint", "deploy/helm/tetral"})
	}
	if !raw || !rendered || !lint {
		t.Fatalf("deployment execution raw=%v rendered=%v lint=%v; all are required", raw, rendered, lint)
	}
}

func TestSharedTypeScriptOwnersSelectTheirRuntimeConsumers(t *testing.T) {
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path   string
		groups []string
	}{
		{"internal/ts-observability/src/index.ts", []string{"runtime", "gateway"}},
		{"internal/ts-dbconnect/src/index.ts", []string{"gateway"}},
	} {
		var got []string
		for _, group := range inventory.MatchPath(tc.path) {
			got = append(got, group.ID)
		}
		for _, want := range tc.groups {
			if !slices.Contains(got, want) {
				t.Errorf("%s omitted %s consumer: %v", tc.path, want, got)
			}
		}
	}
	// Explicit known ownership must not turn unrelated paths into a silent
	// partial run; uncertainty still selects the complete inventory.
	revision := Revision{ChangedPaths: []string{"unknown-service/settings.yaml"}}
	selected, err := affectedSelections(t.TempDir(), inventory, &revision)
	if err != nil || revision.FullFallbackCause == "" || len(selected) != len(inventory.GroupsForProfile("full")) {
		t.Fatalf("unknown ownership did not fail closed to Full: %v, %v, %v", selected, revision, err)
	}
}

func serviceSelectionFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "go.mod", "module github.com/tetral-ai/tetral\n\ngo 1.25\n")
	for _, owner := range []string{
		"services/bridge", "services/job-runner", "integration",
		"internal/runtimeconfig", "internal/mcpmanifest", "internal/runtimecontrol",
	} {
		directory := filepath.Join(root, owner)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		// No fixture import edge connects the owners: the cross-service/RPC
		// relation must be selected explicitly, not accidentally by Go imports.
		writeTestFile(t, directory, "owner.go", "package owner\n")
	}
	return root
}
