package testinfra

import (
	"slices"
	"strings"
	"testing"
)

func TestContentChangesSelectDurableAndTypeScriptConsumers(t *testing.T) {
	root := serviceSelectionFixture(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	// The fixture has no Go imports between the owners. These paths must
	// select the real RPC and spawned-process consumers explicitly.
	for _, changed := range []string{
		"services/agent-runtime/packages/core/src/thread-loop/thread-loop.ts",
		"services/agent-runtime/packages/core/src/session/context-manager.ts",
		"services/agent-runtime/packages/core/src/context/context-loader.ts",
		"services/agent-runtime/packages/runtime-pod/src/bridge-client.ts",
		"services/agent-runtime/packages/runtime-pod/src/gateway-client.ts",
		"services/agent-runtime/packages/runtime-pod/test/fixtures/content-lifecycle-runtime.ts",
		"services/gateway/packages/provider-gateway/src/providers/block-assembler.ts",
		"services/gateway/packages/provider-gateway/src/grpc-server.ts",
		"services/gateway/packages/provider-gateway/src/service.ts",
		"services/gateway/packages/provider-gateway/test/fixtures/content-lifecycle-gateway.ts",
		"services/gateway/packages/protocol/src/bounds.ts",
		"services/bridge/runtime_declaration.go",
		"services/bridge/bridge_api_context.go",
		"integration/content_lifecycle_e2e_test.go",
		"integration/content_lifecycle_topology_test.go",
		"integration/content_provider_failure_lifecycle_test.go",
		"integration/content_process_recovery_test.go",
		"integration/content_resource_lifecycle_test.go",
		"integration/content_transport_limits_test.go",
		"integration/testdata/content-lifecycle-client.ts",
	} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("known content owner fell back to Full: %s", revision.FullFallbackCause)
			}
			groups := map[string]Selection{}
			for _, selection := range selections {
				groups[selection.Group] = selection
			}
			for _, owner := range []string{"go", "runtime", "gateway"} {
				if _, ok := groups[owner]; !ok {
					t.Errorf("missing %s evidence", owner)
				}
			}
			for _, owner := range []string{"services/bridge", "integration"} {
				if !slices.Contains(groups["go"].Packages, "github.com/tetral-ai/tetral/"+owner) {
					t.Errorf("missing durable owner %s: %v", owner, groups["go"].Packages)
				}
			}
			if !strings.Contains(groups["go"].Reason, "Bun") {
				t.Errorf("selection reason omits the cross-language fixture relation: %q", groups["go"].Reason)
			}
		})
	}
}
