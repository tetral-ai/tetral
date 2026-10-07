package testinfra

import (
	"slices"
	"strings"
	"testing"
)

// The Envoy Gateway compositions consume the Auth Check adapter, the rendered
// edge resources, the separately installed Envoy Gateway resources, the native
// TLS consumers and the pinned dependency lock. Several of those inputs have no
// Go import edge into the integration package, so each change must still
// select that package with the declared dependencies of its Envoy Gateway
// roots, without falling back to Full.
func TestEdgeChangesSelectEnvoyGatewayCompositions(t *testing.T) {
	const integrationPackage = "github.com/tetral-ai/tetral/integration"
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	roots := map[string][]string{}
	for _, declared := range inventory.GoTests {
		if declared.Package == integrationPackage && strings.Contains(declared.Name, "EnvoyGateway") {
			roots[declared.Name] = declared.Dependencies
		}
	}
	if len(roots) == 0 {
		t.Fatal("inventory declares no Envoy Gateway integration roots")
	}
	for _, changed := range []string{
		"services/auth/ext_authz.go",
		"deploy/envoy-gateway/gateway-class.yaml",
		"deploy/helm/tetral/templates/edge.yaml",
		"internal/transportsecurity/credentials.go",
		"deploy/dependencies.lock.json",
	} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selected, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("known edge input fell back to Full: %s", revision.FullFallbackCause)
			}
			expanded, err := expandAffectedGoSelection(root, selected)
			if err != nil {
				t.Fatal(err)
			}
			found := map[string]bool{}
			for _, selection := range expanded {
				if !slices.Contains(selection.Packages, integrationPackage) {
					continue
				}
				for name, dependencies := range roots {
					if !slices.Contains(selection.Tests, name) {
						continue
					}
					found[name] = true
					for _, dependency := range dependencies {
						if !slices.Contains(selection.Dependencies, dependency) {
							t.Errorf("%s selected %s without its %s dependency", changed, name, dependency)
						}
					}
				}
			}
			for name := range roots {
				if !found[name] {
					t.Errorf("%s omitted Envoy Gateway composition %s", changed, name)
				}
			}
		})
	}
}
