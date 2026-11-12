package testinfra

import (
	"slices"
	"testing"
)

// An authentication change reaches the public consumers that Go imports do not
// connect to it. The declared tests' dependencies are held for every package
// by TestDeclaredGoDependenciesDriveProfileSelections.
func TestAuthenticationChangesSelectTheirPublicConsumers(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	revision := Revision{ChangedPaths: []string{"internal/auth/authority_resolver.go"}}
	selected, err := affectedSelections(root, inventory, &revision)
	if err != nil {
		t.Fatal(err)
	}
	if revision.FullFallbackCause != "" {
		t.Fatalf("known Auth owner fell back to Full: %s", revision.FullFallbackCause)
	}
	var owners []string
	for _, selection := range selected {
		if selection.Group == "go" {
			owners = selection.Packages
		}
	}
	for _, owner := range []string{"internal/auth", "internal/httpapi", "internal/memory", "internal/eventstream", "services/auth", "services/event-stream", "integration"} {
		if !slices.Contains(owners, "github.com/tetral-ai/tetral/"+owner) {
			t.Errorf("Auth change omitted public consumer %s", owner)
		}
	}
}
