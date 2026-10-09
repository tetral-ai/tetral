package testinfra

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestAuthContractSelectionIncludesPublicActorConsumersWithoutImportEdges(t *testing.T) {
	root := serviceSelectionFixture(t)
	owners := []string{"internal/auth", "services/auth", "internal/httpapi", "internal/memory", "internal/eventstream", "services/event-stream", "integration"}
	for _, owner := range owners {
		// Independent fake packages deliberately contain no Go import edges. The
		// signed transport/actor contract must select its consumers explicitly.
		if err := os.MkdirAll(filepath.Join(root, owner), 0755); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, root, filepath.Join(owner, "owner.go"), "package owner\n")
	}
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, changed := range []string{"internal/auth/principal.go", "internal/auth/authority_resolver.go", "services/auth/routes.go"} {
		t.Run(changed, func(t *testing.T) {
			revision := Revision{ChangedPaths: []string{changed}}
			selections, err := affectedSelections(root, inventory, &revision)
			if err != nil {
				t.Fatal(err)
			}
			if revision.FullFallbackCause != "" {
				t.Fatalf("known Auth contract fell back to Full: %s", revision.FullFallbackCause)
			}
			var packages []string
			for _, selection := range selections {
				if selection.Group == "go" {
					packages = selection.Packages
				}
			}
			for _, owner := range owners {
				if !slices.Contains(packages, "github.com/tetral-ai/tetral/"+owner) {
					t.Errorf("Auth contract omitted %s without incidental Go imports", owner)
				}
			}
		})
	}
}
