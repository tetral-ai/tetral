package testinfra

import (
	"slices"
	"testing"
)

func TestInventoryIsClosedAndUnknownPathsDoNotMatch(t *testing.T) {
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	full := inventory.GroupsForProfile("full")
	if len(full) != len(inventory.Groups) {
		t.Fatalf("full inventory has %d groups; want all %d", len(full), len(inventory.Groups))
	}
	if got := inventory.MatchPath("unowned/new-system/file.txt"); len(got) != 0 {
		t.Fatalf("unknown path matched groups: %#v", got)
	}
}

func TestBroadOwnersRequireFull(t *testing.T) {
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		".github/workflows/pull-request-verification.yml",
		"internal/testinfra/planner.go",
		"proto/tetral/bridge/v1/bridge.proto",
		"internal/storage/postgresql_schema.go",
		"services/gateway/bun.lock",
	} {
		if !inventory.RequiresFull(path) {
			t.Errorf("broad owner %q did not require Full", path)
		}
	}
}

func TestInventoryMapsRepresentativeOwnersAndCrossBoundaryRelations(t *testing.T) {
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"internal/queue/postgresql_store.go":              {"go", "go-static", "repository"},
		"services/agent-runtime/packages/core/src/run.ts": {"repository", "runtime"},
		"services/gateway/packages/schema/src/verify.ts":  {"gateway", "repository"},
		"proto/tetral/bridge/v1/bridge.proto":             {"gateway", "protocol", "runtime"},
		"deploy/helm/tetral/Chart.yaml":                   {"deployment"},
		"Dockerfile.sandbox":                              {"sandbox-image"},
	}
	for path, expected := range want {
		matches := inventory.MatchPath(path)
		got := make([]string, 0, len(matches))
		for _, group := range matches {
			got = append(got, group.ID)
		}
		slices.Sort(got)
		if !slices.Equal(got, expected) {
			t.Errorf("owner closure for %q = %v; want %v", path, got, expected)
		}
	}

	mutated := inventory
	mutated.Groups = append([]Group(nil), inventory.Groups...)
	for index := range mutated.Groups {
		if mutated.Groups[index].ID != "runtime" {
			continue
		}
		mutated.Groups[index].Paths = []string{"services/agent-runtime/**"}
	}
	for _, group := range mutated.MatchPath("proto/tetral/bridge/v1/bridge.proto") {
		if group.ID == "runtime" {
			t.Fatal("mutated inventory retained the removed Runtime protocol relation")
		}
	}
}
