package testinfra

import (
	"context"
	"os/exec"
	"slices"
	"testing"
	"time"
)

func TestConfigurationProjectionProfilesDeclareBunWorkspaceDependency(t *testing.T) {
	const packageName = "github.com/tetral-ai/tetral/deploy/helm"
	const runnable = "TestConfigurationOperationalProjectionUsesOwningParsers"
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	declarations := 0
	for _, contract := range inventory.GoTests {
		if contract.Package == packageName && contract.Name == runnable {
			declarations++
			if !slices.Equal(contract.Dependencies, []string{"bun-workspaces"}) {
				t.Fatalf("startup parser dependencies=%v", contract.Dependencies)
			}
		}
	}
	if declarations != 1 {
		t.Fatal("actual cross-language parser root needs one explicit workspace contract")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "list", "-json", "./deploy/helm")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var pkg listedPackage
	if err := decodeOnePackage(output, &pkg); err != nil {
		t.Fatal(err)
	}
	included, excluded, err := noInfrastructureTests(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(included, runnable) {
		t.Fatal("Fast must compile but not run Bun-dependent startup parser root")
	}
	accounted := 0
	for _, item := range excluded {
		if item.Runnable == runnable {
			accounted++
			if !slices.Contains(append([]string{item.Capability}, item.Capabilities...), "bun-workspaces") {
				t.Fatal("Fast exclusion lost workspace capability")
			}
		}
	}
	if accounted != 1 {
		t.Fatal("Fast must account for the excluded real parser root exactly once")
	}
	full, _, err := fullGoSelections(root, "configuration startup parser contract")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for index := 0; index < 4; index++ {
		shard, err := SelectPlan(Plan{Profile: ProfileFull, Selections: full}, []string{"go"}, index, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, selection := range shard.Selections {
			if slices.Contains(selection.Packages, packageName) && slices.Contains(selection.Tests, runnable) {
				count++
				if !slices.Contains(selection.Dependencies, "bun-workspaces") || !slices.Contains(shard.Dependencies, "bun-workspaces") {
					t.Fatal("Full native shard lacks clean-workspace prerequisite")
				}
			}
		}
	}
	if count != 1 {
		t.Fatalf("Full shards select actual startup parser root %d times", count)
	}
	revision := Revision{ChangedPaths: []string{"deploy/helm/configuration_projection_test.go"}}
	affected, err := affectedSelections(root, inventory, &revision)
	if err != nil {
		t.Fatal(err)
	}
	if revision.FullFallbackCause != "" {
		t.Fatalf("known configuration owner fell back: %s", revision.FullFallbackCause)
	}
	affected, err = expandAffectedGoSelection(root, affected)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, selection := range affected {
		if slices.Contains(selection.Packages, packageName) && slices.Contains(selection.Tests, runnable) {
			found = true
			if !slices.Contains(selection.Dependencies, "bun-workspaces") {
				t.Fatal("Affected native parser lost workspace prerequisite")
			}
		}
	}
	if !found {
		t.Fatal("Affected configuration owner omitted actual parser root")
	}
}

func TestConfigurationFixtureAffectedDeploymentSliceRequiresBunWorkspaces(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	revision := Revision{ChangedPaths: []string{"deploy/helm/testdata/configuration-parsers.ts"}}
	selected, err := affectedSelections(root, inventory, &revision)
	if err != nil {
		t.Fatal(err)
	}
	if revision.FullFallbackCause != "" {
		t.Fatalf("known parser fixture fell back: %s", revision.FullFallbackCause)
	}
	plan, err := SelectPlan(Plan{Profile: ProfileAffected, Revision: revision, Selections: selected}, []string{"deployment"}, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(plan.Dependencies, []string{"bun-workspaces"}) {
		t.Fatalf("deployment-only producer dependencies=%v", plan.Dependencies)
	}
	commands, err := commandsForSelection(plan, plan.Selections[0], root, t.TempDir(), DependencyAuditChanged)
	if err != nil {
		t.Fatal(err)
	}
	executesParserPackage := false
	for _, command := range commands {
		executesParserPackage = executesParserPackage || slices.Contains(command.Arguments, "./deploy/helm")
	}
	if !executesParserPackage {
		t.Fatal("deployment producer no longer executes the real parser package")
	}
}
