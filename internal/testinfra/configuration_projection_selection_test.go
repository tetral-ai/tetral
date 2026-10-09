package testinfra

import (
	"slices"
	"testing"
)

// TestConfigurationProjectionProfilesDeclareBunWorkspaceDependency keeps the
// startup parser root's workspace declaration and Affected closure;
// TestDeclaredGoTestsLeaveFastAndRunOnceAcrossRaceShards asserts its Fast
// exclusion and its placement in exactly one Full race shard.
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
