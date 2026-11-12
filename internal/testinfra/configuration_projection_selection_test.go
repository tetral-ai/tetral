package testinfra

import (
	"slices"
	"testing"
)

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
