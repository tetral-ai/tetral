package testinfra

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMinIOCapabilityBelongsOnlyToTwoBlobRunnables(t *testing.T) {
	root := repositoryRootForTest(t)
	command := exec.Command("go", "list", "-json", "./internal/blob")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var pkg listedPackage
	if err := decodeOnePackage(output, &pkg); err != nil {
		t.Fatal(err)
	}
	_, exclusions, err := noInfrastructureTests(pkg)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, exclusion := range exclusions {
		if exclusion.Capability == "minio" {
			got = append(got, exclusion.Runnable)
		}
	}
	want := []string{
		"TestMinIOAcceptsAndReturnsSevenDayBucketLifecycle",
		"TestS3BlobStoreMinIOCopyDeleteAndPrefixIsolation",
	}
	if len(got) != len(want) {
		t.Fatalf("MinIO runnable set = %v; want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("MinIO runnable set = %v; want %v", got, want)
		}
	}
	dependencies, err := goPackageDependencies(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if len(dependencies) != 1 || dependencies[0] != "minio" {
		t.Fatalf("blob package dependencies = %v; want [minio]", dependencies)
	}
}

func TestGatewayProfilesReconcileOneCompleteTestUniverse(t *testing.T) {
	root := filepath.Join(repositoryRootForTest(t), "services", "gateway")
	all, err := allBunTestFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []Profile{ProfileFast, ProfileFull} {
		selected, excluded, err := gatewayTestFiles(root, profile)
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, file := range selected {
			counts[file]++
		}
		for _, item := range excluded {
			counts[item.Runnable]++
		}
		for _, file := range all {
			if counts[file] != 1 {
				t.Fatalf("profile %s disposition for %s = %d; want 1", profile, file, counts[file])
			}
		}
		if profile == ProfileFull && !slices.Contains(selected, "packages/schema/test/unit/readiness.test.ts") {
			t.Fatal("Full Gateway universe omitted schema readiness")
		}
	}
}

func TestRuntimeProfilesReconcileOneCompleteTestUniverse(t *testing.T) {
	root := filepath.Join(repositoryRootForTest(t), "services", "agent-runtime")
	all, err := allBunTestFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []Profile{ProfileFast, ProfileFull} {
		selected, excluded, err := runtimeTestFiles(root, profile == ProfileFull)
		if err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, file := range selected {
			counts[file]++
		}
		for _, item := range excluded {
			counts[item.Runnable]++
		}
		for _, file := range all {
			if counts[file] != 1 {
				t.Fatalf("profile %s disposition for %s = %d; want 1", profile, file, counts[file])
			}
		}
	}
}

func TestRuntimeTestUniverseRejectsAnUnknownTestLocation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "packages", "new-runtime", "test", "other", "orphan.test.ts")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("import { test } from 'bun:test';\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtimeTestFiles(root, true); err == nil {
		t.Fatal("Runtime inventory accepted a test outside every execution class")
	}
}

func TestAffectedFullFallbackRunsCompleteRuntimeEvidence(t *testing.T) {
	root := repositoryRootForTest(t)
	selected, _, err := runtimeTestFiles(filepath.Join(root, "services", "agent-runtime"), true)
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{Profile: ProfileAffected, Revision: Revision{FullFallbackCause: "test fallback"}}
	commands, err := commandsForSelection(plan, Selection{Group: "runtime", Tests: selected}, root, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var integration, build bool
	for _, command := range commands {
		for _, argument := range command.Arguments {
			integration = integration || strings.Contains(argument, "/test/integration/")
		}
		build = build || slices.Equal(command.Arguments, []string{"bun", "run", "build"})
	}
	if !integration || !build {
		t.Fatalf("affected Full fallback commands: integration=%v build=%v commands=%v", integration, build, commands)
	}
}

func TestFullPlanDelegatesOnlyRootHelperProofs(t *testing.T) {
	plan, err := BuildPlan(repositoryRootForTest(t), ProfileFull, "")
	if err != nil {
		t.Fatal(err)
	}
	var delegated []string
	for _, exclusion := range plan.Excluded {
		if exclusion.Disposition == "delegated" {
			delegated = append(delegated, exclusion.Package+"/"+exclusion.Runnable)
		}
	}
	want := []string{
		"github.com/tetral-ai/tetral/internal/sandbox/helper/TestSupervisorKeepsDetachedTaskAuthorizationAfterPrivilegeDrop",
		"github.com/tetral-ai/tetral/internal/sandbox/helper/internal/cli/TestBuiltHelperDetachedExecReturnsPromptly",
	}
	if !slices.Equal(delegated, want) {
		t.Fatalf("delegated Full evidence = %v; want %v", delegated, want)
	}
}

func TestFastGoClassificationFollowsPostgreSQLHelperCallsAcrossFiles(t *testing.T) {
	root := repositoryRootForTest(t)
	command := exec.Command("go", "list", "-json", "./services/bridge")
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
	selected := false
	for _, name := range included {
		if name == "TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionReplayIsIdentityFenced" {
			selected = true
		}
	}
	for _, item := range excluded {
		if item.Runnable == "TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionReplayIsIdentityFenced" && item.Capability == "postgresql" {
			return
		}
	}
	t.Fatalf("indirect PostgreSQL test classification: selected=%v excluded=%#v", selected, excluded)
}

func TestBridgeGoEvidenceDeclaresBunWorkspaceDependency(t *testing.T) {
	root := repositoryRootForTest(t)
	command := exec.Command("go", "list", "-json", "./services/bridge")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var pkg listedPackage
	if err := decodeOnePackage(output, &pkg); err != nil {
		t.Fatal(err)
	}
	dependencies, err := goPackageDependencies(pkg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(dependencies, "bun-workspaces") {
		t.Fatalf("Bridge dependencies = %v; want bun-workspaces", dependencies)
	}
}

func TestPlanReconciliationRejectsOmittedAndDuplicateGoEvidence(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(root, ProfileFast, "")
	if err != nil {
		t.Fatal(err)
	}
	var selectionIndex int
	for index, selection := range plan.Selections {
		if selection.Group == "go" && len(selection.Tests) > 0 {
			selectionIndex = index
			break
		}
	}

	omitted := plan
	omitted.Selections = append([]Selection(nil), plan.Selections...)
	omitted.Selections[selectionIndex].Tests = append([]string(nil), plan.Selections[selectionIndex].Tests[1:]...)
	if err := reconcilePlan(root, inventory, omitted); err == nil {
		t.Fatal("plan reconciliation accepted an omitted Go runnable")
	}

	duplicated := plan
	duplicated.Selections = append([]Selection(nil), plan.Selections...)
	duplicated.Selections = append(duplicated.Selections, plan.Selections[selectionIndex])
	if err := reconcilePlan(root, inventory, duplicated); err == nil {
		t.Fatal("plan reconciliation accepted a duplicate Go package")
	}
}

func TestFastPlanCompilesEveryGoPackage(t *testing.T) {
	root := repositoryRootForTest(t)
	packages, err := listGoPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(root, ProfileFast, "")
	if err != nil {
		t.Fatal(err)
	}
	selected := map[string]bool{}
	for _, selection := range plan.Selections {
		if selection.Group == "go" {
			selected[selection.Packages[0]] = true
		}
	}
	if len(selected) != len(packages) {
		t.Fatalf("Fast selected %d Go packages; want complete %d-package compile universe", len(selected), len(packages))
	}
	for _, pkg := range packages {
		if !selected[pkg.ImportPath] {
			t.Fatalf("Fast omitted Go package %s", pkg.ImportPath)
		}
	}
}

func TestAffectedGoSelectionIncludesRepositoryReverseDependencies(t *testing.T) {
	root := repositoryRootForTest(t)
	selected, err := affectedGoPackages(root, []string{"database/contract.go"})
	if err != nil {
		t.Fatal(err)
	}
	for _, importPath := range []string{
		"github.com/tetral-ai/tetral/database",
		"github.com/tetral-ai/tetral/internal/storage",
		"github.com/tetral-ai/tetral/internal/storage/storagetest",
		"github.com/tetral-ai/tetral/services/api/cmd/tetral-postgresql-roles",
	} {
		if !slices.Contains(selected, importPath) {
			t.Errorf("affected selection omitted reverse dependency %s; selected=%v", importPath, selected)
		}
	}
}

func repositoryRootForTest(t *testing.T) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root not found")
		}
		root = parent
	}
}
