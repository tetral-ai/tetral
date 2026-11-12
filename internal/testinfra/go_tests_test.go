package testinfra

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestFastGoClassificationFollowsInfrastructureAcrossFiles(t *testing.T) {
	for _, tc := range []struct {
		name         string
		files        map[string]string
		fast         []string
		excluded     map[string]string
		dependencies []string
	}{
		{name: "a test that reads the MinIO endpoint",
			files: map[string]string{"blob_test.go": `package fixture
import ("os"; "testing")
func TestStore(t *testing.T) { _ = os.Getenv("TETRAL_TEST_MINIO_ENDPOINT") }
func TestKey(t *testing.T) {}
`},
			fast: []string{"TestKey"}, excluded: map[string]string{"TestStore": "minio"}, dependencies: []string{"minio"}},
		{name: "a test whose helper in another file opens the test database",
			files: map[string]string{
				"helper_test.go": `package fixture
import ("testing"; "github.com/tetral-ai/tetral/internal/storage/storagetest")
func openStore(t *testing.T) { _ = storagetest.Open(t) }
`,
				"store_test.go": `package fixture
import "testing"
func TestReplay(t *testing.T) { openStore(t) }
func TestParse(t *testing.T) {}
`},
			fast: []string{"TestParse"}, excluded: map[string]string{"TestReplay": "postgresql"}, dependencies: []string{"postgresql"}},
		{name: "a test whose helper looks up Bun",
			files: map[string]string{"parsers_test.go": `package fixture
import ("os/exec"; "testing")
func runParsers(t *testing.T) { bun, _ := exec.LookPath("bun"); _ = exec.Command(bun, "run", "parsers.ts") }
func TestProjection(t *testing.T) { runParsers(t) }
func TestLocal(t *testing.T) {}
`},
			fast: []string{"TestLocal"}, excluded: map[string]string{"TestProjection": "bun-workspaces"}, dependencies: []string{"bun-workspaces"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			pkg := listedPackage{ImportPath: "fixture", Dir: root}
			for name, body := range tc.files {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
					t.Fatal(err)
				}
				pkg.TestGoFiles = append(pkg.TestGoFiles, name)
			}
			fast, excluded, err := classifyGoTests(pkg, nil)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, item := range excluded {
				got[item.Runnable] = item.Capability
			}
			if !slices.Equal(fast, tc.fast) || !maps.Equal(got, tc.excluded) {
				t.Fatalf("Fast disposition = selected %v, excluded %v; want %v, %v", fast, got, tc.fast, tc.excluded)
			}
			if dependencies := dependenciesForCapabilities(excluded); !slices.Equal(dependencies, tc.dependencies) {
				t.Fatalf("dependencies = %v; want %v", dependencies, tc.dependencies)
			}
		})
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
	commands, err := commandsForSelection(plan, Selection{Group: "runtime", Tests: selected}, root, t.TempDir(), DependencyAuditChanged)
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

// The Full plan leaves to the sandbox image's root stage exactly the tests that
// stage runs: its RUN instructions, run under bash with a stub go, call go
// test with each delegated package and test.
func TestFullPlanDelegatesExactlyTheTestsTheRootImageStageRuns(t *testing.T) {
	root := repositoryRootForTest(t)
	plan, err := BuildPlan(root, ProfileFull, "")
	if err != nil {
		t.Fatal(err)
	}
	var delegated []string
	for _, exclusion := range plan.Excluded {
		if exclusion.Disposition == "delegated" {
			delegated = append(delegated, exclusion.Package+"."+exclusion.Runnable)
		}
	}
	dockerfile, err := os.ReadFile(filepath.Join(root, "Dockerfile.sandbox"))
	if err != nil {
		t.Fatal(err)
	}
	var run []string
	for _, script := range dockerfileStageRuns(string(dockerfile), "helper-privilege-test") {
		calls, err := recordGoCalls(script, []string{"TETRAL_HELPER_PRIVILEGE_TEST_NONCE=check"})
		if err != nil {
			t.Fatalf("RUN %s: %v", script, err)
		}
		for _, call := range calls {
			pattern, ok := goFlagValue(call[min(1, len(call)):], "run")
			test, anchored := strings.CutSuffix(strings.TrimPrefix(pattern, "^"), "$")
			packages := goPositionals(call[min(1, len(call)):])
			if len(call) < 2 || call[0] != "test" || len(packages) != 1 || !ok || !anchored || !regexp.MustCompile(`^\w+$`).MatchString(test) {
				t.Fatalf("the root image stage calls go %q; want go test <package> -run '^<Test>$'", call)
			}
			pkg := strings.TrimPrefix(strings.TrimPrefix(call[1+packages[0]], "./"), "github.com/tetral-ai/tetral/")
			run = append(run, "github.com/tetral-ai/tetral/"+pkg+"."+test)
		}
	}
	slices.Sort(delegated)
	slices.Sort(run)
	if len(delegated) == 0 || !slices.Equal(delegated, run) {
		t.Fatalf("delegated Full evidence = %v; the root image stage runs %v", delegated, run)
	}
}

// dockerfileStageRuns returns the shell form of each RUN instruction of the
// named stage, continuation lines joined and comment lines dropped, as
// Docker reads them.
func dockerfileStageRuns(dockerfile, stage string) []string {
	var instructions []string
	var current string
	for _, line := range strings.Split(dockerfile, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if body, continued := strings.CutSuffix(strings.TrimRight(line, " \t"), "\\"); continued {
			current += body + " "
			continue
		}
		instructions = append(instructions, current+line)
		current = ""
	}
	var runs []string
	inside := false
	for _, instruction := range instructions {
		fields := strings.Fields(instruction)
		switch {
		case len(fields) == 0:
		case strings.EqualFold(fields[0], "FROM"):
			inside = len(fields) == 4 && strings.EqualFold(fields[2], "AS") && fields[3] == stage
		case inside && strings.EqualFold(fields[0], "RUN"):
			runs = append(runs, strings.TrimSpace(strings.TrimSpace(instruction)[len(fields[0]):]))
		}
	}
	return runs
}

// The Bun workspaces dependency installs what these packages' tests start
// and sets the switch the gRPC tests wait for; the protocol group runs the
// gRPC tests as its own command.
func TestGoPackagesThatStartBunDeclareItsWorkspaces(t *testing.T) {
	root := repositoryRootForTest(t)
	for _, path := range []string{"./services/bridge", "./integration/grpcinterop"} {
		command := exec.Command("go", "list", "-json", path)
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
			t.Errorf("%s dependencies = %v; want bun-workspaces", path, dependencies)
		}
	}
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	if group, _ := inventory.Group("protocol"); !slices.Contains(group.Dependencies, "bun-workspaces") {
		t.Errorf("protocol group dependencies = %v; want bun-workspaces", group.Dependencies)
	}
}

func TestDeclaredGoDependenciesDriveProfileSelections(t *testing.T) {
	root := repositoryRootForTest(t)
	full, err := BuildPlan(root, ProfileFull, "")
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	// Affected expansion is a function of the package, its listing and the
	// inventory, never of the contract, and yields one selection per package, so
	// one expansion of the declared packages serves every contract.
	var packages []string
	for _, contract := range inventory.GoTests {
		if !slices.Contains(packages, contract.Package) {
			packages = append(packages, contract.Package)
		}
	}
	affected, err := expandAffectedGoSelection(root, []Selection{{Group: "go", Packages: packages}})
	if err != nil {
		t.Fatal(err)
	}
	for _, contract := range inventory.GoTests {
		for _, selections := range [][]Selection{full.Selections, affected} {
			found := false
			for _, selection := range selections {
				if !slices.Contains(selection.Packages, contract.Package) || !slices.Contains(selection.Tests, contract.Name) {
					continue
				}
				found = true
				for _, dependency := range contract.Dependencies {
					if !slices.Contains(selection.Dependencies, dependency) {
						t.Errorf("selected %s without declared dependency %s", contract.Name, dependency)
					}
				}
			}
			if !found {
				t.Errorf("profile did not select declared test %s", contract.Name)
			}
		}
	}
}

// Every inventory-declared Go test with dependencies needs real infrastructure.
// Fast must exclude it and record each declared dependency as a capability,
// and the Full Go race shards together must run it exactly once with its
// dependencies on both its selection and its shard.
func TestDeclaredGoTestsLeaveFastAndRunOnceAcrossRaceShards(t *testing.T) {
	root := repositoryRootForTest(t)
	inventory, err := LoadInventory()
	if err != nil {
		t.Fatal(err)
	}
	packages, err := listGoPackages(root)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]listedPackage{}
	for _, pkg := range packages {
		listed[pkg.ImportPath] = pkg
	}
	declared := map[string][]GoTest{}
	for _, contract := range inventory.GoTests {
		if len(contract.Dependencies) > 0 {
			declared[contract.Package] = append(declared[contract.Package], contract)
		}
	}
	for importPath, contracts := range declared {
		pkg, ok := listed[importPath]
		if !ok {
			t.Errorf("declared Go test package %s is not a listed package", importPath)
			continue
		}
		fast, exclusions, err := noInfrastructureTests(pkg)
		if err != nil {
			t.Fatal(err)
		}
		for _, contract := range contracts {
			if slices.Contains(fast, contract.Name) {
				t.Errorf("Fast executes declared infrastructure test %s", contract.Name)
			}
			found := false
			for _, exclusion := range exclusions {
				if exclusion.Runnable != contract.Name {
					continue
				}
				found = true
				capabilities := append([]string{exclusion.Capability}, exclusion.Capabilities...)
				for _, dependency := range contract.Dependencies {
					if !slices.Contains(capabilities, dependency) {
						t.Errorf("Fast exclusion of %s omits declared dependency %s", contract.Name, dependency)
					}
				}
			}
			if !found {
				t.Errorf("Fast did not account for excluded declared test %s", contract.Name)
			}
		}
	}
	full, _, err := fullGoSelections(root, "declared dependency shard contract")
	if err != nil {
		t.Fatal(err)
	}
	producers, err := LoadPRProducers()
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for index := range producers.GoShards {
		shard, err := SelectPlan(Plan{Profile: ProfileFull, Selections: full}, []string{"go"}, index, len(producers.GoShards))
		if err != nil {
			t.Fatal(err)
		}
		for _, selection := range shard.Selections {
			for _, contracts := range declared {
				for _, contract := range contracts {
					if !slices.Contains(selection.Packages, contract.Package) || !slices.Contains(selection.Tests, contract.Name) {
						continue
					}
					counts[contract.Package+"."+contract.Name]++
					for _, dependency := range contract.Dependencies {
						if !slices.Contains(selection.Dependencies, dependency) || !slices.Contains(shard.Dependencies, dependency) {
							t.Errorf("race shard omitted %s for %s", dependency, contract.Name)
						}
					}
				}
			}
		}
	}
	for _, contracts := range declared {
		for _, contract := range contracts {
			if got := counts[contract.Package+"."+contract.Name]; got != 1 {
				t.Errorf("Full race shards execute %s %d times", contract.Name, got)
			}
		}
	}
}

func TestDeclaredGoDependenciesDoNotRequireSourceHints(t *testing.T) {
	root := t.TempDir()
	// This test has no environment reads or infrastructure helper calls.
	if err := os.WriteFile(filepath.Join(root, "composition_test.go"), []byte("package fixture\nfunc TestComposition() {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	pkg := listedPackage{ImportPath: "fixture", Dir: root, TestGoFiles: []string{"composition_test.go"}}
	contracts := []GoTest{{Package: "fixture", Name: "TestComposition", Dependencies: []string{"nats", "postgresql"}}}
	fast, excluded, err := classifyGoTests(pkg, contracts)
	if err != nil {
		t.Fatal(err)
	}
	if len(fast) != 0 || len(excluded) != 1 || excluded[0].Runnable != "TestComposition" {
		t.Fatalf("Fast disposition = selected %v, excluded %+v", fast, excluded)
	}
	if got := dependenciesForCapabilities(excluded); !slices.Equal(got, contracts[0].Dependencies) {
		t.Fatalf("execution dependencies = %v; want %v", got, contracts[0].Dependencies)
	}
}

func TestFastGoClassificationExcludesTransportDockerFixtures(t *testing.T) {
	root := t.TempDir()
	source := `package fixture
import (
	"testing"
	fixtures "github.com/tetral-ai/tetral/integration/transporttest"
)
func TestUndeclaredPostgreSQL(t *testing.T) { _ = fixtures.NewPostgreSQL(t) }
func TestCertificateOnly(t *testing.T) { _, _ = fixtures.NewAuthority("fixture") }
`
	if err := os.WriteFile(filepath.Join(root, "transport_test.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	pkg := listedPackage{ImportPath: "fixture", Dir: root, TestGoFiles: []string{"transport_test.go"}}
	fast, excluded, err := classifyGoTests(pkg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fast, []string{"TestCertificateOnly"}) || len(excluded) != 1 || excluded[0].Runnable != "TestUndeclaredPostgreSQL" || excluded[0].Capability != "docker" {
		t.Fatalf("Fast disposition = selected %v, excluded %+v", fast, excluded)
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
	root := t.TempDir()
	for name, body := range map[string]string{
		"go.mod":         "module example.com/fixture\n\ngo 1.22\n",
		"a/a.go":         "package a\n",
		"b/b.go":         "package b\nimport _ \"example.com/fixture/a\"\n",
		"c/c.go":         "package c\n",
		"c/c_test.go":    "package c\nimport _ \"example.com/fixture/b\"\n",
		"d/d.go":         "package d\n",
		"d/d_test.go":    "package d_test\nimport _ \"example.com/fixture/a\"\n",
		"unrelated/u.go": "package unrelated\n",
	} {
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(name)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	selected, err := affectedGoPackages(root, []string{"a/a.go"})
	if err != nil {
		t.Fatal(err)
	}
	// b imports a; c's tests import b; d's external tests import a.
	want := []string{"example.com/fixture/a", "example.com/fixture/b", "example.com/fixture/c", "example.com/fixture/d"}
	if !slices.Equal(selected, want) {
		t.Fatalf("affected packages = %v; want %v", selected, want)
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

func TestGoInventoryDistinguishesSuiteMainFromRunnableTest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		imports   string
		mainArg   string
		localType string
		want      []string
	}{
		{"suite_main", `import "testing"`, "m *testing.M", "*testing.T", []string{"TestLocal"}},
		{"aliased_suite_main", `import check "testing"`, "m *check.M", "*check.T", []string{"TestLocal"}},
		{"dot_import_suite_main", `import . "testing"`, "m *M", "*T", []string{"TestLocal"}},
		{"ordinary_test_named_main", `import "testing"`, "t *testing.T", "*testing.T", []string{"TestLocal", "TestMain"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			mainBody := ""
			if strings.HasPrefix(tc.mainArg, "m ") {
				mainBody = "m.Run()"
			}
			body := "package fixture\n" + tc.imports + "\nfunc TestMain(" + tc.mainArg + ") {" + mainBody + "}\nfunc TestLocal(t " + tc.localType + ") {}\n"
			if err := os.WriteFile(filepath.Join(root, "fixture_test.go"), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			pkg := listedPackage{ImportPath: "fixture", Dir: root, TestGoFiles: []string{"fixture_test.go"}}
			all, err := allGoRunnables(pkg)
			if err != nil {
				t.Fatal(err)
			}
			fast, excluded, err := classifyGoTests(pkg, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(all, tc.want) || !slices.Equal(fast, tc.want) || len(excluded) != 0 {
				t.Fatalf("inventory all/fast/excluded = %v/%v/%v; want runnable tests %v", all, fast, excluded, tc.want)
			}
		})
	}
}
