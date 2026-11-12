package ciflow

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestProductsForEachChangedPath(t *testing.T) {
	engine, cli, both := Products{Engine: true}, Products{CLI: true}, Products{Engine: true, CLI: true}
	for _, row := range []struct {
		name    string
		paths   []string
		unknown bool
		want    Products
	}{
		{name: "the CLI's tree", paths: []string{"cli/crates/tetral/src/main.rs"}, want: cli},
		{name: "the CLI's reference server", paths: []string{"cli/reference/wire/errors.go"}, want: cli},
		{name: "the contract between the products", paths: []string{"protocol/connection-v1.schema.json"}, want: both},
		{name: "the CLI's pull-request workflow", paths: []string{".github/workflows/cli-verification.yml"}, want: both},
		{name: "the CLI's dependency workflow", paths: []string{".github/workflows/cli-dependency-check.yml"}, want: both},
		{name: "the CLI's cache workflow", paths: []string{".github/workflows/cli-build-cache.yml"}, want: both},
		{name: "the CLI's cache action", paths: []string{".github/actions/cli-build-cache/action.yml"}, want: both},
		{name: "the CLI's verification script", paths: []string{"scripts/verify-cli.py"}, want: both},
		{name: "the CLI's bubblewrap installer", paths: []string{"scripts/install-bubblewrap.sh"}, want: both},
		{name: "the CLI's cargo-nextest installer", paths: []string{"scripts/install-cargo-nextest.sh"}, want: both},
		{name: "the CLI's cache script", paths: []string{"scripts/cli-build-cache.sh"}, want: both},
		{name: "an Engine service", paths: []string{"services/bridge/store.go"}, want: engine},
		{name: "Engine's pull-request workflow", paths: []string{".github/workflows/pull-request-verification.yml"}, want: engine},
		{name: "the root Go module", paths: []string{"go.mod", "go.sum"}, want: engine},
		{name: "a document", paths: []string{"docs/testing.md"}, want: engine},
		{name: "a directory no list names", paths: []string{"tools/new/thing.txt"}, want: engine},
		{name: "a file whose name only starts like the CLI's tree", paths: []string{"cli-notes.md"}, want: engine},
		{name: "a script beside the CLI's", paths: []string{"scripts/verify-cli.py.orig"}, want: engine},
		{name: "the CLI's tree and an Engine service", paths: []string{"cli/Cargo.lock", "services/gateway/package.json"}, want: both},
		{name: "no changed path", want: Products{}},
		{name: "paths that are not known", unknown: true, want: both},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := ProductsFor(row.paths, !row.unknown); got != row.want {
				t.Fatalf("ProductsFor(%q) = %+v, want %+v", row.paths, got, row.want)
			}
		})
	}
}

// The repository has a base branch that moved after the pull request left
// it, so the two ways of reading a range give different paths.
func TestChangedPathsOfEachEvent(t *testing.T) {
	directory := t.TempDir()
	git := func(arguments ...string) string {
		t.Helper()
		command := exec.Command("git", arguments...)
		command.Dir = directory
		command.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	commit := func(message string, files ...string) string {
		t.Helper()
		for _, file := range files {
			target := filepath.Join(directory, file)
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, []byte(message), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		git("add", "--all")
		git("commit", "--quiet", "--message", message)
		return git("rev-parse", "HEAD")
	}
	git("init", "--quiet", "--initial-branch", "main")
	fork := commit("start", "README.md")
	git("switch", "--quiet", "--create", "change")
	head := commit("the pull request", "cli/src/naïve name.rs")
	git("switch", "--quiet", "main")
	base := commit("the base branch moves", "services/bridge/store.go")
	afterPush := commit("a push of two commits", "cli/Cargo.lock")

	for _, row := range []struct {
		name                string
		event, from, to     string
		wantPaths           []string
		wantUnknown         bool
		wantEngine, wantCLI bool
	}{
		{name: "a pull request changes what its head holds since the merge base",
			event: "pull_request", from: base, to: head, wantPaths: []string{"cli/src/naïve name.rs"}, wantCLI: true},
		{name: "a push changes what lies between the two commits",
			event: "push", from: fork, to: afterPush, wantPaths: []string{"cli/Cargo.lock", "services/bridge/store.go"}, wantEngine: true, wantCLI: true},
		{name: "a push of the CLI's files only",
			event: "push", from: base, to: afterPush, wantPaths: []string{"cli/Cargo.lock"}, wantCLI: true},
		{name: "a forced push changes what either commit holds and the other lacks",
			event: "push", from: head, to: base, wantPaths: []string{"cli/src/naïve name.rs", "services/bridge/store.go"}, wantEngine: true, wantCLI: true},
		{name: "a push of a new branch has no commit before it",
			event: "push", from: strings.Repeat("0", 40), to: afterPush, wantUnknown: true, wantEngine: true, wantCLI: true},
		{name: "a base commit the checkout does not hold",
			event: "pull_request", from: strings.Repeat("a", 40), to: head, wantUnknown: true, wantEngine: true, wantCLI: true},
		{name: "a run started by hand",
			event: "workflow_dispatch", wantUnknown: true, wantEngine: true, wantCLI: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			paths, known := ChangedPaths(directory, row.event, row.from, row.to)
			if known == row.wantUnknown || strings.Join(paths, "\n") != strings.Join(row.wantPaths, "\n") {
				t.Fatalf("ChangedPaths = %q, known %v; want %q, known %v", paths, known, row.wantPaths, !row.wantUnknown)
			}
			if got, want := ProductsFor(paths, known), (Products{Engine: row.wantEngine, CLI: row.wantCLI}); got != want {
				t.Fatalf("products = %+v, want %+v", got, want)
			}
		})
	}
}
