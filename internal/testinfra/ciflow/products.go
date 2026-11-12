package ciflow

import (
	"os/exec"
	"path"
	"strings"
)

// Products are the products whose jobs a change runs.
type Products struct {
	Engine bool
	CLI    bool
}

// cliTree is the CLI's own tree: a change there runs the CLI's jobs only.
const cliTree = "cli/"

// sharedPaths run both products' jobs. protocol/ is the contract between
// them. The CLI's workflows, its cache action and the scripts only its CI
// runs are the CLI's, and Engine's internal/testinfra tests read them.
var sharedPaths = []string{
	"protocol/",
	".github/workflows/cli-*.yml",
	".github/actions/cli-build-cache/",
	"scripts/verify-cli.py",
	"scripts/install-bubblewrap.sh",
	"scripts/install-cargo-nextest.sh",
	"scripts/cli-build-cache.sh",
}

// ProductsFor decides from the paths a change touches. Every path outside the
// CLI's tree runs Engine's jobs. A change whose paths are not known runs
// both.
func ProductsFor(paths []string, known bool) Products {
	if !known {
		return Products{Engine: true, CLI: true}
	}
	var products Products
	for _, changed := range paths {
		switch {
		case strings.HasPrefix(changed, cliTree):
			products.CLI = true
		case shared(changed):
			products.Engine, products.CLI = true, true
		default:
			products.Engine = true
		}
	}
	return products
}

func shared(changed string) bool {
	for _, pattern := range sharedPaths {
		if strings.HasSuffix(pattern, "/") {
			if strings.HasPrefix(changed, pattern) {
				return true
			}
		} else if matched, _ := path.Match(pattern, changed); matched {
			return true
		}
	}
	return false
}

// ChangedPaths lists what an event changes in the repository at directory. A
// pull request changes what its head holds since its merge base with the base
// branch; a push changes what lies between the commit before it and the
// commit after. Any other event, and a range git cannot read, is not known.
func ChangedPaths(directory, event, base, head string) (paths []string, known bool) {
	var revisions string
	switch event {
	case "pull_request":
		revisions = base + "..." + head
	case "push":
		revisions = base + ".." + head
	default:
		return nil, false
	}
	// -z prints each path as it is; without it git quotes a path that holds
	// a non-ASCII byte, and the quoted form matches no pattern.
	// The revisions are the commits the workflow's event names.
	//nolint:gosec
	command := exec.Command("git", "diff", "--name-only", "--no-renames", "-z", revisions, "--")
	command.Dir = directory
	output, err := command.Output()
	if err != nil {
		return nil, false
	}
	for _, changed := range strings.Split(string(output), "\x00") {
		if changed != "" {
			paths = append(paths, changed)
		}
	}
	return paths, true
}
