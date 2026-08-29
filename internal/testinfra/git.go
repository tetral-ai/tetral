package testinfra

import (
	"bytes"
	"fmt"
	"os/exec"
	"sort"
	"strings"
)

func inspectRevision(root, base string) Revision {
	revision := Revision{}
	shallow, err := gitOutput(root, "rev-parse", "--is-shallow-repository")
	if err != nil || shallow == "true" {
		revision.FullFallbackCause = "repository history is shallow or cannot be inspected"
		return revision
	}
	head, err := gitOutput(root, "rev-parse", "HEAD^{commit}")
	if err != nil {
		revision.FullFallbackCause = "HEAD is not a commit"
		return revision
	}
	revision.Head = head
	status, _ := gitOutput(root, "status", "--porcelain=v1", "--untracked-files=all")
	revision.WorktreeDirty = status != ""
	if base == "" {
		base = "refs/remotes/origin/main"
	}
	baseTip, err := gitOutput(root, "rev-parse", "--verify", base+"^{commit}")
	if err != nil {
		revision.FullFallbackCause = "base does not resolve to one local commit"
		return revision
	}
	revision.ResolvedBaseTip = baseTip
	comparison, err := gitOutput(root, "merge-base", baseTip, head)
	if err != nil || comparison == "" {
		revision.FullFallbackCause = "base and HEAD have no merge base"
		return revision
	}
	revision.ComparisonCommit = comparison

	paths := map[string]bool{}
	commands := [][]string{
		{"diff", "--name-only", "--no-renames", "-z", comparison + "..." + head},
		{"diff", "--cached", "--name-only", "--no-renames", "-z"},
		{"diff", "--name-only", "--no-renames", "-z"},
		{"ls-files", "--others", "--exclude-standard", "-z"},
	}
	for _, arguments := range commands {
		output, commandErr := gitBytes(root, arguments...)
		if commandErr != nil {
			revision.FullFallbackCause = "cannot enumerate repository change set"
			return revision
		}
		for _, path := range bytes.Split(output, []byte{0}) {
			if len(path) > 0 {
				paths[string(path)] = true
			}
		}
	}
	for path := range paths {
		revision.ChangedPaths = append(revision.ChangedPaths, path)
	}
	sort.Strings(revision.ChangedPaths)
	return revision
}

func gitOutput(root string, arguments ...string) (string, error) {
	output, err := gitBytes(root, arguments...)
	return strings.TrimSpace(string(output)), err
}

func gitBytes(root string, arguments ...string) ([]byte, error) {
	// Arguments are assembled by fixed repository-inspection call sites; no shell is used.
	//nolint:gosec
	command := exec.Command("git", arguments...)
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(arguments, " "), err)
	}
	return output, nil
}
