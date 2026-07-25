package githubrepo

import "testing"

func TestComparisonKeyTrimsLowercaseGitBeforeFoldingOwnerAndRepository(t *testing.T) {
	tests := map[string]string{
		"https://github.com/Tetral-AI/Repo-Case":     "https://github.com/tetral-ai/repo-case",
		"https://github.com/Tetral-AI/Repo-Case.git": "https://github.com/tetral-ai/repo-case",
		"https://github.com/Tetral-AI/Repo-Case.GIT": "https://github.com/tetral-ai/repo-case.git",
	}
	for input, want := range tests {
		if got := ComparisonKey(input); got != want {
			t.Fatalf("ComparisonKey(%q) = %q; want %q", input, got, want)
		}
	}
}
