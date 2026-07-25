package session

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateResourceRequestTypeClosureRejectsGitHubTokenOnOtherResourceTypes(t *testing.T) {
	for _, resourceType := range []string{string(ResourceTypeFile), string(ResourceTypeMemoryStore)} {
		t.Run(resourceType, func(t *testing.T) {
			err := validateResourceRequestTypeClosure(ResourceRequest{
				Type:               resourceType,
				AuthorizationToken: "github-token",
			})
			var validation *ValidationError
			if !errors.As(err, &validation) || validation.Message != "resource field is not allowed for type" {
				t.Fatalf("validateResourceRequestTypeClosure err = %T %v; want type-closure validation", err, err)
			}
		})
	}
}

func TestValidateCheckoutAcceptsDocumentedCommitSHAAndCanonicalizes(t *testing.T) {
	rawSHA := "ABCDEF0123456789ABCDEF0123456789ABCDEF01"

	checkout, err := validateCheckout(&GitHubCheckout{Type: "commit", SHA: rawSHA})
	if err != nil {
		t.Fatalf("validateCheckout: %v", err)
	}
	if checkout.Type != "commit" || checkout.SHA != strings.ToLower(rawSHA) {
		t.Fatalf("checkout = %+v; want canonical lowercase sha", checkout)
	}
}

func TestValidateCheckoutBranchAcceptsSafeBranchNames(t *testing.T) {
	for _, name := range []string{"main", "feature/x", "release-2026.05"} {
		t.Run(name, func(t *testing.T) {
			checkout, err := validateCheckout(&GitHubCheckout{Type: "branch", Name: name})
			if err != nil {
				t.Fatalf("validateCheckout: %v", err)
			}
			if checkout.Type != "branch" || checkout.Name != name {
				t.Fatalf("checkout = %+v; want branch %q", checkout, name)
			}
		})
	}
}

func TestValidateCheckoutBranchRejectsUnsafeGitNames(t *testing.T) {
	unsafeNames := []string{
		"",
		"@",
		"@{u}",
		"feature//x",
		"feature/",
		"-dash",
		"foo.lock",
		"feature/.lock",
		"feature..x",
		"feature x",
		"feature~x",
		"main.",
		".main",
		"HEAD",
		"refs/heads/main",
		"feature/\u202ex",
		"feature/\x7fx",
		strings.Repeat("a", 256),
	}
	for _, name := range unsafeNames {
		t.Run(name, func(t *testing.T) {
			_, err := validateCheckout(&GitHubCheckout{Type: "branch", Name: name})
			assertCheckoutValidationError(t, err)
			if name != "" && strings.Contains(err.Error(), name) {
				t.Fatalf("checkout error echoed unsafe name %q: %v", name, err)
			}
		})
	}
}

func TestValidateGitHubRepositoryURLCanonicalizesSupportedForms(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantURL  string
		wantRepo string
	}{
		{
			name:     "canonical",
			raw:      "https://github.com/tetral-ai/tetral",
			wantURL:  "https://github.com/tetral-ai/tetral",
			wantRepo: "tetral",
		},
		{
			name:     "git suffix",
			raw:      "https://github.com/tetral-ai/tetral.git",
			wantURL:  "https://github.com/tetral-ai/tetral",
			wantRepo: "tetral",
		},
		{
			name:     "uppercase git suffix is part of repo",
			raw:      "https://github.com/Tetral-AI/Tetral.GIT",
			wantURL:  "https://github.com/Tetral-AI/Tetral.GIT",
			wantRepo: "Tetral.GIT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotURL, gotRepo, err := validateGitHubRepositoryURL(test.raw)
			if err != nil {
				t.Fatalf("validateGitHubRepositoryURL: %v", err)
			}
			if gotURL != test.wantURL || gotRepo != test.wantRepo {
				t.Fatalf("validateGitHubRepositoryURL = %q, %q; want %q, %q", gotURL, gotRepo, test.wantURL, test.wantRepo)
			}
		})
	}
}

func TestValidateGitHubRepositoryURLRejectsAmbiguousDecodedPathComponents(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "encoded question mark in repo", raw: "https://github.com/tetral-ai/tetral%3Faccess_token=ghp_secret"},
		{name: "encoded fragment in repo", raw: "https://github.com/tetral-ai/tetral%23ghp_secret"},
		{name: "encoded slash in repo", raw: "https://github.com/tetral-ai/tet%2Fral"},
		{name: "encoded userinfo at sign in owner", raw: "https://github.com/tetral-ai%40github.com/tetral"},
		{name: "encoded userinfo colon in owner", raw: "https://github.com/tetral-ai%3Aghp_secret/tetral"},
		{name: "encoded backslash in repo", raw: "https://github.com/tetral-ai/tet%5Cral"},
		{name: "encoded space in repo", raw: "https://github.com/tetral-ai/tetral%20private"},
		{name: "encoded nonbreaking space in owner", raw: "https://github.com/tetral-ai%C2%A0/tetral"},
		{name: "encoded control character in repo", raw: "https://github.com/tetral-ai/tetral%00"},
		{name: "encoded format character in repo", raw: "https://github.com/tetral-ai/tetral%E2%80%AE"},
		{name: "encoded query key value material in repo", raw: "https://github.com/tetral-ai/tetral%3Ftoken%3Dghp_secret%26x%3D1"},
		{name: "owner leading underscore rejected by git proxy grammar", raw: "https://github.com/_tetral/tetral"},
		{name: "owner leading dash rejected by git proxy grammar", raw: "https://github.com/-tetral/tetral"},
		{name: "owner leading dot rejected by git proxy grammar", raw: "https://github.com/.tetral/tetral"},
		{name: "repo leading underscore rejected by git proxy grammar", raw: "https://github.com/tetral-ai/_tetral"},
		{name: "repo leading dash rejected by git proxy grammar", raw: "https://github.com/tetral-ai/-tetral"},
		{name: "repo leading dot rejected by git proxy grammar", raw: "https://github.com/tetral-ai/.tetral"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := validateGitHubRepositoryURL(test.raw)
			assertGitHubRepositoryURLValidationError(t, err)
			if strings.Contains(err.Error(), "ghp_secret") {
				t.Fatalf("validation error echoed token-like path material: %v", err)
			}
		})
	}
}

func TestNormalizeFileMountPathRejectsRootLevelFileResourcePaths(t *testing.T) {
	for _, raw := range []string{"/input.csv", "/workspace"} {
		t.Run(raw, func(t *testing.T) {
			_, err := normalizeFileMountPath(raw)
			var validation *ValidationError
			if !errors.As(err, &validation) || validation.Message != "mount_path must have a non-root parent directory" {
				t.Fatalf("normalizeMountPath err = %T %v; want non-root parent validation", err, err)
			}
		})
	}
}

func TestNormalizeFileMountPathAcceptsNestedAbsoluteFileResourcePaths(t *testing.T) {
	for _, raw := range []string{"/workspace/data.csv", "/project/data.csv", "/mnt/session/uploads/file_session_123"} {
		t.Run(raw, func(t *testing.T) {
			got, err := normalizeFileMountPath(raw)
			if err != nil {
				t.Fatalf("normalizeMountPath: %v", err)
			}
			if got != raw {
				t.Fatalf("normalizeMountPath = %q; want %q", got, raw)
			}
		})
	}
}

func assertCheckoutValidationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("validateCheckout must reject checkout")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "checkout is invalid" {
		t.Fatalf("message = %q; want generic checkout error", validation.Message)
	}
}

func assertGitHubRepositoryURLValidationError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("validateGitHubRepositoryURL must reject URL")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	if validation.Message != "invalid GitHub repository URL" {
		t.Fatalf("message = %q; want generic GitHub repository URL error", validation.Message)
	}
}
