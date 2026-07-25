package driver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"unicode"

	"github.com/daytonaio/daytona/libs/sdk-go/pkg/options"
	"github.com/daytonaio/daytona/libs/sdk-go/pkg/types"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/sandbox"
)

var gitHubRepositorySegmentRegexp = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]*\z`)

func (e *DaytonaHelperExecutor) InstalledGitTicketHash(ctx context.Context, providerSandboxID, gitProxyHost string) ([]byte, bool, error) {
	if err := validateGitProxyHost(gitProxyHost); err != nil {
		return nil, false, err
	}
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, providerSandboxID)
	if err != nil {
		return nil, false, err
	}
	output, err := e.executeGitHubPreparationCommand(ctx, sandboxHandle, "git config --global --get-regexp '^http\\.https://"+regexp.QuoteMeta(gitProxyHost)+"/\\.extraheader$' || true")
	if err != nil {
		return nil, false, err
	}
	prefix := gitticket.HeaderName + ": "
	var found []byte
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.EqualFold(fields[0], "http.https://"+gitProxyHost+"/.extraheader") || fields[1] != gitticket.HeaderName+":" || !strings.HasPrefix(strings.Join(fields[1:], " "), prefix) {
			continue
		}
		token := fields[2]
		hash, err := gitticket.HashToken(token)
		if err != nil || found != nil {
			return nil, false, errors.New("sandbox git ticket configuration is invalid")
		}
		found = hash
	}
	return found, found != nil, nil
}

func (e *DaytonaHelperExecutor) InstallGitHubRepositoryConfiguration(ctx context.Context, configuration sandbox.GitHubRepositoryConfiguration) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, configuration.ProviderSandboxID)
	if err != nil {
		return err
	}
	configCommand, err := githubRepositoryConfigCommand(configuration.GitProxyHost, configuration.Ticket, configuration.SessionID)
	if err != nil {
		return err
	}
	_, err = e.executeGitHubPreparationCommand(ctx, sandboxHandle, configCommand)
	return err
}

func (e *DaytonaHelperExecutor) CloneGitHubRepositories(ctx context.Context, preparation sandbox.GitHubRepositoryPreparation) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, preparation.ProviderSandboxID)
	if err != nil {
		return err
	}
	for _, repo := range preparation.Repositories {
		command, err := githubRepositoryCloneCommand(repo)
		if err != nil {
			return err
		}
		output, err := e.executeGitHubPreparationCommand(ctx, sandboxHandle, command)
		if err != nil {
			if gitHubCredentialFailure(output) {
				return &sandbox.GitHubPreparationFailure{
					Reason:      sandbox.GitHubCredentialRequiredReason,
					ResourceID:  repo.ResourceID,
					ResourceURL: repo.URL,
					Cause:       err,
				}
			}
			if gitHubRepositoryUnavailable(output) {
				return &sandbox.GitHubPreparationFailure{
					Reason:      sandbox.GitHubRepositoryUnavailableReason,
					ResourceID:  repo.ResourceID,
					ResourceURL: repo.URL,
					Cause:       err,
				}
			}
			return err
		}
	}
	return nil
}

func (e *DaytonaHelperExecutor) RemoveGitHubRepository(ctx context.Context, providerSandboxID string, repository sandbox.GitHubRepositoryMount) error {
	sandboxHandle, err := e.memoryProjectionSandbox(ctx, providerSandboxID)
	if err != nil {
		return err
	}
	mountPath := strings.TrimRight(repository.MountPath, "/")
	if mountPath == "" || !path.IsAbs(mountPath) || hasUnsafeGitHubMountPathCharacter(mountPath) || path.Clean(mountPath) != mountPath || mountPath == "/workspace" || !strings.HasPrefix(mountPath, "/workspace/") {
		return errors.New("github_repository mount_path must be under /workspace")
	}
	_, err = e.executeGitHubPreparationCommand(ctx, sandboxHandle, "rm -rf -- "+shellQuote(mountPath))
	return err
}

func githubRepositoryConfigCommand(gitProxyHost string, ticket string, sessionID string) (string, error) {
	if err := validateGitProxyHost(gitProxyHost); err != nil {
		return "", err
	}
	if ticket == "" {
		return "", errors.New("git ticket is required")
	}
	if sessionID == "" {
		return "", errors.New("session_id is required")
	}
	rewriteKey := "url.https://" + gitProxyHost + "/github.com/.insteadOf"
	headerKey := "http.https://" + gitProxyHost + "/.extraHeader"
	lines := []string{
		"set -eu",
		"git config --global --get-regexp '^url\\.https://.*/github\\.com/\\.insteadOf$' | while read -r key value; do if [ \"$value\" = 'https://github.com/' ]; then git config --global --unset-all \"$key\" || true; fi; done || true",
		"git config --global --replace-all " + shellQuote(rewriteKey) + " 'https://github.com/'",
		"git config --global --unset-all " + shellQuote(headerKey) + " || true",
		"git config --global --add " + shellQuote(headerKey) + " " + shellQuote(gitticket.HeaderName+": "+ticket),
		"git config --global --replace-all core.askPass /bin/true",
		"git config --global --replace-all credential.helper ''",
		"git config --global --replace-all user.name " + shellQuote("Tetral Agent"),
		"git config --global --replace-all user.email " + shellQuote("session+"+sessionID+"@agents.tetral.ai"),
		"profile=${HOME:-" + shellQuote("/home/"+RuntimeUser) + "}/.profile",
		"mkdir -p \"$(dirname \"$profile\")\"",
		"touch \"$profile\"",
		"grep -qxF 'export GIT_TERMINAL_PROMPT=0' \"$profile\" || printf '%s\\n' 'export GIT_TERMINAL_PROMPT=0' >> \"$profile\"",
		"export GIT_TERMINAL_PROMPT=0",
	}
	return strings.Join(lines, "\n"), nil
}

func githubRepositoryCloneCommand(repo sandbox.GitHubRepositoryMount) (string, error) {
	repoURL, err := canonicalGitHubRepositoryURL(repo.URL)
	if err != nil {
		return "", err
	}
	mountPath := strings.TrimRight(repo.MountPath, "/")
	if mountPath == "" || !path.IsAbs(mountPath) || hasUnsafeGitHubMountPathCharacter(mountPath) || path.Clean(mountPath) != mountPath || mountPath == "/" {
		return "", errors.New("github_repository mount_path must be absolute and lexically clean")
	}
	if mountPath == "/workspace" || !strings.HasPrefix(mountPath, "/workspace/") {
		return "", errors.New("github_repository mount_path must be under /workspace")
	}
	lines := []string{
		"set -eu",
		"export GIT_TERMINAL_PROMPT=0",
		"repo_url=" + shellQuote(repoURL),
		"target=" + shellQuote(mountPath),
		"same_origin=0",
		"if git -C \"$target\" rev-parse --is-inside-work-tree >/dev/null 2>&1; then origin=$(git -C \"$target\" config --get remote.origin.url 2>/dev/null || true); origin=${origin%.git}; if [ \"$origin\" = \"$repo_url\" ]; then same_origin=1; fi; fi",
		"if [ \"$same_origin\" = 1 ]; then exit 0; fi",
		"if [ -e \"$target\" ]; then echo 'github_repository target exists but is not the admitted origin' >&2; exit 65; fi",
		"mkdir -p " + shellQuote(path.Dir(mountPath)),
	}
	switch repo.CheckoutType {
	case "":
		lines = append(lines, "git clone \"$repo_url\" \"$target\"")
	case "branch":
		if repo.CheckoutRef == "" {
			return "", errors.New("github_repository branch checkout_ref is required")
		}
		lines = append(lines, "git clone --branch "+shellQuote(repo.CheckoutRef)+" --single-branch \"$repo_url\" \"$target\"")
	case "commit":
		if repo.CheckoutRef == "" {
			return "", errors.New("github_repository commit checkout_ref is required")
		}
		lines = append(lines,
			"git clone \"$repo_url\" \"$target\"",
			"if git -C \"$target\" checkout --detach "+shellQuote(repo.CheckoutRef)+"; then :; else checkout_status=$?; rm -rf -- \"$target\"; exit \"$checkout_status\"; fi",
		)
	default:
		return "", errors.New("github_repository checkout_type is invalid")
	}
	return strings.Join(lines, "\n"), nil
}

func (e *DaytonaHelperExecutor) executeGitHubPreparationCommand(ctx context.Context, sandboxHandle daytonaSandboxHandle, command string) (string, error) {
	if e.preparationCommandTimeout <= 0 {
		return "", errors.New("preparation command timeout is required")
	}
	response, err := sandboxHandle.Process.ExecuteCommand(ctx, runtimeUserShellCommand(command), options.WithExecuteTimeout(e.preparationCommandTimeout))
	if err != nil {
		return "", err
	}
	if response == nil {
		return "", errors.New("github_repository preparation command returned no response")
	}
	if response.ExitCode != 0 {
		return responseResult(response), fmt.Errorf("github_repository preparation command exited with code %d", response.ExitCode)
	}
	return responseResult(response), nil
}

func responseResult(response *types.ExecuteResponse) string {
	if response == nil {
		return ""
	}
	return response.Result
}

func gitHubCredentialFailure(output string) bool {
	normalized := strings.ToLower(output)
	return strings.Contains(normalized, "authentication failed") ||
		strings.Contains(normalized, "could not read username") ||
		strings.Contains(normalized, "terminal prompts disabled") ||
		strings.Contains(normalized, "authentication required") ||
		strings.Contains(normalized, "credential_required") ||
		strings.Contains(normalized, "requested url returned error: 403")
}

func gitHubRepositoryUnavailable(output string) bool {
	return strings.Contains(output, "Repository not found")
}

func canonicalGitHubRepositoryURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" {
		return "", errors.New("github_repository url is invalid")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("github_repository url is invalid")
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", errors.New("github_repository url is invalid")
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", errors.New("github_repository url is invalid")
	}
	repo, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", errors.New("github_repository url is invalid")
	}
	repo = strings.TrimSuffix(repo, ".git")
	if owner == "" || repo == "" {
		return "", errors.New("github_repository url is invalid")
	}
	if !safeGitHubRepositoryComponent(owner) || !safeGitHubRepositoryComponent(repo) {
		return "", errors.New("github_repository url is invalid")
	}
	return "https://github.com/" + owner + "/" + repo, nil
}

func safeGitHubRepositoryComponent(value string) bool {
	if !gitHubRepositorySegmentRegexp.MatchString(value) || strings.Contains(value, "/") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) || strings.ContainsRune("?#&=:@\\", r) {
			return false
		}
	}
	return true
}

func hasUnsafeGitHubMountPathCharacter(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return true
		}
	}
	return false
}

func validateGitProxyHost(host string) error {
	if host == "" || strings.Contains(host, "://") || strings.Contains(host, "/") {
		return errors.New("git proxy host is invalid")
	}
	for _, r := range host {
		if unicode.IsControl(r) || unicode.IsSpace(r) || strings.ContainsRune("'\"<>\\", r) {
			return errors.New("git proxy host is invalid")
		}
	}
	return nil
}
