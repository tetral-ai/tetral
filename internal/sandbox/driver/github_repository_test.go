package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/gitticket"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testGitTicket = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestGitHubRepositoryConfigCommandPinsExactGitConfig(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for exact config fixture")
	}
	home := t.TempDir()
	command, err := githubRepositoryConfigCommand("git.tetral.test", testGitTicket, "sesn_config")
	if err != nil {
		t.Fatalf("githubRepositoryConfigCommand: %v", err)
	}
	runShellWithHome(t, home, command)

	wantRewriteKey := "url.https://git.tetral.test/github.com/.insteadOf"
	assertGitConfigValue(t, home, wantRewriteKey, "https://github.com/")
	assertGitConfigValue(t, home, "http.https://git.tetral.test/.extraHeader", "X-Tetral-Git-Ticket: "+testGitTicket)
	assertGitConfigValue(t, home, "core.askPass", "/bin/true")
	assertGitConfigValue(t, home, "credential.helper", "")
	assertGitConfigValue(t, home, "user.name", "Tetral Agent")
	assertGitConfigValue(t, home, "user.email", "session+sesn_config@agents.tetral.ai")

	profile, err := os.ReadFile(filepath.Join(home, ".profile"))
	if err != nil {
		t.Fatalf("read profile: %v", err)
	}
	if strings.Count(string(profile), "GIT_TERMINAL_PROMPT=0") != 1 {
		t.Fatalf("profile = %q; want one GIT_TERMINAL_PROMPT export", string(profile))
	}
	if strings.Contains(string(profile), "git.tetral.test") || strings.Contains(string(profile), testGitTicket) {
		t.Fatalf("profile leaked proxy host or ticket: %q", string(profile))
	}
	envOutput := runShellWithHomeOutput(t, home, ". \"$HOME/.profile\" && env")
	if !strings.Contains(envOutput, "GIT_TERMINAL_PROMPT=0\n") {
		t.Fatalf("env output = %q; want GIT_TERMINAL_PROMPT=0", envOutput)
	}
	if strings.Count(envOutput, "git.tetral.test") != 0 || strings.Count(envOutput, testGitTicket) != 0 {
		t.Fatalf("env leaked proxy host or ticket: %q", envOutput)
	}
}

func TestGitHubRepositoryConfigCommandRotatesRewriteRuleWholesale(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for exact config fixture")
	}
	home := t.TempDir()
	oldCommand, err := githubRepositoryConfigCommand("git.tetral.test", testGitTicket, "sesn_config")
	if err != nil {
		t.Fatalf("old config command: %v", err)
	}
	runShellWithHome(t, home, oldCommand)
	newTicket := "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
	newCommand, err := githubRepositoryConfigCommand("git.tetral.test", newTicket, "sesn_config")
	if err != nil {
		t.Fatalf("new config command: %v", err)
	}
	runShellWithHome(t, home, newCommand)

	output := runGitConfig(t, home, "--global", "--get-regexp", "^url\\.https://.*github\\.com/\\.insteadOf$")
	if strings.Contains(output, testGitTicket) {
		t.Fatalf("git config still contains old rotated ticket: %s", output)
	}
	if strings.Contains(output, newTicket) || !strings.Contains(output, "url.https://git.tetral.test/github.com/.insteadof") {
		t.Fatalf("git rewrite config = %s; want ticketless rewrite", output)
	}
	assertGitConfigValue(t, home, "http.https://git.tetral.test/.extraHeader", "X-Tetral-Git-Ticket: "+newTicket)
	if got := strings.TrimSpace(runGit(t, home, "config", "--get-urlmatch", "http.extraHeader", "https://git.tetral.test/github.com/renamed/repo.git/info/refs")); got != "X-Tetral-Git-Ticket: "+newTicket {
		t.Fatalf("redirect follow-up extraHeader = %q; want rotated ticket", got)
	}
}

func TestDaytonaInstalledGitTicketHashRecoversPersistedConfiguration(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.results = []string{
		"http.https://git.tetral.test/.extraheader X-Tetral-Git-Ticket: " + testGitTicket + "\n",
	}
	executor := &DaytonaHelperExecutor{client: client, preparationCommandTimeout: 45 * time.Second}

	got, installed, err := executor.InstalledGitTicketHash(context.Background(), "provider_sandbox_123", "git.tetral.test")
	if err != nil {
		t.Fatalf("InstalledGitTicketHash: %v", err)
	}
	want, err := gitticket.HashToken(testGitTicket)
	if err != nil {
		t.Fatalf("HashToken: %v", err)
	}
	if !installed || !bytes.Equal(got, want) {
		t.Fatalf("installed hash = %x present=%v; want %x", got, installed, want)
	}
	if len(client.process.commands) != 1 || !strings.Contains(client.process.commands[0], "git config --global --get-regexp") {
		t.Fatalf("commands = %+v; want one persisted git-config query", client.process.commands)
	}
}

func TestDaytonaGitHubConfigurationAndCloneAreSeparatePhases(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := &DaytonaHelperExecutor{client: client, preparationCommandTimeout: 45 * time.Second}

	err := executor.InstallGitHubRepositoryConfiguration(context.Background(), sandbox.GitHubRepositoryConfiguration{
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_prepare",
		ProviderSandboxID: "provider_sandbox_123",
		GitProxyHost:      "git.tetral.test",
		Ticket:            testGitTicket,
	})
	if err != nil {
		t.Fatalf("InstallGitHubRepositoryConfiguration: %v", err)
	}
	err = executor.CloneGitHubRepositories(context.Background(), sandbox.GitHubRepositoryPreparation{
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_prepare",
		ProviderSandboxID: "provider_sandbox_123",
		Repositories: []sandbox.GitHubRepositoryMount{{
			ResourceID:   "sesrsc_repo",
			URL:          "https://github.com/tetral-ai/tetral.git",
			MountPath:    "/workspace/tetral",
			CheckoutType: "branch",
			CheckoutRef:  "main",
		}},
	})
	if err != nil {
		t.Fatalf("CloneGitHubRepositories: %v", err)
	}
	if len(client.process.commands) != 2 {
		t.Fatalf("commands = %d; want config + one clone", len(client.process.commands))
	}
	configCommand := client.process.commands[0]
	for _, required := range []string{
		"sudo -H -n -u " + shellQuote(RuntimeUser) + " sh -lc",
		"url.https://git.tetral.test/github.com/.insteadOf",
		"http.https://git.tetral.test/.extraHeader",
		"session+sesn_prepare@agents.tetral.ai",
		"credential.helper",
	} {
		if !strings.Contains(configCommand, required) {
			t.Fatalf("config command missing %q:\n%s", required, configCommand)
		}
	}
	if !strings.Contains(configCommand, "user.email") {
		t.Fatalf("config command missing pinned values:\n%s", configCommand)
	}
	cloneCommand := client.process.commands[1]
	for _, required := range []string{
		"sudo -H -n -u " + shellQuote(RuntimeUser) + " sh -lc",
		`repo_url=`,
		`https://github.com/tetral-ai/tetral`,
		`target=`,
		`/workspace/tetral`,
		"git -C \"$target\" config --get remote.origin.url",
		"if [ \"$same_origin\" = 1 ]; then exit 0; fi",
		"git clone --branch",
		"main",
		"--single-branch \"$repo_url\" \"$target\"",
	} {
		if !strings.Contains(cloneCommand, required) {
			t.Fatalf("clone command missing %q:\n%s", required, cloneCommand)
		}
	}
	if strings.Contains(cloneCommand, "git.tetral.test") || strings.Contains(cloneCommand, testGitTicket) || strings.Contains(cloneCommand, "rm -rf") {
		t.Fatalf("clone command leaked proxy config or would destroy worktree:\n%s", cloneCommand)
	}
}

func TestGitHubRepositoryCloneCommandComparesUnrewrittenOrigin(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for exact config fixture")
	}
	home := t.TempDir()
	repoDir := filepath.Join(t.TempDir(), "tetral")
	runGit(t, home, "init", repoDir)
	runGit(t, home, "-C", repoDir, "remote", "add", "origin", "https://github.com/tetral-ai/tetral.git")
	runGit(t, home, "config", "--global", "url.https://git.tetral.test/github.com/.insteadOf", "https://github.com/")
	runGit(t, home, "config", "--global", "http.https://git.tetral.test/.extraHeader", "X-Tetral-Git-Ticket: "+testGitTicket)

	expanded := strings.TrimSpace(runGit(t, home, "-C", repoDir, "remote", "get-url", "origin"))
	raw := strings.TrimSpace(runGit(t, home, "-C", repoDir, "config", "--get", "remote.origin.url"))
	if !strings.Contains(expanded, "git.tetral.test/github.com") || strings.Contains(expanded, testGitTicket) {
		t.Fatalf("remote get-url = %q; want proxy-expanded URL under insteadOf", expanded)
	}
	if raw != "https://github.com/tetral-ai/tetral.git" {
		t.Fatalf("config --get remote.origin.url = %q; want admitted natural URL", raw)
	}

	command, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:       "https://github.com/tetral-ai/tetral",
		MountPath: "/workspace/tetral",
	})
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	if strings.Contains(command, "remote get-url origin") || !strings.Contains(command, "config --get remote.origin.url") {
		t.Fatalf("clone command must compare unrewritten origin:\n%s", command)
	}
}

func TestGitHubRepositoryRemovesDeletedWorkingTree(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)
	if err := executor.RemoveGitHubRepository(context.Background(), "provider_git_delete", sandbox.GitHubRepositoryMount{MountPath: "/workspace/tetral"}); err != nil {
		t.Fatalf("RemoveGitHubRepository: %v", err)
	}
	if got := strings.Join(client.process.commands, "\n"); !strings.Contains(got, "rm -rf --") || !strings.Contains(got, "/workspace/tetral") {
		t.Fatalf("remove command = %q; want checkout rm -rf", got)
	}
	if len(client.process.opts) != 1 || client.process.opts[0].Timeout == nil || *client.process.opts[0].Timeout != 45*time.Second {
		t.Fatalf("GitHub removal timeout options = %+v; want exact configured 45s", client.process.opts)
	}
}

func TestGitHubRepositoryPropagatesRemovalFailure(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCode = 23
	executor := NewDaytonaHelperExecutorForClientWithPreparationTimeout(client, 45*time.Second)

	err := executor.RemoveGitHubRepository(context.Background(), "provider_git_delete", sandbox.GitHubRepositoryMount{MountPath: "/workspace/tetral"})
	if err == nil {
		t.Fatal("RemoveGitHubRepository succeeded; want command failure")
	}
	if len(client.process.commands) != 1 {
		t.Fatalf("remove commands = %d; want 1", len(client.process.commands))
	}
}

func TestGitHubRepositoryCloneCommandCommitCheckout(t *testing.T) {
	command, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:          "https://github.com/tetral-ai/tetral",
		MountPath:    "/workspace/tetral",
		CheckoutType: "commit",
		CheckoutRef:  "0123456789abcdef0123456789abcdef01234567",
	})
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	if !strings.Contains(command, "git clone \"$repo_url\" \"$target\"") ||
		!strings.Contains(command, "if git -C \"$target\" checkout --detach '0123456789abcdef0123456789abcdef01234567'; then :; else checkout_status=$?; rm -rf -- \"$target\"; exit \"$checkout_status\"; fi") {
		t.Fatalf("commit clone command =\n%s", command)
	}
}

func TestGitHubRepositoryCloneCommandCommitCheckoutFailureRemovesOnlyNewTarget(t *testing.T) {
	if _, err := exec.LookPath("sh"); err != nil {
		t.Skip("sh binary is required for checkout cleanup fixture")
	}
	fixtureRoot := t.TempDir()
	binDir := filepath.Join(fixtureRoot, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatalf("mkdir fake git bin: %v", err)
	}
	fakeGit := `#!/bin/sh
if [ "$1" = "-C" ]; then
  target=$2
  operation=$3
  case "$operation" in
    rev-parse) [ -f "$target/.worktree" ] ;;
    config) cat "$target/.origin" ;;
    checkout) exit 17 ;;
    *) exit 99 ;;
  esac
elif [ "$1" = "clone" ]; then
  mkdir -p "$3"
  touch "$3/.default-head"
else
  exit 99
fi
`
	if err := os.WriteFile(filepath.Join(binDir, "git"), []byte(fakeGit), 0o755); err != nil {
		t.Fatalf("write fake git: %v", err)
	}

	newTarget := filepath.Join(fixtureRoot, "new-target")
	command := commitCheckoutCommandForTarget(t, newTarget)
	cmd := exec.Command("sh", "-c", command)
	cmd.Env = append(os.Environ(), "PATH="+binDir+":"+os.Getenv("PATH"))
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("commit checkout command succeeded; want checkout failure\n%s", output)
	}
	if _, err := os.Stat(newTarget); !os.IsNotExist(err) {
		t.Fatalf("new clone target stat error = %v; want target removed", err)
	}

	existingTarget := filepath.Join(fixtureRoot, "existing-target")
	if err := os.Mkdir(existingTarget, 0o755); err != nil {
		t.Fatalf("mkdir existing target: %v", err)
	}
	for name, content := range map[string]string{
		".worktree": "",
		".origin":   "https://github.com/someone-else/tetral\n",
		"changes":   "preserve me",
	} {
		if err := os.WriteFile(filepath.Join(existingTarget, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write existing target fixture %s: %v", name, err)
		}
	}
	command = commitCheckoutCommandForTarget(t, existingTarget)
	cmd = exec.Command("sh", "-c", command)
	cmd.Env = append(os.Environ(), "PATH="+binDir+":"+os.Getenv("PATH"))
	if output, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("existing wrong-origin command succeeded; want refusal\n%s", output)
	}
	changes, err := os.ReadFile(filepath.Join(existingTarget, "changes"))
	if err != nil {
		t.Fatalf("read preserved existing worktree: %v", err)
	}
	if string(changes) != "preserve me" {
		t.Fatalf("existing worktree changes = %q; want preserved", changes)
	}
}

func commitCheckoutCommandForTarget(t *testing.T, target string) string {
	t.Helper()
	command, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:          "https://github.com/tetral-ai/tetral",
		MountPath:    "/workspace/tetral",
		CheckoutType: "commit",
		CheckoutRef:  "0123456789abcdef0123456789abcdef01234567",
	})
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	command = strings.Replace(command, "target='/workspace/tetral'", "target="+shellQuote(target), 1)
	return strings.Replace(command, "mkdir -p '/workspace'", "mkdir -p "+shellQuote(filepath.Dir(target)), 1)
}

func TestGitHubRepositoryCloneCommandAllowsExplicitPathUnderWorkspace(t *testing.T) {
	command, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:       "https://github.com/tetral-ai/tetral",
		MountPath: "/workspace/repos/tetral",
	})
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	if !strings.Contains(command, `target='/workspace/repos/tetral'`) ||
		!strings.Contains(command, `mkdir -p '/workspace/repos'`) ||
		!strings.Contains(command, `git clone "$repo_url" "$target"`) {
		t.Fatalf("clone command =\n%s\nwant explicit workspace target preserved", command)
	}
}

func TestGitHubRepositoryCloneCommandRejectsProxyInvalidRepositoryComponents(t *testing.T) {
	for _, rawURL := range []string{
		"https://github.com/_tetral/tetral",
		"https://github.com/-tetral/tetral",
		"https://github.com/.tetral/tetral",
		"https://github.com/tetral-ai/_tetral",
		"https://github.com/tetral-ai/-tetral",
		"https://github.com/tetral-ai/.tetral",
	} {
		t.Run(rawURL, func(t *testing.T) {
			if _, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
				URL:       rawURL,
				MountPath: "/workspace/tetral",
			}); err == nil {
				t.Fatal("githubRepositoryCloneCommand accepted a repo URL that git-proxy route grammar rejects")
			}
		})
	}
}

func TestGitHubRepositoryCloneCommandRejectsExplicitPathOutsideWorkspace(t *testing.T) {
	if _, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:       "https://github.com/tetral-ai/tetral",
		MountPath: "/tmp/repos/tetral",
	}); err == nil {
		t.Fatal("githubRepositoryCloneCommand accepted outside-workspace mount path")
	}
}

func TestGitHubRepositoryCloneCommandRejectsMalformedMountPath(t *testing.T) {
	if _, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:       "https://github.com/tetral-ai/tetral",
		MountPath: "/tmp/repos/te\x00tral",
	}); err == nil {
		t.Fatal("githubRepositoryCloneCommand succeeded with NUL mount path; want validation error")
	}
}

func TestDaytonaPrepareGitHubRepositoriesClassifiesCredentialFailure(t *testing.T) {
	for _, output := range []string{
		"fatal: Authentication failed for 'https://github.com/tetral-ai/private/'",
		"remote: credential_required\nfatal: unable to access 'https://github.com/tetral-ai/private/': The requested URL returned error: 424",
		"fatal: unable to access 'https://github.com/tetral-ai/private/': The requested URL returned error: 403",
	} {
		t.Run(output, func(t *testing.T) {
			client := newRecordingMemoryProjectionClient()
			client.process.exitCodes = []int{128}
			client.process.results = []string{output}
			executor := &DaytonaHelperExecutor{client: client, preparationCommandTimeout: 45 * time.Second}

			err := executor.CloneGitHubRepositories(context.Background(), sandbox.GitHubRepositoryPreparation{
				WorkspaceID:       workspace.DefaultID,
				SessionID:         "sesn_prepare",
				ProviderSandboxID: "provider_sandbox_123",
				Repositories: []sandbox.GitHubRepositoryMount{{
					ResourceID: "sesrsc_repo",
					URL:        "https://github.com/tetral-ai/private",
					MountPath:  "/workspace/private",
				}},
			})
			if !sandbox.IsGitHubCredentialRequired(err) {
				t.Fatalf("err = %T %v; want github_credential_required", err, err)
			}
		})
	}
}

func TestTPREP6DaytonaPreparationCapturesFirstFailingRepositoryIdentity(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCodes = []int{0, 128}
	client.process.results = []string{"clone completed", "remote: Repository not found.\nfatal: repository unavailable"}
	executor := &DaytonaHelperExecutor{client: client, preparationCommandTimeout: 45 * time.Second}

	err := executor.CloneGitHubRepositories(context.Background(), sandbox.GitHubRepositoryPreparation{
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_prepare",
		ProviderSandboxID: "provider_sandbox_123",
		Repositories: []sandbox.GitHubRepositoryMount{
			{
				ResourceID: "sesrsc_ready",
				URL:        "https://github.com/tetral-ai/ready",
				MountPath:  "/workspace/ready",
			},
			{
				ResourceID: "sesrsc_missing",
				URL:        "https://github.com/tetral-ai/missing",
				MountPath:  "/workspace/missing",
			},
		},
	})
	if !sandbox.IsGitHubRepositoryUnavailable(err) {
		t.Fatalf("err = %T %v; want github_repository_unavailable", err, err)
	}
	var failure *sandbox.GitHubPreparationFailure
	if !errors.As(err, &failure) || failure.ResourceID != "sesrsc_missing" || failure.ResourceURL != "https://github.com/tetral-ai/missing" {
		t.Fatalf("failure = %+v; want failing repository identity", failure)
	}
	if gitHubRepositoryUnavailable("remote: repository not found") {
		t.Fatal("lowercase transcript classified as the contract's Repository not found manifestation")
	}
	if len(client.process.commands) != 2 ||
		!strings.Contains(client.process.commands[0], "/workspace/ready") ||
		!strings.Contains(client.process.commands[1], "/workspace/missing") {
		t.Fatalf("clone commands = %#v; want first repository success before second repository failure", client.process.commands)
	}
}

func runShellWithHome(t *testing.T, home string, script string) {
	t.Helper()
	_ = runShellWithHomeOutput(t, home, script)
}

func runShellWithHomeOutput(t *testing.T, home string, script string) string {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(), "HOME="+home)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("script failed: %v\n%s\nscript:\n%s", err, string(output), script)
	}
	return string(output)
}

func assertGitConfigValue(t *testing.T, home string, key string, want string) {
	t.Helper()
	got := strings.TrimSuffix(runGitConfig(t, home, "--global", "--get", key), "\n")
	if got != want {
		t.Fatalf("git config %s = %q; want %q", key, got, want)
	}
}

func runGitConfig(t *testing.T, home string, args ...string) string {
	t.Helper()
	return runGit(t, home, append([]string{"config"}, args...)...)
}

func runGit(t *testing.T, home string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Env = append(os.Environ(), "HOME="+home)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(output))
	}
	return string(output)
}
