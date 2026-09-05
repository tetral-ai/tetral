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
	executor := &DaytonaHelperExecutor{client: client, commandTimeout: 45 * time.Second}

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
	executor := &DaytonaHelperExecutor{client: client, commandTimeout: 45 * time.Second}

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
		"if [ \"$same_origin\" != 1 ]; then",
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
	executor := NewDaytonaHelperExecutorForClientWithCommandTimeout(client, 45*time.Second)
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
	executor := NewDaytonaHelperExecutorForClientWithCommandTimeout(client, 45*time.Second)

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
			executor := &DaytonaHelperExecutor{client: client, commandTimeout: 45 * time.Second}

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

func TestDaytonaGitHubMaterializationCapturesFirstFailingRepositoryIdentity(t *testing.T) {
	client := newRecordingMemoryProjectionClient()
	client.process.exitCodes = []int{0, 128}
	client.process.results = []string{"clone completed", "remote: Repository not found.\nfatal: repository unavailable"}
	executor := &DaytonaHelperExecutor{client: client, commandTimeout: 45 * time.Second}

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
	var failure *sandbox.GitHubMaterializationFailure
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

func TestGitHubRepositoryCloneCommandInstallsConfiguredIdentityOnFreshClone(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for clone fixture")
	}
	home := t.TempDir()
	sourceRoot := t.TempDir()
	source := filepath.Join(sourceRoot, "tetral-ai", "tetral")
	runGit(t, home, "init", source)
	runGit(t, home, "-C", source, "config", "user.name", "Source Fixture")
	runGit(t, home, "-C", source, "config", "user.email", "source@example.test")
	runShellWithHome(t, home, "cd "+shellQuote(source)+" && git commit --allow-empty -m seed")
	// Route the admitted github.com URL at the local source so the clone runs offline.
	runGit(t, home, "config", "--global", "url.file://"+sourceRoot+"/.insteadOf", "https://github.com/")

	target := filepath.Join(t.TempDir(), "workspace", "tetral")
	command := retargetCloneCommand(t, sandbox.GitHubRepositoryMount{
		ResourceID:       "sesrsc_identity",
		URL:              "https://github.com/tetral-ai/tetral",
		MountPath:        "/workspace/tetral",
		GitIdentityName:  "Example Automation",
		GitIdentityEmail: "example-automation@users.noreply.github.com",
	}, target)
	runShellWithHome(t, home, command)

	if got := strings.TrimSpace(runGit(t, home, "-C", target, "config", "--local", "--get", "user.name")); got != "Example Automation" {
		t.Fatalf("local user.name = %q; want configured identity", got)
	}
	if got := strings.TrimSpace(runGit(t, home, "-C", target, "config", "--local", "--get", "user.email")); got != "example-automation@users.noreply.github.com" {
		t.Fatalf("local user.email = %q; want configured identity", got)
	}
	if got := runShellWithHomeOutput(t, home, "git config --global --get user.name || echo MISSING"); strings.TrimSpace(got) != "MISSING" {
		t.Fatalf("global user.name = %q; want the global identity left unset by the clone phase", got)
	}
	// An ordinary commit picks the configured identity up as author and committer.
	log := runShellWithHomeOutput(t, home,
		"cd "+shellQuote(target)+" && git commit -q --allow-empty -m probe && git log -1 --format='%an <%ae>|%cn <%ce>'")
	want := "Example Automation <example-automation@users.noreply.github.com>|Example Automation <example-automation@users.noreply.github.com>"
	if strings.TrimSpace(log) != want {
		t.Fatalf("commit identity = %q; want %q", strings.TrimSpace(log), want)
	}
}

func TestGitHubRepositoryCloneCommandKeepsIdentitiesRepositoryLocalAcrossMounts(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for clone fixture")
	}
	home := t.TempDir()
	sourceRoot := t.TempDir()
	for _, repo := range []string{"alpha", "beta"} {
		source := filepath.Join(sourceRoot, "tetral-ai", repo)
		runGit(t, home, "init", source)
	}
	runGit(t, home, "config", "--global", "url.file://"+sourceRoot+"/.insteadOf", "https://github.com/")
	runGit(t, home, "config", "--global", "user.name", "Tetral Agent")
	runGit(t, home, "config", "--global", "user.email", "session+sesn_multi@agents.tetral.ai")

	workspace := t.TempDir()
	targets := map[string]string{"alpha": filepath.Join(workspace, "alpha"), "beta": filepath.Join(workspace, "beta")}
	identities := map[string][2]string{
		"alpha": {"Alpha Automation", "alpha@users.noreply.github.com"},
		"beta":  {"Beta Automation", "beta@users.noreply.github.com"},
	}
	for _, repo := range []string{"alpha", "beta"} {
		command := retargetCloneCommand(t, sandbox.GitHubRepositoryMount{
			ResourceID:       "sesrsc_" + repo,
			URL:              "https://github.com/tetral-ai/" + repo,
			MountPath:        "/workspace/" + repo,
			GitIdentityName:  identities[repo][0],
			GitIdentityEmail: identities[repo][1],
		}, targets[repo])
		runShellWithHome(t, home, command)
	}
	for _, repo := range []string{"alpha", "beta"} {
		if got := strings.TrimSpace(runGit(t, home, "-C", targets[repo], "config", "--local", "--get", "user.name")); got != identities[repo][0] {
			t.Fatalf("%s local user.name = %q; want %q", repo, got, identities[repo][0])
		}
		if got := strings.TrimSpace(runGit(t, home, "-C", targets[repo], "config", "--local", "--get", "user.email")); got != identities[repo][1] {
			t.Fatalf("%s local user.email = %q; want %q", repo, got, identities[repo][1])
		}
	}
	// The Sandbox-global identity remains the untouched fallback.
	assertGitConfigValue(t, home, "user.name", "Tetral Agent")
	assertGitConfigValue(t, home, "user.email", "session+sesn_multi@agents.tetral.ai")
}

func TestGitHubRepositoryCloneCommandReappliesIdentityForAdmittedOrigin(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary is required for clone fixture")
	}
	home := t.TempDir()
	target := filepath.Join(t.TempDir(), "workspace", "tetral")
	runGit(t, home, "init", target)
	runGit(t, home, "-C", target, "remote", "add", "origin", "https://github.com/tetral-ai/tetral.git")

	command := retargetCloneCommand(t, sandbox.GitHubRepositoryMount{
		ResourceID:       "sesrsc_recovered",
		URL:              "https://github.com/tetral-ai/tetral",
		MountPath:        "/workspace/tetral",
		GitIdentityName:  "Recovered Automation",
		GitIdentityEmail: "recovered@users.noreply.github.com",
	}, target)
	if strings.Contains(command, "git clone") && !strings.Contains(command, "if [ \"$same_origin\" != 1 ]; then") {
		t.Fatalf("clone command lost the same-origin guard:\n%s", command)
	}
	runShellWithHome(t, home, command)

	if got := strings.TrimSpace(runGit(t, home, "-C", target, "config", "--local", "--get", "user.name")); got != "Recovered Automation" {
		t.Fatalf("local user.name = %q; want identity reapplied without a fresh clone", got)
	}
	if got := strings.TrimSpace(runGit(t, home, "-C", target, "config", "--local", "--get", "user.email")); got != "recovered@users.noreply.github.com" {
		t.Fatalf("local user.email = %q; want identity reapplied without a fresh clone", got)
	}
}

func TestGitHubRepositoryCloneCommandOmitsIdentityLinesWhenUnconfigured(t *testing.T) {
	command, err := githubRepositoryCloneCommand(sandbox.GitHubRepositoryMount{
		URL:       "https://github.com/tetral-ai/tetral",
		MountPath: "/workspace/tetral",
	})
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	if strings.Contains(command, "config user.name") || strings.Contains(command, "config user.email") {
		t.Fatalf("clone command installs an identity without one declared:\n%s", command)
	}
	if !strings.Contains(command, "if [ \"$same_origin\" != 1 ]; then") {
		t.Fatalf("clone command lost the same-origin guard:\n%s", command)
	}
}

func TestGitHubRepositoryCloneCommandRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mount sandbox.GitHubRepositoryMount
	}{
		{name: "name only", mount: sandbox.GitHubRepositoryMount{GitIdentityName: "Only Name"}},
		{name: "email only", mount: sandbox.GitHubRepositoryMount{GitIdentityEmail: "only@example.test"}},
		{name: "newline in name", mount: sandbox.GitHubRepositoryMount{GitIdentityName: "bad\nname", GitIdentityEmail: "ok@example.test"}},
		{name: "newline in email", mount: sandbox.GitHubRepositoryMount{GitIdentityName: "Ok", GitIdentityEmail: "bad\n@example.test"}},
		{name: "space in email", mount: sandbox.GitHubRepositoryMount{GitIdentityName: "Ok", GitIdentityEmail: "bad @example.test"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.mount.URL = "https://github.com/tetral-ai/tetral"
			tc.mount.MountPath = "/workspace/tetral"
			if _, err := githubRepositoryCloneCommand(tc.mount); err == nil {
				t.Fatal("githubRepositoryCloneCommand accepted an invalid git identity")
			}
		})
	}
}

// retargetCloneCommand builds the clone command for one mount and rewrites its
// fixed /workspace target at the local fixture path, mirroring the admitted
// mount the Sandbox would hold.
func retargetCloneCommand(t *testing.T, mount sandbox.GitHubRepositoryMount, target string) string {
	t.Helper()
	command, err := githubRepositoryCloneCommand(mount)
	if err != nil {
		t.Fatalf("githubRepositoryCloneCommand: %v", err)
	}
	command = strings.Replace(command, "target="+shellQuote(mount.MountPath), "target="+shellQuote(target), 1)
	return strings.Replace(command, "mkdir -p "+shellQuote("/workspace"), "mkdir -p "+shellQuote(filepath.Dir(target)), 1)
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
