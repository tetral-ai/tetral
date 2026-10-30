package joint

import (
	"testing"
)

// TestRepositoriesAreClonedOnlyOnTheirComputer gives one of a Session's two
// cloud computers two repositories, one with a mount_path and one without.
// Its machine clones the first to the mount_path and the second to
// <working_directory>/<repository name>, with origin the github.com URL; the
// other computer's machine clones nothing, and the global Git configuration
// of the machines' user is unchanged.
func TestRepositoriesAreClonedOnlyOnTheirComputer(t *testing.T) {
	in := requireInputs(t, needs{})
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("the requests are recorded in-process with computer.EnsureRequest; no machine-bound call waits for them, since Engine does not route tool calls to a computer yet")
	record.boundary("the requests that answer 400, a user computer with repositories and two repositories with one path, create nothing and reach no machine; they are not repeated here")
	e := startEngine(t, record, engineOptions{})
	developer := e.developer()
	set := newMachines(t, in, needs{}, e, record)
	ctx := t.Context()

	appCommit := e.github.createRepository("acme", "app", "ghs_harness_app_token")
	docsCommit := e.github.createRepository("acme", "docs", "ghs_harness_docs_token")
	withRepositories, without := set.add("repositories", "provider-launcher"), set.add("plain", "provider-launcher")
	globalCode, globalConfig := withRepositories.shell("git", "config", "--global", "--list")
	mountPath := withRepositories.directory("docs-checkout")
	environments := []string{developer.createEnvironment(ctx, "repositories"), developer.createEnvironment(ctx, "plain")}
	session := developer.createSession(ctx, developer.createAgent(ctx, "repositories"),
		map[string]any{"environment_id": environments[0], "working_directory": withRepositories.directory("workspace"), "repositories": []any{
			map[string]any{"url": "https://github.com/acme/app", "authorization_token": "ghs_harness_app_token"},
			map[string]any{"url": "https://github.com/acme/docs", "authorization_token": "ghs_harness_docs_token", "mount_path": mountPath},
		}},
		map[string]any{"environment_id": environments[1], "working_directory": without.directory("workspace")})
	appPath := withRepositories.directory("workspace") + "/app"
	if paths := session.Computers[0].Repositories; len(paths) != 2 || paths[0].Path != appPath || paths[1].Path != mountPath {
		t.Fatalf("repositories of computer %s: %+v; want %s and %s", session.Computers[0].ID, paths, appPath, mountPath)
	}
	startClaimedMachine(t, e, withRepositories, cloudSession{sessionID: session.ID, computerID: session.Computers[0].ID, environmentID: environments[0]})
	startClaimedMachine(t, e, without, cloudSession{sessionID: session.ID, computerID: session.Computers[1].ID, environmentID: environments[1]})

	for _, clone := range []struct{ path, url, commit string }{
		{appPath, "https://github.com/acme/app", appCommit},
		{mountPath, "https://github.com/acme/docs", docsCommit},
	} {
		head := gitOutput(t, withRepositories, "-C", clone.path, "rev-parse", "HEAD")
		origin := gitOutput(t, withRepositories, "-C", clone.path, "config", "--local", "--get", "remote.origin.url")
		if head != clone.commit || origin != clone.url {
			t.Fatalf("the clone at %s: HEAD %s, origin %s; want %s and %s", clone.path, head, origin, clone.commit, clone.url)
		}
	}
	if code, entries := without.shell("ls", "-A", without.directory("workspace")); code != 0 || entries != "" {
		t.Fatalf("the other computer's working directory: ls exited %d with %q; want it empty", code, entries)
	}
	if code, config := withRepositories.shell("git", "config", "--global", "--list"); code != globalCode || config != globalConfig {
		t.Fatal("the global Git configuration of the machines' user changed during the preparations")
	}
	record.observe("acme/app cloned to %s and acme/docs to its mount_path %s, each at its commit with origin its github.com URL; the other computer's working directory is empty; the global Git configuration of the machines' user unchanged", appPath, mountPath)
}
