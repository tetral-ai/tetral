package joint

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
)

// requiredRepositoryFailureReachesSDK prepares a cloud computer whose
// working directory is inside a repository whose requested branch does not
// exist: the call that started the machine and the call after it each
// prepare the computer with a new binding, which fails with
// repository_preparation_failed naming the repository and the checkout,
// publishes no clone and ends the call not executed; the SDK reads one
// computer error for the repeated code. A control with the remote default
// checkout becomes ready inside its clone and runs the call there.
func requiredRepositoryFailureReachesSDK(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	requireSDK(t)
	developer, controller := e.developer(), e.controller()
	ctx := t.Context()
	const token = "ghs_harness_required_repository_token"
	commit := e.github.createRepository("acme", "required", token)
	for _, bad := range []bool{true, false} {
		name := "success"
		if bad {
			name = "missing-branch"
		}
		m := set.add(name, "provider-launcher")
		root := m.directory("repository")
		repository := map[string]any{"url": "https://github.com/acme/required", "authorization_token": token, "mount_path": root}
		if bad {
			repository["checkout"] = map[string]any{"type": "branch", "name": "branch-that-does-not-exist"}
		}
		environmentID := developer.createEnvironment(ctx, name)
		session := developer.createSession(ctx, developer.createAgent(ctx, name), map[string]any{"environment_id": environmentID, "working_directory": root + "/work", "repositories": []any{repository}})
		computerID := session.Computers[0].ID
		rs := e.startRuntime().bind(session.ID)
		call, request := waitingCall(t, e, rs, cloudSession{sessionID: session.ID, computerID: computerID, environmentID: environmentID}, "pwd")
		claimed := controller.claimRequest(ctx, request.ID, "worker-"+name)
		instanceID, credentialID := claimed.bootstrapFields(t)
		m.configure(claimed.Bootstrap)
		m.serve("--idle-exit", "15m")
		m.waitStatus(time.Minute, "connection online", func(status map[string]any) bool {
			return statusField(status, "connection_state") == "online" && statusField(status, "instance_id") == instanceID && statusField(status, "credential_id") == credentialID
		})
		controller.waitInstance(ctx, instanceID, time.Minute, "connection online", func(object instanceObject) bool {
			return object.Connection != nil && object.Connection.State == "online"
		})
		attempts := 1
		if bad {
			attempts = 2
		}
		previousBinding := ""
		for attempt := 1; attempt <= attempts; attempt++ {
			if attempt > 1 {
				call = rs.exec(computerID, "pwd")
			}
			result := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "repository preparation result", func(c computerObject) bool {
				return (c.ExecutionState == "failed" || c.ExecutionState == "ready") && c.BindingID != nil && *c.BindingID != previousBinding
			})
			previousBinding = *result.BindingID
			if bad {
				if result.ExecutionState != "failed" || result.Error == nil || result.Error.Code != "repository_preparation_failed" {
					t.Fatalf("required repository preparation %d: %+v", attempt, result)
				}
				if !strings.Contains(result.Error.Message, root) || !strings.Contains(result.Error.Message, "checkout") {
					t.Fatalf("preparation %d error does not identify the repository and checkout failure: %q", attempt, result.Error.Message)
				}
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Fatalf("failed checkout %d published repository: %v", attempt, err)
				}
				if settled := rs.await(call.use, time.Minute); settled.Settlement != "not_executed" || settled.Code != "repository_preparation_failed" {
					t.Fatalf("call of preparation %d settled %+v; want not_executed with repository_preparation_failed", attempt, settled)
				}
				e.assertSDKComputerErrors(session.ID, computerID, "repository_preparation_failed", "")
				record.observe("preparation %d on binding %s: nonexistent required checkout branch produced failed/repository_preparation_failed with repository path and checkout cause; no ready state or published clone; the call ended not executed with that code; SDK still consumed exactly one matching retrying preparation error", attempt, *result.BindingID)
			} else {
				if result.ExecutionState != "ready" || result.Error != nil {
					t.Fatalf("success control: %+v", result)
				}
				if head := gitOutput(t, m, "-C", root, "rev-parse", "HEAD"); head != commit {
					t.Fatalf("success control HEAD %s, want %s", head, commit)
				}
				rs.requireCompleted(call, time.Minute, root+"/work")
				e.assertSDKComputerErrors(session.ID, computerID, "", "")
				record.observe("success control prepared cwd inside the real clone at %s and ran the call there; SDK read no computer error", commit)
			}
		}
	}
}

// cleanupFailureReachesSDKOnce archives the Session of a prepared cloud
// computer with a repository. Where the repository's root cannot be deleted
// (its parent, outside the owned root, denies writes), the CLI's closed
// report lists the root with its error and the SDK reads one terminal
// computer_cleanup_error; replaying the report's exact bytes returns the
// stored ACK and writes no second event. The control deletes the root and
// has no cleanup error.
func cleanupFailureReachesSDKOnce(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	requireSDK(t)
	if os.Geteuid() == 0 {
		t.Fatal("cleanup failure proof requires an unprivileged test user")
	}
	record.boundary("the root cannot be deleted because its parent denies writes to the unprivileged test user, not because a file in it is owned by root")
	developer := e.developer()
	ctx := t.Context()
	const token = "ghs_harness_cleanup_repository_token"
	e.github.createRepository("acme", "cleanup", token)
	for _, bad := range []bool{true, false} {
		name := "success"
		if bad {
			name = "delete-failure"
		}
		m := set.add(name, "provider-launcher")
		parent := m.directory("owned-parent")
		root := filepath.Join(parent, "repository")
		environmentID := developer.createEnvironment(ctx, name)
		session := developer.createSession(ctx, developer.createAgent(ctx, name), map[string]any{"environment_id": environmentID, "working_directory": m.directory("workspace"), "repositories": []any{map[string]any{"url": "https://github.com/acme/cleanup", "authorization_token": token, "mount_path": root}}})
		computerID := session.Computers[0].ID
		startClaimedMachine(t, e, e.startRuntime().bind(session.ID), m, cloudSession{sessionID: session.ID, computerID: computerID, environmentID: environmentID})
		if bad {
			// The CLI repairs permissions within an owned cleanup root. Its
			// parent is outside that root: removing the root itself must fail.
			probe := filepath.Join(parent, "deletion-probe")
			if err := os.WriteFile(probe, []byte("probe"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(parent, 0500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(parent, 0700) })
			if err := os.Remove(probe); !os.IsPermission(err) {
				t.Fatalf("failure setup: %v; want permission denied", err)
			}
		}
		developer.archiveSession(ctx, session.ID)
		var closeID string
		e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
			return tx.QueryRow(ctx, `SELECT COALESCE(b.close_message_id,'') FROM computers c JOIN computer_bindings b ON b.workspace_id=c.workspace_id AND b.binding_id=c.binding_id WHERE c.workspace_id=$1 AND c.id=$2`, harnessWorkspace, computerID).Scan(&closeID)
		})
		if closeID == "" {
			t.Fatal("no close instruction issued")
		}
		captured, report := e.waitClosedReport(closeID, time.Minute)
		if bad {
			if len(report.CleanupFailures.Value) == 0 || report.CleanupFailures.Value[0].Path != root || report.CleanupFailures.Value[0].Message == "" {
				t.Fatalf("CLI cleanup diagnostics: %+v", report.CleanupFailures)
			}
			if _, err := os.Stat(root); err != nil {
				t.Fatalf("expected residual owned directory: %v", err)
			}
			e.assertSDKComputerErrors(session.ID, computerID, "", root)
		} else {
			if report.CleanupFailures.Set {
				t.Fatalf("success report contains cleanup failures: %+v", report.CleanupFailures)
			}
			if _, err := os.Stat(root); !os.IsNotExist(err) {
				t.Fatalf("success control retained clone: %v", err)
			}
			e.assertSDKComputerErrors(session.ID, computerID, "", "")
		}
		// Replay the actual authenticated bytes observed at the public edge.
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.serverURL+"/v1/client/session-status", bytes.NewReader(captured.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header = captured.headers.Clone()
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || !bytes.Equal(body, captured.ack) {
			t.Fatalf("close replay status %d, ACK differs=%t", response.StatusCode, !bytes.Equal(body, captured.ack))
		}
		path := ""
		if bad {
			path = root
		}
		e.assertSDKComputerErrors(session.ID, computerID, "", path)
		record.observe("%s control: actual CLI closed report accepted; SDK consumed %d terminal cleanup error(s); replay returned the identical ACK and SDK event count remained unchanged", name, map[bool]int{true: 1, false: 0}[bad])
	}
}

func (e *engine) waitClosedReport(closeID string, timeout time.Duration) (capturedReport, protocol.SessionStatus) {
	e.t.Helper()
	return e.waitReport(timeout, "closed report", func(report protocol.SessionStatus) bool {
		return report.RequestMessageID == closeID && report.Status == "closed"
	})
}

// waitReport waits up to timeout for a session-status report that the
// edge saw accepted and that match accepts.
func (e *engine) waitReport(timeout time.Duration, what string, match func(protocol.SessionStatus) bool) (capturedReport, protocol.SessionStatus) {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		e.edge.mu.Lock()
		reports := append([]capturedReport(nil), e.edge.reports...)
		e.edge.mu.Unlock()
		for _, captured := range reports {
			report, err := protocol.DecodeSessionStatus(captured.body)
			if err == nil && captured.status == http.StatusOK && match(report) {
				return captured, report
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	e.t.Fatalf("no accepted actual CLI %s observed within %s", what, timeout)
	return capturedReport{}, protocol.SessionStatus{}
}

func requireSDK(t *testing.T) string {
	t.Helper()
	root := os.Getenv("TETRAL_ENGINE_SDK_ROOT")
	if root == "" {
		t.Fatal("TETRAL_ENGINE_SDK_ROOT is required for the real SDK consumer")
	}
	return root
}

func (e *engine) assertSDKComputerErrors(sessionID, computerID, prepareCode, cleanupPath string) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(e.t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", "--eval", computerErrorSDKProof)
	command.Env = append(os.Environ(), "SDK_ENTRY="+filepath.Join(requireSDK(e.t), "src/index.ts"), "SDK_API="+e.serverURL, "SDK_KEY="+e.apiKey, "SESSION_ID="+sessionID, "COMPUTER_ID="+computerID, "PREPARE_CODE="+prepareCode, "CLEANUP_PATH="+cleanupPath)
	output, err := command.CombinedOutput()
	if err != nil {
		e.t.Fatalf("real SDK event consumption: %v\n%s", err, output)
	}
	var proof struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(output, &proof); err != nil {
		e.t.Fatalf("SDK proof output: %v", err)
	}
	e.record.observe("real SDK sessions.events.list consumed %d computer errors from the authenticated public HTTP API", proof.Count)
}

const computerErrorSDKProof = `
import assert from 'node:assert/strict';
const {default: Anthropic} = await import(process.env.SDK_ENTRY);
const client = new Anthropic({apiKey:process.env.SDK_KEY,baseURL:process.env.SDK_API});
const page = await client.beta.sessions.events.list(process.env.SESSION_ID, {limit:100});
const errors = page.data.filter(e => e.type === 'session.error' && e.error.computer_id === process.env.COMPUTER_ID);
assert.equal(errors.length, process.env.PREPARE_CODE || process.env.CLEANUP_PATH ? 1 : 0);
for (const event of errors) {
 assert.equal(event.session_thread_id, undefined);
 if (process.env.PREPARE_CODE) {
  assert.equal(event.error.type,'computer_preparation_error');
  assert.equal(event.error.code,process.env.PREPARE_CODE);
  assert.equal(event.error.retry_status.type,'retrying');
 } else {
  assert.equal(event.error.type,'computer_cleanup_error');
  assert.equal(event.error.retry_status.type,'terminal');
  assert.deepEqual(event.error.cleanup_failures.map(f => f.path),[process.env.CLEANUP_PATH]);
  assert.ok(event.error.cleanup_failures[0].message.length > 0);
 }
}
console.log(JSON.stringify({count:errors.length}));
`

// failedOuterRepositoryDoesNotBlockRepreparation prepares a cloud
// computer whose repositories are submitted inner first, the outer one with a
// token GitHub refuses: the outer clone fails without a destination, the
// inner one does no Git work and reports its failed parent, and an unrelated
// sibling clones. After the outer token is rotated, the machine is stopped
// and a call wakes it; the new binding prepares all three clones, with the
// paths as submitted, plain and as lexical aliases.
func failedOuterRepositoryDoesNotBlockRepreparation(t *testing.T, e *engine, set *machines, record *acceptanceRecord) {
	developer, controller := e.developer(), e.controller()
	ctx := t.Context()
	const token = "ghs_harness_nested_retry_token"
	outerCommit := e.github.createRepository("acme", "app", token)
	innerCommit := e.github.createRepository("acme", "lib", token)
	siblingCommit := e.github.createRepository("acme", "apple", token)
	for _, alias := range []bool{false, true} {
		name := "plain"
		if alias {
			name = "lexical-alias"
		}
		m := set.add(name, "provider-launcher")
		root := m.directory("repositories")
		outer, inner, sibling := root+"/app", root+"/app/lib", root+"/apple"
		if alias {
			outer, inner = root+"/./app", root+"/app//lib"
		}
		environmentID := developer.createEnvironment(ctx, name)
		session := developer.createSession(ctx, developer.createAgent(ctx, name), map[string]any{
			"environment_id": environmentID, "working_directory": m.directory("workspace"),
			"repositories": []any{
				map[string]any{"url": "https://github.com/acme/lib", "authorization_token": token, "mount_path": inner},
				map[string]any{"url": "https://github.com/acme/app", "authorization_token": "ghs_harness_wrong_token", "mount_path": outer},
				map[string]any{"url": "https://github.com/acme/apple", "authorization_token": token, "mount_path": sibling},
			},
		})
		computerID := session.Computers[0].ID
		if repos := session.Computers[0].Repositories; len(repos) != 3 || repos[0].Path != inner || repos[1].Path != outer || repos[2].Path != sibling {
			t.Fatalf("stored repository paths/order changed: %+v", repos)
		}
		innerRequests := len(e.github.requestsOf("acme/lib"))
		cloud := cloudSession{sessionID: session.ID, computerID: computerID, environmentID: environmentID}
		rs := e.startRuntime().bind(session.ID)
		server, instanceID := startClaimedMachine(t, e, rs, m, cloud)
		first := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "first preparation", func(c computerObject) bool { return c.ExecutionState == "ready" })
		if first.BindingID == nil {
			t.Fatal("first preparation has no binding")
		}
		report := e.waitPreparationReport(session.ID, *first.BindingID)
		if report.Status != "prepared" || len(report.RepositoryFailures.Value) != 2 {
			t.Fatalf("first report = %+v; want two repository failures", report)
		}
		failures := report.RepositoryFailures.Value
		if failures[0].Path != outer || failures[1].Path != inner || failures[1].Cause != "clone" || failures[1].Message != "parent repository "+outer+" is not ready" {
			t.Fatalf("failed-parent report = %+v", failures)
		}
		if _, err := os.Stat(outer); !os.IsNotExist(err) {
			t.Fatalf("failed outer destination was created: %v", err)
		}
		if len(e.github.requestsOf("acme/lib")) != innerRequests {
			t.Fatal("inner repository reached Git despite its failed ancestor")
		}
		if head := gitOutput(t, m, "-C", sibling, "rev-parse", "HEAD"); head != siblingCommit {
			t.Fatalf("sibling clone HEAD %s; want %s", head, siblingCommit)
		}
		inventory := filepath.Join(m.home, "sessions", session.ID, "repositories.json")
		listed := func() []string {
			t.Helper()
			data, err := os.ReadFile(inventory)
			if err != nil {
				t.Fatal(err)
			}
			var paths []string
			if err := json.Unmarshal(data, &paths); err != nil {
				t.Fatal(err)
			}
			return paths
		}
		if paths := listed(); len(paths) != 1 || paths[0] != sibling {
			t.Fatalf("first ownership inventory = %v; want sibling only", paths)
		}
		var resources struct {
			Data []struct {
				ID  string `json:"id"`
				URL string `json:"url"`
			} `json:"data"`
		}
		developer.get(ctx, "/v1/sessions/"+session.ID+"/resources?beta=true", &resources)
		outerResourceID := ""
		for _, resource := range resources.Data {
			if resource.URL == "https://github.com/acme/app" {
				outerResourceID = resource.ID
			}
		}
		if outerResourceID == "" {
			t.Fatal("outer repository resource is missing")
		}
		var rotated map[string]any
		developer.post(ctx, "/v1/sessions/"+session.ID+"/resources/"+outerResourceID+"?beta=true", map[string]any{"authorization_token": token}, http.StatusOK, &rotated)
		stopMachine(t, controller, server, instanceID)
		call, wake := waitingCall(t, e, rs, cloud, "pwd")
		claimed := controller.claimRequest(ctx, wake.ID, "worker-wake-"+name)
		_, credentialID := claimed.bootstrapFields(t)
		m.configure(claimed.Bootstrap)
		m.serve("--idle-exit", "15m")
		requireCloudOnline(t, e, m, instanceID, credentialID, cloud)
		rs.requireCompleted(call, time.Minute, m.directory("workspace"))
		second := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "new preparation result", func(c computerObject) bool {
			return c.BindingID != nil && *c.BindingID != *first.BindingID && (c.ExecutionState == "ready" || c.ExecutionState == "failed")
		})
		if second.ExecutionState != "ready" || second.Error != nil {
			t.Fatalf("second preparation = %+v; want ready without repository_path_exists", second)
		}
		if report := e.waitPreparationReport(session.ID, *second.BindingID); report.Status != "prepared" || len(report.RepositoryFailures.Value) != 0 {
			t.Fatalf("second preparation report = %+v", report)
		}
		for path, want := range map[string]string{outer: outerCommit, inner: innerCommit, sibling: siblingCommit} {
			if head := gitOutput(t, m, "-C", path, "rev-parse", "HEAD"); head != want {
				t.Fatalf("clone %s HEAD %s; want %s", path, head, want)
			}
		}
		paths := listed()
		if len(paths) != 3 || !slices.Contains(paths, outer) || !slices.Contains(paths, inner) || !slices.Contains(paths, sibling) {
			t.Fatalf("retry ownership inventory = %v", paths)
		}
		record.observe("%s inner-first preparation: failed outer clone left no destination, descendant did no Git work, sibling succeeded; token rotation and new binding prepared all three clones with unchanged path spellings in the ownership inventory", name)
	}
}

func (e *engine) waitPreparationReport(sessionID, bindingID string) protocol.SessionStatus {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		e.edge.mu.Lock()
		reports := append([]capturedReport(nil), e.edge.reports...)
		e.edge.mu.Unlock()
		for _, captured := range reports {
			report, err := protocol.DecodeSessionStatus(captured.body)
			if err == nil && report.SessionID == sessionID && report.BindingID == bindingID && (report.Status == "prepared" || report.Status == "failed") && captured.status == http.StatusOK {
				return report
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	e.t.Fatal("no accepted actual CLI preparation report observed for current binding")
	return protocol.SessionStatus{}
}
