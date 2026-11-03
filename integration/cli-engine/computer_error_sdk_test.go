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
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/computer"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/protocol"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// Real Linux CLI preparation/closure and public SDK event reads. Admission
// uses lifecycle owners in-process until production tool/removal paths land.
func TestRequiredRepositoryFailureReachesSDK(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"linux"}})
	requireSDK(t)
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("development evidence: cloud requests and repreparation use computer.EnsureRequest/PrepareAgain in-process; production tool admission is not exercised")
	e := startEngine(t, record, engineOptions{})
	developer, controller := e.developer(), e.controller()
	set := newMachines(t, in, needs{}, e, record)
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
		request := e.recordRequest(cloudSession{sessionID: session.ID, computerID: computerID, environmentID: environmentID})
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
				e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
					issued, err := computer.PrepareAgain(ctx, tx, workspace.ID(harnessWorkspace), session.ID, computerID)
					if err == nil && !issued {
						t.Fatal("repreparation issued no new binding")
					}
					return err
				})
				record.command("in-process computer.PrepareAgain for computer %s after its failed checkout", computerID)
			}
			result := developer.waitComputer(ctx, session.ID, computerID, time.Minute, "repository preparation result", func(c computerObject) bool { return c.ExecutionState == "failed" || c.ExecutionState == "ready" })
			if result.BindingID == nil || *result.BindingID == previousBinding {
				t.Fatalf("preparation %d did not report a new binding: %+v", attempt, result)
			}
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
				e.assertSDKComputerErrors(session.ID, computerID, "repository_preparation_failed", "")
				record.observe("preparation %d on binding %s: nonexistent required checkout branch produced failed/repository_preparation_failed with repository path and checkout cause; no ready state or published clone; SDK still consumed exactly one matching retrying preparation error", attempt, *result.BindingID)
			} else {
				if result.ExecutionState != "ready" || result.Error != nil {
					t.Fatalf("success control: %+v", result)
				}
				if head := gitOutput(t, m, "-C", root, "rev-parse", "HEAD"); head != commit {
					t.Fatalf("success control HEAD %s, want %s", head, commit)
				}
				e.assertSDKComputerErrors(session.ID, computerID, "", "")
				record.observe("success control prepared cwd inside the real clone at %s; SDK read no computer error", commit)
			}
		}
	}
}

func TestCleanupFailureReachesSDKOnce(t *testing.T) {
	in := requireInputs(t, needs{platforms: []string{"linux"}})
	requireSDK(t)
	if os.Geteuid() == 0 {
		t.Fatal("cleanup failure proof requires an unprivileged test user")
	}
	record := newAcceptanceRecord(t, in, platformOf(in))
	record.boundary("development evidence: cloud requests and session.close are issued through computer.EnsureRequest/IssueClose in-process; public removal admission is not exercised")
	e := startEngine(t, record, engineOptions{})
	developer := e.developer()
	set := newMachines(t, in, needs{}, e, record)
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
		startClaimedMachine(t, e, m, cloudSession{sessionID: session.ID, computerID: computerID, environmentID: environmentID})
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
		var closeID string
		e.ownerTx(func(ctx context.Context, tx *dbconnect.Tx) error {
			var err error
			closeID, err = computer.IssueClose(ctx, tx, workspace.ID(harnessWorkspace), session.ID, computerID)
			return err
		})
		if closeID == "" {
			t.Fatal("no close instruction issued")
		}
		developer.waitComputer(ctx, session.ID, computerID, time.Minute, "closed report", func(c computerObject) bool { return c.ExecutionState == "closed" })
		captured, report := e.waitClosedReport(closeID)
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

func (e *engine) waitClosedReport(closeID string) (capturedReport, protocol.SessionStatus) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		e.edge.mu.Lock()
		reports := append([]capturedReport(nil), e.edge.reports...)
		e.edge.mu.Unlock()
		for _, captured := range reports {
			report, err := protocol.DecodeSessionStatus(captured.body)
			if err == nil && report.RequestMessageID == closeID && report.Status == "closed" && captured.status == http.StatusOK {
				return captured, report
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	e.t.Fatal("no accepted actual CLI closed report observed")
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
