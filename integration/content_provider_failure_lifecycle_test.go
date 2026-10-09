package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

// The fault is released only after the original completed Tool has a durable
// route and an observed Runtime owner. A failed provider wait must close before
// that independent Tool/approval owner is allowed to settle.
func TestContentProviderFailureLifecycle(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	for _, variant := range []struct {
		name, failure string
		approval      bool
	}{
		{name: "normal-complete-content"},
		{name: "resource-held-tool", failure: "resource"},
		{name: "resource-approval", failure: "resource", approval: true},
		{name: "transport-held-tool", failure: "transport"},
		{name: "transport-approval", failure: "transport", approval: true},
	} {
		t.Run(variant.name, func(t *testing.T) {
			spec := contentReadCase
			mode := "full_access"
			if variant.approval {
				mode = "ask_for_approval"
				spec = contentToolLifecycleCase{tool: "Write", scenario: "durable-write", callID: "call-write-note", input: `{"content":"first\n","file_path":"/workspace/note.txt"}`, expectedOutput: "{\n  \"bytes_written\": 6,\n  \"created\": false\n}", expectedFile: "first\n", captureOutput: true}
			}
			scenario := spec.scenario
			if variant.failure == "resource" {
				scenario = "resource-after-tool"
				if variant.approval {
					scenario = "resource-after-write"
				}
			}
			provider := &contentCycleProvider{entries: map[string]*contentCycleCommand{}, root: t.TempDir()}
			c := startContentE2EWithOptions(t, scenario, false, false, contentE2EOptions{
				Provider: provider, StopProvider: provider.finishAll, ApprovalMode: mode,
				Runtime: map[string]any{"controlCommands": true, "approvalMode": mode},
				Gateway: map[string]any{"holdFinish": true, "followupScenario": "done", "measureResources": true},
			})
			entry := provider.prepare(t, c.session, spec, true)
			t.Cleanup(entry.finish)
			c.sdk.control(t, "send", map[string]any{"sessionId": c.session, "text": "start-content-fixture"})
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_use'`, c.session, 1)
			var toolID, requestID string
			if err := c.db.QueryRow(`SELECT event_id,model_request_id FROM session_events WHERE session_id=$1 AND type='agent.tool_use'`, c.session).Scan(&toolID, &requestID); err != nil {
				t.Fatal(err)
			}
			if variant.approval {
				waitContentFailureApprovalOwner(t, c, toolID)
				if entry.starts.Load() != 0 {
					t.Fatal("pending public approval dispatched the Tool")
				}
			} else {
				waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running' AND provider_command_reference_json IS NOT NULL`, c.session, 1)
				assertContentHeldOwner(t, c, c.session)
			}
			assertContentFailurePrefix(t, c, requestID, spec)
			// Runtime termination retires this binding. Preserve its actual
			// immutable address to inspect the released owner after that commit.
			originalScope := contentFailureRuntimeScope(t, c)
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'`, c.session, 0)
			if variant.failure == "transport" {
				c.gateway.control(t, map[string]any{"kind": "cut_provider_rpc"}, "provider_rpc_cut")
			} else {
				c.gateway.control(t, map[string]any{"kind": "release_finish"}, "released_finish")
			}
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'`, c.session, 1)
			// These observations precede release of both external completion and
			// the public approval decision. End is not a claim that all work stops.
			observed := waitContentFailureProviderJoined(t, c)
			var firstCalls int
			var credentialStore string
			if json.Unmarshal(observed["providerCalls"], &firstCalls) != nil || firstCalls != 1 || json.Unmarshal(observed["credentialStore"], &credentialStore) != nil || credentialStore != "sql" {
				t.Fatal("original post-output Gateway attempt replayed or bypassed installed SQL credentials")
			}
			assertContentFailureEnd(t, c, requestID, variant.failure)
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, c.session, 0)
			if variant.failure == "resource" {
				assertContentAdmissionFailures(t, observed["admissionFailures"], "provider_stream_limit_exceeded", 1)
			}
			if variant.approval {
				waitContentFailureApprovalOwner(t, c, toolID)
				if entry.starts.Load() != 0 {
					t.Fatal("failed provider wait bypassed public approval")
				}
				c.sdk.control(t, "confirm", map[string]any{"sessionId": c.session, "toolUseEventId": toolID, "result": "allow"})
				waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running' AND provider_command_reference_json IS NOT NULL`, c.session, 1)
			} else {
				assertContentHeldOwner(t, c, c.session)
			}
			entry.finish()
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, c.session, 1)
			wantStatus := "idle"
			if variant.failure == "resource" && !variant.approval {
				wantStatus = "terminated"
			}
			waitContentFailureStatus(t, c, wantStatus)
			if variant.failure == "resource" {
				assertContentResourceCloseout(t, c, variant.approval)
			}
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results r JOIN session_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.consumed_by_terminal_event_id WHERE r.session_id=$1 AND r.execution_state='consumed' AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=r.tool_use_event_id`, c.session, 1)
			if entry.starts.Load() != 1 || entry.active.Load() != 0 {
				t.Fatalf("original Tool execution/join=%d/%d", entry.starts.Load(), entry.active.Load())
			}
			data, err := os.ReadFile(entry.tool.path)
			if err != nil || string(data) != spec.expectedFile {
				t.Fatalf("original Tool effect=%q error=%v", data, err)
			}
			wantTexts := []string{"alpha", "beta", "done"}
			wantRequests := 2
			if variant.failure == "resource" {
				wantTexts = wantTexts[:2]
				wantRequests = 1
			}
			assertContentSDKTextHistory(t, c, wantTexts)
			observed = waitContentFailureProviderJoined(t, c)
			var calls int
			if json.Unmarshal(observed["providerCalls"], &calls) != nil || calls != wantRequests {
				t.Fatalf("native adapter calls=%s want%d", observed["providerCalls"], wantRequests)
			}
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_start'`, c.session, wantRequests)
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'`, c.session, wantRequests)
			wantRetries := 0
			if variant.failure == "transport" {
				wantRetries = 1
			}
			// Pending approval yields after the rescheduled End. Its later run
			// resumes that durable action directly, without the hot retry log.
			if !variant.approval || variant.failure != "transport" {
				waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error' AND payload_json::jsonb #>> '{error,retry_status,type}'='retrying'`, c.session, wantRetries)
			}
			if variant.failure == "" {
				assertContentLifecycleHistory(t, c.db, c.session, spec)
				assertContentNativeContext(t, observed["nativeContexts"], spec)
				waitContentSQLCount(t, c.db, `SELECT count(*) FROM request_usage_details WHERE session_id=$1 AND input_total_tokens=1 AND output_total_tokens=4`, c.session, 2)
			}
			if wantStatus == "terminated" {
				waitContentFailureTerminalOwners(t, c, originalScope)
			} else {
				waitContentFailureCleanup(t, c)
			}
			assertContentFailureGatewayEmpty(t, observed)
		})
	}
}

func assertContentFailureGatewayEmpty(t *testing.T, observed map[string]json.RawMessage) {
	t.Helper()
	var assembly map[string]*int64
	if json.Unmarshal(observed["assemblyTotals"], &assembly) != nil || len(assembly) != 5 {
		t.Fatal("all-request assembler ownership observation unavailable")
	}
	for name, count := range assembly {
		if count == nil || *count != 0 {
			t.Fatalf("assembler owner %s remains: %s", name, observed["assemblyTotals"])
		}
	}
	var writer struct {
		PendingBytes, PendingCallbacks, WriteCalls *int64
	}
	if json.Unmarshal(observed["writer"], &writer) != nil || writer.PendingBytes == nil || writer.PendingCallbacks == nil || writer.WriteCalls == nil || *writer.PendingBytes != 0 || *writer.PendingCallbacks != 0 || *writer.WriteCalls == 0 {
		t.Fatalf("RPC writer ownership unavailable or not joined: %s", observed["writer"])
	}
}

func waitContentFailureApprovalOwner(t *testing.T, c *contentE2E, toolID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reply := c.runtimeControl(t, "inspect", c.session)
		var thread struct {
			Observed bool `json:"observed"`
			Pending  bool `json:"hasPendingApprovalToolJobs"`
			Refs     []struct {
				ToolID string `json:"toolUseEventId"`
			} `json:"activeToolReferences"`
		}
		if json.Unmarshal(reply["thread"], &thread) != nil {
			t.Fatal("invalid approval ownership observation")
		}
		if thread.Observed && thread.Pending && len(thread.Refs) == 1 && thread.Refs[0].ToolID == toolID {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("original pending approval owner was not observed")
}

func waitContentFailureProviderJoined(t *testing.T, c *contentE2E) map[string]json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		observed := c.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
		var sources *int
		var native struct{ Active, Started, Joined *int }
		if json.Unmarshal(observed["activeProviderSources"], &sources) != nil || json.Unmarshal(observed["nativeIterators"], &native) != nil || sources == nil || native.Active == nil || native.Started == nil || native.Joined == nil {
			t.Fatal("provider source/iterator join observation unavailable")
		}
		if *sources == 0 && *native.Active == 0 && *native.Started > 0 && *native.Started == *native.Joined {
			return observed
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("provider wait/source did not close before Tool release")
	return nil
}

func assertContentFailurePrefix(t *testing.T, c *contentE2E, request string, spec contentToolLifecycleCase) {
	t.Helper()
	var raw string
	if err := c.db.QueryRow(`SELECT (`+sessionfixture.MessageContentSQL+`)::jsonb::text FROM session_messages m WHERE session_id=$1 AND model_request_id=$2 AND kind='assistant'`, c.session, request).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var message struct {
		Parts []map[string]any `json:"parts"`
	}
	var input map[string]any
	if json.Unmarshal([]byte(raw), &message) != nil || json.Unmarshal([]byte(spec.input), &input) != nil {
		t.Fatal("malformed committed prefix")
	}
	want := []map[string]any{
		{"type": "reasoning", "text": "reason-before-text", "providerMetadata": map[string]any{"anthropic": map[string]any{"signature": "fixture-signature-text"}}},
		{"type": "text", "text": "alpha"}, {"type": "text", "text": "beta"},
		{"type": "reasoning", "text": "reason-before-tool", "providerMetadata": map[string]any{"anthropic": map[string]any{"signature": "fixture-signature-tool"}}},
		{"type": "tool_call", "modelToolCallId": spec.callID, "toolName": spec.tool, "canonicalInput": input},
	}
	if !reflect.DeepEqual(message.Parts, want) {
		t.Fatalf("committed content/signature/Tool literal differs: %s", raw)
	}
}

func assertContentFailureEnd(t *testing.T, c *contentE2E, request, failure string) {
	t.Helper()
	var ends, failed, uses, retries int
	if err := c.db.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='agent.tool_use'),(SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error' AND payload_json::jsonb #>> '{error,retry_status,type}'='retrying')`, c.session, request).Scan(&ends, &failed, &uses, &retries); err != nil {
		t.Fatal(err)
	}
	wantFailed := 1
	if failure == "" {
		wantFailed = 0
	}
	if ends != 1 || failed != wantFailed || uses != 1 || retries != 0 {
		t.Fatalf("original End/failed/Tool/pre-settlement retry=%d/%d/%d/%d", ends, failed, uses, retries)
	}
	var disposition, errorKind string
	if err := c.db.QueryRow(`SELECT payload_json::jsonb #>> '{provider_context_retention,disposition}',COALESCE(payload_json::jsonb->>'error_kind','') FROM session_events WHERE session_id=$1 AND model_request_id=$2 AND type='span.model_request_end'`, c.session, request).Scan(&disposition, &errorKind); err != nil {
		t.Fatal(err)
	}
	wantDisposition, wantKind := "completed", ""
	switch failure {
	case "resource":
		wantDisposition, wantKind = "failed", "provider_error"
	case "transport":
		wantDisposition, wantKind = "rescheduled", "gateway_stream_error"
	}
	if disposition != wantDisposition || errorKind != wantKind {
		t.Fatalf("durable original End disposition/error=%s/%s want%s/%s", disposition, errorKind, wantDisposition, wantKind)
	}
}

func waitContentFailureStatus(t *testing.T, c *contentE2E, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var session struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(c.sdk.control(t, "session", map[string]any{"sessionId": c.session}), &session) != nil || session.Status == "" {
			t.Fatal("invalid SDK session observation")
		}
		if session.Status == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Tool continuation did not reach actual SDK %s", want)
}

func assertContentResourceCloseout(t *testing.T, c *contentE2E, approval bool) {
	t.Helper()
	var payload string
	if err := c.db.QueryRow(`SELECT payload_json FROM session_events WHERE session_id=$1 AND type='session.error'`, c.session).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if json.Unmarshal([]byte(payload), &value) != nil {
		t.Fatal("malformed public terminal resource failure")
	}
	want := map[string]any{"type": "session.error", "error": map[string]any{
		"type": "model_request_failed_error", "message": "Provider output exceeded Gateway resource limits.",
		"retry_status": map[string]any{"type": "terminal"},
	}}
	if approval {
		// Existing yielded recovery carries the failed End and original Tool
		// identity, but not the original fatal provider descriptor. After the
		// decision it emits the generic exhausted recovery failure and idle.
		want["error"] = map[string]any{"type": "unknown_error", "message": "Runtime operation failed.", "retry_status": map[string]any{"type": "exhausted"}}
	}
	if !reflect.DeepEqual(value, want) {
		t.Fatalf("terminal resource failure differs: %s", payload)
	}
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.error'`, c.session, 1)
	if approval {
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle' AND payload_json::jsonb #>> '{stop_reason,type}'='retries_exhausted'`, c.session, 1)
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_terminated'`, c.session, 0)
	} else {
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_terminated'`, c.session, 1)
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, c.session, 0)
	}
}

func waitContentFailureCleanup(t *testing.T, c *contentE2E) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reply := c.runtimeControl(t, "cleanup", c.session)
		var cleanup struct {
			OK bool `json:"ok"`
		}
		var thread struct {
			Observed *bool `json:"observed"`
		}
		if json.Unmarshal(reply["cleanup"], &cleanup) != nil || json.Unmarshal(reply["thread"], &thread) != nil || thread.Observed == nil {
			t.Fatal("cleanup/inspection observation unavailable")
		}
		if cleanup.OK && !*thread.Observed {
			var owners map[string]json.RawMessage
			if json.Unmarshal(reply["processOwners"], &owners) != nil {
				t.Fatal("owner census unavailable")
			}
			for _, key := range []string{"activeSessions", "activeThreads", "activeFibers", "activeToolFibers", "pendingApprovals", "pendingContentEntries", "pendingContentBytes"} {
				var count *int64
				if json.Unmarshal(owners[key], &count) != nil || count == nil || *count != 0 {
					t.Fatalf("owner %s not joined: %s", key, reply["processOwners"])
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("actual cleanup did not join Runtime owners")
}

func contentFailureRuntimeScope(t *testing.T, c *contentE2E) map[string]any {
	t.Helper()
	var thread, binding, pod, process string
	var generation int64
	if err := c.db.QueryRow(`SELECT t.id,b.binding_id,b.binding_generation,b.agent_runtime_pod_uid,b.runtime_process_id FROM session_runtime_bindings b JOIN session_threads t ON t.workspace_id=b.workspace_id AND t.session_id=b.session_id AND t.role='main' WHERE b.session_id=$1`, c.session).Scan(&thread, &binding, &generation, &pod, &process); err != nil {
		t.Fatal(err)
	}
	return map[string]any{"workspaceId": "default", "sessionId": c.session, "sessionThreadId": thread, "bindingId": binding, "bindingGeneration": generation, "targetPodUid": pod, "runtimeProcessId": process}
}

func waitContentFailureTerminalOwners(t *testing.T, c *contentE2E, scope map[string]any) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		reply := contentFailureInspectOriginalScope(t, c, scope)
		var thread struct {
			Observed *bool `json:"observed"`
		}
		if json.Unmarshal(reply["thread"], &thread) != nil || thread.Observed == nil {
			t.Fatal("terminal Runtime inspection unavailable")
		}
		if !*thread.Observed {
			var owners map[string]json.RawMessage
			if json.Unmarshal(reply["processOwners"], &owners) != nil {
				t.Fatal("terminal Runtime owner census unavailable")
			}
			joined := true
			for _, key := range []string{"activeSessions", "activeThreads", "activeFibers", "activeToolFibers", "pendingApprovals", "pendingContentEntries", "pendingContentBytes"} {
				var count *int64
				if json.Unmarshal(owners[key], &count) != nil || count == nil {
					t.Fatalf("terminal Runtime owner %s unavailable", key)
				}
				joined = joined && *count == 0
			}
			if joined {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("terminal Runtime owners did not join")
}

func contentFailureInspectOriginalScope(t *testing.T, c *contentE2E, scope map[string]any) map[string]json.RawMessage {
	t.Helper()
	c.controlMu.Lock()
	defer c.controlMu.Unlock()
	c.controlOrdinal++
	raw, err := json.Marshal(map[string]any{"id": c.controlOrdinal, "operation": "inspect", "scope": scope})
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("control-%d", c.controlOrdinal)
	temporary := filepath.Join(c.runtime.directory, name+".tmp")
	if err := os.WriteFile(temporary, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, filepath.Join(c.runtime.directory, name+".json")); err != nil {
		t.Fatal(err)
	}
	reply := c.runtime.marker(t, name+"-reply")
	var ok bool
	var id int
	if json.Unmarshal(reply["ok"], &ok) != nil || !ok || json.Unmarshal(reply["id"], &id) != nil || id != c.controlOrdinal {
		t.Fatal("original-scope Runtime inspection failed")
	}
	return reply
}
