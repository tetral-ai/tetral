package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Reuse the actual child processes and their service owners. The external
// adapter owns only fixed command effects/status; cleanup uses Runtime's real
// host API and every cold receipt comes from PostgreSQL.
func TestContentResourceLifecycle(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	provider := &contentCycleProvider{contentE2EProvider: contentE2EProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}}, entries: map[string]*contentCycleCommand{}, root: t.TempDir()}
	chain := startContentE2EWithOptions(t, "text", false, false, contentE2EOptions{Provider: provider, StopProvider: provider.finishAll, ExecutionWorkers: 3, Budget: 300 * time.Second, ApprovalMode: "ask_for_approval", Runtime: map[string]any{"observeStages": true, "controlCommands": true, "approvalMode": "ask_for_approval"}, Gateway: map[string]any{"sessionScenarioPlans": true, "recordContext": false, "measureResources": true, "memorySampleIntervalMs": 1000}})
	runContentResourceCycle(t, chain, provider, chain.session, "normal", nil, false)
	assertContentResourceEmpty(t, chain, chain.session)
	recordContentResourceBatch(t, chain, "warmup")
	cases := []string{"normal", "held", "allow", "cancel", "cold", "normal", "held", "deny", "cancel", "cold"}
	for batch := 0; batch < 3; batch++ {
		t.Run(fmt.Sprintf("sequential-%d", batch+1), func(t *testing.T) {
			for index, kind := range cases {
				t.Run(fmt.Sprintf("%d-%s", index, kind), func(t *testing.T) {
					session := chain.newSession(t)
					runContentResourceCycle(t, chain, provider, session, kind, nil, false)
				})
				if t.Failed() {
					return
				}
			}
		})
		if t.Failed() {
			return
		}
		recordContentResourceBatch(t, chain, fmt.Sprintf("sequential-%d", batch+1))
	}
	completedPeers := make(chan struct{}, 2)
	sessions := []string{chain.newSession(t), chain.newSession(t), chain.newSession(t)}
	t.Run("concurrent-three-sessions", func(t *testing.T) {
		for index, session := range sessions {
			t.Run(fmt.Sprintf("session-%d", index), func(t *testing.T) {
				t.Parallel()
				sequence := append([]string(nil), cases...)
				if index == 0 {
					sequence[0], sequence[1] = sequence[1], sequence[0]
				}
				for cycle, kind := range sequence {
					t.Run(fmt.Sprintf("%d-%s", cycle, kind), func(t *testing.T) {
						var beforeRelease func()
						if index == 0 && cycle == 0 {
							beforeRelease = func() {
								for peer := 0; peer < 2; peer++ {
									select {
									case <-completedPeers:
									case <-time.After(30 * time.Second):
										t.Fatal("other Sessions did not finish while selected tool remained held")
									}
								}
								assertContentHeldOwner(t, chain, session)
							}
						}
						runContentResourceCycle(t, chain, provider, session, kind, beforeRelease, true)
						if index != 0 && cycle == 0 {
							completedPeers <- struct{}{}
						}
					})
					if t.Failed() {
						return
					}
				}
			})
		}
	})
	if t.Failed() {
		return
	}
	assertContentResourceEmpty(t, chain, sessions[0])
	recordContentResourceBatch(t, chain, "concurrent-three-sessions")
	if provider.active.Load() != 0 {
		t.Fatal("external commands did not join")
	}
}

func runContentResourceCycle(t *testing.T, c *contentE2E, p *contentCycleProvider, session, kind string, beforeRelease func(), concurrent bool) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for name, query := range map[string]string{
			"custody":   `SELECT COALESCE(jsonb_agg(to_jsonb(r)), '[]'::jsonb)::text FROM session_runtime_tool_results r WHERE session_id=$1`,
			"events":    `SELECT COALESCE(jsonb_agg(jsonb_build_object('type',type,'id',event_id,'payload',payload_json) ORDER BY sequence),'[]'::jsonb)::text FROM session_events WHERE session_id=$1`,
			"lifecycle": `SELECT COALESCE(jsonb_agg(jsonb_build_object('kind',kind,'state',state,'error',error_kind)), '[]'::jsonb)::text FROM sandbox_lifecycle_operations WHERE session_id=$1`,
		} {
			var facts string
			if err := c.db.QueryRow(query, session).Scan(&facts); err != nil {
				t.Logf("cycle %s observation error: %v", name, err)
			} else {
				t.Logf("cycle %s: %s", name, facts)
			}
		}
		if entry := p.get(session); entry != nil {
			t.Logf("external starts=%d active=%d", entry.starts.Load(), entry.active.Load())
		}
	})
	var beforeSequence int64
	if err := c.db.QueryRow(`SELECT COALESCE(max(sequence),0) FROM session_events WHERE session_id=$1`, session).Scan(&beforeSequence); err != nil {
		t.Fatal(err)
	}
	var beforeIdle int
	if err := c.db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session).Scan(&beforeIdle); err != nil {
		t.Fatal(err)
	}
	scenario := "durable-interleaved"
	spec := contentReadCase
	if strings.HasPrefix(kind, "allow") || strings.HasPrefix(kind, "deny") {
		scenario = "durable-write"
		spec = contentToolLifecycleCase{tool: "Write", input: `{"content":"first\n","file_path":"/workspace/note.txt"}`, expectedFile: "first\n", captureOutput: true}
	}
	held := kind == "held" || kind == "cancel"
	entry := p.prepare(t, session, spec, held)
	scripts := []string{scenario, "done"}
	if kind == "cancel" {
		scripts = scripts[:1]
	}
	c.gateway.control(t, map[string]any{"kind": "configure_scenarios", "sessionId": session, "scenarios": scripts}, "configured_scenarios")
	if kind == "cold" {
		c.lost.faultSettlement(session, true)
	}
	c.sdk.control(t, "send", map[string]any{"sessionId": session, "text": "start-content-fixture"})
	if strings.HasPrefix(kind, "allow") || strings.HasPrefix(kind, "deny") {
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session, beforeIdle+1)
		var tool string
		if err := c.db.QueryRow(`SELECT event_id FROM session_events WHERE session_id=$1 AND type='agent.tool_use' ORDER BY sequence DESC LIMIT 1`, session).Scan(&tool); err != nil {
			t.Fatal(err)
		}
		if entry.starts.Load() != 0 {
			t.Fatal("permission wait dispatched an external command")
		}
		decision := "allow"
		if strings.HasPrefix(kind, "deny") {
			decision = "deny"
		}
		if strings.HasSuffix(kind, "-cold") {
			// Durable idle publication precedes the local run's finalizer. Join
			// that owner before asking cleanup to remove pending approval state.
			waitReply := c.runtimeControl(t, "wait", session)
			var joined struct {
				OK       bool  `json:"ok"`
				Observed *bool `json:"observed"`
				TimedOut *bool `json:"timedOut"`
			}
			waitValid := json.Unmarshal(waitReply["wait"], &joined) == nil
			if !waitValid || !joined.OK || joined.Observed == nil || !*joined.Observed || joined.TimedOut == nil || *joined.TimedOut {
				t.Fatalf("pending approval run did not join: valid=%t ok=%t observed_present=%t observed=%t timeout_present=%t timed_out=%t", waitValid, joined.OK, joined.Observed != nil, joined.Observed != nil && *joined.Observed, joined.TimedOut != nil, joined.TimedOut != nil && *joined.TimedOut)
			}
			reply := c.runtimeControl(t, "cleanup", session)
			var cleanup struct {
				OK      bool `json:"ok"`
				Cleaned bool `json:"cleaned"`
			}
			var thread struct {
				Observed *bool `json:"observed"`
			}
			cleanupValid := json.Unmarshal(reply["cleanup"], &cleanup) == nil
			threadValid := json.Unmarshal(reply["thread"], &thread) == nil
			if !cleanupValid || !threadValid || !cleanup.OK || !cleanup.Cleaned || thread.Observed == nil || *thread.Observed {
				t.Fatalf("pending approval residency was not evicted: cleanup_valid=%t thread_valid=%t ok=%t cleaned=%t observed_present=%t observed=%t", cleanupValid, threadValid, cleanup.OK, cleanup.Cleaned, thread.Observed != nil, thread.Observed != nil && *thread.Observed)
			}
			if entry.starts.Load() != 0 {
				t.Fatal("cold permission preparation dispatched an external command")
			}
			// The owning API commits the decision before Queue can deliver its
			// new input; the released hot owner cannot service this continuation.
		}
		c.sdk.control(t, "confirm", map[string]any{"sessionId": session, "toolUseEventId": tool, "result": decision})
		beforeIdle++
	} else if held {
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running' AND provider_command_reference_json IS NOT NULL`, session, 1)
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND sequence>(SELECT max(sequence) FROM session_events WHERE session_id=$1 AND type='user.message')`, session, 1)
		assertContentHeldOwner(t, c, session)
		if beforeRelease != nil {
			beforeRelease()
		}
		if kind == "cancel" {
			c.sdk.control(t, "interrupt", map[string]any{"sessionId": session})
			waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session, beforeIdle+1)
		}
		entry.finish()
	} else if kind == "cold" {
		waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='terminal_unconsumed'`, session, 1)
		deadline := time.Now().Add(20 * time.Second)
		for {
			reply := c.runtimeControl(t, "inspect", session)
			var thread struct {
				Observed *bool `json:"observed"`
			}
			if json.Unmarshal(reply["thread"], &thread) != nil || thread.Observed == nil {
				t.Fatal("invalid cold owner observation")
			}
			if !*thread.Observed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("failed settlement did not discard hot residency")
			}
			time.Sleep(20 * time.Millisecond)
		}
		c.lost.mu.Lock()
		attempts := c.lost.settlementFaults[session]
		c.lost.mu.Unlock()
		if attempts == 0 {
			t.Fatal("cold case bypassed actual settlement fault")
		}
		c.lost.faultSettlement(session, false)
		c.sdk.control(t, "send", map[string]any{"sessionId": session, "text": "resume-content-fixture"})
	}
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, session, beforeIdle+1)
	deadline := time.Now().Add(10 * time.Second)
	for entry.active.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("external command did not join")
		}
		time.Sleep(10 * time.Millisecond)
	}
	wantStarts := int64(1)
	if strings.HasPrefix(kind, "deny") {
		wantStarts = 0
	}
	if entry.starts.Load() != wantStarts {
		t.Fatalf("external command count=%d want%d", entry.starts.Load(), wantStarts)
	}
	if kind != "cancel" && !strings.HasPrefix(kind, "deny") {
		data, err := os.ReadFile(entry.tool.path)
		if err != nil || string(data) != spec.expectedFile {
			t.Fatalf("independent command effect=%q want%q error=%v", data, spec.expectedFile, err)
		}
	}
	assertContentResourceCycleHistory(t, c, session, kind, beforeSequence)
	deadline = time.Now().Add(10 * time.Second)
	for {
		reply := c.runtimeControl(t, "cleanup", session)
		var cleanup struct {
			OK      bool `json:"ok"`
			Cleaned bool `json:"cleaned"`
		}
		var thread struct {
			Observed *bool `json:"observed"`
		}
		if json.Unmarshal(reply["cleanup"], &cleanup) != nil || json.Unmarshal(reply["thread"], &thread) != nil || thread.Observed == nil {
			t.Fatal("invalid Runtime cleanup response")
		}
		if cleanup.OK && thread.Observed != nil && !*thread.Observed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("real Runtime cleanup did not release completed Session: %s", reply["cleanup"])
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.forget(session, entry)
	if !concurrent {
		assertContentResourceEmpty(t, c, session)
	}
}

func assertContentResourceCycleHistory(t *testing.T, c *contentE2E, session, kind string, after int64) {
	t.Helper()
	rows, err := c.db.Query(`SELECT type,event_id,payload_json,COALESCE(model_request_id,'') FROM session_events WHERE session_id=$1 AND sequence>$2 ORDER BY sequence`, session, after)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var texts []string
	var useID string
	ends, uses, results, failures := 0, 0, 0, 0
	for rows.Next() {
		var typ, id, payload, request string
		if err := rows.Scan(&typ, &id, &payload, &request); err != nil {
			t.Fatal(err)
		}
		var value struct {
			IsError         bool   `json:"is_error"`
			ToolUseID       string `json:"tool_use_id"`
			ModelToolCallID string `json:"model_tool_call_id"`
			Content         []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal([]byte(payload), &value); err != nil {
			t.Fatal(err)
		}
		switch typ {
		case "agent.message":
			if len(value.Content) != 1 {
				t.Fatalf("unexpected complete text: %s", payload)
			}
			texts = append(texts, value.Content[0].Text)
		case "agent.tool_use":
			uses++
			useID = id
		case "agent.tool_result":
			results++
			if value.ToolUseID != useID {
				t.Fatal("result detached from this cycle's Tool Use")
			}
		case "span.model_request_end":
			ends++
			if value.IsError {
				t.Fatalf("resource cycle has failed End: %s", payload)
			}
		case "session.error":
			failures++
			var failure struct {
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
					Retry   struct {
						Type string `json:"type"`
					} `json:"retry_status"`
				} `json:"error"`
			}
			if json.Unmarshal([]byte(payload), &failure) != nil || kind != "cold" || failure.Error.Type != "unknown_error" || failure.Error.Message != "Session event writer operation failed." || failure.Error.Retry.Type != "exhausted" {
				t.Fatalf("unexpected resource cycle failure: %s", payload)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantTexts, wantEnds := []string{"alpha", "beta", "done"}, 2
	if kind == "cancel" {
		wantTexts = []string{"alpha", "beta"}
		wantEnds = 1
	}
	if !reflect.DeepEqual(texts, wantTexts) || uses != 1 || ends != wantEnds {
		t.Fatalf("cycle content=%v uses=%d ends=%d; want %v/1/%d", texts, uses, ends, wantTexts, wantEnds)
	}
	wantFailures := 0
	if kind == "cold" {
		wantFailures = 1
	}
	if failures != wantFailures {
		t.Fatalf("cycle failures=%d want%d", failures, wantFailures)
	}
	if kind != "cancel" && results != 1 {
		t.Fatalf("cycle result count=%d want1", results)
	}
	if kind != "cancel" && !strings.HasPrefix(kind, "deny") {
		var joined int
		err := c.db.QueryRow(`SELECT count(*) FROM session_runtime_tool_results r JOIN session_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.consumed_by_terminal_event_id WHERE r.session_id=$1 AND r.tool_use_event_id=$2 AND r.execution_state='consumed' AND r.provider_command_reference_json IS NOT NULL AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=r.tool_use_event_id`, session, useID).Scan(&joined)
		if err != nil || joined != 1 {
			t.Fatalf("cycle accepted execution/result join=%d error=%v", joined, err)
		}
	}
}

func assertContentHeldOwner(t *testing.T, c *contentE2E, session string) {
	t.Helper()
	reply := c.runtimeControl(t, "inspect", session)
	var thread struct {
		Observed        bool `json:"observed"`
		ActiveToolCount *int `json:"activeToolCount"`
		References      []struct {
			ToolUseEventID string `json:"toolUseEventId"`
		} `json:"activeToolReferences"`
	}
	if json.Unmarshal(reply["thread"], &thread) != nil || !thread.Observed || thread.ActiveToolCount == nil || *thread.ActiveToolCount != 1 || len(thread.References) != 1 {
		t.Fatalf("held tool lost its Runtime owner: %s", reply["thread"])
	}
	var tool string
	if err := c.db.QueryRow(`SELECT tool_use_event_id FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running'`, session).Scan(&tool); err != nil || thread.References[0].ToolUseEventID != tool {
		t.Fatalf("held reference differs from accepted command: %s/%v", reply["thread"], err)
	}
}
func assertContentResourceEmpty(t *testing.T, c *contentE2E, session string) {
	t.Helper()
	reply := c.runtimeControl(t, "inspect", session)
	var owners map[string]json.RawMessage
	if json.Unmarshal(reply["processOwners"], &owners) != nil {
		t.Fatal("missing actual Runtime owner census")
	}
	for _, name := range []string{"activeSessions", "activeThreads", "activeFibers", "activeToolFibers", "pendingApprovals", "pendingContentEntries", "pendingContentBytes"} {
		var count *int64
		if json.Unmarshal(owners[name], &count) != nil || count == nil || *count != 0 {
			t.Fatalf("Runtime owner %s not released: %s", name, reply["processOwners"])
		}
	}
	if len(owners["approvalWaitOutstanding"]) == 0 || string(owners["approvalWaitOutstanding"]) == "null" {
		t.Fatal("approval wait ownership unavailable")
	}
	if string(owners["approvalWaitOutstanding"]) != "[]" {
		var labelled [][]json.RawMessage
		if json.Unmarshal(owners["approvalWaitOutstanding"], &labelled) != nil {
			t.Fatal("missing approval-wait census")
		}
		for _, value := range labelled {
			var count int
			if len(value) != 2 || json.Unmarshal(value[1], &count) != nil || count != 0 {
				t.Fatal("approval-wait owner remains")
			}
		}
	}
	observed := c.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	for _, name := range []string{"activeProviderSources", "pendingScenarioPlans"} {
		var count int
		if json.Unmarshal(observed[name], &count) != nil || count != 0 {
			t.Fatalf("Gateway owner %s remains", name)
		}
	}
	var assembly map[string]int64
	if json.Unmarshal(observed["assemblyTotals"], &assembly) != nil || len(assembly) != 5 {
		t.Fatal("missing all-request assembler census")
	}
	for name, count := range assembly {
		if count != 0 {
			t.Fatalf("assembler %s remains %d", name, count)
		}
	}
	var native struct {
		Active  int `json:"active"`
		Started int `json:"started"`
		Joined  int `json:"joined"`
	}
	var sdk struct {
		Active int `json:"active"`
	}
	var writer struct {
		PendingBytes     int64 `json:"pendingBytes"`
		PendingCallbacks int64 `json:"pendingCallbacks"`
		WriteCalls       int64 `json:"writeCalls"`
		Callbacks        int64 `json:"callbacks"`
	}
	if json.Unmarshal(observed["nativeIterators"], &native) != nil || json.Unmarshal(observed["sdkTotals"], &sdk) != nil || json.Unmarshal(observed["writer"], &writer) != nil || native.Active != 0 || native.Started < 1 || native.Started != native.Joined || sdk.Active != 0 || writer.WriteCalls < 1 || writer.PendingBytes != 0 || writer.PendingCallbacks != 0 || writer.WriteCalls != writer.Callbacks {
		t.Fatalf("Gateway native/SDK/writer owners did not join: native=%s sdk=%s writer=%s", observed["nativeIterators"], observed["sdkTotals"], observed["writer"])
	}
}
func recordContentResourceBatch(t *testing.T, c *contentE2E, label string) {
	t.Helper()
	observed := c.gateway.control(t, map[string]any{"kind": "observe"}, "observation")
	t.Logf("resource batch=%s Gateway memory=%s sampledElapsedMs=%s cpu=%s eventLoop=%s", label, observed["memory"], observed["memorySampleElapsedMs"], observed["cpuMicros"], observed["eventLoop"])
	ready := c.runtime.marker(t, "ready")
	var address string
	if json.Unmarshal(ready["httpUrl"], &address) != nil {
		t.Fatal("Runtime metrics URL absent")
	}
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Get(strings.TrimRight(address, "/") + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	_ = response.Body.Close()
	if err != nil || response.StatusCode != 200 {
		t.Fatal("Runtime memory observation failed")
	}
	memory := map[string]uint64{}
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && (fields[0] == "process_heap_used_bytes" || fields[0] == "process_rss_bytes") {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			memory[fields[0]] = value
		}
	}
	if len(memory) != 2 {
		t.Fatal("actual process heap/RSS metrics absent")
	}
	t.Logf("resource batch=%s Runtime memory=%v", label, memory)

}

type contentCycleCommand struct {
	mu             sync.Mutex
	tool           *contentToolProvider
	invocation     tetralsandbox.ToolExecutionRequest
	release        chan struct{}
	once           sync.Once
	starts, active atomic.Int64
}

func (e *contentCycleCommand) finish() { e.once.Do(func() { close(e.release) }) }

type contentCycleProvider struct {
	contentE2EProvider
	mu      sync.Mutex
	entries map[string]*contentCycleCommand
	root    string
	serial  int
	active  atomic.Int64
}

func (p *contentCycleProvider) prepare(t *testing.T, session string, spec contentToolLifecycleCase, held bool) *contentCycleCommand {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries[session] != nil {
		t.Fatal("previous external command still owned")
	}
	p.serial++
	path := filepath.Join(p.root, fmt.Sprintf("note-%d.txt", p.serial))
	if err := os.WriteFile(path, []byte("fixture-note"), 0600); err != nil {
		t.Fatal(err)
	}
	entry := &contentCycleCommand{tool: &contentToolProvider{spec: spec, path: path}, release: make(chan struct{})}
	if !held {
		entry.finish()
	}
	p.entries[session] = entry
	return entry
}
func (p *contentCycleProvider) forget(session string, entry *contentCycleCommand) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries[session] == entry {
		delete(p.entries, session)
	}
}
func (p *contentCycleProvider) finishAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.entries {
		entry.finish()
	}
}
func (p *contentCycleProvider) get(session string) *contentCycleCommand {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.entries[session]
}
func (p *contentCycleProvider) Activate(_ context.Context, r tetralsandbox.ActivationRequest) tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle] {
	return tetralsandbox.ProviderOutcome[sandboxmodel.ProviderHandle]{Value: sandboxmodel.ProviderHandle{Provider: "daytona", SandboxID: "external-" + string(r.Setup.SessionID)}}
}
func (p *contentCycleProvider) InspectForExecution(_ context.Context, handle string) tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness] {
	value := tetralsandbox.ExecutionReady
	if handle == "" {
		value = tetralsandbox.ExecutionNeedsCreation
	}
	return tetralsandbox.ProviderOutcome[tetralsandbox.ExecutionReadiness]{Value: value}
}
func (p *contentCycleProvider) ExecuteTool(_ context.Context, r tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	entry := p.get(r.Invocation.Target.SessionID)
	if entry == nil || entry.starts.Add(1) != 1 || r.Invocation.ToolName != entry.tool.spec.tool || r.Invocation.InputJSON != entry.tool.spec.input {
		return contentExternalToolFailure()
	}
	entry.mu.Lock()
	entry.invocation = r
	entry.mu.Unlock()
	entry.active.Add(1)
	p.active.Add(1)
	target := r.Invocation.Target
	target.ProviderSandboxID = r.Handle.SandboxID
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ForegroundObservation: &sandboxdriver.ForegroundCommandObservation{Reference: sandboxdriver.CommandReference{Target: target, Task: sandboxdriver.BackgroundTask{TaskID: "resource-" + r.Invocation.ToolUseEventID, ProviderSessionID: r.Handle.SandboxID, ProviderCommandID: r.Invocation.ToolUseEventID}, ToolUseEventID: r.Invocation.ToolUseEventID}}}}
}
func (p *contentCycleProvider) ObserveTool(ctx context.Context, r sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	entry := p.get(r.Reference.Target.SessionID)
	if entry == nil {
		return contentExternalToolFailure()
	}
	defer entry.active.Add(-1)
	defer p.active.Add(-1)
	select {
	case <-entry.release:
	case <-ctx.Done():
		return contentExternalToolFailure()
	}
	entry.mu.Lock()
	invocation := entry.invocation
	entry.mu.Unlock()
	return entry.tool.ExecuteTool(ctx, invocation)
}
func (p *contentCycleProvider) CaptureOutputs(ctx context.Context, r sandboxdriver.OutputCaptureTarget) tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	entry := p.get(r.SessionID)
	if entry == nil {
		return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: sandboxdriver.OutputCaptureScan{}}
	}
	return entry.tool.CaptureOutputs(ctx, r)
}
