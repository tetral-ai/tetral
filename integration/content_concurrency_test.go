package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Three independent Read calls exercise the real per-Session permit owner.
// External command completion is controlled; all admission and settlement are real.
func TestContentToolConcurrencyAndResultPairing(t *testing.T) {
	requireContentLifecycleDependencies(t, true)
	p := &contentConcurrentProvider{contentE2EProvider: contentE2EProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}}, root: t.TempDir(), commands: map[string]*contentConcurrentCommand{}, started: make(chan string, 3)}
	for _, name := range []string{"one", "two", "three"} {
		p.commands[name] = &contentConcurrentCommand{release: make(chan struct{})}
		if err := os.WriteFile(filepath.Join(p.root, name), []byte("content-"+name), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Preserve real cold admission with the normal Sandbox preparation budget
	// (ProviderCommandTimeout defaults to 45s). Held command completion isolates
	// per-Session concurrency and result pairing from preparation deadline tests.
	c := startContentE2EWithOptions(t, "durable-read-three", false, false, contentE2EOptions{Provider: p, StopProvider: p.finishAll, PreparationTimeout: 45 * time.Second, ExecutionWorkers: 3, Runtime: map[string]any{"maxConcurrentTools": 2, "controlCommands": true}, Gateway: map[string]any{"measureResources": true, "followupScenario": "done"}})
	c.sdk.control(t, "send", map[string]any{"sessionId": c.session, "text": "start-content-fixture"})
	first := map[string]bool{p.waitStart(t, c, "first-admission"): true, p.waitStart(t, c, "second-admission"): true}
	if !reflect.DeepEqual(first, map[string]bool{"one": true, "two": true}) {
		t.Fatalf("first admitted commands %v", first)
	}
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_use'`, c.session, 3)
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'`, c.session, 1)
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND execution_state='running' AND provider_command_reference_json IS NOT NULL`, c.session, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		reply := c.runtimeControl(t, "inspect", c.session)
		var state struct {
			Observed   bool `json:"observed"`
			References []struct {
				ToolUseEventID string `json:"toolUseEventId"`
			} `json:"activeToolReferences"`
			Permits *struct{ Running, Waiting int } `json:"sessionToolPermits"`
		}
		if json.Unmarshal(reply["thread"], &state) != nil {
			t.Fatal("invalid owner observation")
		}
		if state.Observed && len(state.References) == 3 && state.Permits != nil && state.Permits.Running == 2 && state.Permits.Waiting == 1 {
			for _, reference := range state.References {
				var count int
				if err := c.db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$2 AND type='agent.tool_use'`, c.session, reference.ToolUseEventID).Scan(&count); err != nil || count != 1 {
					t.Fatal("pending reference detached from durable declaration")
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("three declared owners not preserved across End: %s", reply["thread"])
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case name := <-p.started:
		t.Fatalf("third command bypassed concurrency limit: %s", name)
	default:
	}
	p.finish("two")
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, c.session, 1)
	if name := p.waitStart(t, c, "queued-admission"); name != "three" {
		t.Fatalf("wrong queued command %s", name)
	}
	p.finish("three")
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, c.session, 2)
	p.finish("one")
	waitContentSQLCount(t, c.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.status_idle'`, c.session, 1)
	rows, err := c.db.Query(`SELECT u.payload_json::jsonb->'input'->>'file_path',r.payload_json::jsonb->'content'->0->>'text' FROM session_events r JOIN session_events u ON u.workspace_id=r.workspace_id AND u.event_id=r.payload_json::jsonb->>'tool_use_id' WHERE r.session_id=$1 AND r.type='agent.tool_result' ORDER BY r.sequence`, c.session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var path, output string
		if err := rows.Scan(&path, &output); err != nil {
			t.Fatal(err)
		}
		name := path[len("/workspace/") : len(path)-len(".txt")]
		names = append(names, name)
		if output != "status: success\ncontent:\ncontent-"+name {
			t.Fatalf("wrong result pair %q/%q", path, output)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"two", "three", "one"}) {
		t.Fatalf("settlement order %v", names)
	}
	var joined, failed, ends int
	if err := c.db.QueryRow(`SELECT count(*) FROM session_runtime_tool_results r JOIN session_events e ON e.workspace_id=r.workspace_id AND e.event_id=r.consumed_by_terminal_event_id WHERE r.session_id=$1 AND r.execution_state='consumed' AND r.provider_command_reference_json IS NOT NULL AND e.type='agent.tool_result' AND e.payload_json::jsonb->>'tool_use_id'=r.tool_use_event_id`, c.session).Scan(&joined); err != nil || joined != 3 {
		t.Fatalf("accepted results joined %d/%v", joined, err)
	}
	if err := c.db.QueryRow(`SELECT count(*) FILTER(WHERE type='session.error' OR (type='span.model_request_end' AND (payload_json::jsonb->>'is_error')::boolean)),count(*) FILTER(WHERE type='span.model_request_end') FROM session_events WHERE session_id=$1`, c.session).Scan(&failed, &ends); err != nil || failed != 0 || ends != 2 {
		t.Fatalf("request outcomes %d/%d/%v", failed, ends, err)
	}
	assertContentSDKTexts(t, c, []string{"alpha", "beta", "done"})
	p.mu.Lock()
	active, peak := p.active, p.peak
	commands := len(p.identities)
	p.mu.Unlock()
	if active != 0 || peak != 2 || commands != 3 {
		t.Fatalf("external occupancy active=%d peak=%d commands=%d", active, peak, commands)
	}
	c.runtimeControl(t, "cleanup", c.session)
	assertContentResourceEmpty(t, c, c.session)
}

type contentConcurrentCommand struct {
	release  chan struct{}
	once     sync.Once
	tool     string
	terminal bool
}
type contentConcurrentProvider struct {
	contentE2EProvider
	mu                      sync.Mutex
	root                    string
	commands                map[string]*contentConcurrentCommand
	identities              map[string]string
	started                 chan string
	active, peak            int
	startNames              []string
	executeEntries          []contentConcurrentExecuteDiagnostic
	executeEntriesTruncated bool
}

type contentConcurrentExecuteDiagnostic struct {
	ToolUseEventID string `json:"toolUseEventId"`
	Name           string `json:"name"`
	InputMatched   bool   `json:"inputMatched"`
	NameMatched    bool   `json:"nameMatched"`
	IDPresent      bool   `json:"idPresent"`
	Duplicate      bool   `json:"duplicate"`
}

func (p *contentConcurrentProvider) finish(name string) {
	p.commands[name].once.Do(func() { close(p.commands[name].release) })
}
func (p *contentConcurrentProvider) finishAll() {
	for name := range p.commands {
		p.finish(name)
	}
}
func (p *contentConcurrentProvider) waitStart(t *testing.T, c *contentE2E, phase string) string {
	t.Helper()
	select {
	case name := <-p.started:
		p.mu.Lock()
		p.startNames = append(p.startNames, name)
		p.mu.Unlock()
		return name
	case <-time.After(20 * time.Second):
		p.logStartFailure(t, c, phase)
		t.Fatalf("external command did not start (%s)", phase)
		return ""
	}
}

// Failure-only snapshot occurs before t.Fatal starts cleanup/release. Inspection
// failure is explicit; absence of a row is never reported as successful custody.
func (p *contentConcurrentProvider) logStartFailure(t *testing.T, c *contentE2E, phase string) {
	t.Helper()
	p.mu.Lock()
	adapter, _ := json.Marshal(struct {
		Phase              string                               `json:"phase"`
		ObservedStartNames []string                             `json:"observedStartNames"`
		Entries            []contentConcurrentExecuteDiagnostic `json:"executeEntries"`
		Truncated          bool                                 `json:"truncated"`
	}{phase, p.startNames, p.executeEntries, p.executeEntriesTruncated})
	p.mu.Unlock()
	t.Logf("concurrency adapter diagnostic: %s", adapter)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rows, err := c.db.QueryContext(ctx, `
	WITH selected AS (
	 SELECT workspace_id,session_id,session_thread_id,event_id,sequence,payload_json::jsonb AS payload
	 FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.tool_use'
	 ORDER BY sequence LIMIT 4
	)
	SELECT jsonb_build_object(
	 'toolUseEventId',u.event_id,'declarationSequence',u.sequence,
	 'callId',left(r.model_tool_call_id,128),
	 'expectedInput',CASE u.payload->'input'->>'file_path'
	   WHEN '/workspace/one.txt' THEN 'one' WHEN '/workspace/two.txt' THEN 'two'
	   WHEN '/workspace/three.txt' THEN 'three' ELSE 'unmatched' END,
	 'resultEventId',e.event_id,'resultSequence',e.sequence,'resultCreatedAt',e.created_at,'isError',e.payload_json::jsonb->'is_error',
	 'publicError',CASE split_part(e.payload_json::jsonb->'content'->0->>'text',E'\n',1)
	   WHEN 'sandbox execution could not be started' THEN 'sandbox_execution_unavailable'
	   WHEN 'fixed external operation failed' THEN 'fixture_io_error'
	   WHEN 'sandbox execution outcome is unknown' THEN 'sandbox_execution_outcome_unknown'
	   WHEN 'sandbox execution was cancelled' THEN 'cancelled'
	   WHEN 'sandbox execution is no longer available' THEN 'session_deleted'
	   ELSE CASE WHEN e.event_id IS NULL THEN 'absent' ELSE 'unclassified' END END,
	 'state',r.execution_state,'generation',r.execution_attempt_generation,
	 'preparationDeadline',r.preparation_deadline,'executionUpdatedAt',r.updated_at,
	 'cancelRequestedAt',r.cancel_requested_at,'providerReferencePresent',r.provider_command_reference_json IS NOT NULL,
	 'unconsumedError',CASE r.result_json::jsonb->'error'->>'kind'
	   WHEN 'sandbox_execution_unavailable' THEN 'sandbox_execution_unavailable'
	   WHEN 'fixture_io_error' THEN 'fixture_io_error'
	   WHEN 'sandbox_execution_outcome_unknown' THEN 'sandbox_execution_outcome_unknown'
	   WHEN 'cancelled' THEN 'cancelled' WHEN 'session_deleted' THEN 'session_deleted'
	   ELSE CASE WHEN r.result_json IS NULL THEN 'absent' ELSE 'unclassified' END END,
	 'queueId',q.id,'queueStatus',q.status,'queueAttempts',q.attempt_count,'queueMaxAttempts',q.max_attempts,
	 'queueUpdatedAt',q.updated_at,'queueLeasedAt',q.leased_at,'queueAvailableAt',q.available_at,
	 'queueError',CASE q.last_error_kind
	   WHEN 'sandbox_execution_store_error' THEN 'sandbox_execution_store_error'
	   WHEN 'sandbox_execution_reinspection' THEN 'sandbox_execution_reinspection'
	   WHEN 'sandbox_execution_attempts_exhausted' THEN 'sandbox_execution_attempts_exhausted'
	   WHEN 'lease_expired' THEN 'lease_expired'
	   ELSE CASE WHEN q.last_error_kind IS NULL THEN 'absent' ELSE 'unclassified' END END,
	 'databaseNow',clock_timestamp())
	FROM selected u
	LEFT JOIN session_runtime_tool_results r ON r.workspace_id=u.workspace_id AND r.session_id=u.session_id
	 AND r.session_thread_id=u.session_thread_id AND r.tool_use_event_id=u.event_id
	LEFT JOIN session_events e ON e.workspace_id=u.workspace_id AND e.session_id=u.session_id
	 AND e.session_thread_id=u.session_thread_id AND e.type='agent.tool_result'
	 AND e.payload_json::jsonb->>'tool_use_id'=u.event_id
	LEFT JOIN queue_jobs q ON q.workspace_id=u.workspace_id AND q.kind='sandbox_tool_execute'
	 AND q.partition_key='sandbox-execution:'||u.workspace_id||':'||u.session_id||':'||u.session_thread_id||':'||u.event_id
	 AND q.dedupe_key='sandbox_tool_execute:'||u.workspace_id||':'||u.session_id||':'||u.session_thread_id||':'||u.event_id||':'||r.execution_attempt_generation::text
	ORDER BY u.sequence,e.sequence LIMIT 9`, c.session)
	if err != nil {
		t.Logf("concurrency SQL diagnostic unavailable (phase=%s timeout=%t)", phase, ctx.Err() != nil)
		return
	}
	defer func() { _ = rows.Close() }()
	count := 0
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Logf("concurrency SQL diagnostic scan unavailable (phase=%s)", phase)
			return
		}
		t.Logf("concurrency SQL diagnostic: %s", raw)
		count++
	}
	if err := rows.Err(); err != nil {
		t.Logf("concurrency SQL diagnostic iteration unavailable (phase=%s timeout=%t)", phase, ctx.Err() != nil)
		return
	}
	t.Logf("concurrency SQL diagnostic rows=%d capped=%t", count, count == 9)
}

func (p *contentConcurrentProvider) ExecuteTool(_ context.Context, r tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.mu.Lock()
	defer p.mu.Unlock()
	var name string
	for key := range p.commands {
		if r.Invocation.InputJSON == fmt.Sprintf(`{"file_path":"/workspace/%s.txt"}`, key) {
			name = key
		}
	}
	// Record only closed fixture categories; never retain arbitrary input/errors.
	entry := contentConcurrentExecuteDiagnostic{ToolUseEventID: r.Invocation.ToolUseEventID, Name: name,
		InputMatched: name != "", NameMatched: r.Invocation.ToolName == "Read", IDPresent: r.Invocation.ToolUseEventID != ""}
	if len(entry.ToolUseEventID) > 128 {
		entry.ToolUseEventID = "oversize"
	}
	if name == "" {
		entry.Name = "unmatched"
	} else {
		entry.Duplicate = p.commands[name].tool != ""
	}
	if len(p.executeEntries) < 12 {
		p.executeEntries = append(p.executeEntries, entry)
	} else {
		p.executeEntriesTruncated = true
	}
	if name == "" || r.Invocation.ToolName != "Read" || r.Invocation.ToolUseEventID == "" || p.commands[name].tool != "" {
		return contentExternalToolFailure()
	}
	if p.identities == nil {
		p.identities = map[string]string{}
	}
	p.identities[r.Invocation.ToolUseEventID] = name
	p.commands[name].tool = r.Invocation.ToolUseEventID
	p.active++
	if p.active > p.peak {
		p.peak = p.active
	}
	p.started <- name
	target := r.Invocation.Target
	target.ProviderSandboxID = r.Handle.SandboxID
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ForegroundObservation: &sandboxdriver.ForegroundCommandObservation{Reference: sandboxdriver.CommandReference{Target: target, Task: sandboxdriver.BackgroundTask{TaskID: "content-" + name, ProviderSessionID: r.Handle.SandboxID, ProviderCommandID: name}, ToolUseEventID: r.Invocation.ToolUseEventID}}}}
}
func (p *contentConcurrentProvider) ObserveTool(ctx context.Context, observation sandboxdriver.ForegroundCommandObservation) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	p.mu.Lock()
	name := p.identities[observation.Reference.ToolUseEventID]
	entry := p.commands[name]
	p.mu.Unlock()
	if entry == nil || observation.Reference.Task.ProviderCommandID != name {
		return contentExternalToolFailure()
	}
	select {
	case <-entry.release:
	case <-ctx.Done():
		return contentExternalToolFailure()
	}
	data, err := os.ReadFile(filepath.Join(p.root, name))
	if err != nil {
		return contentExternalToolFailure()
	}
	p.mu.Lock()
	if !entry.terminal {
		entry.terminal = true
		p.active--
	}
	p.mu.Unlock()
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "tool": "read", "status": "success", "truncated": false, "error": nil, "result": map[string]any{"content": string(data)}})
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: string(raw)}}
}
