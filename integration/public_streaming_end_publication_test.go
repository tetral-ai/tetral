package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

// Reuse the external lifecycle fixture for actual activation/materialization,
// with a fixed Write operation whose filesystem effect is checked separately.
type publicApprovalProvider struct {
	contentE2EProvider
	content string
}

func (p *publicApprovalProvider) ExecuteTool(ctx context.Context, request tetralsandbox.ToolExecutionRequest) tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution] {
	var arguments map[string]string
	if p.calls.Add(1) != 1 || request.Invocation.ToolName != "Write" || request.Invocation.ToolUseEventID == "" ||
		json.Unmarshal([]byte(request.Invocation.InputJSON), &arguments) != nil || len(arguments) != 2 ||
		arguments["file_path"] != "/workspace/public-preview.txt" || arguments["content"] != p.content {
		return contentExternalToolFailure()
	}
	if err := os.WriteFile(p.path, []byte(arguments["content"]), 0600); err != nil {
		return contentExternalToolFailure()
	}
	result, _ := json.Marshal(map[string]any{"schema_version": 1, "tool": "write", "status": "success", "truncated": false, "error": nil, "result": map[string]any{"created": true, "bytes_written": len(arguments["content"])}})
	return tetralsandbox.ProviderOutcome[sandboxdriver.ToolExecution]{Value: sandboxdriver.ToolExecution{ResultJSON: string(result)}}
}

func (p *publicApprovalProvider) CaptureOutputs(context.Context, sandboxdriver.OutputCaptureTarget) tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan] {
	return tetralsandbox.ProviderOutcome[sandboxdriver.OutputCaptureScan]{Value: sandboxdriver.OutputCaptureScan{}}
}

// This sink blocks one actual production Write call, retaining no body copy.
// Unwrap preserves the real connection's deadline and flush ownership.
type publicTextWriteBarrier struct {
	entered, release chan struct{}
	armed            atomic.Bool
	once             sync.Once
	eventName        string
}

func newPublicTextWriteBarrier(t *testing.T) *publicTextWriteBarrier {
	b := &publicTextWriteBarrier{entered: make(chan struct{}), release: make(chan struct{}), eventName: "agent.message"}
	t.Cleanup(b.unblock)
	return b
}
func (b *publicTextWriteBarrier) unblock() { b.once.Do(func() { close(b.release) }) }
func (b *publicTextWriteBarrier) wrap(next http.Handler) http.Handler {
	var selected atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stream") && selected.CompareAndSwap(false, true) {
			next.ServeHTTP(&publicHeldWriter{ResponseWriter: w, ctx: r.Context(), barrier: b}, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type publicHeldWriter struct {
	http.ResponseWriter
	ctx     context.Context
	barrier *publicTextWriteBarrier
}

func (w *publicHeldWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *publicHeldWriter) Write(data []byte) (int, error) {
	if bytes.HasPrefix(data, []byte("event: "+w.barrier.eventName+"\n")) && w.barrier.armed.CompareAndSwap(true, false) {
		close(w.barrier.entered)
		select {
		case <-w.barrier.release:
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		}
	}
	return w.ResponseWriter.Write(data)
}

func TestPostgreSQLPublicStreamingEndPublication(t *testing.T) {
	for _, variant := range []struct {
		name     string
		preview  bool
		decision string
	}{
		{name: "ordinary-held", decision: "allow"},
		{name: "preview-held", preview: true, decision: "allow"},
		{name: "denied-before-End", decision: "deny"},
	} {
		t.Run(variant.name, func(t *testing.T) {
			vectors := loadPublicStreamingVectors(t)
			provider := &publicApprovalProvider{contentE2EProvider: contentE2EProvider{handoffCaptureProvider: handoffCaptureProvider{bridgeMemoryProjectionProvider: &bridgeMemoryProjectionProvider{}}, path: filepath.Join(t.TempDir(), "effect.txt")}, content: vectors.Content.ToolInputMarker}
			barrier := newPublicTextWriteBarrier(t)
			// Keep any post-tool provider request at its own source gate so the
			// snapshots below describe this request's exact End group.
			h := newPublicStreamingHarness(t, "public-approval", publicStreamingOptions{approval: "ask_for_approval", provider: provider, wrapWriter: barrier.wrap, gateway: map[string]any{"holdEveryRequest": true}})
			t.Cleanup(barrier.unblock)
			var types []string
			if variant.preview {
				types = []string{"agent.message"}
			}
			h.open(t, "held", types, "")
			if variant.preview {
				h.open(t, "healthy", nil, "")
			} else {
				h.open(t, "healthy", []string{"agent.message"}, "")
			}
			h.send(t)
			h.waitFragments(t)
			h.releaseFragments(t, 3)
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 1)
			for _, viewer := range []string{"held", "healthy"} {
				s := h.waitEvent(t, viewer, "agent.tool_use", 1)
				if countPublicEvents(s, "agent.message") != 0 || countPublicEvents(s, "span.model_request_end") != 0 {
					t.Fatal("complete text/End escaped while provider request remained open")
				}
			}
			var tool string
			if err := h.db.QueryRow(`SELECT event_id FROM session_events WHERE session_id=$1 AND type='agent.tool_use' AND payload_json::jsonb->>'evaluated_permission'='ask'`, h.session).Scan(&tool); err != nil {
				t.Fatal(err)
			}
			var listed publicProjectionPage
			if json.Unmarshal(h.client.control(t, "list", map[string]any{"sessionId": h.session, "limit": 100, "order": "asc"}), &listed) != nil {
				t.Fatal("actual SDK history decode")
			}
			found := false
			for _, event := range listed.Data {
				if publicEventType(event) == "agent.message" && publicText(event) == "alpha βeta omega\n" {
					found = true
				}
			}
			if !found || provider.calls.Load() != 0 {
				t.Fatal("committed text unavailable in history or permission wait dispatched tool")
			}
			h.client.control(t, "confirm", map[string]any{"sessionId": h.session, "toolUseEventId": tool, "result": variant.decision})
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='user.tool_confirmation'`, h.session, 1)
			// Observe the actual Runtime confirmation ACK through its committed
			// inbox and Queue custody before releasing the original provider End.
			// This targets live route dispatch, rather than a later idle recovery.
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_runtime_inbox i
			  JOIN queue_jobs j ON j.workspace_id=i.workspace_id
			    AND j.dedupe_key='runtime_input:'||i.workspace_id||':'||i.session_id||':'||i.runtime_input_id
			  JOIN session_events e ON e.workspace_id=i.workspace_id AND e.session_id=i.session_id
			    AND i.event_ids_json::jsonb ? e.event_id
			  WHERE i.session_id=$1 AND i.input_kind='tool_confirmation' AND i.status='committed'
			    AND j.status='acknowledged' AND e.type='user.tool_confirmation'`, h.session, 1)
			var confirmationTool, confirmationDecision string
			if err := h.db.QueryRow(`SELECT payload_json::jsonb->>'tool_use_id',payload_json::jsonb->>'result'
			  FROM session_events WHERE session_id=$1 AND type='user.tool_confirmation'`, h.session).Scan(&confirmationTool, &confirmationDecision); err != nil || confirmationTool != tool || confirmationDecision != variant.decision {
				t.Fatalf("applied confirmation differs from named decision: tool=%s decision=%s err=%v", confirmationTool, confirmationDecision, err)
			}
			barrier.armed.Store(true)
			h.finish(t)
			select {
			case <-barrier.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("production End-group text write not reached")
			}
			// Tool continuation runs through its existing scheduler after provider
			// completion while this particular consumer still owns a blocked write.
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, h.session, 1)
			var resultTool string
			var resultIsError bool
			if err := h.db.QueryRow(`SELECT payload_json::jsonb->>'tool_use_id',
			  COALESCE((payload_json::jsonb->>'is_error')::boolean,false)
			  FROM session_events WHERE session_id=$1 AND type='agent.tool_result'`, h.session).Scan(&resultTool, &resultIsError); err != nil || resultTool != tool || resultIsError != (variant.decision == "deny") {
				t.Fatalf("named Tool Result differs from decision: tool=%s error=%t err=%v", resultTool, resultIsError, err)
			}
			if variant.decision == "deny" {
				if provider.calls.Load() != 0 {
					t.Fatal("denied live Tool dispatched external execution")
				}
				waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_runtime_tool_results WHERE session_id=$1 AND tool_kind='sandbox_tool'`, h.session, 0)
				if _, err := os.Stat(provider.path); !os.IsNotExist(err) {
					t.Fatalf("denied Tool has a filesystem effect: %v", err)
				}
			} else {
				if provider.calls.Load() != 1 {
					t.Fatal("approved external tool did not execute exactly once")
				}
				body, err := os.ReadFile(provider.path)
				if err != nil || string(body) != vectors.Content.ToolInputMarker {
					t.Fatal("external execution ledger differs")
				}
			}
			h.assertFormal(t, h.waitEvent(t, "healthy", "span.model_request_end", 1), []string{"alpha βeta omega\n"})
			if countPublicEvents(h.snapshot(t, "held"), "agent.message") != 0 {
				t.Fatal("held viewer consumed blocked formal message")
			}
			barrier.unblock()
			h.assertFormal(t, h.waitEvent(t, "held", "span.model_request_end", 1), []string{"alpha βeta omega\n"})
			publicLogAssertion(t, "committed-text-permission-confirmation-and-tool-settlement-independent-of-held-End-writer")
		})
	}
}
