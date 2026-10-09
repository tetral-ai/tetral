package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridge "github.com/tetral-ai/tetral/services/bridge"
)

func TestReplicaProviderLifecycle(t *testing.T) {
	t.Run("persisted_continuation", testReplicaProviderContinuation)
	for _, variant := range []string{"admission_cancel", "drop", "drain_complete", "drain_force"} {
		t.Run(variant, func(t *testing.T) {
			backend := newReplicaProviderHTTP(t)
			ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "bun", "run", "packages/runtime-pod/test/fixtures/replica-provider.ts", backend.server.URL, variant)
			command.Dir = "../services/agent-runtime"
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("actual Runtime/Gateway/provider child: %v\n%s", err, output)
			}
			var report struct {
				CompletionSamples []replicaCompletionSample `json:"completionSamples"`
				OK                bool                      `json:"ok"`
				Starts            int                       `json:"starts"`
				Cancelled         int                       `json:"cancelled"`
				Completed         int                       `json:"completed"`
			}
			if err := json.Unmarshal(output, &report); err != nil || !report.OK {
				t.Fatalf("missing child report: %s (%v)", output, err)
			}
			if len(report.CompletionSamples) == 0 {
				t.Fatal("missing actual completion samples")
			}
			for _, sample := range report.CompletionSamples {
				replicaLogCompletion(t, sample)
			}
			actual := backend.snapshot()
			if report.Starts != actual.Starts || report.Cancelled != actual.Cancelled || report.Completed != actual.Completed || actual.Active != 0 {
				t.Fatalf("HTTP ledger/child disagreement: %+v %+v", actual, report)
			}
			t.Logf("actual Runtime adapter→Gateway auth/admission→provider SDK→HTTP mode=%s starts=%d cancel=%d complete=%d active=%d", variant, actual.Starts, actual.Cancelled, actual.Completed, actual.Active)
		})
	}
}

type replicaProviderHTTP struct {
	server                               *httptest.Server
	mu                                   sync.Mutex
	calls                                map[string]*replicaProviderHTTPCall
	starts, cancelled, completed, active int
}
type replicaProviderHTTPCall struct {
	finish, drop         chan struct{}
	finishOnce, dropOnce sync.Once
}
type replicaProviderHTTPState struct {
	Starts    int      `json:"starts"`
	Cancelled int      `json:"cancelled"`
	Completed int      `json:"completed"`
	Active    int      `json:"active"`
	Markers   []string `json:"markers"`
}

func (s *replicaProviderHTTP) snapshot() replicaProviderHTTPState {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := replicaProviderHTTPState{Starts: s.starts, Cancelled: s.cancelled, Completed: s.completed, Active: s.active}
	for marker := range s.calls {
		v.Markers = append(v.Markers, marker)
	}
	return v
}

var replicaProviderMarker = regexp.MustCompile(`replica_[a-z0-9_]+`)

func newReplicaProviderHTTP(t *testing.T) *replicaProviderHTTP {
	t.Helper()
	s := &replicaProviderHTTP{calls: map[string]*replicaProviderHTTPCall{}}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/state" {
			_ = json.NewEncoder(w).Encode(s.snapshot())
			return
		}
		if r.URL.Path == "/finish" || r.URL.Path == "/drop" {
			s.mu.Lock()
			call := s.calls[r.URL.Query().Get("marker")]
			s.mu.Unlock()
			if call == nil {
				http.Error(w, "unknown HTTP request", 404)
				return
			}
			if r.URL.Path == "/finish" {
				call.finishOnce.Do(func() { close(call.finish) })
			} else {
				call.dropOnce.Do(func() { close(call.drop) })
			}
			w.WriteHeader(200)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1024*1024))
		if err != nil {
			http.Error(w, "read", 400)
			return
		}
		marker := r.Header.Get("x-replica-marker")
		if marker == "" {
			marker = string(replicaProviderMarker.Find(body))
		}
		if marker == "" {
			http.Error(w, "missing fixture marker", 400)
			return
		}
		call := &replicaProviderHTTPCall{finish: make(chan struct{}), drop: make(chan struct{})}
		s.mu.Lock()
		if _, duplicate := s.calls[marker]; duplicate {
			s.mu.Unlock()
			http.Error(w, "generic replay detected", http.StatusConflict)
			return
		}
		s.calls[marker] = call
		s.starts++
		s.active++
		s.mu.Unlock()
		defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
		w.Header().Set("Content-Type", "text/event-stream")
		send := func(event string, value any) {
			data, _ := json.Marshal(value)
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
			w.(http.Flusher).Flush()
		}
		send("message_start", map[string]any{"type": "message_start", "message": map[string]any{"id": "msg_" + marker, "type": "message", "role": "assistant", "model": "claude-opus-4-8", "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 1, "output_tokens": 0}}})
		send("content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}})
		send("content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": marker + " partial"}})
		select {
		case <-call.finish:
			send("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
			send("message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]any{"output_tokens": 1}})
			send("message_stop", map[string]any{"type": "message_stop"})
			s.mu.Lock()
			s.completed++
			s.mu.Unlock()
		case <-call.drop:
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
		case <-r.Context().Done():
			s.mu.Lock()
			s.cancelled++
			s.mu.Unlock()
		}
	}))
	t.Cleanup(func() {
		s.server.CloseClientConnections()
		s.server.Close()
		if active := s.snapshot().Active; active != 0 {
			t.Errorf("provider HTTP handlers still active: %d", active)
		}
	})
	return s
}

func testReplicaProviderContinuation(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID, threadID, bindingID, podUID = "sesn_replica_provider_core", "sthr_replica_provider_core", "bind_replica_provider_core", "pod_replica_provider_core"
	const signingKey = "replica-provider-binding-key-at-least-32-bytes"
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	sessionfixture.SeedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	client := dbconnect.NewClientForTesting(runtimeDB)
	store := bridge.NewPostgreSQLBridgeAPIStore(client)
	store.RuntimeBindingTokenHMACKey = []byte(signingKey)
	receiver := serveReplicaBridge(t, store, map[string]string{"replica-runtime": podUID}, nil)
	backend := newReplicaProviderHTTP(t)
	dir := t.TempDir()
	readyPath := filepath.Join(dir, "ready.json")
	process := &providerFailureRuntimeProcess{statePath: filepath.Join(dir, "state.json"), closePath: filepath.Join(dir, "close")}
	inputPath := filepath.Join(dir, "input.json")
	input, err := json.Marshal(map[string]any{"bridgeAddress": receiver.Address, "backendURL": backend.server.URL, "signingKey": signingKey, "workspaceId": "default", "sessionId": sessionID, "sessionThreadId": threadID, "bindingId": bindingID, "bindingGeneration": 1, "targetPodUid": podUID, "runtimeProcessId": "process_" + podUID, "readyPath": readyPath, "statePath": process.statePath, "closePath": process.closePath})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(inputPath, input, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	process.command = exec.CommandContext(ctx, "bun", "run", "packages/runtime-pod/test/fixtures/replica-provider-continuation.ts", inputPath)
	process.command.Dir = "../services/agent-runtime"
	process.command.Stdout = &process.output
	process.command.Stderr = &process.output
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if process.command.ProcessState == nil {
			_ = process.command.Process.Kill()
			_ = process.command.Wait()
		}
	})
	deadline := time.Now().Add(120 * time.Second)
	for process.port == 0 && time.Now().Before(deadline) {
		data, err := os.ReadFile(readyPath)
		var ready struct {
			Port int `json:"port"`
		}
		if err == nil && json.Unmarshal(data, &ready) == nil {
			process.port = ready.Port
		}
		if process.port == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if process.port == 0 {
		t.Fatal("actual Runtime readiness report absent")
	}
	events := sessionevent.NewService(sessionevent.NewPostgreSQLStore(client))
	lastCapture := ""
	for turn, text := range []string{"replica_core_first", "replica_core_next_business_input"} {
		appended, err := events.AppendClientEvents(ctx, "default", sessionID, fmt.Sprintf("idem_replica_provider_%d", turn), sessionevent.AppendRequest{Events: []sessionevent.IncomingEvent{{Type: sessionevent.EventTypeUserMessage, Content: []sessionevent.ContentBlock{{Type: sessionevent.ContentBlockTypeText, Text: text}}}}})
		if err != nil || len(appended.Data) != 1 {
			t.Fatalf("append=%+v/%v", appended, err)
		}
		deliverAttachmentRuntimeInput(t, runtimeDB, admin, process.port, sessionID, "runtime-pod-0", podUID)
		writeID, generation, err := waitForPendingOutputCapture(admin, sessionID, lastCapture)
		if err != nil {
			t.Fatal(err)
		}
		if err := settleOutputCaptureGenerationForTest(admin, sessionID, writeID, generation, "staged"); err != nil {
			t.Fatal(err)
		}
		lastCapture = writeID
		waitForProviderFailureFacts(t, admin, sessionID, turn+1, "idle", process)
	}
	report := process.close(t)
	if report.ProviderInvocations != 2 || report.FinishIdleInvocations != 2 || report.FinishIdleResult != "committed" {
		t.Fatalf("durable continuation report=%+v", report)
	}
	var starts, ends, errorEnds, users int
	var assistants string
	if err := admin.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_start'),
 (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end'),
 (SELECT count(*) FROM session_events WHERE session_id=$1 AND type='span.model_request_end' AND payload_json::jsonb->>'is_error'='true'),
 (SELECT count(*) FROM session_messages WHERE session_id=$1 AND kind='user'),
 COALESCE((SELECT string_agg(`+sessionfixture.MessageContentSQL+`,' ') FROM session_messages m WHERE session_id=$1 AND kind='assistant'),'')`, sessionID).Scan(&starts, &ends, &errorEnds, &users, &assistants); err != nil {
		t.Fatal(err)
	}
	if starts != 2 || ends != 2 || errorEnds != 1 || users != 2 || !strings.Contains(assistants, "replica_core_2 partial") || strings.Contains(assistants, "replica_core_1 partial") {
		t.Fatalf("persisted start/end/error/user=%d/%d/%d/%d assistant=%s", starts, ends, errorEnds, users, assistants)
	}
	state := backend.snapshot()
	if state.Starts != 2 || state.Completed != 1 || state.Active != 0 {
		t.Fatalf("HTTP persisted continuation ledger=%+v", state)
	}
	t.Log("actual Core + Bridge persisted failed partial stream and separate successful business input; two distinct model requests, configured reschedule budget0, no generic replay")
}
