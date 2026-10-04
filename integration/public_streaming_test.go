package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPostgreSQLPublicStreamingIdentity(t *testing.T) {
	t.Run("session-options-primary-thread-and-private-content", func(t *testing.T) {
		h := newPublicStreamingHarness(t, "public-text", publicStreamingOptions{})
		options := []struct {
			name  string
			types []string
		}{{"ordinary", nil}, {"message", []string{"agent.message"}}, {"thinking", []string{"agent.thinking"}}, {"both", []string{"agent.message", "agent.thinking"}}, {"duplicates", []string{"agent.message", "agent.message", "agent.thinking"}}}
		h.open(t, "ordinary", nil, "")
		if h.metric(t, "event_stream_preview_subscriptions") != 0 {
			t.Fatal("ordinary viewer subscribed to previews")
		}
		for _, option := range options[1:] {
			h.open(t, option.name, option.types, "")
		}
		h.open(t, "primary-thread", nil, h.thread(t))
		if h.metric(t, "event_stream_preview_subscriptions") != 1 || h.metric(t, "event_stream_preview_viewers") != 4 {
			t.Fatal("Session opted-in viewers do not share exactly one subscription")
		}
		for _, query := range []string{"event_deltas[]=agent.tool_use", "event_deltas=agent.message", "event_deltas[]=", "event_deltas[0]=agent.message"} {
			h.rejectPreviewQuery(t, "/v1/sessions/"+h.session+"/events/stream?beta=true&"+query)
		}
		for _, query := range []string{"event_deltas[]=agent.message", "event_deltas[]=agent.thinking"} {
			h.rejectPreviewQuery(t, "/v1/sessions/"+h.session+"/threads/"+h.thread(t)+"/stream?beta=true&"+query)
		}
		h.send(t)
		h.waitFragments(t)
		h.releaseFragments(t, 1)
		h.waitEvent(t, "message", "event_delta", 1)
		h.releaseFragments(t, 3)
		waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 2)
		// Receipt commit is independent of End publication. The known thinking
		// event supplies a formal sentinel after the first preview start.
		h.waitEvent(t, "ordinary", "agent.thinking", 1)
		for _, name := range []string{"ordinary", "message", "both", "primary-thread"} {
			for _, event := range h.snapshot(t, name).Events {
				if publicEventType(event) == "agent.message" {
					t.Fatal("complete text published before request End")
				}
			}
		}
		before := h.reader.polls.Load()
		publicWait(t, "formal poll after committed texts", func() bool { return h.reader.polls.Load() > before })
		h.finish(t)
		expected := []string{"alpha βeta omega\n", "second\n"}
		for _, option := range options {
			result := h.waitEvent(t, option.name, "span.model_request_end", 1)
			h.assertFormal(t, result, expected)
			assertPublicPreviewShapes(t, result, option.types, expected)
		}
		thread := h.waitEvent(t, "primary-thread", "span.model_request_end", 1)
		h.assertFormal(t, thread, expected)
		assertPublicPreviewShapes(t, thread, nil, expected)
		h.assertPrivateContent(t)
		h.assertReceiptIdentities(t, h.snapshot(t, "both"))
		assertContentSDKTextHistory(t, h.contentE2E, expected)
		assertContentE2EProvider(t, h.contentE2E, h.gateway.control(t, map[string]any{"kind": "observe"}, "observation"), 1, 0)
		publicLogAssertion(t, "actual-sdk-session-query-options-thread-formal-only")
	})
	for _, scenario := range []string{"public-unicode-interleaved", "public-unicode-healthy"} {
		t.Run(scenario, func(t *testing.T) {
			h := newPublicStreamingHarness(t, scenario, publicStreamingOptions{})
			h.open(t, "unicode", []string{"agent.message"}, "")
			h.send(t)
			h.waitFragments(t)
			fragments := 4
			if scenario == "public-unicode-healthy" {
				fragments = 2
			}
			for i := 0; i < fragments; i++ {
				h.releaseFragments(t, 1)
				if i < fragments-1 {
					h.waitFragments(t)
				}
			}
			waitContentSQLCount(t, h.db, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='agent.message'`, h.session, 2)
			if scenario == "public-unicode-healthy" {
				h.waitEvent(t, "unicode", "event_delta", 2)
			}
			h.finish(t)
			result := h.waitEvent(t, "unicode", "span.model_request_end", 1)
			// Block B completes first in the provider stream, therefore its original
			// durable insert sequence precedes A. Preview identities still bind each.
			h.assertFormal(t, result, []string{"𝄞", "😀"})
			assertPublicPreviewShapes(t, result, []string{"agent.message"}, []string{"𝄞", "😀"})
			if scenario == "public-unicode-healthy" && countPublicEvents(result, "event_delta") == 0 {
				t.Fatal("healthy Unicode control did not preview")
			}
			publicLogAssertion(t, "unicode-correct-per-event-prefix-and-original-final")
		})
	}
}

func (h *publicStreamingHarness) assertReceiptIdentities(t *testing.T, snapshot publicSDKSnapshot) {
	t.Helper()
	type receipt struct{ Boundary, EventType, EventID, WriteID, ModelRequestID string }
	var trace []receipt
	publicWait(t, "actual Runtime submissions and Bridge receipt trace", func() bool {
		raw, err := os.ReadFile(filepath.Join(h.runtime.directory, "trace.jsonl"))
		if err != nil {
			return false
		}
		trace = nil
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var item receipt
			if json.Unmarshal([]byte(line), &item) != nil {
				return false
			}
			trace = append(trace, item)
		}
		count := 0
		for _, item := range trace {
			if item.Boundary == "event-ack" && (item.EventType == "agent.message" || item.EventType == "agent.thinking") {
				count++
			}
		}
		return count == 3
	})
	for _, event := range snapshot.Events {
		kind := publicEventType(event)
		if kind != "agent.message" && kind != "agent.thinking" {
			continue
		}
		id := publicEventID(event)
		submission := ""
		ack := ""
		request := ""
		for _, item := range trace {
			if item.EventID == id && item.EventType == kind {
				if item.Boundary == "event-submission" {
					submission = item.WriteID
					request = item.ModelRequestID
				}
				if item.Boundary == "event-ack" {
					ack = item.WriteID
				}
			}
		}
		if submission == "" || submission != ack || request == "" {
			t.Fatal("Gateway identity not preserved through actual Runtime submission and Bridge acknowledgment")
		}
		found := false
		for _, frame := range h.tap.snapshot() {
			if frame.Kind == "event_start" && frame.EventID == id && frame.EventType == kind && frame.ModelRequestID == request {
				found = true
			}
		}
		if !found {
			t.Fatal("public identity lacks actual Gateway allocation on native NATS")
		}
		var count int
		if err := h.db.QueryRow(`SELECT count(*) FROM session_events WHERE session_id=$1 AND event_id=$2 AND model_request_id=$3`, h.session, id, request).Scan(&count); err != nil || count != 1 {
			t.Fatal("acknowledged identity changed at SQL scope boundary")
		}
	}
	publicLogAssertion(t, "Gateway-allocation-Runtime-submission-Bridge-receipt-SQL-SDK-identities-match")
}
func (h *publicStreamingHarness) rejectPreviewQuery(t *testing.T, path string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Api-Key", h.apiKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusBadRequest || strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("invalid preview query status=%d type=%s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 65536))
}
func countPublicEvents(snapshot publicSDKSnapshot, kind string) int {
	count := 0
	for _, event := range snapshot.Events {
		if publicEventType(event) == kind {
			count++
		}
	}
	return count
}
func publicKeys(object map[string]any) []string {
	result := make([]string, 0, len(object))
	for key := range object {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
func requirePublicKeys(t *testing.T, object map[string]any, keys ...string) {
	t.Helper()
	sort.Strings(keys)
	if !reflect.DeepEqual(publicKeys(object), keys) {
		t.Fatalf("public wrapper keys=%v expected=%v", publicKeys(object), keys)
	}
}
func assertPublicPreviewShapes(t *testing.T, snapshot publicSDKSnapshot, allowed []string, expected []string) {
	t.Helper()
	types := map[string]bool{}
	for _, kind := range allowed {
		types[kind] = true
	}
	starts := map[string]string{}
	previews := map[string]string{}
	finals := map[string]string{}
	for _, event := range snapshot.Events {
		switch publicEventType(event) {
		case "event_start":
			requirePublicKeys(t, event, "type", "event")
			body, ok := event["event"].(map[string]any)
			if !ok {
				t.Fatal("invalid SDK preview start")
			}
			requirePublicKeys(t, body, "id", "type")
			id, kind := publicEventID(body), publicEventType(body)
			if id == "" || !types[kind] || starts[id] != "" {
				t.Fatal("unexpected or duplicate preview start")
			}
			starts[id] = kind
		case "event_delta":
			requirePublicKeys(t, event, "type", "event_id", "delta")
			id, _ := event["event_id"].(string)
			if starts[id] != "agent.message" {
				t.Fatal("delta lacks matching message start")
			}
			delta, ok := event["delta"].(map[string]any)
			if !ok {
				t.Fatal("invalid delta")
			}
			requirePublicKeys(t, delta, "type", "index", "content")
			if delta["type"] != "content_delta" || delta["index"] != float64(0) {
				t.Fatal("wrong delta shape")
			}
			content, ok := delta["content"].(map[string]any)
			if !ok {
				t.Fatal("invalid delta content")
			}
			requirePublicKeys(t, content, "type", "text")
			text, ok := content["text"].(string)
			if !ok || text == "" || content["type"] != "text" || strings.ContainsRune(text, '�') {
				t.Fatal("invalid Unicode preview text")
			}
			previews[id] += text
		case "agent.message":
			finals[publicEventID(event)] = publicText(event)
		}
	}
	for id, prefix := range previews {
		if !strings.HasPrefix(finals[id], prefix) {
			t.Fatalf("preview is not an original event's contiguous prefix: id=%s", id)
		}
	}
	if len(allowed) == 0 && (len(previews) > 0 || len(starts) > 0) {
		t.Fatal("formal-only viewer received preview")
	}
	publicLogAssertion(t, "exact-sdk-preview-wrappers-and-per-event-prefix")
}
func (h *publicStreamingHarness) assertPrivateContent(t *testing.T) {
	t.Helper()
	var data string
	if err := h.db.QueryRow(`SELECT data_json::text FROM session_messages WHERE session_id=$1 AND data_json::text LIKE '%private-reasoning-marker%' LIMIT 1`, h.session).Scan(&data); err != nil {
		t.Fatal("reasoning body was not durably committed")
	}
	if !strings.Contains(data, "private-provider-signature-marker") {
		t.Fatal("reasoning metadata was not durably committed")
	}
	var thinking string
	if err := h.db.QueryRow(`SELECT payload_json::text FROM session_events WHERE session_id=$1 AND type='agent.thinking' LIMIT 1`, h.session).Scan(&thinking); err != nil {
		t.Fatal(err)
	}
	frames, err := json.Marshal(h.tap.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"private-reasoning-marker", "private-provider-signature-marker", "private-tool-input-marker"} {
		if strings.Contains(string(frames), marker) || strings.Contains(thinking, marker) {
			t.Fatal("private content escaped through preview/thinking projection")
		}
	}
	publicLogAssertion(t, "durable-private-reasoning-and-metadata-no-preview-leak")
}
