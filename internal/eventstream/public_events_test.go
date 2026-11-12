package eventstream_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/api"
	"github.com/tetral-ai/tetral/integration/enginetest"
	eventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/eventwire"
)

// storedPublicEvents is one payload, with the fields its writer stores, for
// every public event type, and for session.error one for every error the
// file declares.
func storedPublicEvents() (events map[string]string, sessionErrors map[string]string) {
	repository := func(errorType string) string {
		return `{"type":"` + errorType + `","repository_url":"https://github.com/acme/app","message":"preparation failed","retry_status":{"type":"retrying"}}`
	}
	model := func(errorType string) string {
		return `{"type":"` + errorType + `","message":"the model request failed","retry_status":{"type":"terminal"}}`
	}
	return map[string]string{
			"user.message":                      `{"content":[{"type":"text","text":"hello"}]}`,
			"user.interrupt":                    `{}`,
			"user.tool_confirmation":            `{"tool_use_id":"sevt_tool_confirmation","result":"deny","deny_message":"not now"}`,
			"agent.message":                     `{"content":[{"type":"text","text":"answer"}]}`,
			"agent.thinking":                    `{}`,
			"agent.tool_use":                    `{"name":"Bash","input":{"command":"pwd"},"evaluated_permission":"ask"}`,
			"agent.tool_result":                 `{"tool_use_id":"sevt_tool_use","content":[{"type":"text","text":"done"}],"is_error":false}`,
			"agent.mcp_tool_use":                `{"name":"create_issue","input":{"title":"Bug"},"mcp_server_name":"github","evaluated_permission":"allow"}`,
			"agent.mcp_tool_result":             `{"mcp_tool_use_id":"sevt_mcp_tool_use","content":[{"type":"text","text":"created"}],"is_error":false}`,
			"agent.thread_context_compacted":    `{}`,
			"agent.thread_message_sent":         `{"delivery_id":"delivery_sent","source_thread_id":"sthr_source","target_thread_id":"sthr_target","target_task_name":"callable_target","source_tool_use_event_id":"sevt_source_tool","message":{"content":[{"type":"text","text":"first block"}]},"task_name":"alias","agent_type":"research"}`,
			"agent.thread_message_received":     `{"delivery_id":"delivery_received","source_thread_id":"sthr_source","source_task_name":"callable_source","source_tool_use_event_id":"sevt_source_tool","message":{"content":[{"type":"text","text":"first block"}]},"task_name":"alias","agent_type":"general"}`,
			"session.status_running":            `{}`,
			"session.status_rescheduled":        `{}`,
			"session.status_idle":               `{"stop_reason":{"type":"end_turn"}}`,
			"session.status_terminated":         `{}`,
			"session.thread_created":            `{"session_thread_id":"sthr_created","parent_thread_id":"sthr_parent","role":"subagent","visibility":"public","task_name":"callable_created","agent_type":"worker"}`,
			"session.thread_status_running":     `{"session_thread_id":"sthr_running","task_name":"callable_running","agent_type":"general"}`,
			"session.thread_status_idle":        `{"session_thread_id":"sthr_idle","task_name":"callable_idle","agent_type":"research","stop_reason":{"type":"requires_action","event_ids":["sevt_wait_second","sevt_wait_first"]}}`,
			"session.thread_status_rescheduled": `{"session_thread_id":"sthr_rescheduled","task_name":"callable_rescheduled","agent_type":"worker"}`,
			"session.thread_status_terminated":  `{"session_thread_id":"sthr_terminated","task_name":"callable_terminated","agent_type":"general"}`,
			"session.updated":                   `{}`,
			"session.computer_replaced":         `{"computer_id":"comp_example","instance_id":"inst_retired"}`,
			"session.deleted":                   `{}`,
			"span.model_request_start":          `{}`,
			"span.model_request_end":            `{"model_request_start_id":"sevt_model_start","is_error":false,"model_usage":{"cache_creation_input_tokens":1,"cache_read_input_tokens":2,"input_tokens":3,"output_tokens":4,"speed":null}}`,
		}, map[string]string{
			"unknown_error":                   model("unknown_error"),
			"model_overloaded_error":          model("model_overloaded_error"),
			"model_rate_limited_error":        model("model_rate_limited_error"),
			"model_request_failed_error":      model("model_request_failed_error"),
			"mcp_connection_failed_error":     `{"type":"mcp_connection_failed_error","mcp_server_name":"github","message":"MCP connection failed.","retry_status":{"type":"exhausted"}}`,
			"mcp_authentication_failed_error": `{"type":"mcp_authentication_failed_error","mcp_server_name":"github","message":"MCP authentication failed after refresh.","retry_status":{"type":"terminal"}}`,
			"repository_authentication_error": repository("repository_authentication_error"),
			"repository_forbidden_error":      repository("repository_forbidden_error"),
			"repository_not_found_error":      repository("repository_not_found_error"),
			"repository_checkout_error":       repository("repository_checkout_error"),
			"repository_clone_error":          repository("repository_clone_error"),
			"computer_preparation_error":      `{"type":"computer_preparation_error","message":"The working directory /Users/me/code/shop does not exist.","computer_id":"comp_example","code":"cwd_not_found","retry_status":{"type":"retrying"}}`,
			"computer_sandbox_error":          `{"type":"computer_sandbox_error","message":"The machine cannot enforce the sandbox.","computer_id":"comp_example","retry_status":{"type":"retrying"}}`,
			"computer_cleanup_error":          `{"type":"computer_cleanup_error","computer_id":"comp_example","message":"Operator cleanup required.","cleanup_failures":[{"path":"/owned/repository","message":"Permission denied"}],"retry_status":{"type":"terminal"}}`,
			"drive_mount_error":               `{"type":"drive_mount_error","message":"Drive drive_team_ai did not mount: drives are not enabled.","drive_id":"drive_team_ai","computer_id":"comp_example","retry_status":{"type":"retrying"}}`,
			"drive_changes_lost_error":        `{"type":"drive_changes_lost_error","message":"1 change to drive drive_team_ai could not be confirmed saved before computer comp_example was removed.","drive_id":"drive_team_ai","computer_id":"comp_example","paths":["notes/a.md"],"paths_total":1,"retry_status":{"type":"terminal"}}`,
		}
}

// declaredSessionErrors lists the error types the file gives session.error.
func declaredSessionErrors(t *testing.T) []string {
	t.Helper()
	data, err := api.Files.ReadFile(api.File)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Discriminator struct {
						Mapping map[string]string `json:"mapping"`
					} `json:"discriminator"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	return slices.Sorted(maps.Keys(file.Components.Schemas["BetaManagedAgentsSessionErrorEvent"].Properties["error"].Discriminator.Mapping))
}

// Every public event type, and every error of session.error, is projected
// from its stored payload into an event within Engine's OpenAPI file, on the
// list and on the stream of a Session and of a thread. The stored payload
// also holds the row's internal fields and fields named as the event's own;
// none of them reaches the event, which the file's closed objects show.
func TestEveryPublicEventProjectsWithinTheFile(t *testing.T) {
	stored, sessionErrors := storedPublicEvents()
	types := slices.Sorted(maps.Keys(stored))
	if want := slices.DeleteFunc(slices.Sorted(slices.Values(eventwire.PublicEventTypes())), func(name string) bool { return name == "session.error" }); !slices.Equal(types, want) {
		t.Fatalf("stored payloads for %v; want one for each public event type but session.error, %v", types, want)
	}
	if declared := declaredSessionErrors(t); !slices.Equal(slices.Sorted(maps.Keys(sessionErrors)), declared) {
		t.Fatalf("stored session.error payloads for %v; want one for each error the file declares, %v", slices.Sorted(maps.Keys(sessionErrors)), declared)
	}
	processedAt := "2026-07-14T12:34:56Z"
	var events []eventstream.Event
	add := func(eventType, payload string) {
		t.Helper()
		var fields map[string]any
		if err := json.Unmarshal([]byte(payload), &fields); err != nil {
			t.Fatalf("stored %s payload: %v", eventType, err)
		}
		for name, value := range map[string]any{"id": "payload_id", "type": "payload.type", "processed_at": "payload_time", "payload": map[string]any{"secret": true},
			"session_id": "sesn_internal", "thread_id": "sthr_internal", "runtime_input_id": "rin_internal", "queue_job_id": "job_internal",
			"partition_key": "partition_internal", "correlation_id": "correlation_internal"} {
			fields[name] = value
		}
		row, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, eventstream.Event{ID: "sevt_" + eventType, Type: eventType, SessionID: "sesn_internal", ThreadID: "sthr_internal", ProcessedAt: &processedAt, Payload: row})
	}
	for _, eventType := range types {
		if eventType != "session.deleted" {
			add(eventType, stored[eventType])
		}
	}
	for _, errorType := range slices.Sorted(maps.Keys(sessionErrors)) {
		add("session.error", `{"error":`+sessionErrors[errorType]+`}`)
	}
	// session.deleted ends a stream, so it is the last row.
	add("session.deleted", stored["session.deleted"])
	changes := make([]eventstream.StreamChange, len(events))
	for index, event := range events {
		changes[index] = eventstream.StreamChange{StreamPosition: int64(index + 1), Event: event}
	}
	signer, verifier := testInternalPrincipalPair(t)
	for _, scope := range []string{"/v1/sessions/sesn_public", "/v1/sessions/sesn_public/threads/sthr_public"} {
		list, stream := scope+"/events?beta=true", scope+"/stream?beta=true"
		if scope == "/v1/sessions/sesn_public" {
			stream = scope + "/events/stream?beta=true"
		}
		recorder := httptest.NewRecorder()
		enginetest.NewDeveloper(t).Handler(listRouter(&recordingReader{listResult: eventstream.ListResult{Data: events}}, verifier, nil)).
			ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, list))
		var page struct {
			Data []struct {
				ID          string `json:"id"`
				Type        string `json:"type"`
				ProcessedAt string `json:"processed_at"`
			} `json:"data"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &page); err != nil || recorder.Code != http.StatusOK || len(page.Data) != len(events) {
			t.Fatalf("GET %s = %d %s; want the %d events", list, recorder.Code, recorder.Body, len(events))
		}
		for index, event := range page.Data {
			if event.ID != events[index].ID || event.Type != events[index].Type || event.ProcessedAt != processedAt {
				t.Fatalf("listed event %d = %+v; want the row's id, type and time of %s", index, event, events[index].ID)
			}
		}
		streamed := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
		request := signedRequest(t, signer, http.MethodGet, stream)
		request.Header.Set("Accept", "text/event-stream")
		enginetest.NewDeveloper(t).Handler(streamRouter(t, &recordingReader{changes: changes}, verifier)).ServeHTTP(streamed, request)
		if sent := strings.Count(streamed.Body.String(), "\ndata: "); streamed.Code != http.StatusOK || sent != len(events) {
			t.Fatalf("GET %s = %d with %d events; want the %d events: %s", stream, streamed.Code, sent, len(events), streamed.Body)
		}
	}
}
