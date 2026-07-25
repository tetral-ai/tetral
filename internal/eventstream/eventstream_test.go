package eventstream_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	eventstream "github.com/tetral-ai/tetral/internal/eventstream"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	eventstreamservice "github.com/tetral-ai/tetral/services/event-stream"
)

var eventStreamPageTokenSecret = []byte("eventstream-page-token-secret-32")

func TestEventStreamRoutesRequireSignedInternalPrincipal(t *testing.T) {
	_, verifier := testInternalPrincipalPair(t)
	router := eventstreamservice.NewRouter(&recordingReader{}, verifier)

	for _, test := range []struct {
		name    string
		headers map[string]string
	}{
		{name: "missing", headers: nil},
		{name: "raw api key ignored", headers: map[string]string{"x-api-key": "test-api-key"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_events/events/stream", nil)
			for key, value := range test.headers {
				request.Header.Set(key, value)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorEnvelope(t, recorder, "authentication_error", true)
		})
	}
}

func TestEventStreamListReturnsPublicEventEnvelope(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	processedAt := "2026-07-01T12:00:00Z"
	reader := &recordingReader{
		listResult: eventstream.ListResult{
			Data: []eventstream.Event{
				{ID: "evt_list_1", Type: "user.message", SessionID: "sesn_events", ThreadID: "thr_main", Payload: json.RawMessage(`{"content":[{"type":"text","text":"hello"}]}`)},
				{ID: "evt_list_2", Type: "agent.message", SessionID: "sesn_events", Payload: nil, ProcessedAt: &processedAt},
			},
			NextPage: stringPtr("next-token"),
		},
	}
	router := eventstream.NewListRouter(reader, verifier)
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events?beta=true&limit=2&order=asc")
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if reader.listWorkspace != workspace.DefaultID || reader.listSessionID != "sesn_events" || reader.listOptions.Limit != 2 || reader.listOptions.Order != "asc" {
		t.Fatalf("reader list scope/options = %s %s %+v", reader.listWorkspace, reader.listSessionID, reader.listOptions)
	}
	var response struct {
		Data     []map[string]json.RawMessage `json:"data"`
		NextPage *string                      `json:"next_page"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	if len(response.Data) != 2 || string(response.Data[0]["id"]) != `"evt_list_1"` || string(response.Data[1]["id"]) != `"evt_list_2"` {
		t.Fatalf("events response = %+v body=%s", response.Data, recorder.Body.String())
	}
	for _, event := range response.Data {
		for _, forbidden := range []string{"payload", "session_id", "thread_id"} {
			if _, exists := event[forbidden]; exists {
				t.Fatalf("event contains generic envelope field %q: %s", forbidden, recorder.Body.String())
			}
		}
	}
	if string(response.Data[0]["content"]) != `[{"type":"text","text":"hello"}]` {
		t.Fatalf("flattened content = %s", response.Data[0]["content"])
	}
	if response.NextPage == nil || *response.NextPage != "next-token" {
		t.Fatalf("next_page = %v; want next-token", response.NextPage)
	}
	if string(response.Data[1]["processed_at"]) != `"2026-07-01T12:00:00Z"` {
		t.Fatalf("processed_at = %s; want %s", response.Data[1]["processed_at"], processedAt)
	}
}

func TestEventStreamSessionListProjectsAllPublicChildEventVariants(t *testing.T) {
	testEventStreamListProjectsAllPublicChildEventVariants(t, "/v1/sessions/sesn_child_projection/events?beta=true")
}

func TestEventStreamThreadListProjectsAllPublicChildEventVariants(t *testing.T) {
	testEventStreamListProjectsAllPublicChildEventVariants(t, "/v1/sessions/sesn_child_projection/threads/sthr_child_projection/events?beta=true")
}

func TestEventStreamSessionSSEProjectsAllPublicChildEventVariants(t *testing.T) {
	testEventStreamSSEProjectsAllPublicChildEventVariants(t, "/v1/sessions/sesn_child_projection/events/stream?beta=true")
}

func TestEventStreamThreadSSEProjectsAllPublicChildEventVariants(t *testing.T) {
	testEventStreamSSEProjectsAllPublicChildEventVariants(t, "/v1/sessions/sesn_child_projection/threads/sthr_child_projection/stream?beta=true")
}

func testEventStreamListProjectsAllPublicChildEventVariants(t *testing.T, path string) {
	t.Helper()
	signer, verifier := testInternalPrincipalPair(t)
	fixtures := publicChildOutletFixtures()
	data := make([]eventstream.Event, 0, len(fixtures))
	for _, fixture := range fixtures {
		data = append(data, fixture.event)
	}
	reader := &recordingReader{listResult: eventstream.ListResult{Data: data}}
	recorder := httptest.NewRecorder()
	eventstream.NewListRouter(reader, verifier).ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, path))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode list response: %v body=%s", err, recorder.Body.String())
	}
	assertPublicChildOutletEvents(t, response.Data, fixtures)
}

func testEventStreamSSEProjectsAllPublicChildEventVariants(t *testing.T, path string) {
	t.Helper()
	signer, verifier := testInternalPrincipalPair(t)
	fixtures := publicChildOutletFixtures()
	changes := make([]eventstream.StreamChange, 0, len(fixtures))
	for index, fixture := range fixtures {
		changes = append(changes, eventstream.StreamChange{StreamPosition: int64(index + 1), Event: fixture.event})
	}
	reader := &recordingReader{changes: changes}
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	request := signedRequest(t, signer, http.MethodGet, path)
	request.Header.Set("Accept", "text/event-stream")
	eventstreamservice.NewRouter(
		reader,
		verifier,
		eventstreamservice.WithStreamPollInterval(time.Millisecond),
		eventstreamservice.WithStreamMaxEmptyPolls(1),
	).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var events []map[string]any
	for _, line := range strings.Split(recorder.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatalf("decode SSE data: %v line=%s", err, line)
		}
		events = append(events, event)
	}
	assertPublicChildOutletEvents(t, events, fixtures)
}

type publicChildOutletFixture struct {
	event eventstream.Event
	want  map[string]any
}

func publicChildOutletFixtures() []publicChildOutletFixture {
	processedAt := "2026-07-14T12:34:56Z"
	content := []any{
		map[string]any{"type": "text", "text": "first child block"},
		map[string]any{"type": "text", "text": "second child block"},
	}
	fixtures := []publicChildOutletFixture{
		{
			event: eventstream.Event{ID: "sevt_child_sent", Type: "agent.thread_message_sent", SessionID: "sesn_internal", ThreadID: "sthr_internal_sender", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"delivery_id":"delivery_sent","source_thread_id":"sthr_sender_distinct","target_thread_id":"sthr_target_distinct","target_task_name":"callable_target","source_tool_use_event_id":"sevt_tool_sent","message":{"content":[{"type":"text","text":"first child block"},{"type":"text","text":"second child block"}]},"agent_type":"research","task_name":"wrong_alias"}`)},
			want:  map[string]any{"content": content, "to_session_thread_id": "sthr_target_distinct", "to_agent_name": "callable_target"},
		},
		{
			event: eventstream.Event{ID: "sevt_child_received", Type: "agent.thread_message_received", SessionID: "sesn_internal", ThreadID: "sthr_internal_receiver", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"delivery_id":"delivery_received","source_thread_id":"sthr_source_distinct","source_task_name":"callable_source","source_tool_use_event_id":"sevt_tool_received","message":{"content":[{"type":"text","text":"first child block"},{"type":"text","text":"second child block"}]},"agent_type":"general","task_name":"wrong_alias"}`)},
			want:  map[string]any{"content": content, "from_session_thread_id": "sthr_source_distinct", "from_agent_name": "callable_source"},
		},
		{
			event: eventstream.Event{ID: "sevt_child_created", Type: "session.thread_created", SessionID: "sesn_internal", ThreadID: "sthr_created_distinct", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"session_thread_id":"sthr_created_distinct","parent_thread_id":"sthr_parent_distinct","role":"subagent","visibility":"public","task_name":"callable_created","agent_type":"worker"}`)},
			want:  map[string]any{"session_thread_id": "sthr_created_distinct", "agent_name": "callable_created"},
		},
		{
			event: eventstream.Event{ID: "sevt_child_running", Type: "session.thread_status_running", SessionID: "sesn_internal", ThreadID: "sthr_running_distinct", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"session_thread_id":"sthr_running_distinct","task_name":"callable_running","agent_type":"general"}`)},
			want:  map[string]any{"session_thread_id": "sthr_running_distinct", "agent_name": "callable_running"},
		},
		{
			event: eventstream.Event{ID: "sevt_child_idle", Type: "session.thread_status_idle", SessionID: "sesn_internal", ThreadID: "sthr_idle_distinct", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"session_thread_id":"sthr_idle_distinct","task_name":"callable_idle","agent_type":"research","stop_reason":{"type":"requires_action","event_ids":["sevt_wait_second","sevt_wait_first"]}}`)},
			want:  map[string]any{"session_thread_id": "sthr_idle_distinct", "agent_name": "callable_idle", "stop_reason": map[string]any{"type": "requires_action", "event_ids": []any{"sevt_wait_second", "sevt_wait_first"}}},
		},
		{
			event: eventstream.Event{ID: "sevt_child_rescheduled", Type: "session.thread_status_rescheduled", SessionID: "sesn_internal", ThreadID: "sthr_rescheduled_distinct", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"session_thread_id":"sthr_rescheduled_distinct","task_name":"callable_rescheduled","agent_type":"worker"}`)},
			want:  map[string]any{"session_thread_id": "sthr_rescheduled_distinct", "agent_name": "callable_rescheduled"},
		},
		{
			event: eventstream.Event{ID: "sevt_child_terminated", Type: "session.thread_status_terminated", SessionID: "sesn_internal", ThreadID: "sthr_terminated_distinct", ProcessedAt: &processedAt, Payload: json.RawMessage(`{"session_thread_id":"sthr_terminated_distinct","task_name":"callable_terminated","agent_type":"general"}`)},
			want:  map[string]any{"session_thread_id": "sthr_terminated_distinct", "agent_name": "callable_terminated"},
		},
	}
	for index := range fixtures {
		fixtures[index].want["id"] = fixtures[index].event.ID
		fixtures[index].want["type"] = fixtures[index].event.Type
		fixtures[index].want["processed_at"] = processedAt
	}
	return fixtures
}

func assertPublicChildOutletEvents(t *testing.T, got []map[string]any, fixtures []publicChildOutletFixture) {
	t.Helper()
	if len(got) != len(fixtures) {
		t.Fatalf("public child events count = %d; want %d: %#v", len(got), len(fixtures), got)
	}
	for index, event := range got {
		if !reflect.DeepEqual(event, fixtures[index].want) {
			t.Fatalf("public child event %d = %#v; want exact %#v", index, event, fixtures[index].want)
		}
	}
}

func TestEventStreamSessionListDecodesSDKFiltersAndRejectsUnknownParameters(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	reader := &recordingReader{}
	router := eventstream.NewListRouter(reader, verifier)
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events?beta=true&types[]=user.message&types[]=agent.message&created_at[gt]=2026-01-01T00:00:00Z&created_at[lte]=2026-01-02T00:00:00Z")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if !equalStrings(reader.listOptions.Types, []string{"user.message", "agent.message"}) ||
		reader.listOptions.CreatedAtGT != "2026-01-01T00:00:00.000000000Z" ||
		reader.listOptions.CreatedAtLTE != "2026-01-02T00:00:00.000000000Z" {
		t.Fatalf("decoded list filters = %+v", reader.listOptions)
	}

	request = signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/threads/thr_child/events?beta=true&types[]=user.message&created_at[gte]=2026-01-01T00:00:00Z")
	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("thread status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if !equalStrings(reader.listOptions.Types, []string{"user.message"}) || reader.listOptions.CreatedAtGTE != "2026-01-01T00:00:00.000000000Z" {
		t.Fatalf("decoded thread list filters = %+v", reader.listOptions)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events?beta=true&unknown=1"))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unknown param status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestEventStreamRoutesRequireExactlyOneBetaMarkerBeforeReaderAccess(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	for _, route := range []string{
		"/v1/sessions/sesn_events/events",
		"/v1/sessions/sesn_events/threads/thr_child/events",
		"/v1/sessions/sesn_events/events/stream",
		"/v1/sessions/sesn_events/threads/thr_child/stream",
	} {
		for _, query := range []string{"", "?beta=false", "?beta=true&beta=true", "?beta=true&unknown=1", "?beta=true&bad=%"} {
			t.Run(route+query, func(t *testing.T) {
				reader := &recordingReader{}
				var router http.Handler
				if strings.HasSuffix(route, "/stream") {
					router = eventstreamservice.NewRouter(reader, verifier, eventstreamservice.WithStreamMaxEmptyPolls(1))
				} else {
					router = eventstream.NewListRouter(reader, verifier)
				}
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, route+query))
				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
				}
				if reader.currentCalls != 0 || reader.listCalls != 0 {
					t.Fatalf("reader calls = current:%d list:%d; want zero", reader.currentCalls, reader.listCalls)
				}
			})
		}
	}
}

func TestEventStreamSSEStartsAtCurrentHighWaterAndClosesOnDeleted(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	reader := &recordingReader{
		currentPosition: 10,
		changes: []eventstream.StreamChange{
			{StreamPosition: 9, Event: eventstream.Event{ID: "evt_old", Type: "agent.message", SessionID: "sesn_stream", Payload: json.RawMessage(`{"content":[{"type":"text","text":"old"}]}`)}},
			{StreamPosition: 11, Event: eventstream.Event{ID: "evt_new", Type: "agent.message", SessionID: "sesn_stream", Payload: json.RawMessage(`{"content":[{"type":"text","text":"new"}]}`)}},
			{StreamPosition: 12, Event: eventstream.Event{ID: "evt_deleted", Type: "session.deleted", SessionID: "sesn_stream", Payload: json.RawMessage(`{"deleted":true}`)}},
		},
	}
	router := eventstreamservice.NewRouter(reader, verifier, eventstreamservice.WithStreamPollInterval(time.Millisecond), eventstreamservice.WithStreamMaxEmptyPolls(1))
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_stream/events/stream?beta=true")
	request.Header.Set("Accept", "text/event-stream")
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q; want text/event-stream", got)
	}
	if reader.firstChangeAfter != 10 {
		t.Fatalf("first change cursor = %d; want current high-water 10", reader.firstChangeAfter)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "evt_old") {
		t.Fatalf("SSE replayed pre-high-water event: %s", body)
	}
	for _, want := range []string{
		"event: agent.message\n",
		`data: {"content":[{"type":"text","text":"new"}],"id":"evt_new","processed_at":null,"type":"agent.message"}` + "\n\n",
		"event: session.deleted\n",
		`data: {"id":"evt_deleted","processed_at":null,"type":"session.deleted"}` + "\n\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("SSE body missing %q in %s", want, body)
		}
	}
}

func TestIdleEventStreamEmitsHeartbeatBeforeTheNextPoll(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	reader := &recordingReader{}
	pollInterval := 20 * time.Millisecond
	router := eventstreamservice.NewRouter(
		reader,
		verifier,
		eventstreamservice.WithStreamPollInterval(pollInterval),
	)
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_idle/events/stream?beta=true")
	ctx, cancel := context.WithCancel(request.Context())
	request = request.WithContext(ctx)
	recorder := newLiveFlushRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(recorder, request)
	}()

	deadline := time.NewTimer(pollInterval)
	defer deadline.Stop()
	for {
		select {
		case body := <-recorder.flushes:
			if strings.Contains(body, ": heartbeat\n\n") {
				cancel()
				select {
				case <-done:
					return
				case <-time.After(5 * pollInterval):
					t.Fatal("idle SSE handler did not stop promptly after cancellation")
				}
			}
		case <-deadline.C:
			cancel()
			t.Fatal("idle SSE did not flush a heartbeat within one poll interval")
		}
	}
}

func TestEventStreamThreadRoutesUseThreadScope(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	reader := &recordingReader{
		listResult: eventstream.ListResult{
			Data: []eventstream.Event{
				{ID: "evt_thread", Type: "agent.thread_message_sent", SessionID: "sesn_thread_events", ThreadID: "thr_child", Payload: json.RawMessage(`{"target_thread_id":"thr_target","target_task_name":"child","message":{"content":[{"type":"text","text":"child"}]}}`)},
			},
		},
	}
	router := eventstream.NewListRouter(reader, verifier)
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_thread_events/threads/thr_child/events?limit=2&order=desc&beta=true")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if reader.listWorkspace != workspace.DefaultID || reader.listSessionID != "sesn_thread_events" || reader.listThreadID != "thr_child" || reader.listOptions.Limit != 2 || reader.listOptions.Order != "desc" {
		t.Fatalf("thread reader scope/options = %s %s %s %+v", reader.listWorkspace, reader.listSessionID, reader.listThreadID, reader.listOptions)
	}
	if strings.Contains(recorder.Body.String(), `"thread_id"`) || strings.Contains(recorder.Body.String(), `"session_id"`) || strings.Contains(recorder.Body.String(), `"payload"`) {
		t.Fatalf("thread list body contains generic envelope fields: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"to_agent_name":"child"`) || !strings.Contains(recorder.Body.String(), `"text":"child"`) {
		t.Fatalf("thread list body missing flattened payload field: %s", recorder.Body.String())
	}
}

func TestEventStreamThreadSSEStartsAtThreadHighWater(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	reader := &recordingReader{
		currentPosition: 20,
		changes: []eventstream.StreamChange{
			{StreamPosition: 19, Event: eventstream.Event{ID: "evt_old_thread", Type: "agent.message", SessionID: "sesn_thread_stream", ThreadID: "thr_child", Payload: json.RawMessage(`{"content":[{"type":"text","text":"old"}]}`)}},
			{StreamPosition: 21, Event: eventstream.Event{ID: "evt_new_thread", Type: "agent.thread_message_sent", SessionID: "sesn_thread_stream", ThreadID: "thr_child", Payload: json.RawMessage(`{"target_thread_id":"thr_target","target_task_name":"child","message":{"content":[{"type":"text","text":"new"}]}}`)}},
			{StreamPosition: 22, Event: eventstream.Event{ID: "evt_deleted_thread", Type: "session.deleted", SessionID: "sesn_thread_stream", ThreadID: "thr_child", Payload: json.RawMessage(`{"deleted":true}`)}},
		},
	}
	router := eventstreamservice.NewRouter(reader, verifier, eventstreamservice.WithStreamPollInterval(time.Millisecond), eventstreamservice.WithStreamMaxEmptyPolls(1))
	request := signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_thread_stream/threads/thr_child/stream?beta=true")
	request.Header.Set("Accept", "text/event-stream")
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if reader.firstChangeAfter != 20 {
		t.Fatalf("first thread cursor = %d; want current high-water 20", reader.firstChangeAfter)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "evt_old_thread") || !strings.Contains(body, "evt_new_thread") {
		t.Fatalf("thread SSE body = %s; want only post-high-water thread events", body)
	}
	for _, forbidden := range []string{`"payload"`, `"session_id"`, `"thread_id"`} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("thread SSE body contains generic envelope field %s: %s", forbidden, body)
		}
	}
	if !strings.Contains(body, `"text":"new"`) {
		t.Fatalf("thread SSE body missing flattened payload field: %s", body)
	}
}

func TestEventStreamServiceRouterDoesNotServeListRoutes(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	router := eventstreamservice.NewRouter(&recordingReader{}, verifier)
	for _, path := range []string{
		"/v1/sessions/sesn_events/events",
		"/v1/sessions/sesn_events/threads/thr_events/events",
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, path))
		if recorder.Code != http.StatusNotFound && recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d; want route absent", path, recorder.Code)
		}
	}
}

func TestPostgreSQLReaderListsAndStreamsPublicSessionVisibleEvents(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_reader", "thr_reader")
	seedEventStreamSubagentThread(t, admin, "default", "sesn_reader", "thr_reader", "thr_second")
	seedEventStreamInternalReviewer(t, admin, "default", "sesn_reader", "thr_reader", "thr_reviewer")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_reader", "evt_visible_1", 1, "user.message", `{"content":[{"type":"text","text":"one"}]}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_second", "evt_visible_same_sequence", 1, "agent.message", `{"text":"same sequence"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_reader", "evt_internal", 2, "agent.message", `{"text":"internal"}`, "internal", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_child", "evt_child_hidden", 3, "agent.message", `{"text":"hidden"}`, "public", false, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_reader", "evt_visible_2", 4, "agent.message", `{"text":"two"}`, "public", true, "2026-07-01T12:00:00Z")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_reviewer", "evt_reviewer_failure_mislabeled_public", 5, "approval_review.failure", `{"type":"approval_review.failure","review_id":"arvw_reader","parent_thread_id":"thr_reader","target_model_tool_call_id":"tool_call_reader","target_tool_name":"Write","failure_kind":"parse_failure","message":"approval reviewer decision is not JSON"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "thr_reviewer", "evt_reviewer_compaction_mislabeled_public", 6, "agent.thread_context_compacted", `{"type":"agent.thread_context_compacted","summary":"internal reviewer summary","recent_context":[]}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_reader", "", "evt_deleted", 6, "session.deleted", `{"id":"sesn_reader","type":"session.deleted"}`, "public", true, "2026-07-01T12:01:00Z")
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reader", "evt_visible_1", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_second", "evt_visible_same_sequence", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reader", "evt_internal", 1, "internal", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_child", "evt_child_hidden", 1, "public", false)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reader", "evt_visible_2", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reader", "evt_visible_2", 2, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reviewer", "evt_reviewer_failure_mislabeled_public", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "thr_reviewer", "evt_reviewer_compaction_mislabeled_public", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_reader", "", "evt_deleted", 1, "public", true)

	reader := newPostgreSQLEventReader(runtime)
	first, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_reader", eventstream.ListOptions{Limit: 1, Order: "asc"})
	if err != nil {
		t.Fatalf("ListSessionEvents first page: %v", err)
	}
	if len(first.Data) != 1 || first.Data[0].ID != "evt_visible_1" || first.NextPage == nil {
		t.Fatalf("first page = %+v next=%v", first.Data, first.NextPage)
	}
	second, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_reader", eventstream.ListOptions{Limit: 10, Order: "asc", Page: *first.NextPage})
	if err != nil {
		t.Fatalf("ListSessionEvents second page: %v", err)
	}
	if len(second.Data) != 3 || second.Data[0].ID != "evt_visible_same_sequence" || second.Data[1].ID != "evt_visible_2" || second.Data[1].ProcessedAt == nil || second.Data[2].ID != "evt_deleted" || second.Data[2].ThreadID != "" {
		t.Fatalf("second page = %+v", second.Data)
	}
	position, err := reader.CurrentStreamPosition(context.Background(), workspace.DefaultID, "sesn_reader")
	if err != nil {
		t.Fatalf("CurrentStreamPosition: %v", err)
	}
	if position == 0 {
		t.Fatal("stream position = 0; want visible change high-water")
	}
	changes, err := reader.ListSessionEventChanges(context.Background(), workspace.DefaultID, "sesn_reader", 0, 10)
	if err != nil {
		t.Fatalf("ListSessionEventChanges: %v", err)
	}
	if len(changes) != 5 || changes[0].Event.ID != "evt_visible_1" || changes[1].Event.ID != "evt_visible_same_sequence" || changes[2].Event.ID != "evt_visible_2" || changes[3].Event.ID != "evt_visible_2" || changes[4].Event.ID != "evt_deleted" || changes[4].Event.ThreadID != "" {
		t.Fatalf("stream changes = %+v; want public session-visible stream revisions", changes)
	}
	if position != changes[4].StreamPosition {
		t.Fatalf("stream position = %d; want last public non-reviewer stream position %d", position, changes[4].StreamPosition)
	}

	threadEvents, err := reader.ListThreadEvents(context.Background(), workspace.DefaultID, "sesn_reader", "thr_child", eventstream.ListOptions{Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("ListThreadEvents: %v", err)
	}
	if len(threadEvents.Data) != 1 || threadEvents.Data[0].ID != "evt_child_hidden" || threadEvents.Data[0].ThreadID != "thr_child" {
		t.Fatalf("thread events = %+v; want public child event even when session_visible=false", threadEvents.Data)
	}
	threadPosition, err := reader.CurrentThreadStreamPosition(context.Background(), workspace.DefaultID, "sesn_reader", "thr_child")
	if err != nil {
		t.Fatalf("CurrentThreadStreamPosition: %v", err)
	}
	if threadPosition == 0 {
		t.Fatal("thread stream position = 0; want child-thread high-water")
	}
	threadChanges, err := reader.ListThreadEventChanges(context.Background(), workspace.DefaultID, "sesn_reader", "thr_child", 0, 10)
	if err != nil {
		t.Fatalf("ListThreadEventChanges: %v", err)
	}
	if len(threadChanges) != 1 || threadChanges[0].Event.ID != "evt_child_hidden" {
		t.Fatalf("thread stream changes = %+v; want child hidden event only", threadChanges)
	}
	if _, err := reader.ListThreadEvents(context.Background(), workspace.DefaultID, "sesn_reader", "thr_reviewer", eventstream.ListOptions{Limit: 10, Order: "asc"}); err == nil {
		t.Fatal("ListThreadEvents returned internal reviewer; want not found")
	}
}

func TestPostgreSQLReaderRedactsStableReasoningLedgerFromListAndStream(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_reasoning_redaction", "thr_reasoning_redaction")
	seedEventStreamEvent(
		t,
		admin,
		"default",
		"sesn_reasoning_redaction",
		"thr_reasoning_redaction",
		"evt_reasoning_redaction",
		1,
		"agent.tool_use",
		`{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"}}`,
		"public",
		true,
		"",
	)
	seedEventStreamChange(t, admin, "default", "sesn_reasoning_redaction", "thr_reasoning_redaction", "evt_reasoning_redaction", 1, "public", true)

	reader := newPostgreSQLEventReader(runtime)
	listBefore, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_reasoning_redaction", eventstream.ListOptions{Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("list before stable reasoning ledger: %v", err)
	}
	streamBefore, err := reader.ListSessionEventChanges(context.Background(), workspace.DefaultID, "sesn_reasoning_redaction", 0, 10)
	if err != nil {
		t.Fatalf("stream before stable reasoning ledger: %v", err)
	}
	listBeforeJSON, err := json.Marshal(listBefore.Data)
	if err != nil {
		t.Fatalf("marshal list before stable reasoning ledger: %v", err)
	}
	streamBeforeJSON, err := json.Marshal(streamBefore)
	if err != nil {
		t.Fatalf("marshal stream before stable reasoning ledger: %v", err)
	}

	const privateReasoning = "private-ledger-reasoning"
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET stable_reasoning_json = $1
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_reasoning_redaction'
		    AND event_id = 'evt_reasoning_redaction'`,
		`[{"reasoning_part_id":"reason_redaction","provider_part_id":"","part_sequence":0,"text":"`+privateReasoning+`","metadata":{},"truncated":false}]`,
	); err != nil {
		t.Fatalf("seed stable reasoning ledger: %v", err)
	}

	listAfter, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_reasoning_redaction", eventstream.ListOptions{Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("list after stable reasoning ledger: %v", err)
	}
	streamAfter, err := reader.ListSessionEventChanges(context.Background(), workspace.DefaultID, "sesn_reasoning_redaction", 0, 10)
	if err != nil {
		t.Fatalf("stream after stable reasoning ledger: %v", err)
	}
	listAfterJSON, err := json.Marshal(listAfter.Data)
	if err != nil {
		t.Fatalf("marshal list after stable reasoning ledger: %v", err)
	}
	streamAfterJSON, err := json.Marshal(streamAfter)
	if err != nil {
		t.Fatalf("marshal stream after stable reasoning ledger: %v", err)
	}
	if !bytes.Equal(listBeforeJSON, listAfterJSON) || !bytes.Equal(streamBeforeJSON, streamAfterJSON) {
		t.Fatalf("public event bytes changed after internal ledger write: list %s -> %s stream %s -> %s", listBeforeJSON, listAfterJSON, streamBeforeJSON, streamAfterJSON)
	}
	if bytes.Contains(listAfterJSON, []byte(privateReasoning)) || bytes.Contains(streamAfterJSON, []byte(privateReasoning)) {
		t.Fatalf("private stable reasoning leaked: list=%s stream=%s", listAfterJSON, streamAfterJSON)
	}
}

func TestPostgreSQLReaderSessionListUsesInsertPositionForCrossThreadOrdering(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_insert_order", "thr_zmain")
	seedEventStreamSubagentThread(t, admin, "default", "sesn_insert_order", "thr_zmain", "thr_achild")
	seedEventStreamEvent(t, admin, "default", "sesn_insert_order", "thr_zmain", "evt_spawn", 1, "user.message", `{"text":"spawn"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_insert_order", "thr_achild", "evt_child", 1, "agent.message", `{"text":"child"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_insert_order", "thr_zmain", "evt_after", 2, "agent.message", `{"text":"after"}`, "public", true, "")
	seedEventStreamChange(t, admin, "default", "sesn_insert_order", "thr_zmain", "evt_spawn", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_insert_order", "thr_achild", "evt_child", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_insert_order", "thr_zmain", "evt_after", 1, "public", true)

	reader := newPostgreSQLEventReader(runtime)
	result, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_insert_order", eventstream.ListOptions{Limit: 10, Order: "asc"})
	if err != nil {
		t.Fatalf("ListSessionEvents: %v", err)
	}
	if ids := eventIDs(result.Data); !equalStrings(ids, []string{"evt_spawn", "evt_child", "evt_after"}) {
		t.Fatalf("session event order = %v; want insert-stream order across threads", ids)
	}
}

func TestPostgreSQLReaderSessionListPaginationStableAcrossRevisionBump(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_page_stable", "thr_page")
	seedEventStreamEvent(t, admin, "default", "sesn_page_stable", "thr_page", "evt_first", 1, "user.message", `{"text":"first"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_page_stable", "thr_page", "evt_second", 2, "agent.message", `{"text":"second"}`, "public", true, "")
	seedEventStreamChange(t, admin, "default", "sesn_page_stable", "thr_page", "evt_first", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_page_stable", "thr_page", "evt_second", 1, "public", true)

	reader := newPostgreSQLEventReader(runtime)
	first, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_page_stable", eventstream.ListOptions{Limit: 1, Order: "asc"})
	if err != nil {
		t.Fatalf("ListSessionEvents first page: %v", err)
	}
	if first.NextPage == nil || len(first.Data) != 1 || first.Data[0].ID != "evt_first" {
		t.Fatalf("first page = %+v next=%v", first.Data, first.NextPage)
	}
	seedEventStreamChange(t, admin, "default", "sesn_page_stable", "thr_page", "evt_first", 2, "public", true)
	second, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_page_stable", eventstream.ListOptions{Limit: 10, Order: "asc", Page: *first.NextPage})
	if err != nil {
		t.Fatalf("ListSessionEvents second page: %v", err)
	}
	if ids := eventIDs(second.Data); !equalStrings(ids, []string{"evt_second"}) {
		t.Fatalf("second page = %v; want no duplicate after revision bump", ids)
	}
}

func TestPostgreSQLReaderRejectsOldSessionListCursorVersion(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_old_cursor", "thr_old_cursor")
	oldCursor := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"r":"session_events","w":"default","s":"sesn_old_cursor","o":"asc","seq":1,"e":"evt_old"}`))

	reader := newPostgreSQLEventReader(runtime)
	if _, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_old_cursor", eventstream.ListOptions{Limit: 10, Order: "asc", Page: oldCursor}); err == nil {
		t.Fatal("ListSessionEvents accepted old page token version; want validation error")
	}
}

func TestPostgreSQLReaderRejectsTamperedAndWrongScopePageTokens(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_token_scope", "thr_token_scope")
	seedEventStreamEvent(t, admin, "default", "sesn_token_scope", "thr_token_scope", "evt_token_first", 1, "user.message", `{"text":"first"}`, "public", true, "")
	seedEventStreamEvent(t, admin, "default", "sesn_token_scope", "thr_token_scope", "evt_token_second", 2, "agent.message", `{"text":"second"}`, "public", true, "")
	seedEventStreamChange(t, admin, "default", "sesn_token_scope", "thr_token_scope", "evt_token_first", 1, "public", true)
	seedEventStreamChange(t, admin, "default", "sesn_token_scope", "thr_token_scope", "evt_token_second", 1, "public", true)

	reader := newPostgreSQLEventReader(runtime)
	first, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_token_scope", eventstream.ListOptions{Limit: 1, Order: "asc"})
	if err != nil {
		t.Fatalf("ListSessionEvents first page: %v", err)
	}
	if first.NextPage == nil {
		t.Fatal("first page did not return next page token")
	}

	for _, test := range []struct {
		name      string
		sessionID string
		options   eventstream.ListOptions
	}{
		{name: "byte tampered", sessionID: "sesn_token_scope", options: eventstream.ListOptions{Limit: 10, Order: "asc", Page: tamperEventPageToken(*first.NextPage)}},
		{name: "wrong session scope", sessionID: "sesn_token_other", options: eventstream.ListOptions{Limit: 10, Order: "asc", Page: *first.NextPage}},
		{name: "changed filters", sessionID: "sesn_token_scope", options: eventstream.ListOptions{Limit: 10, Order: "asc", Page: *first.NextPage, Types: []string{"agent.message"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, test.sessionID, test.options); err == nil {
				t.Fatal("ListSessionEvents accepted invalid page token; want validation error")
			}
		})
	}
}

func TestPostgreSQLReaderSessionListFiltersByTypeAndCreatedAt(t *testing.T) {
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedEventStreamSession(t, admin, "default", "sesn_filters", "thr_filters")
	seedEventStreamEventAt(t, admin, "default", "sesn_filters", "thr_filters", "evt_old_user", 1, "user.message", `{"text":"old"}`, "public", true, "2026-01-01T00:00:00.000000000Z", "")
	seedEventStreamEventAt(t, admin, "default", "sesn_filters", "thr_filters", "evt_mid_agent", 2, "agent.message", `{"text":"mid"}`, "public", true, "2026-01-01T00:00:01.000000000Z", "")
	seedEventStreamEventAt(t, admin, "default", "sesn_filters", "thr_filters", "evt_new_user", 3, "user.message", `{"text":"new"}`, "public", true, "2026-01-01T00:00:02.000000000Z", "")
	for _, eventID := range []string{"evt_old_user", "evt_mid_agent", "evt_new_user"} {
		seedEventStreamChange(t, admin, "default", "sesn_filters", "thr_filters", eventID, 1, "public", true)
	}

	reader := newPostgreSQLEventReader(runtime)
	for _, test := range []struct {
		name    string
		options eventstream.ListOptions
		want    []string
	}{
		{name: "types", options: eventstream.ListOptions{Limit: 10, Order: "asc", Types: []string{"user.message"}}, want: []string{"evt_old_user", "evt_new_user"}},
		{name: "gt", options: eventstream.ListOptions{Limit: 10, Order: "asc", CreatedAtGT: "2026-01-01T00:00:00Z"}, want: []string{"evt_mid_agent", "evt_new_user"}},
		{name: "gte", options: eventstream.ListOptions{Limit: 10, Order: "asc", CreatedAtGTE: "2026-01-01T00:00:01Z"}, want: []string{"evt_mid_agent", "evt_new_user"}},
		{name: "lt", options: eventstream.ListOptions{Limit: 10, Order: "asc", CreatedAtLT: "2026-01-01T00:00:02Z"}, want: []string{"evt_old_user", "evt_mid_agent"}},
		{name: "lte", options: eventstream.ListOptions{Limit: 10, Order: "asc", CreatedAtLTE: "2026-01-01T00:00:01Z"}, want: []string{"evt_old_user", "evt_mid_agent"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := reader.ListSessionEvents(context.Background(), workspace.DefaultID, "sesn_filters", test.options)
			if err != nil {
				t.Fatalf("ListSessionEvents: %v", err)
			}
			if ids := eventIDs(result.Data); !equalStrings(ids, test.want) {
				t.Fatalf("events = %v; want %v", ids, test.want)
			}
		})
	}

	t.Run("thread filters", func(t *testing.T) {
		result, err := reader.ListThreadEvents(context.Background(), workspace.DefaultID, "sesn_filters", "thr_filters", eventstream.ListOptions{
			Limit:        10,
			Order:        "asc",
			Types:        []string{"user.message"},
			CreatedAtGTE: "2026-01-01T00:00:02Z",
		})
		if err != nil {
			t.Fatalf("ListThreadEvents: %v", err)
		}
		if ids := eventIDs(result.Data); !equalStrings(ids, []string{"evt_new_user"}) {
			t.Fatalf("thread events = %v; want evt_new_user", ids)
		}
	})
}

func newPostgreSQLEventReader(database *sql.DB) *eventstream.PostgreSQLReader {
	return eventstream.NewPostgreSQLReader(
		dbconnect.NewClientForTesting(database),
		eventstream.WithPageTokenSecret(eventStreamPageTokenSecret),
	)
}

func tamperEventPageToken(token string) string {
	if token == "" {
		return "tampered"
	}
	last := token[len(token)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	return token[:len(token)-1] + string(replacement)
}

func TestEventStreamBoundaryLogsServerErrorsOnly(t *testing.T) {
	signer, verifier := testInternalPrincipalPair(t)
	t.Run("fast 2xx emits no boundary log", func(t *testing.T) {
		var buffer bytes.Buffer
		router := eventstream.NewListRouter(&recordingReader{}, verifier, eventstream.WithListLogger(captureLogger(&buffer)))
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events?beta=true"))
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
		}
		if buffer.String() != "" {
			t.Fatalf("fast 2xx emitted boundary log: %s", buffer.String())
		}
	})

	t.Run("missing principal is auth-only and not boundary logged", func(t *testing.T) {
		var buffer bytes.Buffer
		router := eventstream.NewListRouter(&recordingReader{}, verifier, eventstream.WithListLogger(captureLogger(&buffer)))
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/sessions/sesn_events/events", nil))
		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d; want 401", recorder.Code)
		}
		if got := countLogLines(buffer.String(), "http.request"); got != 0 {
			t.Fatalf("boundary http.request lines on 401 = %d; want 0: %s", got, buffer.String())
		}
	})

	t.Run("500 emits boundary log", func(t *testing.T) {
		var buffer bytes.Buffer
		router := eventstream.NewListRouter(nil, verifier, eventstream.WithListLogger(captureLogger(&buffer)))
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, signedRequest(t, signer, http.MethodGet, "/v1/sessions/sesn_events/events?beta=true"))
		if recorder.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d; want 500 body=%s", recorder.Code, recorder.Body.String())
		}
		if got := countLogLines(buffer.String(), "http.request"); got != 1 {
			t.Fatalf("boundary http.request lines = %d; want exactly 1: %s", got, buffer.String())
		}
	})
}

type recordingReader struct {
	listResult       ListResult
	listWorkspace    workspace.ID
	listSessionID    string
	listThreadID     string
	listOptions      ListOptions
	currentPosition  int64
	changes          []eventstream.StreamChange
	firstChangeAfter int64
	changeCalls      int
	currentCalls     int
	listCalls        int
}

type ListResult = eventstream.ListResult
type ListOptions = eventstream.ListOptions

type flushRecorder struct {
	*httptest.ResponseRecorder
}

func (*flushRecorder) Flush() {}

type liveFlushRecorder struct {
	header  http.Header
	mu      sync.Mutex
	body    bytes.Buffer
	flushes chan string
}

func newLiveFlushRecorder() *liveFlushRecorder {
	return &liveFlushRecorder{
		header:  make(http.Header),
		flushes: make(chan string, 4),
	}
}

func (r *liveFlushRecorder) Header() http.Header {
	return r.header
}

func (*liveFlushRecorder) WriteHeader(int) {}

func (r *liveFlushRecorder) Write(data []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(data)
}

func (r *liveFlushRecorder) Flush() {
	r.mu.Lock()
	body := r.body.String()
	r.mu.Unlock()
	select {
	case r.flushes <- body:
	default:
	}
}

func (r *recordingReader) ListSessionEvents(_ context.Context, ws workspace.ID, sessionID string, options eventstream.ListOptions) (eventstream.ListResult, error) {
	r.listCalls++
	r.listWorkspace = ws
	r.listSessionID = sessionID
	r.listOptions = options
	if r.listResult.Data == nil {
		r.listResult.Data = []eventstream.Event{}
	}
	return r.listResult, nil
}

func (r *recordingReader) CurrentStreamPosition(_ context.Context, _ workspace.ID, _ string) (int64, error) {
	r.currentCalls++
	return r.currentPosition, nil
}

func (r *recordingReader) ListSessionEventChanges(_ context.Context, _ workspace.ID, _ string, after int64, _ int) ([]eventstream.StreamChange, error) {
	r.changeCalls++
	if r.changeCalls == 1 {
		r.firstChangeAfter = after
	}
	var changes []eventstream.StreamChange
	for _, change := range r.changes {
		if change.StreamPosition > after {
			changes = append(changes, change)
		}
	}
	return changes, nil
}

func (r *recordingReader) ListThreadEvents(_ context.Context, ws workspace.ID, sessionID string, threadID string, options eventstream.ListOptions) (eventstream.ListResult, error) {
	r.listCalls++
	r.listWorkspace = ws
	r.listSessionID = sessionID
	r.listThreadID = threadID
	r.listOptions = options
	if r.listResult.Data == nil {
		r.listResult.Data = []eventstream.Event{}
	}
	return r.listResult, nil
}

func (r *recordingReader) CurrentThreadStreamPosition(_ context.Context, _ workspace.ID, _ string, _ string) (int64, error) {
	r.currentCalls++
	return r.currentPosition, nil
}

func (r *recordingReader) ListThreadEventChanges(_ context.Context, _ workspace.ID, _ string, _ string, after int64, _ int) ([]eventstream.StreamChange, error) {
	return r.ListSessionEventChanges(context.Background(), workspace.DefaultID, "", after, 0)
}

func testInternalPrincipalPair(t *testing.T) (*auth.InternalPrincipalSigner, *auth.InternalPrincipalVerifier) {
	t.Helper()
	privateKey, err := auth.GenerateEd25519PrivateKeyBase64()
	if err != nil {
		t.Fatalf("generate principal key: %v", err)
	}
	signer, err := auth.NewInternalPrincipalSignerFromBase64(privateKey)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(signer.PublicKeyBase64())
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	return signer, verifier
}

func signedRequest(t *testing.T, signer *auth.InternalPrincipalSigner, method string, target string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, target, nil)
	token, err := signer.Mint(auth.Principal{ //nolint:gosec // Test principal token fixture.
		Workspace: workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default"},
		APIKeyID:  "ak_event_stream_test", //nolint:gosec // Test principal id, not a secret.
	}, method, request.URL.Path, "req_event_stream_test", time.Minute)
	if err != nil {
		t.Fatalf("mint principal: %v", err)
	}
	request.Header.Set("X-Tetral-Internal-Principal", token)
	return request
}

func captureLogger(buffer *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buffer, nil)).With(slog.String("service.name", "event-stream"))
}

func countLogLines(output string, message string) int {
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if line == "" {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			continue
		}
		if fields["msg"] == message {
			count++
		}
	}
	return count
}

func assertErrorEnvelope(t *testing.T, recorder *httptest.ResponseRecorder, wantType string, wantRequestID bool) {
	t.Helper()
	var response struct {
		Type  string `json:"type"`
		Error struct {
			Type string `json:"type"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v body=%s", err, recorder.Body.String())
	}
	if response.Type != "error" || response.Error.Type != wantType {
		t.Fatalf("error envelope = %+v; want type=%s body=%s", response, wantType, recorder.Body.String())
	}
	if wantRequestID && response.RequestID == "" {
		t.Fatalf("request_id is empty in body=%s", recorder.Body.String())
	}
}

func seedEventStreamSession(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string) {
	t.Helper()
	agentID := "agent_" + sessionID
	agentVersionID := "agv_" + sessionID
	environmentID := "env_" + sessionID
	now := "2026-01-01T00:00:00Z"
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO workspaces (id, type, name, created_at) VALUES ($1, 'workspace', $1, $2) ON CONFLICT (id) DO NOTHING`, []any{workspaceID, now}},
		{`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at) VALUES ($1, $2, $2, 1, $3, $3)`, []any{workspaceID, agentID, now}},
		{`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at) VALUES ($1, $2, $3, 1, '{}', $4, $5)`, []any{workspaceID, agentVersionID, agentID, "hash_" + sessionID, now}},
		{`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at) VALUES ($1, $2, $2, '{}', $3, $3)`, []any{workspaceID, environmentID, now}},
		{`INSERT INTO sessions (workspace_id, id, main_thread_id, type, status, lifecycle_state, agent_id, agent_version, environment_id, created_at, updated_at) VALUES ($1, $2, $3, 'session', 'idle', 'active', $4, 1, $5, $6, $6)`, []any{workspaceID, sessionID, threadID, agentID, environmentID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at) VALUES ($1, $2, $3, 'main', 'public', 'idle', $4, $4, $4)`, []any{workspaceID, threadID, sessionID, now}},
		{`INSERT INTO session_threads (workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name, created_at, last_active_at, updated_at) VALUES ($1, 'thr_child', $2, $3, 'subagent', 'public', 'idle', 'child', $4, $4, $4)`, []any{workspaceID, sessionID, threadID, now}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement.query, statement.args...); err != nil {
			t.Fatalf("seed session statement %q: %v", statement.query, err)
		}
	}
}

func seedEventStreamSubagentThread(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, threadID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'subagent', 'public', 'idle', $2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		threadID,
		sessionID,
		parentThreadID,
	); err != nil {
		t.Fatalf("seed subagent thread %s: %v", threadID, err)
	}
}

func seedEventStreamEvent(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64, eventType string, payloadJSON string, visibility string, sessionVisible bool, processedAt string) {
	seedEventStreamEventAt(t, db, workspaceID, sessionID, threadID, eventID, sequence, eventType, payloadJSON, visibility, sessionVisible, "2026-01-01T00:00:00.000000000Z", processedAt)
}

func seedEventStreamEventAt(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, sequence int64, eventType string, payloadJSON string, visibility string, sessionVisible bool, createdAt string, processedAt string) {
	t.Helper()
	var processed any
	if processedAt != "" {
		processed = processedAt
	}
	var thread any = threadID
	if threadID == "" {
		thread = nil
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11)`,
		workspaceID, sessionID, thread, eventID, sequence, eventType, payloadJSON, visibility, sessionVisible, createdAt, processed); err != nil {
		t.Fatalf("seed event %s: %v", eventID, err)
	}
}

func seedEventStreamChange(t *testing.T, db *sql.DB, workspaceID string, sessionID string, threadID string, eventID string, revision int64, visibility string, sessionVisible bool) {
	t.Helper()
	var thread any = threadID
	if threadID == "" {
		thread = nil
	}
	var streamPosition int64
	if err := db.QueryRowContext(context.Background(),
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, '2026-01-01T00:00:00Z')
		RETURNING stream_position`,
		workspaceID, sessionID, eventID, thread, revision, visibility, sessionVisible).Scan(&streamPosition); err != nil {
		t.Fatalf("seed stream change %s: %v", eventID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_events
		    SET latest_stream_position = $4,
		        insert_stream_position = CASE WHEN $5 = 1 AND insert_stream_position = 0 THEN $4 ELSE insert_stream_position END
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		workspaceID, sessionID, eventID, streamPosition, revision); err != nil {
		t.Fatalf("backfill stream positions for %s: %v", eventID, err)
	}
}

func seedEventStreamInternalReviewer(t *testing.T, db *sql.DB, workspaceID string, sessionID string, parentThreadID string, reviewerThreadID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'approval_reviewer', 'internal', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		workspaceID,
		reviewerThreadID,
		sessionID,
		parentThreadID,
	); err != nil {
		t.Fatalf("seed internal reviewer: %v", err)
	}
}

func stringPtr(value string) *string {
	return &value
}

func eventIDs(events []eventstream.Event) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.ID)
	}
	return ids
}

func equalStrings(left []string, right []string) bool {
	return slices.Equal(left, right)
}
