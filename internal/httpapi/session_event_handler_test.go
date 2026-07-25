package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/sessionevent"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const sessionEventHTTPIdempotencyKey = "idem_http_accept_123"

func TestSessionEventIngressRouteRequiresAPIKeyAndUsesAuthenticatedWorkspace(t *testing.T) {
	service := &recordingSessionEventHTTPService{
		result: testSessionEventHTTPResult("sevt_http_accept", "sesn_http_accept"),
	}
	router := newSessionEventHTTPRouter(service)

	noKey := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_accept/events", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`))
	noKeyRecorder := httptest.NewRecorder()
	router.ServeHTTP(noKeyRecorder, noKey)
	if noKeyRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing key status = %d; want 401 body=%s", noKeyRecorder.Code, noKeyRecorder.Body.String())
	}
	if len(service.calls) != 0 {
		t.Fatalf("service calls without api key = %d; want 0", len(service.calls))
	}

	invalidKey := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_accept/events", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`))
	invalidKey.Header.Set("x-api-key", "invalid-key")
	invalidKeyRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidKeyRecorder, invalidKey)
	if invalidKeyRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("invalid key status = %d; want 401 body=%s", invalidKeyRecorder.Code, invalidKeyRecorder.Body.String())
	}
	if len(service.calls) != 0 {
		t.Fatalf("service calls with invalid api key = %d; want 0", len(service.calls))
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_accept/events?beta=true", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}]}]}`))
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if len(service.calls) != 1 {
		t.Fatalf("service calls = %d; want 1", len(service.calls))
	}
	call := service.calls[0]
	if call.workspaceID != workspace.DefaultID || call.sessionID != "sesn_http_accept" {
		t.Fatalf("call = %#v; want authenticated workspace and path session", call)
	}
	if call.idempotencyKey != sessionEventHTTPIdempotencyKey {
		t.Fatalf("idempotency key = %q; want bounded header value", call.idempotencyKey)
	}
	if len(call.request.Events) != 1 || call.request.Events[0].Type != sessionevent.EventTypeUserMessage || call.request.Events[0].Content[0].Text != "hello" {
		t.Fatalf("append request = %#v; want user.message text", call.request)
	}
	assertPublicSessionEventResponse(t, recorder.Body.String(), []publicSessionEventExpectation{{
		ID:        "sevt_http_accept",
		Type:      sessionevent.EventTypeUserMessage,
		SessionID: "sesn_http_accept",
		Payload:   `{"content":[{"type":"text","text":"hello"}]}`,
	}})
}

func TestSessionEventIngressReturnsAnthropicPublicProjection(t *testing.T) {
	service := &recordingSessionEventHTTPService{
		result: &sessionevent.AppendResult{
			Data: []*sessionevent.Event{
				{
					ID:          "sevt_http_message",
					WorkspaceID: workspace.DefaultID,
					SessionID:   "sesn_http_projection",
					ThreadID:    "thread_http_projection",
					Sequence:    5,
					Type:        sessionevent.EventTypeUserMessage,
					Payload:     json.RawMessage(`{"content":[{"type":"text","text":"hello"},{"type":"text","text":"again"}]}`),
					CreatedAt:   time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
				},
				{
					ID:          "sevt_http_interrupt",
					WorkspaceID: workspace.DefaultID,
					SessionID:   "sesn_http_projection",
					ThreadID:    "thread_http_projection",
					Sequence:    6,
					Type:        sessionevent.EventTypeUserInterrupt,
					Payload:     json.RawMessage(`{}`),
					CreatedAt:   time.Date(2026, 5, 31, 10, 0, 1, 0, time.UTC),
				},
			},
		},
	}
	router := newSessionEventHTTPRouter(service)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_projection/events?beta=true", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"},{"type":"text","text":"again"}]},{"type":"user.interrupt"}]}`))
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	assertPublicSessionEventResponse(t, recorder.Body.String(), []publicSessionEventExpectation{
		{
			ID:        "sevt_http_message",
			Type:      sessionevent.EventTypeUserMessage,
			SessionID: "sesn_http_projection",
			ThreadID:  "thread_http_projection",
			Payload:   `{"content":[{"type":"text","text":"hello"},{"type":"text","text":"again"}]}`,
		},
		{
			ID:        "sevt_http_interrupt",
			Type:      sessionevent.EventTypeUserInterrupt,
			SessionID: "sesn_http_projection",
			ThreadID:  "thread_http_projection",
			Payload:   `{}`,
		},
	})
}

func TestSessionEventIngressProjectsAllPublicChildEventVariants(t *testing.T) {
	processedAt := time.Date(2026, 7, 14, 12, 34, 56, 0, time.UTC)
	content := []any{
		map[string]any{"type": "text", "text": "first child block"},
		map[string]any{"type": "text", "text": "second child block"},
	}
	fixtures := []struct {
		id        string
		eventType string
		payload   string
		want      map[string]any
	}{
		{
			id:        "sevt_http_child_sent",
			eventType: "agent.thread_message_sent",
			payload:   `{"type":"wrong","id":"payload_id","processed_at":"wrong","delivery_id":"delivery_sent","source_thread_id":"sthr_sender_distinct","target_thread_id":"sthr_target_distinct","target_task_name":"callable_target","source_tool_use_event_id":"sevt_tool_sent","message":{"content":[{"type":"text","text":"first child block"},{"type":"text","text":"second child block"}]},"agent_type":"research","task_name":"wrong_alias","partition_key":"internal"}`,
			want:      map[string]any{"content": content, "to_session_thread_id": "sthr_target_distinct", "to_agent_name": "callable_target"},
		},
		{
			id:        "sevt_http_child_received",
			eventType: "agent.thread_message_received",
			payload:   `{"delivery_id":"delivery_received","source_thread_id":"sthr_source_distinct","source_task_name":"callable_source","source_tool_use_event_id":"sevt_tool_received","message":{"content":[{"type":"text","text":"first child block"},{"type":"text","text":"second child block"}]},"agent_type":"general","task_name":"wrong_alias","queue_job_id":"internal"}`,
			want:      map[string]any{"content": content, "from_session_thread_id": "sthr_source_distinct", "from_agent_name": "callable_source"},
		},
		{
			id:        "sevt_http_child_created",
			eventType: "session.thread_created",
			payload:   `{"session_thread_id":"sthr_created_distinct","parent_thread_id":"sthr_parent_distinct","role":"subagent","visibility":"public","task_name":"callable_created","agent_type":"worker"}`,
			want:      map[string]any{"session_thread_id": "sthr_created_distinct", "agent_name": "callable_created"},
		},
		{
			id:        "sevt_http_child_running",
			eventType: "session.thread_status_running",
			payload:   `{"session_thread_id":"sthr_running_distinct","task_name":"callable_running","agent_type":"general"}`,
			want:      map[string]any{"session_thread_id": "sthr_running_distinct", "agent_name": "callable_running"},
		},
		{
			id:        "sevt_http_child_idle",
			eventType: "session.thread_status_idle",
			payload:   `{"session_thread_id":"sthr_idle_distinct","task_name":"callable_idle","agent_type":"research","stop_reason":{"type":"requires_action","event_ids":["sevt_wait_second","sevt_wait_first"]}}`,
			want:      map[string]any{"session_thread_id": "sthr_idle_distinct", "agent_name": "callable_idle", "stop_reason": map[string]any{"type": "requires_action", "event_ids": []any{"sevt_wait_second", "sevt_wait_first"}}},
		},
		{
			id:        "sevt_http_child_rescheduled",
			eventType: "session.thread_status_rescheduled",
			payload:   `{"session_thread_id":"sthr_rescheduled_distinct","task_name":"callable_rescheduled","agent_type":"worker"}`,
			want:      map[string]any{"session_thread_id": "sthr_rescheduled_distinct", "agent_name": "callable_rescheduled"},
		},
		{
			id:        "sevt_http_child_terminated",
			eventType: "session.thread_status_terminated",
			payload:   `{"session_thread_id":"sthr_terminated_distinct","task_name":"callable_terminated","agent_type":"general"}`,
			want:      map[string]any{"session_thread_id": "sthr_terminated_distinct", "agent_name": "callable_terminated"},
		},
	}
	result := &sessionevent.AppendResult{Data: make([]*sessionevent.Event, 0, len(fixtures))}
	for _, fixture := range fixtures {
		result.Data = append(result.Data, &sessionevent.Event{
			ID:          fixture.id,
			WorkspaceID: workspace.DefaultID,
			SessionID:   "sesn_http_child_projection_internal",
			ThreadID:    "sthr_http_child_projection_internal",
			Type:        fixture.eventType,
			Payload:     json.RawMessage(fixture.payload),
			ProcessedAt: &processedAt,
		})
		fixture.want["id"] = fixture.id
		fixture.want["type"] = fixture.eventType
		fixture.want["processed_at"] = "2026-07-14T12:34:56Z"
	}
	service := &recordingSessionEventHTTPService{result: result}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/sessions/sesn_http_child_projection/events?beta=true",
		strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"trigger projection"}]}]}`),
	)
	request.Header.Set("x-api-key", testAPIKey)
	recorder := httptest.NewRecorder()
	newSessionEventHTTPRouter(service).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, recorder.Body.String())
	}
	if len(response.Data) != len(fixtures) {
		t.Fatalf("response events = %d; want %d body=%s", len(response.Data), len(fixtures), recorder.Body.String())
	}
	for index, event := range response.Data {
		if !reflect.DeepEqual(event, fixtures[index].want) {
			t.Fatalf("public child event %d = %#v; want exact %#v", index, event, fixtures[index].want)
		}
	}
}

func TestSessionEventIngressPublicProjectionUsesDurableEventRow(t *testing.T) {
	processedAt := time.Date(2026, 5, 31, 10, 0, 2, 0, time.UTC)
	service := &recordingSessionEventHTTPService{
		result: &sessionevent.AppendResult{
			Data: []*sessionevent.Event{{
				ID:          "sevt_http_internal_payload_ignored",
				WorkspaceID: workspace.DefaultID,
				SessionID:   "sesn_http_internal_payload_ignored",
				ThreadID:    "thread_http_internal_payload_ignored",
				Sequence:    42,
				Type:        sessionevent.EventTypeUserMessage,
				Payload:     json.RawMessage(`{"content":[{"type":"text","text":"from stored row"}]}`),
				CreatedAt:   time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
				ProcessedAt: &processedAt,
			}},
		},
	}
	router := newSessionEventHTTPRouter(service)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_internal_payload_ignored/events?beta=true", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"from request"}]}]}`))
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	assertPublicSessionEventResponse(t, recorder.Body.String(), []publicSessionEventExpectation{{
		ID:          "sevt_http_internal_payload_ignored",
		Type:        sessionevent.EventTypeUserMessage,
		SessionID:   "sesn_http_internal_payload_ignored",
		ThreadID:    "thread_http_internal_payload_ignored",
		Payload:     `{"content":[{"type":"text","text":"from stored row"}]}`,
		ProcessedAt: "2026-05-31T10:00:02Z",
	}})
}

func TestSessionEventIngressAcceptsUserInterrupt(t *testing.T) {
	service := &recordingSessionEventHTTPService{
		result: &sessionevent.AppendResult{
			Data: []*sessionevent.Event{{
				ID:          "sevt_http_interrupt_accept",
				WorkspaceID: workspace.DefaultID,
				SessionID:   "sesn_http_interrupt_accept",
				ThreadID:    "thread_http_interrupt_accept",
				Sequence:    7,
				Type:        sessionevent.EventTypeUserInterrupt,
				Payload:     json.RawMessage(`{}`),
				CreatedAt:   time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
			}},
		},
	}
	router := newSessionEventHTTPRouter(service)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_interrupt_accept/events?beta=true", strings.NewReader(`{"events":[{"type":"user.interrupt","session_thread_id":"thread_http_interrupt_accept"}]}`))
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d; want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if len(service.calls) != 1 ||
		service.calls[0].request.Events[0].Type != sessionevent.EventTypeUserInterrupt ||
		service.calls[0].request.Events[0].SessionThreadID != "thread_http_interrupt_accept" {
		t.Fatalf("service call = %#v; want one user.interrupt append", service.calls)
	}
	assertPublicSessionEventResponse(t, recorder.Body.String(), []publicSessionEventExpectation{{
		ID:        "sevt_http_interrupt_accept",
		Type:      sessionevent.EventTypeUserInterrupt,
		SessionID: "sesn_http_interrupt_accept",
		ThreadID:  "thread_http_interrupt_accept",
		Payload:   `{}`,
	}})
}

func TestSessionEventIngressRejectsRemovedTypeAndInterruptContent(t *testing.T) {
	// The removed legacy interrupt type is now an unknown event type and is
	// rejected through the unknown-type path. The acceptance proof keeps the old
	// token name out of production code while pinning the rejection behavior:
	// only the supported user input union is accepted.
	for _, testCase := range []struct {
		name string
		body string
	}{
		{name: "removed legacy interrupt type", body: `{"events":[{"type":"user.cancel"}]}`},
		{name: "interrupt with content", body: `{"events":[{"type":"user.interrupt","content":[{"type":"text","text":"stop"}]}]}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := &recordingSessionEventHTTPService{}
			router := newSessionEventHTTPRouter(service)
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_reject/events?beta=true", strings.NewReader(testCase.body))
			request.Header.Set("x-api-key", testAPIKey)
			request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if len(service.calls) != 0 {
				t.Fatalf("service calls = %d; want 0", len(service.calls))
			}
		})
	}
}

func TestSessionEventIngressStrictBodyRejectsBeforeService(t *testing.T) {
	tooManyEvents := make([]string, sessionevent.MaxEventsPerRequest+1)
	for index := range tooManyEvents {
		tooManyEvents[index] = `{"type":"user.interrupt"}`
	}
	for _, testCase := range []struct {
		name        string
		body        string
		contentType string
		want        string
	}{
		{name: "malformed json", body: `{"events":[`},
		{name: "trailing json", body: `{"events":[{"type":"user.interrupt"}]} {}`},
		{name: "non object envelope", body: `[]`},
		{name: "unknown envelope field", body: `{"events":[{"type":"user.interrupt"}],"workspace_id":"default"}`},
		{name: "missing events", body: `{}`},
		{name: "empty events", body: `{"events":[]}`},
		{name: "too many events", body: `{"events":[` + strings.Join(tooManyEvents, ",") + `]}`},
		{name: "unknown event field", body: `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello"}],"scheduler":{"dirty":true}}]}`},
		{name: "missing event type", body: `{"events":[{"content":[{"type":"text","text":"hello"}]}]}`},
		{name: "unsupported event type", body: `{"events":[{"type":"user.cancel"}]}`},
		{name: "unsupported custom tool result", body: `{"events":[{"type":"user.custom_tool_result","custom_tool_use_id":"sevt_custom","content":[]}]}`, want: "custom tools are not supported in this stage"},
		{name: "unsupported tool result", body: `{"events":[{"type":"user.tool_result","tool_use_id":"sevt_tool","content":[]}]}`},
		{name: "unsupported define outcome", body: `{"events":[{"type":"user.define_outcome","outcome":"done"}]}`},
		{name: "unsupported system message", body: `{"events":[{"type":"system.message","content":[{"type":"text","text":"internal"}]}]}`},
		{name: "tool confirmation missing tool use", body: `{"events":[{"type":"user.tool_confirmation","result":"allow"}]}`},
		{name: "tool confirmation unknown result", body: `{"events":[{"type":"user.tool_confirmation","tool_use_id":"sevt_tool","result":"maybe"}]}`},
		{name: "allow with deny message", body: `{"events":[{"type":"user.tool_confirmation","tool_use_id":"sevt_tool","result":"allow","deny_message":"no"}]}`},
		{name: "mock run rejected", body: `{"type":"mock_run","input":{}}`},
		{name: "message missing content", body: `{"events":[{"type":"user.message"}]}`},
		{name: "message empty content", body: `{"events":[{"type":"user.message","content":[]}]}`},
		{name: "content not array", body: `{"events":[{"type":"user.message","content":"hello"}]}`},
		{name: "content missing text", body: `{"events":[{"type":"user.message","content":[{"type":"text"}]}]}`},
		{name: "content non string text", body: `{"events":[{"type":"user.message","content":[{"type":"text","text":123}]}]}`},
		{name: "unknown content field", body: `{"events":[{"type":"user.message","content":[{"type":"text","text":"hello","file_id":"file_abc"}]}]}`},
		{name: "file block rejected", body: `{"events":[{"type":"user.message","content":[{"type":"file","file_id":"file_abc"}]}]}`},
		{name: "image block rejected", body: `{"events":[{"type":"user.message","content":[{"type":"image","url":"https://example.test/image.png"}]}]}`},
		{name: "document block rejected", body: `{"events":[{"type":"user.message","content":[{"type":"document","url":"https://example.test/doc.pdf"}]}]}`},
		{name: "interrupt blank thread rejected", body: `{"events":[{"type":"user.interrupt","session_thread_id":"  "}]}`},
		{name: "interrupt extra field rejected", body: `{"events":[{"type":"user.interrupt","reason":"stop"}]}`},
		{name: "raw bytes rejected", body: strings.Repeat("x", 32)},
		{name: "multipart rejected", body: "--boundary\r\ncontent\r\n", contentType: "multipart/form-data; boundary=boundary"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := &recordingSessionEventHTTPService{}
			router := newSessionEventHTTPRouter(service)
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_strict/events?beta=true", strings.NewReader(testCase.body))
			request.Header.Set("x-api-key", testAPIKey)
			request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
			if testCase.contentType != "" {
				request.Header.Set("Content-Type", testCase.contentType)
			}
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			if testCase.want != "" && !strings.Contains(recorder.Body.String(), testCase.want) {
				t.Fatalf("body=%s; want message %q", recorder.Body.String(), testCase.want)
			}
			if len(service.calls) != 0 {
				t.Fatalf("service calls = %d; want 0", len(service.calls))
			}
		})
	}
}

func TestSessionEventIngressOversizedBodyRejectsBeforeService(t *testing.T) {
	service := &recordingSessionEventHTTPService{}
	router := newSessionEventHTTPRouter(service)
	body := `{"events":[{"type":"user.message","content":[{"type":"text","text":"` + strings.Repeat("x", 1<<20) + `"}]}]}`
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_big/events?beta=true", strings.NewReader(body))
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d; want 413 body=%s", recorder.Code, recorder.Body.String())
	}
	assertErrorType(t, recorder, "invalid_request_error")
	if len(service.calls) != 0 {
		t.Fatalf("service calls = %d; want 0", len(service.calls))
	}
}

func TestSessionEventIngressUsesConfiguredBodyCapAndBatchLimit(t *testing.T) {
	service := &recordingSessionEventHTTPService{
		result: testSessionEventHTTPResult("sevt_http_configured", "sesn_http_configured"),
	}
	authenticator := auth.AuthenticatorFunc(func(_ context.Context, rawKey string) (auth.Principal, error) {
		if rawKey != testAPIKey {
			return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
		}
		return auth.Principal{Workspace: workspace.Workspace{ID: workspace.DefaultID}}, nil
	})
	handler := httpapi.NewSessionEventHandler(
		service,
		httpapi.WithSessionEventBodyByteCap(128),
		httpapi.WithSessionEventLimits(sessionevent.Limits{
			MaxEventsPerRequest: 1,
		}),
	)
	router := httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithSessionEventHandler(handler))

	tooLarge := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_configured/events?beta=true", strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"`+strings.Repeat("x", 120)+`"}]}]}`))
	tooLarge.Header.Set("x-api-key", testAPIKey)
	tooLarge.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	tooLargeRecorder := httptest.NewRecorder()
	router.ServeHTTP(tooLargeRecorder, tooLarge)
	if tooLargeRecorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("too-large status = %d body=%s; want 413", tooLargeRecorder.Code, tooLargeRecorder.Body.String())
	}
	assertErrorType(t, tooLargeRecorder, "invalid_request_error")
	if len(service.calls) != 0 {
		t.Fatalf("service calls after configured body cap rejection = %d; want 0", len(service.calls))
	}

	tooMany := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_http_configured/events?beta=true", strings.NewReader(`{"events":[{"type":"user.interrupt"},{"type":"user.interrupt"}]}`))
	tooMany.Header.Set("x-api-key", testAPIKey)
	tooMany.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
	tooManyRecorder := httptest.NewRecorder()
	router.ServeHTTP(tooManyRecorder, tooMany)
	if tooManyRecorder.Code != http.StatusBadRequest {
		t.Fatalf("too-many status = %d body=%s; want 400", tooManyRecorder.Code, tooManyRecorder.Body.String())
	}
	if len(service.calls) != 0 {
		t.Fatalf("service calls after configured batch rejection = %d; want 0", len(service.calls))
	}
}

func TestSessionEventIngressAllowsMissingIdempotencyKey(t *testing.T) {
	service := &recordingSessionEventHTTPService{
		result: testSessionEventHTTPResult("sevt_http_generated_idem", "sesn_generated_idem"),
	}
	router := newSessionEventHTTPRouter(service)
	request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_generated_idem/events?beta=true", strings.NewReader(`{"events":[{"type":"user.interrupt"}]}`))
	request.Header.Set("x-api-key", testAPIKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s; want 200 for SDK body without Idempotency-Key", recorder.Code, recorder.Body.String())
	}
	if len(service.calls) != 1 {
		t.Fatalf("service calls = %d; want 1", len(service.calls))
	}
	if !strings.HasPrefix(service.calls[0].idempotencyKey, "idem_") || service.calls[0].idempotencyKey == sessionEventHTTPIdempotencyKey {
		t.Fatalf("generated idempotency key = %q; want internal idem_ key", service.calls[0].idempotencyKey)
	}
}

func TestSessionEventIngressRejectsMalformedIdempotencyKeyBeforeService(t *testing.T) {
	for _, testCase := range []struct {
		name string
		key  string
	}{
		{name: "blank", key: " \t "},
		{name: "too long", key: strings.Repeat("x", 256)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := &recordingSessionEventHTTPService{}
			router := newSessionEventHTTPRouter(service)
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_idempotency/events?beta=true", strings.NewReader(`{"events":[{"type":"user.interrupt"}]}`))
			request.Header.Set("x-api-key", testAPIKey)
			request.Header.Set("Idempotency-Key", testCase.key)
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 body=%s", recorder.Code, recorder.Body.String())
			}
			assertErrorType(t, recorder, "invalid_request_error")
			assertErrorRequestID(t, recorder)
			if testCase.key != "" && strings.Contains(recorder.Body.String(), testCase.key) {
				t.Fatalf("response leaked raw idempotency key: %s", recorder.Body.String())
			}
			if len(service.calls) != 0 {
				t.Fatalf("service calls = %d; want 0", len(service.calls))
			}
		})
	}
}

func TestSessionEventIngressErrorMapping(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		err         error
		wantStatus  int
		wantType    string
		wantMessage string
	}{
		{name: "validation", err: &sessionevent.ValidationError{Message: "events is required"}, wantStatus: http.StatusBadRequest, wantType: "invalid_request_error"},
		{name: "not found", err: &sessionevent.NotFoundError{Message: "session not found"}, wantStatus: http.StatusNotFound, wantType: "not_found_error"},
		{name: "conflict", err: &sessionevent.ConflictError{Message: "session is archived"}, wantStatus: http.StatusConflict, wantType: "invalid_request_error"},
		{name: "expired tool confirmation", err: &sessionevent.ConflictError{Message: "pending approval expired"}, wantStatus: http.StatusConflict, wantType: "invalid_request_error", wantMessage: "pending approval expired"},
		{name: "non pending tool confirmation", err: &sessionevent.ConflictError{Message: "pending approval is not pending"}, wantStatus: http.StatusConflict, wantType: "invalid_request_error", wantMessage: "pending approval is not pending"},
		{name: "internal", err: errors.New("database raw prompt runtime token"), wantStatus: http.StatusInternalServerError, wantType: "api_error"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			service := &recordingSessionEventHTTPService{err: testCase.err}
			router := newSessionEventHTTPRouter(service)
			request := httptest.NewRequest(http.MethodPost, "/v1/sessions/sesn_error/events?beta=true", strings.NewReader(`{"events":[{"type":"user.interrupt"}]}`))
			request.Header.Set("x-api-key", testAPIKey)
			request.Header.Set("Idempotency-Key", sessionEventHTTPIdempotencyKey)
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != testCase.wantStatus {
				t.Fatalf("status = %d; want %d body=%s", recorder.Code, testCase.wantStatus, recorder.Body.String())
			}
			assertErrorType(t, recorder, testCase.wantType)
			if testCase.wantMessage != "" && !strings.Contains(recorder.Body.String(), testCase.wantMessage) {
				t.Fatalf("body = %s; want message %q", recorder.Body.String(), testCase.wantMessage)
			}
			if testCase.wantType == "invalid_request_error" || testCase.wantType == "api_error" {
				assertErrorRequestID(t, recorder)
			}
			for _, forbidden := range []string{"database raw prompt", "runtime token"} {
				if strings.Contains(recorder.Body.String(), forbidden) {
					t.Fatalf("response leaked %q: %s", forbidden, recorder.Body.String())
				}
			}
		})
	}
}

func TestSessionEventIngressDispatchFailureUsesSafeStandardErrorEnvelope(t *testing.T) {
	idempotencyKey := "idem_http_secret_raw_key_123"
	userText := "user text sentinel should not echo"
	service := &recordingSessionEventHTTPService{
		err: errors.New(`notify failed to passthrough:///10.0.0.9:9090 runtime-a pod-uid-a https://kubernetes.default.svc resourceVersion=123 {"kind":"Pod"} ` + idempotencyKey + ` ` + userText),
	}
	router := newSessionEventHTTPRouter(service)
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/sessions/sesn_dispatch_error/events?beta=true",
		strings.NewReader(`{"events":[{"type":"user.message","content":[{"type":"text","text":"`+userText+`"}]}]}`),
	)
	request.Header.Set("x-api-key", testAPIKey)
	request.Header.Set("Idempotency-Key", idempotencyKey)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500 body=%s", recorder.Code, recorder.Body.String())
	}
	var envelope struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, recorder.Body.String())
	}
	if envelope.Type != "error" || envelope.Error.Type != "api_error" || envelope.Error.Message != "internal error" || envelope.RequestID == "" {
		t.Fatalf("envelope = %+v; want standard internal error envelope", envelope)
	}
	for _, forbidden := range []string{
		idempotencyKey,
		userText,
		"10.0.0.9",
		"9090",
		"passthrough:///",
		"runtime-a",
		"pod-uid-a",
		"kubernetes.default.svc",
		"resourceVersion",
		`"kind":"Pod"`,
	} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Fatalf("response leaked %q: %s", forbidden, recorder.Body.String())
		}
	}
}

func newSessionEventHTTPRouter(service *recordingSessionEventHTTPService) http.Handler {
	authenticator := auth.AuthenticatorFunc(func(_ context.Context, rawKey string) (auth.Principal, error) {
		if rawKey != testAPIKey {
			return auth.Principal{}, &auth.AuthenticationError{Message: "invalid api key"}
		}
		return auth.Principal{
			Workspace: workspace.Workspace{ID: workspace.DefaultID, Type: "workspace", Name: "Default", CreatedAt: "2026-01-01T00:00:00Z"},
			APIKeyID:  "ak_test",
		}, nil
	})
	return httpapi.NewRouter(nil, "", httpapi.WithAuthenticator(authenticator), httpapi.WithSessionEventHandler(httpapi.NewSessionEventHandler(service)))
}

type recordingSessionEventHTTPService struct {
	calls  []sessionEventHTTPCall
	result *sessionevent.AppendResult
	err    error
}

type sessionEventHTTPCall struct {
	workspaceID    workspace.ID
	sessionID      string
	idempotencyKey string
	request        sessionevent.AppendRequest
}

func (s *recordingSessionEventHTTPService) AppendClientEvents(_ context.Context, workspaceID workspace.ID, sessionID string, idempotencyKey string, request sessionevent.AppendRequest) (*sessionevent.AppendResult, error) {
	s.calls = append(s.calls, sessionEventHTTPCall{workspaceID: workspaceID, sessionID: sessionID, idempotencyKey: idempotencyKey, request: request})
	return s.result, s.err
}

func testSessionEventHTTPResult(eventID string, sessionID string) *sessionevent.AppendResult {
	return &sessionevent.AppendResult{
		Data: []*sessionevent.Event{{
			ID:          eventID,
			WorkspaceID: workspace.DefaultID,
			SessionID:   sessionID,
			Sequence:    1,
			Type:        sessionevent.EventTypeUserMessage,
			Payload:     json.RawMessage(`{"content":[{"type":"text","text":"hello"}]}`),
			CreatedAt:   time.Date(2026, 5, 31, 10, 0, 0, 0, time.UTC),
		}},
	}
}

type publicSessionEventExpectation struct {
	ID          string
	Type        string
	SessionID   string
	ThreadID    string
	Payload     string
	ProcessedAt string
}

func assertPublicSessionEventResponse(t *testing.T, body string, want []publicSessionEventExpectation) {
	t.Helper()
	var response struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil {
		t.Fatalf("decode response: %v body=%s", err, body)
	}
	if len(response.Data) != len(want) {
		t.Fatalf("response events = %d; want %d body=%s", len(response.Data), len(want), body)
	}
	for index, event := range response.Data {
		if index >= len(want) {
			t.Fatalf("response carries %d events; want %d body=%s", len(response.Data), len(want), body)
		}
		//nolint:gosec // G602: index bound is asserted on the line above; gosec cannot follow it.
		wantEvent := want[index]
		for _, forbidden := range []string{"sequence", "source", "created_at", "runtime_input_id", "queue_job_id", "partition_key", "dedupe_key", "payload", "session_id", "thread_id"} {
			if _, ok := event[forbidden]; ok {
				t.Fatalf("event %d has internal field %q: %s", index, forbidden, body)
			}
		}
		var eventID string
		if err := json.Unmarshal(event["id"], &eventID); err != nil {
			t.Fatalf("event %d id: %v body=%s", index, err, body)
		}
		var eventType string
		if err := json.Unmarshal(event["type"], &eventType); err != nil {
			t.Fatalf("event %d type: %v body=%s", index, err, body)
		}
		if eventID != wantEvent.ID || eventType != wantEvent.Type {
			t.Fatalf("event %d id/type = %q/%q; want %q/%q body=%s", index, eventID, eventType, wantEvent.ID, wantEvent.Type, body)
		}
		var payloadFields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(wantEvent.Payload), &payloadFields); err != nil {
			t.Fatalf("decode expected payload: %v", err)
		}
		for field, expected := range payloadFields {
			actual, exists := event[field]
			if !exists {
				t.Fatalf("event %d missing flattened field %q body=%s", index, field, body)
			}
			assertJSONEqualHTTP(t, actual, string(expected))
		}
		rawProcessedAt, hasProcessedAt := event["processed_at"]
		if !hasProcessedAt {
			t.Fatalf("event %d missing processed_at body=%s", index, body)
		}
		if wantEvent.ProcessedAt == "" {
			if string(rawProcessedAt) != "null" {
				t.Fatalf("event %d processed_at = %s; want null body=%s", index, rawProcessedAt, body)
			}
		} else {
			var processedAt string
			if err := json.Unmarshal(rawProcessedAt, &processedAt); err != nil {
				t.Fatalf("event %d processed_at: %v body=%s", index, err, body)
			}
			if processedAt != wantEvent.ProcessedAt {
				t.Fatalf("event %d processed_at = %q; want %q body=%s", index, processedAt, wantEvent.ProcessedAt, body)
			}
		}
	}
}

func assertJSONEqualHTTP(t *testing.T, raw json.RawMessage, want string) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(raw, &gotValue); err != nil {
		t.Fatalf("decode response payload %s: %v", raw, err)
	}
	var wantValue any
	if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
		t.Fatalf("decode expected payload %s: %v", want, err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("payload = %#v; want %#v", gotValue, wantValue)
	}
}
