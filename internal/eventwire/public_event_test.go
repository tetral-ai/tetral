package eventwire_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

func TestMarshalPublicEventFlattensPayloadAndKeepsRowMetadataAuthoritative(t *testing.T) {
	processedAt := "2026-07-10T00:00:00Z"
	body, err := eventwire.MarshalPublicEvent(
		"evt_row",
		"agent.message",
		json.RawMessage(`{"id":"payload-id","type":"wrong","processed_at":"wrong","content":[{"type":"text","text":"hello"}]}`),
		&processedAt,
	)
	if err != nil {
		t.Fatalf("MarshalPublicEvent: %v", err)
	}
	var event map[string]json.RawMessage
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if string(event["id"]) != `"evt_row"` || string(event["type"]) != `"agent.message"` || string(event["processed_at"]) != `"2026-07-10T00:00:00Z"` {
		t.Fatalf("row metadata did not win: %s", body)
	}
	if _, exists := event["payload"]; exists {
		t.Fatalf("generic payload envelope leaked: %s", body)
	}
	if string(event["content"]) != `[{"type":"text","text":"hello"}]` {
		t.Fatalf("content not flattened: %s", body)
	}
}

func TestMarshalPublicEventProjectsBridgeChildVariantsWithExactLineage(t *testing.T) {
	t.Parallel()

	processedAt := "2026-07-14T12:34:56Z"
	content := []any{
		map[string]any{"type": "text", "text": "first block"},
		map[string]any{"type": "text", "text": "second block"},
	}
	tests := []struct {
		name      string
		eventType string
		payload   string
		want      map[string]any
	}{
		{
			name:      "message sent uses target thread and callable task name",
			eventType: "agent.thread_message_sent",
			payload:   `{"type":"agent.thread_message_sent","delivery_id":"delivery_sent","source_thread_id":"sthr_source_distinct","target_thread_id":"sthr_target_distinct","target_task_name":"callable_target","source_tool_use_event_id":"sevt_tool_sent","message":{"id":"msg_sent","role":"assistant","content":[{"type":"text","text":"first block"},{"type":"text","text":"second block"}]},"task_name":"wrong_task","agent_type":"research"}`,
			want: map[string]any{
				"content":              content,
				"to_session_thread_id": "sthr_target_distinct",
				"to_agent_name":        "callable_target",
			},
		},
		{
			name:      "message received uses source thread and callable task name",
			eventType: "agent.thread_message_received",
			payload:   `{"type":"agent.thread_message_received","delivery_id":"delivery_received","source_thread_id":"sthr_source_received_distinct","source_task_name":"callable_source","source_tool_use_event_id":"sevt_tool_received","message":{"id":"msg_received","role":"user","content":[{"type":"text","text":"first block"},{"type":"text","text":"second block"}]},"task_name":"wrong_task","agent_type":"general"}`,
			want: map[string]any{
				"content":                content,
				"from_session_thread_id": "sthr_source_received_distinct",
				"from_agent_name":        "callable_source",
			},
		},
		{
			name:      "thread created uses callable task name",
			eventType: "session.thread_created",
			payload:   `{"type":"session.thread_created","session_thread_id":"sthr_created_distinct","parent_thread_id":"sthr_parent_distinct","role":"subagent","visibility":"public","agent_type":"worker","task_name":"callable_created","source_tool_use_event_id":"sevt_tool_created"}`,
			want: map[string]any{
				"session_thread_id": "sthr_created_distinct",
				"agent_name":        "callable_created",
			},
		},
		{
			name:      "thread running uses callable task name",
			eventType: "session.thread_status_running",
			payload:   `{"type":"session.thread_status_running","session_thread_id":"sthr_running_distinct","task_name":"callable_running","agent_type":"general"}`,
			want: map[string]any{
				"session_thread_id": "sthr_running_distinct",
				"agent_name":        "callable_running",
			},
		},
		{
			name:      "thread idle retains stop reason and callable task name",
			eventType: "session.thread_status_idle",
			payload:   `{"type":"session.thread_status_idle","session_thread_id":"sthr_idle_distinct","task_name":"callable_idle","agent_type":"research","stop_reason":{"type":"requires_action","event_ids":["sevt_wait_second","sevt_wait_first"]}}`,
			want: map[string]any{
				"session_thread_id": "sthr_idle_distinct",
				"agent_name":        "callable_idle",
				"stop_reason": map[string]any{
					"type":      "requires_action",
					"event_ids": []any{"sevt_wait_second", "sevt_wait_first"},
				},
			},
		},
		{
			name:      "thread rescheduled uses callable task name",
			eventType: "session.thread_status_rescheduled",
			payload:   `{"type":"session.thread_status_rescheduled","session_thread_id":"sthr_rescheduled_distinct","task_name":"callable_rescheduled","agent_type":"worker"}`,
			want: map[string]any{
				"session_thread_id": "sthr_rescheduled_distinct",
				"agent_name":        "callable_rescheduled",
			},
		},
		{
			name:      "thread terminated uses callable task name",
			eventType: "session.thread_status_terminated",
			payload:   `{"type":"session.thread_status_terminated","session_thread_id":"sthr_terminated_distinct","task_name":"callable_terminated","agent_type":"general"}`,
			want: map[string]any{
				"session_thread_id": "sthr_terminated_distinct",
				"agent_name":        "callable_terminated",
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			body, err := eventwire.MarshalPublicEvent(
				"sevt_child_projection_"+string(rune('a'+index)),
				test.eventType,
				json.RawMessage(test.payload),
				&processedAt,
			)
			if err != nil {
				t.Fatalf("MarshalPublicEvent: %v", err)
			}
			var got map[string]any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode public event: %v", err)
			}
			want := map[string]any{
				"id":           "sevt_child_projection_" + string(rune('a'+index)),
				"type":         test.eventType,
				"processed_at": processedAt,
			}
			for name, value := range test.want {
				want[name] = value
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("public event = %#v; want exact %#v; body=%s", got, want, body)
			}
		})
	}
}

func TestMarshalPublicEventOmitsPrimaryThreadAgentNameAliases(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		eventType string
		payload   string
		forbidden string
	}{
		{
			name:      "sent to primary",
			eventType: "agent.thread_message_sent",
			payload:   `{"target_thread_id":"sthr_primary_target","target_task_name":null,"message":{"content":[{"type":"text","text":"sent"}]}}`,
			forbidden: "to_agent_name",
		},
		{
			name:      "received from primary",
			eventType: "agent.thread_message_received",
			payload:   `{"source_thread_id":"sthr_primary_source","source_task_name":null,"message":{"content":[{"type":"text","text":"received"}]}}`,
			forbidden: "from_agent_name",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			body, err := eventwire.MarshalPublicEvent("sevt_primary", test.eventType, json.RawMessage(test.payload), nil)
			if err != nil {
				t.Fatalf("MarshalPublicEvent: %v", err)
			}
			var event map[string]json.RawMessage
			if err := json.Unmarshal(body, &event); err != nil {
				t.Fatalf("decode public event: %v", err)
			}
			if _, exists := event[test.forbidden]; exists {
				t.Fatalf("primary-thread optional agent name %q was emitted: %s", test.forbidden, body)
			}
		})
	}
}

func TestMarshalPublicEventRedactsInternalModelRequestEndFields(t *testing.T) {
	processedAt := "2026-07-10T00:00:00Z"
	body, err := eventwire.MarshalPublicEvent(
		"evt_end",
		"span.model_request_end",
		json.RawMessage(`{"model_request_id":"mreq_internal","model_request_start_id":"evt_start","request_kind":"approval_reviewer","is_error":true,"finish_reason":"error","model_usage":{"input_tokens":3,"output_tokens":1},"request_usage":{"provider_usage_json":"secret"},"error_kind":"runtime_persistence_error"}`),
		&processedAt,
	)
	if err != nil {
		t.Fatalf("MarshalPublicEvent: %v", err)
	}
	var event map[string]json.RawMessage
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	for _, forbidden := range []string{"model_request_id", "request_kind", "finish_reason", "request_usage", "error_kind"} {
		if _, exists := event[forbidden]; exists {
			t.Fatalf("public request-end event leaked %q: %s", forbidden, body)
		}
	}
	if string(event["model_request_start_id"]) != `"evt_start"` || string(event["is_error"]) != "true" {
		t.Fatalf("public request-end fields = %s; want SDK fields retained", body)
	}
	if string(event["model_usage"]) != `{"input_tokens":3,"output_tokens":1}` {
		t.Fatalf("public model_usage = %s; want public usage retained", event["model_usage"])
	}
}
