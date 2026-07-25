package eventwire

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const (
	eventTypeAgentMCPToolResult             = "agent.mcp_tool_result"
	eventTypeAgentMCPToolUse                = "agent.mcp_tool_use"
	eventTypeAgentMessage                   = "agent.message"
	eventTypeAgentThinking                  = "agent.thinking"
	eventTypeAgentThreadContextCompacted    = "agent.thread_context_compacted"
	eventTypeAgentThreadMessageReceived     = "agent.thread_message_received"
	eventTypeAgentThreadMessageSent         = "agent.thread_message_sent"
	eventTypeAgentToolResult                = "agent.tool_result"
	eventTypeAgentToolUse                   = "agent.tool_use"
	eventTypeSessionDeleted                 = "session.deleted"
	eventTypeSessionError                   = "session.error"
	eventTypeSessionStatusIdle              = "session.status_idle"
	eventTypeSessionStatusRescheduled       = "session.status_rescheduled"
	eventTypeSessionStatusRunning           = "session.status_running"
	eventTypeSessionStatusTerminated        = "session.status_terminated"
	eventTypeSessionThreadCreated           = "session.thread_created"
	eventTypeSessionThreadStatusIdle        = "session.thread_status_idle"
	eventTypeSessionThreadStatusRescheduled = "session.thread_status_rescheduled"
	eventTypeSessionThreadStatusRunning     = "session.thread_status_running"
	eventTypeSessionThreadStatusTerminated  = "session.thread_status_terminated"
	eventTypeSessionUpdated                 = "session.updated"
	eventTypeSpanModelRequestEnd            = "span.model_request_end"
	eventTypeSpanModelRequestStart          = "span.model_request_start"
	eventTypeUserInterrupt                  = "user.interrupt"
	eventTypeUserMessage                    = "user.message"
	eventTypeUserToolConfirmation           = "user.tool_confirmation"
)

// MarshalPublicEvent flattens the durable type-specific payload into the
// fork-SDK Event union while keeping row-owned metadata authoritative.
func MarshalPublicEvent(id string, eventType string, payload json.RawMessage, processedAt *string) ([]byte, error) {
	fields := map[string]json.RawMessage{}
	if len(bytes.TrimSpace(payload)) > 0 {
		if err := json.Unmarshal(payload, &fields); err != nil {
			return nil, fmt.Errorf("session event payload must be an object: %w", err)
		}
	}
	fields, err := projectPublicPayload(eventType, fields)
	if err != nil {
		return nil, err
	}
	idJSON, err := json.Marshal(id)
	if err != nil {
		return nil, err
	}
	typeJSON, err := json.Marshal(eventType)
	if err != nil {
		return nil, err
	}
	processedAtJSON, err := json.Marshal(processedAt)
	if err != nil {
		return nil, err
	}
	fields["id"] = idJSON
	fields["type"] = typeJSON
	fields["processed_at"] = processedAtJSON
	return json.Marshal(fields)
}

// PublicEventTypes returns the Engine event producer set owned by this
// projection. The compatibility gate derives its required fixtures from this
// set and the frozen SDK registry rather than copying the SDK union.
func PublicEventTypes() []string {
	return []string{
		eventTypeAgentMCPToolResult,
		eventTypeAgentMCPToolUse,
		eventTypeAgentMessage,
		eventTypeAgentThinking,
		eventTypeAgentThreadContextCompacted,
		eventTypeAgentThreadMessageReceived,
		eventTypeAgentThreadMessageSent,
		eventTypeAgentToolResult,
		eventTypeAgentToolUse,
		eventTypeSessionDeleted,
		eventTypeSessionError,
		eventTypeSessionStatusIdle,
		eventTypeSessionStatusRescheduled,
		eventTypeSessionStatusRunning,
		eventTypeSessionStatusTerminated,
		eventTypeSessionThreadCreated,
		eventTypeSessionThreadStatusIdle,
		eventTypeSessionThreadStatusRescheduled,
		eventTypeSessionThreadStatusRunning,
		eventTypeSessionThreadStatusTerminated,
		eventTypeSessionUpdated,
		eventTypeSpanModelRequestEnd,
		eventTypeSpanModelRequestStart,
		eventTypeUserInterrupt,
		eventTypeUserMessage,
		eventTypeUserToolConfirmation,
	}
}

func projectPublicPayload(eventType string, fields map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	switch eventType {
	case eventTypeUserMessage, eventTypeAgentMessage:
		return retainFields(fields, "content"), nil
	case eventTypeUserInterrupt:
		return retainFields(fields, "session_thread_id"), nil
	case eventTypeUserToolConfirmation:
		return retainFields(fields, "tool_use_id", "result", "deny_message", "session_thread_id"), nil
	case eventTypeAgentToolUse:
		return retainFields(fields, "name", "input", "evaluated_permission", "session_thread_id"), nil
	case eventTypeAgentToolResult:
		return retainFields(fields, "tool_use_id", "content", "is_error"), nil
	case eventTypeAgentMCPToolUse:
		return retainFields(fields, "name", "input", "mcp_server_name", "evaluated_permission", "session_thread_id"), nil
	case eventTypeAgentMCPToolResult:
		return retainFields(fields, "mcp_tool_use_id", "content", "is_error"), nil
	case eventTypeAgentThreadMessageSent:
		return projectThreadMessage(fields, "target_thread_id", "target_task_name", "to_session_thread_id", "to_agent_name")
	case eventTypeAgentThreadMessageReceived:
		return projectThreadMessage(fields, "source_thread_id", "source_task_name", "from_session_thread_id", "from_agent_name")
	case eventTypeSessionStatusIdle:
		return retainFields(fields, "stop_reason"), nil
	case eventTypeSessionError:
		return retainFields(fields, "error"), nil
	case eventTypeSessionThreadCreated,
		eventTypeSessionThreadStatusRunning,
		eventTypeSessionThreadStatusRescheduled,
		eventTypeSessionThreadStatusTerminated:
		return projectThreadStatus(fields, false), nil
	case eventTypeSessionThreadStatusIdle:
		return projectThreadStatus(fields, true), nil
	case eventTypeSessionUpdated:
		return retainFields(fields, "agent", "metadata", "title"), nil
	case eventTypeSpanModelRequestEnd:
		return retainFields(fields, "model_request_start_id", "is_error", "model_usage"), nil
	case eventTypeAgentThinking,
		eventTypeAgentThreadContextCompacted,
		eventTypeSessionDeleted,
		eventTypeSessionStatusRescheduled,
		eventTypeSessionStatusRunning,
		eventTypeSessionStatusTerminated,
		eventTypeSpanModelRequestStart:
		return map[string]json.RawMessage{}, nil
	default:
		return nil, fmt.Errorf("unsupported public session event type %q", eventType)
	}
}

func projectThreadMessage(
	fields map[string]json.RawMessage,
	durableThreadID string,
	durableTaskName string,
	publicThreadID string,
	publicAgentName string,
) (map[string]json.RawMessage, error) {
	projected := make(map[string]json.RawMessage, 3)
	if value, ok := fields[durableThreadID]; ok {
		projected[publicThreadID] = value
	}
	if value, ok := fields[durableTaskName]; ok && publicOptionalStringPresent(value) {
		projected[publicAgentName] = value
	}
	message, ok := fields["message"]
	if !ok {
		return projected, nil
	}
	var messageFields map[string]json.RawMessage
	if err := json.Unmarshal(message, &messageFields); err != nil {
		return nil, fmt.Errorf("thread message payload must be an object: %w", err)
	}
	if content, ok := messageFields["content"]; ok {
		projected["content"] = content
	}
	return projected, nil
}

func projectThreadStatus(fields map[string]json.RawMessage, includeStopReason bool) map[string]json.RawMessage {
	projected := retainFields(fields, "session_thread_id")
	if taskName, ok := fields["task_name"]; ok && publicOptionalStringPresent(taskName) {
		projected["agent_name"] = taskName
	}
	if includeStopReason {
		if stopReason, ok := fields["stop_reason"]; ok {
			projected["stop_reason"] = stopReason
		}
	}
	return projected
}

func publicOptionalStringPresent(value json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return false
	}
	var text string
	return json.Unmarshal(value, &text) == nil && text != ""
}

func retainFields(fields map[string]json.RawMessage, names ...string) map[string]json.RawMessage {
	retained := make(map[string]json.RawMessage, len(names))
	for _, name := range names {
		if value, ok := fields[name]; ok {
			retained[name] = value
		}
	}
	return retained
}
