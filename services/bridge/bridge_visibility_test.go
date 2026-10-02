package agentruntimebridge

import (
	"testing"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"
)

func TestThreadMutationScopeDerivesSessionVisibilityFromDurableRoleAndEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		scope          runtimecontrol.ThreadMutationScope
		eventType      string
		visibility     string
		sessionVisible bool
	}{
		{name: "main span", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "main"}, eventType: "span.model_request_start", visibility: "public", sessionVisible: true},
		{name: "main ordinary", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "main"}, eventType: "agent.message", visibility: "public", sessionVisible: true},
		{name: "child message sent", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "agent.thread_message_sent", visibility: "public", sessionVisible: true},
		{name: "child message received", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "agent.thread_message_received", visibility: "public", sessionVisible: true},
		{name: "child created", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "session.thread_created", visibility: "public", sessionVisible: true},
		{name: "child running", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "session.thread_status_running", visibility: "public", sessionVisible: true},
		{name: "child idle", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "session.thread_status_idle", visibility: "public", sessionVisible: true},
		{name: "child rescheduled", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "session.thread_status_rescheduled", visibility: "public", sessionVisible: true},
		{name: "child terminated", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "session.thread_status_terminated", visibility: "public", sessionVisible: true},
		{name: "child span", scope: runtimecontrol.ThreadMutationScope{Visibility: "public", Role: "subagent"}, eventType: "span.model_request_end", visibility: "public", sessionVisible: false},
		{name: "reviewer", scope: runtimecontrol.ThreadMutationScope{Visibility: "internal", Role: "approval_reviewer"}, eventType: "approval_review.decision", visibility: "internal", sessionVisible: false},
		{name: "reviewer compaction", scope: runtimecontrol.ThreadMutationScope{Visibility: "internal", Role: "approval_reviewer"}, eventType: "agent.thread_context_compacted", visibility: "internal", sessionVisible: false},
		{name: "internal main", scope: runtimecontrol.ThreadMutationScope{Visibility: "internal", Role: "main"}, eventType: "agent.message", visibility: "internal", sessionVisible: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			visibility, sessionVisible := test.scope.PublicProjection(test.eventType)
			if visibility != test.visibility || sessionVisible != test.sessionVisible {
				t.Fatalf("projection = %s/%v; want %s/%v", visibility, sessionVisible, test.visibility, test.sessionVisible)
			}
		})
	}
}
