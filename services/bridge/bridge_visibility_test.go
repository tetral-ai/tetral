package agentruntimebridge

import "testing"

func TestThreadMutationScopeDerivesSessionVisibilityFromDurableRoleAndEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		scope          threadMutationScope
		eventType      string
		visibility     string
		sessionVisible bool
	}{
		{name: "main span", scope: threadMutationScope{visibility: "public", role: "main"}, eventType: "span.model_request_start", visibility: "public", sessionVisible: true},
		{name: "main ordinary", scope: threadMutationScope{visibility: "public", role: "main"}, eventType: "agent.message", visibility: "public", sessionVisible: true},
		{name: "child message sent", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "agent.thread_message_sent", visibility: "public", sessionVisible: true},
		{name: "child message received", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "agent.thread_message_received", visibility: "public", sessionVisible: true},
		{name: "child created", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "session.thread_created", visibility: "public", sessionVisible: true},
		{name: "child running", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "session.thread_status_running", visibility: "public", sessionVisible: true},
		{name: "child idle", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "session.thread_status_idle", visibility: "public", sessionVisible: true},
		{name: "child rescheduled", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "session.thread_status_rescheduled", visibility: "public", sessionVisible: true},
		{name: "child terminated", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "session.thread_status_terminated", visibility: "public", sessionVisible: true},
		{name: "child span", scope: threadMutationScope{visibility: "public", role: "subagent"}, eventType: "span.model_request_end", visibility: "public", sessionVisible: false},
		{name: "reviewer", scope: threadMutationScope{visibility: "internal", role: "approval_reviewer"}, eventType: "approval_review.decision", visibility: "internal", sessionVisible: false},
		{name: "reviewer compaction", scope: threadMutationScope{visibility: "internal", role: "approval_reviewer"}, eventType: "agent.thread_context_compacted", visibility: "internal", sessionVisible: false},
		{name: "internal main", scope: threadMutationScope{visibility: "internal", role: "main"}, eventType: "agent.message", visibility: "internal", sessionVisible: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			visibility, sessionVisible := test.scope.publicProjection(test.eventType)
			if visibility != test.visibility || sessionVisible != test.sessionVisible {
				t.Fatalf("projection = %s/%v; want %s/%v", visibility, sessionVisible, test.visibility, test.sessionVisible)
			}
		})
	}
}
