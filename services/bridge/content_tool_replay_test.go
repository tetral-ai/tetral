package agentruntimebridge

import (
	"encoding/json"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLToolDeclarationReplay(t *testing.T) {
	testToolDeclarationReplay(t, false)
}

func TestPostgreSQLPostEndDeclarationReplay(t *testing.T) {
	testToolDeclarationReplay(t, true)
}

// Each call uses the ordinary Tool digest and per-invocation prepared context.
// The second signed declaration is distinct; replaying the first must never
// append a prefix or call borrowed from the recently prepared second request.
func testToolDeclarationReplay(t *testing.T, afterEnd bool) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	firstRequest := &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "tool-a", ModelRequestId: "request", ToolDeclaration: bridgeSignedReasoningToolDeclarationForTest("call-a", "Read", `{"path":"alpha"}`, "ask")}
	loss := &contentResponseLossServer{BridgeAPIServer: NewBridgeAPIServer(f.store), drop: true}
	firstClient, _ := startSandboxProductionBoundaryBridgeClient(t, loss, f.scope.Binding.TargetPodUid)
	ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
	if _, err := firstClient.WriteEvent(ctx, firstRequest); status.Code(err) != codes.Unavailable {
		t.Fatalf("committed Tool ACK loss = %v", err)
	}
	first := loss.receipt()
	secondRequest := &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "tool-b", ModelRequestId: "request", ToolDeclaration: bridgeSignedReasoningToolDeclarationForTest("call-b", "Read", `{"path":"beta"}`, "ask")}
	secondRequest.ToolDeclaration.LeadingReasoning[0].Text = "second declaration reasoning"
	second, err := f.store.WriteEvent(f.ctx, secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetCommitted().GetAssignedMessageSequence() != second.GetCommitted().GetAssignedMessageSequence() {
		t.Fatal("Tool declarations split the request Assistant")
	}
	var stored string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT data_json FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND model_request_id='request' AND kind='assistant'`, f.scope.WorkspaceId, f.scope.SessionId).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	var actual, expected any
	if err := json.Unmarshal([]byte(stored), &actual); err != nil {
		t.Fatal(err)
	}
	// This literal does not invoke the context or digest builders under test.
	if err := json.Unmarshal([]byte(`{"parts":[{"type":"reasoning","text":"provider-declared reasoning","providerMetadata":{"anthropic":{"signature":"sig_provider_context"}}},{"type":"tool_call","modelToolCallId":"call-a","toolName":"Read","canonicalInput":{"path":"alpha"}},{"type":"reasoning","text":"second declaration reasoning","providerMetadata":{"anthropic":{"signature":"sig_provider_context"}}},{"type":"tool_call","modelToolCallId":"call-b","toolName":"Read","canonicalInput":{"path":"beta"}}]}`), &expected); err != nil {
		t.Fatal(err)
	}
	actualJSON, _ := json.Marshal(actual)
	expectedJSON, _ := json.Marshal(expected)
	if string(actualJSON) != string(expectedJSON) {
		t.Fatalf("two prepared Tool contexts = %s; want literal %s", actualJSON, expectedJSON)
	}
	if afterEnd {
		sequence := first.GetCommitted().GetAssignedMessageSequence()
		if _, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{first.GetCommitted().GetEventId(), second.GetCommitted().GetEventId()}}}); err != nil {
			t.Fatal(err)
		}
	}
	before := f.snapshot(t)
	other := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "bridge", nil)))
	otherClient, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(other), f.scope.Binding.TargetPodUid)
	for range 2 {
		replayed, err := otherClient.WriteEvent(ctx, proto.Clone(firstRequest).(*bridgev1.WriteEventRequest))
		if err != nil || replayed.GetDuplicate().GetEventId() != first.GetCommitted().GetEventId() || replayed.GetDuplicate().GetAssignedMessageSequence() != first.GetCommitted().GetAssignedMessageSequence() {
			t.Fatalf("ordinary Tool duplicate ACK = %v/%v", replayed, err)
		}
	}
	changed := proto.Clone(firstRequest).(*bridgev1.WriteEventRequest)
	changed.ToolDeclaration.PublicExecutionInputJson = `{"path":"changed"}`
	if _, err := other.WriteEvent(f.ctx, changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed Tool replay = %v; want AlreadyExists", err)
	}
	if afterEnd {
		newMember := proto.Clone(secondRequest).(*bridgev1.WriteEventRequest)
		newMember.RuntimeWriteId = "new-tool-after-end"
		newMember.ToolDeclaration.ModelToolCallId = "call-c"
		stale, err := other.WriteEvent(f.ctx, newMember)
		if err != nil || stale.GetStale() == nil || stale.GetCommitted() != nil || stale.GetDuplicate() != nil {
			t.Fatalf("new postEnd Tool member = %v/%v; want typed stale", stale, err)
		}
	}
	if f.snapshot(t) != before {
		t.Fatal("ordinary Tool replay/conflict/postEnd rejection changed exact durable rows")
	}
	// A fixture generation advance isolates the receipt custody fence. Actual
	// JobRunner/Runtime takeover is exercised by the process compositions.
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_runtime_bindings SET binding_generation=2 WHERE workspace_id=$1 AND session_id=$2`, f.scope.WorkspaceId, f.scope.SessionId); err != nil {
		t.Fatal(err)
	}
	oldScopeRequest := proto.Clone(firstRequest).(*bridgev1.WriteEventRequest)
	stale, err := otherClient.WriteEvent(ctx, oldScopeRequest)
	if err != nil || stale.GetStale() == nil || stale.GetDuplicate() != nil || stale.GetCommitted() != nil {
		t.Fatalf("old receipt custody = %v/%v; want stale", stale, err)
	}
	current := proto.Clone(firstRequest).(*bridgev1.WriteEventRequest)
	current.Scope.Binding.BindingGeneration = 2
	replayed, err := otherClient.WriteEvent(ctx, current)
	if err != nil || replayed.GetDuplicate().GetEventId() != first.GetCommitted().GetEventId() {
		t.Fatalf("current custody receipt recovery = %v/%v", replayed, err)
	}
	if f.snapshot(t) != before {
		t.Fatal("custody replay changed content/Events/receipts")
	}
	if !afterEnd {
		sequence := first.GetCommitted().GetAssignedMessageSequence()
		if _, err := other.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{Scope: current.Scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{first.GetCommitted().GetEventId(), second.GetCommitted().GetEventId()}}}); err != nil {
			t.Fatal(err)
		}
	}
	seedBridgeAPIRequestStart(t, other, current.Scope, "next-start", "request-next", runtimecontrol.RequestKindAgentProviderRequest, first.GetCommitted().GetAssignedMessageSequence())
	next := proto.Clone(firstRequest).(*bridgev1.WriteEventRequest)
	next.Scope = current.Scope
	next.RuntimeWriteId = "later-request-same-call"
	next.ModelRequestId = "request-next"
	before = f.snapshot(t)
	if _, err := otherClient.WriteEvent(ctx, next); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("later same-Thread historical call identity = %v; want AlreadyExists", err)
	}
	if f.snapshot(t) != before {
		t.Fatal("historical call rejection changed old durable state")
	}
	childID := id.New("thr_")
	seedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID)
	childScope := proto.Clone(current.Scope).(*bridgev1.RuntimeScope)
	childScope.SessionThreadId = childID
	seedBridgeAPIRequestStart(t, other, childScope, "other-thread-start", "request-other-thread", runtimecontrol.RequestKindAgentProviderRequest, 0)
	independent := proto.Clone(firstRequest).(*bridgev1.WriteEventRequest)
	independent.Scope = childScope
	independent.RuntimeWriteId = "other-thread-same-call"
	independent.ModelRequestId = "request-other-thread"
	independent.ToolDeclaration.PublicExecutionInputJson = `{"path":"independent"}`
	child, err := otherClient.WriteEvent(ctx, independent)
	if err != nil || child.GetCommitted() == nil || child.GetCommitted().GetEventId() == first.GetCommitted().GetEventId() {
		t.Fatalf("authorized other-Thread call scope = %v/%v", child, err)
	}
	var originalInput, childInput string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT (SELECT part->'canonicalInput'->>'path' FROM session_messages m CROSS JOIN LATERAL jsonb_array_elements(m.data_json::jsonb->'parts') part WHERE m.workspace_id=$1 AND m.session_id=$2 AND m.session_thread_id=$3 AND m.model_request_id='request' AND part->>'type'='tool_call' AND part->>'modelToolCallId'='call-a'),(SELECT part->'canonicalInput'->>'path' FROM session_messages m CROSS JOIN LATERAL jsonb_array_elements(m.data_json::jsonb->'parts') part WHERE m.workspace_id=$1 AND m.session_id=$2 AND m.session_thread_id=$4 AND part->>'type'='tool_call' AND part->>'modelToolCallId'='call-a')`, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID).Scan(&originalInput, &childInput); err != nil {
		t.Fatal(err)
	}
	if originalInput != "alpha" || childInput != "independent" {
		t.Fatalf("call association crossed Threads: original %q other %q", originalInput, childInput)
	}
}
