package agentruntimebridge

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func contentRepairRequest(scope *bridgev1.RuntimeScope) *bridgev1.CommitInternalToolRepairRequest {
	return &bridgev1.CommitInternalToolRepairRequest{
		Scope: scope, ModelRequestId: "request", ModelToolCallId: "call", ToolName: "unknown", CanonicalInputJson: `{"q":"x"}`,
		RepairKey:                   internalToolRepairKey("request", "call", "unknown"),
		Error:                       &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`},
		ReasoningPrefixContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "reason-before-tool", ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"fixture-signature-tool"}}`)}}}}},
	}
}

func TestInternalToolRepairPrefixDigestUsesIndependentCanonicalBytes(t *testing.T) {
	request := contentRepairRequest(&bridgev1.RuntimeScope{SessionThreadId: "thread"})
	const prefixLiteral = `{"canonical_input":{"q":"x"},"error":{"message":"invalid tool","retryable":false,"type":"provider_tool_protocol_error"},"model_request_id":"request","model_tool_call_id":"call","operation_kind":"commit_internal_tool_repair","reasoning_prefix_context_delta":{"parts":[{"providerMetadata":{"anthropic":{"signature":"fixture-signature-tool"}},"text":"reason-before-tool","type":"reasoning"}]},"repair_key":"internal_invalid_tool_d5423292561dc475693ba9695c2ef8768de5c4921c67aa6e90ba6b1030856912","session_thread_id":"thread","tool_name":"unknown"}`
	const absentLiteral = `{"canonical_input":{"q":"x"},"error":{"message":"invalid tool","retryable":false,"type":"provider_tool_protocol_error"},"model_request_id":"request","model_tool_call_id":"call","operation_kind":"commit_internal_tool_repair","repair_key":"internal_invalid_tool_d5423292561dc475693ba9695c2ef8768de5c4921c67aa6e90ba6b1030856912","session_thread_id":"thread","tool_name":"unknown"}`
	for _, tc := range []struct {
		name, literal, hash string
		prefix              *bridgev1.RuntimeContextDelta
	}{
		{"signed-prefix", prefixLiteral, "86af699e0f85ced039aa7949271c425c123cafac028e6b6709782ea427408117", request.ReasoningPrefixContextDelta},
		{"absent", absentLiteral, "4c7b5cee053e645780f19dbbbaf00ec3f5771b95b6fa0e2fc8a1dbcf7df142fa", nil},
		{"empty", absentLiteral, "4c7b5cee053e645780f19dbbbaf00ec3f5771b95b6fa0e2fc8a1dbcf7df142fa", &bridgev1.RuntimeContextDelta{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if fmt.Sprintf("%x", sha256.Sum256([]byte(tc.literal))) != tc.hash {
				t.Fatal("independent literal hash differs")
			}
			request.ReasoningPrefixContextDelta = tc.prefix
			got, err := internalToolRepairDeclarationDigest(request, request.RepairKey)
			if err != nil || got != tc.hash {
				t.Fatalf("digest %s/%v want %s", got, err, tc.hash)
			}
		})
	}
}

func TestPostgreSQLInternalToolRepairReasoningPrefix(t *testing.T) {
	for _, variant := range []string{"signed-replay", "changed-prefix", "post-end-replay", "post-end-new-member", "stale-caller", "invalid-prefix", "aggregate-budget"} {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.start(t)
			request := contentRepairRequest(f.scope)
			client, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(f.store), f.scope.Binding.TargetPodUid)
			ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
			if variant == "post-end-new-member" {
				f.end(t, nil)
			}
			if variant == "stale-caller" {
				request.Scope = proto.Clone(f.scope).(*bridgev1.RuntimeScope)
				request.Scope.Binding.BindingGeneration++
			}
			if variant == "invalid-prefix" {
				request.ReasoningPrefixContextDelta.Parts = []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "not reasoning"}}}}
			}
			if variant == "aggregate-budget" {
				existing := f.request("agent.message", "prior", "evt_00000000000000000000000000000001")
				part := existing.AssistantContextDelta.Parts[0]
				existing.AssistantContextDelta.Parts = nil
				for i := 0; i < 16; i++ {
					existing.AssistantContextDelta.Parts = append(existing.AssistantContextDelta.Parts, proto.Clone(part).(*bridgev1.RuntimeContextPart))
				}
				existing.AssistantContextDelta.Parts = append(existing.AssistantContextDelta.Parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "alpha"}}})
				if _, err := f.store.WriteEvent(f.ctx, existing); err != nil {
					t.Fatal(err)
				}
			}
			before := f.snapshot(t)
			first, err := client.CommitInternalToolRepair(ctx, request)
			switch variant {
			case "post-end-new-member":
				if status.Code(err) != codes.FailedPrecondition || first != nil {
					t.Fatalf("expected sealed-member rejection: %v/%v", first, err)
				}
			case "stale-caller":
				if err != nil || first.GetStale() == nil || first.GetCommitted() != nil || first.GetDuplicate() != nil {
					t.Fatalf("expected typed stale: %v/%v", first, err)
				}
			case "invalid-prefix", "aggregate-budget":
				if status.Code(err) != codes.InvalidArgument || first != nil {
					t.Fatalf("expected invalid argument: %v/%v", first, err)
				}
				if variant == "aggregate-budget" && !strings.Contains(status.Convert(err).Message(), "stable reasoning exceeds per-request budget") {
					t.Fatalf("aggregate-budget rejected before owning budget validation: %v", err)
				}
			default:
				if err != nil || first.GetCommitted() == nil {
					t.Fatalf("repair commit %v/%v", first, err)
				}
				var data string
				if err := f.admin.QueryRowContext(f.ctx, `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE workspace_id=$1 AND session_id=$2 AND model_request_id='request'`, f.scope.WorkspaceId, f.scope.SessionId).Scan(&data); err != nil {
					t.Fatal(err)
				}
				const want = `{"parts":[{"type":"reasoning","text":"reason-before-tool","providerMetadata":{"anthropic":{"signature":"fixture-signature-tool"}}},{"type":"tool_call","modelToolCallId":"call","toolName":"unknown","canonicalInput":{"q":"x"}},{"type":"tool_result","modelToolCallId":"call","result":{"type":"error","error":{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}}}]}`
				var gotValue, wantValue any
				if json.Unmarshal([]byte(data), &gotValue) != nil || json.Unmarshal([]byte(want), &wantValue) != nil {
					t.Fatal("invalid independent context literal")
				}
				gotBytes, _ := json.Marshal(gotValue)
				wantBytes, _ := json.Marshal(wantValue)
				if string(gotBytes) != string(wantBytes) {
					t.Fatalf("repair order/signature %s want %s", gotBytes, wantBytes)
				}
				var payload string
				if err := f.admin.QueryRowContext(f.ctx, `SELECT payload_json FROM session_events WHERE event_id=$1`, first.GetCommitted().GetRepairEventId()).Scan(&payload); err != nil {
					t.Fatal(err)
				}
				var public map[string]any
				_ = json.Unmarshal([]byte(payload), &public)
				if len(public) != 4 || public["repair_kind"] != "invalid_tool" {
					t.Fatalf("repair public projection contains private prefix: %s", payload)
				}
				if variant == "post-end-replay" {
					seq := first.GetCommitted().GetAssignedMessageSequence()
					ended, err := client.WriteRequestEnd(ctx, &bridgev1.WriteRequestEndRequest{
						Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "stop", UsageJson: `{}`,
						ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &seq, RepairEventIds: []string{first.GetCommitted().GetRepairEventId()}},
					})
					if err != nil || ended.GetCommitted() == nil {
						t.Fatalf("End retains actual repair receipt: %v/%v", ended, err)
					}
				}
				before = f.snapshot(t)
				if variant == "changed-prefix" {
					request.ReasoningPrefixContextDelta.Parts[0].GetReasoning().Text = "changed-prefix"
				}
				replay, err := client.CommitInternalToolRepair(ctx, request)
				if variant == "changed-prefix" {
					if status.Code(err) != codes.AlreadyExists || replay != nil {
						t.Fatalf("changed-prefix conflict %v/%v", replay, err)
					}
				} else if err != nil || replay.GetDuplicate().GetRepairEventId() != first.GetCommitted().GetRepairEventId() || replay.GetDuplicate().GetAssignedMessageSequence() != first.GetCommitted().GetAssignedMessageSequence() {
					t.Fatalf("repair replay %v/%v", replay, err)
				}
			}
			if f.snapshot(t) != before {
				t.Fatal("rejected/replayed repair changed durable state")
			}
		})
	}
}
