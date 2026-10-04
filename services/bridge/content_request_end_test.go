package agentruntimebridge

import (
	"encoding/json"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLRequestEndAppends(t *testing.T) {
	for _, variant := range []string{"reasoning-first-assistant", "existing-assistant", "empty-request", "failed-suffix", "stale-suffix", "changed-replay"} {
		t.Run(variant, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.store.RuntimeBindingTokenHMACKey = []byte("request-end-cold-test-key")
			f.start(t)
			client, _ := startSandboxProductionBoundaryBridgeClient(t, NewBridgeAPIServer(f.store), f.scope.Binding.TargetPodUid)
			ctx := metadata.AppendToOutgoingContext(f.ctx, "authorization", "Bearer sandbox-production-runtime-token")
			var existing *int64
			if variant == "existing-assistant" || variant == "changed-replay" {
				written, err := f.store.WriteEvent(f.ctx, f.request("agent.message", "alpha", "evt_00000000000000000000000000000001"))
				if err != nil {
					t.Fatal(err)
				}
				existing = written.GetCommitted().AssignedMessageSequence
			}
			request := &bridgev1.WriteRequestEndRequest{Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "stop", UsageJson: `{}`, ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: existing}}
			if variant != "empty-request" {
				request.TrailingContextDelta = &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "reason-tail", ProviderMetadataJson: bridgeString(`{"anthropic":{"signature":"tail-signature"}}`)}}}}}
			}
			if variant == "failed-suffix" {
				request.IsError = true
				request.ErrorKind = "provider_error"
				request.FinishReason = "error"
				request.ProviderContextRetention.Disposition = "failed"
			}
			if variant == "stale-suffix" {
				request.Scope = proto.Clone(f.scope).(*bridgev1.RuntimeScope)
				request.Scope.Binding.BindingGeneration++
			}
			before := f.snapshot(t)
			first, err := client.WriteRequestEnd(ctx, request)
			if variant == "failed-suffix" || variant == "stale-suffix" {
				if variant == "failed-suffix" && status.Code(err) != codes.InvalidArgument {
					t.Fatalf("failed suffix = %v/%v", first, err)
				}
				if variant == "stale-suffix" && (err != nil || first.GetStale() == nil || first.GetCommitted() != nil || first.GetDuplicate() != nil) {
					t.Fatalf("stale suffix = %v/%v", first, err)
				}
				if f.snapshot(t) != before {
					t.Fatal("rejected suffix changed exact durable rows")
				}
				return
			}
			if err != nil || first.GetCommitted() == nil {
				t.Fatalf("End = %v/%v", first, err)
			}
			sequence := first.GetCommitted().GetOrdinary().SealedMessageSequence
			if variant == "empty-request" {
				if sequence != nil {
					t.Fatalf("empty End created phantom Assistant %d", *sequence)
				}
			} else if sequence == nil || *sequence != 1 || (existing != nil && *sequence != *existing) {
				t.Fatalf("End assigned/retained Assistant = %v", sequence)
			}
			before = f.snapshot(t)
			replay, err := client.WriteRequestEnd(ctx, proto.Clone(request).(*bridgev1.WriteRequestEndRequest))
			if err != nil || replay.GetDuplicate().GetRequestEndEventId() != first.GetCommitted().GetRequestEndEventId() || replay.GetDuplicate().GetOrdinary().GetSealedMessageSequence() != first.GetCommitted().GetOrdinary().GetSealedMessageSequence() {
				t.Fatalf("duplicate End = %v/%v", replay, err)
			}
			if variant == "changed-replay" {
				changed := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
				changed.TrailingContextDelta.Parts[0].GetReasoning().Text = "changed-tail"
				if _, err := client.WriteRequestEnd(ctx, changed); status.Code(err) != codes.AlreadyExists {
					t.Fatalf("changed suffix replay = %v", err)
				}
			}
			if f.snapshot(t) != before {
				t.Fatal("End replay/conflict modified persisted prefix or suffix")
			}
			loaded, err := client.LoadContext(ctx, &bridgev1.LoadContextRequest{Scope: f.scope})
			if err != nil {
				t.Fatal(err)
			}
			var payload bridgeLoadContextPayload
			if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
				t.Fatal(err)
			}
			if variant == "empty-request" {
				if len(payload.Messages) != 0 || payload.CurrentRequestMessage != nil {
					t.Fatalf("empty cold context = %#v", payload)
				}
				return
			}
			if len(payload.Messages) != 1 || payload.Messages[0].MessageSequence != *sequence || payload.CurrentRequestMessage == nil || payload.CurrentRequestMessage.ModelRequestID != "request" || payload.CurrentRequestMessage.AssistantMessageSequence != *sequence {
				t.Fatalf("End-created/retained cold Assistant = %#v/%#v", payload.Messages, payload.CurrentRequestMessage)
			}
			wantParts := 1
			if existing != nil {
				wantParts = 3
			}
			if len(payload.Messages[0].Parts) != wantParts {
				t.Fatalf("cold parts = %#v; want %d", payload.Messages[0].Parts, wantParts)
			}
			var tail map[string]any
			if err := json.Unmarshal(payload.Messages[0].Parts[wantParts-1], &tail); err != nil {
				t.Fatal(err)
			}
			metadataJSON, _ := json.Marshal(tail["providerMetadata"])
			if tail["type"] != "reasoning" || tail["text"] != "reason-tail" || string(metadataJSON) != `{"anthropic":{"signature":"tail-signature"}}` {
				t.Fatalf("cold suffix = %#v", tail)
			}
			var start, end int
			for _, event := range payload.TurnFacts.Events {
				if event.ModelRequestID != nil && *event.ModelRequestID == "request" {
					if event.RequestStart != nil {
						start++
					}
					if event.RequestEnd != nil {
						end++
						if existing == nil && event.RequestEnd.ProviderContextRetention.AssistantMessageSequence != nil {
							t.Fatal("cold loader invented caller retention selection")
						}
					}
				}
			}
			if start != 1 || end != 1 {
				t.Fatalf("cold direct Start/End facts = %d/%d", start, end)
			}
			if f.snapshot(t) != before {
				t.Fatal("cold End readback modified durable declarations")
			}
		})
	}
}
