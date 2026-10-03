package agentruntimebridge

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLMCPManifestNotificationsCommitOneGeneration(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, variant := range []string{"concurrent", "committed-ack-loss", "etag-mismatch", "unready-restore"} {
			t.Run(adapter+"/"+variant, func(t *testing.T) {
				h := newMCPDurableComposition(t)
				server := "work-" + adapter
				for replica := 0; replica < 2; replica++ {
					initial := h.action(map[string]any{"kind": "discover", "adapter": adapter, "replica": replica})
					var d struct {
						OK       bool
						Response struct{ ManifestETag string }
					}
					if err := json.Unmarshal(initial, &d); err != nil {
						t.Fatal(err)
					}
					_, etag := mcpDurableExpectedManifest(t, "v1")
					if !d.OK || d.Response.ManifestETag != etag {
						t.Fatalf("actual SDK baseline=%s", initial)
					}
				}
				baseTools, baseETag := mcpDurableExpectedManifest(t, "v1")
				readiness, diagnostic := "ready", any(nil)
				version := "v2"
				if variant == "unready-restore" {
					readiness, diagnostic, version = "unready", mcpmanifest.DiagnosticDiscoveryUnavailable, "v1"
				}
				if _, err := h.admin.Exec(`INSERT INTO session_mcp_manifests(workspace_id,session_id,mcp_server_name,tools_json,manifest_etag,manifest_generation,readiness,diagnostic,created_at,updated_at)VALUES('default','sesn_mcp_durable',$1,$2,$3,7,$4,$5,now(),now())`, server, baseTools, baseETag, readiness, diagnostic); err != nil {
					t.Fatal(err)
				}
				entered := make(chan struct{}, 2)
				release := make(chan struct{})
				released := false
				defer func() {
					if !released {
						close(release)
					}
				}()
				h.bridge.mu.Lock()
				h.bridge.manifestBarrier = func(ctx context.Context, _ *bridgev1.McpManifestChangedRequest) error {
					entered <- struct{}{}
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				h.bridge.dropManifestACK = variant == "committed-ack-loss"
				h.bridge.mu.Unlock()
				verificationBarrier := make(chan struct{})
				verificationResponses := make(chan struct{}, 2)
				verificationReleased := false
				defer func() {
					if !verificationReleased {
						close(verificationBarrier)
					}
				}()
				delegate := h.store.MCPManifestLister
				h.store.MCPManifestLister = mcpDurableListerFunc(func(ctx context.Context, request mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
					result, err := delegate.ListMCPTools(ctx, request)
					if err != nil {
						return result, err
					}
					verificationResponses <- struct{}{}
					select {
					case <-verificationBarrier:
						return result, nil
					case <-ctx.Done():
						return mcpmanifest.ListResult{}, ctx.Err()
					}
				})
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "version": version})
				h.action(map[string]any{"kind": "reset"})
				h.action(map[string]any{"kind": "notify", "adapter": adapter, "expectedStreams": 2})
				for i := 0; i < 2; i++ {
					select {
					case <-entered:
					case <-h.ctx.Done():
						t.Fatal(h.ctx.Err())
					case <-time.After(5 * time.Second):
						t.Fatal("two actual SDK notifications did not reach Bridge barrier")
					}
				}
				// No SQL acceptance or ACK substitution occurs before the actual owner.
				h.assertManifest(server, baseTools, baseETag, 7, readiness, 0)
				if variant == "etag-mismatch" {
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "version": "removed"})
				}
				close(release)
				released = true
				if variant != "unready-restore" {
					for i := 0; i < 2; i++ {
						select {
						case <-verificationResponses:
						case <-h.ctx.Done():
							t.Fatal(h.ctx.Err())
						case <-time.After(5 * time.Second):
							t.Fatal("two actual Bridge verification RPC responses did not reach acceptance barrier")
						}
					}
					h.assertManifest(server, baseTools, baseETag, 7, readiness, 0)
				}
				close(verificationBarrier)
				verificationReleased = true
				wantOutcomes := 2
				if variant == "committed-ack-loss" {
					wantOutcomes = 3
				}
				h.awaitManifestOutcomes(wantOutcomes)
				outcomes := h.bridge.manifestResults()
				wantTools, wantETag := mcpDurableExpectedManifest(t, version)
				wantGen, wantJobs := int64(8), 1
				if variant == "etag-mismatch" {
					wantTools, wantETag, wantGen, wantJobs = baseTools, baseETag, 7, 0
					for _, o := range outcomes {
						if o.Outcome != "rejected" || o.Code != "FailedPrecondition" {
							t.Fatalf("mismatched notice accepted: %+v", outcomes)
						}
					}
				} else {
					committed, duplicate := 0, 0
					for _, o := range outcomes {
						if o.Outcome == "committed" {
							committed++
						}
						if o.Outcome == "duplicate" {
							duplicate++
						}
						if o.ETag != wantETag {
							t.Fatalf("accepted wrong notice identity: %+v", outcomes)
						}
					}
					if committed != 1 || duplicate != wantOutcomes-1 {
						t.Fatalf("notification outcomes=%+v", outcomes)
					}
				}
				h.assertManifest(server, wantTools, wantETag, wantGen, "ready", wantJobs)
				observed := h.action(map[string]any{"kind": "observe", "adapter": adapter})
				var proof struct {
					Counts   struct{ Initialize, List, Call, Effects, Notifications int }
					Requests []struct{ Origin, Method string }
				}
				if err := json.Unmarshal(observed, &proof); err != nil {
					t.Fatal(err)
				}
				origins, verification := 0, 0
				for _, r := range proof.Requests {
					if r.Method == "tools/list" {
						if r.Origin == "sdk-notification" {
							origins++
						} else if r.Origin == "bridge-verification" {
							verification++
						} else {
							t.Fatalf("unidentified notification list origin: %+v", r)
						}
					}
				}
				wantVerification := 2
				if variant == "unready-restore" {
					wantVerification = 0
				}
				if proof.Counts.Initialize != 0 || proof.Counts.Call != 0 || proof.Counts.Effects != 0 || proof.Counts.Notifications != 1 || origins != 2 || proof.Counts.List != origins+verification || verification != wantVerification {
					t.Fatalf("notification phase/count oracle=%s", observed)
				}
				h.evidence("manifest-notification", adapter+"/"+variant, observed, map[string]any{"actual_sdk_notifications": 2, "before_acceptance_generation": 7, "generation": wantGen, "runtime_config_jobs": wantJobs, "tools_json": json.RawMessage(wantTools), "etag": wantETag, "bridge_outcomes": outcomes, "origin_L": origins, "verification_L": verification, "committed_ack_dropped_after_sql": variant == "committed-ack-loss"})
				if variant == "concurrent" || variant == "committed-ack-loss" {
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "result": "wrong-type"})
					h.action(map[string]any{"kind": "reset"})
					for replica := 0; replica < 2; replica++ {
						nonce := fmt.Sprintf("notification-metadata-%s-%s-%d", adapter, variant, replica)
						event, call := h.declareTool(server, "read_extra", nonce, "allow")
						actual := h.action(map[string]any{"kind": "execute", "adapter": adapter, "replica": replica, "eventId": event, "callId": call, "nonce": nonce, "toolName": "read_extra"})
						h.assertRejectedMCPResult(event, actual)
						var metadataProof struct {
							Counts   struct{ Initialize, List, Call, Effects int }
							Requests []struct{ Method, Tool, Origin string }
						}
						if err := json.Unmarshal(actual, &metadataProof); err != nil {
							t.Fatal(err)
						}
						if metadataProof.Counts.Initialize != 0 || metadataProof.Counts.List != 0 || metadataProof.Counts.Call != replica+1 || metadataProof.Counts.Effects != replica+1 {
							t.Fatalf("SDK metadata proof rebuilt or repeated call: %s", actual)
						}
						for _, r := range metadataProof.Requests {
							if r.Method != "tools/call" || r.Tool != "read_extra" || r.Origin != "origin-execution" {
								t.Fatalf("metadata proof trace=%s", actual)
							}
						}
						h.evidence("notification-sdk-metadata", fmt.Sprintf("%s/%s/replica%d", adapter, variant, replica), actual, map[string]any{"tool_use_event_id": event, "sdk_rejected_new_tool_malformed_output": true, "public_error": 1, "settlement_receipt": 1, "status": 2, "error_kind": 2, "no_refresh": true})
					}
				}

			})
		}
	}
}

func (h *mcpDurableComposition) awaitManifestOutcomes(want int) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		actual := len(h.bridge.manifestResults())
		if actual >= want {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("manifest callbacks=%d want%d", actual, want)
		}
		select {
		case <-h.ctx.Done():
			h.t.Fatal(h.ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

type mcpDurableListerFunc func(context.Context, mcpmanifest.ListRequest) (mcpmanifest.ListResult, error)

func (f mcpDurableListerFunc) ListMCPTools(ctx context.Context, r mcpmanifest.ListRequest) (mcpmanifest.ListResult, error) {
	return f(ctx, r)
}
