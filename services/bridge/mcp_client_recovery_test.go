package agentruntimebridge

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/tetral-ai/tetral/internal/mcpmanifest"
)

func TestPostgreSQLMCPClientRecreationReconcilesManifest(t *testing.T) {
	for _, adapter := range []string{"github", "slack"} {
		for _, version := range []string{"v2", "v1", "output-only", "removed"} {
			t.Run(adapter+"/"+version, func(t *testing.T) {
				h := newMCPDurableComposition(t)
				server := "work-" + adapter
				initial := h.action(map[string]any{"kind": "discover", "adapter": adapter})
				var discovery struct {
					OK       bool
					Response struct {
						ManifestETag string                                                `json:"manifestEtag"`
						Tools        []struct{ Name, Description, InputSchemaJSON string } `json:"tools"`
					}
				}
				if err := json.Unmarshal(initial, &discovery); err != nil {
					t.Fatal(err)
				}
				baseTools, baseETag := mcpDurableExpectedManifest(t, "v1")
				if !discovery.OK || discovery.Response.ManifestETag != baseETag || len(discovery.Response.Tools) != 1 || discovery.Response.Tools[0].Name != "read_echo" {
					t.Fatalf("initial actual SDK projection=%s", initial)
				}
				if _, err := h.admin.Exec(`INSERT INTO session_mcp_manifests(workspace_id,session_id,mcp_server_name,tools_json,manifest_etag,manifest_generation,readiness,diagnostic,created_at,updated_at) VALUES('default','sesn_mcp_durable',$1,$2,$3,7,'ready',NULL,now(),now())`, server, baseTools, baseETag); err != nil {
					t.Fatal(err)
				}
				oldEvent, oldCall := "", ""
				if version == "removed" {
					oldEvent, oldCall = h.declare(server, "declared-before-removal", "allow")
				}
				recreation := h.action(map[string]any{"kind": "recreate"})
				var retired struct {
					Recreated                                                                                                  bool
					OwnerGeneration, OldConnections, NewConnections, RetiredConnections, PendingBefore, PendingAfterDisconnect int
				}
				if err := json.Unmarshal(recreation, &retired); err != nil {
					t.Fatal(err)
				}
				if !retired.Recreated || retired.OwnerGeneration != 2 || retired.OldConnections != 0 || retired.NewConnections != 0 || retired.RetiredConnections != 1 || retired.PendingAfterDisconnect != 0 {
					t.Fatalf("old SDK resources did not disconnect before new owner: %s", recreation)
				}
				h.action(map[string]any{"kind": "configure", "adapter": adapter, "version": version, "result": "valid"})
				h.action(map[string]any{"kind": "reset"})
				var removedProof json.RawMessage
				if version == "removed" {
					oldAction := map[string]any{"kind": "execute", "adapter": adapter, "eventId": oldEvent, "callId": oldCall, "nonce": "declared-before-removal", "toolName": "read_echo"}
					removedProof = h.action(oldAction)
					h.assertRejectedMCPResult(oldEvent, removedProof)
					oldAction["replica"] = 1
					replay := h.action(oldAction)
					var a, b struct {
						Result   json.RawMessage
						Counts   struct{ Call, Effects int }
						Requests []struct{ Method, Tool string }
					}
					if err := json.Unmarshal(removedProof, &a); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(replay, &b); err != nil {
						t.Fatal(err)
					}
					if string(a.Result) != string(b.Result) || b.Counts.Call != 1 || b.Counts.Effects != 0 {
						t.Fatalf("removed original Tool Use replay repeated call: %s", replay)
					}
					for _, r := range b.Requests {
						if r.Method == "tools/call" && r.Tool != "read_echo" {
							t.Fatalf("removed declaration rewritten: %s", replay)
						}
					}
				}
				tool := "read_echo"
				if version == "removed" {
					tool = "read_echo_v2"
				}
				nonce := "recreated-" + adapter + "-" + version
				event, call := h.declareTool(server, tool, nonce, "allow")
				execution := h.action(map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce, "toolName": tool})
				var proof struct {
					Result struct {
						Type   string
						Output struct{ Text string }
					}
					Counts   struct{ Initialize, List, Call, Effects int }
					Requests []struct{ Method, Session, Origin string }
				}
				if err := json.Unmarshal(execution, &proof); err != nil {
					t.Fatal(err)
				}
				wantCalls := 1
				if version == "removed" {
					wantCalls = 2
				}
				if proof.Counts.Initialize != 1 || proof.Counts.Call != wantCalls || proof.Counts.Effects != 1 {
					t.Fatalf("new client did not cold-initialize exactly once: %s", execution)
				}
				if version == "output-only" {
					h.assertRejectedMCPResult(event, execution)
					if proof.Result.Type != "error" {
						t.Fatalf("new SDK metadata did not validate changed output schema: %s", execution)
					}
				} else if proof.Result.Type != "completed" || proof.Result.Output.Text != fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce) {
					t.Fatalf("recreated execution literal result: %s", execution)
				}
				wantTools, wantETag := mcpDurableExpectedManifest(t, version)
				wantGeneration, wantJobs := int64(8), 1
				if version == "v1" || version == "output-only" {
					wantGeneration, wantJobs = 7, 0
				}
				originI, originL, originC, verificationL := 0, 0, 0, 0
				for _, r := range proof.Requests {
					if r.Origin == "origin-execution" {
						switch r.Method {
						case "initialize":
							originI++
						case "tools/list":
							originL++
						case "tools/call":
							originC++
						}
					} else if r.Origin == "bridge-verification" && r.Method == "tools/list" {
						verificationL++
					}
				}
				if originI != 1 || originL != 1 || originC != wantCalls || verificationL != wantJobs || proof.Counts.List != 1+wantJobs {
					t.Fatalf("recreation readiness/verification counts=%d/%d/%d/%d totalL%d: %s", originI, originL, originC, verificationL, proof.Counts.List, execution)
				}
				h.assertManifest(server, wantTools, wantETag, wantGeneration, "ready", wantJobs)
				// A second, independent new client reports the same snapshot through its
				// actual readiness notifier. Bridge accepts it as a duplicate.
				h.action(map[string]any{"kind": "recreate"})
				nonce += "-duplicate"
				event, call = h.declareTool(server, tool, nonce, "allow")
				duplicate := h.action(map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce, "toolName": tool})
				var duplicateProof struct {
					Counts struct{ Initialize, List, Call, Effects int }
				}
				if err := json.Unmarshal(duplicate, &duplicateProof); err != nil {
					t.Fatal(err)
				}
				if duplicateProof.Counts.Initialize != 2 || duplicateProof.Counts.List != 2+wantJobs || duplicateProof.Counts.Call != wantCalls+1 || duplicateProof.Counts.Effects != 2 {
					t.Fatalf("duplicate readiness caused unexpected verification or call: %s", duplicate)
				}
				if version == "output-only" {
					h.assertRejectedMCPResult(event, duplicate)
				}
				h.assertManifest(server, wantTools, wantETag, wantGeneration, "ready", wantJobs)
				h.evidence("client-recreation", adapter+"/"+version, duplicate, map[string]any{"initial_generation": 7, "generation": wantGeneration, "runtime_config_jobs": wantJobs, "tools_json": json.RawMessage(wantTools), "etag": wantETag, "old_endpoint_handlers_disconnected_before_new_owner": retired.PendingAfterDisconnect == 0, "retirement": json.RawMessage(recreation), "first_runtime_observation": json.RawMessage(execution), "duplicate_preserves_generation": true, "origin_I": originI, "origin_L": originL, "origin_C": originC, "verification_L": verificationL, "removed_original_outcome": removedProof})
			})
		}
	}
}

func mcpDurableExpectedManifest(t *testing.T, version string) (string, string) {
	t.Helper()
	schema := `{"additionalProperties":false,"properties":{"nonce":{"type":"string"}},"required":["nonce"],"type":"object"}`
	name := "read_echo"
	if version == "removed" {
		name = "read_echo_v2"
	}
	tools := []mcpmanifest.Tool{{Name: name, Description: "Echo the supplied nonce.", InputSchemaJSON: schema}}
	if version == "v2" {
		tools = append(tools, mcpmanifest.Tool{Name: "read_extra", Description: "Read an extra fixture value.", InputSchemaJSON: schema})
	}
	durable, err := mcpmanifest.CanonicalToolsJSON(tools)
	if err != nil {
		t.Fatal(err)
	}
	logical := make([]map[string]any, 0, len(tools))
	for _, tool := range tools {
		var input map[string]any
		if err := json.Unmarshal([]byte(schema), &input); err != nil {
			t.Fatal(err)
		}
		logical = append(logical, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": input})
	}
	encoded, err := json.Marshal(logical)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	return durable, hex.EncodeToString(digest[:])
}

func (h *mcpDurableComposition) assertManifest(server, tools, etag string, generation int64, readiness string, jobs int) {
	h.t.Helper()
	var actualTools, actualETag, actualReady string
	var actualGeneration int64
	var actualJobs int
	if err := h.admin.QueryRow(`SELECT tools_json,manifest_etag,manifest_generation,readiness,(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND causal_session_id='sesn_mcp_durable' AND kind='runtime_config_update' AND payload_json::jsonb->>'mcp_server_name'=$1) FROM session_mcp_manifests WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND mcp_server_name=$1`, server).Scan(&actualTools, &actualETag, &actualGeneration, &actualReady, &actualJobs); err != nil {
		h.t.Fatal(err)
	}
	if actualTools != tools || actualETag != etag || actualGeneration != generation || actualReady != readiness || actualJobs != jobs {
		h.t.Fatalf("manifest %s actual=(%s,%s,%d,%s,%d) want=(%s,%s,%d,%s,%d)", server, actualTools, actualETag, actualGeneration, actualReady, actualJobs, tools, etag, generation, readiness, jobs)
	}
}

func (h *mcpDurableComposition) assertRejectedMCPResult(event string, output json.RawMessage) {
	h.t.Helper()
	var visible struct {
		Result struct {
			Type  string
			Error struct {
				Message   string
				Retryable bool
			}
		}
	}
	if err := json.Unmarshal(output, &visible); err != nil {
		h.t.Fatal(err)
	}
	if visible.Result.Type != "error" || visible.Result.Error.Message != "MCP server rejected the arguments." || visible.Result.Error.Retryable {
		h.t.Fatalf("SDK rejection differs from literal Runtime error: %s", output)
	}
	var status, kind, publicEvents, receipts int
	var state, toolName string
	if err := h.admin.QueryRow(`SELECT (result_json::jsonb->'response'->>'status')::int,(result_json::jsonb->'response'->>'error_kind')::int,mcp_claim_status,tool_name,(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1 AND payload_json::jsonb->>'is_error'='true' AND payload_json::jsonb->'content'->0->>'text'='MCP server rejected the arguments.'),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND operation='settle_tool_result' AND idempotency_key=$1 AND ack_status='committed') FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND tool_use_event_id=$1`, event).Scan(&status, &kind, &state, &toolName, &publicEvents, &receipts); err != nil {
		h.t.Fatal(err)
	}
	if status != 2 || kind != 2 || state != "stored" || publicEvents != 1 || receipts != 1 {
		h.t.Fatalf("rejected durable result=(%d,%d,%s,%s,%d,%d)", status, kind, state, toolName, publicEvents, receipts)
	}
}
