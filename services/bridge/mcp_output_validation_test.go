package agentruntimebridge

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestPostgreSQLMCPOutputValidationSettlesRejectedResults(t *testing.T) {
	h := newMCPDurableComposition(t)
	for _, adapter := range []string{"github", "slack"} {
		for _, temperature := range []string{"cold", "warm"} {
			for _, variant := range []string{"valid", "wrong-type", "missing", "tool-error", "error-malformed", "no-schema-text", "no-schema-structured", "read-401", "read-403", "jsonrpc-401", "jsonrpc-403"} {
				t.Run(adapter+"/"+temperature+"/"+variant, func(t *testing.T) {
					server := "work-" + adapter
					h.action(map[string]any{"kind": "recreate"})
					version, result := "v1", variant
					toolName := "read_echo"
					if variant == "no-schema-structured" {
						version = "no-output"
						result = "wrong-type"
					}
					if variant == "no-schema-text" {
						version, result = "no-output", "plain-text"
					}
					if variant == "read-401" || variant == "read-403" {
						toolName, result = variant, "missing"
					}
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "version": version, "result": result, "toolName": toolName})
					if temperature == "warm" {
						h.action(map[string]any{"kind": "list", "adapter": adapter, "serverName": server})
					}
					h.action(map[string]any{"kind": "reset"})
					nonce := "nonce-" + adapter + "-" + temperature + "-" + variant
					event, call := h.declareTool(server, toolName, nonce, "allow")
					action := map[string]any{"kind": "execute", "adapter": adapter, "serverName": server, "toolName": toolName, "eventId": event, "callId": call, "nonce": nonce, "settle": false}
					first := h.action(action)
					var proof struct {
						Result   json.RawMessage                               `json:"result"`
						Counts   struct{ Call, Effects, Initialize, List int } `json:"counts"`
						Requests []struct{ Method, Origin string }             `json:"requests"`
					}
					if err := json.Unmarshal(first, &proof); err != nil {
						t.Fatal(err)
					}
					if proof.Counts.Call != 1 || proof.Counts.Effects != 1 {
						t.Fatalf("output case repeated external call: %s", first)
					}
					originI, originL, originC, verifyL := 0, 0, 0, 0
					for _, request := range proof.Requests {
						if request.Origin == "origin-execution" {
							switch request.Method {
							case "initialize":
								originI++
							case "tools/list":
								originL++
							case "tools/call":
								originC++
							}
						} else if request.Origin == "bridge-verification" && request.Method == "tools/list" {
							verifyL++
						}
					}
					wantI, wantL := 0, 0
					if temperature == "cold" {
						wantI, wantL = 1, 1
					}
					if originI != wantI || originL != wantL || originC != 1 {
						t.Fatalf("origin counts I/L/C=%d/%d/%d want=%d/%d/1; verificationL=%d trace=%s", originI, originL, originC, wantI, wantL, verifyL, first)
					}
					var storedJSON, state string
					if err := h.admin.QueryRow(`SELECT result_json,mcp_claim_status FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND tool_use_event_id=$1`, event).Scan(&storedJSON, &state); err != nil {
						t.Fatal(err)
					}
					var stored struct {
						Response struct {
							Status     int    `json:"status"`
							ResultText string `json:"result_text"`
							ErrorKind  int    `json:"error_kind"`
						} `json:"response"`
						RefreshTriggered bool `json:"refresh_triggered"`
					}
					if err := json.Unmarshal([]byte(storedJSON), &stored); err != nil {
						t.Fatal(err)
					}
					wantStatus, wantText, wantKind := 3, "MCP connector failed.", 10
					// Pinned SDK callTool validates outputSchema itself: malformed
					// structured content throws InvalidParams; absent structured
					// content on a non-error result throws InvalidRequest.
					if variant == "wrong-type" || variant == "error-malformed" {
						wantStatus, wantText, wantKind = 2, "MCP server rejected the arguments.", 2
					}
					if variant == "valid" || variant == "no-schema-structured" {
						wantStatus, wantKind = 1, 0
						ok := `true`
						if variant == "no-schema-structured" {
							ok = `"invalid"`
						}
						wantText = fmt.Sprintf(`{"ok":%s,"source":%q,"nonce":%q}`, ok, adapter+"-fixture", nonce)
					}
					if variant == "no-schema-text" {
						wantStatus, wantText, wantKind = 1, "plain tool output", 0
					}
					if variant == "tool-error" {
						wantStatus, wantKind = 2, 1
						wantText = "controlled tool error"
					}
					if state != "stored" || stored.RefreshTriggered || stored.Response.Status != wantStatus || stored.Response.ErrorKind != wantKind || stored.Response.ResultText != wantText {
						t.Fatalf("SDK output validation durable result=%s state=%s; want status=%d text=%q kind=%d refresh=false", storedJSON, state, wantStatus, wantText, wantKind)
					}
					var visible struct {
						Type   string `json:"type"`
						Output struct {
							Text string `json:"text"`
						} `json:"output"`
						Error struct {
							Message   string `json:"message"`
							Retryable bool   `json:"retryable"`
						} `json:"error"`
					}
					if err := json.Unmarshal(proof.Result, &visible); err != nil {
						t.Fatal(err)
					}
					if wantStatus == 1 {
						if visible.Type != "completed" || visible.Output.Text != wantText {
							t.Fatalf("Runtime success differs from literal validated output: %s", proof.Result)
						}
					} else {
						message := wantText
						if wantStatus == 3 {
							message = "The MCP tool outcome could not be confirmed. Check the external service before retrying."
						}
						if visible.Type != "error" || visible.Error.Message != message || visible.Error.Retryable {
							t.Fatalf("Runtime failure differs from literal expected output: %s want=%q", proof.Result, message)
						}
					}
					action["replica"] = 1
					action["settle"] = true
					replay := h.action(action)
					var replayProof struct {
						Result json.RawMessage             `json:"result"`
						Counts struct{ Call, Effects int } `json:"counts"`
					}
					if err := json.Unmarshal(replay, &replayProof); err != nil {
						t.Fatal(err)
					}
					if string(replayProof.Result) != string(proof.Result) || replayProof.Counts.Call != 1 || replayProof.Counts.Effects != 1 {
						t.Fatalf("durable error/result replay mismatch or repeat: first=%s replay=%s", first, replay)
					}
					var settled string
					if err := h.admin.QueryRow(`SELECT mcp_claim_status FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND tool_use_event_id=$1`, event).Scan(&settled); err != nil {
						t.Fatal(err)
					}
					wantState := "stored"
					if wantStatus == 1 {
						wantState = "consumed"
					}
					if settled != wantState {
						t.Fatalf("runtime durable result state=%s want=%s", settled, wantState)
					}
					var resultEvents, receipts int
					if err := h.admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1`, event).Scan(&resultEvents); err != nil {
						t.Fatal(err)
					}
					if err := h.admin.QueryRow(`SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND operation='settle_tool_result' AND idempotency_key=$1 AND ack_status='committed'`, event).Scan(&receipts); err != nil {
						t.Fatal(err)
					}
					if resultEvents != 1 || receipts != 1 {
						t.Fatalf("Runtime terminal public results/settlement receipts=%d/%d want1/1", resultEvents, receipts)
					}
					var publicJSON string
					if err := h.admin.QueryRow(`SELECT payload_json FROM session_events WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1`, event).Scan(&publicJSON); err != nil {
						t.Fatal(err)
					}
					var public struct {
						ToolUseID string                        `json:"mcp_tool_use_id"`
						IsError   bool                          `json:"is_error"`
						Content   []struct{ Type, Text string } `json:"content"`
					}
					if err := json.Unmarshal([]byte(publicJSON), &public); err != nil {
						t.Fatal(err)
					}
					publicText := wantText
					if wantStatus == 3 {
						publicText = "The MCP tool outcome could not be confirmed. Check the external service before retrying."
					}
					if public.ToolUseID != event || public.IsError != (wantStatus != 1) || len(public.Content) != 1 || public.Content[0].Type != "text" || public.Content[0].Text != publicText {
						t.Fatalf("public terminal payload does not match original Tool Use and literal Runtime outcome: %s", publicJSON)
					}
					t.Logf("case=output-validation variant=%s/%s/%s boundary=actual-sdk-http,durable-bridge origin_F/I/L/C=0/%d/%d/1 verification_L=%d effects=1 durable_status=%d error_kind=%d replay=identical row_state=%s public_terminal_results=1 settlement_receipts=1", adapter, temperature, variant, originI, originL, verifyL, wantStatus, wantKind, wantState)
					h.evidence("output-validation", adapter+"/"+temperature+"/"+variant, replay, map[string]any{"tool_use_event_id": event, "model_tool_call_id": call, "tool_name": toolName, "status": wantStatus, "error_kind": wantKind, "stored_result_text": wantText, "state": wantState, "public_terminal_results": 1, "settlement_receipts": 1, "public_payload": json.RawMessage(publicJSON), "origin_I": originI, "origin_L": originL, "origin_C": originC, "verification_L": verifyL, "refresh_triggered": false, "replay_equals_literal": true})
				})
			}
		}
	}
}
