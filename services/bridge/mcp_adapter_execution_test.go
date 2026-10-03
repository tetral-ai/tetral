package agentruntimebridge

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestPostgreSQLMCPAdaptersExecuteThroughDurableClaims(t *testing.T) {
	h := newMCPDurableComposition(t)
	for _, adapter := range []string{"github", "slack"} {
		t.Run(adapter, func(t *testing.T) {
			nonce := "nonce-" + adapter
			server := "work-" + adapter
			eventID, callID := h.declare(server, nonce, "allow")
			action := map[string]any{"kind": "execute", "adapter": adapter, "serverName": server, "eventId": eventID, "callId": callID, "nonce": nonce, "settle": false}
			first := h.action(action)
			var proof struct {
				Result struct {
					Type   string `json:"type"`
					Output struct {
						Text string `json:"text"`
					} `json:"output"`
				} `json:"result"`
				Counts   struct{ Initialize, List, Call, Effects int }                       `json:"counts"`
				Requests []struct{ Method, Session, CredentialLabel, Toolset, Nonce string } `json:"requests"`
			}
			if err := json.Unmarshal(first, &proof); err != nil {
				t.Fatal(err)
			}
			expected := fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce)
			if proof.Result.Type != "completed" || proof.Result.Output.Text != expected || proof.Counts.Call != 1 || proof.Counts.Effects != 1 {
				t.Fatalf("actual SDK adapter execution=%s; want exact %s and one effect", first, expected)
			}
			for _, request := range proof.Requests {
				if request.CredentialLabel != adapter+"-original" {
					t.Fatalf("wrong credential label: %+v", request)
				}
				if adapter == "github" && request.Toolset != "default,actions" {
					t.Fatalf("GitHub selection missing: %+v", request)
				}
				if adapter == "slack" && request.Toolset != "" {
					t.Fatalf("Slack must omit GitHub selection: %+v", request)
				}
				if request.Method == "tools/call" && request.Nonce != nonce {
					t.Fatalf("external call did not retain original nonce: %+v", request)
				}
			}
			action["replica"] = 1
			action["settle"] = true
			replay := h.action(action)
			replayProof := proof
			if err := json.Unmarshal(replay, &replayProof); err != nil {
				t.Fatal(err)
			}
			if replayProof.Result != proof.Result || replayProof.Counts.Call != 1 || replayProof.Counts.Effects != 1 {
				t.Fatalf("other Connector replay repeated external effect: %s", replay)
			}
			var rows int
			var state, result, toolName, inputJSON, projectionJSON string
			if err := h.admin.QueryRow(`SELECT count(*),max(r.mcp_claim_status),max(r.result_json),max(r.tool_name),max(r.input_json),max(e.projection_json)
			 FROM session_runtime_tool_results r JOIN session_events e ON e.workspace_id=r.workspace_id AND e.session_id=r.session_id AND e.event_id=r.tool_use_event_id
			 WHERE r.workspace_id='default' AND r.session_id='sesn_mcp_durable' AND r.tool_use_event_id=$1`, eventID).Scan(&rows, &state, &result, &toolName, &inputJSON, &projectionJSON); err != nil {
				t.Fatal(err)
			}
			if rows != 1 || state != "consumed" {
				t.Fatalf("durable result rows/state=%d/%s", rows, state)
			}
			var stored struct {
				Response struct {
					Status     int    `json:"status"`
					ResultText string `json:"result_text"`
					ErrorKind  int    `json:"error_kind"`
				} `json:"response"`
				RefreshTriggered bool `json:"refresh_triggered"`
			}
			var projection struct {
				MCPServerName   string `json:"mcp_server_name"`
				ModelToolCallID string `json:"model_tool_call_id"`
				ToolName        string `json:"tool_name"`
			}
			if err := json.Unmarshal([]byte(result), &stored); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil {
				t.Fatal(err)
			}
			if stored.Response.Status != 1 || stored.Response.ResultText != expected || stored.Response.ErrorKind != 0 || stored.RefreshTriggered || toolName != server+"/read_echo" || inputJSON != fmt.Sprintf(`{"nonce":%q}`, nonce) || projection.MCPServerName != server || projection.ModelToolCallID != callID || projection.ToolName != "read_echo" {
				t.Fatalf("independent durable identity/result mismatch: result=%s tool=%s input=%s projection=%s", result, toolName, inputJSON, projectionJSON)
			}
			for _, variant := range []string{"invalid-binding-token", "undeclared", "denied-declaration"} {
				event, call := eventID, callID
				if variant == "undeclared" {
					event = "evt_mcp_missing_" + adapter
					call = "call_mcp_missing_" + adapter
				}
				if variant == "denied-declaration" {
					event, call = h.declare(server, "denied-"+adapter, "deny")
				}
				negative := map[string]any{"kind": "execute", "adapter": adapter, "serverName": server, "eventId": event, "callId": call, "nonce": nonce, "settle": false}
				if variant == "invalid-binding-token" {
					negative["bindingToken"] = "rtbt_v1.invalid.signature"
				}
				output := h.action(negative)
				var rejection struct {
					Result struct {
						Type string `json:"type"`
					} `json:"result"`
					Counts struct{ Call, Effects int } `json:"counts"`
				}
				if err := json.Unmarshal(output, &rejection); err != nil {
					t.Fatal(err)
				}
				if rejection.Result.Type == "completed" || rejection.Counts.Call != 1 || rejection.Counts.Effects != 1 {
					t.Fatalf("%s crossed admission fence: %s", variant, output)
				}
				if variant != "invalid-binding-token" {
					var rejectedRows int
					if err := h.admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id='default' AND session_id='sesn_mcp_durable' AND tool_use_event_id=$1`, event).Scan(&rejectedRows); err != nil {
						t.Fatal(err)
					}
					if rejectedRows != 0 {
						t.Fatalf("%s persisted a claim", variant)
					}
				}
				t.Logf("case=mixed-adapters variant=%s/%s external_calls=1 effects=1 new_traffic=0", adapter, variant)
				h.evidence("mixed-adapters", adapter+"/"+variant, output, map[string]any{"new_external_calls": 0, "admission_rejected": true, "tool_use_event_id": event, "new_claim_persisted": false})
			}
			t.Logf("case=mixed-adapters variant=%s protocol=actual-sdk-http boundaries=runtime-runner,connector-grpc,bridge-claim-commit sql_rows=1 sql_state=consumed external_calls=1 effects=1 replay_replica=2", adapter)
			h.evidence("mixed-adapters", adapter, replay, map[string]any{"rows": 1, "state": "consumed", "tool_name": toolName, "tool_use_event_id": eventID, "model_tool_call_id": callID, "canonical_input": inputJSON, "stored_result_text": stored.Response.ResultText, "replay_equals_literal": true})
		})
	}
	staleEvent, staleCall := h.declare("work-github", "stale", "allow")
	if _, err := h.admin.Exec(`UPDATE session_runtime_bindings SET binding_generation=2 WHERE workspace_id='default' AND session_id='sesn_mcp_durable'`); err != nil {
		t.Fatal(err)
	}
	stale := h.action(map[string]any{"kind": "execute", "adapter": "github", "serverName": "work-github", "eventId": staleEvent, "callId": staleCall, "nonce": "stale", "settle": false})
	var staleProof struct {
		Result struct {
			Type string `json:"type"`
		} `json:"result"`
		Counts struct{ Call, Effects int } `json:"counts"`
	}
	if err := json.Unmarshal(stale, &staleProof); err != nil {
		t.Fatal(err)
	}
	if staleProof.Result.Type != "stale_custody" || staleProof.Counts.Call != 1 || staleProof.Counts.Effects != 1 {
		t.Fatalf("stale binding crossed durable fence: %s", stale)
	}
	h.evidence("mixed-adapters", "stale-binding", stale, map[string]any{"binding_generation": 2, "old_generation_rejected": true, "new_external_calls": 0})
}
