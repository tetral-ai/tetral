package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/encryption"
)

func TestPostgreSQLMCPOAuthRefreshSharesCredentialRotation(t *testing.T) {
	rows := []struct {
		name                                       string
		listing, warm, success, twoPages, deadline bool
		initialization, list, calls                []int
		f, i, l, c                                 int
	}{
		{name: "cold-discovery-init401-list401", listing: true, success: true, initialization: []int{401, 0, 0}, list: []int{401, 0}, f: 2, i: 3, l: 2},
		{name: "cold-call-init401-call401", success: true, initialization: []int{401, 0, 0}, calls: []int{401, 0}, f: 2, i: 3, l: 2, c: 2},
		{name: "cold-call-list401-call401", list: []int{401, 0}, calls: []int{401}, f: 1, i: 2, l: 2, c: 1},
		{name: "cold-call-init401-list401-call401", initialization: []int{401, 0, 0}, list: []int{401, 0}, calls: []int{401}, f: 2, i: 3, l: 2, c: 1},
		{name: "call401-rebuild-init401", initialization: []int{0, 401}, calls: []int{401}, f: 1, i: 2, l: 1, c: 1},
		{name: "call401-rebuild-list401", list: []int{0, 401}, calls: []int{401}, f: 1, i: 2, l: 2, c: 1},
		{name: "init401-retry-init401", initialization: []int{401, 401}, f: 1, i: 2},
		{name: "warm-call401-retry", warm: true, success: true, calls: []int{401, 0}, f: 1, i: 1, l: 1, c: 2},
		{name: "warm-discovery-list401-retry", warm: true, listing: true, success: true, list: []int{401, 0}, f: 1, i: 1, l: 2},
		{name: "second-page401-both-attempts", twoPages: true, list: []int{0, 401, 0, 401}, f: 1, i: 2, l: 4},
		{name: "call401-refresh-response-deadline", deadline: true, calls: []int{401}, f: 1, i: 1, l: 1, c: 1},
	}
	for _, adapter := range []string{"github", "slack"} {
		for _, row := range rows {
			t.Run(adapter+"/"+row.name, func(t *testing.T) {
				var options map[string]any
				if row.deadline {
					options = map[string]any{"executionTimeoutMs": 1000}
				}
				h := newMCPDurableCompositionWithOptions(t, options)
				before := h.seedMCPOAuth(adapter, false)
				if row.deadline {
					h.action(map[string]any{"kind": "hold", "adapter": adapter, "holdPhase": "issuer"})
				}
				if row.warm {
					h.action(map[string]any{"kind": "list", "adapter": adapter})
				}
				if row.twoPages {
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "twoPages": true})
				}
				h.action(map[string]any{"kind": "reset"})
				faultOrigin := "origin-execution"
				if row.listing {
					faultOrigin = "origin-discovery"
				}
				for method, queue := range map[string][]int{"initialize": row.initialization, "tools/list": row.list, "tools/call": row.calls} {
					h.action(map[string]any{"kind": "configure", "adapter": adapter, "faultMethod": method, "faultOrigin": faultOrigin, "faults": queue})
				}
				nonce := "oauth-" + adapter + "-" + row.name
				var event, call string
				action := map[string]any{"kind": "oauth-list", "adapter": adapter}
				if !row.listing {
					event, call = h.declare("work-"+adapter, nonce, "allow")
					action = map[string]any{"kind": "execute", "adapter": adapter, "eventId": event, "callId": call, "nonce": nonce}
				}
				output := h.action(action)
				var proof struct {
					OK     bool
					Code   string
					Result struct {
						Type   string
						Output struct{ Text string }
					}
					IssuerCalls     int
					IssuerCancelled int
					IssuerRecords   []struct {
						Adapter                      string
						ValidForm, ClientSecretBasic bool
					}
					Counts   struct{ Initialize, List, Call, Effects int }
					Requests []struct {
						Method, Origin, Session, CredentialLabel string
						Cursor                                   *string
					}
					Records []map[string]any
				}
				if err := json.Unmarshal(output, &proof); err != nil {
					t.Fatal(err)
				}
				originI, originL, originC, verificationL := 0, 0, 0, 0
				for _, request := range proof.Requests {
					if request.Origin == "origin-execution" || request.Origin == "origin-discovery" {
						switch request.Method {
						case "initialize":
							originI++
						case "tools/list":
							originL++
						case "tools/call":
							originC++
						}
					} else if request.Origin == "bridge-verification" && request.Method == "tools/list" {
						verificationL++
					} else {
						t.Fatalf("unidentified OAuth trace=%s", output)
					}
					if request.Method != "initialize" && request.Session == "missing-session" {
						t.Fatalf("rebuilt HTTP transport omitted session: %s", output)
					}
					if request.CredentialLabel == "unrecognized" {
						t.Fatalf("OAuth scope/rotation token mismatch: %s", output)
					}
				}
				if row.twoPages {
					initialSessions := []string{}
					pageCursors := []string{}
					for _, r := range proof.Requests {
						if r.Origin != faultOrigin {
							continue
						}
						if r.Method == "initialize" {
							initialSessions = append(initialSessions, r.Session)
						}
						if r.Method == "tools/list" {
							if r.Cursor == nil {
								pageCursors = append(pageCursors, "root")
							} else {
								pageCursors = append(pageCursors, *r.Cursor)
							}
						}
					}
					if len(initialSessions) != 2 || initialSessions[0] == initialSessions[1] || strings.Join(pageCursors, ",") != "root,page-two,root,page-two" {
						t.Fatalf("pagination did not restart with a new SDK session/root cursor: %s", output)
					}
				}
				if proof.IssuerCalls != row.f || originI != row.i || originL != row.l || originC != row.c || proof.Counts.List != originL+verificationL {
					t.Fatalf("literal OAuth F/I/L/C=%d/%d/%d/%d want %d/%d/%d/%d verification L %d: %s", proof.IssuerCalls, originI, originL, originC, row.f, row.i, row.l, row.c, verificationL, output)
				}
				effects := 0
				if row.success && !row.listing {
					effects = 1
				}
				if proof.Counts.Effects != effects {
					t.Fatalf("OAuth effect count=%s", output)
				}
				if row.listing {
					if proof.OK != row.success {
						t.Fatalf("OAuth discovery outcome=%s", output)
					}
					if !row.success && proof.Code != "mcp_authentication_failed" {
						t.Fatalf("unexpected discovery error: %s", output)
					}
				} else if row.success {
					if proof.Result.Type != "completed" || proof.Result.Output.Text != fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce) {
						t.Fatalf("OAuth Runtime literal=%s", output)
					}
				} else if proof.Result.Type != "error" {
					t.Fatalf("repeated unauthorized operation completed: %s", output)
				}
				for _, issuer := range proof.IssuerRecords {
					if issuer.Adapter != adapter || !issuer.ValidForm || issuer.ClientSecretBasic != (adapter == "slack") {
						t.Fatalf("actual OAuth form/Basic mismatch: %+v", issuer)
					}
				}
				remaining := h.action(map[string]any{"kind": "observe", "adapter": adapter})
				var triggers struct{ RemainingFaults map[string]int }
				if err := json.Unmarshal(remaining, &triggers); err != nil {
					t.Fatal(err)
				}
				for method, count := range triggers.RemainingFaults {
					if count != 0 {
						t.Fatalf("planned fault trigger was not reached: method/origin=%q remaining=%d", method, count)
					}
				}
				if row.deadline {
					h.action(map[string]any{"kind": "wait-held", "adapter": adapter, "holdPhase": "issuer"})
					h.action(map[string]any{"kind": "wait-issuer-cancelled", "adapter": adapter})
					var after []byte
					if err := h.admin.QueryRow(`SELECT encrypted_auth FROM credentials WHERE workspace_id='default' AND id=$1`, "cred_mcp_"+adapter).Scan(&after); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(before, after) {
						t.Fatal("deadline-aborted issuer changed encrypted credential")
					}
					h.action(map[string]any{"kind": "release", "adapter": adapter, "holdPhase": "issuer"})
				} else {
					h.assertMCPOAuthRotation(adapter, before, row.f)
				}
				logs, _ := json.Marshal(proof.Records)
				issuerLogs, _ := json.Marshal(proof.IssuerRecords)
				h.assertNoMCPOAuthSecrets(adapter, string(logs)+string(issuerLogs))
				if !row.listing {
					wantStatus, wantKind, wantText := 3, 4, "MCP authentication failed after refresh."
					if row.deadline {
						wantStatus, wantKind, wantText = 2, 5, "MCP tool call timed out."
					}
					if row.success {
						wantStatus, wantKind, wantText = 1, 0, fmt.Sprintf(`{"ok":true,"source":%q,"nonce":%q}`, adapter+"-fixture", nonce)
					}
					h.assertMCPOriginalSettlement(event, output, wantStatus, wantKind, wantText)
					action["replica"] = 1
					replay := h.action(action)
					var a, b struct {
						Result      json.RawMessage
						IssuerCalls int
						Counts      struct{ Call, Effects int }
					}
					if err := json.Unmarshal(output, &a); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(replay, &b); err != nil {
						t.Fatal(err)
					}
					if string(a.Result) != string(b.Result) || b.IssuerCalls != row.f || b.Counts.Call != row.c || b.Counts.Effects != effects {
						t.Fatalf("OAuth durable replay repeated issuer/call: %s", replay)
					}
				}
				h.evidence("oauth-refresh", adapter+"/"+row.name, output, map[string]any{"origin_F": row.f, "origin_I": row.i, "origin_L": row.l, "origin_C": row.c, "verification_L": verificationL, "accepted_effects": effects, "credential_id": "cred_mcp_" + adapter, "selected_workspace": "default", "selected_vault": "vlt_mcp_durable", "encrypted_rotation_observed": !row.deadline, "issuer_form_verified": true, "issuer_observations": proof.IssuerRecords, "durable_tool_use": event, "manager_instances": 2})
			})
		}
	}
	t.Run("concurrent-owners", testMCPOAuthConcurrentOwners)
	t.Run("issuer-rollback", testMCPOAuthIssuerRollback)
	t.Run("waiter-deadlines-and-retry", testMCPOAuthWaiterDeadlinesAndRetry)
}

func (h *mcpDurableComposition) seedMCPOAuth(adapter string, expired bool) []byte {
	h.t.Helper()
	endpoint, issuer := "https://api.githubcopilot.com/mcp/", "https://github.com/login/oauth/access_token"
	authType := "none"
	if adapter == "slack" {
		endpoint, issuer, authType = "https://mcp.slack.com/mcp", "https://slack.com/api/oauth.v2.user.access", "client_secret_basic"
	}
	expiry := time.Now().Add(24 * time.Hour)
	if expired {
		expiry = time.Now().Add(-time.Hour)
	}
	refresh := map[string]any{"refresh_token": "fixture-" + adapter + "-refresh", "client_id": "fixture-client", "token_endpoint": issuer, "scope": "fixture-scope", "resource": endpoint, "token_endpoint_auth": map[string]any{"type": authType, "client_secret": "fixture-client-secret"}}
	secret, _ := json.Marshal(map[string]any{"type": "mcp_oauth", "mcp_server_url": endpoint, "access_token": "fixture-" + adapter + "-token", "expires_at": expiry.UTC().Format(time.RFC3339), "refresh": refresh})
	crypt, err := encryption.NewAES256GCMEncryptor(mcpDurableKey)
	if err != nil {
		h.t.Fatal(err)
	}
	sealed, err := crypt.Encrypt(secret)
	if err != nil {
		h.t.Fatal(err)
	}
	public, _ := json.Marshal(map[string]any{"type": "mcp_oauth", "mcp_server_url": endpoint, "expires_at": expiry.UTC().Format(time.RFC3339)})
	if _, err := h.admin.Exec(`UPDATE credentials SET auth_type='mcp_oauth',auth_public_json=$2,encrypted_auth=$3,expires_at=$4 WHERE workspace_id='default' AND id=$1`, "cred_mcp_"+adapter, string(public), sealed, expiry.UTC().Format(time.RFC3339)); err != nil {
		h.t.Fatal(err)
	}
	return sealed
}
func (h *mcpDurableComposition) assertMCPOAuthRotation(adapter string, before []byte, refreshes int) {
	h.t.Helper()
	var sealed []byte
	var public string
	if err := h.admin.QueryRow(`SELECT encrypted_auth,auth_public_json FROM credentials WHERE workspace_id='default' AND id=$1`, "cred_mcp_"+adapter).Scan(&sealed, &public); err != nil {
		h.t.Fatal(err)
	}
	if bytes.Equal(before, sealed) {
		h.t.Fatal("successful issuer rotation did not change encrypted row")
	}
	if strings.Contains(public, "fixture-client-secret") || strings.Contains(public, "fixture-"+adapter+"-refresh") || strings.Contains(public, "fixture-"+adapter+"-rotated") {
		h.t.Fatal("public credential metadata exposed secret material")
	}
	crypt, err := encryption.NewAES256GCMEncryptor(mcpDurableKey)
	if err != nil {
		h.t.Fatal(err)
	}
	plain, err := crypt.Decrypt(sealed)
	if err != nil {
		h.t.Fatal(err)
	}
	var auth struct {
		AccessToken string `json:"access_token"`
		ExpiresAt   string `json:"expires_at"`
		Refresh     struct {
			RefreshToken string `json:"refresh_token"`
		}
	}
	if err := json.Unmarshal(plain, &auth); err != nil {
		h.t.Fatal(err)
	}
	parsedExpiry, err := time.Parse(time.RFC3339, auth.ExpiresAt)
	if err != nil {
		h.t.Fatal(err)
	}
	remaining := time.Until(parsedExpiry)
	if remaining < 3540*time.Second || remaining > 3660*time.Second {
		h.t.Fatal("encrypted credential expiry differs from issuer 3600-second lifetime")
	}
	h.assertNoMCPOAuthSecrets(adapter, public)
	if auth.AccessToken != fmt.Sprintf("fixture-%s-rotated-%d", adapter, refreshes) || auth.Refresh.RefreshToken != fmt.Sprintf("fixture-%s-refresh-%d", adapter, refreshes) {
		h.t.Fatal("encrypted credential did not retain literal latest issuer rotation")
	}
}

func (h *mcpDurableComposition) assertNoMCPOAuthSecrets(adapter, value string) {
	h.t.Helper()
	for _, secret := range []string{"fixture-client-secret", "fixture-" + adapter + "-token", "fixture-" + adapter + "-refresh", "fixture-" + adapter + "-rotated-"} {
		if strings.Contains(value, secret) {
			h.t.Fatal("secret material escaped into public metadata or captured logs")
		}
	}
}
func (h *mcpDurableComposition) assertMCPOriginalSettlement(event string, output json.RawMessage, status, kind int, text string) {
	h.t.Helper()
	var visible struct {
		Result struct {
			Type   string
			Output struct{ Text string }
			Error  struct {
				Message   string
				Retryable bool
			}
		}
	}
	if err := json.Unmarshal(output, &visible); err != nil {
		h.t.Fatal(err)
	}
	publicText := text
	if status == 3 {
		switch kind {
		case 4, 9:
			publicText = "MCP authorization is unavailable. Reconnect the integration and try again."
		case 3:
			publicText = "MCP tool execution is unavailable."
		default:
			publicText = "The MCP tool outcome could not be confirmed. Check the external service before retrying."
		}
	}
	if status == 1 {
		if visible.Result.Type != "completed" || visible.Result.Output.Text != text {
			h.t.Fatalf("Runtime literal success=%s", output)
		}
	} else if visible.Result.Type != "error" || visible.Result.Error.Message != publicText || visible.Result.Error.Retryable {
		h.t.Fatalf("Runtime literal failure=%s", output)
	}
	var storedStatus, storedKind, events, receipts int
	var storedText, state, payload string
	if err := h.admin.QueryRow(`SELECT (result_json::jsonb->'response'->>'status')::int,COALESCE((result_json::jsonb->'response'->>'error_kind')::int,0),result_json::jsonb->'response'->>'result_text',mcp_claim_status,(SELECT count(*) FROM session_events WHERE workspace_id=$2 AND session_id=$3 AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id=$2 AND session_id=$3 AND operation='settle_tool_result' AND idempotency_key=$1 AND ack_status='committed') FROM session_runtime_tool_results WHERE workspace_id=$2 AND session_id=$3 AND tool_use_event_id=$1`, event, h.scope.WorkspaceId, h.scope.SessionId).Scan(&storedStatus, &storedKind, &storedText, &state, &events, &receipts); err != nil {
		h.t.Fatal(err)
	}
	wantState := "stored"
	if status == 1 {
		wantState = "consumed"
	}
	if storedStatus != status || storedKind != kind || storedText != text || state != wantState || events != 1 || receipts != 1 {
		h.t.Fatalf("original durable settlement=(%d,%d,%q,%s,%d,%d)", storedStatus, storedKind, storedText, state, events, receipts)
	}
	if err := h.admin.QueryRow(`SELECT payload_json FROM session_events WHERE workspace_id=$2 AND session_id=$3 AND type='agent.mcp_tool_result' AND payload_json::jsonb->>'mcp_tool_use_id'=$1`, event, h.scope.WorkspaceId, h.scope.SessionId).Scan(&payload); err != nil {
		h.t.Fatal(err)
	}
	var public struct {
		ToolUseID string `json:"mcp_tool_use_id"`
		IsError   bool   `json:"is_error"`
		Content   []struct{ Type, Text string }
	}
	if err := json.Unmarshal([]byte(payload), &public); err != nil {
		h.t.Fatal(err)
	}
	if public.ToolUseID != event || public.IsError != (status != 1) || len(public.Content) != 1 || public.Content[0].Type != "text" || public.Content[0].Text != publicText {
		h.t.Fatalf("original public literal payload=%s", payload)
	}
}
