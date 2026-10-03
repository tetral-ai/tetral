package agentruntimebridge

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/encryption"
	"github.com/tetral-ai/tetral/internal/mcpmanifest"
)

func TestPostgreSQLMCPServerResolutionPreservesCredentialScope(t *testing.T) {
	h := newMCPDurableComposition(t)
	seedBridgeAPISession(t, h.admin, "default", "sesn_mcp_other", "thr_mcp_other")
	if _, err := h.admin.Exec(`UPDATE sessions SET installed_tools_json=(SELECT installed_tools_json FROM sessions WHERE workspace_id='default' AND id='sesn_mcp_durable'),vault_ids_json='[]' WHERE workspace_id='default' AND id='sesn_mcp_other'`); err != nil {
		t.Fatal(err)
	}
	seedBridgeAPISession(t, h.admin, "workspace_mcp_other", "sesn_mcp_w2", "thr_mcp_w2")
	seedBridgeAPIRuntimeBinding(t, h.admin, "default", "sesn_mcp_other", "bind_mcp_other", 1, "pod_mcp_durable")
	otherScope := bridgeAPIScope("sesn_mcp_other", "thr_mcp_other", "bind_mcp_other", 1, "pod_mcp_durable")
	seedBridgeAPIRequestStart(t, h.store, otherScope, "rwrite_mcp_other_start", "mreq_mcp_durable", "agent_provider_request", 0)
	for _, identity := range []struct{ workspace, session, vault, credential, adapter string }{
		{"default", "sesn_mcp_other", "vlt_mcp_s2", "cred_mcp_s2_slack", "slack"},
		{"workspace_mcp_other", "sesn_mcp_w2", "vlt_mcp_w2", "cred_mcp_w2_github", "github"},
	} {
		if _, err := h.admin.Exec(`INSERT INTO vaults(workspace_id,id,display_name,metadata_json,created_at,updated_at) VALUES($1,$2,$2,'{}',now(),now())`, identity.workspace, identity.vault); err != nil {
			t.Fatal(err)
		}
		if _, err := h.admin.Exec(`UPDATE sessions SET installed_tools_json=(SELECT installed_tools_json FROM sessions WHERE workspace_id='default' AND id='sesn_mcp_durable'),vault_ids_json=$3 WHERE workspace_id=$1 AND id=$2`, identity.workspace, identity.session, `["`+identity.vault+`"]`); err != nil {
			t.Fatal(err)
		}
		endpoint := "https://api.githubcopilot.com/mcp/"
		if identity.adapter == "slack" {
			endpoint = "https://mcp.slack.com/mcp"
		}
		token := "fixture-" + identity.credential + "-token"
		encryptor, err := encryption.NewAES256GCMEncryptor(mcpDurableKey)
		if err != nil {
			t.Fatal(err)
		}
		auth, _ := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint, "token": token})
		encrypted, err := encryptor.Encrypt(auth)
		if err != nil {
			t.Fatal(err)
		}
		public, _ := json.Marshal(map[string]string{"type": "static_bearer", "mcp_server_url": endpoint})
		if _, err := h.admin.Exec(`INSERT INTO credentials(workspace_id,id,vault_id,display_name,metadata_json,auth_type,auth_public_json,mcp_server_url,encrypted_auth,created_at,updated_at) VALUES($1,$2,$3,$2,'{}','static_bearer',$4,$5,$6,now(),now())`, identity.workspace, identity.credential, identity.vault, string(public), endpoint, encrypted); err != nil {
			t.Fatal(err)
		}
		h.action(map[string]any{"kind": "configure", "adapter": identity.adapter, "credentialToken": token, "credentialLabel": identity.credential})
		positive := h.action(map[string]any{"kind": "discover", "adapter": identity.adapter, "workspaceId": identity.workspace, "sessionId": identity.session, "serverName": "work-" + identity.adapter})
		var proof struct {
			OK       bool                               `json:"ok"`
			Requests []struct{ CredentialLabel string } `json:"requests"`
		}
		if err := json.Unmarshal(positive, &proof); err != nil {
			t.Fatal(err)
		}
		if !proof.OK {
			t.Fatalf("independent scoped positive discovery=%s", positive)
		}
		found := false
		for _, request := range proof.Requests {
			if request.CredentialLabel == identity.credential {
				found = true
			}
		}
		if !found {
			t.Fatalf("scoped positive failed to select its own credential: %s", positive)
		}
	}
	before := mcpCredentialCiphertexts(t, h)
	h.seedFault(`INSERT INTO vaults(workspace_id,id,display_name,metadata_json,created_at,updated_at) VALUES('default','vlt_mcp_ambiguous','Ambiguity fixture','{}',now(),now())`)
	for _, variant := range []string{"cross-workspace", "session-vault-isolation", "revoked", "archived", "zero-match", "ambiguous", "unsupported-installed-endpoint"} {
		t.Run(variant, func(t *testing.T) {
			originalScope := h.scope
			if variant == "session-vault-isolation" {
				h.scope = otherScope
			}
			defer func() { h.scope = originalScope }()
			nonce := "invalid-selector-" + variant
			event, call := h.declare("work-github", nonce, "allow")
			action := map[string]any{"kind": "resolve", "serverName": "work-github"}
			want := "credential_required"
			switch variant {
			case "cross-workspace":
				action["workspaceId"] = "workspace_mcp_other"
				want = "server_unavailable"
			case "session-vault-isolation":
				action["sessionId"] = "sesn_mcp_other"
			case "revoked":
				h.seedFault(`UPDATE credentials SET revoked_at=now() WHERE workspace_id='default' AND id='cred_mcp_github'`)
			case "archived":
				h.seedFault(`UPDATE credentials SET archived_at=now() WHERE workspace_id='default' AND id='cred_mcp_github'`)
			case "zero-match":
				h.seedFault(`UPDATE sessions SET vault_ids_json='[]' WHERE workspace_id='default' AND id='sesn_mcp_durable'`)
			case "ambiguous":
				want = "ambiguous"
				h.seedFault(`INSERT INTO credentials(workspace_id,id,vault_id,display_name,metadata_json,auth_type,auth_public_json,mcp_server_url,encrypted_auth,created_at,updated_at) SELECT workspace_id,'cred_mcp_ambiguous','vlt_mcp_ambiguous',display_name,metadata_json,auth_type,auth_public_json,mcp_server_url,encrypted_auth,created_at,updated_at FROM credentials WHERE workspace_id='default' AND id='cred_mcp_github'`)
				h.seedFault(`UPDATE sessions SET vault_ids_json='["vlt_mcp_durable","vlt_mcp_ambiguous"]' WHERE workspace_id='default' AND id='sesn_mcp_durable'`)
			case "unsupported-installed-endpoint":
				want = "server_unavailable"
				h.seedFault(`UPDATE sessions SET installed_tools_json=replace(installed_tools_json,'https://api.githubcopilot.com/mcp/','https://unsupported.example/mcp') WHERE workspace_id='default' AND id='sesn_mcp_durable'`)
			}
			output := h.action(action)
			var rejected struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(output, &rejected); err != nil {
				t.Fatal(err)
			}
			if rejected.OK || rejected.Error != want {
				t.Fatalf("%s resolution=%s; want %s", variant, output, want)
			}
			h.action(map[string]any{"kind": "reset"})
			discovery := map[string]any{}
			for key, value := range action {
				discovery[key] = value
			}
			discovery["kind"] = "discover"
			actual := h.action(discovery)
			var negative struct {
				OK     bool                                                     `json:"ok"`
				Code   int                                                      `json:"code"`
				Counts map[string]struct{ Initialize, List, Call, Effects int } `json:"counts"`
			}
			if err := json.Unmarshal(actual, &negative); err != nil {
				t.Fatal(err)
			}
			if negative.OK || negative.Code == 0 {
				t.Fatalf("invalid selector reached successful real ListMcpTools: %s", actual)
			}
			for endpoint, counts := range negative.Counts {
				if counts.Initialize != 0 || counts.List != 0 || counts.Call != 0 || counts.Effects != 0 {
					t.Fatalf("%s reached %s external endpoint: %s", variant, endpoint, actual)
				}
			}
			execution := map[string]any{"kind": "execute", "adapter": "github", "eventId": event, "callId": call, "nonce": nonce}
			if variant == "session-vault-isolation" {
				execution["sessionId"] = otherScope.SessionId
				execution["threadId"] = otherScope.SessionThreadId
				execution["bindingId"] = otherScope.Binding.BindingId
			}
			if variant == "cross-workspace" {
				execution["workspaceId"] = "workspace_mcp_other"
				execution["settle"] = false
			}
			actualExecution := h.action(execution)
			if variant == "cross-workspace" {
				var fenced struct{ Result struct{ Type string } }
				if err := json.Unmarshal(actualExecution, &fenced); err != nil {
					t.Fatal(err)
				}
				if fenced.Result.Type != "stale_custody" {
					t.Fatalf("cross-workspace custody fence=%s", actualExecution)
				}
				var rows int
				if err := h.admin.QueryRow(`SELECT count(*) FROM session_runtime_tool_results WHERE tool_use_event_id=$1`, event).Scan(&rows); err != nil {
					t.Fatal(err)
				}
				if rows != 0 {
					t.Fatal("cross-workspace fence created a durable execution/result")
				}
			} else {
				status, kind, text := 3, 9, "MCP server requires a configured credential."
				if variant == "ambiguous" {
					kind, text = 4, "MCP authentication failed after refresh."
				}
				if variant == "unsupported-installed-endpoint" {
					status, kind, text = 2, 2, "Configured MCP server is unavailable or unsupported."
				}
				h.assertMCPOriginalSettlement(event, actualExecution, status, kind, text)
				execution["replica"] = 1
				replay := h.action(execution)
				h.assertMCPOriginalSettlement(event, replay, status, kind, text)
				var a, b struct{ Result json.RawMessage }
				if err := json.Unmarshal(actualExecution, &a); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(replay, &b); err != nil {
					t.Fatal(err)
				}
				if string(a.Result) != string(b.Result) {
					t.Fatal("invalid selector durable replay changed Runtime result")
				}
			}
			for _, adapter := range []string{"github", "slack"} {
				observed := h.action(map[string]any{"kind": "observe", "adapter": adapter})
				var proof struct {
					Counts struct{ Initialize, List, Call, Effects int }
				}
				if err := json.Unmarshal(observed, &proof); err != nil {
					t.Fatal(err)
				}
				if proof.Counts.Initialize != 0 || proof.Counts.List != 0 || proof.Counts.Call != 0 || proof.Counts.Effects != 0 {
					t.Fatalf("invalid selector execution reached %s endpoint: %s", adapter, observed)
				}
			}
			h.evidence("credential-scope", variant+"/execution", actualExecution, map[string]any{"tool_use_id": event, "custody_fenced_before_claim": variant == "cross-workspace", "original_error_receipt": variant != "cross-workspace", "external_I": 0, "external_L": 0, "external_C": 0, "accepted_effects": 0, "legitimate_session_custody": variant == "session-vault-isolation"})
			immediate := mcpCredentialCiphertexts(t, h)
			for id, encrypted := range before {
				if !bytes.Equal(immediate[id], encrypted) {
					t.Fatalf("negative %s changed encrypted row %s before fixture restoration", variant, id)
				}
			}
			// Restore only the independently seeded selector/liveness fault. Neither
			// operation rewrites encrypted material.
			h.seedFault(`UPDATE credentials SET revoked_at=NULL,archived_at=NULL WHERE workspace_id='default' AND id='cred_mcp_github'`)
			h.seedFault(`DELETE FROM credentials WHERE workspace_id='default' AND id='cred_mcp_ambiguous'`)
			h.seedFault(`UPDATE sessions SET vault_ids_json='["vlt_mcp_durable"]',installed_tools_json=replace(installed_tools_json,'https://unsupported.example/mcp','https://api.githubcopilot.com/mcp/') WHERE workspace_id='default' AND id='sesn_mcp_durable'`)
			for _, adapter := range []string{"github", "slack"} {
				positive := h.action(map[string]any{"kind": "resolve", "serverName": "work-" + adapter})
				var accepted struct {
					OK           bool   `json:"ok"`
					CredentialID string `json:"credentialId"`
					VaultID      string `json:"vaultId"`
				}
				if err := json.Unmarshal(positive, &accepted); err != nil {
					t.Fatal(err)
				}
				if !accepted.OK || accepted.CredentialID != "cred_mcp_"+adapter || accepted.VaultID != "vlt_mcp_durable" {
					t.Fatalf("%s positive control=%s", adapter, positive)
				}
				positiveDiscovery := h.action(map[string]any{"kind": "discover", "adapter": adapter, "serverName": "work-" + adapter})
				var actualPositive struct {
					OK bool `json:"ok"`
				}
				if err := json.Unmarshal(positiveDiscovery, &actualPositive); err != nil {
					t.Fatal(err)
				}
				if !actualPositive.OK {
					t.Fatalf("actual %s discovery positive=%s", adapter, positiveDiscovery)
				}
				h.evidence("credential-scope", variant+"/positive-"+adapter, positiveDiscovery, map[string]any{"credential_id": accepted.CredentialID, "vault_id": accepted.VaultID, "positive_discovery": true})
			}
			after := mcpCredentialCiphertexts(t, h)
			for id, encrypted := range before {
				if !bytes.Equal(after[id], encrypted) {
					t.Fatalf("%s changed encrypted row %s", variant, id)
				}
			}
			t.Logf("case=credential-scope variant=%s boundary=actual-list-mcp-tools,installed-gateway-sql negative=%s grpc_code=%d external_I/L/C=0/0/0 positive=both-adapters encrypted_rows=unchanged-before-restoration", variant, want, negative.Code)
			h.evidence("credential-scope", variant, actual, map[string]any{"negative": want, "grpc_code": negative.Code, "positive_controls": "both-adapters", "encrypted_rows": "unchanged-before-restoration", "workspace_session_vault_controls": true})
		})
	}
	// Shared Go discovery retains the installed configured names. Unsupported
	// references reach the owning SQL resolver rather than disappearing here.
	var names []string
	err := h.store.Client.WithWorkspaceTx(h.ctx, "default", "agentruntimebridge.test_mcp_toolsets", func(tx *dbconnect.Tx) error {
		toolsets, err := mcpmanifest.SessionToolsetsTx(h.ctx, tx, "default", "sesn_mcp_durable")
		for _, toolset := range toolsets {
			names = append(names, toolset.MCPServerName)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "work-github" || names[1] != "work-slack" {
		t.Fatalf("installed toolsets=%v", names)
	}
}

func (h *mcpDurableComposition) seedFault(query string) {
	h.t.Helper()
	if _, err := h.admin.Exec(query); err != nil {
		h.t.Fatal(err)
	}
}
func mcpCredentialCiphertexts(t *testing.T, h *mcpDurableComposition) map[string][]byte {
	t.Helper()
	rows, err := h.admin.Query(`SELECT workspace_id||'/'||id,encrypted_auth FROM credentials ORDER BY workspace_id,id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			h.t.Errorf("close credential observation rows: %v", err)
		}
	}()
	result := map[string][]byte{}
	for rows.Next() {
		var id string
		var encrypted []byte
		if err := rows.Scan(&id, &encrypted); err != nil {
			t.Fatal(err)
		}
		result[id] = encrypted
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}
