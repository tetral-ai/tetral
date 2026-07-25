package agent

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestAgentPageTokensBindScopeWithoutEncodingSensitiveScope(t *testing.T) {
	secret := bytes.Repeat([]byte{3}, 32)
	service := &Service{pageTokenSecret: secret}
	lower := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	upper := time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
	options := ListOptions{Limit: 2, IncludeArchived: false, CreatedAtGTE: &lower, CreatedAtLTE: &upper}
	token, err := service.encodeAgentListPageToken(workspace.DefaultID, options, lower, "agent_cursor")
	if err != nil {
		t.Fatalf("encodeAgentListPageToken: %v", err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		t.Fatalf("decode token envelope: %v", err)
	}
	for _, forbidden := range []string{string(workspace.DefaultID), "workspace_b", "include_archived", "created_at_gte", "storage_sequence", "sql"} {
		if strings.Contains(string(decoded), forbidden) {
			t.Fatalf("token envelope leaked associated-data/sensitive field %q: %s", forbidden, decoded)
		}
	}
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, options); err != nil {
		t.Fatalf("decode valid token: %v", err)
	}
	if _, err := service.decodeAgentListPageToken(token, "workspace_b", options); err == nil {
		t.Fatal("cross-workspace replay must reject")
	}
	changedFilter := options
	changedFilter.IncludeArchived = true
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, changedFilter); err == nil {
		t.Fatal("include_archived replay must reject")
	}
	changedFilter = options
	changedFilter.CreatedAtGTE = nil
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, changedFilter); err == nil {
		t.Fatal("created_at lower-bound removal replay must reject")
	}
	changedFilter = options
	changedLower := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	changedFilter.CreatedAtGTE = &changedLower
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, changedFilter); err == nil {
		t.Fatal("created_at lower-bound change replay must reject")
	}
	changedFilter = options
	changedFilter.CreatedAtLTE = nil
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, changedFilter); err == nil {
		t.Fatal("created_at upper-bound removal replay must reject")
	}
	changedFilter = options
	changedUpper := time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)
	changedFilter.CreatedAtLTE = &changedUpper
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, changedFilter); err == nil {
		t.Fatal("created_at upper-bound change replay must reject")
	}
}

func TestAgentPageTokensRejectMalformedWrongVersionAndMissingCursor(t *testing.T) {
	secret := bytes.Repeat([]byte{5}, 32)
	service := &Service{pageTokenSecret: secret}
	options := ListOptions{}

	cases := map[string]string{
		"malformed_base64": "not-a-token",
		"malformed_json":   base64.RawURLEncoding.EncodeToString([]byte("not-json")),
		"wrong_version": signedAgentPageTokenForTest(t, secret,
			agentPageTokenPayload{Version: 2, Kind: agentPageKindList, CreatedAt: "2026-01-01T00:00:00Z", AgentID: "agent_cursor"},
			agentListAssociatedData(workspace.DefaultID, options)),
		"missing_cursor": signedAgentPageTokenForTest(t, secret,
			agentPageTokenPayload{Version: 1, Kind: agentPageKindList},
			agentListAssociatedData(workspace.DefaultID, options)),
		"bad_signature": signedAgentPageTokenEnvelopeForTest(t,
			agentPageTokenPayload{Version: 1, Kind: agentPageKindList, CreatedAt: "2026-01-01T00:00:00Z", AgentID: "agent_cursor"},
			"bad-signature"),
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, options); err == nil {
				t.Fatal("invalid page token must reject")
			}
		})
	}
}

func TestAgentVersionPageTokensBindParentAndKind(t *testing.T) {
	secret := bytes.Repeat([]byte{8}, 32)
	service := &Service{pageTokenSecret: secret}
	token, err := service.encodeAgentVersionsPageToken(workspace.DefaultID, "agent_parent", 2)
	if err != nil {
		t.Fatalf("encodeAgentVersionsPageToken: %v", err)
	}
	if _, err := service.decodeAgentVersionsPageToken(token, workspace.DefaultID, "agent_parent"); err != nil {
		t.Fatalf("decode valid version token: %v", err)
	}
	if _, err := service.decodeAgentVersionsPageToken(token, "workspace_b", "agent_parent"); err == nil {
		t.Fatal("cross-workspace version token replay must reject")
	}
	if _, err := service.decodeAgentVersionsPageToken(token, workspace.DefaultID, "agent_other"); err == nil {
		t.Fatal("different parent Agent replay must reject")
	}
	if _, err := service.decodeAgentListPageToken(token, workspace.DefaultID, ListOptions{}); err == nil {
		t.Fatal("version token replay on Agent list must reject")
	}
}

func signedAgentPageTokenForTest(t *testing.T, secret []byte, payload agentPageTokenPayload, associatedData agentPageTokenAssociatedData) string {
	t.Helper()
	body, err := json.Marshal(agentPageTokenSignedBody{Payload: payload, AssociatedData: associatedData})
	if err != nil {
		t.Fatalf("marshal signed body: %v", err)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return signedAgentPageTokenEnvelopeForTest(t, payload, base64.RawURLEncoding.EncodeToString(mac.Sum(nil)))
}

func signedAgentPageTokenEnvelopeForTest(t *testing.T, payload agentPageTokenPayload, signature string) string {
	t.Helper()
	raw, err := json.Marshal(signedAgentPageToken{Payload: payload, Signature: signature})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}
