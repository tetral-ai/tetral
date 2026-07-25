package resourceprojection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v7/option"
	"github.com/cloudflare/cloudflare-go/v7/r2"
)

func TestBuildTemporaryCredentialParamsPinsObjectReadOnlyPrefixAndTTL(t *testing.T) {
	params, prefix, ttl, err := buildTemporaryCredentialParams("acct_123", "tetral-files", "parent-access-key", CredentialMintRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		TTL:         24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("buildTemporaryCredentialParams: %v", err)
	}
	if prefix != "workspaces/ws_test/sessions/sesn_test/resources/" {
		t.Fatalf("prefix = %q; want session resources prefix", prefix)
	}
	if ttl != 24*time.Hour {
		t.Fatalf("ttl = %s; want explicit request TTL", ttl)
	}
	if params.AccountID.Value != "acct_123" {
		t.Fatalf("AccountID = %q; want acct_123 path parameter", params.AccountID.Value)
	}
	body := marshalTemporaryCredentialParams(t, params)
	if body["bucket"] != "tetral-files" {
		t.Fatalf("bucket = %v; want configured bucket", body["bucket"])
	}
	if body["parentAccessKeyId"] != "parent-access-key" {
		t.Fatalf("parentAccessKeyId = %v; want configured parent access key", body["parentAccessKeyId"])
	}
	if body["permission"] != "object-read-only" {
		t.Fatalf("permission = %v; want object-read-only", body["permission"])
	}
	if body["ttlSeconds"] != float64(86400) {
		t.Fatalf("ttlSeconds = %v; want 86400", body["ttlSeconds"])
	}
	gotPrefixes, ok := body["prefixes"].([]any)
	if !ok || len(gotPrefixes) != 1 || gotPrefixes[0] != "workspaces/ws_test/sessions/sesn_test/resources/" {
		t.Fatalf("prefixes = %#v; want single session resources prefix", body["prefixes"])
	}
	if _, ok := body["objects"]; ok {
		t.Fatalf("objects = %#v; want absent object scope", body["objects"])
	}
}

func TestCredentialPrefixAssertionRejectsEmptyAbsentOrWrongScope(t *testing.T) {
	tests := []struct {
		name      string
		workspace string
		session   string
		prefixes  []string
	}{
		{name: "empty workspace", workspace: "", session: "sesn_test", prefixes: []string{"workspaces/ws_test/sessions/sesn_test/resources/"}},
		{name: "empty session", workspace: "ws_test", session: "", prefixes: []string{"workspaces/ws_test/sessions/sesn_test/resources/"}},
		{name: "absent prefixes", workspace: "ws_test", session: "sesn_test", prefixes: nil},
		{name: "empty prefix", workspace: "ws_test", session: "sesn_test", prefixes: []string{""}},
		{name: "canonical files prefix", workspace: "ws_test", session: "sesn_test", prefixes: []string{"files/ws_test/"}},
		{name: "another session prefix", workspace: "ws_test", session: "sesn_test", prefixes: []string{"workspaces/ws_test/sessions/sesn_other/"}},
		{name: "two prefixes", workspace: "ws_test", session: "sesn_test", prefixes: []string{"workspaces/ws_test/sessions/sesn_test/", "workspaces/ws_test/sessions/sesn_other/"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := assertSessionCredentialPrefixes(tc.workspace, tc.session, tc.prefixes); err == nil {
				t.Fatal("assertSessionCredentialPrefixes accepted invalid prefix scope")
			}
		})
	}
}

func TestCredentialMinterParsesResponseAndComputesLocalExpiry(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	api := &recordingTemporaryCredentialAPI{
		response: &r2.TemporaryCredentialNewResponse{
			AccessKeyID:     "minted-access",
			SecretAccessKey: "minted-secret",
			SessionToken:    "minted-session-token",
		},
	}
	minter, err := NewCredentialMinter(CredentialMintConfig{
		AccountID:           "acct_123",
		Bucket:              "tetral-files",
		ParentAccessKeyID:   "parent-access-key",
		TemporaryCredential: api,
		ParentAPIToken:      "not-used-when-api-is-injected",
	})
	if err != nil {
		t.Fatalf("NewCredentialMinter: %v", err)
	}
	result, err := minter.Mint(context.Background(), CredentialMintRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		TTL:         48 * time.Hour,
		Now:         now,
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if result.Credential.AccessKeyID != "minted-access" ||
		result.Credential.SecretAccessKey != "minted-secret" ||
		result.Credential.SessionToken != "minted-session-token" {
		t.Fatalf("credential = %+v; want parsed SigV4 triple", result.Credential)
	}
	if !result.ExpiresAt.Equal(now.Add(48 * time.Hour)) {
		t.Fatalf("ExpiresAt = %s; want now + ttl", result.ExpiresAt)
	}
	if result.Prefix != "workspaces/ws_test/sessions/sesn_test/resources/" {
		t.Fatalf("Prefix = %q; want session resources prefix", result.Prefix)
	}
	body := marshalTemporaryCredentialParams(t, api.params)
	if body["ttlSeconds"] != float64(172800) {
		t.Fatalf("ttlSeconds = %v; want explicit 48h ttl", body["ttlSeconds"])
	}
}

func TestCredentialMinterDoesNotClampRequestedTTLWhenProviderReturnsNoExpiry(t *testing.T) {
	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	requestedTTL := 45 * 24 * time.Hour
	api := &recordingTemporaryCredentialAPI{
		response: &r2.TemporaryCredentialNewResponse{
			AccessKeyID:     "minted-access",
			SecretAccessKey: "minted-secret",
			SessionToken:    "minted-session-token",
		},
	}
	minter, err := NewCredentialMinter(CredentialMintConfig{
		AccountID:           "acct_123",
		Bucket:              "tetral-files",
		ParentAccessKeyID:   "parent-access-key",
		TemporaryCredential: api,
	})
	if err != nil {
		t.Fatalf("NewCredentialMinter: %v", err)
	}
	result, err := minter.Mint(context.Background(), CredentialMintRequest{
		WorkspaceID: "ws_test",
		SessionID:   "sesn_test",
		TTL:         requestedTTL,
		Now:         now,
	})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if !result.ExpiresAt.Equal(now.Add(requestedTTL)) {
		t.Fatalf("ExpiresAt = %s; want local now + requested ttl %s", result.ExpiresAt, requestedTTL)
	}
	body := marshalTemporaryCredentialParams(t, api.params)
	if body["ttlSeconds"] != float64(requestedTTL/time.Second) {
		t.Fatalf("ttlSeconds = %v; want unclamped requested TTL seconds", body["ttlSeconds"])
	}
}

func TestCredentialFormattingAndMintErrorsRedactSecrets(t *testing.T) {
	const (
		accessKey   = "minted-access-secret"
		secretKey   = "minted-secret-key"
		sessionTok  = "minted-session-token"
		parentToken = "parent-api-token"
	)
	credential := Credential{AccessKeyID: accessKey, SecretAccessKey: secretKey, SessionToken: sessionTok}
	result := CredentialMintResult{Credential: credential, ExpiresAt: time.Now(), Prefix: "workspaces/ws/sessions/sesn/"}
	config := CredentialMintConfig{
		AccountID:         "acct_123",
		Bucket:            "tetral-files",
		ParentAccessKeyID: accessKey,
		ParentAPIToken:    parentToken,
	}
	api := &recordingTemporaryCredentialAPI{err: errors.New("provider body included " + accessKey + " " + secretKey + " " + sessionTok + " " + parentToken)}
	minter, err := NewCredentialMinter(CredentialMintConfig{
		AccountID:           "acct_123",
		Bucket:              "tetral-files",
		ParentAccessKeyID:   "parent-access-key",
		ParentAPIToken:      parentToken,
		TemporaryCredential: api,
	})
	if err != nil {
		t.Fatalf("NewCredentialMinter: %v", err)
	}
	_, err = minter.Mint(context.Background(), CredentialMintRequest{WorkspaceID: "ws", SessionID: "sesn", TTL: time.Hour})
	if err == nil {
		t.Fatal("Mint succeeded; want provider error")
	}
	rendered := strings.Join([]string{
		fmt.Sprint(credential),
		fmt.Sprintf("%+v", credential),
		fmt.Sprintf("%#v", credential),
		fmt.Sprint(result),
		fmt.Sprintf("%+v", result),
		fmt.Sprintf("%#v", result),
		fmt.Sprintf("%+v", config),
		fmt.Sprint(err),
		fmt.Sprintf("%+v", err),
		fmt.Sprintf("%#v", err),
	}, "\n")
	for _, secret := range []string{accessKey, secretKey, sessionTok, parentToken} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("rendered credential/error leaked secret %q:\n%s", secret, rendered)
		}
	}
}

func marshalTemporaryCredentialParams(t *testing.T, params r2.TemporaryCredentialNewParams) map[string]any {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	return body
}

type recordingTemporaryCredentialAPI struct {
	params   r2.TemporaryCredentialNewParams
	response *r2.TemporaryCredentialNewResponse
	err      error
}

func (r *recordingTemporaryCredentialAPI) New(_ context.Context, params r2.TemporaryCredentialNewParams, _ ...option.RequestOption) (*r2.TemporaryCredentialNewResponse, error) {
	r.params = params
	if r.err != nil {
		return nil, r.err
	}
	return r.response, nil
}
