package vault

import (
	"encoding/base64"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	pageTokenVersion   = 1
	pageTokenVaults    = "vaults"
	pageTokenVaultAuth = "vault_auth"
)

type pageToken struct {
	Version         int    `json:"v"`
	Resource        string `json:"r"`
	WorkspaceID     string `json:"w"`
	ParentVaultID   string `json:"p,omitempty"`
	IncludeArchived bool   `json:"a"`
	LastSequence    int64  `json:"s"`
}

func encodePageToken(token pageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodePageToken(raw string, ws workspace.ID, resource string, parentVaultID string, includeArchived bool) (pageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return pageToken{}, &ValidationError{Message: "invalid page token"}
	}
	var token pageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return pageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Version != pageTokenVersion ||
		token.Resource != resource ||
		token.WorkspaceID != string(ws) ||
		token.ParentVaultID != parentVaultID ||
		token.IncludeArchived != includeArchived ||
		token.LastSequence < 0 {
		return pageToken{}, &ValidationError{Message: "invalid page token"}
	}
	return token, nil
}
