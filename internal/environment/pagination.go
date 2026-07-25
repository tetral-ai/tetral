package environment

import (
	"encoding/base64"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	environmentPageTokenVersion  = 1
	environmentPageTokenResource = "environments"
)

type environmentPageToken struct {
	Version         int    `json:"v"`
	Resource        string `json:"r"`
	WorkspaceID     string `json:"w"`
	IncludeArchived bool   `json:"a"`
	LastSequence    int64  `json:"s"`
}

func encodeEnvironmentPageToken(token environmentPageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeEnvironmentPageToken(raw string, ws workspace.ID, includeArchived bool) (environmentPageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return environmentPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	var token environmentPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return environmentPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Version != environmentPageTokenVersion ||
		token.Resource != environmentPageTokenResource ||
		token.WorkspaceID != string(ws) ||
		token.IncludeArchived != includeArchived ||
		token.LastSequence < 0 {
		return environmentPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	return token, nil
}
