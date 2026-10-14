package files

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type filePagePayload struct {
	Version   int    `json:"version"`
	Kind      string `json:"kind"`
	Workspace string `json:"workspace"`
	ScopeID   string `json:"scope_id"`
	Sequence  int64  `json:"sequence"`
}

type filePageToken struct {
	Payload   filePagePayload `json:"payload"`
	Signature string          `json:"signature"`
}

func (s *Service) encodePageToken(ws workspace.ID, scopeID string, sequence int64) (string, error) {
	if len(s.pageTokenSecret) != 32 {
		return "", &ValidationError{Message: "invalid page token"}
	}
	payload := filePagePayload{Version: 1, Kind: "files", Workspace: string(ws), ScopeID: scopeID, Sequence: sequence}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = mac.Write(body)
	token := filePageToken{Payload: payload, Signature: base64.RawURLEncoding.EncodeToString(mac.Sum(nil))}
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) decodePageToken(raw string, ws workspace.ID, scopeID string) (int64, error) {
	invalid := &ValidationError{Message: "invalid page token"}
	if len(s.pageTokenSecret) != 32 {
		return 0, invalid
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return 0, invalid
	}
	var token filePageToken
	if err := json.Unmarshal(decoded, &token); err != nil {
		return 0, invalid
	}
	payload := token.Payload
	if payload.Version != 1 || payload.Kind != "files" || payload.Workspace != string(ws) || payload.ScopeID != scopeID || payload.Sequence <= 0 {
		return 0, invalid
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	mac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = mac.Write(body)
	signature, err := base64.RawURLEncoding.DecodeString(token.Signature)
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return 0, invalid
	}
	return payload.Sequence, nil
}
