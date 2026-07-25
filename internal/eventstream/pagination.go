package eventstream

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"

	"github.com/tetral-ai/tetral/internal/httpapi"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	sessionEventsPageTokenVersion  = 3
	sessionEventsPageTokenResource = "session_events"
)

type sessionEventsPageToken struct {
	Version        int      `json:"v"`
	Resource       string   `json:"r"`
	WorkspaceID    string   `json:"w"`
	SessionID      string   `json:"s"`
	ThreadID       string   `json:"t,omitempty"`
	Order          string   `json:"o"`
	Sequence       int64    `json:"seq"`
	Position       int64    `json:"pos,omitempty"`
	CursorThreadID string   `json:"ct,omitempty"`
	EventID        string   `json:"e,omitempty"`
	Types          []string `json:"types,omitempty"`
	CreatedAtGT    string   `json:"cat_gt,omitempty"`
	CreatedAtGTE   string   `json:"cat_gte,omitempty"`
	CreatedAtLT    string   `json:"cat_lt,omitempty"`
	CreatedAtLTE   string   `json:"cat_lte,omitempty"`
}

type signedSessionEventsPageToken struct {
	Payload   sessionEventsPageToken `json:"payload"`
	Signature string                 `json:"signature"`
}

type sessionEventsPageTokenSignedBody struct {
	Payload sessionEventsPageToken `json:"payload"`
}

func encodeSessionEventsPageToken(secret []byte, token sessionEventsPageToken) (string, error) {
	if len(secret) != 32 {
		return "", &httpapi.ValidationError{Message: "invalid page token"}
	}
	token.Version = sessionEventsPageTokenVersion
	body, err := json.Marshal(sessionEventsPageTokenSignedBody{Payload: token})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	payload, err := json.Marshal(signedSessionEventsPageToken{
		Payload:   token,
		Signature: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeSessionEventsPageToken(secret []byte, raw string, ws workspace.ID, sessionID string, threadID string, options ListOptions) (sessionEventsPageToken, error) {
	if len(secret) != 32 {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	var envelope signedSessionEventsPageToken
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	body, err := json.Marshal(sessionEventsPageTokenSignedBody{Payload: envelope.Payload})
	if err != nil {
		return sessionEventsPageToken{}, err
	}
	wantMAC := hmac.New(sha256.New, secret)
	_, _ = wantMAC.Write(body)
	gotSignature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	if !hmac.Equal(gotSignature, wantMAC.Sum(nil)) {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	token := envelope.Payload
	if token.Version != sessionEventsPageTokenVersion ||
		token.Resource != sessionEventsPageTokenResource ||
		token.WorkspaceID != string(ws) ||
		token.SessionID != sessionID ||
		token.ThreadID != threadID ||
		token.Order != options.Order ||
		!pageTokenFiltersMatch(token, options) {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	if threadID == "" {
		if token.Position <= 0 || token.EventID == "" {
			return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
		}
	} else if token.Sequence <= 0 {
		return sessionEventsPageToken{}, &httpapi.ValidationError{Message: "invalid page token"}
	}
	return token, nil
}

func pageTokenFiltersMatch(token sessionEventsPageToken, options ListOptions) bool {
	if !stringSlicesEqual(token.Types, options.Types) {
		return false
	}
	return token.CreatedAtGT == options.CreatedAtGT &&
		token.CreatedAtGTE == options.CreatedAtGTE &&
		token.CreatedAtLT == options.CreatedAtLT &&
		token.CreatedAtLTE == options.CreatedAtLTE
}

func stringSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
