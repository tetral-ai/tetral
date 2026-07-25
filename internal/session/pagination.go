package session

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	pageTokenVersion    = 1
	pageKindSessions    = "sessions"
	pageKindResources   = "session_resources"
	pageKindThreads     = "session_threads"
	pageTokenTimeFormat = time.RFC3339Nano
)

type pageTokenPayload struct {
	Version    int    `json:"version"`
	Kind       string `json:"kind"`
	CreatedAt  string `json:"created_at,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
	ResourceID string `json:"resource_id,omitempty"`
	ThreadID   string `json:"thread_id,omitempty"`
}

type pageTokenAssociatedData struct {
	Version         int    `json:"version"`
	Kind            string `json:"kind"`
	WorkspaceID     string `json:"workspace_id"`
	SessionID       string `json:"session_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	AgentID         string `json:"agent_id,omitempty"`
	AgentVersion    int    `json:"agent_version,omitempty"`
	MemoryStoreID   string `json:"memory_store_id,omitempty"`
	DeploymentID    string `json:"deployment_id,omitempty"`
	Statuses        string `json:"statuses,omitempty"`
	CreatedAtGT     string `json:"created_at_gt,omitempty"`
	CreatedAtGTE    string `json:"created_at_gte,omitempty"`
	CreatedAtLT     string `json:"created_at_lt,omitempty"`
	CreatedAtLTE    string `json:"created_at_lte,omitempty"`
	Order           string `json:"order,omitempty"`
}

type signedPageToken struct {
	Payload   pageTokenPayload `json:"payload"`
	Signature string           `json:"signature"`
}

type pageTokenSignedBody struct {
	Payload        pageTokenPayload        `json:"payload"`
	AssociatedData pageTokenAssociatedData `json:"associated_data"`
}

func encodePageToken(secret []byte, payload pageTokenPayload, associatedData pageTokenAssociatedData) (string, error) {
	if len(secret) != 32 {
		return "", &ValidationError{Message: "invalid page token"}
	}
	payload.Version = pageTokenVersion
	body, err := json.Marshal(pageTokenSignedBody{Payload: payload, AssociatedData: associatedData})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	raw, err := json.Marshal(signedPageToken{
		Payload:   payload,
		Signature: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodePageToken(secret []byte, raw string, associatedData pageTokenAssociatedData) (pageTokenPayload, error) {
	if len(secret) != 32 {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	var envelope signedPageToken
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if envelope.Payload.Version != pageTokenVersion {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	body, err := json.Marshal(pageTokenSignedBody{Payload: envelope.Payload, AssociatedData: associatedData})
	if err != nil {
		return pageTokenPayload{}, err
	}
	wantMAC := hmac.New(sha256.New, secret)
	_, _ = wantMAC.Write(body)
	gotSignature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if !hmac.Equal(gotSignature, wantMAC.Sum(nil)) {
		return pageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return envelope.Payload, nil
}

func sessionListAssociatedData(ws workspace.ID, options ListOptions) pageTokenAssociatedData {
	return pageTokenAssociatedData{
		Version:         pageTokenVersion,
		Kind:            pageKindSessions,
		WorkspaceID:     string(ws),
		IncludeArchived: options.IncludeArchived,
		AgentID:         options.AgentID,
		AgentVersion:    options.AgentVersion,
		MemoryStoreID:   options.MemoryStoreID,
		DeploymentID:    options.DeploymentID,
		Statuses:        joinedSessionStatuses(options.Statuses),
		CreatedAtGT:     formatOptionalPageTime(options.CreatedAtGT),
		CreatedAtGTE:    formatOptionalPageTime(options.CreatedAtGTE),
		CreatedAtLT:     formatOptionalPageTime(options.CreatedAtLT),
		CreatedAtLTE:    formatOptionalPageTime(options.CreatedAtLTE),
		Order:           string(normalizeListOrder(options.Order)),
	}
}

func joinedSessionStatuses(statuses []Status) string {
	parts := make([]string, 0, len(statuses))
	for _, status := range statuses {
		parts = append(parts, string(status))
	}
	return strings.Join(parts, ",")
}

func resourceListAssociatedData(ws workspace.ID, sessionID string) pageTokenAssociatedData {
	return pageTokenAssociatedData{
		Version:     pageTokenVersion,
		Kind:        pageKindResources,
		WorkspaceID: string(ws),
		SessionID:   sessionID,
	}
}

func threadListAssociatedData(ws workspace.ID, sessionID string) pageTokenAssociatedData {
	return pageTokenAssociatedData{
		Version:     pageTokenVersion,
		Kind:        pageKindThreads,
		WorkspaceID: string(ws),
		SessionID:   sessionID,
	}
}

func resourcePageTokenPayload(resource *Resource) pageTokenPayload {
	return pageTokenPayload{
		Kind:       pageKindResources,
		ResourceID: resource.ID,
	}
}

func threadPageTokenPayload(thread *Thread) pageTokenPayload {
	return pageTokenPayload{
		Kind:     pageKindThreads,
		ThreadID: thread.ID,
	}
}

func formatOptionalPageTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(pageTokenTimeFormat)
}

func normalizeListOrder(order ListOrder) ListOrder {
	if order == ListOrderAscending {
		return ListOrderAscending
	}
	return ListOrderDescending
}

func validateListOrder(order ListOrder) error {
	switch order {
	case "", ListOrderAscending, ListOrderDescending:
		return nil
	default:
		return &ValidationError{Message: "order must be asc or desc"}
	}
}
