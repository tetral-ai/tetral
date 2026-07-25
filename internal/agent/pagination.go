package agent

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	agentPageTokenVersion     = 1
	agentPageKindList         = "agents"
	agentPageKindVersions     = "agent_versions"
	agentCreatedAtTokenFormat = time.RFC3339Nano
)

type ListOptions struct {
	Limit           int
	Page            string
	IncludeArchived bool
	CreatedAtGTE    *time.Time
	CreatedAtLTE    *time.Time

	cursorCreatedAt time.Time
	cursorID        string
}

type ListVersionsOptions struct {
	Limit int
	Page  string

	cursorVersion int
}

type ListResult struct {
	Data     []*Agent `json:"data"`
	NextPage *string  `json:"next_page"`
}

type agentPageTokenPayload struct {
	Version      int    `json:"version"`
	Kind         string `json:"kind"`
	CreatedAt    string `json:"created_at,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	VersionValue int    `json:"version_value,omitempty"`
}

type agentPageTokenAssociatedData struct {
	Version         int    `json:"version"`
	Kind            string `json:"kind"`
	WorkspaceID     string `json:"workspace_id"`
	ParentAgentID   string `json:"parent_agent_id,omitempty"`
	IncludeArchived bool   `json:"include_archived,omitempty"`
	CreatedAtGTE    string `json:"created_at_gte,omitempty"`
	CreatedAtLTE    string `json:"created_at_lte,omitempty"`
}

type signedAgentPageToken struct {
	Payload   agentPageTokenPayload `json:"payload"`
	Signature string                `json:"signature"`
}

type agentPageTokenSignedBody struct {
	Payload        agentPageTokenPayload        `json:"payload"`
	AssociatedData agentPageTokenAssociatedData `json:"associated_data"`
}

func (s *Service) encodeAgentListPageToken(ws workspace.ID, options ListOptions, createdAt time.Time, agentID string) (string, error) {
	return s.encodePageToken(
		agentPageTokenPayload{
			Version:   agentPageTokenVersion,
			Kind:      agentPageKindList,
			CreatedAt: createdAt.UTC().Format(agentCreatedAtTokenFormat),
			AgentID:   agentID,
		},
		agentListAssociatedData(ws, options),
	)
}

func (s *Service) decodeAgentListPageToken(raw string, ws workspace.ID, options ListOptions) (agentPageTokenPayload, error) {
	payload, err := s.decodePageToken(raw, agentListAssociatedData(ws, options))
	if err != nil {
		return agentPageTokenPayload{}, err
	}
	if payload.Kind != agentPageKindList || payload.AgentID == "" || payload.CreatedAt == "" {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if _, err := time.Parse(agentCreatedAtTokenFormat, payload.CreatedAt); err != nil {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func (s *Service) encodeAgentVersionsPageToken(ws workspace.ID, agentID string, version int) (string, error) {
	return s.encodePageToken(
		agentPageTokenPayload{
			Version:      agentPageTokenVersion,
			Kind:         agentPageKindVersions,
			VersionValue: version,
		},
		agentVersionsAssociatedData(ws, agentID),
	)
}

func (s *Service) decodeAgentVersionsPageToken(raw string, ws workspace.ID, agentID string) (agentPageTokenPayload, error) {
	payload, err := s.decodePageToken(raw, agentVersionsAssociatedData(ws, agentID))
	if err != nil {
		return agentPageTokenPayload{}, err
	}
	if payload.Kind != agentPageKindVersions || payload.VersionValue <= 0 {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return payload, nil
}

func (s *Service) encodePageToken(payload agentPageTokenPayload, associatedData agentPageTokenAssociatedData) (string, error) {
	if len(s.pageTokenSecret) != 32 {
		return "", &ValidationError{Message: "invalid page token"}
	}
	payload.Version = agentPageTokenVersion
	body, err := json.Marshal(agentPageTokenSignedBody{Payload: payload, AssociatedData: associatedData})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = mac.Write(body)
	envelope := signedAgentPageToken{
		Payload:   payload,
		Signature: base64.RawURLEncoding.EncodeToString(mac.Sum(nil)),
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) decodePageToken(raw string, associatedData agentPageTokenAssociatedData) (agentPageTokenPayload, error) {
	if len(s.pageTokenSecret) != 32 {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	var envelope signedAgentPageToken
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if envelope.Payload.Version != agentPageTokenVersion {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	body, err := json.Marshal(agentPageTokenSignedBody{Payload: envelope.Payload, AssociatedData: associatedData})
	if err != nil {
		return agentPageTokenPayload{}, err
	}
	wantMac := hmac.New(sha256.New, s.pageTokenSecret)
	_, _ = wantMac.Write(body)
	gotSignature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	if !hmac.Equal(gotSignature, wantMac.Sum(nil)) {
		return agentPageTokenPayload{}, &ValidationError{Message: "invalid page token"}
	}
	return envelope.Payload, nil
}

func agentListAssociatedData(ws workspace.ID, options ListOptions) agentPageTokenAssociatedData {
	return agentPageTokenAssociatedData{
		Version:         agentPageTokenVersion,
		Kind:            agentPageKindList,
		WorkspaceID:     string(ws),
		IncludeArchived: options.IncludeArchived,
		CreatedAtGTE:    formatOptionalTokenTime(options.CreatedAtGTE),
		CreatedAtLTE:    formatOptionalTokenTime(options.CreatedAtLTE),
	}
}

func agentVersionsAssociatedData(ws workspace.ID, agentID string) agentPageTokenAssociatedData {
	return agentPageTokenAssociatedData{
		Version:       agentPageTokenVersion,
		Kind:          agentPageKindVersions,
		WorkspaceID:   string(ws),
		ParentAgentID: agentID,
	}
}

func formatOptionalTokenTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(agentCreatedAtTokenFormat)
}
