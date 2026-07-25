package memory

import (
	"encoding/base64"
	"encoding/json"

	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	storePageTokenVersion    = 1
	storePageTokenResource   = "memory_stores"
	memoryPageTokenVersion   = 2
	memoryPageTokenResource  = "memories"
	versionPageTokenVersion  = 1
	versionPageTokenResource = "memory_versions"
)

type storePageToken struct {
	Version         int    `json:"v"`
	Resource        string `json:"r"`
	WorkspaceID     string `json:"w"`
	IncludeArchived bool   `json:"a"`
	CreatedAtGTE    string `json:"gte,omitempty"`
	CreatedAtLTE    string `json:"lte,omitempty"`
	LastCreatedAt   string `json:"c"`
	LastID          string `json:"id"`
}

func encodeStorePageToken(token storePageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeStorePageToken(raw string, ws workspace.ID, options ListStoresOptions) (storePageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return storePageToken{}, &ValidationError{Message: "invalid page token"}
	}
	var token storePageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return storePageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Version != storePageTokenVersion ||
		token.Resource != storePageTokenResource ||
		token.WorkspaceID != string(ws) ||
		token.IncludeArchived != options.IncludeArchived ||
		token.CreatedAtGTE != options.CreatedAtGTE ||
		token.CreatedAtLTE != options.CreatedAtLTE ||
		token.LastCreatedAt == "" ||
		token.LastID == "" {
		return storePageToken{}, &ValidationError{Message: "invalid page token"}
	}
	return token, nil
}

type memoryPageToken struct {
	Version        int    `json:"v"`
	Resource       string `json:"r"`
	WorkspaceID    string `json:"w"`
	MemoryStoreID  string `json:"s"`
	PathPrefix     string `json:"p"`
	Depth          int    `json:"d"`
	DepthSet       bool   `json:"ds"`
	View           string `json:"view"`
	OrderBy        string `json:"ob"`
	Order          string `json:"o"`
	LastOrderValue string `json:"lov"`
	LastPath       string `json:"lp"`
	LastType       string `json:"lt"`
	LastMemoryID   string `json:"lm,omitempty"`
}

func encodeMemoryPageToken(token memoryPageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeMemoryPageToken(raw string, ws workspace.ID, storeID string, options ListMemoriesOptions) (memoryPageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	var token memoryPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Version != memoryPageTokenVersion ||
		token.Resource != memoryPageTokenResource ||
		token.WorkspaceID != string(ws) ||
		token.MemoryStoreID != storeID ||
		token.PathPrefix != options.PathPrefix ||
		token.Depth != options.Depth ||
		token.DepthSet != options.DepthSet ||
		token.View != options.View ||
		token.OrderBy != options.OrderBy ||
		token.Order != options.Order ||
		token.LastOrderValue == "" ||
		token.LastPath == "" ||
		token.LastType == "" {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.LastType != memoryType && token.LastType != "memory_prefix" {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.LastType == memoryType && token.LastMemoryID == "" {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.LastType == "memory_prefix" && token.LastMemoryID != "" {
		return memoryPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	return token, nil
}

type versionPageToken struct {
	Version       int    `json:"v"`
	Resource      string `json:"r"`
	WorkspaceID   string `json:"w"`
	MemoryStoreID string `json:"s"`
	MemoryID      string `json:"m,omitempty"`
	Operation     string `json:"op,omitempty"`
	SessionID     string `json:"sid,omitempty"`
	APIKeyID      string `json:"ak,omitempty"`
	CreatedAtGTE  string `json:"gte,omitempty"`
	CreatedAtLTE  string `json:"lte,omitempty"`
	View          string `json:"view"`
	LastCreatedAt string `json:"lc"`
	LastID        string `json:"id"`
}

func encodeVersionPageToken(token versionPageToken) (string, error) {
	payload, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeVersionPageToken(raw string, ws workspace.ID, storeID string, options ListMemoryVersionsOptions) (versionPageToken, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return versionPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	var token versionPageToken
	if err := json.Unmarshal(payload, &token); err != nil {
		return versionPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	if token.Version != versionPageTokenVersion ||
		token.Resource != versionPageTokenResource ||
		token.WorkspaceID != string(ws) ||
		token.MemoryStoreID != storeID ||
		token.MemoryID != options.MemoryID ||
		token.Operation != options.Operation ||
		token.SessionID != options.SessionID ||
		token.APIKeyID != options.APIKeyID ||
		token.CreatedAtGTE != options.CreatedAtGTE ||
		token.CreatedAtLTE != options.CreatedAtLTE ||
		token.View != options.View ||
		token.LastCreatedAt == "" ||
		token.LastID == "" {
		return versionPageToken{}, &ValidationError{Message: "invalid page token"}
	}
	return token, nil
}
