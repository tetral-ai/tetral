// Package vault provides vault and credential CRUD with AES-256-GCM encryption at rest.
package vault

import (
	"encoding/json"
	"time"
)

const (
	credentialAuthTypeMCPOAuth       = "mcp_oauth" //nolint:gosec // Auth type literal, not a credential value.
	credentialAuthTypeStaticBearer   = "static_bearer"
	credentialAuthTypeProviderAPIKey = "provider_api_key"
	credentialAuthTypeProviderOAuth  = "provider_oauth" //nolint:gosec // Auth type literal, not a credential value.
)

func unsupportedCredentialAuthTypeError() *ValidationError {
	return &ValidationError{Message: "auth.type must be mcp_oauth, static_bearer, provider_api_key, or provider_oauth"}
}

// StringMap is public non-secret string metadata.
type StringMap map[string]string

// MarshalJSON keeps empty metadata maps as `{}` instead of `null`.
func (m StringMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("{}"), nil
	}
	type alias map[string]string
	return json.Marshal(alias(m))
}

// Vault represents a vault resource that holds credentials.
type Vault struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`
	DisplayName string     `json:"display_name"`
	Metadata    StringMap  `json:"metadata"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ArchivedAt  *time.Time `json:"archived_at"`

	storageSequence int64
}

// Credential represents a credential stored in a vault.
type Credential struct {
	ID          string               `json:"id"`
	Type        string               `json:"type"`
	VaultID     string               `json:"vault_id"`
	DisplayName string               `json:"display_name"`
	Metadata    StringMap            `json:"metadata"`
	Auth        CredentialAuthPublic `json:"auth"`
	CreatedAt   time.Time            `json:"created_at"`
	UpdatedAt   time.Time            `json:"updated_at"`
	ArchivedAt  *time.Time           `json:"archived_at"`

	storageSequence int64
}

// CredentialMetadata is kept as the store-facing public credential view.
type CredentialMetadata = Credential

// CredentialAuth holds secret-bearing create/update auth payloads.
type CredentialAuth struct {
	Type         string                  `json:"type"`
	MCPServerURL string                  `json:"mcp_server_url,omitempty"`
	ProviderID   string                  `json:"provider_id,omitempty"`
	AccessMode   string                  `json:"access_mode,omitempty"`
	AccessToken  string                  `json:"access_token,omitempty"`
	RefreshToken string                  `json:"refresh_token,omitempty"`
	ExpiresAt    string                  `json:"expires_at,omitempty"`
	AccountID    string                  `json:"account_id,omitempty"`
	Refresh      *CredentialOAuthRefresh `json:"refresh,omitempty"`
	Token        string                  `json:"token,omitempty"`
}

// CredentialOAuthRefresh holds mcp_oauth refresh material.
type CredentialOAuthRefresh struct {
	RefreshToken      string                       `json:"refresh_token"`
	ClientID          string                       `json:"client_id"`
	TokenEndpoint     string                       `json:"token_endpoint"`
	Scope             string                       `json:"scope,omitempty"`
	Resource          string                       `json:"resource,omitempty"`
	TokenEndpointAuth *CredentialTokenEndpointAuth `json:"token_endpoint_auth"`
}

// CredentialTokenEndpointAuth holds OAuth token endpoint auth material.
type CredentialTokenEndpointAuth struct {
	Type         string `json:"type"`
	ClientSecret string `json:"client_secret,omitempty"`
}

// CredentialAuthPublic is the public view of credential auth (no secrets).
type CredentialAuthPublic struct {
	Type            string                        `json:"type"`
	MCPServerURL    string                        `json:"mcp_server_url,omitempty"`
	ProviderID      string                        `json:"provider_id,omitempty"`
	AccessMode      string                        `json:"access_mode,omitempty"`
	ExpiresAt       string                        `json:"expires_at,omitempty"`
	AccountID       string                        `json:"account_id,omitempty"`
	Refresh         *CredentialOAuthRefreshPublic `json:"refresh,omitempty"`
	HasRefreshToken *bool                         `json:"has_refresh_token,omitempty"`
}

// CredentialOAuthRefreshPublic is the redacted public OAuth refresh view.
type CredentialOAuthRefreshPublic struct {
	ClientID          string                            `json:"client_id"`
	TokenEndpoint     string                            `json:"token_endpoint"`
	Scope             string                            `json:"scope,omitempty"`
	Resource          string                            `json:"resource,omitempty"`
	TokenEndpointAuth CredentialTokenEndpointAuthPublic `json:"token_endpoint_auth"`
}

// CredentialTokenEndpointAuthPublic is the redacted token endpoint auth view.
type CredentialTokenEndpointAuthPublic struct {
	Type string `json:"type"`
}

// CreateVaultRequest is the input for creating a vault.
type CreateVaultRequest struct {
	DisplayName string    `json:"display_name"`
	Metadata    StringMap `json:"metadata,omitempty"`
}

// VaultPatch is the partial update shape for a Vault.
type VaultPatch struct {
	raw map[string]json.RawMessage
}

// CreateCredentialRequest is the input for creating a credential.
type CreateCredentialRequest struct {
	DisplayName string         `json:"display_name,omitempty"`
	Metadata    StringMap      `json:"metadata,omitempty"`
	Auth        CredentialAuth `json:"auth"`
}

// CredentialPatch is the partial update shape for a Credential.
type CredentialPatch struct {
	raw  map[string]json.RawMessage
	Auth *CredentialAuth
}

// DeleteResult is returned when a resource is hard-deleted.
type DeleteResult struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// ListOptions controls Anthropic-compatible Vault/Credential pagination.
type ListOptions struct {
	Limit           int    `json:"limit"`
	Page            string `json:"page"`
	IncludeArchived bool   `json:"include_archived"`
}

// VaultListResult is the Vault list result.
type VaultListResult struct {
	Data     []*Vault `json:"data"`
	NextPage *string  `json:"next_page"`
}

// CredentialListResult is the Credential list result.
type CredentialListResult struct {
	Data     []*Credential `json:"data"`
	NextPage *string       `json:"next_page"`
}

// CredentialValidation is the public MCP OAuth credential diagnostic response.
type CredentialValidation struct {
	Type            string                     `json:"type"`
	CredentialID    string                     `json:"credential_id"`
	VaultID         string                     `json:"vault_id"`
	Status          string                     `json:"status"`
	ValidatedAt     time.Time                  `json:"validated_at"`
	HasRefreshToken bool                       `json:"has_refresh_token"`
	Refresh         *CredentialValidationCheck `json:"refresh"`
	MCPProbe        *CredentialValidationCheck `json:"mcp_probe"`
}

// CredentialValidationCheck describes one scrubbed validation sub-check.
type CredentialValidationCheck struct {
	Status       string                            `json:"status,omitempty"`
	Method       string                            `json:"method,omitempty"`
	HTTPResponse *CredentialValidationHTTPResponse `json:"http_response"`

	ErrorKind  string `json:"-"`
	HTTPStatus int    `json:"-"`
}

type CredentialValidationHTTPResponse struct {
	StatusCode    int    `json:"status_code"`
	ContentType   string `json:"content_type"`
	Body          string `json:"body"`
	BodyTruncated bool   `json:"body_truncated"`
}

// NotFoundError indicates the resource was not found.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// ValidationError indicates invalid input.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// ConflictError indicates a duplicate active credential or other state conflict.
type ConflictError struct {
	Message        string
	InvalidRequest bool
}

func (e *ConflictError) Error() string { return e.Message }
