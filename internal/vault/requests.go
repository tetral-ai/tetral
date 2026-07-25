package vault

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/tetral-ai/tetral/internal/endpointurl"
)

const (
	maxDisplayNameCodePoints   = 255
	maxMetadataPairs           = 16
	maxMetadataKeyCodePoints   = 64
	maxMetadataValueCodePoints = 512
)

var allowedVaultCreateFields = map[string]struct{}{
	"display_name": {},
	"metadata":     {},
}

var allowedVaultUpdateFields = map[string]struct{}{
	"display_name": {},
	"metadata":     {},
}

var allowedCredentialCreateFields = map[string]struct{}{
	"display_name": {},
	"metadata":     {},
	"auth":         {},
}

var allowedCredentialUpdateFields = map[string]struct{}{
	"display_name": {},
	"metadata":     {},
	"auth":         {},
}

// DecodeCreateVaultRequest strictly decodes the public Vault create body.
func DecodeCreateVaultRequest(body []byte) (CreateVaultRequest, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return CreateVaultRequest{}, err
	}
	if err := rejectUnknownFields(raw, allowedVaultCreateFields, ""); err != nil {
		return CreateVaultRequest{}, err
	}
	displayNameRaw, ok := raw["display_name"]
	if !ok {
		return CreateVaultRequest{}, &ValidationError{Message: "display_name is required"}
	}
	displayName, err := decodeString("display_name", displayNameRaw)
	if err != nil {
		return CreateVaultRequest{}, err
	}
	if err := validateRequiredDisplayName("display_name", displayName); err != nil {
		return CreateVaultRequest{}, err
	}
	request := CreateVaultRequest{DisplayName: displayName, Metadata: StringMap{}}
	if metadataRaw, ok := raw["metadata"]; ok {
		metadata, err := decodeCreateMetadata(metadataRaw)
		if err != nil {
			return CreateVaultRequest{}, err
		}
		request.Metadata = metadata
	}
	return request, nil
}

// DecodeUpdateVaultRequest strictly decodes the public Vault update body.
func DecodeUpdateVaultRequest(body []byte) (VaultPatch, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return VaultPatch{}, err
	}
	if err := rejectUnknownFields(raw, allowedVaultUpdateFields, ""); err != nil {
		return VaultPatch{}, err
	}
	if value, ok := raw["display_name"]; ok {
		if !isJSONNull(value) {
			displayName, err := decodeString("display_name", value)
			if err != nil {
				return VaultPatch{}, err
			}
			if err := validateRequiredDisplayName("display_name", displayName); err != nil {
				return VaultPatch{}, err
			}
		}
	}
	if value, ok := raw["metadata"]; ok {
		if _, err := decodeMetadataPatch(value); err != nil {
			return VaultPatch{}, err
		}
	}
	return VaultPatch{raw: raw}, nil
}

// Materialize applies a Vault update patch to the current Vault value.
func (p VaultPatch) Materialize(current Vault) (Vault, error) {
	out := current
	out.Metadata = normalizeMetadata(current.Metadata)
	if value, ok := p.raw["display_name"]; ok {
		if isJSONNull(value) {
			out.DisplayName = ""
		} else {
			displayName, err := decodeString("display_name", value)
			if err != nil {
				return Vault{}, err
			}
			if err := validateRequiredDisplayName("display_name", displayName); err != nil {
				return Vault{}, err
			}
			out.DisplayName = displayName
		}
	}
	if value, ok := p.raw["metadata"]; ok {
		metadata, err := mergeMetadataPatch(out.Metadata, value)
		if err != nil {
			return Vault{}, err
		}
		out.Metadata = metadata
	}
	return out, nil
}

// DecodeCreateCredentialRequest strictly decodes a credential create body.
func DecodeCreateCredentialRequest(body []byte) (CreateCredentialRequest, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return CreateCredentialRequest{}, err
	}
	if err := rejectUnknownFields(raw, allowedCredentialCreateFields, ""); err != nil {
		return CreateCredentialRequest{}, err
	}
	request := CreateCredentialRequest{Metadata: StringMap{}}
	if value, ok := raw["display_name"]; ok {
		displayName, err := decodeString("display_name", value)
		if err != nil {
			return CreateCredentialRequest{}, err
		}
		if err := validateOptionalDisplayName("display_name", displayName); err != nil {
			return CreateCredentialRequest{}, err
		}
		request.DisplayName = displayName
	}
	if value, ok := raw["metadata"]; ok {
		metadata, err := decodeCreateMetadata(value)
		if err != nil {
			return CreateCredentialRequest{}, err
		}
		request.Metadata = metadata
	}
	authRaw, ok := raw["auth"]
	if !ok {
		return CreateCredentialRequest{}, &ValidationError{Message: "auth is required"}
	}
	auth, err := decodeCredentialAuth(authRaw, true)
	if err != nil {
		return CreateCredentialRequest{}, err
	}
	request.Auth = auth
	return request, nil
}

// DecodeUpdateCredentialRequest strictly decodes a credential update body.
func DecodeUpdateCredentialRequest(body []byte) (CredentialPatch, error) {
	raw, err := decodeObject(body)
	if err != nil {
		return CredentialPatch{}, err
	}
	if err := rejectUnknownFields(raw, allowedCredentialUpdateFields, ""); err != nil {
		return CredentialPatch{}, err
	}
	patch := CredentialPatch{raw: raw}
	if value, ok := raw["display_name"]; ok {
		if !isJSONNull(value) {
			displayName, err := decodeString("display_name", value)
			if err != nil {
				return CredentialPatch{}, err
			}
			if err := validateOptionalDisplayName("display_name", displayName); err != nil {
				return CredentialPatch{}, err
			}
		}
	}
	if value, ok := raw["metadata"]; ok {
		if _, err := decodeMetadataPatch(value); err != nil {
			return CredentialPatch{}, err
		}
	}
	if value, ok := raw["auth"]; ok {
		auth, err := decodeCredentialAuth(value, false)
		if err != nil {
			return CredentialPatch{}, err
		}
		patch.Auth = &auth
	}
	return patch, nil
}

// Materialize applies a Credential update patch to current public metadata.
func (p CredentialPatch) Materialize(current Credential) (Credential, error) {
	out := current
	out.Metadata = normalizeMetadata(current.Metadata)
	if value, ok := p.raw["display_name"]; ok {
		if isJSONNull(value) {
			out.DisplayName = ""
		} else {
			displayName, err := decodeString("display_name", value)
			if err != nil {
				return Credential{}, err
			}
			if err := validateOptionalDisplayName("display_name", displayName); err != nil {
				return Credential{}, err
			}
			out.DisplayName = displayName
		}
	}
	if value, ok := p.raw["metadata"]; ok {
		metadata, err := mergeMetadataPatch(out.Metadata, value)
		if err != nil {
			return Credential{}, err
		}
		out.Metadata = metadata
	}
	return out, nil
}

func decodeObject(body []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	var raw map[string]json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, &ValidationError{Message: "request body must be a JSON object"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, &ValidationError{Message: "request body must contain exactly one JSON object"}
	}
	if raw == nil {
		return nil, &ValidationError{Message: "request body must be a JSON object"}
	}
	return raw, nil
}

func rejectUnknownFields(raw map[string]json.RawMessage, allowed map[string]struct{}, prefix string) error {
	for key := range raw {
		if _, ok := allowed[key]; !ok {
			field := key
			if prefix != "" {
				field = prefix + "." + key
			}
			return &ValidationError{Message: "unknown field " + field}
		}
	}
	return nil
}

func decodeString(field string, raw json.RawMessage) (string, error) {
	if isJSONNull(raw) {
		return "", &ValidationError{Message: field + " cannot be null"}
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", &ValidationError{Message: field + " must be a string"}
	}
	return value, nil
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func decodeRequiredString(field string, raw json.RawMessage) (string, error) {
	value, err := decodeString(field, raw)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", &ValidationError{Message: field + " is required"}
	}
	return value, nil
}

func decodeNonEmptyString(field string, raw json.RawMessage) (string, error) {
	value, err := decodeString(field, raw)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", &ValidationError{Message: field + " must be non-empty when provided"}
	}
	return value, nil
}

func validateRequiredDisplayName(field string, value string) error {
	if value == "" {
		return &ValidationError{Message: field + " is required"}
	}
	return validateOptionalDisplayName(field, value)
}

func validateOptionalDisplayName(field string, value string) error {
	if utf8.RuneCountInString(value) > maxDisplayNameCodePoints {
		return &ValidationError{Message: fmt.Sprintf("%s must be <= %d Unicode code points", field, maxDisplayNameCodePoints)}
	}
	return nil
}

func decodeCreateMetadata(raw json.RawMessage) (StringMap, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: "metadata cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	out := make(StringMap, len(obj))
	for key, value := range obj {
		if string(value) == "null" {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string"}
		}
		str, err := decodeString("metadata["+key+"]", value)
		if err != nil {
			return nil, err
		}
		out[key] = str
	}
	if err := validateMetadataLimits(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeMetadataPatch(raw json.RawMessage) (map[string]json.RawMessage, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: "metadata must be an object"}
	}
	for key, value := range obj {
		if string(value) == "null" {
			continue
		}
		if _, err := decodeString("metadata["+key+"]", value); err != nil {
			return nil, &ValidationError{Message: "metadata[" + key + "] must be a string or null"}
		}
	}
	return obj, nil
}

func mergeMetadataPatch(current StringMap, raw json.RawMessage) (StringMap, error) {
	patch, err := decodeMetadataPatch(raw)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return StringMap{}, nil
	}
	out := normalizeMetadata(current)
	for key, value := range patch {
		if string(value) == "null" {
			delete(out, key)
			continue
		}
		str, _ := decodeString("metadata["+key+"]", value)
		out[key] = str
	}
	if err := validateMetadataLimits(out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeMetadata(metadata StringMap) StringMap {
	out := make(StringMap, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}

func validateMetadataLimits(metadata StringMap) error {
	if len(metadata) > maxMetadataPairs {
		return &ValidationError{Message: fmt.Sprintf("metadata must have <= %d pairs", maxMetadataPairs)}
	}
	for key, value := range metadata {
		if utf8.RuneCountInString(key) > maxMetadataKeyCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata key must be <= %d Unicode code points", maxMetadataKeyCodePoints)}
		}
		if utf8.RuneCountInString(value) > maxMetadataValueCodePoints {
			return &ValidationError{Message: fmt.Sprintf("metadata value must be <= %d Unicode code points", maxMetadataValueCodePoints)}
		}
	}
	return nil
}

func decodeCredentialAuth(raw json.RawMessage, create bool) (CredentialAuth, error) {
	obj, err := decodeNestedObject("auth", raw)
	if err != nil {
		return CredentialAuth{}, err
	}
	rawType, ok := obj["type"]
	if !ok {
		if create {
			return CredentialAuth{}, &ValidationError{Message: "auth.type is required"}
		}
		return decodeCredentialAuthPatch(obj)
	}
	authType, err := decodeString("auth.type", rawType)
	if err != nil {
		return CredentialAuth{}, err
	}
	switch authType {
	case credentialAuthTypeMCPOAuth:
		return decodeMCPOAuth(obj, create)
	case credentialAuthTypeStaticBearer:
		return decodeStaticBearer(obj, create)
	case credentialAuthTypeProviderAPIKey:
		return decodeProviderAPIKey(obj, create)
	case credentialAuthTypeProviderOAuth:
		return decodeProviderOAuth(obj, create)
	default:
		return CredentialAuth{}, unsupportedCredentialAuthTypeError()
	}
}

func decodeCredentialAuthPatch(obj map[string]json.RawMessage) (CredentialAuth, error) {
	allowed := map[string]struct{}{
		"type":           {},
		"mcp_server_url": {},
		"access_token":   {},
		"refresh_token":  {},
		"expires_at":     {},
		"account_id":     {},
		"refresh":        {},
		"token":          {},
		"provider_id":    {},
		"access_mode":    {},
	}
	if err := rejectUnknownFields(obj, allowed, "auth"); err != nil {
		return CredentialAuth{}, err
	}
	auth := CredentialAuth{}
	if value, ok := obj["provider_id"]; ok {
		providerID, err := decodeRequiredString("auth.provider_id", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.ProviderID = providerID
	}
	if value, ok := obj["access_mode"]; ok {
		accessMode, err := decodeRequiredString("auth.access_mode", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessMode = accessMode
	}
	if value, ok := obj["mcp_server_url"]; ok {
		urlValue, err := decodeCanonicalEndpoint("auth.mcp_server_url", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.MCPServerURL = urlValue
	}
	if value, ok := obj["access_token"]; ok {
		accessToken, err := decodeRequiredString("auth.access_token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessToken = accessToken
	}
	if value, ok := obj["refresh_token"]; ok {
		refreshToken, err := decodeRequiredString("auth.refresh_token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.RefreshToken = refreshToken
	}
	if value, ok := obj["expires_at"]; ok {
		if !isJSONNull(value) {
			expiresAt, err := decodeNonEmptyString("auth.expires_at", value)
			if err != nil {
				return CredentialAuth{}, err
			}
			auth.ExpiresAt = expiresAt
		}
	}
	if value, ok := obj["account_id"]; ok {
		accountID, err := decodeNonEmptyString("auth.account_id", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccountID = accountID
	}
	if value, ok := obj["refresh"]; ok {
		if !isJSONNull(value) {
			refresh, err := decodeOAuthRefresh(value, false)
			if err != nil {
				return CredentialAuth{}, err
			}
			auth.Refresh = refresh
		}
	}
	if value, ok := obj["token"]; ok {
		token, err := decodeRequiredString("auth.token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.Token = token
	}
	return auth, nil
}

func decodeStaticBearer(obj map[string]json.RawMessage, create bool) (CredentialAuth, error) {
	allowed := map[string]struct{}{"type": {}, "mcp_server_url": {}, "token": {}}
	if err := rejectUnknownFields(obj, allowed, "auth"); err != nil {
		return CredentialAuth{}, err
	}
	auth := CredentialAuth{Type: credentialAuthTypeStaticBearer}
	if value, ok := obj["mcp_server_url"]; ok {
		urlValue, err := decodeCanonicalEndpoint("auth.mcp_server_url", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.MCPServerURL = urlValue
	}
	if value, ok := obj["token"]; ok {
		token, err := decodeRequiredString("auth.token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.Token = token
	}
	if create && auth.MCPServerURL == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.mcp_server_url is required"}
	}
	if create && auth.Token == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.token is required"}
	}
	return auth, nil
}

func decodeMCPOAuth(obj map[string]json.RawMessage, create bool) (CredentialAuth, error) {
	allowed := map[string]struct{}{"type": {}, "mcp_server_url": {}, "access_token": {}, "expires_at": {}, "refresh": {}}
	if err := rejectUnknownFields(obj, allowed, "auth"); err != nil {
		return CredentialAuth{}, err
	}
	auth := CredentialAuth{Type: credentialAuthTypeMCPOAuth}
	if value, ok := obj["mcp_server_url"]; ok {
		urlValue, err := decodeCanonicalEndpoint("auth.mcp_server_url", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.MCPServerURL = urlValue
	}
	if value, ok := obj["access_token"]; ok {
		accessToken, err := decodeRequiredString("auth.access_token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessToken = accessToken
	}
	if value, ok := obj["expires_at"]; ok {
		if !isJSONNull(value) {
			expiresAt, err := decodeString("auth.expires_at", value)
			if !create && err == nil && expiresAt == "" {
				err = &ValidationError{Message: "auth.expires_at must be non-empty when provided"}
			}
			if err != nil {
				return CredentialAuth{}, err
			}
			auth.ExpiresAt = expiresAt
		}
	}
	if value, ok := obj["refresh"]; ok {
		if !isJSONNull(value) {
			refresh, err := decodeOAuthRefresh(value, create)
			if err != nil {
				return CredentialAuth{}, err
			}
			auth.Refresh = refresh
		}
	}
	if create && auth.MCPServerURL == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.mcp_server_url is required"}
	}
	if create && auth.AccessToken == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.access_token is required"}
	}
	return auth, nil
}

func decodeProviderAPIKey(obj map[string]json.RawMessage, create bool) (CredentialAuth, error) {
	allowed := map[string]struct{}{"type": {}, "provider_id": {}, "access_mode": {}, "token": {}}
	if err := rejectUnknownFields(obj, allowed, "auth"); err != nil {
		return CredentialAuth{}, err
	}
	auth := CredentialAuth{Type: credentialAuthTypeProviderAPIKey}
	if value, ok := obj["provider_id"]; ok {
		providerID, err := decodeRequiredString("auth.provider_id", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.ProviderID = providerID
	}
	if value, ok := obj["access_mode"]; ok {
		accessMode, err := decodeRequiredString("auth.access_mode", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessMode = accessMode
	}
	if value, ok := obj["token"]; ok {
		token, err := decodeRequiredString("auth.token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.Token = token
	}
	if create && auth.ProviderID == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.provider_id is required"}
	}
	if create && auth.AccessMode == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.access_mode is required"}
	}
	if create && auth.Token == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.token is required"}
	}
	return auth, nil
}

func decodeProviderOAuth(obj map[string]json.RawMessage, create bool) (CredentialAuth, error) {
	allowed := map[string]struct{}{"type": {}, "provider_id": {}, "access_mode": {}, "access_token": {}, "refresh_token": {}, "expires_at": {}, "account_id": {}}
	if err := rejectUnknownFields(obj, allowed, "auth"); err != nil {
		return CredentialAuth{}, err
	}
	auth := CredentialAuth{Type: credentialAuthTypeProviderOAuth}
	if value, ok := obj["provider_id"]; ok {
		providerID, err := decodeRequiredString("auth.provider_id", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.ProviderID = providerID
	}
	if value, ok := obj["access_mode"]; ok {
		accessMode, err := decodeRequiredString("auth.access_mode", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessMode = accessMode
	}
	if value, ok := obj["access_token"]; ok {
		accessToken, err := decodeRequiredString("auth.access_token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccessToken = accessToken
	}
	if value, ok := obj["refresh_token"]; ok {
		refreshToken, err := decodeRequiredString("auth.refresh_token", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.RefreshToken = refreshToken
	}
	if value, ok := obj["expires_at"]; ok {
		expiresAt, err := decodeNonEmptyString("auth.expires_at", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.ExpiresAt = expiresAt
	}
	if value, ok := obj["account_id"]; ok {
		accountID, err := decodeNonEmptyString("auth.account_id", value)
		if err != nil {
			return CredentialAuth{}, err
		}
		auth.AccountID = accountID
	}
	if create && auth.ProviderID == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.provider_id is required"}
	}
	if create && auth.AccessMode == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.access_mode is required"}
	}
	if create && auth.AccessToken == "" {
		return CredentialAuth{}, &ValidationError{Message: "auth.access_token is required"}
	}
	return auth, nil
}

func decodeOAuthRefresh(raw json.RawMessage, create bool) (*CredentialOAuthRefresh, error) {
	obj, err := decodeNestedObject("auth.refresh", raw)
	if err != nil {
		return nil, err
	}
	allowed := map[string]struct{}{"refresh_token": {}, "client_id": {}, "token_endpoint": {}, "scope": {}, "resource": {}, "token_endpoint_auth": {}}
	if err := rejectUnknownFields(obj, allowed, "auth.refresh"); err != nil {
		return nil, err
	}
	refresh := &CredentialOAuthRefresh{}
	if value, ok := obj["refresh_token"]; ok {
		refresh.RefreshToken, err = decodeRequiredString("auth.refresh.refresh_token", value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := obj["client_id"]; ok {
		refresh.ClientID, err = decodeString("auth.refresh.client_id", value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := obj["token_endpoint"]; ok {
		refresh.TokenEndpoint, err = decodeCanonicalEndpoint("auth.refresh.token_endpoint", value)
		if err != nil {
			return nil, err
		}
	}
	if value, ok := obj["scope"]; ok {
		if !isJSONNull(value) {
			refresh.Scope, err = decodeString("auth.refresh.scope", value)
			if !create && err == nil && refresh.Scope == "" {
				err = &ValidationError{Message: "auth.refresh.scope must be non-empty when provided"}
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if value, ok := obj["resource"]; ok {
		if !isJSONNull(value) {
			refresh.Resource, err = decodeString("auth.refresh.resource", value)
			if err != nil {
				return nil, err
			}
		}
	}
	if value, ok := obj["token_endpoint_auth"]; ok {
		refresh.TokenEndpointAuth, err = decodeTokenEndpointAuth(value, create)
		if err != nil {
			return nil, err
		}
	}
	if create {
		if refresh.RefreshToken == "" {
			return nil, &ValidationError{Message: "auth.refresh.refresh_token is required"}
		}
		if refresh.ClientID == "" {
			return nil, &ValidationError{Message: "auth.refresh.client_id is required"}
		}
		if refresh.TokenEndpoint == "" {
			return nil, &ValidationError{Message: "auth.refresh.token_endpoint is required"}
		}
		if refresh.TokenEndpointAuth == nil {
			return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth is required"}
		}
	}
	return refresh, nil
}

func decodeTokenEndpointAuth(raw json.RawMessage, create bool) (*CredentialTokenEndpointAuth, error) {
	obj, err := decodeNestedObject("auth.refresh.token_endpoint_auth", raw)
	if err != nil {
		return nil, err
	}
	allowed := map[string]struct{}{"type": {}, "client_secret": {}}
	if err := rejectUnknownFields(obj, allowed, "auth.refresh.token_endpoint_auth"); err != nil {
		return nil, err
	}
	rawType, ok := obj["type"]
	if !ok {
		return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth.type is required"}
	}
	authType, err := decodeString("auth.refresh.token_endpoint_auth.type", rawType)
	if err != nil {
		return nil, err
	}
	out := &CredentialTokenEndpointAuth{Type: authType}
	clientSecretProvided := false
	if value, ok := obj["client_secret"]; ok {
		clientSecretProvided = true
		out.ClientSecret, err = decodeString("auth.refresh.token_endpoint_auth.client_secret", value)
		if err != nil {
			return nil, err
		}
	}
	switch authType {
	case "none":
		if !create {
			return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth.type cannot be none on update"}
		}
		if clientSecretProvided {
			return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth.client_secret is not allowed when type is none"}
		}
	case "client_secret_basic", "client_secret_post":
		if out.ClientSecret == "" {
			return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth.client_secret is required"}
		}
	default:
		return nil, &ValidationError{Message: "auth.refresh.token_endpoint_auth.type must be none, client_secret_basic, or client_secret_post"}
	}
	return out, nil
}

func decodeNestedObject(field string, raw json.RawMessage) (map[string]json.RawMessage, error) {
	if string(raw) == "null" {
		return nil, &ValidationError{Message: field + " cannot be null"}
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, &ValidationError{Message: field + " must be an object"}
	}
	if obj == nil {
		return nil, &ValidationError{Message: field + " must be an object"}
	}
	return obj, nil
}

func decodeCanonicalEndpoint(field string, raw json.RawMessage) (string, error) {
	value, err := decodeString(field, raw)
	if err != nil {
		return "", err
	}
	canonical, err := endpointurl.CanonicalizePublicURL(value, field)
	if err != nil {
		return "", &ValidationError{Message: err.Error()}
	}
	return canonical, nil
}
