package auth

import (
	"slices"
	"strings"

	"github.com/tetral-ai/tetral/internal/workspace"
)

// Principal is an authenticated admission snapshot. Actor attribution and
// authority provenance are distinct: a derived API key still acts as that key.
type Principal struct {
	Workspace  workspace.Workspace
	APIKeyID   string
	Identity   *Identity
	Credential Credential
	Authority  Authority
}

type Identity struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

type Credential struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// Authority records the revisions checked by Auth and the immutable issuance
// ceiling. Receivers verify the signature, then apply current operation policy.
type Authority struct {
	Kind             string            `json:"kind"`
	RuleID           string            `json:"rule_id,omitempty"`
	IdentityID       string            `json:"identity_id,omitempty"`
	GrantID          string            `json:"grant_id,omitempty"`
	RuleRevision     int64             `json:"rule_revision,omitempty"`
	IdentityRevision int64             `json:"identity_revision,omitempty"`
	GrantRevision    int64             `json:"grant_revision,omitempty"`
	PolicyVersion    int64             `json:"policy_version"`
	Operations       []Operation       `json:"operations"`
	Scope            ResourceReference `json:"scope"`
}

// ResourceReference carries trusted resource facts supplied by the business
// owner. Workspace collections use only WorkspaceID; individual resources also
// identify their type and ID after a tenant-safe repository lookup.
type ResourceReference struct {
	WorkspaceID workspace.ID `json:"workspace_id"`
	Type        string       `json:"type,omitempty"`
	ID          string       `json:"id,omitempty"`
}

const (
	CredentialAPIKey        = "api_key"
	CredentialAccessToken   = "access_token"
	IdentityHuman           = "human"
	IdentityService         = "service"
	AuthorityIndependentKey = "independent_key"
	AuthorityIdentityGrant  = "identity_grant"
)

// IndependentKeyPrincipal is used only after resolving a durable independent
// key. It is an explicit trusted construction step, never a signed-token fallback.
func IndependentKeyPrincipal(ws workspace.Workspace, keyID string) Principal {
	return Principal{Workspace: ws, APIKeyID: keyID, Credential: Credential{ID: keyID, Kind: CredentialAPIKey}, Authority: Authority{
		Kind: AuthorityIndependentKey, PolicyVersion: PolicyVersion, Operations: RegisteredOperations(), Scope: ResourceReference{WorkspaceID: ws.ID, Type: "workspace"},
	}}
}

func boundedIdentity(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\x00\r\n")
}

// Validate rejects incomplete unions rather than interpreting absent fields as
// independent authority. It also bounds signed data before it reaches handlers.
func (p Principal) Validate() error {
	invalid := func() error { return &AuthenticationError{Message: "invalid internal principal"} }
	if !boundedIdentity(string(p.Workspace.ID)) || !boundedIdentity(p.Credential.ID) || p.Authority.Scope.WorkspaceID != p.Workspace.ID || p.Authority.Scope.Type != "workspace" || p.Authority.Scope.ID != "" || p.Authority.PolicyVersion != PolicyVersion {
		return invalid()
	}
	switch p.Credential.Kind {
	case CredentialAPIKey:
		if p.APIKeyID != p.Credential.ID {
			return invalid()
		}
	case CredentialAccessToken:
		if p.APIKeyID != "" || p.Authority.Kind != AuthorityIdentityGrant {
			return invalid()
		}
	default:
		return invalid()
	}
	switch p.Authority.Kind {
	case AuthorityIndependentKey:
		if p.Credential.Kind != CredentialAPIKey || p.Identity != nil || p.Authority.RuleID != "" || p.Authority.IdentityID != "" || p.Authority.GrantID != "" || p.Authority.RuleRevision != 0 || p.Authority.IdentityRevision != 0 || p.Authority.GrantRevision != 0 {
			return invalid()
		}
	case AuthorityIdentityGrant:
		if p.Identity == nil || !boundedIdentity(p.Identity.ID) || (p.Identity.Kind != IdentityHuman && p.Identity.Kind != IdentityService) || p.Authority.IdentityID != p.Identity.ID || !boundedIdentity(p.Authority.RuleID) || !boundedIdentity(p.Authority.GrantID) || p.Authority.RuleRevision <= 0 || p.Authority.IdentityRevision <= 0 || p.Authority.GrantRevision <= 0 {
			return invalid()
		}
	default:
		return invalid()
	}
	if len(p.Authority.Operations) == 0 || len(p.Authority.Operations) > len(registeredOperations) {
		return invalid()
	}
	seen := make(map[Operation]bool, len(p.Authority.Operations))
	for _, op := range p.Authority.Operations {
		if !IsRegisteredOperation(op) || seen[op] {
			return invalid()
		}
		seen[op] = true
	}
	return nil
}

// PermissionError is an authenticated denial, distinct from credential failure.
type PermissionError struct{}

func (*PermissionError) Error() string { return "permission denied" }

// Authorize is the shared public-operation gate. Resource ownership remains the
// responsibility of the owning repository; a foreign workspace fails closed.
func Authorize(p Principal, operation Operation, resource ResourceReference) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if !IsRegisteredOperation(operation) || !slices.Contains(p.Authority.Operations, operation) || resource.WorkspaceID != p.Authority.Scope.WorkspaceID || !validResourceReference(resource) {
		return &PermissionError{}
	}
	return nil
}

// These resource kinds are owned by the public operation registry. Workspace is
// a typed collection scope; individual references require an actual resource ID.
func validResourceReference(resource ResourceReference) bool {
	if !boundedIdentity(string(resource.WorkspaceID)) {
		return false
	}
	switch resource.Type {
	case "workspace":
		return resource.ID == ""
	case "session", "thread", "session_resource", "agent", "agent_version", "environment", "vault", "credential", "file", "memory_store", "memory", "memory_version", "skill", "skill_version", "model", "api_key":
		return boundedIdentity(resource.ID)
	default:
		return false
	}
}
