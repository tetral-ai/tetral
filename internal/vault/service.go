package vault

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type VaultStoreBackend interface {
	Create(context.Context, workspace.ID, CreateVaultRequest) (*Vault, error)
	Get(context.Context, workspace.ID, string) (*Vault, error)
	List(context.Context, workspace.ID, ListOptions) (VaultListResult, error)
	Update(context.Context, workspace.ID, string, VaultPatch) (*Vault, error)
	Archive(context.Context, workspace.ID, string) (*Vault, error)
	Delete(context.Context, workspace.ID, string) (*DeleteResult, error)
}

type CredentialStoreBackend interface {
	Create(context.Context, workspace.ID, string, CreateCredentialRequest) (*CredentialMetadata, error)
	GetMetadata(context.Context, workspace.ID, string, string) (*CredentialMetadata, error)
	GetSecret(context.Context, workspace.ID, string, string) (*CredentialMetadata, *CredentialAuth, error)
	List(context.Context, workspace.ID, string, ListOptions) (CredentialListResult, error)
	Update(context.Context, workspace.ID, string, string, CredentialPatch) (*CredentialMetadata, error)
	UpdateWithLockedCredential(context.Context, workspace.ID, string, string, LockedCredentialPatchFunc) (*CredentialMetadata, error)
	Archive(context.Context, workspace.ID, string, string) (*CredentialMetadata, error)
	Delete(context.Context, workspace.ID, string, string) (*DeleteResult, error)
}

type MCPOAuthValidator interface {
	Validate(context.Context, MCPOAuthValidationInput) (MCPOAuthValidationResult, error)
}

type ServiceOption func(*Service)

// Service is the Vault business boundary shared by HTTP handlers and internal
// Engine consumers.
type Service struct {
	vaults            VaultStoreBackend
	credentials       CredentialStoreBackend
	mcpOAuthValidator MCPOAuthValidator
}

func NewService(vaults VaultStoreBackend, credentials CredentialStoreBackend, opts ...ServiceOption) *Service {
	service := &Service{
		vaults:            vaults,
		credentials:       credentials,
		mcpOAuthValidator: HTTPMCPOAuthValidator{},
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

func WithMCPOAuthValidator(validator MCPOAuthValidator) ServiceOption {
	return func(service *Service) {
		if validator != nil {
			service.mcpOAuthValidator = validator
		}
	}
}

func (s *Service) CreateVault(ctx context.Context, ws workspace.ID, request CreateVaultRequest) (*Vault, error) {
	return s.vaults.Create(ctx, ws, request)
}

func (s *Service) GetVault(ctx context.Context, ws workspace.ID, vaultID string) (*Vault, error) {
	return s.vaults.Get(ctx, ws, vaultID)
}

func (s *Service) ListVaults(ctx context.Context, ws workspace.ID, options ListOptions) (VaultListResult, error) {
	return s.vaults.List(ctx, ws, options)
}

func (s *Service) UpdateVault(ctx context.Context, ws workspace.ID, vaultID string, patch VaultPatch) (*Vault, error) {
	return s.vaults.Update(ctx, ws, vaultID, patch)
}

func (s *Service) ArchiveVault(ctx context.Context, ws workspace.ID, vaultID string) (*Vault, error) {
	return s.vaults.Archive(ctx, ws, vaultID)
}

func (s *Service) DeleteVault(ctx context.Context, ws workspace.ID, vaultID string) (*DeleteResult, error) {
	return s.vaults.Delete(ctx, ws, vaultID)
}

func (s *Service) CreateCredential(ctx context.Context, ws workspace.ID, vaultID string, request CreateCredentialRequest) (*CredentialMetadata, error) {
	if err := s.validateLiveVault(ctx, ws, vaultID); err != nil {
		return nil, err
	}
	return s.credentials.Create(ctx, ws, vaultID, request)
}

func (s *Service) GetCredential(ctx context.Context, ws workspace.ID, vaultID string, credentialID string) (*CredentialMetadata, error) {
	return s.credentials.GetMetadata(ctx, ws, vaultID, credentialID)
}

func (s *Service) ListCredentials(ctx context.Context, ws workspace.ID, vaultID string, options ListOptions) (CredentialListResult, error) {
	if err := s.validateVaultExists(ctx, ws, vaultID); err != nil {
		return CredentialListResult{}, err
	}
	return s.credentials.List(ctx, ws, vaultID, options)
}

func (s *Service) UpdateCredential(ctx context.Context, ws workspace.ID, vaultID string, credentialID string, patch CredentialPatch) (*CredentialMetadata, error) {
	return s.credentials.Update(ctx, ws, vaultID, credentialID, patch)
}

// ValidateMCPOAuthCredential validates one mcp_oauth credential and, when the
// stored access token is due for refresh, rotates it in place.
//
// The refresh is single-flight per credential row. It runs through the
// credential store's UpdateWithLockedCredential, which takes a row-level
// SELECT ... FOR UPDATE lock on the credential (getCredentialForUpdateTx) and
// holds it across the external validator call. At most one refresh is in flight
// for a given row at a time. A second caller that also finds the token expired
// blocks on that lock rather than issuing its own refresh; once the in-flight
// refresh commits, the waiter re-reads the rotated row, the validator sees a
// token that is no longer due (mcpOAuthRefreshDue), and it returns the
// already-rotated result with no second external refresh and no second durable
// write.
//
// INVARIANTS:
//   - At most one external refresh is in flight per credential row.
//   - A blocked waiter issues no external refresh and no durable write once the
//     winning refresh is committed.
//
// UPDATE-WITH:
//   - internal/vault/postgresql_store.go: UpdateWithLockedCredential and
//     getCredentialForUpdateTx
//   - internal/vault/validation.go: mcpOAuthRefreshDue and
//     HTTPMCPOAuthValidator.Validate
func (s *Service) ValidateMCPOAuthCredential(ctx context.Context, ws workspace.ID, vaultID string, credentialID string) (*CredentialValidation, error) {
	var validation *CredentialValidation
	_, err := s.credentials.UpdateWithLockedCredential(ctx, ws, vaultID, credentialID, func(current CredentialAuth) (*CredentialPatch, error) {
		if current.Type != credentialAuthTypeMCPOAuth {
			return nil, &ValidationError{Message: "credential auth.type must be mcp_oauth"}
		}
		if current.MCPServerURL == "" || current.AccessToken == "" {
			return nil, &ValidationError{Message: "credential secret is not available"}
		}
		result, err := s.mcpOAuthValidator.Validate(ctx, MCPOAuthValidationInput{
			Credential: CredentialMetadata{ID: credentialID, Type: "vault_credential", VaultID: vaultID},
			Auth:       current,
		})
		if err != nil {
			return nil, err
		}
		if result.Validation == nil {
			return nil, &ValidationError{Message: "credential validation result is required"}
		}
		validation = result.Validation
		if result.RefreshedAuth == nil {
			return nil, nil
		}
		return credentialRefreshPatch(*result.RefreshedAuth)
	})
	if err != nil {
		return nil, err
	}
	return validation, nil
}

func (s *Service) ArchiveCredential(ctx context.Context, ws workspace.ID, vaultID string, credentialID string) (*CredentialMetadata, error) {
	return s.credentials.Archive(ctx, ws, vaultID, credentialID)
}

func (s *Service) DeleteCredential(ctx context.Context, ws workspace.ID, vaultID string, credentialID string) (*DeleteResult, error) {
	return s.credentials.Delete(ctx, ws, vaultID, credentialID)
}

func (s *Service) ValidateVaultReferences(ctx context.Context, ws workspace.ID, vaultIDs []string) error {
	for _, vaultID := range vaultIDs {
		if err := s.validateLiveVault(ctx, ws, vaultID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) validateVaultExists(ctx context.Context, ws workspace.ID, vaultID string) error {
	_, err := s.vaults.Get(ctx, ws, vaultID)
	return err
}

func (s *Service) validateLiveVault(ctx context.Context, ws workspace.ID, vaultID string) error {
	v, err := s.vaults.Get(ctx, ws, vaultID)
	if err != nil {
		return err
	}
	if v.ArchivedAt != nil {
		return &ValidationError{Message: "vault is archived"}
	}
	return nil
}
