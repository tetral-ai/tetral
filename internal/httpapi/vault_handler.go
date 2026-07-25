package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/tetral-ai/tetral/internal/vault"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// VaultService is the workspace-aware Vault business boundary used by the HTTP
// handler. *vault.Service satisfies it; tests may substitute their own
// implementation as long as it preserves the workspace-scoped signatures.
type VaultService interface {
	CreateVault(ctx context.Context, ws workspace.ID, request vault.CreateVaultRequest) (*vault.Vault, error)
	GetVault(ctx context.Context, ws workspace.ID, vaultID string) (*vault.Vault, error)
	ListVaults(ctx context.Context, ws workspace.ID, options vault.ListOptions) (vault.VaultListResult, error)
	UpdateVault(ctx context.Context, ws workspace.ID, vaultID string, patch vault.VaultPatch) (*vault.Vault, error)
	ArchiveVault(ctx context.Context, ws workspace.ID, vaultID string) (*vault.Vault, error)
	DeleteVault(ctx context.Context, ws workspace.ID, vaultID string) (*vault.DeleteResult, error)
	CreateCredential(ctx context.Context, ws workspace.ID, vaultID string, request vault.CreateCredentialRequest) (*vault.CredentialMetadata, error)
	GetCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*vault.CredentialMetadata, error)
	ListCredentials(ctx context.Context, ws workspace.ID, vaultID string, options vault.ListOptions) (vault.CredentialListResult, error)
	UpdateCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string, patch vault.CredentialPatch) (*vault.CredentialMetadata, error)
	ValidateMCPOAuthCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*vault.CredentialValidation, error)
	ArchiveCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*vault.CredentialMetadata, error)
	DeleteCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*vault.DeleteResult, error)
}

// VaultHandler groups HTTP handler dependencies for vault and credential routes.
type VaultHandler struct {
	service VaultService
}

// NewVaultHandler creates a new VaultHandler.
func NewVaultHandler(service VaultService) *VaultHandler {
	return &VaultHandler{service: service}
}

func (h *VaultHandler) createVault(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readVaultRequestBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := vault.DecodeCreateVaultRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.CreateVault(r.Context(), ws, request)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) getVault(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.GetVault(r.Context(), ws, vaultID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) listVaults(w http.ResponseWriter, r *http.Request) {
	options, err := parseVaultListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ListVaults(r.Context(), ws, options)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) updateVault(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readVaultRequestBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	patch, err := vault.DecodeUpdateVaultRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.UpdateVault(r.Context(), ws, vaultID, patch)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) archiveVault(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ArchiveVault(r.Context(), ws, vaultID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) deleteVault(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.DeleteVault(r.Context(), ws, vaultID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) createCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readVaultRequestBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	request, err := vault.DecodeCreateCredentialRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}

	result, err := h.service.CreateCredential(r.Context(), ws, vaultID, request)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) getCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	credentialID := chi.URLParam(r, "credential_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.GetCredential(r.Context(), ws, vaultID, credentialID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) listCredentials(w http.ResponseWriter, r *http.Request) {
	vaultID := chi.URLParam(r, "vault_id")
	options, err := parseVaultListOptions(r)
	if err != nil {
		writeError(w, r, err)
		return
	}

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}

	result, err := h.service.ListCredentials(r.Context(), ws, vaultID, options)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) updateCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	body, err := readVaultRequestBody(w, r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	patch, err := vault.DecodeUpdateCredentialRequest(body)
	if err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	credentialID := chi.URLParam(r, "credential_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.UpdateCredential(r.Context(), ws, vaultID, credentialID, patch)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) archiveCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	credentialID := chi.URLParam(r, "credential_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ArchiveCredential(r.Context(), ws, vaultID, credentialID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) validateMCPOAuthCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	credentialID := chi.URLParam(r, "credential_id")
	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.ValidateMCPOAuthCredential(r.Context(), ws, vaultID, credentialID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *VaultHandler) deleteCredential(w http.ResponseWriter, r *http.Request) {
	if _, err := requireBetaQuery(r); err != nil {
		writeError(w, r, err)
		return
	}
	vaultID := chi.URLParam(r, "vault_id")
	credentialID := chi.URLParam(r, "credential_id")

	ws, err := requestWorkspace(r.Context())
	if err != nil {
		writeError(w, r, err)
		return
	}
	result, err := h.service.DeleteCredential(r.Context(), ws, vaultID, credentialID)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func readVaultRequestBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, &RequestTooLargeError{Message: "request body too large"}
		}
		return nil, &ValidationError{Message: "invalid request body"}
	}
	return body, nil
}

func parseVaultListOptions(r *http.Request) (vault.ListOptions, error) {
	values, err := requireBetaQuery(r, "limit", "page", "include_archived")
	if err != nil {
		return vault.ListOptions{}, err
	}
	for key, value := range values {
		if len(value) > 1 {
			return vault.ListOptions{}, &vault.ValidationError{Message: "duplicate query parameter " + key}
		}
	}

	options := vault.ListOptions{Limit: 20}
	if rawValues, ok := values["limit"]; ok {
		raw := rawValues[0]
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			return vault.ListOptions{}, &vault.ValidationError{Message: "limit must be a positive integer"}
		}
		if limit > 100 {
			limit = 100
		}
		options.Limit = limit
	}
	if rawValues, ok := values["page"]; ok {
		options.Page = rawValues[0]
		if options.Page == "" {
			return vault.ListOptions{}, &vault.ValidationError{Message: "page must be a non-empty token"}
		}
	}
	if rawValues, ok := values["include_archived"]; ok {
		switch rawValues[0] {
		case "true":
			options.IncludeArchived = true
		case "false":
			options.IncludeArchived = false
		default:
			return vault.ListOptions{}, &vault.ValidationError{Message: "include_archived must be true or false"}
		}
	}
	return options, nil
}
