package vault

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workspace"
)

type fakeVaultStoreBackend struct {
	vaults  map[workspace.ID]map[string]*Vault
	errs    map[workspace.ID]map[string]error
	calls   []vaultLookupCall
	callLog *[]string

	createCalled  bool
	listCalled    bool
	updateCalled  bool
	archiveCalled bool
	deleteCalled  bool
}

func newFakeVaultStoreBackend() *fakeVaultStoreBackend {
	return &fakeVaultStoreBackend{
		vaults: map[workspace.ID]map[string]*Vault{},
		errs:   map[workspace.ID]map[string]error{},
	}
}

type vaultLookupCall struct {
	WorkspaceID workspace.ID
	VaultID     string
}

func (s *fakeVaultStoreBackend) setVault(ws workspace.ID, vaultID string, v *Vault) {
	if s.vaults[ws] == nil {
		s.vaults[ws] = map[string]*Vault{}
	}
	s.vaults[ws][vaultID] = v
}

func (s *fakeVaultStoreBackend) setErr(ws workspace.ID, vaultID string, err error) {
	if s.errs[ws] == nil {
		s.errs[ws] = map[string]error{}
	}
	s.errs[ws][vaultID] = err
}

func (s *fakeVaultStoreBackend) Create(context.Context, workspace.ID, CreateVaultRequest) (*Vault, error) {
	s.createCalled = true
	return &Vault{ID: "vlt_created", Type: "vault"}, nil
}

func (s *fakeVaultStoreBackend) Get(_ context.Context, ws workspace.ID, vaultID string) (*Vault, error) {
	s.calls = append(s.calls, vaultLookupCall{WorkspaceID: ws, VaultID: vaultID})
	if s.callLog != nil {
		*s.callLog = append(*s.callLog, "vault.get:"+string(ws)+":"+vaultID)
	}
	if err := s.errs[ws][vaultID]; err != nil {
		return nil, err
	}
	if v, ok := s.vaults[ws][vaultID]; ok {
		return v, nil
	}
	return nil, &NotFoundError{Message: "vault " + vaultID + " not found"}
}

func (s *fakeVaultStoreBackend) List(context.Context, workspace.ID, ListOptions) (VaultListResult, error) {
	s.listCalled = true
	return VaultListResult{Data: []*Vault{{ID: "vlt_listed", Type: "vault"}}}, nil
}

func (s *fakeVaultStoreBackend) Update(context.Context, workspace.ID, string, VaultPatch) (*Vault, error) {
	s.updateCalled = true
	return &Vault{ID: "vlt_updated", Type: "vault"}, nil
}

func (s *fakeVaultStoreBackend) Archive(context.Context, workspace.ID, string) (*Vault, error) {
	s.archiveCalled = true
	return &Vault{ID: "vlt_archived", Type: "vault"}, nil
}

func (s *fakeVaultStoreBackend) Delete(context.Context, workspace.ID, string) (*DeleteResult, error) {
	s.deleteCalled = true
	return &DeleteResult{ID: "vlt_deleted", Type: "vault_deleted"}, nil
}

type fakeCredentialStoreBackend struct {
	t                  *testing.T
	failIfTouched      bool
	callLog            *[]string
	createCalled       bool
	getCalled          bool
	getSecretCalled    bool
	listCalled         bool
	updateCalled       bool
	updateLockedCalled bool
	archiveCalled      bool
	deleteCalled       bool
	listVaultID        string
	listOptions        ListOptions
	secretMeta         *CredentialMetadata
	secretAuth         *CredentialAuth
	lockedCurrent      CredentialAuth
	lockedPatch        *CredentialPatch
}

func (s *fakeCredentialStoreBackend) touch(method string) {
	if s.failIfTouched {
		s.t.Fatalf("credential backend %s must not be called", method)
	}
}

func (s *fakeCredentialStoreBackend) Create(context.Context, workspace.ID, string, CreateCredentialRequest) (*CredentialMetadata, error) {
	s.touch("Create")
	if s.callLog != nil {
		*s.callLog = append(*s.callLog, "credential.create")
	}
	s.createCalled = true
	return &CredentialMetadata{ID: "cred_created", Type: "vault_credential"}, nil
}

func (s *fakeCredentialStoreBackend) GetMetadata(context.Context, workspace.ID, string, string) (*CredentialMetadata, error) {
	s.touch("GetMetadata")
	s.getCalled = true
	return &CredentialMetadata{ID: "cred_got", Type: "vault_credential"}, nil
}

func (s *fakeCredentialStoreBackend) GetSecret(context.Context, workspace.ID, string, string) (*CredentialMetadata, *CredentialAuth, error) {
	s.touch("GetSecret")
	s.getSecretCalled = true
	if s.secretMeta != nil && s.secretAuth != nil {
		return s.secretMeta, s.secretAuth, nil
	}
	return &CredentialMetadata{ID: "cred_secret", Type: "vault_credential", VaultID: "vlt_test"}, &CredentialAuth{Type: credentialAuthTypeMCPOAuth, MCPServerURL: "https://mcp.example.com/", AccessToken: "access"}, nil
}

func (s *fakeCredentialStoreBackend) List(_ context.Context, _ workspace.ID, vaultID string, options ListOptions) (CredentialListResult, error) {
	s.touch("List")
	if s.callLog != nil {
		*s.callLog = append(*s.callLog, "credential.list")
	}
	s.listCalled = true
	s.listVaultID = vaultID
	s.listOptions = options
	return CredentialListResult{Data: []*Credential{{ID: "cred_listed", Type: "vault_credential"}}}, nil
}

func (s *fakeCredentialStoreBackend) Update(context.Context, workspace.ID, string, string, CredentialPatch) (*CredentialMetadata, error) {
	s.touch("Update")
	s.updateCalled = true
	return &CredentialMetadata{ID: "cred_updated", Type: "vault_credential"}, nil
}

func (s *fakeCredentialStoreBackend) UpdateWithLockedCredential(_ context.Context, _ workspace.ID, _ string, _ string, buildPatch LockedCredentialPatchFunc) (*CredentialMetadata, error) {
	s.touch("UpdateWithLockedCredential")
	s.updateLockedCalled = true
	current := s.lockedCurrent
	if current.Type == "" {
		current = CredentialAuth{
			Type:        credentialAuthTypeMCPOAuth,
			AccessToken: "old-access",
			ExpiresAt:   "2026-05-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				RefreshToken: "old-refresh",
			},
		}
	}
	patch, err := buildPatch(current)
	if err != nil {
		return nil, err
	}
	s.lockedPatch = patch
	return &CredentialMetadata{ID: "cred_locked", Type: "vault_credential"}, nil
}

func (s *fakeCredentialStoreBackend) Archive(context.Context, workspace.ID, string, string) (*CredentialMetadata, error) {
	s.touch("Archive")
	s.archiveCalled = true
	return &CredentialMetadata{ID: "cred_archived", Type: "vault_credential"}, nil
}

func (s *fakeCredentialStoreBackend) Delete(context.Context, workspace.ID, string, string) (*DeleteResult, error) {
	s.touch("Delete")
	s.deleteCalled = true
	return &DeleteResult{ID: "cred_deleted", Type: "vault_credential_deleted"}, nil
}

func TestServiceLifecycleDelegates(t *testing.T) {
	vaults := newFakeVaultStoreBackend()
	credentials := &fakeCredentialStoreBackend{t: t}
	service := NewService(vaults, credentials)
	ctx := context.Background()

	if _, err := service.CreateVault(ctx, workspace.DefaultID, CreateVaultRequest{DisplayName: "vault"}); err != nil {
		t.Fatalf("CreateVault: %v", err)
	}
	if _, err := service.ListVaults(ctx, workspace.DefaultID, ListOptions{}); err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	if _, err := service.UpdateVault(ctx, workspace.DefaultID, "vlt_test", VaultPatch{}); err != nil {
		t.Fatalf("UpdateVault: %v", err)
	}
	if _, err := service.ArchiveVault(ctx, workspace.DefaultID, "vlt_test"); err != nil {
		t.Fatalf("ArchiveVault: %v", err)
	}
	if _, err := service.DeleteVault(ctx, workspace.DefaultID, "vlt_test"); err != nil {
		t.Fatalf("DeleteVault: %v", err)
	}
	if !vaults.createCalled || !vaults.listCalled || !vaults.updateCalled || !vaults.archiveCalled || !vaults.deleteCalled {
		t.Fatalf("vault lifecycle delegation incomplete: %+v", vaults)
	}

	if _, err := service.GetCredential(ctx, workspace.DefaultID, "vlt_test", "cred_test"); err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if _, err := service.UpdateCredential(ctx, workspace.DefaultID, "vlt_test", "cred_test", CredentialPatch{}); err != nil {
		t.Fatalf("UpdateCredential: %v", err)
	}
	if _, err := service.ArchiveCredential(ctx, workspace.DefaultID, "vlt_test", "cred_test"); err != nil {
		t.Fatalf("ArchiveCredential: %v", err)
	}
	if _, err := service.DeleteCredential(ctx, workspace.DefaultID, "vlt_test", "cred_test"); err != nil {
		t.Fatalf("DeleteCredential: %v", err)
	}
	if !credentials.getCalled || !credentials.updateCalled || !credentials.archiveCalled || !credentials.deleteCalled {
		t.Fatalf("credential lifecycle delegation incomplete: %+v", credentials)
	}
}

type fakeMCPOAuthValidator struct {
	called bool
	input  MCPOAuthValidationInput
	result MCPOAuthValidationResult
	err    error
}

func (v *fakeMCPOAuthValidator) Validate(_ context.Context, input MCPOAuthValidationInput) (MCPOAuthValidationResult, error) {
	v.called = true
	v.input = input
	return v.result, v.err
}

func TestServiceValidateMCPOAuthCredentialPersistsRefreshedMaterialThroughLockedUpdate(t *testing.T) {
	ctx := context.Background()
	credentials := &fakeCredentialStoreBackend{
		t: t,
		secretMeta: &CredentialMetadata{
			ID:      "cred_oauth",
			Type:    "vault_credential",
			VaultID: "vlt_test",
		},
		secretAuth: &CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://mcp.example.com/",
			AccessToken:  "old-access",
			ExpiresAt:    "2026-05-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				RefreshToken: "old-refresh",
			},
		},
		lockedCurrent: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://mcp.example.com/",
			AccessToken:  "old-access",
			ExpiresAt:    "2026-05-01T00:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				RefreshToken: "old-refresh",
			},
		},
	}
	validator := &fakeMCPOAuthValidator{
		result: MCPOAuthValidationResult{
			Validation: &CredentialValidation{
				Type:         "vault_credential_validation",
				CredentialID: "cred_oauth",
				VaultID:      "vlt_test",
				Status:       "valid",
				Refresh:      &CredentialValidationCheck{Status: "succeeded", HTTPResponse: &CredentialValidationHTTPResponse{StatusCode: 200}},
			},
			RefreshedAuth: &CredentialAuth{
				Type:        credentialAuthTypeMCPOAuth,
				AccessToken: "new-access",
				ExpiresAt:   "2026-05-01T01:00:00Z",
				Refresh: &CredentialOAuthRefresh{
					RefreshToken: "new-refresh",
				},
			},
		},
	}
	service := NewService(newFakeVaultStoreBackend(), credentials, WithMCPOAuthValidator(validator))

	validation, err := service.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, "vlt_test", "cred_oauth")
	if err != nil {
		t.Fatalf("ValidateMCPOAuthCredential: %v", err)
	}
	if validation.Status != "valid" || validation.Refresh == nil || validation.Refresh.Status != "succeeded" ||
		!validator.called || credentials.getSecretCalled || !credentials.updateLockedCalled {
		t.Fatalf("validation flow incomplete: validation=%+v validator=%v credentials=%+v", validation, validator.called, credentials)
	}
	if validator.input.Auth.AccessToken != "old-access" {
		t.Fatalf("validator input access token = %q", validator.input.Auth.AccessToken)
	}
	if credentials.lockedPatch == nil || credentials.lockedPatch.Auth == nil {
		t.Fatalf("locked update patch missing: %+v", credentials.lockedPatch)
	}
	rawAuth, err := credentialPatchAuthObject(*credentials.lockedPatch)
	if err != nil {
		t.Fatalf("credentialPatchAuthObject: %v", err)
	}
	if _, ok := rawAuth["access_token"]; !ok || credentials.lockedPatch.Auth.AccessToken != "new-access" {
		t.Fatalf("access token patch = %+v raw=%v", credentials.lockedPatch.Auth, rawAuth)
	}
	if _, ok := rawAuth["expires_at"]; !ok || credentials.lockedPatch.Auth.ExpiresAt != "2026-05-01T01:00:00Z" {
		t.Fatalf("expires patch = %+v raw=%v", credentials.lockedPatch.Auth, rawAuth)
	}
	refreshRaw, err := credentialPatchRefreshObject(rawAuth)
	if err != nil {
		t.Fatalf("credentialPatchRefreshObject: %v", err)
	}
	if _, ok := refreshRaw["refresh_token"]; !ok || credentials.lockedPatch.Auth.Refresh.RefreshToken != "new-refresh" {
		t.Fatalf("refresh patch = %+v raw=%v", credentials.lockedPatch.Auth.Refresh, refreshRaw)
	}
}

func TestServiceValidateMCPOAuthCredentialUsesAuthoritativeLockedSecret(t *testing.T) {
	ctx := context.Background()
	credentials := &fakeCredentialStoreBackend{
		t: t,
		secretMeta: &CredentialMetadata{
			ID:      "cred_oauth",
			Type:    "vault_credential",
			VaultID: "vlt_test",
		},
		secretAuth: &CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://mcp.example.com/",
			AccessToken:  "old-access",
			Refresh: &CredentialOAuthRefresh{
				RefreshToken: "old-refresh",
			},
		},
		lockedCurrent: CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: "https://authoritative.example.com/mcp",
			AccessToken:  "rotated-by-other-worker",
			ExpiresAt:    "2026-05-01T02:00:00Z",
			Refresh: &CredentialOAuthRefresh{
				RefreshToken: "already-rotated-refresh",
			},
		},
	}
	validator := &fakeMCPOAuthValidator{
		result: MCPOAuthValidationResult{
			Validation: &CredentialValidation{Type: "vault_credential_validation", CredentialID: "cred_oauth", VaultID: "vlt_test", Status: "valid"},
		},
	}
	service := NewService(newFakeVaultStoreBackend(), credentials, WithMCPOAuthValidator(validator))

	if _, err := service.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, "vlt_test", "cred_oauth"); err != nil {
		t.Fatalf("ValidateMCPOAuthCredential: %v", err)
	}
	if !credentials.updateLockedCalled || credentials.getSecretCalled {
		t.Fatalf("locked=%v get-secret=%v; want true,false", credentials.updateLockedCalled, credentials.getSecretCalled)
	}
	if credentials.lockedPatch != nil {
		t.Fatalf("authoritative fresh validation wrote a patch: %+v", credentials.lockedPatch)
	}
	if validator.input.Auth.AccessToken != "rotated-by-other-worker" || validator.input.Auth.Refresh == nil ||
		validator.input.Auth.Refresh.RefreshToken != "already-rotated-refresh" {
		t.Fatalf("validator input = %+v; want authoritative locked material", validator.input.Auth)
	}
}

func TestServiceCreateCredentialRequiresLiveParentVault(t *testing.T) {
	ctx := context.Background()
	archivedAt := time.Now().UTC()

	tests := []struct {
		name              string
		parent            *Vault
		parentErr         error
		createShouldCall  bool
		wantValidationErr bool
		wantNotFoundErr   bool
	}{
		{
			name:             "valid parent",
			parent:           &Vault{ID: "vlt_parent", Type: "vault"},
			createShouldCall: true,
		},
		{
			name:            "missing parent",
			parentErr:       &NotFoundError{Message: "vault vlt_parent not found"},
			wantNotFoundErr: true,
		},
		{
			name:              "archived parent",
			parent:            &Vault{ID: "vlt_parent", Type: "vault", ArchivedAt: &archivedAt},
			wantValidationErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callLog := []string{}
			vaults := newFakeVaultStoreBackend()
			vaults.callLog = &callLog
			if tc.parent != nil {
				vaults.setVault(workspace.DefaultID, "vlt_parent", tc.parent)
			}
			if tc.parentErr != nil {
				vaults.setErr(workspace.DefaultID, "vlt_parent", tc.parentErr)
			}
			credentials := &fakeCredentialStoreBackend{t: t, callLog: &callLog}
			service := NewService(vaults, credentials)

			_, err := service.CreateCredential(ctx, workspace.DefaultID, "vlt_parent", CreateCredentialRequest{})
			assertServiceParentResult(t, err, tc.wantValidationErr, tc.wantNotFoundErr)
			if credentials.createCalled != tc.createShouldCall {
				t.Fatalf("credential Create called = %v; want %v", credentials.createCalled, tc.createShouldCall)
			}
			wantLookupCalls := []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_parent"}}
			if !reflect.DeepEqual(vaults.calls, wantLookupCalls) {
				t.Fatalf("vault lookup calls = %v; want %v", vaults.calls, wantLookupCalls)
			}
			wantCallLog := []string{"vault.get:default:vlt_parent"}
			if tc.createShouldCall {
				wantCallLog = append(wantCallLog, "credential.create")
			}
			if !reflect.DeepEqual(callLog, wantCallLog) {
				t.Fatalf("call log = %v; want %v", callLog, wantCallLog)
			}
		})
	}
}

func TestServiceListCredentialsRequiresExistingParentVault(t *testing.T) {
	ctx := context.Background()
	archivedAt := time.Now().UTC()

	tests := []struct {
		name            string
		parent          *Vault
		parentErr       error
		options         ListOptions
		listShouldCall  bool
		wantNotFoundErr bool
	}{
		{
			name:           "valid parent",
			parent:         &Vault{ID: "vlt_parent", Type: "vault"},
			listShouldCall: true,
		},
		{
			name:            "missing parent",
			parentErr:       &NotFoundError{Message: "vault vlt_parent not found"},
			wantNotFoundErr: true,
		},
		{
			name:           "archived parent with archived credentials included",
			parent:         &Vault{ID: "vlt_parent", Type: "vault", ArchivedAt: &archivedAt},
			options:        ListOptions{IncludeArchived: true},
			listShouldCall: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			callLog := []string{}
			vaults := newFakeVaultStoreBackend()
			vaults.callLog = &callLog
			if tc.parent != nil {
				vaults.setVault(workspace.DefaultID, "vlt_parent", tc.parent)
			}
			if tc.parentErr != nil {
				vaults.setErr(workspace.DefaultID, "vlt_parent", tc.parentErr)
			}
			credentials := &fakeCredentialStoreBackend{t: t, callLog: &callLog}
			service := NewService(vaults, credentials)

			_, err := service.ListCredentials(ctx, workspace.DefaultID, "vlt_parent", tc.options)
			assertServiceParentResult(t, err, false, tc.wantNotFoundErr)
			if credentials.listCalled != tc.listShouldCall {
				t.Fatalf("credential List called = %v; want %v", credentials.listCalled, tc.listShouldCall)
			}
			if credentials.listOptions != tc.options {
				t.Fatalf("credential List options = %+v; want %+v", credentials.listOptions, tc.options)
			}
			wantLookupCalls := []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_parent"}}
			if !reflect.DeepEqual(vaults.calls, wantLookupCalls) {
				t.Fatalf("vault lookup calls = %v; want %v", vaults.calls, wantLookupCalls)
			}
			wantCallLog := []string{"vault.get:default:vlt_parent"}
			if tc.listShouldCall {
				wantCallLog = append(wantCallLog, "credential.list")
			}
			if !reflect.DeepEqual(callLog, wantCallLog) {
				t.Fatalf("call log = %v; want %v", callLog, wantCallLog)
			}
		})
	}
}

func TestServiceValidateVaultReferences(t *testing.T) {
	ctx := context.Background()
	archivedAt := time.Now().UTC()

	tests := []struct {
		name              string
		workspaceID       workspace.ID
		vaultIDs          []string
		seedVaults        map[workspace.ID]map[string]*Vault
		wantCalls         []vaultLookupCall
		wantValidationErr bool
		wantNotFoundErr   bool
	}{
		{
			name:        "nil no op",
			workspaceID: workspace.DefaultID,
			vaultIDs:    nil,
			wantCalls:   nil,
		},
		{
			name:        "empty no op",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{},
			wantCalls:   nil,
		},
		{
			name:        "valid",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{"vlt_a"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.DefaultID: {"vlt_a": {ID: "vlt_a", Type: "vault"}},
			},
			wantCalls: []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_a"}},
		},
		{
			name:        "supplied workspace",
			workspaceID: workspace.ID("workspace_b"),
			vaultIDs:    []string{"vlt_b"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.ID("workspace_b"): {"vlt_b": {ID: "vlt_b", Type: "vault"}},
			},
			wantCalls: []vaultLookupCall{{WorkspaceID: workspace.ID("workspace_b"), VaultID: "vlt_b"}},
		},
		{
			name:        "cross workspace invisible",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{"vlt_other_workspace"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.ID("workspace_b"): {"vlt_other_workspace": {ID: "vlt_other_workspace", Type: "vault"}},
			},
			wantCalls:       []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_other_workspace"}},
			wantNotFoundErr: true,
		},
		{
			workspaceID:     workspace.DefaultID,
			name:            "empty string is not skipped",
			vaultIDs:        []string{""},
			wantCalls:       []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: ""}},
			wantNotFoundErr: true,
		},
		{
			name:        "missing after valid",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{"vlt_a", "vlt_missing"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.DefaultID: {"vlt_a": {ID: "vlt_a", Type: "vault"}},
			},
			wantCalls:       []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_a"}, {WorkspaceID: workspace.DefaultID, VaultID: "vlt_missing"}},
			wantNotFoundErr: true,
		},
		{
			name:        "archived after valid",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{"vlt_a", "vlt_archived"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.DefaultID: {
					"vlt_a":        {ID: "vlt_a", Type: "vault"},
					"vlt_archived": {ID: "vlt_archived", Type: "vault", ArchivedAt: &archivedAt},
				},
			},
			wantCalls:         []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_a"}, {WorkspaceID: workspace.DefaultID, VaultID: "vlt_archived"}},
			wantValidationErr: true,
		},
		{
			name:        "preserves order",
			workspaceID: workspace.DefaultID,
			vaultIDs:    []string{"vlt_a", "vlt_b"},
			seedVaults: map[workspace.ID]map[string]*Vault{
				workspace.DefaultID: {
					"vlt_a": {ID: "vlt_a", Type: "vault"},
					"vlt_b": {ID: "vlt_b", Type: "vault"},
				},
			},
			wantCalls: []vaultLookupCall{{WorkspaceID: workspace.DefaultID, VaultID: "vlt_a"}, {WorkspaceID: workspace.DefaultID, VaultID: "vlt_b"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vaults := newFakeVaultStoreBackend()
			for ws, vaultsByID := range tc.seedVaults {
				for vaultID, v := range vaultsByID {
					vaults.setVault(ws, vaultID, v)
				}
			}
			credentials := &fakeCredentialStoreBackend{t: t, failIfTouched: true}
			service := NewService(vaults, credentials)

			err := service.ValidateVaultReferences(ctx, tc.workspaceID, tc.vaultIDs)
			assertServiceParentResult(t, err, tc.wantValidationErr, tc.wantNotFoundErr)
			if !reflect.DeepEqual(vaults.calls, tc.wantCalls) {
				t.Fatalf("vault lookup calls = %v; want %v", vaults.calls, tc.wantCalls)
			}
		})
	}
}

func assertServiceParentResult(t *testing.T, err error, wantValidationErr bool, wantNotFoundErr bool) {
	t.Helper()
	switch {
	case wantValidationErr:
		var validation *ValidationError
		if !errors.As(err, &validation) {
			t.Fatalf("error = %T %v; want ValidationError", err, err)
		}
	case wantNotFoundErr:
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("error = %T %v; want NotFoundError", err, err)
		}
	default:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}
