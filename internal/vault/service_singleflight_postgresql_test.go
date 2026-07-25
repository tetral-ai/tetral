package vault

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const singleFlightTestEncryptionKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type rotatingValidationIssuer struct {
	now          time.Time
	issuerCalls  atomic.Int64
	entered      chan struct{}
	release      chan struct{}
	refreshError bool
	once         sync.Once
}

func (v *rotatingValidationIssuer) Validate(ctx context.Context, input MCPOAuthValidationInput) (MCPOAuthValidationResult, error) {
	validation := &CredentialValidation{
		Type:            "vault_credential_validation",
		CredentialID:    input.Credential.ID,
		VaultID:         input.Credential.VaultID,
		Status:          "valid",
		ValidatedAt:     v.now,
		HasRefreshToken: hasMCPRefreshToken(input.Auth),
		Refresh:         &CredentialValidationCheck{Status: "succeeded"},
		MCPProbe:        &CredentialValidationCheck{Status: "succeeded", Method: "initialize"},
	}
	if !mcpOAuthRefreshDue(input.Auth.ExpiresAt, v.now) {
		return MCPOAuthValidationResult{Validation: validation}, nil
	}
	v.issuerCalls.Add(1)
	if v.entered != nil {
		v.once.Do(func() { close(v.entered) })
	}
	if v.release != nil {
		select {
		case <-ctx.Done():
			return MCPOAuthValidationResult{}, ctx.Err()
		case <-v.release:
		}
	}
	if v.refreshError {
		validation.Status = "invalid"
		validation.Refresh = &CredentialValidationCheck{Status: "failed", ErrorKind: "refresh_http_error"}
		return MCPOAuthValidationResult{Validation: validation}, nil
	}
	refreshed := input.Auth
	refreshed.AccessToken = "winner-access-token"
	refreshed.ExpiresAt = v.now.Add(time.Hour).Format(time.RFC3339Nano)
	if refreshed.Refresh != nil {
		refreshed.Refresh.RefreshToken = "winner-refresh-token"
	}
	return MCPOAuthValidationResult{Validation: validation, RefreshedAuth: &refreshed}, nil
}

func TestPostgreSQLVaultValidationRefreshIsDurableSingleFlightAcrossServices(t *testing.T) {
	runtimeDB, adminDB, encryptor := newSingleFlightVaultEnvironment(t)
	ctx := context.Background()
	vaults := NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtimeDB))
	credentials := NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor)
	container, credential := createExpiredValidationCredential(ctx, t, vaults, credentials)
	installCredentialUpdateAudit(t, adminDB)

	issuer := &rotatingValidationIssuer{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC), entered: make(chan struct{}), release: make(chan struct{})}
	serviceOne := NewService(vaults, NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor), WithMCPOAuthValidator(issuer))
	serviceTwo := NewService(vaults, NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor), WithMCPOAuthValidator(issuer))

	results := make(chan *CredentialValidation, 2)
	errorsCh := make(chan error, 2)
	go func() {
		validation, err := serviceOne.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, container.ID, credential.ID)
		results <- validation
		errorsCh <- err
	}()
	<-issuer.entered
	go func() {
		validation, err := serviceTwo.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, container.ID, credential.ID)
		results <- validation
		errorsCh <- err
	}()
	close(issuer.release)

	for range 2 {
		if err := <-errorsCh; err != nil {
			t.Fatalf("ValidateMCPOAuthCredential: %v", err)
		}
		if validation := <-results; validation == nil || validation.Status != "valid" {
			t.Fatalf("validation = %+v; want valid", validation)
		}
	}
	if got := issuer.issuerCalls.Load(); got != 1 {
		t.Fatalf("issuer refresh calls = %d; want 1", got)
	}
	var durableWrites int
	if err := adminDB.QueryRowContext(ctx, `SELECT count(*) FROM credential_update_audit`).Scan(&durableWrites); err != nil {
		t.Fatalf("count durable writes: %v", err)
	}
	if durableWrites != 1 {
		t.Fatalf("durable credential writes = %d; want 1", durableWrites)
	}
	_, finalAuth, err := credentials.GetSecret(ctx, workspace.DefaultID, container.ID, credential.ID)
	if err != nil {
		t.Fatalf("GetSecret final row: %v", err)
	}
	if finalAuth.AccessToken != "winner-access-token" || finalAuth.Refresh == nil || finalAuth.Refresh.RefreshToken != "winner-refresh-token" {
		t.Fatalf("final auth = %+v; want winner material", finalAuth)
	}
}

func TestPostgreSQLVaultValidationBlockedWaiterCancellationMakesNoIssuerCall(t *testing.T) {
	runtimeDB, _, encryptor := newSingleFlightVaultEnvironment(t)
	ctx := context.Background()
	vaults := NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtimeDB))
	credentials := NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor)
	container, credential := createExpiredValidationCredential(ctx, t, vaults, credentials)
	issuer := &rotatingValidationIssuer{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC), entered: make(chan struct{}), release: make(chan struct{})}
	serviceOne := NewService(vaults, NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor), WithMCPOAuthValidator(issuer))
	serviceTwo := NewService(vaults, NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor), WithMCPOAuthValidator(issuer))

	firstDone := make(chan error, 1)
	go func() {
		_, err := serviceOne.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, container.ID, credential.ID)
		firstDone <- err
	}()
	<-issuer.entered
	waiterCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if _, err := serviceTwo.ValidateMCPOAuthCredential(waiterCtx, workspace.DefaultID, container.ID, credential.ID); err == nil {
		t.Fatal("blocked waiter succeeded; want cancellation")
	}
	if got := issuer.issuerCalls.Load(); got != 1 {
		t.Fatalf("issuer calls before releasing winner = %d; want 1", got)
	}
	close(issuer.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("winner validation: %v", err)
	}
}

func TestPostgreSQLVaultValidationRefreshFailureAndInactiveRowsWriteNothing(t *testing.T) {
	for _, test := range []struct {
		name         string
		makeInactive func(context.Context, *sql.DB, string) error
	}{
		{name: "refresh failure"},
		{name: "archived", makeInactive: func(ctx context.Context, db *sql.DB, credentialID string) error {
			_, err := db.ExecContext(ctx, `UPDATE credentials SET archived_at = '2026-07-14T00:00:00Z' WHERE id = $1`, credentialID)
			return err
		}},
		{name: "revoked", makeInactive: func(ctx context.Context, db *sql.DB, credentialID string) error {
			_, err := db.ExecContext(ctx, `UPDATE credentials SET revoked_at = '2026-07-14T00:00:00Z' WHERE id = $1`, credentialID)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeDB, adminDB, encryptor := newSingleFlightVaultEnvironment(t)
			ctx := context.Background()
			vaults := NewPostgreSQLVaultStore(dbconnect.NewClientForTesting(runtimeDB))
			credentials := NewPostgreSQLCredentialStore(dbconnect.NewClientForTesting(runtimeDB), encryptor)
			container, credential := createExpiredValidationCredential(ctx, t, vaults, credentials)
			if test.makeInactive != nil {
				if err := test.makeInactive(ctx, adminDB, credential.ID); err != nil {
					t.Fatalf("make inactive: %v", err)
				}
			}
			installCredentialUpdateAudit(t, adminDB)
			issuer := &rotatingValidationIssuer{now: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC), refreshError: true}
			service := NewService(vaults, credentials, WithMCPOAuthValidator(issuer))
			validation, err := service.ValidateMCPOAuthCredential(ctx, workspace.DefaultID, container.ID, credential.ID)
			if test.makeInactive == nil {
				if err != nil || validation == nil || validation.Status != "invalid" {
					t.Fatalf("refresh failure validation=%+v err=%v", validation, err)
				}
			} else if err == nil {
				t.Fatal("inactive credential validation succeeded")
			}
			var writes int
			if err := adminDB.QueryRowContext(ctx, `SELECT count(*) FROM credential_update_audit`).Scan(&writes); err != nil {
				t.Fatalf("count writes: %v", err)
			}
			if writes != 0 {
				t.Fatalf("credential writes = %d; want 0", writes)
			}
		})
	}
}

func newSingleFlightVaultEnvironment(t *testing.T) (*sql.DB, *sql.DB, *Encryptor) {
	t.Helper()
	runtimeDB, adminDB := storagetest.NewPostgreSQLDBWithAdmin(t)
	encryptor, err := NewEncryptor(singleFlightTestEncryptionKey)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return runtimeDB, adminDB, encryptor
}

func createExpiredValidationCredential(ctx context.Context, t *testing.T, vaults *PostgreSQLVaultStore, credentials *PostgreSQLCredentialStore) (*Vault, *CredentialMetadata) {
	t.Helper()
	container, err := vaults.Create(ctx, workspace.DefaultID, CreateVaultRequest{DisplayName: "single flight"})
	if err != nil {
		t.Fatalf("Create vault: %v", err)
	}
	credential, err := credentials.Create(ctx, workspace.DefaultID, container.ID, CreateCredentialRequest{Auth: CredentialAuth{
		Type:         credentialAuthTypeMCPOAuth,
		MCPServerURL: "https://mcp.example.com/mcp",
		AccessToken:  "expired-access-token",
		ExpiresAt:    "2000-01-01T00:00:00Z",
		Refresh: &CredentialOAuthRefresh{
			ClientID:          "public-client-id",
			RefreshToken:      "rotating-refresh-token",
			TokenEndpoint:     "https://auth.example.com/token",
			TokenEndpointAuth: &CredentialTokenEndpointAuth{Type: "none"},
		},
	}})
	if err != nil {
		t.Fatalf("Create credential: %v", err)
	}
	return container, credential
}

func installCredentialUpdateAudit(t *testing.T, adminDB *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		`CREATE TABLE credential_update_audit (credential_id text NOT NULL)`,
		`GRANT INSERT ON credential_update_audit TO tetral_runtime_test`,
		`CREATE FUNCTION record_credential_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO credential_update_audit (credential_id) VALUES (NEW.id); RETURN NEW; END $$`,
		`CREATE TRIGGER credential_update_audit_trigger AFTER UPDATE ON credentials FOR EACH ROW EXECUTE FUNCTION record_credential_update()`,
	} {
		if _, err := adminDB.ExecContext(ctx, statement); err != nil {
			t.Fatalf("install credential update audit: %v", err)
		}
	}
}
