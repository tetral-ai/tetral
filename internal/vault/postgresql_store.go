package vault

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/endpointurl"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// PostgreSQLVaultStore is the PG-backed VaultStore. Workspace identity
// is required on every method call; the resolver layer takes it from
// the authenticated request context. Cascade delete on credentials
// is enforced by the composite (workspace_id, vault_id) FK.
type PostgreSQLVaultStore struct {
	client *dbconnect.Client
}

func NewPostgreSQLVaultStore(runtimeClient *dbconnect.Client) *PostgreSQLVaultStore {
	return &PostgreSQLVaultStore{client: runtimeClient}
}

func (s *PostgreSQLVaultStore) Create(ctx context.Context, ws workspace.ID, request CreateVaultRequest) (*Vault, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	normalized, err := normalizeVaultCreateRequest(request)
	if err != nil {
		return nil, err
	}
	now := storage.Now()
	vaultID := id.New("vlt_")
	metadata := normalized.Metadata
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		_, execErr := tx.Exec(ctx,
			`INSERT INTO vaults (id, workspace_id, display_name, metadata_json, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			vaultID, string(ws), normalized.DisplayName, string(metadataJSON), now.Format(time.RFC3339),
		)
		return execErr
	}); err != nil {
		return nil, err
	}
	return &Vault{
		ID:          vaultID,
		Type:        "vault",
		DisplayName: normalized.DisplayName,
		Metadata:    metadata,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func (s *PostgreSQLVaultStore) Get(ctx context.Context, ws workspace.ID, vaultID string) (*Vault, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *Vault
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		var v Vault
		var metadataJSON string
		var createdAt, updatedAt time.Time
		var archivedAt sql.NullTime
		err := tx.QueryRow(ctx,
			`SELECT id, display_name, metadata_json, archived_at, created_at, updated_at FROM vaults WHERE id = $1 AND workspace_id = $2`,
			vaultID, string(ws),
		).Scan(&v.ID, &v.DisplayName, &metadataJSON, &archivedAt, &createdAt, &updatedAt)
		if err == sql.ErrNoRows {
			return &NotFoundError{Message: "vault " + vaultID + " not found"}
		}
		if err != nil {
			return err
		}
		if err := hydrateVault(&v, metadataJSON, archivedAt, createdAt, updatedAt); err != nil {
			return err
		}
		result = &v
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLVaultStore) List(ctx context.Context, ws workspace.ID, options ListOptions) (VaultListResult, error) {
	if ws == "" {
		return VaultListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	limit := normalizedListLimit(options.Limit)
	lastSequence := int64(0)
	if options.Page != "" {
		token, err := decodePageToken(options.Page, ws, pageTokenVaults, "", options.IncludeArchived)
		if err != nil {
			return VaultListResult{}, err
		}
		lastSequence = token.LastSequence
	}
	result := VaultListResult{Data: []*Vault{}}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		rows, queryErr := tx.Query(ctx,
			`SELECT id, display_name, metadata_json, archived_at, created_at, updated_at, storage_sequence FROM vaults
			  WHERE workspace_id = $1
			    AND storage_sequence > $2
			    AND ($3 OR archived_at IS NULL)
			  ORDER BY storage_sequence ASC LIMIT $4`,
			string(ws), lastSequence, options.IncludeArchived, limit+1,
		)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var v Vault
			var metadataJSON string
			var createdAt, updatedAt time.Time
			var archivedAt sql.NullTime
			if scanErr := rows.Scan(&v.ID, &v.DisplayName, &metadataJSON, &archivedAt, &createdAt, &updatedAt, &v.storageSequence); scanErr != nil {
				return scanErr
			}
			if err := hydrateVault(&v, metadataJSON, archivedAt, createdAt, updatedAt); err != nil {
				return err
			}
			result.Data = append(result.Data, &v)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(result.Data) > limit {
			result.Data = result.Data[:limit]
			nextPage, err := encodePageToken(pageToken{
				Version:         pageTokenVersion,
				Resource:        pageTokenVaults,
				WorkspaceID:     string(ws),
				IncludeArchived: options.IncludeArchived,
				LastSequence:    result.Data[len(result.Data)-1].storageSequence,
			})
			if err != nil {
				return err
			}
			result.NextPage = &nextPage
		}
		return nil
	})
	if err != nil {
		return VaultListResult{}, err
	}
	return result, nil
}

func (s *PostgreSQLVaultStore) Update(ctx context.Context, ws workspace.ID, vaultID string, patch VaultPatch) (*Vault, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var noChange *Vault
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		var current Vault
		var metadataJSON string
		var createdAt, updatedAt time.Time
		var archivedAt sql.NullTime
		err := tx.QueryRow(ctx,
			`SELECT id, display_name, metadata_json, archived_at, created_at, updated_at
			   FROM vaults WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
			vaultID, string(ws),
		).Scan(&current.ID, &current.DisplayName, &metadataJSON, &archivedAt, &createdAt, &updatedAt)
		if err == sql.ErrNoRows {
			return &NotFoundError{Message: "vault " + vaultID + " not found"}
		}
		if err != nil {
			return err
		}
		if err := hydrateVault(&current, metadataJSON, archivedAt, createdAt, updatedAt); err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return &ValidationError{Message: "vault is archived"}
		}
		materialized, err := patch.Materialize(current)
		if err != nil {
			return err
		}
		if current.DisplayName == materialized.DisplayName && equalStringMap(current.Metadata, materialized.Metadata) {
			noChange = &current
			return nil
		}
		metadataBytes, err := json.Marshal(normalizeMetadata(materialized.Metadata))
		if err != nil {
			return err
		}
		now := storage.Now().Format(time.RFC3339)
		result, execErr := tx.Exec(ctx,
			`UPDATE vaults
			    SET display_name = $1, metadata_json = $2, updated_at = $3
			  WHERE id = $4 AND workspace_id = $5 AND archived_at IS NULL`,
			materialized.DisplayName, string(metadataBytes), now, vaultID, string(ws),
		)
		if execErr != nil {
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "vault " + vaultID + " not found"}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if noChange != nil {
		return noChange, nil
	}
	return s.Get(ctx, ws, vaultID)
}

func (s *PostgreSQLVaultStore) Archive(ctx context.Context, ws workspace.ID, vaultID string) (*Vault, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	now := storage.Now().Format(time.RFC3339)
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		result, execErr := tx.Exec(ctx,
			`UPDATE vaults
			    SET archived_at = COALESCE(archived_at, $1),
			        updated_at = CASE WHEN archived_at IS NULL THEN $1 ELSE updated_at END
			  WHERE id = $2 AND workspace_id = $3`,
			now, vaultID, string(ws),
		)
		if execErr != nil {
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "vault " + vaultID + " not found"}
		}
		_, execErr = tx.Exec(ctx,
			`UPDATE credentials
			    SET archived_at = COALESCE(archived_at, $1),
			        updated_at = CASE WHEN archived_at IS NULL THEN $1 ELSE updated_at END,
			        encrypted_auth = NULL
			  WHERE vault_id = $2 AND workspace_id = $3`,
			now, vaultID, string(ws),
		)
		return execErr
	}); err != nil {
		return nil, err
	}
	return s.Get(ctx, ws, vaultID)
}

func hydrateVault(v *Vault, metadataJSON string, archivedAt sql.NullTime, createdAt time.Time, updatedAt time.Time) error {
	v.Type = "vault"
	if err := json.Unmarshal([]byte(metadataJSON), &v.Metadata); err != nil {
		return err
	}
	v.Metadata = normalizeMetadata(v.Metadata)
	v.CreatedAt = createdAt.UTC()
	v.UpdatedAt = updatedAt.UTC()
	if archivedAt.Valid {
		archived := archivedAt.Time.UTC()
		v.ArchivedAt = &archived
	}
	return nil
}

func equalStringMap(a StringMap, b StringMap) bool {
	left, _ := json.Marshal(normalizeMetadata(a))
	right, _ := json.Marshal(normalizeMetadata(b))
	return string(left) == string(right)
}

func (s *PostgreSQLVaultStore) Delete(ctx context.Context, ws workspace.ID, vaultID string) (*DeleteResult, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		result, execErr := tx.Exec(ctx,
			`DELETE FROM vaults WHERE id = $1 AND workspace_id = $2`,
			vaultID, string(ws),
		)
		if execErr != nil {
			return execErr
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "vault " + vaultID + " not found"}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &DeleteResult{ID: vaultID, Type: "vault_deleted"}, nil
}

// PostgreSQLCredentialStore is the workspace-aware PG-backed
// CredentialStore. Encryption-at-rest is preserved (encrypted_auth
// BYTEA), and DecryptCredential is workspace-scoped so a resolver
// for workspace B cannot decrypt a workspace A credential by id.
type PostgreSQLCredentialStore struct {
	client    *dbconnect.Client
	encryptor CredentialEncryptor
}

type CredentialEncryptor interface {
	Encrypt([]byte) ([]byte, error)
	Decrypt([]byte) ([]byte, error)
}

type LockedCredentialPatchFunc func(CredentialAuth) (*CredentialPatch, error)

func NewPostgreSQLCredentialStore(runtimeClient *dbconnect.Client, encryptor CredentialEncryptor) *PostgreSQLCredentialStore {
	return &PostgreSQLCredentialStore{client: runtimeClient, encryptor: encryptor}
}

func (s *PostgreSQLCredentialStore) Create(ctx context.Context, ws workspace.ID, vaultID string, request CreateCredentialRequest) (*CredentialMetadata, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	normalized, publicAuth, err := normalizeCredentialCreateRequest(request)
	if err != nil {
		return nil, err
	}
	now := storage.Now()
	credID := id.New("cred_")
	metadataJSON, err := json.Marshal(normalized.Metadata)
	if err != nil {
		return nil, err
	}
	publicAuthJSON, err := json.Marshal(publicAuth)
	if err != nil {
		return nil, err
	}
	authJSON, err := json.Marshal(normalized.Auth) //nolint:gosec // intentional: marshaling secrets for encryption at rest
	if err != nil {
		return nil, err
	}
	encryptedAuth, err := s.encryptor.Encrypt(authJSON)
	if err != nil {
		return nil, err
	}
	providerIDColumn, accessModeColumn := credentialProviderColumns(publicAuth)
	if err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		if err := ensureVaultAcceptsCredentialTx(ctx, tx, ws, vaultID); err != nil {
			return err
		}
		if isMCPCredential(normalized.Auth.Type) {
			if err := ensureMCPActiveCredentialCapTx(ctx, tx, ws, vaultID); err != nil {
				return err
			}
		}
		_, execErr := tx.Exec(ctx,
			`INSERT INTO credentials (id, workspace_id, vault_id, display_name, metadata_json, auth_type, auth_public_json, mcp_server_url, provider_id, access_mode, expires_at, encrypted_auth, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)`,
			credID, string(ws), vaultID, normalized.DisplayName, string(metadataJSON), normalized.Auth.Type,
			string(publicAuthJSON), publicAuth.MCPServerURL, providerIDColumn, accessModeColumn,
			publicAuth.ExpiresAt, encryptedAuth, now.Format(time.RFC3339),
		)
		if execErr != nil {
			return mapCredentialPostgreSQLError(execErr)
		}
		return execErr
	}); err != nil {
		return nil, err
	}
	return &CredentialMetadata{
		ID:          credID,
		Type:        "vault_credential",
		VaultID:     vaultID,
		DisplayName: normalized.DisplayName,
		Metadata:    normalized.Metadata,
		Auth:        publicAuth,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}

func ensureVaultAcceptsCredentialTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, vaultID string) error {
	var archivedAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT archived_at FROM vaults WHERE id = $1 AND workspace_id = $2 FOR UPDATE`,
		vaultID, string(ws),
	).Scan(&archivedAt)
	if err == sql.ErrNoRows {
		return &NotFoundError{Message: "vault " + vaultID + " not found"}
	}
	if err != nil {
		return err
	}
	if archivedAt.Valid {
		return &ValidationError{Message: "vault is archived"}
	}
	return nil
}

func ensureMCPActiveCredentialCapTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, vaultID string) error {
	var activeCount int
	if err := tx.QueryRow(ctx,
		`SELECT count(*)
		   FROM credentials
		  WHERE workspace_id = $1
		    AND vault_id = $2
		    AND archived_at IS NULL
		    AND auth_type IN ('mcp_oauth', 'static_bearer')`,
		string(ws), vaultID,
	).Scan(&activeCount); err != nil {
		return err
	}
	if activeCount >= 20 {
		return &ValidationError{Message: "vault has reached the active MCP credential limit"}
	}
	return nil
}

func (s *PostgreSQLCredentialStore) GetMetadata(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*CredentialMetadata, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *CredentialMetadata
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		meta := &CredentialMetadata{Type: "vault_credential"}
		var metadataJSON, publicAuthJSON string
		var createdAt, updatedAt time.Time
		var encryptedAuth []byte
		var archivedAt sql.NullTime
		row := tx.QueryRow(ctx,
			`SELECT id, vault_id, display_name, metadata_json, auth_public_json, archived_at, created_at, updated_at, encrypted_auth
			   FROM credentials WHERE id = $1 AND vault_id = $2 AND workspace_id = $3`,
			credentialID, vaultID, string(ws),
		)
		if scanErr := row.Scan(&meta.ID, &meta.VaultID, &meta.DisplayName, &metadataJSON, &publicAuthJSON, &archivedAt, &createdAt, &updatedAt, &encryptedAuth); scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "credential " + credentialID + " not found"}
		} else if scanErr != nil {
			return scanErr
		}
		if err := hydrateCredential(meta, metadataJSON, publicAuthJSON, archivedAt, createdAt, updatedAt); err != nil {
			return err
		}
		if err := s.deriveProviderOAuthRefreshPresence(meta, encryptedAuth); err != nil {
			return err
		}
		result = meta
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLCredentialStore) GetSecret(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*CredentialMetadata, *CredentialAuth, error) {
	if ws == "" {
		return nil, nil, &ValidationError{Message: "workspace_id is required"}
	}
	var meta *CredentialMetadata
	var encryptedAuth []byte
	var revokedAt sql.NullTime
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		current, currentEncryptedAuth, currentRevokedAt, err := getCredentialForUpdateTx(ctx, tx, ws, vaultID, credentialID)
		if err != nil {
			return err
		}
		meta = current
		encryptedAuth = currentEncryptedAuth
		revokedAt = currentRevokedAt
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if meta.ArchivedAt != nil || credentialIsRevoked(revokedAt) || encryptedAuth == nil {
		return nil, nil, &ValidationError{Message: "credential secret is not available"}
	}
	auth, err := decryptCredentialAuth(s.encryptor, encryptedAuth)
	if err != nil {
		return nil, nil, err
	}
	return meta, auth, nil
}

// DecryptCredential returns the full credential auth (including
// secrets) — scoped to workspaceID so a resolver for one tenant
// cannot decrypt another tenant's row by id alone.
func (s *PostgreSQLCredentialStore) DecryptCredential(ctx context.Context, ws workspace.ID, credentialID string) (*CredentialAuth, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var encryptedAuth []byte
	var archivedAt sql.NullTime
	var revokedAt sql.NullTime
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT encrypted_auth, archived_at, revoked_at FROM credentials WHERE id = $1 AND workspace_id = $2`,
			credentialID, string(ws),
		)
		if scanErr := row.Scan(&encryptedAuth, &archivedAt, &revokedAt); scanErr == sql.ErrNoRows {
			return &NotFoundError{Message: "credential " + credentialID + " not found"}
		} else if scanErr != nil {
			return scanErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if archivedAt.Valid || credentialIsRevoked(revokedAt) || encryptedAuth == nil {
		return nil, &ValidationError{Message: "credential secret is not available"}
	}
	plaintext, err := s.encryptor.Decrypt(encryptedAuth)
	if err != nil {
		return nil, err
	}
	var auth CredentialAuth
	if err := json.Unmarshal(plaintext, &auth); err != nil {
		return nil, err
	}
	return &auth, nil
}

func (s *PostgreSQLCredentialStore) List(ctx context.Context, ws workspace.ID, vaultID string, options ListOptions) (CredentialListResult, error) {
	if ws == "" {
		return CredentialListResult{}, &ValidationError{Message: "workspace_id is required"}
	}
	limit := normalizedListLimit(options.Limit)
	lastSequence := int64(0)
	if options.Page != "" {
		token, err := decodePageToken(options.Page, ws, pageTokenVaultAuth, vaultID, options.IncludeArchived)
		if err != nil {
			return CredentialListResult{}, err
		}
		lastSequence = token.LastSequence
	}
	result := CredentialListResult{Data: []*Credential{}}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		archivedFilter := "AND archived_at IS NULL"
		if options.IncludeArchived {
			archivedFilter = ""
		}
		rows, queryErr := tx.Query(ctx,
			`SELECT id, vault_id, display_name, metadata_json, auth_public_json, archived_at, created_at, updated_at, storage_sequence, encrypted_auth
			   FROM credentials
			  WHERE vault_id = $1 AND workspace_id = $2
			    AND storage_sequence > $3
			    `+archivedFilter+`
			  ORDER BY storage_sequence ASC LIMIT $4`,
			vaultID, string(ws), lastSequence, limit+1,
		)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			meta := &CredentialMetadata{Type: "vault_credential"}
			var metadataJSON, publicAuthJSON string
			var createdAt, updatedAt time.Time
			var encryptedAuth []byte
			var archivedAt sql.NullTime
			if scanErr := rows.Scan(&meta.ID, &meta.VaultID, &meta.DisplayName, &metadataJSON, &publicAuthJSON, &archivedAt, &createdAt, &updatedAt, &meta.storageSequence, &encryptedAuth); scanErr != nil {
				return scanErr
			}
			if err := hydrateCredential(meta, metadataJSON, publicAuthJSON, archivedAt, createdAt, updatedAt); err != nil {
				return err
			}
			if err := s.deriveProviderOAuthRefreshPresence(meta, encryptedAuth); err != nil {
				return err
			}
			result.Data = append(result.Data, meta)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(result.Data) > limit {
			result.Data = result.Data[:limit]
			nextPage, err := encodePageToken(pageToken{
				Version:         pageTokenVersion,
				Resource:        pageTokenVaultAuth,
				WorkspaceID:     string(ws),
				ParentVaultID:   vaultID,
				IncludeArchived: options.IncludeArchived,
				LastSequence:    result.Data[len(result.Data)-1].storageSequence,
			})
			if err != nil {
				return err
			}
			result.NextPage = &nextPage
		}
		return nil
	})
	if err != nil {
		return CredentialListResult{}, err
	}
	return result, nil
}

func normalizedListLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func (s *PostgreSQLCredentialStore) Update(ctx context.Context, ws workspace.ID, vaultID, credentialID string, patch CredentialPatch) (*CredentialMetadata, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *CredentialMetadata
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		current, encryptedAuth, revokedAt, err := getCredentialForUpdateTx(ctx, tx, ws, vaultID, credentialID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return &ValidationError{Message: "credential is archived"}
		}
		if credentialIsRevoked(revokedAt) {
			return &ValidationError{Message: "credential is revoked"}
		}
		if encryptedAuth == nil {
			return &ValidationError{Message: "credential secret is not available"}
		}
		currentSecret, err := decryptCredentialAuth(s.encryptor, encryptedAuth)
		if err != nil {
			return err
		}
		current.Auth = publicAuthFromSecret(*currentSecret)
		materialized, err := patch.Materialize(*current)
		if err != nil {
			return err
		}
		nextSecret := *currentSecret
		nextPublicAuth := current.Auth
		if patch.Auth != nil {
			rawAuth, err := credentialPatchAuthObject(patch)
			if err != nil {
				return err
			}
			nextSecret, nextPublicAuth, err = applyCredentialAuthPatch(nextSecret, current.Auth, *patch.Auth, rawAuth)
			if err != nil {
				return err
			}
		}
		if nextSecret.Type == credentialAuthTypeProviderOAuth {
			nextSecret, err = normalizeProviderOAuthAuth(nextSecret)
			if err != nil {
				return err
			}
			nextPublicAuth = publicAuthFromSecret(nextSecret)
		}
		now := storage.Now()
		metadataJSON, err := json.Marshal(materialized.Metadata)
		if err != nil {
			return err
		}
		publicAuthJSON, err := json.Marshal(nextPublicAuth)
		if err != nil {
			return err
		}
		authJSON, err := json.Marshal(nextSecret) //nolint:gosec // intentional: marshaling secrets for encryption at rest
		if err != nil {
			return err
		}
		nextEncryptedAuth, err := s.encryptor.Encrypt(authJSON)
		if err != nil {
			return err
		}
		providerIDColumn, accessModeColumn := credentialProviderColumns(nextPublicAuth)
		_, execErr := tx.Exec(ctx,
			`UPDATE credentials
			    SET display_name = $1,
			        metadata_json = $2,
			        auth_public_json = $3,
			        mcp_server_url = $4,
			        provider_id = $5,
			        access_mode = $6,
			        expires_at = $7,
			        encrypted_auth = $8,
			        updated_at = $9
			  WHERE id = $10 AND vault_id = $11 AND workspace_id = $12`,
			materialized.DisplayName, string(metadataJSON), string(publicAuthJSON), nextPublicAuth.MCPServerURL,
			providerIDColumn, accessModeColumn, nextPublicAuth.ExpiresAt,
			nextEncryptedAuth, now.Format(time.RFC3339),
			credentialID, vaultID, string(ws),
		)
		if execErr != nil {
			return mapCredentialPostgreSQLError(execErr)
		}
		materialized.Auth = nextPublicAuth
		materialized.UpdatedAt = now
		result = &materialized
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLCredentialStore) UpdateWithLockedCredential(ctx context.Context, ws workspace.ID, vaultID, credentialID string, buildPatch LockedCredentialPatchFunc) (*CredentialMetadata, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	if buildPatch == nil {
		return nil, &ValidationError{Message: "credential update callback is required"}
	}
	var result *CredentialMetadata
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.locked_update", func(tx *dbconnect.Tx) error {
		current, encryptedAuth, revokedAt, err := getCredentialForUpdateTx(ctx, tx, ws, vaultID, credentialID)
		if err != nil {
			return err
		}
		if current.ArchivedAt != nil {
			return &ValidationError{Message: "credential is archived"}
		}
		if credentialIsRevoked(revokedAt) {
			return &ValidationError{Message: "credential is revoked"}
		}
		if encryptedAuth == nil {
			return &ValidationError{Message: "credential secret is not available"}
		}
		currentSecret, err := decryptCredentialAuth(s.encryptor, encryptedAuth)
		if err != nil {
			return err
		}
		current.Auth = publicAuthFromSecret(*currentSecret)
		patch, err := buildPatch(*currentSecret)
		if err != nil {
			return err
		}
		if patch == nil {
			result = current
			return nil
		}
		materialized, err := patch.Materialize(*current)
		if err != nil {
			return err
		}
		nextSecret := *currentSecret
		nextPublicAuth := current.Auth
		if patch.Auth != nil {
			rawAuth, err := credentialPatchAuthObject(*patch)
			if err != nil {
				return err
			}
			nextSecret, nextPublicAuth, err = applyCredentialAuthPatch(nextSecret, current.Auth, *patch.Auth, rawAuth)
			if err != nil {
				return err
			}
		}
		if nextSecret.Type == credentialAuthTypeProviderOAuth {
			nextSecret, err = normalizeProviderOAuthAuth(nextSecret)
			if err != nil {
				return err
			}
			nextPublicAuth = publicAuthFromSecret(nextSecret)
		}
		now := storage.Now()
		metadataJSON, err := json.Marshal(materialized.Metadata)
		if err != nil {
			return err
		}
		publicAuthJSON, err := json.Marshal(nextPublicAuth)
		if err != nil {
			return err
		}
		authJSON, err := json.Marshal(nextSecret) //nolint:gosec // intentional: marshaling secrets for encryption at rest
		if err != nil {
			return err
		}
		nextEncryptedAuth, err := s.encryptor.Encrypt(authJSON)
		if err != nil {
			return err
		}
		providerIDColumn, accessModeColumn := credentialProviderColumns(nextPublicAuth)
		_, execErr := tx.Exec(ctx,
			`UPDATE credentials
			    SET display_name = $1,
			        metadata_json = $2,
			        auth_public_json = $3,
			        mcp_server_url = $4,
			        provider_id = $5,
			        access_mode = $6,
			        expires_at = $7,
			        encrypted_auth = $8,
			        updated_at = $9
			  WHERE id = $10 AND vault_id = $11 AND workspace_id = $12`,
			materialized.DisplayName, string(metadataJSON), string(publicAuthJSON), nextPublicAuth.MCPServerURL,
			providerIDColumn, accessModeColumn, nextPublicAuth.ExpiresAt,
			nextEncryptedAuth, now.Format(time.RFC3339),
			credentialID, vaultID, string(ws),
		)
		if execErr != nil {
			return mapCredentialPostgreSQLError(execErr)
		}
		materialized.Auth = nextPublicAuth
		materialized.UpdatedAt = now
		result = &materialized
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLCredentialStore) Archive(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*CredentialMetadata, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	var result *CredentialMetadata
	now := storage.Now()
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		meta := &CredentialMetadata{Type: "vault_credential"}
		var metadataJSON, publicAuthJSON string
		var createdAt, updatedAt time.Time
		var encryptedAuth []byte
		var archivedAt sql.NullTime
		if err := tx.QueryRow(ctx,
			`SELECT encrypted_auth FROM credentials WHERE id = $1 AND vault_id = $2 AND workspace_id = $3 FOR UPDATE`,
			credentialID, vaultID, string(ws),
		).Scan(&encryptedAuth); err == sql.ErrNoRows {
			return &NotFoundError{Message: "credential " + credentialID + " not found"}
		} else if err != nil {
			return err
		}
		err := tx.QueryRow(ctx,
			`UPDATE credentials
			    SET archived_at = COALESCE(archived_at, $1),
			        encrypted_auth = NULL,
			        updated_at = CASE WHEN archived_at IS NULL THEN $1 ELSE updated_at END
			  WHERE id = $2 AND vault_id = $3 AND workspace_id = $4
			  RETURNING id, vault_id, display_name, metadata_json, auth_public_json, archived_at, created_at, updated_at`,
			now.Format(time.RFC3339), credentialID, vaultID, string(ws),
		).Scan(&meta.ID, &meta.VaultID, &meta.DisplayName, &metadataJSON, &publicAuthJSON, &archivedAt, &createdAt, &updatedAt)
		if err == sql.ErrNoRows {
			return &NotFoundError{Message: "credential " + credentialID + " not found"}
		}
		if err != nil {
			return err
		}
		if err := hydrateCredential(meta, metadataJSON, publicAuthJSON, archivedAt, createdAt, updatedAt); err != nil {
			return err
		}
		if err := s.deriveProviderOAuthRefreshPresence(meta, encryptedAuth); err != nil {
			return err
		}
		result = meta
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PostgreSQLCredentialStore) Delete(ctx context.Context, ws workspace.ID, vaultID, credentialID string) (*DeleteResult, error) {
	if ws == "" {
		return nil, &ValidationError{Message: "workspace_id is required"}
	}
	err := s.client.WithWorkspaceTx(ctx, string(ws), "vault.transaction", func(tx *dbconnect.Tx) error {
		if _, execErr := tx.Exec(ctx,
			`DELETE FROM session_provider_auth
			  WHERE workspace_id = $1
			    AND vault_id = $2
			    AND credential_id = $3
			    AND deleted_at IS NOT NULL`,
			string(ws), vaultID, credentialID,
		); execErr != nil {
			return execErr
		}
		result, execErr := tx.Exec(ctx,
			`DELETE FROM credentials WHERE id = $1 AND vault_id = $2 AND workspace_id = $3`,
			credentialID, vaultID, string(ws),
		)
		if execErr != nil {
			return mapCredentialDeletePostgreSQLError(execErr)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "credential " + credentialID + " not found"}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &DeleteResult{ID: credentialID, Type: "vault_credential_deleted"}, nil
}

func normalizeVaultCreateRequest(request CreateVaultRequest) (CreateVaultRequest, error) {
	if err := validateRequiredDisplayName("display_name", request.DisplayName); err != nil {
		return CreateVaultRequest{}, err
	}
	request.Metadata = normalizeMetadata(request.Metadata)
	if err := validateMetadataLimits(request.Metadata); err != nil {
		return CreateVaultRequest{}, err
	}
	return request, nil
}

func normalizeCredentialCreateRequest(request CreateCredentialRequest) (CreateCredentialRequest, CredentialAuthPublic, error) {
	if err := validateOptionalDisplayName("display_name", request.DisplayName); err != nil {
		return CreateCredentialRequest{}, CredentialAuthPublic{}, err
	}
	metadata := normalizeMetadata(request.Metadata)
	if err := validateMetadataLimits(metadata); err != nil {
		return CreateCredentialRequest{}, CredentialAuthPublic{}, err
	}
	auth, publicAuth, err := normalizeCredentialAuthForCreate(request.Auth)
	if err != nil {
		return CreateCredentialRequest{}, CredentialAuthPublic{}, err
	}
	request.Metadata = metadata
	request.Auth = auth
	return request, publicAuth, nil
}

func normalizeCredentialAuthForCreate(auth CredentialAuth) (CredentialAuth, CredentialAuthPublic, error) {
	switch auth.Type {
	case credentialAuthTypeStaticBearer:
		if auth.MCPServerURL == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.mcp_server_url is required"}
		}
		if auth.Token == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.token is required"}
		}
		canonical, err := canonicalizeCredentialEndpoint(auth.MCPServerURL, "auth.mcp_server_url")
		if err != nil {
			return CredentialAuth{}, CredentialAuthPublic{}, err
		}
		normalized := CredentialAuth{
			Type:         credentialAuthTypeStaticBearer,
			MCPServerURL: canonical,
			Token:        auth.Token,
		}
		return normalized, publicAuthFromSecret(normalized), nil
	case credentialAuthTypeMCPOAuth:
		if auth.MCPServerURL == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.mcp_server_url is required"}
		}
		if auth.AccessToken == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.access_token is required"}
		}
		canonical, err := canonicalizeCredentialEndpoint(auth.MCPServerURL, "auth.mcp_server_url")
		if err != nil {
			return CredentialAuth{}, CredentialAuthPublic{}, err
		}
		normalized := CredentialAuth{
			Type:         credentialAuthTypeMCPOAuth,
			MCPServerURL: canonical,
			AccessToken:  auth.AccessToken,
			ExpiresAt:    auth.ExpiresAt,
			Refresh:      auth.Refresh,
		}
		if auth.Refresh != nil {
			if err := normalizeOAuthRefreshForCreate(normalized.Refresh); err != nil {
				return CredentialAuth{}, CredentialAuthPublic{}, err
			}
		}
		return normalized, publicAuthFromSecret(normalized), nil
	case credentialAuthTypeProviderAPIKey:
		if auth.ProviderID == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.provider_id is required"}
		}
		if auth.AccessMode == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.access_mode is required"}
		}
		if auth.Token == "" {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.token is required"}
		}
		normalized := CredentialAuth{
			Type:       credentialAuthTypeProviderAPIKey,
			ProviderID: auth.ProviderID,
			AccessMode: auth.AccessMode,
			Token:      auth.Token,
		}
		return normalized, publicAuthFromSecret(normalized), nil
	case credentialAuthTypeProviderOAuth:
		normalized, err := normalizeProviderOAuthAuth(auth)
		if err != nil {
			return CredentialAuth{}, CredentialAuthPublic{}, err
		}
		return normalized, publicAuthFromSecret(normalized), nil
	default:
		return CredentialAuth{}, CredentialAuthPublic{}, unsupportedCredentialAuthTypeError()
	}
}

func normalizeOAuthRefreshForCreate(refresh *CredentialOAuthRefresh) error {
	if refresh.RefreshToken == "" {
		return &ValidationError{Message: "auth.refresh.refresh_token is required"}
	}
	if refresh.ClientID == "" {
		return &ValidationError{Message: "auth.refresh.client_id is required"}
	}
	if refresh.TokenEndpoint == "" {
		return &ValidationError{Message: "auth.refresh.token_endpoint is required"}
	}
	canonical, err := canonicalizeCredentialEndpoint(refresh.TokenEndpoint, "auth.refresh.token_endpoint")
	if err != nil {
		return err
	}
	refresh.TokenEndpoint = canonical
	if refresh.TokenEndpointAuth == nil {
		return &ValidationError{Message: "auth.refresh.token_endpoint_auth is required"}
	}
	return validateTokenEndpointAuthForCreate(refresh.TokenEndpointAuth)
}

func validateTokenEndpointAuthForCreate(auth *CredentialTokenEndpointAuth) error {
	switch auth.Type {
	case "none":
		if auth.ClientSecret != "" {
			return &ValidationError{Message: "auth.refresh.token_endpoint_auth.client_secret is not allowed when type is none"}
		}
	case "client_secret_basic", "client_secret_post":
		if auth.ClientSecret == "" {
			return &ValidationError{Message: "auth.refresh.token_endpoint_auth.client_secret is required"}
		}
	default:
		return &ValidationError{Message: "auth.refresh.token_endpoint_auth.type must be none, client_secret_basic, or client_secret_post"}
	}
	return nil
}

func canonicalizeCredentialEndpoint(raw string, fieldPath string) (string, error) {
	canonical, err := endpointurl.CanonicalizePublicURL(raw, fieldPath)
	if err != nil {
		return "", &ValidationError{Message: err.Error()}
	}
	return canonical, nil
}

func publicAuthFromSecret(auth CredentialAuth) CredentialAuthPublic {
	publicAuth := CredentialAuthPublic{
		Type:         auth.Type,
		MCPServerURL: auth.MCPServerURL,
		ProviderID:   auth.ProviderID,
		AccessMode:   auth.AccessMode,
		ExpiresAt:    auth.ExpiresAt,
		AccountID:    auth.AccountID,
	}
	if auth.Type == credentialAuthTypeProviderOAuth {
		hasRefreshToken := auth.RefreshToken != ""
		publicAuth.HasRefreshToken = &hasRefreshToken
	}
	if auth.Refresh != nil {
		publicAuth.Refresh = &CredentialOAuthRefreshPublic{
			ClientID:      auth.Refresh.ClientID,
			TokenEndpoint: auth.Refresh.TokenEndpoint,
			Scope:         auth.Refresh.Scope,
			Resource:      auth.Refresh.Resource,
		}
		if auth.Refresh.TokenEndpointAuth != nil {
			publicAuth.Refresh.TokenEndpointAuth = CredentialTokenEndpointAuthPublic{Type: auth.Refresh.TokenEndpointAuth.Type}
		}
	}
	return publicAuth
}

func (s *PostgreSQLCredentialStore) deriveProviderOAuthRefreshPresence(credential *CredentialMetadata, encryptedAuth []byte) error {
	if credential.Auth.Type != credentialAuthTypeProviderOAuth {
		credential.Auth.HasRefreshToken = nil
		return nil
	}
	if credential.Auth.HasRefreshToken != nil {
		return nil
	}
	hasRefreshToken := false
	if len(encryptedAuth) > 0 {
		auth, err := decryptCredentialAuth(s.encryptor, encryptedAuth)
		if err != nil {
			return err
		}
		hasRefreshToken = auth.RefreshToken != ""
	}
	credential.Auth.HasRefreshToken = &hasRefreshToken
	return nil
}

func isMCPCredential(authType string) bool {
	return authType == credentialAuthTypeMCPOAuth || authType == credentialAuthTypeStaticBearer
}

func credentialProviderColumns(auth CredentialAuthPublic) (any, any) {
	switch auth.Type {
	case credentialAuthTypeProviderAPIKey, credentialAuthTypeProviderOAuth:
		return auth.ProviderID, auth.AccessMode
	default:
		return nil, nil
	}
}

func hydrateCredential(credential *CredentialMetadata, metadataJSON string, publicAuthJSON string, archivedAt sql.NullTime, createdAt time.Time, updatedAt time.Time) error {
	credential.Type = "vault_credential"
	if err := json.Unmarshal([]byte(metadataJSON), &credential.Metadata); err != nil {
		return err
	}
	credential.Metadata = normalizeMetadata(credential.Metadata)
	if err := json.Unmarshal([]byte(publicAuthJSON), &credential.Auth); err != nil {
		return err
	}
	credential.CreatedAt = createdAt.UTC()
	credential.UpdatedAt = updatedAt.UTC()
	if archivedAt.Valid {
		archived := archivedAt.Time.UTC()
		credential.ArchivedAt = &archived
	}
	return nil
}

func decryptCredentialAuth(encryptor CredentialEncryptor, encryptedAuth []byte) (*CredentialAuth, error) {
	plaintext, err := encryptor.Decrypt(encryptedAuth)
	if err != nil {
		return nil, err
	}
	var auth CredentialAuth
	if err := json.Unmarshal(plaintext, &auth); err != nil {
		return nil, err
	}
	return &auth, nil
}

func getCredentialForUpdateTx(ctx context.Context, tx *dbconnect.Tx, ws workspace.ID, vaultID string, credentialID string) (*CredentialMetadata, []byte, sql.NullTime, error) {
	meta := &CredentialMetadata{Type: "vault_credential"}
	var metadataJSON, publicAuthJSON string
	var createdAt, updatedAt time.Time
	var archivedAt sql.NullTime
	var revokedAt sql.NullTime
	var encryptedAuth []byte
	err := tx.QueryRow(ctx,
		`SELECT id, vault_id, display_name, metadata_json, auth_public_json, archived_at, revoked_at, created_at, updated_at, encrypted_auth
		   FROM credentials
		  WHERE id = $1 AND vault_id = $2 AND workspace_id = $3
		  FOR UPDATE`,
		credentialID, vaultID, string(ws),
	).Scan(&meta.ID, &meta.VaultID, &meta.DisplayName, &metadataJSON, &publicAuthJSON, &archivedAt, &revokedAt, &createdAt, &updatedAt, &encryptedAuth)
	if err == sql.ErrNoRows {
		return nil, nil, sql.NullTime{}, &NotFoundError{Message: "credential " + credentialID + " not found"}
	}
	if err != nil {
		return nil, nil, sql.NullTime{}, err
	}
	if err := hydrateCredential(meta, metadataJSON, publicAuthJSON, archivedAt, createdAt, updatedAt); err != nil {
		return nil, nil, sql.NullTime{}, err
	}
	return meta, encryptedAuth, revokedAt, nil
}

func credentialIsRevoked(revokedAt sql.NullTime) bool {
	return revokedAt.Valid
}

func credentialPatchAuthObject(patch CredentialPatch) (map[string]json.RawMessage, error) {
	rawAuth, ok := patch.raw["auth"]
	if !ok {
		return map[string]json.RawMessage{}, nil
	}
	return decodeNestedObject("auth", rawAuth)
}

func applyCredentialAuthPatch(currentSecret CredentialAuth, currentPublic CredentialAuthPublic, patch CredentialAuth, rawAuth map[string]json.RawMessage) (CredentialAuth, CredentialAuthPublic, error) {
	if patch.Type != "" && patch.Type != currentPublic.Type {
		return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.type is immutable"}
	}
	if err := rejectCredentialAuthPatchFieldsForStoredType(currentPublic.Type, rawAuth); err != nil {
		return CredentialAuth{}, CredentialAuthPublic{}, err
	}
	if err := validateCredentialAuthPatchSecretRotations(rawAuth); err != nil {
		return CredentialAuth{}, CredentialAuthPublic{}, err
	}
	nextSecret := currentSecret
	nextSecret.Type = currentPublic.Type
	switch currentPublic.Type {
	case credentialAuthTypeStaticBearer:
		if _, ok := rawAuth["mcp_server_url"]; ok {
			canonical, err := canonicalizeCredentialEndpoint(patch.MCPServerURL, "auth.mcp_server_url")
			if err != nil {
				return CredentialAuth{}, CredentialAuthPublic{}, err
			}
			if canonical != currentPublic.MCPServerURL {
				return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.mcp_server_url is immutable"}
			}
		}
		if patch.Token != "" {
			nextSecret.Token = patch.Token
		}
	case credentialAuthTypeMCPOAuth:
		if _, ok := rawAuth["mcp_server_url"]; ok {
			canonical, err := canonicalizeCredentialEndpoint(patch.MCPServerURL, "auth.mcp_server_url")
			if err != nil {
				return CredentialAuth{}, CredentialAuthPublic{}, err
			}
			if canonical != currentPublic.MCPServerURL {
				return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.mcp_server_url is immutable"}
			}
		}
		if patch.AccessToken != "" {
			nextSecret.AccessToken = patch.AccessToken
		}
		if value, ok := rawAuth["expires_at"]; ok {
			if isJSONNull(value) {
				nextSecret.ExpiresAt = ""
			} else {
				nextSecret.ExpiresAt = patch.ExpiresAt
			}
		}
		if value, ok := rawAuth["refresh"]; ok && isJSONNull(value) {
			nextSecret.Refresh = nil
		} else if patch.Refresh != nil {
			if nextSecret.Refresh == nil {
				replacement := *patch.Refresh
				if err := normalizeOAuthRefreshForCreate(&replacement); err != nil {
					return CredentialAuth{}, CredentialAuthPublic{}, err
				}
				nextSecret.Refresh = &replacement
			} else {
				rawRefresh, err := credentialPatchRefreshObject(rawAuth)
				if err != nil {
					return CredentialAuth{}, CredentialAuthPublic{}, err
				}
				if err := applyOAuthRefreshPatch(nextSecret.Refresh, currentPublic.Refresh, patch.Refresh, rawRefresh); err != nil {
					return CredentialAuth{}, CredentialAuthPublic{}, err
				}
			}
		}
	case credentialAuthTypeProviderAPIKey:
		if _, ok := rawAuth["provider_id"]; ok && patch.ProviderID != currentPublic.ProviderID {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.provider_id is immutable"}
		}
		if _, ok := rawAuth["access_mode"]; ok && patch.AccessMode != currentPublic.AccessMode {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.access_mode is immutable"}
		}
		if patch.Token != "" {
			nextSecret.Token = patch.Token
		}
	case credentialAuthTypeProviderOAuth:
		if _, ok := rawAuth["provider_id"]; ok && patch.ProviderID != currentPublic.ProviderID {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.provider_id is immutable"}
		}
		if _, ok := rawAuth["access_mode"]; ok && patch.AccessMode != currentPublic.AccessMode {
			return CredentialAuth{}, CredentialAuthPublic{}, &ValidationError{Message: "auth.access_mode is immutable"}
		}
		if patch.AccessToken != "" {
			nextSecret.AccessToken = patch.AccessToken
		}
		if patch.RefreshToken != "" {
			nextSecret.RefreshToken = patch.RefreshToken
		}
		if patch.ExpiresAt != "" {
			nextSecret.ExpiresAt = patch.ExpiresAt
		}
		if _, ok := rawAuth["account_id"]; ok {
			nextSecret.AccountID = patch.AccountID
		}
	default:
		return CredentialAuth{}, CredentialAuthPublic{}, unsupportedCredentialAuthTypeError()
	}
	return nextSecret, publicAuthFromSecret(nextSecret), nil
}

func normalizeProviderOAuthAuth(auth CredentialAuth) (CredentialAuth, error) {
	if auth.ProviderID != "openai" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "provider_id", "auth.provider_id must be openai")
	}
	if auth.AccessMode != "oauth" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "access_mode", "auth.access_mode must be oauth")
	}
	if auth.AccessToken == "" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "access_token", "auth.access_token is required")
	}
	if auth.RefreshToken == "" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "refresh_token", "auth.refresh_token is required")
	}
	if auth.ExpiresAt == "" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "expires_at", "auth.expires_at is required")
	}
	if auth.AccountID == "" {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "account_id", "auth.account_id is required")
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, auth.ExpiresAt)
	if err != nil || !providerOAuthExpiryHasExplicitZone(auth.ExpiresAt) {
		return CredentialAuth{}, providerOAuthValidationError(auth.ProviderID, "expires_at", "auth.expires_at must be an RFC 3339 timestamp with an explicit timezone")
	}
	return CredentialAuth{
		Type:         credentialAuthTypeProviderOAuth,
		ProviderID:   "openai",
		AccessMode:   "oauth",
		AccessToken:  auth.AccessToken,
		RefreshToken: auth.RefreshToken,
		ExpiresAt:    expiresAt.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		AccountID:    auth.AccountID,
	}, nil
}

func providerOAuthValidationError(providerID string, member string, message string) error {
	slog.Error("provider credential rejected",
		"event", "provider_credential_rejected",
		"event.kind", "provider_credential_rejected",
		"component", "vault",
		"provider.id", providerID,
		"validation.member", member,
		"error.class", "validation",
		"error.code", "invalid_provider_oauth",
		"error.message_safe", "provider credential rejected",
	)
	return &ValidationError{Message: message}
}

func providerOAuthExpiryHasExplicitZone(value string) bool {
	if len(value) < len("2006-01-02T15:04:05Z") || value[10] != 'T' {
		return false
	}
	if value[len(value)-1] == 'Z' {
		return true
	}
	if len(value) < 6 {
		return false
	}
	zone := value[len(value)-6:]
	return (zone[0] == '+' || zone[0] == '-') && zone[3] == ':'
}

func rejectCredentialAuthPatchFieldsForStoredType(storedType string, rawAuth map[string]json.RawMessage) error {
	var allowed map[string]struct{}
	switch storedType {
	case credentialAuthTypeStaticBearer:
		allowed = map[string]struct{}{"type": {}, "mcp_server_url": {}, "token": {}}
	case credentialAuthTypeMCPOAuth:
		allowed = map[string]struct{}{"type": {}, "mcp_server_url": {}, "access_token": {}, "expires_at": {}, "refresh": {}}
	case credentialAuthTypeProviderAPIKey:
		allowed = map[string]struct{}{"type": {}, "provider_id": {}, "access_mode": {}, "token": {}}
	case credentialAuthTypeProviderOAuth:
		allowed = map[string]struct{}{"type": {}, "provider_id": {}, "access_mode": {}, "access_token": {}, "refresh_token": {}, "expires_at": {}, "account_id": {}}
	default:
		return unsupportedCredentialAuthTypeError()
	}
	return rejectUnknownFields(rawAuth, allowed, "auth")
}

func validateCredentialAuthPatchSecretRotations(rawAuth map[string]json.RawMessage) error {
	if value, ok := rawAuth["token"]; ok {
		if _, err := decodeRequiredString("auth.token", value); err != nil {
			return err
		}
	}
	if value, ok := rawAuth["access_token"]; ok {
		if _, err := decodeRequiredString("auth.access_token", value); err != nil {
			return err
		}
	}
	if value, ok := rawAuth["refresh_token"]; ok {
		if _, err := decodeRequiredString("auth.refresh_token", value); err != nil {
			return err
		}
	}
	rawRefresh, ok := rawAuth["refresh"]
	if !ok {
		return nil
	}
	if isJSONNull(rawRefresh) {
		return nil
	}
	refresh, err := decodeNestedObject("auth.refresh", rawRefresh)
	if err != nil {
		return err
	}
	if value, ok := refresh["refresh_token"]; ok {
		if _, err := decodeRequiredString("auth.refresh.refresh_token", value); err != nil {
			return err
		}
	}
	return nil
}

func credentialPatchRefreshObject(rawAuth map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	rawRefresh, ok := rawAuth["refresh"]
	if !ok {
		return map[string]json.RawMessage{}, nil
	}
	return decodeNestedObject("auth.refresh", rawRefresh)
}

func applyOAuthRefreshPatch(currentSecret *CredentialOAuthRefresh, currentPublic *CredentialOAuthRefreshPublic, patch *CredentialOAuthRefresh, rawRefresh map[string]json.RawMessage) error {
	if currentSecret == nil || currentPublic == nil {
		return &ValidationError{Message: "auth.refresh is not available"}
	}
	if _, ok := rawRefresh["client_id"]; ok && patch.ClientID != currentPublic.ClientID {
		return &ValidationError{Message: "auth.refresh.client_id is immutable"}
	}
	if _, ok := rawRefresh["token_endpoint"]; ok {
		canonical, err := canonicalizeCredentialEndpoint(patch.TokenEndpoint, "auth.refresh.token_endpoint")
		if err != nil {
			return err
		}
		if canonical != currentPublic.TokenEndpoint {
			return &ValidationError{Message: "auth.refresh.token_endpoint is immutable"}
		}
	}
	if value, ok := rawRefresh["resource"]; ok && !isJSONNull(value) && patch.Resource != currentPublic.Resource {
		return &ValidationError{Message: "auth.refresh.resource is immutable"}
	}
	if patch.RefreshToken != "" {
		currentSecret.RefreshToken = patch.RefreshToken
	}
	if _, ok := rawRefresh["scope"]; ok {
		currentSecret.Scope = patch.Scope
	}
	if _, ok := rawRefresh["resource"]; ok {
		currentSecret.Resource = patch.Resource
	}
	if patch.TokenEndpointAuth != nil {
		if err := validateTokenEndpointAuthForCreate(patch.TokenEndpointAuth); err != nil {
			return err
		}
		if patch.TokenEndpointAuth.Type == "none" {
			return &ValidationError{Message: "auth.refresh.token_endpoint_auth.type cannot be none on update"}
		}
		currentSecret.TokenEndpointAuth = patch.TokenEndpointAuth
	}
	return nil
}

func mapCredentialPostgreSQLError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &ConflictError{Message: "active MCP credential already exists for this vault and URL"}
	}
	return err
}

func mapCredentialDeletePostgreSQLError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(pgErr.Code == "23503" || pgErr.Code == "23001") &&
		pgErr.ConstraintName == "session_provider_auth_workspace_vault_credential_fkey" {
		return &ConflictError{
			Message:        "credential is still referenced by sessions",
			InvalidRequest: true,
		}
	}
	return err
}
