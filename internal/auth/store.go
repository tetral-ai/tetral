package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// KindBootstrap and KindStandard match the api_keys.key_kind CHECK
// values defined in the schema. Bootstrap rows are managed by Engine
// startup from ENGINE_API_KEY; standard rows are created by
// authenticated POST /v1/api_keys requests.
const (
	KindBootstrap = "bootstrap"
	KindStandard  = "standard"
)

// keyMetadataNameMaxRunes is the upper bound on the caller-supplied
// `name` field for POST /v1/api_keys, expressed in Unicode code
// points so multi-byte names are counted consistently.
const keyMetadataNameMaxRunes = 256

// APIKeyMetadata is the public-safe view of an API key row. Raw key
// material and the stored digest are never included; key_prefix is
// metadata only and cannot authenticate.
type APIKeyMetadata struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	WorkspaceID string `json:"workspace_id"`
	Name        string `json:"name"`
	KeyPrefix   string `json:"key_prefix"`
	KeyKind     string `json:"key_kind"`
	CreatedAt   string `json:"created_at"`
	LastUsedAt  string `json:"last_used_at,omitempty"`
	RevokedAt   string `json:"revoked_at,omitempty"`
}

// CreateAPIKeyResult is the one-time response shape for
// POST /v1/api_keys. APIKey is the raw bearer token; it is returned
// only in the create response and never read back from PostgreSQL.
type CreateAPIKeyResult struct {
	APIKeyMetadata
	APIKey string `json:"api_key"`
}

// APIKeyStore manages workspace-scoped API key rows on top of the
// PostgreSQL `api_keys` table. Workspace-scoped management calls run
// under transaction-local `tetral.workspace_id` set by
// storage.WithWorkspaceTx; the narrow authentication lookup uses a
// transaction-local `tetral.auth_lookup = 'true'` setting that the
// auth_lookup RLS policy permits FOR SELECT only.
type APIKeyStore struct {
	db *sql.DB
}

// NewAPIKeyStore constructs an APIKeyStore backed by db.
func NewAPIKeyStore(db *sql.DB) *APIKeyStore {
	return &APIKeyStore{db: db}
}

// CreateForWorkspace inserts a new standard API key for workspaceID.
// It generates the raw token through GenerateAPIKey, stores its
// digest + prefix metadata, and returns the create-only response
// shape including the raw token. The raw token is never persisted in
// any text or byte column.
func (s *APIKeyStore) CreateForWorkspace(ctx context.Context, workspaceID workspace.ID, name string) (*CreateAPIKeyResult, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return nil, &ValidationError{Message: "name must not be empty"}
	}
	if runeCount(trimmed) > keyMetadataNameMaxRunes {
		return nil, &ValidationError{Message: fmt.Sprintf("name must be at most %d Unicode code points", keyMetadataNameMaxRunes)}
	}

	rawToken, err := GenerateAPIKey()
	if err != nil {
		return nil, err
	}
	digest := DigestAPIKey(rawToken)
	prefix := KeyPrefixFor(rawToken)
	now := storage.Now().Format(time.RFC3339)
	apiKeyID := id.New("ak_")

	var stored APIKeyMetadata
	if err := storage.WithWorkspaceTx(ctx, s.db, string(workspaceID), func(tx *sql.Tx) error {
		_, execErr := tx.ExecContext(ctx,
			`INSERT INTO api_keys (id, workspace_id, name, key_prefix, key_digest, key_kind, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			apiKeyID, string(workspaceID), trimmed, prefix, digest, KindStandard, now,
		)
		if execErr != nil {
			return mapPostgreSQLError(execErr)
		}
		stored = APIKeyMetadata{
			ID:          apiKeyID,
			Type:        "api_key",
			WorkspaceID: string(workspaceID),
			Name:        trimmed,
			KeyPrefix:   prefix,
			KeyKind:     KindStandard,
			CreatedAt:   now,
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &CreateAPIKeyResult{
		APIKeyMetadata: stored,
		APIKey:         rawToken,
	}, nil
}

// ListActiveForWorkspace returns active (non-revoked) API key
// metadata for workspaceID, ordered deterministically by
// storage_sequence. Cursor IDs from another workspace are rejected
// through ValidationError so cross-workspace cursor probes do not
// leak metadata.
func (s *APIKeyStore) ListActiveForWorkspace(ctx context.Context, workspaceID workspace.ID, limit int, afterID, beforeID string) ([]APIKeyMetadata, bool, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	var (
		results []APIKeyMetadata
		hasMore bool
	)
	err := storage.WithWorkspaceTx(ctx, s.db, string(workspaceID), func(tx *sql.Tx) error {
		if afterID != "" {
			if err := validateCursorTx(ctx, tx, workspaceID, afterID); err != nil {
				return err
			}
		}
		if beforeID != "" {
			if err := validateCursorTx(ctx, tx, workspaceID, beforeID); err != nil {
				return err
			}
		}

		var query string
		var args []any
		switch {
		case afterID != "" && beforeID != "":
			query = `SELECT id, workspace_id, name, key_prefix, key_kind, created_at, last_used_at, revoked_at
			          FROM api_keys
			          WHERE workspace_id = $1 AND revoked_at IS NULL
			            AND storage_sequence > (SELECT storage_sequence FROM api_keys WHERE id = $2 AND workspace_id = $1)
			            AND storage_sequence < (SELECT storage_sequence FROM api_keys WHERE id = $3 AND workspace_id = $1)
			          ORDER BY storage_sequence ASC LIMIT $4`
			args = []any{string(workspaceID), afterID, beforeID, limit + 1}
		case afterID != "":
			query = `SELECT id, workspace_id, name, key_prefix, key_kind, created_at, last_used_at, revoked_at
			          FROM api_keys
			          WHERE workspace_id = $1 AND revoked_at IS NULL
			            AND storage_sequence > (SELECT storage_sequence FROM api_keys WHERE id = $2 AND workspace_id = $1)
			          ORDER BY storage_sequence ASC LIMIT $3`
			args = []any{string(workspaceID), afterID, limit + 1}
		case beforeID != "":
			query = `SELECT id, workspace_id, name, key_prefix, key_kind, created_at, last_used_at, revoked_at
			          FROM api_keys
			          WHERE workspace_id = $1 AND revoked_at IS NULL
			            AND storage_sequence < (SELECT storage_sequence FROM api_keys WHERE id = $2 AND workspace_id = $1)
			          ORDER BY storage_sequence ASC LIMIT $3`
			args = []any{string(workspaceID), beforeID, limit + 1}
		default:
			query = `SELECT id, workspace_id, name, key_prefix, key_kind, created_at, last_used_at, revoked_at
			          FROM api_keys
			          WHERE workspace_id = $1 AND revoked_at IS NULL
			          ORDER BY storage_sequence ASC LIMIT $2`
			args = []any{string(workspaceID), limit + 1}
		}

		rows, queryErr := tx.QueryContext(ctx, query, args...)
		if queryErr != nil {
			return queryErr
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			meta := APIKeyMetadata{Type: "api_key"}
			var lastUsed, revokedAt sql.NullString
			if err := rows.Scan(&meta.ID, &meta.WorkspaceID, &meta.Name, &meta.KeyPrefix, &meta.KeyKind, &meta.CreatedAt, &lastUsed, &revokedAt); err != nil {
				return err
			}
			if lastUsed.Valid {
				meta.LastUsedAt = lastUsed.String
			}
			if revokedAt.Valid {
				meta.RevokedAt = revokedAt.String
			}
			results = append(results, meta)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		hasMore = len(results) > limit
		if hasMore {
			results = results[:limit]
		}
		if results == nil {
			results = []APIKeyMetadata{}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return results, hasMore, nil
}

// RevokeForWorkspace marks the API key with apiKeyID as revoked in
// workspaceID. Returns NotFoundError when the row does not exist, was
// already revoked, or belongs to another workspace — RLS plus the
// explicit workspace predicate together ensure cross-workspace
// requests cannot succeed.
func (s *APIKeyStore) RevokeForWorkspace(ctx context.Context, workspaceID workspace.ID, apiKeyID string) error {
	now := storage.Now().Format(time.RFC3339)
	return storage.WithWorkspaceTx(ctx, s.db, string(workspaceID), func(tx *sql.Tx) error {
		result, execErr := tx.ExecContext(ctx,
			`UPDATE api_keys SET revoked_at = $1
			 WHERE id = $2 AND workspace_id = $3 AND revoked_at IS NULL`,
			now, apiKeyID, string(workspaceID),
		)
		if execErr != nil {
			return mapPostgreSQLError(execErr)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &NotFoundError{Message: "api key not found"}
		}
		return nil
	})
}

// TouchLastUsedForWorkspace updates audit metadata for a successfully
// authenticated key. auth calls this after the digest lookup
// succeeds, through the workspace-scoped RLS path instead of the
// cross-workspace auth lookup transaction.
func (s *APIKeyStore) TouchLastUsedForWorkspace(ctx context.Context, workspaceID workspace.ID, apiKeyID string) error {
	now := storage.Now().Format(time.RFC3339)
	return storage.WithWorkspaceTx(ctx, s.db, string(workspaceID), func(tx *sql.Tx) error {
		result, execErr := tx.ExecContext(ctx,
			`UPDATE api_keys SET last_used_at = $1
			 WHERE id = $2 AND workspace_id = $3 AND revoked_at IS NULL`,
			now, apiKeyID, string(workspaceID),
		)
		if execErr != nil {
			return mapPostgreSQLError(execErr)
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return &AuthenticationError{Message: "invalid api key"}
		}
		return nil
	})
}

// AuthenticationResult is the result of a successful raw-key lookup:
// the workspace.Workspace owning the matching api_key row.
type AuthenticationResult struct {
	APIKeyID  string
	Workspace workspace.Workspace
}

// AuthenticateRawKey looks up the api_keys row whose digest matches
// SHA-256(rawToken) and is not revoked. The lookup runs inside a
// transaction with `tetral.auth_lookup = 'true'` set
// transaction-locally so the narrow auth_lookup RLS policy permits
// the cross-workspace SELECT; no other workspace_id GUC is set, so
// any accidental write through this transaction would be blocked by
// workspace_isolation. Workspace fields (id, type, name, created_at)
// are joined in the same query so callers do not need a follow-up
// workspaces.Get call.
func (s *APIKeyStore) AuthenticateRawKey(ctx context.Context, rawToken string) (*AuthenticationResult, error) {
	if rawToken == "" {
		return nil, &AuthenticationError{Message: "missing api key"}
	}
	digest := DigestAPIKey(rawToken)

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx,
		`SELECT set_config('tetral.auth_lookup', 'true', true)`); err != nil {
		return nil, err
	}

	var apiKeyID string
	var ws workspace.Workspace
	err = tx.QueryRowContext(ctx,
		`SELECT k.id, k.workspace_id, w.type, w.name, w.created_at
		   FROM api_keys k JOIN workspaces w ON w.id = k.workspace_id
		  WHERE k.key_digest = $1 AND k.revoked_at IS NULL`,
		digest,
	).Scan(&apiKeyID, &ws.ID, &ws.Type, &ws.Name, &ws.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, &AuthenticationError{Message: "invalid api key"}
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return &AuthenticationResult{APIKeyID: apiKeyID, Workspace: ws}, nil
}

// UpsertBootstrap atomically refreshes the bootstrap api_keys row
// for workspaceID to point at digest + prefix derived from
// rawBootstrap. If no bootstrap row exists, one is created; if a
// bootstrap row exists with a different digest, it is updated in
// place; if the digest already matches an active row, the operation
// is a no-op; if the digest matches a revoked bootstrap row,
// revoked_at is cleared on that same row so the configured
// ENGINE_API_KEY is authoritative after restart. Standard
// (key_kind='standard') rows are untouched, so refreshing the
// bootstrap key never invalidates or reactivates workspace-managed
// keys.
func (s *APIKeyStore) UpsertBootstrap(ctx context.Context, workspaceID workspace.ID, rawBootstrap string) error {
	digest := DigestAPIKey(rawBootstrap)
	prefix := KeyPrefixFor(rawBootstrap)
	now := storage.Now().Format(time.RFC3339)

	return storage.WithWorkspaceTx(ctx, s.db, string(workspaceID), func(tx *sql.Tx) error {
		var existingID string
		var existingDigest []byte
		var existingRevokedAt sql.NullTime
		err := tx.QueryRowContext(ctx,
			`SELECT id, key_digest, revoked_at FROM api_keys
			  WHERE workspace_id = $1 AND key_kind = $2`,
			string(workspaceID), KindBootstrap,
		).Scan(&existingID, &existingDigest, &existingRevokedAt)
		switch {
		case err == sql.ErrNoRows:
			apiKeyID := id.New("ak_")
			_, insertErr := tx.ExecContext(ctx,
				`INSERT INTO api_keys (id, workspace_id, name, key_prefix, key_digest, key_kind, created_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				apiKeyID, string(workspaceID), "bootstrap", prefix, digest, KindBootstrap, now,
			)
			if insertErr != nil {
				return mapPostgreSQLError(insertErr)
			}
			return nil
		case err != nil:
			return err
		}
		if bytesEqual(existingDigest, digest) {
			if !existingRevokedAt.Valid {
				return nil
			}
			_, updateErr := tx.ExecContext(ctx,
				`UPDATE api_keys
				    SET last_used_at = NULL,
				        revoked_at = NULL
				  WHERE id = $1 AND workspace_id = $2 AND key_kind = $3`,
				existingID, string(workspaceID), KindBootstrap,
			)
			if updateErr != nil {
				return mapPostgreSQLError(updateErr)
			}
			return nil
		}
		// Refresh: replace the digest and prefix in place. last_used_at
		// is cleared because the previous secret no longer applies.
		_, updateErr := tx.ExecContext(ctx,
			`UPDATE api_keys
			    SET key_digest = $1,
			        key_prefix = $2,
			        last_used_at = NULL,
			        revoked_at = NULL,
			        created_at = $3
			  WHERE id = $4 AND workspace_id = $5 AND key_kind = $6`,
			digest, prefix, now, existingID, string(workspaceID), KindBootstrap,
		)
		if updateErr != nil {
			return mapPostgreSQLError(updateErr)
		}
		return nil
	})
}

func validateCursorTx(ctx context.Context, tx *sql.Tx, workspaceID workspace.ID, cursorID string) error {
	var exists int
	err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM api_keys WHERE id = $1 AND workspace_id = $2`,
		cursorID, string(workspaceID),
	).Scan(&exists)
	if err == sql.ErrNoRows {
		return &ValidationError{Message: "invalid api key cursor"}
	}
	return err
}

// mapPostgreSQLError converts pgconn.PgError values returned by the
// PostgreSQL driver into typed Engine errors. Today this mapping is
// narrow — unique-violation on api_keys becomes ValidationError —
// but the helper exists so future store paths can extend it without
// scattering SQLSTATE strings through the codebase.
func mapPostgreSQLError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return &ValidationError{Message: "api key constraint violated"}
	}
	return err
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}
