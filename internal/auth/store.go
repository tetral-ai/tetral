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

// APIKeyStore manages scoped credential metadata and issuance. Pre-workspace
// authentication delegates to AuthorityResolver's fixed digest lookup, current
// root locks, credential recheck and usage transaction. A caller-supplied lookup
// flag alone does not confer global table access.
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
			`INSERT INTO api_keys (id, workspace_id, name, key_prefix, key_digest, key_kind, authority_kind, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, 'independent_key', $7)`,
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

// AuthenticationResult retains the exact typed principal resolved for the key.
// Workspace/APIKeyID fields preserve the existing local caller contract.
type AuthenticationResult struct {
	Principal Principal
	APIKeyID  string
	Workspace workspace.Workspace
}

// AuthenticateRawKey checks current authority and credential usage atomically.
// Derived keys keep their identity-grant revisions and immutable ceiling.
func (s *APIKeyStore) AuthenticateRawKey(ctx context.Context, rawToken string) (*AuthenticationResult, error) {
	if rawToken == "" {
		return nil, &AuthenticationError{Message: "missing api key"}
	}
	principal, err := NewAuthorityResolver(s.db, "").AuthenticateKey(ctx, rawToken)
	if err != nil {
		return nil, err
	}
	return &AuthenticationResult{APIKeyID: principal.APIKeyID, Workspace: principal.Workspace, Principal: principal}, nil
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
		// Serialize each workspace's bootstrap startup without requiring Workspace
		// mutation privileges. The bootstrap workspace index alone cannot arbitrate
		// simultaneous inserts that also conflict on the global key_digest index.
		// The transaction releases this namespaced advisory lock on commit/rollback;
		// a hash collision only serializes otherwise independent bootstrap starts.
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('tetral.auth.bootstrap:' || $1::text, 0))`, string(workspaceID)); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at)
   VALUES($1,$2,'bootstrap',$3,$4,'bootstrap','independent_key',$5)
   ON CONFLICT(workspace_id) WHERE key_kind='bootstrap'
   DO UPDATE SET key_digest=EXCLUDED.key_digest,key_prefix=EXCLUDED.key_prefix,
     last_used_at=NULL,revoked_at=NULL,
     created_at=CASE WHEN api_keys.key_digest=EXCLUDED.key_digest THEN api_keys.created_at ELSE EXCLUDED.created_at END
   WHERE api_keys.key_digest<>EXCLUDED.key_digest OR api_keys.revoked_at IS NOT NULL`, id.New("ak_"), string(workspaceID), prefix, digest, now)
		return mapPostgreSQLError(err)
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

func runeCount(s string) int {
	n := 0
	for range s {
		n++
	}
	return n
}

// GetForWorkspace supplies trusted key ownership facts for the public gate.
func (s *APIKeyStore) GetForWorkspace(ctx context.Context, ws workspace.ID, keyID string) (APIKeyMetadata, error) {
	meta := APIKeyMetadata{Type: "api_key"}
	err := storage.WithWorkspaceTx(ctx, s.db, string(ws), func(tx *sql.Tx) error {
		var last, revoked sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT id,workspace_id,name,key_prefix,key_kind,created_at,last_used_at,revoked_at FROM api_keys WHERE id=$1 AND workspace_id=$2 AND revoked_at IS NULL`, keyID, string(ws)).Scan(&meta.ID, &meta.WorkspaceID, &meta.Name, &meta.KeyPrefix, &meta.KeyKind, &meta.CreatedAt, &last, &revoked)
		if err == sql.ErrNoRows {
			return &NotFoundError{Message: "api key not found"}
		}
		if last.Valid {
			meta.LastUsedAt = last.String
		}
		if revoked.Valid {
			meta.RevokedAt = revoked.String
		}
		return err
	})
	return meta, err
}
