// Package authtest seeds Auth credential fixtures for tests. Production code
// issues API keys only through bootstrap refresh and
// auth.APIKeyStore.CreateForPrincipal; this package is imported only by tests.
package authtest

import (
	"context"
	"database/sql"
	"time"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// SeedIndependentKey inserts a standard independent API key for ws and returns
// its one-time raw material. The row matches the independent key that
// CreateForPrincipal issues: key_kind 'standard', authority_kind
// 'independent_key' and no identity lineage. It is a fixture shortcut for tests
// that need a key without first admitting an issuing principal.
func SeedIndependentKey(ctx context.Context, db *sql.DB, ws workspace.ID, name string) (*auth.CreateAPIKeyResult, error) {
	raw, err := auth.GenerateAPIKey()
	if err != nil {
		return nil, err
	}
	keyID := id.New("ak_")
	prefix := auth.KeyPrefixFor(raw)
	createdAt := storage.Now().Format(time.RFC3339)
	if err := storage.WithWorkspaceTx(ctx, db, string(ws), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO api_keys(id,workspace_id,name,key_prefix,key_digest,key_kind,authority_kind,created_at) VALUES($1,$2,$3,$4,$5,'standard','independent_key',$6)`, keyID, string(ws), name, prefix, auth.DigestAPIKey(raw), createdAt)
		return err
	}); err != nil {
		return nil, err
	}
	return &auth.CreateAPIKeyResult{APIKey: raw, APIKeyMetadata: auth.APIKeyMetadata{ID: keyID, Type: "api_key", WorkspaceID: string(ws), Name: name, KeyPrefix: prefix, KeyKind: auth.KindStandard, CreatedAt: createdAt}}, nil
}
