package gitproxy

import (
	"context"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/githubrepo"
	"github.com/tetral-ai/tetral/internal/vault"
)

var errRepositoryIdentityAmbiguous = errors.New("git-proxy repository identity is ambiguous")

// PostgreSQLRepositoryTokenResolver resolves the git credential regime: the
// per-repository, write-only resource token that git-proxy injects as
// the upstream Authorization header for a mounted repository. This is one of
// two independent GitHub credential regimes, and the boundary between them is a
// cross-service law:
//
//   - The git regime (this resolver) and the MCP vault regime are separate
//     layers with no shared resolution. Neither reads the other's store; vault
//     credentials are never consumed by git transport, and this resolver never
//     yields an MCP credential.
//   - The resource token is write-only and is never platform-refreshed.
//     Identity (workspace, session, owner, repo) and the token are read fresh
//     per request with no cache, so a user-driven rotation takes effect on the
//     next request with no proxy coordination.
//   - A mounted row whose token is NULL/absent or undecryptable fails closed
//     with ErrGitHubCredentialRequired (relayed as 424 credential_required),
//     never an anonymous downgrade; a repository not mounted on the session
//     resolves unmounted, which policy.go relays anonymously with no upstream
//     Authorization header.
//
// UPDATE-WITH: policy.go (turns a resolution into the injected/anonymous/424
// arm), relay.go (injects the upstream Authorization and runs the 401 reactive
// re-read).
type PostgreSQLRepositoryTokenResolver struct {
	client    *dbconnect.Client
	decryptor vault.CredentialEncryptor
}

func NewPostgreSQLRepositoryTokenResolver(client *dbconnect.Client, decryptor vault.CredentialEncryptor) *PostgreSQLRepositoryTokenResolver {
	return &PostgreSQLRepositoryTokenResolver{client: client, decryptor: decryptor}
}

func (r *PostgreSQLRepositoryTokenResolver) ResolveRepositoryToken(ctx context.Context, request RepositoryAuthRequest) (RepositoryTokenResolution, error) {
	if r == nil || r.client == nil || r.decryptor == nil {
		return RepositoryTokenResolution{}, ErrRepositoryPolicyUnavailable
	}
	if request.WorkspaceID == "" || request.SessionID == "" || request.Owner == "" || request.Repo == "" {
		return RepositoryTokenResolution{}, ErrRepositoryPolicyUnavailable
	}

	comparisonKey := githubrepo.ComparisonKey("https://github.com/" + request.Owner + "/" + request.Repo)
	var encryptedTokens [][]byte
	err := r.client.WithWorkspaceReadOnlyTx(ctx, string(request.WorkspaceID), "gitproxy.repository_token_resolve", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT gr.authorization_token_encrypted
			   FROM session_github_repository_resources gr
			   JOIN session_resources sr
			     ON sr.workspace_id = gr.workspace_id
			    AND sr.session_id = gr.session_id
			    AND sr.resource_id = gr.resource_id
			   JOIN sessions s
			     ON s.workspace_id = gr.workspace_id
			    AND s.id = gr.session_id
			  WHERE gr.workspace_id = $1
			    AND gr.session_id = $2
			    AND TRANSLATE(
			        CASE
			            WHEN RIGHT(gr.url, 4) = '.git' THEN LEFT(gr.url, LENGTH(gr.url) - 4)
			            ELSE gr.url
			        END,
			        'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
			        'abcdefghijklmnopqrstuvwxyz'
			    ) = $3
			    AND sr.type = 'github_repository'
			    AND sr.detached_at IS NULL
			    AND sr.delete_requested_at IS NULL
			    AND s.lifecycle_state <> 'deleted'
			  ORDER BY gr.resource_id
			  LIMIT 2`,
			string(request.WorkspaceID),
			request.SessionID,
			comparisonKey,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var encryptedToken []byte
			if err := rows.Scan(&encryptedToken); err != nil {
				return err
			}
			encryptedTokens = append(encryptedTokens, append([]byte(nil), encryptedToken...))
		}
		return rows.Err()
	})
	if err != nil {
		return RepositoryTokenResolution{}, err
	}
	if len(encryptedTokens) == 0 {
		return RepositoryTokenResolution{}, nil
	}
	if len(encryptedTokens) > 1 {
		return RepositoryTokenResolution{}, errRepositoryIdentityAmbiguous
	}
	if len(encryptedTokens[0]) == 0 {
		return RepositoryTokenResolution{}, ErrGitHubCredentialRequired
	}
	plaintext, err := r.decryptor.Decrypt(encryptedTokens[0])
	if err != nil || len(plaintext) == 0 {
		return RepositoryTokenResolution{}, ErrGitHubCredentialRequired
	}
	return RepositoryTokenResolution{Mounted: true, Token: string(plaintext)}, nil
}
