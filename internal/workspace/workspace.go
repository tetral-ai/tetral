// Package workspace defines the workspace identity type that flows
// through Engine after authentication. The workspace is the isolation
// unit for sessions, agents, environments, vaults, credentials, and
// API keys: every business handler observes its workspace through
// context, and PostgreSQL row-level security enforces the same boundary.
//
// Workspace identity is set on the request context by the
// authentication middleware (engine/internal/auth) and read by
// handlers/stores via FromContext / IDFromContext. Clients never
// supply workspace_id directly — the value always comes from the API
// key resolution path.
package workspace

import (
	"context"
	"errors"
)

// ID is the textual identifier of a workspace. It matches the
// `workspaces.id` column in the PostgreSQL schema.
type ID string

// MaxWorkspaceIDBytes bounds workspace identities at queue accept boundaries.
// Workspace rows are deployment-owned, so this is a transport fuse rather
// than a minting format or a system-wide workspace policy.
const MaxWorkspaceIDBytes = 128

// String returns the underlying identifier as a string. Useful for
// SQL bind parameters and log messages.
func (i ID) String() string { return string(i) }

// DefaultID is the legacy test fixture identifier used by many Engine tests.
// Production schema initialization never creates it and bootstrap startup must
// still be pointed at an already-existing workspace row.
const DefaultID ID = "default"

// Workspace is the canonical in-memory record for an authenticated
// workspace principal and storage row. CreatedAt is represented as a UTC
// RFC3339/ISO 8601 string consistent with every other Engine timestamp
// surface.
type Workspace struct {
	ID        ID     `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
}

// ErrNoWorkspaceInContext is returned by FromContext / IDFromContext
// when no authenticated workspace has been attached. Handlers and
// stores must treat this as a closed-failure signal: no row should
// be read or written without an explicit workspace identity.
var ErrNoWorkspaceInContext = errors.New("workspace: request context carries no authenticated workspace")

type contextKey struct{}

// WithContext returns a new context carrying the given workspace.
// The middleware calls this once per authenticated request; downstream
// handlers and stores read it through FromContext / IDFromContext.
func WithContext(ctx context.Context, ws Workspace) context.Context {
	return context.WithValue(ctx, contextKey{}, ws)
}

// FromContext returns the workspace previously attached by the
// authentication middleware. If no workspace was attached the second
// return is false; callers must fail closed in that case rather than
// defaulting to the seeded workspace.
func FromContext(ctx context.Context) (Workspace, bool) {
	ws, ok := ctx.Value(contextKey{}).(Workspace)
	return ws, ok
}

// IDFromContext is a thin wrapper around FromContext for callers that
// only need the workspace identifier — for example PostgreSQL store
// reads that pass the ID into WithWorkspaceTx.
func IDFromContext(ctx context.Context) (ID, bool) {
	ws, ok := FromContext(ctx)
	if !ok {
		return "", false
	}
	return ws.ID, true
}

// MustIDFromContext returns the workspace ID or
// ErrNoWorkspaceInContext if no workspace was attached. Convenient
// for store/manager methods that want to return the typed error
// directly to their HTTP caller.
func MustIDFromContext(ctx context.Context) (ID, error) {
	id, ok := IDFromContext(ctx)
	if !ok {
		return "", ErrNoWorkspaceInContext
	}
	return id, nil
}
