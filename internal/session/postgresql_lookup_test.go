package session_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

// lookupStatementTracer records the text of every statement its pool sends.
type lookupStatementTracer struct {
	mu         sync.Mutex
	statements []string
}

func (r *lookupStatementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	r.statements = append(r.statements, data.SQL)
	r.mu.Unlock()
	return ctx
}

func (*lookupStatementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *lookupStatementTracer) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	statements := r.statements
	r.statements = nil
	return statements
}

// The authorization lookups run as the API workload role. Each predicate has a
// fixture that only it rejects, every nondeleted lifecycle state stays visible,
// and another workspace's rows resolve only for their own workspace. Every
// lookup is one SELECT in one read-only transaction.
func TestPostgreSQLSessionLookupsResolveIdentityUnderAPIRole(t *testing.T) {
	_, admin := newControlPlaneSessionStoreTestDB(t)
	workload := storagetest.OpenWorkloadDB(t, admin, "api")
	fixtures := newControlPlaneSessionStore(t, workload.DB)
	tracer := &lookupStatementTracer{}
	store := newControlPlaneSessionStore(t, storagetest.OpenRuntimeRoleDBWithTracer(t, workload.DB, tracer))
	ctx := context.Background()
	ws := workspace.DefaultID
	foreign := workspace.ID("workspace_lookup_foreign")
	now := time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC)
	seedSessionStoreReferences(t, admin, ws, "agent_lookup", 1, "env_lookup")
	seedSessionStoreReferences(t, admin, foreign, "agent_lookup_foreign", 1, "env_lookup_foreign")

	// One Session per lifecycle state, each with its primary Thread and a
	// non-file Resource; the deleted Session keeps its Resource attached so only
	// the parent lifecycle predicate hides it.
	lifecycleStates := []string{"admitted", "active", "archiving", "archived", "deleted"}
	for _, state := range lifecycleStates {
		createStoreSessionWithPrimaryThread(t, fixtures, "sesn_lookup_"+state, "thr_lookup_"+state, "agent_lookup", "env_lookup", now)
		seedLookupResource(t, admin, ws, "sesn_lookup_"+state, "sesrsc_lookup_"+state, "github_repository", "", nil, nil)
		var archivedAt *time.Time
		if state == "archived" {
			archivedAt = &now
		}
		if _, err := admin.ExecContext(ctx, `UPDATE sessions SET lifecycle_state = $2, archived_at = $3 WHERE id = $1`,
			"sesn_lookup_"+state, state, nullableTestTime(archivedAt)); err != nil {
			t.Fatalf("set lifecycle %s: %v", state, err)
		}
	}
	const parent, otherParent = "sesn_lookup_active", "sesn_lookup_admitted"
	archivedAt := now.Add(time.Minute)
	seedSessionThread(t, admin, ws, parent, "thr_lookup_child", "thr_lookup_active", "subagent", "public", "idle", now, nil)
	seedSessionThread(t, admin, ws, parent, "thr_lookup_archived_child", "thr_lookup_active", "subagent", "public", "idle", now, &archivedAt)
	seedSessionThread(t, admin, ws, parent, "thr_lookup_internal", "thr_lookup_active", "subagent", "internal", "idle", now, nil)
	seedSessionThread(t, admin, ws, parent, "thr_lookup_reviewer", "thr_lookup_active", "approval_reviewer", "public", "idle", now, nil)

	seedLookupFile(t, admin, ws, "file_lookup_source", "", false)
	seedLookupFile(t, admin, ws, "file_lookup_live", parent, false)
	seedLookupFile(t, admin, ws, "file_lookup_tombstoned", parent, true)
	seedLookupFile(t, admin, ws, "file_lookup_unscoped", "", false)
	seedLookupFile(t, admin, ws, "file_lookup_other_session", otherParent, false)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_file", "file", "file_lookup_live", nil, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_tombstoned", "file", "file_lookup_tombstoned", nil, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_unscoped", "file", "file_lookup_unscoped", nil, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_other_session", "file", "file_lookup_other_session", nil, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_unlinked", "file", "", nil, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_detached", "github_repository", "", &now, nil)
	seedLookupResource(t, admin, ws, parent, "sesrsc_lookup_pending_delete", "github_repository", "", nil, &now)

	createWorkspaceStoreSessionWithPrimaryThread(t, fixtures, foreign, "sesn_lookup_foreign", "thr_lookup_foreign", "agent_lookup_foreign", "env_lookup_foreign", now)
	seedLookupResource(t, admin, foreign, "sesn_lookup_foreign", "sesrsc_lookup_foreign", "github_repository", "", nil, nil)

	const (
		sessionMissing  = "session not found"
		threadMissing   = "session thread not found"
		resourceMissing = "session resource not found"
	)
	type lookupCase struct {
		name    string
		lookup  func() (string, error)
		want    string // canonical ID when found
		missing string // NotFound message when absent
	}
	lookupSession := func(ws workspace.ID, sessionID string) func() (string, error) {
		return func() (string, error) { return store.LookupSession(ctx, ws, sessionID) }
	}
	lookupThread := func(ws workspace.ID, sessionID, threadID string) func() (string, error) {
		return func() (string, error) { return store.LookupThread(ctx, ws, sessionID, threadID) }
	}
	lookupResource := func(ws workspace.ID, sessionID, resourceID string) func() (string, error) {
		return func() (string, error) { return store.LookupResource(ctx, ws, sessionID, resourceID) }
	}
	var cases []lookupCase
	for _, state := range lifecycleStates {
		sessionID, threadID, resourceID := "sesn_lookup_"+state, "thr_lookup_"+state, "sesrsc_lookup_"+state
		if state == "deleted" {
			cases = append(cases,
				lookupCase{"deleted session", lookupSession(ws, sessionID), "", sessionMissing},
				lookupCase{"thread of deleted session", lookupThread(ws, sessionID, threadID), "", threadMissing},
				lookupCase{"resource of deleted session", lookupResource(ws, sessionID, resourceID), "", resourceMissing})
			continue
		}
		cases = append(cases,
			lookupCase{state + " session", lookupSession(ws, sessionID), sessionID, ""},
			lookupCase{"thread of " + state + " session", lookupThread(ws, sessionID, threadID), threadID, ""},
			lookupCase{"non-file resource of " + state + " session", lookupResource(ws, sessionID, resourceID), resourceID, ""})
	}
	cases = append(cases,
		lookupCase{"missing session", lookupSession(ws, "sesn_lookup_missing"), "", sessionMissing},
		lookupCase{"foreign session from its workspace", lookupSession(foreign, "sesn_lookup_foreign"), "sesn_lookup_foreign", ""},
		lookupCase{"foreign session from another workspace", lookupSession(ws, "sesn_lookup_foreign"), "", sessionMissing},

		lookupCase{"public child thread", lookupThread(ws, parent, "thr_lookup_child"), "thr_lookup_child", ""},
		lookupCase{"archived public thread", lookupThread(ws, parent, "thr_lookup_archived_child"), "thr_lookup_archived_child", ""},
		lookupCase{"internal thread", lookupThread(ws, parent, "thr_lookup_internal"), "", threadMissing},
		lookupCase{"public reviewer thread", lookupThread(ws, parent, "thr_lookup_reviewer"), "", threadMissing},
		lookupCase{"thread under wrong parent", lookupThread(ws, otherParent, "thr_lookup_child"), "", threadMissing},
		lookupCase{"missing thread", lookupThread(ws, parent, "thr_lookup_missing"), "", threadMissing},
		lookupCase{"foreign thread from its workspace", lookupThread(foreign, "sesn_lookup_foreign", "thr_lookup_foreign"), "thr_lookup_foreign", ""},
		lookupCase{"foreign thread from another workspace", lookupThread(ws, "sesn_lookup_foreign", "thr_lookup_foreign"), "", threadMissing},

		lookupCase{"live session file resource", lookupResource(ws, parent, "sesrsc_lookup_file"), "sesrsc_lookup_file", ""},
		lookupCase{"tombstoned file resource", lookupResource(ws, parent, "sesrsc_lookup_tombstoned"), "", resourceMissing},
		lookupCase{"workspace-scoped file resource", lookupResource(ws, parent, "sesrsc_lookup_unscoped"), "", resourceMissing},
		lookupCase{"file scoped to another session", lookupResource(ws, parent, "sesrsc_lookup_other_session"), "", resourceMissing},
		lookupCase{"file resource without file relation", lookupResource(ws, parent, "sesrsc_lookup_unlinked"), "", resourceMissing},
		lookupCase{"detached resource", lookupResource(ws, parent, "sesrsc_lookup_detached"), "", resourceMissing},
		lookupCase{"delete-requested resource", lookupResource(ws, parent, "sesrsc_lookup_pending_delete"), "", resourceMissing},
		lookupCase{"resource under wrong parent", lookupResource(ws, otherParent, "sesrsc_lookup_file"), "", resourceMissing},
		lookupCase{"foreign resource from its workspace", lookupResource(foreign, "sesn_lookup_foreign", "sesrsc_lookup_foreign"), "sesrsc_lookup_foreign", ""},
		lookupCase{"foreign resource from another workspace", lookupResource(ws, "sesn_lookup_foreign", "sesrsc_lookup_foreign"), "", resourceMissing},
	)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tracer.take()
			got, err := c.lookup()
			statements := tracer.take()
			if c.missing == "" {
				if err != nil || got != c.want {
					t.Fatalf("lookup = %q, %v; want %q", got, err, c.want)
				}
			} else {
				var notFound *session.NotFoundError
				if !errors.As(err, &notFound) || notFound.Message != c.missing || got != "" {
					t.Fatalf("lookup = %q, %T %v; want NotFoundError %q", got, err, err, c.missing)
				}
			}
			assertOneReadOnlyLookupSelect(t, statements)
		})
	}
}

// assertOneReadOnlyLookupSelect accepts transaction control and the workspace
// setting around exactly one SELECT in a read-only transaction.
func assertOneReadOnlyLookupSelect(t *testing.T, statements []string) {
	t.Helper()
	var begins, reads []string
	for _, statement := range statements {
		normalized := strings.ToLower(strings.TrimSpace(statement))
		switch {
		case strings.HasPrefix(normalized, "begin"):
			begins = append(begins, normalized)
		case normalized == "commit", normalized == "rollback":
		case strings.HasPrefix(normalized, "select set_config('tetral.workspace_id'"):
		default:
			reads = append(reads, normalized)
		}
	}
	if len(begins) != 1 || begins[0] != "begin read only" || len(reads) != 1 || !strings.HasPrefix(reads[0], "select") {
		t.Fatalf("lookup statements = %q; want one SELECT in one read-only transaction", statements)
	}
}

func seedLookupFile(t *testing.T, db *sql.DB, ws workspace.ID, fileID string, scopeSessionID string, tombstoned bool) {
	t.Helper()
	objectID := "fobj_" + fileID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, 1, 'sha', '2026-01-01T00:00:00Z')`,
		string(ws), objectID, "files/"+string(ws)+"/"+objectID,
	); err != nil {
		t.Fatalf("seed file object %s: %v", objectID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at, deleted_at)
		 VALUES ($1, $2, $3, $4, 'text/plain', false, $5, $6, '2026-01-01T00:00:00Z',
		         CASE WHEN $7 THEN '2026-01-02T00:00:00Z'::timestamptz END)`,
		string(ws), fileID, objectID, fileID+".txt",
		sql.NullString{String: "session", Valid: scopeSessionID != ""},
		sql.NullString{String: scopeSessionID, Valid: scopeSessionID != ""},
		tombstoned,
	); err != nil {
		t.Fatalf("seed file %s: %v", fileID, err)
	}
}

// seedLookupResource writes a Resource row and, when fileID is set, its file
// relation. A file Resource without fileID has no relation row.
func seedLookupResource(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, resourceID string, resourceType string, fileID string, detachedAt *time.Time, deleteRequestedAt *time.Time) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at, detached_at, delete_requested_at)
		 VALUES ($1, $2, $3, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', $5, $6)`,
		string(ws), sessionID, resourceID, resourceType, nullableTestTime(detachedAt), nullableTestTime(deleteRequestedAt),
	); err != nil {
		t.Fatalf("seed resource %s: %v", resourceID, err)
	}
	if fileID == "" {
		return
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_file_resources (workspace_id, session_id, resource_id, source_file_id, file_id, mount_path)
		 VALUES ($1, $2, $3, 'file_lookup_source', $4, $5)`,
		string(ws), sessionID, resourceID, fileID, "/workspace/"+resourceID+".txt",
	); err != nil {
		t.Fatalf("seed file relation %s: %v", resourceID, err)
	}
}
