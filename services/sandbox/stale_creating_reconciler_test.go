package tetralsandbox

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestStaleCreatingReconcilerScansExpiredCreatingSandboxes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedStaleCreatingSession(t, admin, "sesn_stale_creating_old")
	seedStaleCreatingSession(t, admin, "sesn_stale_creating_fresh")
	seedStaleCreatingSession(t, admin, "sesn_stale_resuming_old")
	seedStaleCreatingSession(t, admin, "sesn_stale_deleted")
	seedStaleCreatingSandbox(t, admin, "sandbox_stale_creating_old", "sesn_stale_creating_old", "2026-01-01T00:00:00Z")
	seedStaleCreatingSandbox(t, admin, "sandbox_stale_creating_fresh", "sesn_stale_creating_fresh", "2026-01-01T00:02:00Z")
	seedStaleCreatingSandbox(t, admin, "sandbox_stale_resuming_old", "sesn_stale_resuming_old", "2026-01-01T00:00:01Z")
	seedStaleCreatingSandbox(t, admin, "sandbox_stale_deleted", "sesn_stale_deleted", "2026-01-01T00:00:02Z")
	if _, err := admin.ExecContext(context.Background(), `UPDATE sandboxes SET status='resuming' WHERE id='sandbox_stale_resuming_old'`); err != nil {
		t.Fatalf("mark resuming: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted' WHERE id='sesn_stale_deleted'`); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}
	handler := &recordingStaleCreatingHandler{}
	runner := &StaleCreatingReconciler{
		Client:  dbconnect.NewClientForTesting(runtime),
		Handler: handler,
		Config: StaleCreatingReconcilerConfig{
			WorkspaceID: string(workspace.DefaultID),
			StaleAfter:  time.Minute,
			Clock:       func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) },
		},
	}

	due, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !reflect.DeepEqual(due, []string{"sandbox_stale_creating_old", "sandbox_stale_resuming_old", "sandbox_stale_deleted"}) {
		t.Fatalf("due sandboxes = %+v; want all stale creating and resuming rows including deleted-session crash state", due)
	}
	if !reflect.DeepEqual(handler.calls, []string{"default/sandbox_stale_creating_old", "default/sandbox_stale_resuming_old", "default/sandbox_stale_deleted"}) {
		t.Fatalf("handler calls = %+v; want stale startup reconciles", handler.calls)
	}
}

func TestStaleCreatingReconcilerSettlesDeletedStartupCrashForDeleteOwnerOnly(t *testing.T) {
	for _, status := range []sandbox.Status{sandbox.StatusCreating, sandbox.StatusResuming} {
		t.Run(string(status), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			ctx := context.Background()
			sessionID := "sesn_deleted_crash_" + string(status)
			sandboxID := "sandbox_deleted_crash_" + string(status)
			seedStaleCreatingSession(t, admin, sessionID)
			seedStaleCreatingSandbox(t, admin, sandboxID, sessionID, "2026-01-01T00:00:00Z")
			if _, err := admin.ExecContext(ctx, `UPDATE sandboxes SET status=$2 WHERE id=$1`, sandboxID, status); err != nil {
				t.Fatalf("set startup status: %v", err)
			}
			if _, err := admin.ExecContext(ctx, `UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, sessionID, "delcln_"+string(status)); err != nil {
				t.Fatalf("tombstone session: %v", err)
			}
			store := sandbox.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			provider := &recordingReleaseProvider{}
			service := sandbox.NewService(store, provider, sandbox.WithProviderName("tetral"), sandbox.WithStaleStartupThreshold(time.Minute), sandbox.WithClock(func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }))
			runner := &StaleCreatingReconciler{Client: dbconnect.NewClientForTesting(runtime), Handler: service, Config: StaleCreatingReconcilerConfig{WorkspaceID: "default", StaleAfter: time.Minute, Clock: func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }}}
			if _, err := runner.RunOnce(ctx); err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
			if err != nil {
				t.Fatalf("FindLatestBySessionID: %v", err)
			}
			if got.Status != sandbox.StatusFailed || got.CleanupStatus != sandbox.CleanupStatusPending || got.CleanupAttemptCount != 0 || provider.statusCalls != 0 || provider.releaseCalls != 0 {
				t.Fatalf("after startup reconcile sandbox=%+v provider status/release=%d/%d; want failed pending and zero startup-cleanup ownership", got, provider.statusCalls, provider.releaseCalls)
			}
			if err := service.ReleaseForSession(ctx, workspace.DefaultID, sessionID, sandbox.ReleaseReasonDelete); err != nil {
				t.Fatalf("delete owner ReleaseForSession: %v", err)
			}
			got, err = store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
			if err != nil || got.Status != sandbox.StatusReleased || provider.statusCalls != 1 || provider.releaseCalls != 1 {
				t.Fatalf("delete owner result sandbox=%+v err=%v provider status/release=%d/%d", got, err, provider.statusCalls, provider.releaseCalls)
			}
		})
	}
}

type recordingStaleCreatingHandler struct {
	calls []string
}

func (h *recordingStaleCreatingHandler) ReconcileStaleCreating(_ context.Context, ws workspace.ID, sandboxID string) (*sandbox.Sandbox, error) {
	h.calls = append(h.calls, string(ws)+"/"+sandboxID)
	return &sandbox.Sandbox{WorkspaceID: ws, ID: sandboxID, Status: sandbox.StatusFailed}, nil
}

func seedStaleCreatingSession(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	createdAt := "2026-01-01T00:00:00Z"
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ('default', 'workspace', 'default', $1)
		 ON CONFLICT (id) DO NOTHING`,
		createdAt,
	); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, description, version, created_at, updated_at)
		 VALUES ('default', 'agent_stale_creating', 'Sandbox Agent', '', 1, $1, $1)
		 ON CONFLICT (id) DO NOTHING`,
		createdAt,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ('default', 'agentver_stale_creating', 'agent_stale_creating', 1, '{}', 'hash_stale_creating', $1)
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		createdAt,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, description, config_json, metadata_json, created_at, updated_at)
		 VALUES ('default', 'env_stale_creating', 'Sandbox Env', '', '{"type":"container","networking":{"type":"unrestricted"},"packages":{}}', '{}', $1, $1)
		 ON CONFLICT (id) DO NOTHING`,
		createdAt,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, agent_id, agent_version,
			environment_id, vault_ids_json, created_at, updated_at
		) VALUES ('default', $1, 'session', NULL, '{}', 'idle', 'agent_stale_creating', 1, 'env_stale_creating', '[]', $2, $2)`,
		sessionID,
		createdAt,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
}

func seedStaleCreatingSandbox(t *testing.T, db *sql.DB, sandboxID string, sessionID string, updatedAt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_sandbox_id,
			provider_metadata_json, created_at, updated_at, cleanup_status
		) VALUES ('default', $1, $2, 'creating', 'tetral', NULL, '{}', '2026-01-01T00:00:00Z', $3, 'none')`,
		sandboxID,
		sessionID,
		updatedAt,
	); err != nil {
		t.Fatalf("seed sandbox: %v", err)
	}
}
