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

func TestStartupCleanupReconcilerScansOnlyDueFailedSandboxes(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	now := time.Date(2026, 1, 1, 0, 5, 0, 120500000, time.UTC)
	for _, sessionID := range []string{
		"sesn_cleanup_pending",
		"sesn_cleanup_due",
		"sesn_cleanup_future",
		"sesn_cleanup_terminal",
		"sesn_cleanup_deleted",
		"sesn_cleanup_expired_lease",
		"sesn_cleanup_fractional_expired_lease",
		"sesn_cleanup_live_lease",
	} {
		seedStaleCreatingSession(t, admin, sessionID)
	}
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_pending", "sesn_cleanup_pending", "pending", "")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_due", "sesn_cleanup_due", "retryable_failed", "2026-01-01T00:04:00Z")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_future", "sesn_cleanup_future", "retryable_failed", "2026-01-01T00:06:00Z")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_terminal", "sesn_cleanup_terminal", "released", "")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_deleted", "sesn_cleanup_deleted", "pending", "")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_expired_lease", "sesn_cleanup_expired_lease", "pending", "")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_fractional_expired_lease", "sesn_cleanup_fractional_expired_lease", "pending", "")
	seedStartupCleanupSandbox(t, admin, "sandbox_cleanup_live_lease", "sesn_cleanup_live_lease", "pending", "")
	for _, lease := range []struct {
		sandboxID string
		token     string
		expiry    string
	}{
		{sandboxID: "sandbox_cleanup_expired_lease", token: "lease_expired", expiry: "2026-01-01T00:04:30Z"},
		{sandboxID: "sandbox_cleanup_fractional_expired_lease", token: "lease_fractional_expired", expiry: "2026-01-01T00:05:00.12Z"},
		{sandboxID: "sandbox_cleanup_live_lease", token: "lease_live", expiry: "2026-01-01T00:06:00Z"},
	} {
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE sandboxes
			    SET cleanup_status='in_progress',
			        cleanup_lease_token=$2,
			        cleanup_lease_expires_at=$3
			  WHERE id=$1`,
			lease.sandboxID, lease.token, lease.expiry,
		); err != nil {
			t.Fatalf("lease startup cleanup sandbox: %v", err)
		}
	}
	if _, err := admin.ExecContext(context.Background(), `UPDATE sessions SET lifecycle_state='deleted' WHERE id='sesn_cleanup_deleted'`); err != nil {
		t.Fatalf("mark deleted session: %v", err)
	}

	handler := &recordingStartupCleanupHandler{}
	runner := &StartupCleanupReconciler{
		Client:  dbconnect.NewClientForTesting(runtime),
		Handler: handler,
		Config: StartupCleanupReconcilerConfig{
			WorkspaceID: string(workspace.DefaultID),
			Clock:       func() time.Time { return now },
		},
	}
	due, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	want := []string{"sandbox_cleanup_pending", "sandbox_cleanup_due", "sandbox_cleanup_expired_lease", "sandbox_cleanup_fractional_expired_lease"}
	if !reflect.DeepEqual(due, want) {
		t.Fatalf("due sandboxes = %v; want %v", due, want)
	}
	if !reflect.DeepEqual(handler.calls, []string{"default/sandbox_cleanup_pending", "default/sandbox_cleanup_due", "default/sandbox_cleanup_expired_lease", "default/sandbox_cleanup_fractional_expired_lease"}) {
		t.Fatalf("handler calls = %v; want due cleanup rows only", handler.calls)
	}
}

type recordingStartupCleanupHandler struct {
	calls []string
}

func (h *recordingStartupCleanupHandler) ReconcileStartupCleanup(_ context.Context, ws workspace.ID, sandboxID string) (*sandbox.Sandbox, error) {
	h.calls = append(h.calls, string(ws)+"/"+sandboxID)
	return &sandbox.Sandbox{WorkspaceID: ws, ID: sandboxID, Status: sandbox.StatusArchived}, nil
}

func seedStartupCleanupSandbox(t *testing.T, db *sql.DB, sandboxID string, sessionID string, cleanupStatus string, nextAttemptAt string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_sandbox_id,
			provider_metadata_json, created_at, updated_at, failed_at,
			startup_failure_reason, cleanup_status, cleanup_next_attempt_at
		) VALUES (
			'default', $1, $2, 'failed', 'tetral', $3, '{}',
			'2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', '2026-01-01T00:01:00Z',
			'startup_failed', $4, NULLIF($5, '')::timestamptz
		)`,
		sandboxID,
		sessionID,
		"provider_"+sandboxID,
		cleanupStatus,
		nextAttemptAt,
	); err != nil {
		t.Fatalf("seed startup cleanup sandbox: %v", err)
	}
}
