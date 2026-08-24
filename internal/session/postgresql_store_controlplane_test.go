package session_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/session"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func newControlPlaneSessionStoreTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func newControlPlaneSessionStore(t *testing.T, runtime *sql.DB) *session.PostgreSQLSessionStore {
	t.Helper()
	return session.NewPostgreSQLSessionStore(
		dbconnect.NewClientForTesting(runtime),
		session.WithPageTokenSecret([]byte("0123456789abcdef0123456789abcdef")),
		session.WithSessionDeleteSandboxRelease(func(ctx context.Context, tx *dbconnect.Tx, workspaceID workspace.ID, sessionID string, now time.Time) error {
			_, _, err := tetralsandbox.EnsureSandboxReleaseTx(ctx, tx, string(workspaceID), sessionID, tetralsandbox.SandboxReleaseSessionDelete, "", now)
			return err
		}),
	)
}

func TestPostgreSQLSessionRuntimeMutationUsesOnePoolConnection(t *testing.T) {
	runtime, _ := newControlPlaneSessionStoreTestDB(t)
	runtime.SetMaxOpenConns(1)
	runtime.SetMaxIdleConns(1)
	store := newControlPlaneSessionStore(t, runtime)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := store.WithRuntimeMutationTx(ctx, workspace.DefaultID, "sesn_single_connection", func(tx session.Transaction) error {
		var one int
		return tx.QueryRowScanner(ctx, "SELECT 1").Scan(&one)
	})
	if err != nil {
		t.Fatalf("Runtime mutation with one pooled connection: %v", err)
	}
}

func TestPostgreSQLSessionStoreDeleteRecordsSandboxReleaseBeforeCommit(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_delete_release", 1, "env_delete_release")
	now := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	sessionID := "sesn_delete_release"
	threadID := "thr_delete_release"
	createStoreSessionWithPrimaryThread(t, store, sessionID, threadID, "agent_delete_release", "env_delete_release", now)

	if _, err := admin.ExecContext(ctx, `INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id,
		environment_generation, provider, provider_resource_id, binding_revision,
		created_at, updated_at
	) VALUES ($1,$2,'sbox_delete_release','env_delete_release',1,'daytona','provider_delete_release',1,$3,$3)`,
		string(workspace.DefaultID), sessionID, now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_background_tasks (
		workspace_id, session_id, session_thread_id, task_id, source_tool_use_event_id,
		sandbox_id, provider, binding_revision, provider_session_id, provider_command_id,
		provider_command_metadata_json, resource_roots_json, status, reconcile_generation,
		next_poll_at, created_at, updated_at
	) VALUES ($1,$2,$3,'task_delete_release','evt_delete_release','sbox_delete_release',
		'daytona',1,'provider_delete_release','provider_command','{}','[]','running',1,$4,$4,$4)`,
		string(workspace.DefaultID), sessionID, threadID, now); err != nil {
		t.Fatalf("seed background task: %v", err)
	}

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	checks := []struct {
		name  string
		query string
		want  int
	}{
		{
			name: "release fence",
			query: `SELECT count(*) FROM session_sandbox_bindings
				WHERE workspace_id = 'default' AND session_id = 'sesn_delete_release'
				  AND release_requested_at IS NOT NULL AND release_reason = 'session_delete'`,
			want: 1,
		},
		{
			name: "release operation",
			query: `SELECT count(*) FROM sandbox_lifecycle_operations
				WHERE workspace_id = 'default' AND session_id = 'sesn_delete_release'
				  AND kind = 'release' AND state = 'pending' AND release_reason = 'session_delete'`,
			want: 1,
		},
		{
			name: "release queue job",
			query: `SELECT count(*) FROM queue_jobs
				WHERE workspace_id = 'default' AND kind = 'sandbox_release' AND status = 'pending'`,
			want: 1,
		},
		{
			name: "background cancel receipt",
			query: `SELECT count(*) FROM session_runtime_tool_results
				WHERE workspace_id = 'default' AND session_id = 'sesn_delete_release'
				  AND tool_kind = 'sandbox_background' AND background_operation_kind = 'cancel'
				  AND background_operation_state = 'pending'`,
			want: 1,
		},
		{
			name: "background cancel queue job",
			query: `SELECT count(*) FROM queue_jobs
				WHERE workspace_id = 'default' AND kind = 'sandbox_background_command' AND status = 'pending'`,
			want: 1,
		},
	}
	for _, check := range checks {
		if got := sessionStoreRowCount(t, admin, check.query); got != check.want {
			t.Fatalf("%s count = %d; want %d", check.name, got, check.want)
		}
	}
}

func TestPostgreSQLSessionStoreArchiveAndDeleteRejectRunningSessions(t *testing.T) {
	for _, action := range []string{"archive", "delete"} {
		t.Run(action, func(t *testing.T) {
			runtime, admin := newControlPlaneSessionStoreTestDB(t)
			ctx := context.Background()
			store := newControlPlaneSessionStore(t, runtime)
			seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_running_close", 1, "env_running_close")
			now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
			sessionID := "sesn_running_close_" + action
			if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				sess := minimalStoreSession(sessionID, "agent_running_close", 1, "env_running_close", now)
				sess.Status = session.StatusRunning
				return tx.CreateSession(ctx, sess)
			}); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}

			err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				if action == "archive" {
					_, err := tx.ArchiveSession(ctx, sessionID, now.Add(time.Minute))
					return err
				}
				return tx.DeleteSession(ctx, sessionID)
			})
			var conflict *session.ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("%s running session err = %T %v; want ConflictError", action, err, err)
			}
			if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1`, sessionID); got != 1 {
				t.Fatalf("session count after rejected %s = %d; want 1", action, got)
			}
		})
	}
}

func TestPostgreSQLSessionStoreProjectsPublicStatusFromRuntimeStatus(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_status_projection", 1, "env_status_projection")
	now := time.Date(2026, 6, 9, 12, 10, 0, 0, time.UTC)
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		if err := tx.CreateSession(ctx, minimalStoreSession("sesn_status_projection_running", "agent_status_projection", 1, "env_status_projection", now)); err != nil {
			return err
		}
		terminated := minimalStoreSession("sesn_status_projection_terminated", "agent_status_projection", 1, "env_status_projection", now.Add(time.Second))
		terminated.Status = session.StatusTerminated
		return tx.CreateSession(ctx, terminated)
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	setSessionRuntimeStatus(t, admin, workspace.DefaultID, "sesn_status_projection_running", "running")
	setSessionRuntimeStatus(t, admin, workspace.DefaultID, "sesn_status_projection_terminated", "running")

	running, err := store.Get(ctx, workspace.DefaultID, "sesn_status_projection_running")
	if err != nil {
		t.Fatalf("Get running projection: %v", err)
	}
	if running.Status != session.StatusRunning {
		t.Fatalf("retrieved status = %q; want runtime-projected running", running.Status)
	}
	terminated, err := store.Get(ctx, workspace.DefaultID, "sesn_status_projection_terminated")
	if err != nil {
		t.Fatalf("Get terminated projection: %v", err)
	}
	if terminated.Status != session.StatusTerminated {
		t.Fatalf("terminated status = %q; want durable terminated to win over runtime status", terminated.Status)
	}
	listed, err := store.List(ctx, workspace.DefaultID, session.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List sessions: %v", err)
	}
	statusByID := map[string]session.Status{}
	for _, sess := range listed.Data {
		statusByID[sess.ID] = sess.Status
	}
	if statusByID["sesn_status_projection_running"] != session.StatusRunning {
		t.Fatalf("listed running status = %q; want runtime-projected running", statusByID["sesn_status_projection_running"])
	}
	if statusByID["sesn_status_projection_terminated"] != session.StatusTerminated {
		t.Fatalf("listed terminated status = %q; want durable terminated", statusByID["sesn_status_projection_terminated"])
	}
}

func TestPostgreSQLSessionStoreUpdateRejectsProjectedRunningRuntimeStatus(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_update_running", 1, "env_update_running")
	now := time.Date(2026, 6, 9, 12, 15, 0, 0, time.UTC)
	sessionID := "sesn_update_projected_running"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_update_running", 1, "env_update_running", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	setSessionRuntimeStatus(t, admin, workspace.DefaultID, sessionID, "running")

	title := "renamed while running"
	metadataValue := "infra"
	approvalMode := session.ApprovalModeFullAccess
	for _, test := range []struct {
		name   string
		update session.UpdateSession
	}{
		{name: "title", update: session.UpdateSession{Title: &title, UpdatedAt: now.Add(time.Minute)}},
		{name: "metadata", update: session.UpdateSession{MetadataPatch: map[string]*string{"team": &metadataValue}, UpdatedAt: now.Add(time.Minute)}},
		{name: "runtime_config", update: session.UpdateSession{ApprovalMode: &approvalMode, UpdatedAt: now.Add(time.Minute)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				_, err := tx.UpdateSession(ctx, sessionID, test.update)
				return err
			})
			var conflict *session.ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("UpdateSession err = %T %v; want ConflictError", err, err)
			}
		})
	}
}

func TestPostgreSQLSessionStoreProviderCredentialForAdmissionProjectsCredentialLifecycle(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	now := time.Date(2026, 6, 9, 12, 25, 0, 0, time.UTC).Format(time.RFC3339Nano)
	vaultID := "vlt_provider_admission_lifecycle"
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
		 VALUES ($1, $2, 'provider admission lifecycle', $3, $3)`,
		string(workspace.DefaultID), vaultID, now,
	); err != nil {
		t.Fatalf("seed vault: %v", err)
	}
	for _, credential := range []struct {
		id         string
		archivedAt string
		revokedAt  string
	}{
		{id: "cred_provider_admission_live"},
		{id: "cred_provider_admission_archived", archivedAt: "2026-06-09T12:26:00Z"},
		{id: "cred_provider_admission_revoked", revokedAt: "2026-06-09T12:27:00Z"},
	} {
		if _, err := admin.ExecContext(ctx,
			`INSERT INTO credentials (
				workspace_id, id, vault_id, display_name, auth_type, auth_public_json,
				provider_id, access_mode, archived_at, revoked_at, created_at, updated_at
			) VALUES (
				$1, $2, $3, $2, 'provider_api_key', '{}',
				'anthropic', 'user_api_key', NULLIF($4, '')::timestamptz, NULLIF($5, '')::timestamptz, $6, $6
			)`,
			string(workspace.DefaultID), credential.id, vaultID, credential.archivedAt, credential.revokedAt, now,
		); err != nil {
			t.Fatalf("seed credential %s: %v", credential.id, err)
		}
	}

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		live, err := tx.GetProviderCredentialForAdmission(ctx, "cred_provider_admission_live", []string{vaultID})
		if err != nil {
			return err
		}
		if live.Archived || live.Revoked {
			t.Fatalf("live credential lifecycle = archived:%v revoked:%v; want both false", live.Archived, live.Revoked)
		}
		archived, err := tx.GetProviderCredentialForAdmission(ctx, "cred_provider_admission_archived", []string{vaultID})
		if err != nil {
			return err
		}
		if !archived.Archived || archived.Revoked {
			t.Fatalf("archived credential lifecycle = archived:%v revoked:%v; want archived only", archived.Archived, archived.Revoked)
		}
		revoked, err := tx.GetProviderCredentialForAdmission(ctx, "cred_provider_admission_revoked", []string{vaultID})
		if err != nil {
			return err
		}
		if revoked.Archived || !revoked.Revoked {
			t.Fatalf("revoked credential lifecycle = archived:%v revoked:%v; want revoked only", revoked.Archived, revoked.Revoked)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithWorkspaceTx: %v", err)
	}
}

func TestPostgreSQLSessionStoreProviderCredentialForAdmissionIsVaultScoped(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	now := time.Date(2026, 6, 9, 12, 35, 0, 0, time.UTC).Format(time.RFC3339Nano)
	for _, vaultID := range []string{"vlt_provider_scope_a", "vlt_provider_scope_b"} {
		if _, err := admin.ExecContext(ctx,
			`INSERT INTO vaults (workspace_id, id, display_name, created_at, updated_at)
			 VALUES ($1, $2, $2, $3, $3)`,
			string(workspace.DefaultID), vaultID, now,
		); err != nil {
			t.Fatalf("seed vault %s: %v", vaultID, err)
		}
		if _, err := admin.ExecContext(ctx,
			`INSERT INTO credentials (
				workspace_id, id, vault_id, display_name, auth_type, auth_public_json,
				provider_id, access_mode, created_at, updated_at
			) VALUES (
				$1, 'cred_provider_scope_shared', $2, $2, 'provider_api_key', '{}',
				'anthropic', 'user_api_key', $3, $3
			)`,
			string(workspace.DefaultID), vaultID, now,
		); err != nil {
			t.Fatalf("seed duplicate credential in %s: %v", vaultID, err)
		}
	}

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		credential, err := tx.GetProviderCredentialForAdmission(ctx, "cred_provider_scope_shared", []string{"vlt_provider_scope_b"})
		if err != nil {
			return err
		}
		if credential.VaultID != "vlt_provider_scope_b" {
			t.Fatalf("credential vault = %q; want vlt_provider_scope_b", credential.VaultID)
		}
		_, err = tx.GetProviderCredentialForAdmission(ctx, "cred_provider_scope_shared", []string{"vlt_provider_scope_a", "vlt_provider_scope_b"})
		var permissionErr *session.PermissionError
		if !errors.As(err, &permissionErr) {
			t.Fatalf("ambiguous credential error = %T %v; want PermissionError", err, err)
		}
		return nil
	}); err != nil {
		t.Fatalf("WithWorkspaceTx: %v", err)
	}
}

func TestPostgreSQLSessionStoreProjectsSessionUsageJSON(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_usage_projection", 1, "env_usage_projection")
	now := time.Date(2026, 6, 9, 12, 35, 0, 0, time.UTC)
	sessionID := "sesn_usage_projection"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_usage_projection", 1, "env_usage_projection", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	usageJSON := `{"request_count":4,"input_tokens":11,"output_tokens":7,"cache_read_input_tokens":3,"cache_creation":{"ephemeral_1h_input_tokens":2,"ephemeral_5m_input_tokens":0},"web_search_requests":6,"web_fetch_requests":5,"reasoning_output_tokens":99,"provider_usage_json":{"opaque":true}}`
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions
		    SET usage_json = $3
		  WHERE workspace_id = $1
		    AND id = $2`,
		string(workspace.DefaultID), sessionID, usageJSON,
	); err != nil {
		t.Fatalf("seed usage_json: %v", err)
	}

	got, err := store.Get(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertSessionUsageProjection(t, got.Usage)
	listed, err := store.List(ctx, workspace.DefaultID, session.ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, sess := range listed.Data {
		if sess.ID == sessionID {
			assertSessionUsageProjection(t, sess.Usage)
			return
		}
	}
	t.Fatalf("List did not return %s", sessionID)
}

func assertSessionUsageProjection(t *testing.T, usage session.Usage) {
	t.Helper()
	if usage.InputTokens != 11 || usage.OutputTokens != 7 || usage.CacheReadInputTokens != 3 {
		t.Fatalf("usage = %+v; want public token counts", usage)
	}
	if usage.CacheCreation.Ephemeral1hInputTokens == nil || *usage.CacheCreation.Ephemeral1hInputTokens != 2 {
		t.Fatalf("usage cache_creation 1h = %v; want 2", usage.CacheCreation.Ephemeral1hInputTokens)
	}
	if usage.CacheCreation.Ephemeral5mInputTokens == nil || *usage.CacheCreation.Ephemeral5mInputTokens != 0 {
		t.Fatalf("usage cache_creation 5m = %v; want 0", usage.CacheCreation.Ephemeral5mInputTokens)
	}
	if usage.ServerToolUse.WebSearchRequests != 6 || usage.ServerToolUse.WebFetchRequests != 5 {
		t.Fatalf("usage server_tool_use = %+v; want projected web counts", usage.ServerToolUse)
	}
}

func TestPostgreSQLSessionStoreArchiveAndDeleteRejectRuntimeRunningSessions(t *testing.T) {
	for _, action := range []string{"archive", "delete"} {
		t.Run(action, func(t *testing.T) {
			runtime, admin := newControlPlaneSessionStoreTestDB(t)
			ctx := context.Background()
			store := newControlPlaneSessionStore(t, runtime)
			seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_runtime_running_close", 1, "env_runtime_running_close")
			now := time.Date(2026, 6, 9, 12, 20, 0, 0, time.UTC)
			sessionID := "sesn_runtime_running_close_" + action
			if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_runtime_running_close", 1, "env_runtime_running_close", now))
			}); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			setSessionRuntimeStatus(t, admin, workspace.DefaultID, sessionID, "running")

			err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				if action == "archive" {
					_, err := tx.ArchiveSession(ctx, sessionID, now.Add(time.Minute))
					return err
				}
				return tx.DeleteSession(ctx, sessionID)
			})
			var conflict *session.ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("%s runtime-running session err = %T %v; want ConflictError", action, err, err)
			}
			if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND lifecycle_state = 'active'`, sessionID); got != 1 {
				t.Fatalf("active session count after rejected %s = %d; want 1", action, got)
			}
			if action == "delete" {
				if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND delete_cleanup_id IS NOT NULL`, sessionID); got != 0 {
					t.Fatalf("delete cleanup identities after rejected delete = %d; want 0", got)
				}
				if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND type = 'session.deleted'`, sessionID); got != 0 {
					t.Fatalf("session.deleted events after rejected delete = %d; want 0", got)
				}
				if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'session_delete_cleanup'`, string(workspace.DefaultID)); got != 0 {
					t.Fatalf("delete cleanup jobs after rejected delete = %d; want 0", got)
				}
				if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_resource_prefix_gc WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), sessionID); got != 0 {
					t.Fatalf("prefix GC markers after rejected delete = %d; want 0", got)
				}
			}
		})
	}
}

func TestPostgreSQLSessionStoreArchiveAndDeleteIgnoreQueuedEvents(t *testing.T) {
	for _, action := range []string{"archive", "delete"} {
		t.Run(action, func(t *testing.T) {
			runtime, admin := newControlPlaneSessionStoreTestDB(t)
			ctx := context.Background()
			store := newControlPlaneSessionStore(t, runtime)
			seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_queued_close", 1, "env_queued_close")
			now := time.Date(2026, 6, 9, 12, 30, 0, 0, time.UTC)
			sessionID := "sesn_queued_close_" + action
			if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_queued_close", 1, "env_queued_close", now))
			}); err != nil {
				t.Fatalf("CreateSession: %v", err)
			}
			seedQueuedSessionEvent(t, admin, workspace.DefaultID, sessionID, "sevt_queued_close_"+action)

			err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
				if action == "archive" {
					_, err := tx.ArchiveSession(ctx, sessionID, now.Add(time.Minute))
					return err
				}
				return tx.DeleteSession(ctx, sessionID)
			})
			if err != nil {
				t.Fatalf("%s idle session with queued event: %v", action, err)
			}
			if action == "archive" {
				assertSessionArchivedAt(t, admin, sessionID, now.Add(time.Minute))
				if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND processed_at IS NULL`, sessionID); got != 1 {
					t.Fatalf("queued event count after archive = %d; want 1", got)
				}
				return
			}
			if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND lifecycle_state = 'deleted'`, sessionID); got != 1 {
				t.Fatalf("deleted session tombstone count = %d; want 1", got)
			}
			if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND processed_at IS NULL`, sessionID); got != 1 {
				t.Fatalf("queued event count after delete tombstone = %d; want 1", got)
			}
		})
	}
}

func TestPostgreSQLSessionStoreDeleteRejectsReschedulingSession(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_rescheduling_delete", 1, "env_rescheduling_delete")
	now := time.Date(2026, 6, 9, 12, 35, 0, 0, time.UTC)
	sessionID := "sesn_rescheduling_delete"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_rescheduling_delete", 1, "env_rescheduling_delete", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions
		    SET status = 'rescheduling'
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark rescheduling: %v", err)
	}

	err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	})
	var conflict *session.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("DeleteSession rescheduling err = %T %v; want ConflictError", err, err)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND lifecycle_state = 'active'`, sessionID); got != 1 {
		t.Fatalf("active session count after rejected delete = %d; want 1", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND type = 'session.deleted'`, sessionID); got != 0 {
		t.Fatalf("deleted event count after rejected delete = %d; want 0", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND delete_cleanup_id IS NOT NULL`, sessionID); got != 0 {
		t.Fatalf("delete cleanup identities after rejected delete = %d; want 0", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'session_delete_cleanup'`, string(workspace.DefaultID)); got != 0 {
		t.Fatalf("delete cleanup jobs after rejected delete = %d; want 0", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_resource_prefix_gc WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), sessionID); got != 0 {
		t.Fatalf("prefix GC markers after rejected delete = %d; want 0", got)
	}
}

func TestPostgreSQLSessionStoreDeleteRejectsCommittedRuntimeDeliveryLease(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_leased_delete", 1, "env_leased_delete")
	now := time.Date(2026, 8, 24, 20, 20, 0, 0, time.UTC)
	const (
		sessionID = "sesn_leased_delete"
		threadID  = "thrd_leased_delete"
		inputID   = "rin_leased_delete"
	)
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		if err := tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_leased_delete", 1, "env_leased_delete", now)); err != nil {
			return err
		}
		return tx.CreatePrimaryThread(ctx, &session.Thread{
			ID: threadID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			CreatedAt: now, UpdatedAt: now,
		})
	}); err != nil {
		t.Fatalf("create Session and Thread: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{"evt_leased_delete"},
		"sequence_from": 1, "sequence_to": 1, "input_kind": "messages",
	})
	if err != nil {
		t.Fatalf("marshal Runtime input payload: %v", err)
	}
	job, err := queueStore.Enqueue(ctx, queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey: queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:    queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadJSON:  payload, Now: now,
	})
	if err != nil {
		t.Fatalf("enqueue Runtime input: %v", err)
	}
	leased, err := queueStore.Lease(ctx, queue.LeaseRequest{
		WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "bridge-delete-race",
		MaxJobs: 1, LeaseDuration: time.Minute, Now: now.Add(time.Second),
	})
	if err != nil || len(leased) != 1 || leased[0].ID != job.ID {
		t.Fatalf("lease Runtime input = %+v/%v; want %s", leased, err, job.ID)
	}

	err = store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	})
	var conflict *session.ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("DeleteSession with live delivery lease err = %T %v; want ConflictError", err, err)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id=$1 AND lifecycle_state='active' AND delete_cleanup_id IS NULL`, sessionID); got != 1 {
		t.Fatalf("active Session after rejected delete = %d; want 1", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id=$1 AND type='session.deleted'`, sessionID); got != 0 {
		t.Fatalf("deleted Events after rejected delete = %d; want 0", got)
	}
}

func TestPostgreSQLSessionStoreDeleteWritesPublicDeletedEvent(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_delete_event", 1, "env_delete_event")
	now := time.Date(2026, 6, 9, 12, 40, 0, 0, time.UTC)
	sessionID := "sesn_delete_event"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_delete_event", 1, "env_delete_event", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	seedQueuedSessionEvent(t, admin, workspace.DefaultID, sessionID, "sevt_delete_event_queued")

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	var eventID string
	var sequence int64
	var payload string
	var processedAt sql.NullString
	var latestStreamPosition int64
	if err := admin.QueryRowContext(ctx,
		`SELECT event_id, sequence, payload_json, processed_at, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND type = 'session.deleted'`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&eventID, &sequence, &payload, &processedAt, &latestStreamPosition); err != nil {
		t.Fatalf("load session.deleted event: %v", err)
	}
	if !strings.HasPrefix(eventID, "sevt_") {
		t.Fatalf("session.deleted event_id = %q; want sevt_ prefix", eventID)
	}
	if sequence != 2 {
		t.Fatalf("session.deleted sequence = %d; want after queued user event sequence 2", sequence)
	}
	if !processedAt.Valid {
		t.Fatal("session.deleted processed_at is NULL; want non-runtime-input event marked processed")
	}
	if latestStreamPosition <= 0 {
		t.Fatalf("latest_stream_position = %d; want stream change", latestStreamPosition)
	}
	if gotType := jsonFieldString(t, payload, "type"); gotType != "session.deleted" {
		t.Fatalf("payload type = %q; want session.deleted", gotType)
	}
	if gotID := jsonFieldString(t, payload, "id"); gotID != sessionID {
		t.Fatalf("payload id = %q; want %q", gotID, sessionID)
	}
	if got := sessionStoreRowCount(t, admin,
		`SELECT count(*)
		   FROM session_event_stream_changes
		  WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3 AND stream_position = $4 AND visibility = 'public' AND session_visible`,
		string(workspace.DefaultID),
		sessionID,
		eventID,
		latestStreamPosition,
	); got != 1 {
		t.Fatalf("session.deleted stream changes = %d; want 1", got)
	}
}

func TestPostgreSQLSessionStoreCreatePrimaryThreadWritesPublicCreatedEvent(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_thread_created_event", 1, "env_thread_created_event")
	now := time.Date(2026, 6, 9, 12, 45, 0, 0, time.UTC)
	sessionID := "sesn_thread_created_event"
	threadID := "thread_primary_created_event"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		if err := tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_thread_created_event", 1, "env_thread_created_event", now)); err != nil {
			return err
		}
		return tx.CreatePrimaryThread(ctx, &session.Thread{
			ID:          threadID,
			WorkspaceID: workspace.DefaultID,
			SessionID:   sessionID,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}); err != nil {
		t.Fatalf("CreatePrimaryThread: %v", err)
	}

	var eventID string
	var sequence int64
	var eventThreadID string
	var payload string
	var processedAt sql.NullString
	var latestStreamPosition int64
	if err := admin.QueryRowContext(ctx,
		`SELECT event_id, sequence, session_thread_id, payload_json, processed_at, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND type = 'session.thread_created'`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&eventID, &sequence, &eventThreadID, &payload, &processedAt, &latestStreamPosition); err != nil {
		t.Fatalf("load session.thread_created event: %v", err)
	}
	if !strings.HasPrefix(eventID, "sevt_") {
		t.Fatalf("session.thread_created event_id = %q; want sevt_ prefix", eventID)
	}
	if sequence != 1 {
		t.Fatalf("session.thread_created sequence = %d; want 1", sequence)
	}
	if eventThreadID != threadID {
		t.Fatalf("session.thread_created thread id = %q; want %q", eventThreadID, threadID)
	}
	if !processedAt.Valid {
		t.Fatal("session.thread_created processed_at is NULL; want non-runtime-input event marked processed")
	}
	if latestStreamPosition <= 0 {
		t.Fatalf("latest_stream_position = %d; want stream change", latestStreamPosition)
	}
	if gotType := jsonFieldString(t, payload, "type"); gotType != "session.thread_created" {
		t.Fatalf("payload type = %q; want session.thread_created", gotType)
	}
	if gotThreadID := jsonFieldString(t, payload, "session_thread_id"); gotThreadID != threadID {
		t.Fatalf("payload session_thread_id = %q; want %q", gotThreadID, threadID)
	}
	if gotVisibility := jsonFieldString(t, payload, "visibility"); gotVisibility != string(session.ThreadVisibilityPublic) {
		t.Fatalf("payload visibility = %q; want public", gotVisibility)
	}
	var changeThreadID string
	if err := admin.QueryRowContext(ctx,
		`SELECT session_thread_id
		   FROM session_event_stream_changes
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3
		    AND stream_position = $4
		    AND visibility = 'public'
		    AND session_visible`,
		string(workspace.DefaultID),
		sessionID,
		eventID,
		latestStreamPosition,
	).Scan(&changeThreadID); err != nil {
		t.Fatalf("load session.thread_created stream change: %v", err)
	}
	if changeThreadID != threadID {
		t.Fatalf("session.thread_created stream change thread id = %q; want %q", changeThreadID, threadID)
	}
}

func TestPostgreSQLSessionStoreUpdateWritesPublicUpdatedEvent(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_update_event", 1, "env_update_event")
	now := time.Date(2026, 6, 9, 12, 50, 0, 0, time.UTC)
	sessionID := "sesn_update_event"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_update_event", 1, "env_update_event", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	seedQueuedSessionEvent(t, admin, workspace.DefaultID, sessionID, "sevt_update_event_queued")
	title := "renamed"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		_, err := tx.UpdateSession(ctx, sessionID, session.UpdateSession{
			Title:     &title,
			UpdatedAt: now.Add(time.Minute),
		})
		return err
	}); err != nil {
		t.Fatalf("UpdateSession: %v", err)
	}

	var eventID string
	var sequence int64
	var payload string
	var processedAt sql.NullString
	var latestStreamPosition int64
	if err := admin.QueryRowContext(ctx,
		`SELECT event_id, sequence, payload_json, processed_at, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND type = 'session.updated'`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&eventID, &sequence, &payload, &processedAt, &latestStreamPosition); err != nil {
		t.Fatalf("load session.updated event: %v", err)
	}
	if !strings.HasPrefix(eventID, "sevt_") {
		t.Fatalf("session.updated event_id = %q; want sevt_ prefix", eventID)
	}
	if sequence != 2 {
		t.Fatalf("session.updated sequence = %d; want after queued user event sequence 2", sequence)
	}
	if !processedAt.Valid {
		t.Fatal("session.updated processed_at is NULL; want non-runtime-input event marked processed")
	}
	if latestStreamPosition <= 0 {
		t.Fatalf("latest_stream_position = %d; want stream change", latestStreamPosition)
	}
	if gotType := jsonFieldString(t, payload, "type"); gotType != "session.updated" {
		t.Fatalf("payload type = %q; want session.updated", gotType)
	}
	if gotID := jsonFieldString(t, payload, "id"); gotID != sessionID {
		t.Fatalf("payload id = %q; want %q", gotID, sessionID)
	}
	if got := sessionStoreRowCount(t, admin,
		`SELECT count(*)
		   FROM session_event_stream_changes
		  WHERE workspace_id = $1 AND session_id = $2 AND event_id = $3 AND stream_position = $4 AND visibility = 'public' AND session_visible`,
		string(workspace.DefaultID),
		sessionID,
		eventID,
		latestStreamPosition,
	); got != 1 {
		t.Fatalf("session.updated stream changes = %d; want 1", got)
	}
}

func TestPostgreSQLSessionStoreArchivePersistsArchivingThenArchivedLifecycle(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_archive", 1, "env_archive")
	installSessionLifecycleAudit(t, admin)
	now := time.Date(2026, 6, 9, 13, 0, 0, 0, time.UTC)
	sessionID := "sesn_archive"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_archive", 1, "env_archive", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	archivedAt := now.Add(time.Minute)
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		_, err := tx.ArchiveSession(ctx, sessionID, archivedAt)
		return err
	}); err != nil {
		t.Fatalf("ArchiveSession: %v", err)
	}
	assertSessionArchivedAt(t, admin, sessionID, archivedAt)
	assertSessionLifecycleTransitions(t, admin, sessionID, []string{"active->archiving", "archiving->archived"})
}

func TestPostgreSQLSessionStoreDeleteRemovesIdleSession(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_delete", 1, "env_delete")
	now := time.Date(2026, 6, 9, 14, 0, 0, 0, time.UTC)
	sessionID := "sesn_delete"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_delete", 1, "env_delete", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND lifecycle_state = 'deleted'`, sessionID); got != 1 {
		t.Fatalf("deleted session tombstone count = %d; want 1", got)
	}
	var deleteCleanupID string
	if err := admin.QueryRowContext(ctx,
		`SELECT delete_cleanup_id
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&deleteCleanupID); err != nil {
		t.Fatalf("read delete cleanup identity: %v", err)
	}
	if deleteCleanupID == "" {
		t.Fatal("delete_cleanup_id is empty")
	}
	var jobKind string
	var dedupeKey string
	var payloadJSON string
	if err := admin.QueryRowContext(ctx,
		`SELECT kind, dedupe_key, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = 'session_delete_cleanup'
		    AND status IN ('pending', 'leased')`,
		string(workspace.DefaultID),
	).Scan(&jobKind, &dedupeKey, &payloadJSON); err != nil {
		t.Fatalf("read delete cleanup job: %v", err)
	}
	wantDedupeKey := "session_delete_cleanup:" + string(workspace.DefaultID) + ":" + sessionID + ":" + deleteCleanupID
	if jobKind != "session_delete_cleanup" || dedupeKey != wantDedupeKey {
		t.Fatalf("delete cleanup job = %q/%q; want session_delete_cleanup/%q", jobKind, dedupeKey, wantDedupeKey)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode delete cleanup payload: %v", err)
	}
	if payload["workspace_id"] != string(workspace.DefaultID) || payload["session_id"] != sessionID || payload["delete_cleanup_id"] != deleteCleanupID {
		t.Fatalf("delete cleanup payload = %+v; want durable workspace/session/delete identity", payload)
	}
	var gcPrefix string
	var gcStatus string
	if err := admin.QueryRowContext(ctx,
		`SELECT prefix, status
		   FROM session_resource_prefix_gc
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&gcPrefix, &gcStatus); err != nil {
		t.Fatalf("read resource prefix gc marker: %v", err)
	}
	wantPrefix := "workspaces/" + string(workspace.DefaultID) + "/sessions/" + sessionID + "/"
	if gcPrefix != wantPrefix || gcStatus != "pending" {
		t.Fatalf("resource prefix gc marker = %q/%q; want %q/pending", gcPrefix, gcStatus, wantPrefix)
	}

	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.DeleteSession(ctx, sessionID)
	}); err != nil {
		t.Fatalf("duplicate DeleteSession: %v", err)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND type = 'session.deleted'`, sessionID); got != 1 {
		t.Fatalf("session.deleted events after duplicate delete = %d; want 1", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'session_delete_cleanup'`, string(workspace.DefaultID)); got != 1 {
		t.Fatalf("delete cleanup jobs after duplicate delete = %d; want 1", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_resource_prefix_gc WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), sessionID); got != 1 {
		t.Fatalf("prefix GC markers after duplicate delete = %d; want 1", got)
	}
	var replayedDeleteCleanupID string
	if err := admin.QueryRowContext(ctx,
		`SELECT delete_cleanup_id FROM sessions WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), sessionID,
	).Scan(&replayedDeleteCleanupID); err != nil {
		t.Fatalf("read replayed delete cleanup identity: %v", err)
	}
	if replayedDeleteCleanupID != deleteCleanupID {
		t.Fatalf("delete_cleanup_id after duplicate = %q; want preserved %q", replayedDeleteCleanupID, deleteCleanupID)
	}
}

func TestPostgreSQLSessionStoreDeleteRollbackExposesNoPartialArtifacts(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_delete_rollback", 1, "env_delete_rollback")
	now := time.Date(2026, 6, 9, 14, 5, 0, 0, time.UTC)
	sessionID := "sesn_delete_rollback"
	if err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		return tx.CreateSession(ctx, minimalStoreSession(sessionID, "agent_delete_rollback", 1, "env_delete_rollback", now))
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	rollbackErr := errors.New("force delete rollback")
	err := store.WithWorkspaceTx(ctx, workspace.DefaultID, func(tx session.Transaction) error {
		if err := tx.DeleteSession(ctx, sessionID); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(err, rollbackErr) {
		t.Fatalf("DeleteSession rollback err = %v; want forced rollback", err)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM sessions WHERE id = $1 AND lifecycle_state = 'active' AND delete_cleanup_id IS NULL`, sessionID); got != 1 {
		t.Fatalf("active session without delete identity after rollback = %d; want 1", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_events WHERE session_id = $1 AND type = 'session.deleted'`, sessionID); got != 0 {
		t.Fatalf("session.deleted events after rollback = %d; want 0", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'session_delete_cleanup'`, string(workspace.DefaultID)); got != 0 {
		t.Fatalf("delete cleanup jobs after rollback = %d; want 0", got)
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_resource_prefix_gc WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), sessionID); got != 0 {
		t.Fatalf("prefix GC markers after rollback = %d; want 0", got)
	}
}

func TestPostgreSQLSessionStorePublicThreadsFilterInternalAndPaginate(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_threads", 1, "env_threads")
	now := time.Date(2026, 6, 9, 15, 0, 0, 0, time.UTC)
	createStoreSessionWithPrimaryThread(t, store, "sesn_threads", "thread_main", "agent_threads", "env_threads", now)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_threads", "thread_child", "thread_main", "subagent", "public", "closed_for_runtime", now.Add(time.Minute), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_threads", "thread_reviewer", "thread_main", "approval_reviewer", "internal", "idle", now.Add(2*time.Minute), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_threads", "thread_internal", "thread_main", "subagent", "internal", "idle", now.Add(3*time.Minute), nil)

	first, err := store.ListThreads(ctx, workspace.DefaultID, "sesn_threads", session.ThreadListOptions{Limit: 1})
	if err != nil {
		t.Fatalf("ListThreads first page: %v", err)
	}
	if len(first.Data) != 1 || first.Data[0].ID != "thread_main" || first.NextPage == nil {
		t.Fatalf("first page = %+v next=%v; want main plus next page", first.Data, first.NextPage)
	}
	second, err := store.ListThreads(ctx, workspace.DefaultID, "sesn_threads", session.ThreadListOptions{Limit: 10, Page: *first.NextPage})
	if err != nil {
		t.Fatalf("ListThreads second page: %v", err)
	}
	if len(second.Data) != 1 || second.Data[0].ID != "thread_child" {
		t.Fatalf("second page = %+v; want public child only", second.Data)
	}
	if second.Data[0].Status != session.ThreadStatusClosedForRuntime {
		t.Fatalf("stored child status = %s; want closed_for_runtime internal state", second.Data[0].Status)
	}
	if _, err := store.GetThread(ctx, workspace.DefaultID, "sesn_threads", "thread_reviewer"); err == nil {
		t.Fatal("GetThread returned internal reviewer; want not found")
	}
	if _, err := store.GetThread(ctx, workspace.DefaultID, "sesn_threads", "thread_internal"); err == nil {
		t.Fatal("GetThread returned internal subagent; want not found")
	}
}

func TestPostgreSQLSessionStorePublicThreadUsageIsScopedToThread(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_thread_usage", 1, "env_thread_usage")
	now := time.Date(2026, 6, 9, 15, 30, 0, 0, time.UTC)
	createStoreSessionWithPrimaryThread(t, store, "sesn_thread_usage", "thread_usage_main", "agent_thread_usage", "env_thread_usage", now)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_usage", "thread_usage_child", "thread_usage_main", "subagent", "public", "idle", now.Add(time.Minute), nil)
	seedThreadRequestUsage(t, admin, "sesn_thread_usage", "thread_usage_main", "mreq_usage_main", 11, 7, 3, 2, 1, 2)
	seedThreadRequestUsage(t, admin, "sesn_thread_usage", "thread_usage_child", "mreq_usage_child", 29, 13, 5, 4, 3, 4)

	listed, err := store.ListThreads(ctx, workspace.DefaultID, "sesn_thread_usage", session.ThreadListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	if len(listed.Data) != 2 {
		t.Fatalf("listed threads = %d; want 2", len(listed.Data))
	}
	assertThreadUsage(t, listed.Data[0].Usage, 11, 7, 3, 2)
	assertThreadUsage(t, listed.Data[1].Usage, 29, 13, 5, 4)

	child, err := store.GetThread(ctx, workspace.DefaultID, "sesn_thread_usage", "thread_usage_child")
	if err != nil {
		t.Fatalf("GetThread: %v", err)
	}
	assertThreadUsage(t, child.Usage, 29, 13, 5, 4)
	archived, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_usage", "thread_usage_child", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("ArchiveThread: %v", err)
	}
	assertThreadUsage(t, archived.Usage, 29, 13, 5, 4)
}

func seedThreadRequestUsage(t *testing.T, db *sql.DB, sessionID string, threadID string, modelRequestID string, inputTokens int64, outputTokens int64, cacheRead int64, cacheWrite int64, webSearch int64, webFetch int64) {
	t.Helper()
	runtimeWriteID := "rwrite_" + modelRequestID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO request_usage_details (
			workspace_id, session_id, session_thread_id, model_request_id, runtime_write_id,
			request_kind, input_total_tokens, input_uncached_tokens, input_cache_read_tokens,
			input_cache_write_tokens, output_total_tokens, created_at
		) VALUES ($1, $2, $3, $4, $5, 'agent_provider_request', $6, 0, $7, $8, $9, $10)`,
		string(workspace.DefaultID), sessionID, threadID, modelRequestID, runtimeWriteID,
		inputTokens, cacheRead, cacheWrite, outputTokens, "2026-06-09T15:30:00Z"); err != nil {
		t.Fatalf("seed thread request usage: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"model_usage": map[string]any{
			"web_search_requests": webSearch,
			"web_fetch_requests":  webFetch,
		},
	})
	if err != nil {
		t.Fatalf("marshal thread request-end payload: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 100, 'span.model_request_end', $5, 'public', FALSE, $6, $7, $8, $8)`,
		string(workspace.DefaultID), sessionID, threadID, "sevt_"+modelRequestID, string(payload), runtimeWriteID, modelRequestID, "2026-06-09T15:30:00Z"); err != nil {
		t.Fatalf("seed thread request-end event: %v", err)
	}
}

func assertThreadUsage(t *testing.T, got session.Usage, inputTokens int64, outputTokens int64, cacheRead int64, cacheWrite int64) {
	t.Helper()
	if got.InputTokens != inputTokens || got.OutputTokens != outputTokens || got.CacheReadInputTokens != cacheRead {
		t.Fatalf("thread usage = %+v; want input/output/cache-read %d/%d/%d", got, inputTokens, outputTokens, cacheRead)
	}
	if got.CacheCreation.Ephemeral1hInputTokens == nil || *got.CacheCreation.Ephemeral1hInputTokens != cacheWrite {
		t.Fatalf("thread cache creation = %+v; want %d", got.CacheCreation, cacheWrite)
	}
	if got.ServerToolUse.WebSearchRequests != 0 || got.ServerToolUse.WebFetchRequests != 0 {
		t.Fatalf("thread usage retained session-only server tool counters: %+v", got.ServerToolUse)
	}
}

func TestPostgreSQLSessionStoreArchiveThreadSafetyAndIdempotency(t *testing.T) {
	runtime, admin := newControlPlaneSessionStoreTestDB(t)
	ctx := context.Background()
	store := newControlPlaneSessionStore(t, runtime)
	seedSessionStoreReferences(t, admin, workspace.DefaultID, "agent_thread_archive", 1, "env_thread_archive")
	now := time.Date(2026, 6, 9, 16, 0, 0, 0, time.UTC)
	createStoreSessionWithPrimaryThread(t, store, "sesn_thread_archive", "thread_main_archive", "agent_thread_archive", "env_thread_archive", now)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_running", "thread_main_archive", "subagent", "public", "running", now.Add(time.Minute), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_input", "thread_main_archive", "subagent", "public", "idle", now.Add(2*time.Minute), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_queued", "thread_main_archive", "subagent", "public", "idle", now.Add(2500*time.Millisecond), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_wait", "thread_main_archive", "subagent", "public", "idle", now.Add(3*time.Minute), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_event", "thread_main_archive", "subagent", "public", "idle", now.Add(3500*time.Millisecond), nil)
	seedSessionThread(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_idle", "thread_main_archive", "subagent", "public", "idle", now.Add(4*time.Minute), nil)
	seedThreadRuntimeInbox(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_input", "rtin_thread_input")
	seedThreadRuntimeInputQueueJob(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_queued", "rtin_thread_queued", "pending")
	seedThreadPendingWait(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_wait", "sevt_wait")
	seedUnprocessedThreadEvent(t, admin, workspace.DefaultID, "sesn_thread_archive", "thread_event", "sevt_thread_event")

	for _, threadID := range []string{"thread_running", "thread_input", "thread_queued", "thread_wait", "thread_event"} {
		_, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_archive", threadID, now.Add(10*time.Minute))
		var conflict *session.ConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("ArchiveThread(%s) err = %T %v; want ConflictError", threadID, err, err)
		}
		if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_threads WHERE id = $1 AND archived_at IS NOT NULL`, threadID); got != 0 {
			t.Fatalf("%s archived despite conflict = %d", threadID, got)
		}
	}

	archivedAt := now.Add(20 * time.Minute)
	archived, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_archive", "thread_idle", archivedAt)
	if err != nil {
		t.Fatalf("ArchiveThread idle: %v", err)
	}
	if archived.ArchivedAt == nil || !archived.ArchivedAt.Equal(archivedAt) {
		t.Fatalf("archived_at = %v; want %s", archived.ArchivedAt, archivedAt)
	}
	again, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_archive", "thread_idle", archivedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("ArchiveThread replay: %v", err)
	}
	if again.ArchivedAt == nil || !again.ArchivedAt.Equal(archivedAt) {
		t.Fatalf("replay archived_at = %v; want unchanged %s", again.ArchivedAt, archivedAt)
	}

	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status
		    SET status = 'running'
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID), "sesn_thread_archive"); err != nil {
		t.Fatalf("mark session runtime running: %v", err)
	}
	if _, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_archive", "thread_main_archive", archivedAt); err == nil {
		t.Fatal("ArchiveThread running primary succeeded; want conflict")
	}
	if got := sessionStoreRowCount(t, admin, `SELECT count(*) FROM session_threads WHERE id = 'thread_main_archive' AND archived_at IS NOT NULL`); got != 0 {
		t.Fatalf("primary archived while runtime running = %d", got)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status
		    SET status = 'idle'
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID), "sesn_thread_archive"); err != nil {
		t.Fatalf("mark session runtime idle: %v", err)
	}
	primaryArchived, err := store.ArchiveThread(ctx, workspace.DefaultID, "sesn_thread_archive", "thread_main_archive", archivedAt)
	if err != nil {
		t.Fatalf("ArchiveThread idle primary: %v", err)
	}
	if primaryArchived.ArchivedAt == nil {
		t.Fatal("idle primary archived_at is nil")
	}
}

func seedUnprocessedThreadEvent(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, threadID string, eventID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type,
			payload_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, 1, 'user.message',
			'{"content":[{"type":"text","text":"queued"}]}', '2026-06-09T12:30:00Z', '2026-06-09T12:30:00Z', NULL)`,
		string(ws), sessionID, threadID, eventID,
	); err != nil {
		t.Fatalf("seed unprocessed thread event: %v", err)
	}
}

func seedSessionStoreReferences(t *testing.T, db *sql.DB, ws workspace.ID, agentID string, agentVersion int, environmentID string, extraIDs ...string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $2, '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(ws), "workspace-"+string(ws)); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(ws), agentID, "agent-"+agentID, agentVersion); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, $2, $3, $4, '{}', $5, '2026-01-01T00:00:00Z')
		 ON CONFLICT (workspace_id, agent_id, version) DO NOTHING`,
		string(ws), "agv_"+agentID, agentID, agentVersion, "hash-"+agentID); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO environments (workspace_id, id, name, config_json, created_at, updated_at)
		 VALUES ($1, $2, $3, '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (workspace_id, id) DO NOTHING`,
		string(ws), environmentID, "environment-"+environmentID); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if len(extraIDs) >= 3 {
		memoryStoreID := extraIDs[2]
		if _, err := db.ExecContext(ctx,
			`INSERT INTO memory_stores (workspace_id, memory_store_id, name, description, metadata_json, created_at, updated_at)
			 VALUES ($1, $2, $3, '', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
			 ON CONFLICT (workspace_id, memory_store_id) DO NOTHING`,
			string(ws), memoryStoreID, "memory-"+memoryStoreID); err != nil {
			t.Fatalf("seed memory store: %v", err)
		}
	}
}

func minimalStoreSession(id string, agentID string, agentVersion int, environmentID string, now time.Time) *session.Session {
	return &session.Session{
		ID:             id,
		Type:           "session",
		Status:         session.StatusIdle,
		LifecycleState: session.LifecycleStateActive,
		Metadata:       map[string]string{},
		AgentID:        agentID,
		AgentVersion:   agentVersion,
		EnvironmentID:  environmentID,
		VaultIDs:       []string{},
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func createStoreSessionWithPrimaryThread(t *testing.T, store *session.PostgreSQLSessionStore, sessionID string, threadID string, agentID string, environmentID string, now time.Time) {
	t.Helper()
	if err := store.WithWorkspaceTx(context.Background(), workspace.DefaultID, func(tx session.Transaction) error {
		if err := tx.CreateSession(context.Background(), minimalStoreSession(sessionID, agentID, 1, environmentID, now)); err != nil {
			return err
		}
		return tx.CreatePrimaryThread(context.Background(), &session.Thread{
			ID:           threadID,
			WorkspaceID:  workspace.DefaultID,
			SessionID:    sessionID,
			Role:         session.ThreadRoleMain,
			Visibility:   session.ThreadVisibilityPublic,
			Status:       session.ThreadStatusIdle,
			AgentType:    "default",
			CreatedAt:    now,
			LastActiveAt: now,
			UpdatedAt:    now,
		})
	}); err != nil {
		t.Fatalf("create session with primary thread: %v", err)
	}
}

func seedSessionThread(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, threadID string, parentThreadID string, role string, visibility string, status string, now time.Time, archivedAt *time.Time) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			agent_type, title, task_name, created_at, last_active_at, archived_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, 'default', $8, $8, $9, $9, $10, $9)`,
		string(ws),
		threadID,
		sessionID,
		parentThreadID,
		role,
		visibility,
		status,
		sql.NullString{String: "task " + threadID, Valid: role != "main"},
		now.UTC().Format(time.RFC3339Nano),
		nullableTestTime(archivedAt),
	); err != nil {
		t.Fatalf("seed session thread %s: %v", threadID, err)
	}
}

func seedThreadRuntimeInbox(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, threadID string, runtimeInputID string) {
	t.Helper()
	now := time.Date(2026, 6, 9, 16, 5, 0, 0, time.UTC).Format(time.RFC3339Nano)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'messages', '[]', 1, 1, 'accepted', 'bind_thread_archive', 1, 'pod_thread_archive', $5, $5)`,
		string(ws),
		sessionID,
		threadID,
		runtimeInputID,
		now,
	); err != nil {
		t.Fatalf("seed runtime inbox: %v", err)
	}
}

func seedThreadRuntimeInputQueueJob(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, threadID string, runtimeInputID string, status string) {
	t.Helper()
	now := time.Date(2026, 6, 9, 16, 5, 30, 0, time.UTC).Format(time.RFC3339Nano)
	payload := map[string]any{
		"workspace_id":      string(ws),
		"session_id":        sessionID,
		"session_thread_id": threadID,
		"runtime_input_id":  runtimeInputID,
		"event_ids":         []string{"sevt_" + runtimeInputID},
		"sequence_from":     1,
		"sequence_to":       1,
		"input_kind":        "messages",
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal runtime input queue payload: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, queue_partition_sequence, dedupe_key,
			payload_version, status, payload_json, priority, attempt_count, max_attempts,
			available_at, created_at, updated_at
		) VALUES ($1, $2, 'runtime_input', $3, 1, $4, 1, $5, $6, 0, 0, 10, $7, $7, $7)`,
		"qjob_"+runtimeInputID,
		string(ws),
		"session:"+string(ws)+":"+sessionID,
		"runtime_input:"+string(ws)+":"+sessionID+":"+runtimeInputID,
		status,
		string(payloadBytes),
		now,
	); err != nil {
		t.Fatalf("seed runtime_input queue job: %v", err)
	}
}

func seedThreadPendingWait(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, threadID string, toolUseEventID string) {
	t.Helper()
	now := time.Date(2026, 6, 9, 16, 6, 0, 0, time.UTC)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'tool_call_wait', 'bash', '{}', 'pending', $5, $5)`,
		string(ws),
		sessionID,
		threadID,
		toolUseEventID,
		now.Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed pending wait: %v", err)
	}
}

func nullableTestTime(value *time.Time) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: value.UTC().Format(time.RFC3339Nano), Valid: true}
}

func sessionStoreRowCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
}

func memoryStoreResourceForList(resourceID string, sessionID string, memoryStoreID string, now time.Time) *session.Resource {
	return &session.Resource{
		ID:          resourceID,
		SessionID:   sessionID,
		Type:        session.ResourceTypeMemoryStore,
		CreatedAt:   now,
		UpdatedAt:   now,
		MemoryStore: &session.MemoryStoreResource{MemoryStoreID: memoryStoreID, Access: "read_only", Name: "Memory " + memoryStoreID, MountPath: "/mnt/memory/" + memoryStoreID},
	}
}

func sessionIDs(sessions []*session.Session) []string {
	ids := make([]string, 0, len(sessions))
	for _, sess := range sessions {
		ids = append(ids, sess.ID)
	}
	return ids
}

func seedQueuedSessionEvent(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, eventID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (workspace_id, session_id, event_id, sequence, type, payload_json, created_at, updated_at, processed_at)
		 VALUES ($1, $2, $3, 1, 'user.message', '{"content":[{"type":"text","text":"queued"}]}', '2026-06-09T12:30:00Z', '2026-06-09T12:30:00Z', NULL)`,
		string(ws),
		sessionID,
		eventID,
	); err != nil {
		t.Fatalf("seed queued event: %v", err)
	}
}

func setSessionRuntimeStatus(t *testing.T, db *sql.DB, ws workspace.ID, sessionID string, status string) {
	t.Helper()
	var idleSince any = "2026-06-09T12:00:00Z"
	if status == "running" {
		idleSince = nil
	}
	result, err := db.ExecContext(context.Background(),
		`UPDATE session_runtime_status
		    SET status = $1,
		        idle_since = $2,
		        updated_at = '2026-06-09T12:00:00Z'
		  WHERE workspace_id = $3 AND session_id = $4`,
		status,
		idleSince,
		string(ws),
		sessionID,
	)
	if err != nil {
		t.Fatalf("set runtime status: %v", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		t.Fatalf("set runtime status affected %d rows; want 1", affected)
	}
}

func jsonFieldString(t *testing.T, raw string, field string) string {
	t.Helper()
	var body map[string]string
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode json payload %s: %v", raw, err)
	}
	return body[field]
}

func installSessionLifecycleAudit(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE session_lifecycle_audit (
		    seq BIGSERIAL PRIMARY KEY,
		    session_id TEXT NOT NULL,
		    old_state TEXT NOT NULL,
		    new_state TEXT NOT NULL
		  )`,
		`CREATE OR REPLACE FUNCTION session_lifecycle_audit_fn()
		  RETURNS trigger
		  LANGUAGE plpgsql
		  SECURITY DEFINER
		  AS $$
		  BEGIN
		    IF OLD.lifecycle_state IS DISTINCT FROM NEW.lifecycle_state THEN
		      INSERT INTO session_lifecycle_audit(session_id, old_state, new_state)
		      VALUES (NEW.id, OLD.lifecycle_state, NEW.lifecycle_state);
		    END IF;
		    RETURN NEW;
		  END;
		  $$`,
		`CREATE TRIGGER session_lifecycle_audit_trigger
		  AFTER UPDATE OF lifecycle_state ON sessions
		  FOR EACH ROW
		  EXECUTE FUNCTION session_lifecycle_audit_fn()`,
	} {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("install lifecycle audit: %v", err)
		}
	}
}

func assertSessionLifecycleTransitions(t *testing.T, db *sql.DB, sessionID string, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT old_state || '->' || new_state
		   FROM session_lifecycle_audit
		  WHERE session_id = $1
		  ORDER BY seq`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("read lifecycle audit: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close lifecycle audit rows: %v", err)
		}
	}()
	var got []string
	for rows.Next() {
		var transition string
		if err := rows.Scan(&transition); err != nil {
			t.Fatalf("scan lifecycle audit: %v", err)
		}
		got = append(got, transition)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate lifecycle audit: %v", err)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("lifecycle transitions = %v; want %v", got, want)
	}
}

func assertSessionArchivedAt(t *testing.T, db *sql.DB, sessionID string, want time.Time) {
	t.Helper()
	var archivedAt string
	if err := db.QueryRowContext(context.Background(), `SELECT archived_at FROM sessions WHERE id = $1`, sessionID).Scan(&archivedAt); err != nil {
		t.Fatalf("read archived_at: %v", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, archivedAt)
	if err != nil {
		t.Fatalf("parse archived_at %q: %v", archivedAt, err)
	}
	if !parsed.Equal(want) {
		t.Fatalf("archived_at = %s; want %s", parsed, want)
	}
}
