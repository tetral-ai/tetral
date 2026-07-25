package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	"github.com/tetral-ai/tetral/services/sandbox/internal/resourceprojection"
)

func TestResourcePrefixGCRunnerDeletesDueUnboundPrefixAndMarksDeleted(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_resource_prefix_gc_delete"
	fixedNow := time.Date(2026, 7, 5, 9, 0, 0, 0, time.UTC)
	blobStore := blob.NewFakeBlobStore()
	prefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), sessionID)
	seedResourcePrefixGCSession(t, admin, sessionID, false)
	markResourcePrefixGCSessionDeleted(t, admin, sessionID)
	seedSessionResourcePrefixGCMarker(t, admin, sessionID, prefix, fixedNow.Add(-time.Minute))
	seedSessionResourceRow(t, admin, sessionID, "sesrsc_gc_detached", true)
	putResourcePrefixGCBlob(t, blobStore, prefix+"sesrsc_gc_detached/file", "detached")
	putResourcePrefixGCBlob(t, blobStore, prefix+"older/file", "older")
	putResourcePrefixGCBlob(t, blobStore, "workspaces/default/sessions/other/resources/file", "other")

	runner := newTestResourcePrefixGCRunner(t, runtime, blobStore, fixedNow)
	claimed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(claimed) != 1 || claimed[0].SessionID != sessionID || claimed[0].Prefix != prefix {
		t.Fatalf("claimed = %+v; want exactly %s", claimed, sessionID)
	}
	if blobStore.Has(prefix+"sesrsc_gc_detached/file") || blobStore.Has(prefix+"older/file") {
		t.Fatalf("session prefix objects still exist after GC")
	}
	if !blobStore.Has("workspaces/default/sessions/other/resources/file") {
		t.Fatalf("sibling session object was deleted")
	}
	assertResourcePrefixGCStatus(t, admin, sessionID, "deleted", 1, "", fixedNow, "")
}

func TestResourcePrefixGCRunnerClaimsDeleteNullHandleProviderMissingAfterDurableRelease(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	fixedNow := time.Date(2026, 7, 5, 9, 30, 0, 0, time.UTC)
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{status: sandbox.ProviderStatus{Availability: sandbox.ProviderMissing}}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	request := seedPreparationScopedDeleteRelease(t, admin, "null_handle_missing_gc")
	if _, err := admin.ExecContext(ctx,
		`UPDATE sandboxes
		    SET status='failed', provider_sandbox_id=NULL, provider_metadata_json='{}',
		        cleanup_status='released', startup_failure_reason='startup_failed', failed_at=updated_at
		  WHERE id=$1`, request.SandboxID); err != nil {
		t.Fatalf("seed NULL-handle failed sandbox: %v", err)
	}
	prefix := resourceprojection.SessionPrefix(request.WorkspaceID, request.SessionID)
	seedSessionResourcePrefixGCMarker(t, admin, request.SessionID, prefix, fixedNow.Add(-time.Minute))
	blobStore := blob.NewFakeBlobStore()
	putResourcePrefixGCBlob(t, blobStore, prefix+"detached/file", "detached")
	runner := newTestResourcePrefixGCRunner(t, runtime, blobStore, fixedNow)
	claimed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce before release: %v", err)
	}
	if len(claimed) != 0 || !blobStore.Has(prefix+"detached/file") {
		t.Fatalf("pre-release claim=%+v object_exists=%v; want failed sandbox to block GC", claimed, blobStore.Has(prefix+"detached/file"))
	}
	assertResourcePrefixGCStatus(t, admin, request.SessionID, "pending", 0, "", time.Time{}, "")

	result, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || result != (ReleaseSandboxResult{Status: ReleaseSandboxStatusAlreadyReleased, SandboxStatus: "released"}) {
		t.Fatalf("ReleaseSandbox = %#v, %v; want already_released/released", result, err)
	}
	if provider.statusCalls != 1 || provider.releaseCalls != 0 {
		t.Fatalf("provider probes/releases = %d/%d; want 1/0", provider.statusCalls, provider.releaseCalls)
	}
	var sandboxStatus string
	if err := admin.QueryRowContext(ctx, `SELECT status FROM sandboxes WHERE id=$1`, request.SandboxID).Scan(&sandboxStatus); err != nil {
		t.Fatalf("read sandbox status: %v", err)
	}
	if sandboxStatus != "released" {
		t.Fatalf("sandbox status = %q; want released before prefix GC", sandboxStatus)
	}

	claimed, err = runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(claimed) != 1 || claimed[0].SessionID != request.SessionID || claimed[0].Prefix != prefix {
		t.Fatalf("claimed = %+v; want exactly released session %s", claimed, request.SessionID)
	}
	if blobStore.Has(prefix + "detached/file") {
		t.Fatal("released NULL-handle session prefix object still exists after GC")
	}
	assertResourcePrefixGCStatus(t, admin, request.SessionID, "deleted", 1, "", fixedNow, "")
}

func TestResourcePrefixGCRunnerSkipsBoundAndActiveResourceMarkers(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	fixedNow := time.Date(2026, 7, 5, 10, 0, 0, 0, time.UTC)
	blobStore := blob.NewFakeBlobStore()

	boundSessionID := "sesn_resource_prefix_gc_bound"
	boundPrefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), boundSessionID)
	seedResourcePrefixGCSession(t, admin, boundSessionID, true)
	markResourcePrefixGCSessionDeleted(t, admin, boundSessionID)
	seedSessionResourcePrefixGCMarker(t, admin, boundSessionID, boundPrefix, fixedNow.Add(-time.Minute))
	putResourcePrefixGCBlob(t, blobStore, boundPrefix+"sesrsc_bound/file", "bound")

	activeSessionID := "sesn_resource_prefix_gc_active"
	activePrefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), activeSessionID)
	seedResourcePrefixGCSession(t, admin, activeSessionID, false)
	markResourcePrefixGCSessionDeleted(t, admin, activeSessionID)
	seedSessionResourcePrefixGCMarker(t, admin, activeSessionID, activePrefix, fixedNow.Add(-time.Minute))
	seedSessionResourceRow(t, admin, activeSessionID, "sesrsc_gc_active", false)
	putResourcePrefixGCBlob(t, blobStore, activePrefix+"sesrsc_gc_active/file", "active")

	liveSessionID := "sesn_resource_prefix_gc_live"
	livePrefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), liveSessionID)
	seedResourcePrefixGCSession(t, admin, liveSessionID, false)
	seedSessionResourcePrefixGCMarker(t, admin, liveSessionID, livePrefix, fixedNow.Add(-time.Minute))
	seedSessionResourceRow(t, admin, liveSessionID, "sesrsc_gc_live", true)
	putResourcePrefixGCBlob(t, blobStore, livePrefix+"sesrsc_gc_live/file", "live")

	archivedSandboxSessionID := "sesn_resource_prefix_gc_archived_sandbox"
	archivedSandboxPrefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), archivedSandboxSessionID)
	seedResourcePrefixGCSession(t, admin, archivedSandboxSessionID, false)
	markResourcePrefixGCSessionDeleted(t, admin, archivedSandboxSessionID)
	seedSessionResourcePrefixGCMarker(t, admin, archivedSandboxSessionID, archivedSandboxPrefix, fixedNow.Add(-time.Minute))
	if _, err := admin.ExecContext(ctx, `UPDATE sandboxes SET status='archived' WHERE session_id=$1`, archivedSandboxSessionID); err != nil {
		t.Fatalf("mark sandbox archived: %v", err)
	}
	putResourcePrefixGCBlob(t, blobStore, archivedSandboxPrefix+"archived/file", "archived")

	runner := newTestResourcePrefixGCRunner(t, runtime, blobStore, fixedNow)
	claimed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed = %+v; want none while runtime is bound or a file resource is active", claimed)
	}
	if !blobStore.Has(boundPrefix+"sesrsc_bound/file") ||
		!blobStore.Has(activePrefix+"sesrsc_gc_active/file") ||
		!blobStore.Has(livePrefix+"sesrsc_gc_live/file") ||
		!blobStore.Has(archivedSandboxPrefix+"archived/file") {
		t.Fatalf("resource prefix GC deleted a fenced object")
	}
	assertResourcePrefixGCStatus(t, admin, boundSessionID, "pending", 0, "", time.Time{}, "")
	assertResourcePrefixGCStatus(t, admin, activeSessionID, "pending", 0, "", time.Time{}, "")
	assertResourcePrefixGCStatus(t, admin, liveSessionID, "pending", 0, "", time.Time{}, "")
	assertResourcePrefixGCStatus(t, admin, archivedSandboxSessionID, "pending", 0, "", time.Time{}, "")
}

func TestResourcePrefixGCRunnerRetryableFailureKeepsMarkerDueLater(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_resource_prefix_gc_retry"
	fixedNow := time.Date(2026, 7, 5, 11, 0, 0, 0, time.UTC)
	blobStore := blob.NewFakeBlobStore()
	prefix := resourceprojection.SessionPrefix(string(workspace.DefaultID), sessionID)
	seedResourcePrefixGCSession(t, admin, sessionID, false)
	markResourcePrefixGCSessionDeleted(t, admin, sessionID)
	seedSessionResourcePrefixGCMarker(t, admin, sessionID, prefix, fixedNow.Add(-time.Minute))
	seedSessionResourceRow(t, admin, sessionID, "sesrsc_gc_retry", true)
	putResourcePrefixGCBlob(t, blobStore, prefix+"sesrsc_gc_retry/file", "retry")
	blobStore.SetDeletePrefixHook(func(context.Context, string) error {
		return errors.New("forced delete prefix failure")
	})

	runner := newTestResourcePrefixGCRunner(t, runtime, blobStore, fixedNow)
	runner.Config.RetryAfter = 2 * time.Minute
	claimed, err := runner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed = %+v; want one retryable claim", claimed)
	}
	if !blobStore.Has(prefix + "sesrsc_gc_retry/file") {
		t.Fatalf("failed DeletePrefix removed object")
	}
	assertResourcePrefixGCStatus(t, admin, sessionID, "retryable_failed", 1, resourcePrefixGCErrorDeleteFailed, time.Time{}, fixedNow.Add(2*time.Minute).Format(time.RFC3339Nano))
}

func TestResourcePrefixGCRunnerRequiresConfiguredWorkspaceID(t *testing.T) {
	runtime, _ := newReleaseHandlerTestDB(t)
	runner := newTestResourcePrefixGCRunner(t, runtime, blob.NewFakeBlobStore(), time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC))
	runner.Config.WorkspaceID = ""

	_, err := runner.RunOnce(context.Background())
	if err == nil {
		t.Fatal("RunOnce accepted empty workspace_id")
	}
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) || !strings.Contains(cfgErr.Error(), "workspace_id is required") {
		t.Fatalf("RunOnce empty workspace err = %v; want ConfigError", err)
	}
}

func newTestResourcePrefixGCRunner(t *testing.T, runtime *sql.DB, blobStore *blob.FakeBlobStore, now time.Time) *ResourcePrefixGCRunner {
	t.Helper()
	return &ResourcePrefixGCRunner{
		Client: dbconnect.NewClientForTesting(runtime),
		Preparer: newTestResourceProjectionPreparer(t, blobStore,
			&recordingResourceCredentialMinter{},
			&recordingPreparationCommandRunner{},
		),
		Config: ResourcePrefixGCRunnerConfig{
			WorkspaceID: string(workspace.DefaultID),
			Limit:       10,
			RetryAfter:  time.Minute,
			ClaimLease:  time.Minute,
			Clock:       func() time.Time { return now },
		},
	}
}

func seedResourcePrefixGCSession(t *testing.T, db *sql.DB, sessionID string, bound bool) {
	t.Helper()
	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         sessionID,
		SandboxID:         "sandbox_" + sessionID,
		BindingID:         "binding_" + sessionID,
		BindingGeneration: 1,
	}
	seedReleaseHandlerClaimedSandbox(t, db, request)
	if bound {
		return
	}
	if _, err := db.Exec(
		`UPDATE session_runtime_status
		    SET cleanup_job_id = NULL,
		        cleanup_after = NULL,
		        cleanup_enqueued_at = NULL,
		        cleanup_claimed_at = NULL,
		        binding_id = NULL,
		        binding_generation = NULL
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark session unbound: %v", err)
	}
	if _, err := db.Exec(
		`UPDATE sandboxes
		    SET status = 'released'
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark sandbox released: %v", err)
	}
}

func markResourcePrefixGCSessionDeleted(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	if _, err := db.Exec(
		`UPDATE sessions
		    SET lifecycle_state = 'deleted'
		  WHERE workspace_id = $1
		    AND id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
}

func seedSessionResourcePrefixGCMarker(t *testing.T, db *sql.DB, sessionID string, prefix string, createdAt time.Time) {
	t.Helper()
	timestamp := createdAt.UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(
		`INSERT INTO session_resource_prefix_gc (
			workspace_id, session_id, prefix, status, created_at, updated_at
		) VALUES ($1, $2, $3, 'pending', $4, $4)`,
		string(workspace.DefaultID),
		sessionID,
		prefix,
		timestamp,
	); err != nil {
		t.Fatalf("seed resource prefix gc marker: %v", err)
	}
}

func seedSessionResourceRow(t *testing.T, db *sql.DB, sessionID string, resourceID string, detached bool) {
	t.Helper()
	createdAt := "2026-07-05T08:00:00Z"
	var detachedAt any
	if detached {
		detachedAt = createdAt
	}
	if _, err := db.Exec(
		`INSERT INTO session_resources (
			workspace_id, session_id, resource_id, type, created_at, updated_at, detached_at
		) VALUES ($1, $2, $3, 'file', $4, $4, $5)`,
		string(workspace.DefaultID),
		sessionID,
		resourceID,
		createdAt,
		detachedAt,
	); err != nil {
		t.Fatalf("seed session resource row: %v", err)
	}
}

func putResourcePrefixGCBlob(t *testing.T, store *blob.FakeBlobStore, key string, content string) {
	t.Helper()
	if err := store.Put(context.Background(), key, strings.NewReader(content), int64(len(content))); err != nil {
		t.Fatalf("put blob %q: %v", key, err)
	}
}

func assertResourcePrefixGCStatus(t *testing.T, db *sql.DB, sessionID string, wantStatus string, wantAttempts int, wantErrorKind string, wantCompletedAt time.Time, wantNextAttemptAt string) {
	t.Helper()
	var status string
	var attemptCount int
	var completedAt sql.NullString
	var nextAttemptAt sql.NullString
	var lastErrorKind sql.NullString
	if err := db.QueryRow(
		`SELECT status, attempt_count, completed_at, next_attempt_at, last_error_kind
		   FROM session_resource_prefix_gc
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&status, &attemptCount, &completedAt, &nextAttemptAt, &lastErrorKind); err != nil {
		t.Fatalf("read resource prefix gc marker: %v", err)
	}
	if status != wantStatus || attemptCount != wantAttempts {
		t.Fatalf("marker status/attempts = %q/%d; want %q/%d", status, attemptCount, wantStatus, wantAttempts)
	}
	if wantCompletedAt.IsZero() {
		if completedAt.Valid {
			t.Fatalf("completed_at = %v; want null", completedAt)
		}
	} else if !completedAt.Valid || completedAt.String != wantCompletedAt.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("completed_at = %v; want %s", completedAt, wantCompletedAt.UTC().Format(time.RFC3339Nano))
	}
	if wantNextAttemptAt == "" {
		if nextAttemptAt.Valid {
			t.Fatalf("next_attempt_at = %v; want null", nextAttemptAt)
		}
	} else if !nextAttemptAt.Valid || nextAttemptAt.String != wantNextAttemptAt {
		t.Fatalf("next_attempt_at = %v; want %s", nextAttemptAt, wantNextAttemptAt)
	}
	if wantErrorKind == "" {
		if lastErrorKind.Valid {
			t.Fatalf("last_error_kind = %v; want null", lastErrorKind)
		}
	} else if !lastErrorKind.Valid || lastErrorKind.String != wantErrorKind {
		t.Fatalf("last_error_kind = %v; want %s", lastErrorKind, wantErrorKind)
	}
}
