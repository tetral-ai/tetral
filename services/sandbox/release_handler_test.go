package tetralsandbox

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestMigrationPreservesRuntimePodLostReleaseReplayAndLegacyEnvironmentUnknown(t *testing.T) {
	db := storagetest.NewEmptyPostgreSQLAdminDB(t)
	ctx := context.Background()
	if err := storage.InitializePostgreSQLSchema(ctx, db); err != nil {
		t.Fatalf("InitializePostgreSQLSchema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO workspaces (id,type,name,created_at) VALUES ('default','workspace','default','2026-07-01T12:00:00Z')`); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	request := ReleaseSandboxRequest{WorkspaceID: "default", SessionID: "sesn_legacy_pod_lost", SandboxID: "sandbox_legacy_pod_lost", BindingID: "bind_legacy_pod_lost", BindingGeneration: 4, Reason: string(sandbox.ReleaseReasonRuntimePodLost), IdempotencyKey: "runtime_pod_lost:sesn_legacy_pod_lost:bind_legacy_pod_lost:4"}
	seedReleaseHandlerClaimedSandbox(t, db, request)
	if _, err := db.ExecContext(ctx, `INSERT INTO sandbox_release_idempotency_keys (workspace_id,idempotency_key,session_id,sandbox_id,binding_id,binding_generation,status,response_status,sandbox_status,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,'completed','released','released','2026-07-01T12:00:00Z','2026-07-01T12:00:00Z')`, request.WorkspaceID, request.IdempotencyKey, request.SessionID, request.SandboxID, request.BindingID, request.BindingGeneration); err != nil {
		t.Fatalf("seed legacy idempotency: %v", err)
	}
	if err := storage.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	var reason, responseStatus, sandboxStatus string
	var environmentID sql.NullString
	var environmentGeneration sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT reason,response_status,sandbox_status FROM sandbox_release_idempotency_keys WHERE workspace_id=$1 AND idempotency_key=$2`, request.WorkspaceID, request.IdempotencyKey).Scan(&reason, &responseStatus, &sandboxStatus); err != nil {
		t.Fatalf("read migrated release: %v", err)
	}
	if reason != string(sandbox.ReleaseReasonRuntimePodLost) || responseStatus != "released" || sandboxStatus != "released" {
		t.Fatalf("migrated release = reason %q response %q sandbox %q; want pod-loss reason with terminal facts unchanged", reason, responseStatus, sandboxStatus)
	}
	if err := db.QueryRowContext(ctx, `SELECT environment_id,environment_generation FROM sandboxes WHERE id=$1`, request.SandboxID).Scan(&environmentID, &environmentGeneration); err != nil {
		t.Fatalf("read migrated sandbox: %v", err)
	}
	if environmentID.Valid || environmentGeneration.Valid {
		t.Fatalf("legacy environment identity = %v/%v; want unknown to force REPLACE", environmentID, environmentGeneration)
	}
	client := dbconnect.NewClientForTesting(db)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("ReleaseSandbox replay: %v", err)
	}
	if replay.Status != ReleaseSandboxStatusReleased || replay.SandboxStatus != "released" || provider.releaseCalls != 0 {
		t.Fatalf("legacy replay = %+v provider calls=%d; want identical terminal replay without rewrite", replay, provider.releaseCalls)
	}
}

func TestReleaseHandlerReleaseSandboxIsIdempotentOnKeyAndFence(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{}
	service := sandbox.NewService(
		store,
		provider,
		sandbox.WithProviderName("tetral"),
		sandbox.WithClock(func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) }),
	)
	handler := NewReleaseHandler(client, service, store)

	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         "sesn_release_idempotent",
		SandboxID:         "sandbox_release_idempotent",
		BindingID:         "bind_release_idempotent",
		BindingGeneration: 7,
		Reason:            string(sandbox.ReleaseReasonCleanup),
		IdempotencyKey:    "cleanup_session:sesn_release_idempotent:cleanup_1",
	}
	seedReleaseHandlerClaimedSandbox(t, admin, request)

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("first ReleaseSandbox: %v", err)
	}
	if first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusArchived, SandboxStatus: "archived"}) {
		t.Fatalf("first result = %#v; want archived", first)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after first = %d; want 1", provider.releaseCalls)
	}

	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("replay ReleaseSandbox: %v", err)
	}
	if replay != first {
		t.Fatalf("replay result = %#v; want %#v", replay, first)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after replay = %d; want 1", provider.releaseCalls)
	}

	differentKey := request
	differentKey.IdempotencyKey = "cleanup_session:sesn_release_idempotent:cleanup_2"
	conflict, err := handler.ReleaseSandbox(ctx, differentKey)
	if err != nil {
		t.Fatalf("different key ReleaseSandbox: %v", err)
	}
	if conflict.Status != ReleaseSandboxStatusFailed {
		t.Fatalf("different key result = %#v; want failed", conflict)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after different key = %d; want 1", provider.releaseCalls)
	}

	reasonCollision := request
	reasonCollision.IdempotencyKey = "runtime_pod_lost:sesn_release_idempotent:bind_release_idempotent:7"
	reasonCollision.Reason = string(sandbox.ReleaseReasonRuntimePodLost)
	collision, err := handler.ReleaseSandbox(ctx, reasonCollision)
	if err != nil {
		t.Fatalf("cleanup/runtime_pod_lost identity collision: %v", err)
	}
	if collision.Status != ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("reason collision = %#v provider calls=%d; want deterministic failed with zero new provider calls", collision, provider.releaseCalls)
	}

	differentFence := request
	differentFence.BindingGeneration = 8
	mismatch, err := handler.ReleaseSandbox(ctx, differentFence)
	if err != nil {
		t.Fatalf("different fence ReleaseSandbox: %v", err)
	}
	if mismatch.Status != ReleaseSandboxStatusFailed {
		t.Fatalf("different fence result = %#v; want failed", mismatch)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after different fence = %d; want 1", provider.releaseCalls)
	}
}

func TestReleaseHandlerRuntimePodLostUsesBindingFenceWithoutCleanupClaim(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{}
	service := sandbox.NewService(store, provider, sandbox.WithProviderName("tetral"))
	handler := NewReleaseHandler(client, service, store)
	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         "sesn_release_pod_lost",
		SandboxID:         "sandbox_release_pod_lost",
		BindingID:         "bind_release_pod_lost",
		BindingGeneration: 9,
		Reason:            string(sandbox.ReleaseReasonRuntimePodLost),
		IdempotencyKey:    "runtime_pod_lost:sesn_release_pod_lost:bind_release_pod_lost:9",
	}
	seedReleaseHandlerClaimedSandbox(t, admin, request)
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_job_id = NULL, cleanup_claimed_at = NULL
		  WHERE workspace_id = $1 AND session_id = $2`,
		request.WorkspaceID, request.SessionID,
	); err != nil {
		t.Fatalf("clear cleanup claim: %v", err)
	}

	result, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("ReleaseSandbox runtime_pod_lost: %v", err)
	}
	if result != (ReleaseSandboxResult{Status: ReleaseSandboxStatusArchived, SandboxStatus: "archived"}) {
		t.Fatalf("result = %#v; want archived", result)
	}
	if provider.releaseCalls != 1 || len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != sandbox.ReleaseReasonRuntimePodLost {
		t.Fatalf("provider releases = %d reasons=%v", provider.releaseCalls, provider.releaseReasons)
	}
	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || replay != result || provider.releaseCalls != 1 {
		t.Fatalf("replay = %#v err=%v calls=%d", replay, err, provider.releaseCalls)
	}

	stale := request
	stale.IdempotencyKey += ":stale"
	stale.BindingGeneration++
	staleResult, err := handler.ReleaseSandbox(ctx, stale)
	if err != nil {
		t.Fatalf("stale ReleaseSandbox: %v", err)
	}
	if staleResult.Status != ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("stale result = %#v calls=%d; want failed without provider call", staleResult, provider.releaseCalls)
	}
}

func TestReleaseHandlerOrdinaryReasonsDoNotWidenFailedCleanupReleasedCandidate(t *testing.T) {
	for _, reason := range []sandbox.ReleaseReason{sandbox.ReleaseReasonCleanup, sandbox.ReleaseReasonRuntimePodLost} {
		t.Run(string(reason), func(t *testing.T) {
			runtime, admin := newReleaseHandlerTestDB(t)
			client := dbconnect.NewClientForTesting(runtime)
			store := sandbox.NewPostgreSQLStore(client)
			provider := &recordingReleaseProvider{}
			handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
			request := ReleaseSandboxRequest{
				WorkspaceID: "default", SessionID: "sesn_ordinary_failed_" + string(reason), SandboxID: "sandbox_ordinary_failed_" + string(reason),
				BindingID: "bind_ordinary_failed_" + string(reason), BindingGeneration: 5, Reason: string(reason),
				IdempotencyKey: string(reason) + ":sesn_ordinary_failed:" + string(reason),
			}
			seedReleaseHandlerClaimedSandbox(t, admin, request)
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sandboxes SET status='failed',cleanup_status='released',startup_failure_reason='startup_failed',failed_at=updated_at WHERE id=$1`, request.SandboxID); err != nil {
				t.Fatalf("seed failed cleanup-released sandbox: %v", err)
			}
			got, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil || got.Status != ReleaseSandboxStatusAlreadyReleased || provider.statusCalls != 0 || provider.releaseCalls != 0 {
				t.Fatalf("ordinary %s result=%#v err=%v probes=%d releases=%d; want already_released without widened provider call", reason, got, err, provider.statusCalls, provider.releaseCalls)
			}
			var status, cleanupStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT status,cleanup_status FROM sandboxes WHERE id=$1`, request.SandboxID).Scan(&status, &cleanupStatus); err != nil {
				t.Fatalf("read sandbox: %v", err)
			}
			if status != "failed" || cleanupStatus != "released" {
				t.Fatalf("ordinary reason rewrote terminal facts to %q/%q", status, cleanupStatus)
			}
		})
	}
}

func TestReleaseHandlerRejectsUnclaimedCleanupJob(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{}
	service := sandbox.NewService(
		store,
		provider,
		sandbox.WithProviderName("tetral"),
		sandbox.WithClock(func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) }),
	)
	handler := NewReleaseHandler(client, service, store)

	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         "sesn_release_unclaimed",
		SandboxID:         "sandbox_release_unclaimed",
		BindingID:         "bind_release_unclaimed",
		BindingGeneration: 7,
		Reason:            string(sandbox.ReleaseReasonCleanup),
		IdempotencyKey:    "cleanup_session:sesn_release_unclaimed:cleanup_1",
	}
	seedReleaseHandlerClaimedSandbox(t, admin, request)
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_claimed_at = NULL
		  WHERE workspace_id = $1
		    AND session_id = $2`,
		request.WorkspaceID,
		request.SessionID,
	); err != nil {
		t.Fatalf("mark cleanup job unclaimed: %v", err)
	}

	result, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("ReleaseSandbox unclaimed cleanup: %v", err)
	}
	if result.Status != ReleaseSandboxStatusFailed {
		t.Fatalf("unclaimed cleanup result = %#v; want failed", result)
	}
	if provider.releaseCalls != 0 {
		t.Fatalf("provider release calls = %d; want 0", provider.releaseCalls)
	}
}

func TestReleaseHandlerDoesNotRepeatProviderReleaseAfterDurableFinalizeFailure(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	delegateStore := sandbox.NewPostgreSQLStore(client)
	store := &failOnceCompleteReleaseStore{Store: delegateStore, failures: 2}
	provider := &recordingReleaseProvider{}
	service := sandbox.NewService(
		store,
		provider,
		sandbox.WithProviderName("tetral"),
		sandbox.WithClock(func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) }),
	)
	handler := NewReleaseHandler(client, service, store)

	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         "sesn_release_finalize_retry",
		SandboxID:         "sandbox_release_finalize_retry",
		BindingID:         "bind_release_finalize_retry",
		BindingGeneration: 9,
		Reason:            string(sandbox.ReleaseReasonCleanup),
		IdempotencyKey:    "cleanup_session:sesn_release_finalize_retry:cleanup_1",
	}
	seedReleaseHandlerClaimedSandbox(t, admin, request)

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("first ReleaseSandbox: %v", err)
	}
	if first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "releasing"}) {
		t.Fatalf("first result = %#v; want retry_later/releasing", first)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after first = %d; want 1", provider.releaseCalls)
	}
	assertReleaseIdempotencyStatus(t, admin, request, "provider_released")

	retry, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("retry ReleaseSandbox: %v", err)
	}
	if retry != (ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "releasing"}) {
		t.Fatalf("retry result = %#v; want retry_later/releasing", retry)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after retry = %d; want 1", provider.releaseCalls)
	}
	assertReleaseIdempotencyStatus(t, admin, request, "provider_released")

	final, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("final ReleaseSandbox: %v", err)
	}
	if final != (ReleaseSandboxResult{Status: ReleaseSandboxStatusArchived, SandboxStatus: "archived"}) {
		t.Fatalf("final result = %#v; want archived", final)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after final = %d; want 1", provider.releaseCalls)
	}
	assertReleaseIdempotencyStatus(t, admin, request, "completed")
}

func TestReleaseHandlerDoesNotRepeatProviderReleaseAfterReleaseStarted(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{
		releaseErr: &sandbox.ProviderError{
			Provider:    "tetral",
			Stage:       sandbox.StageReleaseSandbox,
			Kind:        sandbox.ProviderErrorUnavailable,
			Retryable:   true,
			SafeMessage: "provider release state unknown",
		},
		status: sandbox.ProviderStatus{Availability: sandbox.ProviderMissing},
	}
	service := sandbox.NewService(
		store,
		provider,
		sandbox.WithProviderName("tetral"),
		sandbox.WithClock(func() time.Time { return time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC) }),
	)
	handler := NewReleaseHandler(client, service, store)

	request := ReleaseSandboxRequest{
		WorkspaceID:       string(workspace.DefaultID),
		SessionID:         "sesn_release_started_retry",
		SandboxID:         "sandbox_release_started_retry",
		BindingID:         "bind_release_started_retry",
		BindingGeneration: 10,
		Reason:            string(sandbox.ReleaseReasonCleanup),
		IdempotencyKey:    "cleanup_session:sesn_release_started_retry:cleanup_1",
	}
	seedReleaseHandlerClaimedSandbox(t, admin, request)

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("first ReleaseSandbox: %v", err)
	}
	if first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "releasing"}) {
		t.Fatalf("first result = %#v; want retry_later/releasing", first)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after first = %d; want 1", provider.releaseCalls)
	}
	assertReleaseIdempotencyStatus(t, admin, request, "provider_release_started")

	provider.releaseErr = nil
	retry, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("retry ReleaseSandbox: %v", err)
	}
	if retry != (ReleaseSandboxResult{Status: ReleaseSandboxStatusArchived, SandboxStatus: "archived"}) {
		t.Fatalf("retry result = %#v; want archived", retry)
	}
	if provider.releaseCalls != 1 {
		t.Fatalf("provider release calls after retry = %d; want still 1", provider.releaseCalls)
	}
	if provider.statusCalls != 1 {
		t.Fatalf("provider status calls after retry = %d; want 1", provider.statusCalls)
	}
	assertReleaseIdempotencyStatus(t, admin, request, "completed")
}

func TestReleaseHandlerPersistsAndReplaysTerminalProviderReleaseFailure(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{releaseErr: &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageReleaseSandbox, Kind: sandbox.ProviderErrorAuthFailed,
		Retryable: false, SafeMessage: "provider authentication failed",
	}}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	request := seedPreparationScopedDeleteRelease(t, admin, "terminal_provider_release")

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "releasing"}) {
		t.Fatalf("first result = %#v, %v; want failed/releasing", first, err)
	}
	assertReleaseIdempotencyResult(t, admin, request, "completed", "failed", "releasing")
	assertReleaseSandboxRow(t, admin, request.SandboxID, "releasing", "none")

	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || replay != first || provider.releaseCalls != 1 || provider.statusCalls != 0 {
		t.Fatalf("replay = %#v, %v calls release/status=%d/%d; want verbatim replay and 1/0", replay, err, provider.releaseCalls, provider.statusCalls)
	}
}

func TestReleaseHandlerRetryableProviderReleaseFailureRemainsRetryLater(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	retryable := &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageReleaseSandbox, Kind: sandbox.ProviderErrorUnavailable,
		Retryable: true, SafeMessage: "provider temporarily unavailable",
	}
	provider := &recordingReleaseProvider{releaseErr: retryable, statusErr: retryable}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	request := seedPreparationScopedDeleteRelease(t, admin, "retryable_provider_release")

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusRetryLater, SandboxStatus: "releasing"}) {
		t.Fatalf("first result = %#v, %v; want retry_later/releasing", first, err)
	}
	assertReleaseIdempotencyResult(t, admin, request, "provider_release_started", "retry_later", "releasing")
	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || replay != first || provider.releaseCalls != 1 || provider.statusCalls != 1 {
		t.Fatalf("replay = %#v, %v calls release/status=%d/%d; want retry_later and 1/1", replay, err, provider.releaseCalls, provider.statusCalls)
	}
	assertReleaseIdempotencyResult(t, admin, request, "provider_release_started", "retry_later", "releasing")
}

func TestReleaseHandlerPersistsAndReplaysTerminalProviderObserveFailure(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{releaseErr: &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageReleaseSandbox, Kind: sandbox.ProviderErrorUnavailable,
		Retryable: true, SafeMessage: "provider release state unknown",
	}}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	request := seedPreparationScopedDeleteRelease(t, admin, "terminal_provider_observe")

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || first.Status != ReleaseSandboxStatusRetryLater {
		t.Fatalf("first result = %#v, %v; want retry_later", first, err)
	}
	provider.releaseErr = nil
	provider.statusErr = &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageStatus, Kind: sandbox.ProviderErrorConfigInvalid,
		Retryable: false, SafeMessage: "provider status request is invalid",
	}
	terminal, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || terminal != (ReleaseSandboxResult{Status: ReleaseSandboxStatusFailed, SandboxStatus: "releasing"}) {
		t.Fatalf("observe result = %#v, %v; want failed/releasing", terminal, err)
	}
	assertReleaseIdempotencyResult(t, admin, request, "completed", "failed", "releasing")

	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || replay != terminal || provider.releaseCalls != 1 || provider.statusCalls != 1 {
		t.Fatalf("replay = %#v, %v calls release/status=%d/%d; want verbatim replay and 1/1", replay, err, provider.releaseCalls, provider.statusCalls)
	}
}

func TestReleaseHandlerRetryableProviderObserveFailureRemainsRetryLater(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{releaseErr: &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageReleaseSandbox, Kind: sandbox.ProviderErrorUnavailable,
		Retryable: true, SafeMessage: "provider release state unknown",
	}}
	handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
	request := seedPreparationScopedDeleteRelease(t, admin, "retryable_provider_observe")

	first, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || first.Status != ReleaseSandboxStatusRetryLater {
		t.Fatalf("first result = %#v, %v; want retry_later", first, err)
	}
	provider.releaseErr = nil
	provider.statusErr = &sandbox.ProviderError{
		Provider: "tetral", Stage: sandbox.StageStatus, Kind: sandbox.ProviderErrorUnavailable,
		Retryable: true, SafeMessage: "provider status temporarily unavailable",
	}
	for attempt := 1; attempt <= 2; attempt++ {
		got, err := handler.ReleaseSandbox(ctx, request)
		if err != nil || got != first {
			t.Fatalf("observe attempt %d = %#v, %v; want retry_later replay", attempt, got, err)
		}
	}
	if provider.releaseCalls != 1 || provider.statusCalls != 2 {
		t.Fatalf("calls release/status=%d/%d; want 1/2", provider.releaseCalls, provider.statusCalls)
	}
	assertReleaseIdempotencyResult(t, admin, request, "provider_release_started", "retry_later", "releasing")
}

func TestNormalizeReleaseSandboxStatusUsesClosedProtocolEnum(t *testing.T) {
	statuses := []sandbox.Status{
		sandbox.StatusCreating,
		sandbox.StatusActive,
		sandbox.StatusStopped,
		sandbox.StatusArchived,
		sandbox.StatusResuming,
		sandbox.StatusReleasing,
		sandbox.StatusReleased,
		sandbox.StatusArchived,
		sandbox.StatusFailed,
		"legacy_unknown",
	}
	for _, status := range statuses {
		got := releaseResponseSandboxStatus(status)
		if got != "releasing" && got != "released" && got != "archived" && got != "failed" {
			t.Fatalf("sandbox status %q normalized to %q outside the protocol enum", status, got)
		}
	}
	if got := releaseResponseSandboxStatus(sandbox.StatusActive); got != "failed" {
		t.Fatalf("active sandbox normalized to %q; want failed", got)
	}
}

func TestReleaseHandlerDeleteUsesPreparationFenceAndWidensLegacyFailedCandidate(t *testing.T) {
	runtime, admin := newReleaseHandlerTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := sandbox.NewPostgreSQLStore(client)
	provider := &recordingReleaseProvider{}
	service := sandbox.NewService(store, provider, sandbox.WithProviderName("tetral"))
	handler := NewReleaseHandler(client, service, store)

	seed := ReleaseSandboxRequest{
		WorkspaceID: string(workspace.DefaultID), SessionID: "sesn_release_delete", SandboxID: "sandbox_release_delete",
		BindingID: "bind_release_delete_seed", BindingGeneration: 11,
	}
	seedReleaseHandlerClaimedSandbox(t, admin, seed)
	request := ReleaseSandboxRequest{
		WorkspaceID: seed.WorkspaceID, SessionID: seed.SessionID, SandboxID: seed.SandboxID,
		PreparationAttemptID: "prep_release_delete", DeleteCleanupID: "delcln_release_delete",
		Reason: string(sandbox.ReleaseReasonDelete), IdempotencyKey: "session_delete_cleanup:default:sesn_release_delete:delcln_release_delete",
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions SET lifecycle_state = 'deleted', delete_cleanup_id = $2 WHERE id = $1`, request.SessionID, request.DeleteCleanupID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations SET preparation_attempt_id = $2 WHERE session_id = $1`, request.SessionID, request.PreparationAttemptID); err != nil {
		t.Fatalf("stamp preparation identity: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sandboxes SET status = 'failed', provider_sandbox_id = NULL, provider_metadata_json = '{}', cleanup_status = 'released', startup_failure_reason = 'legacy_failure', failed_at = updated_at WHERE id = $1`, request.SandboxID); err != nil {
		t.Fatalf("seed legacy failed sandbox: %v", err)
	}

	got, err := handler.ReleaseSandbox(ctx, request)
	if err != nil {
		t.Fatalf("ReleaseSandbox delete: %v", err)
	}
	if got != (ReleaseSandboxResult{Status: ReleaseSandboxStatusReleased, SandboxStatus: "released"}) {
		var sandboxStatus string
		_ = admin.QueryRowContext(ctx, `SELECT status FROM sandboxes WHERE id = $1`, request.SandboxID).Scan(&sandboxStatus)
		var idempotencyCount int
		_ = admin.QueryRowContext(ctx, `SELECT count(*) FROM sandbox_release_idempotency_keys WHERE session_id = $1`, request.SessionID).Scan(&idempotencyCount)
		var idempotencyStatus string
		_ = admin.QueryRowContext(ctx, `SELECT status FROM sandbox_release_idempotency_keys WHERE session_id = $1`, request.SessionID).Scan(&idempotencyStatus)
		var fence bool
		_ = admin.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sessions s JOIN session_preparations sp ON sp.session_id=s.id AND sp.workspace_id=s.workspace_id WHERE s.id=$1 AND s.lifecycle_state='deleted' AND s.delete_cleanup_id=$2 AND sp.preparation_attempt_id=$3 AND sp.sandbox_id=$4 AND sp.superseded_at IS NULL)`, request.SessionID, request.DeleteCleanupID, request.PreparationAttemptID, request.SandboxID).Scan(&fence)
		t.Fatalf("delete result = %#v sandbox=%q provider_calls=%d idempotency=%d/%q fence=%v; want released", got, sandboxStatus, provider.releaseCalls, idempotencyCount, idempotencyStatus, fence)
	}
	if provider.releaseCalls != 1 || len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != sandbox.ReleaseReasonDelete {
		t.Fatalf("provider releases = %d reasons=%v; want one delete", provider.releaseCalls, provider.releaseReasons)
	}
	if provider.statusCalls == 0 {
		t.Fatal("delete cleanup did not name-probe failed NULL-handle sandbox")
	}
	var status string
	if err := admin.QueryRowContext(ctx, `SELECT status FROM sandboxes WHERE id = $1`, request.SandboxID).Scan(&status); err != nil {
		t.Fatalf("read sandbox: %v", err)
	}
	if status != string(sandbox.StatusReleased) {
		t.Fatalf("sandbox status = %q; want released", status)
	}
	replay, err := handler.ReleaseSandbox(ctx, request)
	if err != nil || replay != got || provider.releaseCalls != 1 {
		t.Fatalf("delete replay = %#v err=%v calls=%d; want identical without duplicate release", replay, err, provider.releaseCalls)
	}
	differentKeySamePreparation := request
	differentKeySamePreparation.IdempotencyKey += ":conflict"
	preparationConflict, err := handler.ReleaseSandbox(ctx, differentKeySamePreparation)
	if err != nil || preparationConflict.Status != ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("preparation identity conflict = %#v err=%v calls=%d; want failed without provider call", preparationConflict, err, provider.releaseCalls)
	}
	sameKeyDifferentPreparation := request
	sameKeyDifferentPreparation.PreparationAttemptID = "prep_release_delete_conflict"
	sameKeyConflict, err := handler.ReleaseSandbox(ctx, sameKeyDifferentPreparation)
	if err != nil || sameKeyConflict.Status != ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("preparation key replay conflict = %#v err=%v calls=%d; want failed without provider call", sameKeyConflict, err, provider.releaseCalls)
	}
	stale := request
	stale.IdempotencyKey += ":stale"
	stale.DeleteCleanupID = "delcln_stale"
	staleResult, err := handler.ReleaseSandbox(ctx, stale)
	if err != nil || staleResult.Status != ReleaseSandboxStatusFailed || provider.releaseCalls != 1 {
		t.Fatalf("stale delete = %#v err=%v calls=%d; want fenced failure", staleResult, err, provider.releaseCalls)
	}
}

func TestReleaseHandlerDeleteRecordedHandleBranchMatrixDeletesExactlyOnce(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        sandbox.Status
		cleanupStatus string
	}{
		{name: "active", status: sandbox.StatusActive},
		{name: "stopped", status: sandbox.StatusStopped},
		{name: "archived", status: sandbox.StatusArchived},
		{name: "released", status: sandbox.StatusReleased},
		{name: "releasing", status: sandbox.StatusReleasing},
		{name: "failed recorded handle cleanup released", status: sandbox.StatusFailed, cleanupStatus: "released"},
		{name: "legacy released recorded handle cleanup released", status: sandbox.StatusReleased, cleanupStatus: "released"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newReleaseHandlerTestDB(t)
			client := dbconnect.NewClientForTesting(runtime)
			store := sandbox.NewPostgreSQLStore(client)
			provider := &recordingReleaseProvider{}
			handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
			request := seedPreparationScopedDeleteRelease(t, admin, strings.ReplaceAll(tc.name, " ", "_"))
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sandboxes
				    SET status=$2, provider_sandbox_id='provider_recorded_delete', cleanup_status=CASE WHEN $3='' THEN 'none' ELSE $3 END,
				        released_at=CASE WHEN $2='released' THEN updated_at ELSE NULL END,
				        failed_at=CASE WHEN $2='failed' THEN updated_at ELSE NULL END,
				        startup_failure_reason=CASE WHEN $2='failed' THEN 'startup_failed' ELSE NULL END
				  WHERE id=$1`, request.SandboxID, tc.status, tc.cleanupStatus); err != nil {
				t.Fatalf("seed sandbox branch: %v", err)
			}

			first, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil {
				t.Fatalf("ReleaseSandbox: %v", err)
			}
			if first != (ReleaseSandboxResult{Status: ReleaseSandboxStatusReleased, SandboxStatus: "released"}) {
				t.Fatalf("result = %#v; want released", first)
			}
			var status, cleanupStatus string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status, cleanup_status FROM sandboxes WHERE id=$1`, request.SandboxID).Scan(&status, &cleanupStatus); err != nil {
				t.Fatalf("read terminal sandbox: %v", err)
			}
			wantCleanupStatus := tc.cleanupStatus
			if wantCleanupStatus == "" {
				wantCleanupStatus = "none"
			}
			if status != "released" || cleanupStatus != wantCleanupStatus || provider.releaseCalls != 1 ||
				len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != sandbox.ReleaseReasonDelete {
				t.Fatalf("terminal sandbox=%q/%q provider=%d/%v; want released/%s and one delete", status, cleanupStatus, provider.releaseCalls, provider.releaseReasons, wantCleanupStatus)
			}
			replay, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil || replay != first || provider.releaseCalls != 1 {
				t.Fatalf("replay=%#v err=%v provider calls=%d; want stable replay without second delete", replay, err, provider.releaseCalls)
			}
		})
	}
}

func TestReleaseHandlerDeleteNullHandleNameProbeFoundAndAbsent(t *testing.T) {
	for _, tc := range []struct {
		name            string
		providerStatus  sandbox.ProviderStatus
		wantResult      ReleaseSandboxStatus
		wantReleaseCall int
	}{
		{name: "found by deterministic name", providerStatus: sandbox.ProviderStatus{Availability: sandbox.ProviderAvailable, SandboxStatus: sandbox.StatusActive}, wantResult: ReleaseSandboxStatusReleased, wantReleaseCall: 1},
		{name: "absent is vacuous", providerStatus: sandbox.ProviderStatus{Availability: sandbox.ProviderMissing}, wantResult: ReleaseSandboxStatusAlreadyReleased},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newReleaseHandlerTestDB(t)
			client := dbconnect.NewClientForTesting(runtime)
			store := sandbox.NewPostgreSQLStore(client)
			provider := &recordingReleaseProvider{status: tc.providerStatus}
			handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
			request := seedPreparationScopedDeleteRelease(t, admin, strings.ReplaceAll(tc.name, " ", "_"))
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sandboxes SET status='failed', provider_sandbox_id=NULL, provider_metadata_json='{}',
				 cleanup_status='released', startup_failure_reason='startup_failed', failed_at=updated_at WHERE id=$1`, request.SandboxID); err != nil {
				t.Fatalf("seed NULL-handle failed sandbox: %v", err)
			}

			got, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil || got.Status != tc.wantResult || provider.statusCalls != 1 || provider.releaseCalls != tc.wantReleaseCall {
				t.Fatalf("result=%#v err=%v status probes=%d releases=%d; want %s/1/%d", got, err, provider.statusCalls, provider.releaseCalls, tc.wantResult, tc.wantReleaseCall)
			}
			var status, cleanupStatus string
			if err := admin.QueryRowContext(context.Background(), `SELECT status,cleanup_status FROM sandboxes WHERE id=$1`, request.SandboxID).Scan(&status, &cleanupStatus); err != nil {
				t.Fatalf("read terminal sandbox: %v", err)
			}
			if status != "released" || cleanupStatus != "released" {
				t.Fatalf("terminal sandbox=%q/%q; want released/released", status, cleanupStatus)
			}
			replay, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil || replay != got || provider.statusCalls != 1 || provider.releaseCalls != tc.wantReleaseCall {
				t.Fatalf("replay=%#v err=%v probes=%d releases=%d; want stable result without another provider call", replay, err, provider.statusCalls, provider.releaseCalls)
			}
		})
	}
}

func TestReleaseHandlerPreparationIdentityRejectMatrixMakesZeroProviderCalls(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*ReleaseSandboxRequest, *sql.DB)
	}{
		{name: "neither identity", mutate: func(r *ReleaseSandboxRequest, _ *sql.DB) { r.PreparationAttemptID = ""; r.DeleteCleanupID = "" }},
		{name: "both identities", mutate: func(r *ReleaseSandboxRequest, _ *sql.DB) { r.BindingID = "bind_illegal"; r.BindingGeneration = 1 }},
		{name: "foreign workspace", mutate: func(r *ReleaseSandboxRequest, db *sql.DB) {
			if _, err := db.Exec(`INSERT INTO workspaces (id,type,name,created_at) VALUES ('foreign','workspace','foreign','2026-07-01T12:00:00Z')`); err != nil {
				t.Fatalf("seed foreign workspace: %v", err)
			}
			r.WorkspaceID = "foreign"
		}},
		{name: "stale preparation", mutate: func(r *ReleaseSandboxRequest, _ *sql.DB) { r.PreparationAttemptID += "_stale" }},
		{name: "stale delete cleanup", mutate: func(r *ReleaseSandboxRequest, _ *sql.DB) { r.DeleteCleanupID += "_stale" }},
		{name: "live binding", mutate: func(r *ReleaseSandboxRequest, db *sql.DB) {
			if _, err := db.Exec(`UPDATE session_runtime_status SET binding_id='bind_live',binding_generation=2 WHERE session_id=$1`, r.SessionID); err != nil {
				t.Fatalf("seed live binding: %v", err)
			}
			if _, err := db.Exec(`INSERT INTO session_runtime_bindings (
				workspace_id,session_id,binding_id,binding_generation,agent_runtime_namespace,
				agent_runtime_pod_name,agent_runtime_pod_uid,agent_runtime_pod_ip,bound_at,updated_at
			) VALUES ($1,$2,'bind_live',2,'runtime','pod','uid','127.0.0.1','2026-07-01T12:00:00Z','2026-07-01T12:00:00Z')`, r.WorkspaceID, r.SessionID); err != nil {
				t.Fatalf("seed live binding row: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newReleaseHandlerTestDB(t)
			client := dbconnect.NewClientForTesting(runtime)
			store := sandbox.NewPostgreSQLStore(client)
			provider := &recordingReleaseProvider{}
			handler := NewReleaseHandler(client, sandbox.NewService(store, provider, sandbox.WithProviderName("tetral")), store)
			request := seedPreparationScopedDeleteRelease(t, admin, strings.ReplaceAll(tc.name, " ", "_"))
			tc.mutate(&request, admin)
			got, err := handler.ReleaseSandbox(context.Background(), request)
			if err != nil {
				t.Fatalf("ReleaseSandbox reject: %v", err)
			}
			if got.Status != ReleaseSandboxStatusFailed || provider.statusCalls != 0 || provider.releaseCalls != 0 {
				t.Fatalf("reject result=%#v probes=%d releases=%d; want failed and zero provider calls", got, provider.statusCalls, provider.releaseCalls)
			}
		})
	}
}

func seedPreparationScopedDeleteRelease(t *testing.T, db *sql.DB, suffix string) ReleaseSandboxRequest {
	t.Helper()
	request := ReleaseSandboxRequest{
		WorkspaceID: string(workspace.DefaultID), SessionID: "sesn_delete_" + suffix, SandboxID: "sandbox_delete_" + suffix,
		PreparationAttemptID: "prep_delete_" + suffix, DeleteCleanupID: "delcln_delete_" + suffix,
		Reason: string(sandbox.ReleaseReasonDelete), IdempotencyKey: "session_delete_cleanup:default:sesn_delete_" + suffix + ":delcln_delete_" + suffix,
	}
	seed := request
	seed.BindingID = "bind_seed_" + suffix
	seed.BindingGeneration = 1
	seedReleaseHandlerClaimedSandbox(t, db, seed)
	if _, err := db.Exec(`UPDATE sessions SET lifecycle_state='deleted',delete_cleanup_id=$2 WHERE id=$1`, request.SessionID, request.DeleteCleanupID); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}
	if _, err := db.Exec(`UPDATE session_preparations SET preparation_attempt_id=$2 WHERE session_id=$1`, request.SessionID, request.PreparationAttemptID); err != nil {
		t.Fatalf("stamp preparation attempt: %v", err)
	}
	if _, err := db.Exec(`UPDATE session_runtime_status SET binding_id=NULL,binding_generation=NULL WHERE session_id=$1`, request.SessionID); err != nil {
		t.Fatalf("clear runtime binding: %v", err)
	}
	return request
}

func newReleaseHandlerTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func assertReleaseIdempotencyStatus(t *testing.T, db *sql.DB, request ReleaseSandboxRequest, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow(
		`SELECT status
		   FROM sandbox_release_idempotency_keys
		  WHERE workspace_id = $1
		    AND idempotency_key = $2`,
		request.WorkspaceID,
		request.IdempotencyKey,
	).Scan(&got); err != nil {
		t.Fatalf("read release idempotency status: %v", err)
	}
	if got != want {
		t.Fatalf("release idempotency status = %q; want %q", got, want)
	}
}

func assertReleaseIdempotencyResult(t *testing.T, db *sql.DB, request ReleaseSandboxRequest, wantStatus string, wantResponse string, wantSandbox string) {
	t.Helper()
	var status, responseStatus, sandboxStatus string
	if err := db.QueryRow(
		`SELECT status, response_status, sandbox_status
		   FROM sandbox_release_idempotency_keys
		  WHERE workspace_id = $1 AND idempotency_key = $2`,
		request.WorkspaceID, request.IdempotencyKey,
	).Scan(&status, &responseStatus, &sandboxStatus); err != nil {
		t.Fatalf("read release idempotency result: %v", err)
	}
	if status != wantStatus || responseStatus != wantResponse || sandboxStatus != wantSandbox {
		t.Fatalf("release idempotency = %q/%q/%q; want %q/%q/%q", status, responseStatus, sandboxStatus, wantStatus, wantResponse, wantSandbox)
	}
}

func assertReleaseSandboxRow(t *testing.T, db *sql.DB, sandboxID string, wantStatus string, wantCleanupStatus string) {
	t.Helper()
	var status, cleanupStatus string
	if err := db.QueryRow(`SELECT status, cleanup_status FROM sandboxes WHERE id=$1`, sandboxID).Scan(&status, &cleanupStatus); err != nil {
		t.Fatalf("read release sandbox row: %v", err)
	}
	if status != wantStatus || cleanupStatus != wantCleanupStatus {
		t.Fatalf("sandbox row = %q/%q; want %q/%q", status, cleanupStatus, wantStatus, wantCleanupStatus)
	}
}

func seedReleaseHandlerClaimedSandbox(t *testing.T, db *sql.DB, request ReleaseSandboxRequest) {
	t.Helper()
	createdAt := "2026-07-01T12:00:00Z"
	if _, err := db.Exec(
		`INSERT INTO agents (workspace_id, id, name, description, version, created_at, updated_at)
		 VALUES ($1, 'agent_release_handler', 'Release Handler Agent', '', 1, $2, $2)
		 ON CONFLICT (id) DO NOTHING`,
		request.WorkspaceID, createdAt,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agentver_release_handler', 'agent_release_handler', 1, '{}', 'hash_release_handler', $2)
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		request.WorkspaceID, createdAt,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO environments (workspace_id, id, name, description, config_json, metadata_json, created_at, updated_at)
		 VALUES ($1, 'env_release_handler', 'Release Handler Env', '', '{"type":"container","networking":{"type":"unrestricted"},"packages":{}}', '{}', $2, $2)
		 ON CONFLICT (id) DO NOTHING`,
		request.WorkspaceID, createdAt,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, agent_id, agent_version,
			environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, 'session', NULL, '{}', 'idle', 'agent_release_handler', 1, 'env_release_handler', '[]', $3, $3)`,
		request.WorkspaceID, request.SessionID, createdAt,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	var hasMachineWasUsable bool
	if err := db.QueryRow(
		`SELECT EXISTS (
			SELECT 1
			  FROM information_schema.columns
			 WHERE table_schema = current_schema()
			   AND table_name = 'sandboxes'
			   AND column_name = 'machine_was_usable'
		)`,
	).Scan(&hasMachineWasUsable); err != nil {
		t.Fatalf("inspect sandbox schema: %v", err)
	}
	insertSandbox := `INSERT INTO sandboxes (
		workspace_id, id, session_id, status, provider, provider_sandbox_id,
		created_at, updated_at, status_refreshed_at
	) VALUES ($1, $2, $3, 'active', 'tetral', 'provider_release_handler', $4, $4, $4)`
	if hasMachineWasUsable {
		insertSandbox = `INSERT INTO sandboxes (
			workspace_id, id, session_id, status, provider, provider_sandbox_id,
			machine_was_usable, created_at, updated_at, status_refreshed_at
		) VALUES ($1, $2, $3, 'active', 'tetral', 'provider_release_handler', TRUE, $4, $4, $4)`
	}
	if _, err := db.Exec(
		insertSandbox,
		request.WorkspaceID, request.SandboxID, request.SessionID, createdAt,
	); err != nil {
		t.Fatalf("seed sandbox: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at, ready_at
		) VALUES ($1, $2, 'prep_release_handler', 'env_release_handler', 1, $3, 'ready', $4, $4, $4)`,
		request.WorkspaceID, request.SessionID, request.SandboxID, createdAt,
	); err != nil {
		t.Fatalf("seed preparation: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, idle_since, cleanup_after, cleanup_enqueued_at,
			cleanup_claimed_at, cleanup_job_id, binding_id, binding_generation, created_at, updated_at
		) VALUES ($1, $2, 'idle', $5, $5, $5, $5, 'cleanup_1', $3, $4, $5, $5)`,
		request.WorkspaceID, request.SessionID, request.BindingID, request.BindingGeneration, createdAt,
	); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
}

type recordingReleaseProvider struct {
	releaseCalls   int
	releaseReasons []sandbox.ReleaseReason
	statusCalls    int
	releaseErr     error
	status         sandbox.ProviderStatus
	statusErr      error
}

type failOnceCompleteReleaseStore struct {
	sandbox.Store
	failures int
}

func (s *failOnceCompleteReleaseStore) MarkArchived(ctx context.Context, ws workspace.ID, sandboxID string, archivedAt time.Time) (*sandbox.Sandbox, error) {
	if s.failures > 0 {
		s.failures--
		return nil, errors.New("forced release finalization failure")
	}
	return s.Store.MarkArchived(ctx, ws, sandboxID, archivedAt)
}

func (p *recordingReleaseProvider) CreateSandbox(_ context.Context, _ sandbox.CreateSandboxRequest) (sandbox.ProviderHandle, error) {
	return sandbox.ProviderHandle{Provider: "tetral", SandboxID: "provider_release_handler"}, nil
}

func (p *recordingReleaseProvider) StartSandbox(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *recordingReleaseProvider) CheckBaseTemplateHealth(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *recordingReleaseProvider) ApplyNetworkPolicy(context.Context, sandbox.ProviderHandle, sandbox.NetworkSetup) error {
	return nil
}

func (p *recordingReleaseProvider) PrepareBaseDirectories(context.Context, sandbox.ProviderHandle) error {
	return nil
}

func (p *recordingReleaseProvider) GetStatus(context.Context, sandbox.ProviderHandle) (sandbox.ProviderStatus, error) {
	p.statusCalls++
	if p.statusErr != nil {
		return sandbox.ProviderStatus{}, p.statusErr
	}
	if p.status.Availability != "" {
		return p.status, nil
	}
	return sandbox.ProviderStatus{Availability: sandbox.ProviderAvailable, SandboxStatus: sandbox.StatusActive}, nil
}

func (p *recordingReleaseProvider) ReleaseSandbox(_ context.Context, _ sandbox.ProviderHandle, reason sandbox.ReleaseReason) error {
	p.releaseCalls++
	p.releaseReasons = append(p.releaseReasons, reason)
	return p.releaseErr
}
