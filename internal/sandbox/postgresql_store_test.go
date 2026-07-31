package sandbox

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const testPostgreSQLSandboxProvider = "tetral"

func TestPostgreSQLStorePersistsProviderHandleAndReleaseStates(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_pg")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	createdAt := time.Date(2026, 5, 22, 11, 0, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_pg",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_pg",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	handle := ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_pg",
		Metadata:  map[string]string{"region": "iad", "pool": "default"},
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_pg", handle, createdAt.Add(time.Minute)); err != nil {
		t.Fatalf("SaveProviderHandle: %v", err)
	}
	active, err := store.MarkActive(ctx, workspace.DefaultID, "sandbox_pg", createdAt.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("MarkActive: %v", err)
	}
	if active.Status != StatusActive || active.ProviderHandle.SandboxID != "provider_pg" || active.ProviderMetadata["region"] != "iad" ||
		active.EnvironmentID != "env_sandbox" || active.EnvironmentGeneration != 1 {
		t.Fatalf("active sandbox = %+v", active)
	}

	live, err := store.FindLiveBySessionID(ctx, workspace.DefaultID, "sesn_pg")
	if err != nil {
		t.Fatalf("FindLiveBySessionID: %v", err)
	}
	if live.ID != "sandbox_pg" || live.Status != StatusActive || live.ProviderHandle.SandboxID != "provider_pg" {
		t.Fatalf("live sandbox = %+v", live)
	}

	releasing, err := store.MarkReleasing(ctx, workspace.DefaultID, "sandbox_pg", createdAt.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("MarkReleasing: %v", err)
	}
	if releasing.Status != StatusReleasing || releasing.ProviderHandle.SandboxID != "provider_pg" {
		t.Fatalf("releasing sandbox = %+v", releasing)
	}

	released, err := store.MarkReleased(ctx, workspace.DefaultID, "sandbox_pg", createdAt.Add(4*time.Minute))
	if err != nil {
		t.Fatalf("MarkReleased: %v", err)
	}
	if released.Status != StatusReleased || released.ReleasedAt == nil || released.ProviderHandle.SandboxID != "provider_pg" {
		t.Fatalf("released sandbox = %+v", released)
	}
	if _, err := store.FindLiveBySessionID(ctx, workspace.DefaultID, "sesn_pg"); err == nil {
		t.Fatal("FindLiveBySessionID found released sandbox; want not found")
	} else {
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("err = %T %v; want NotFoundError", err, err)
		}
	}
}

func TestPostgreSQLStoreDeleteReleaseSettlesSupersededStartupCleanup(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	oldLastAttempt := base.Add(-2 * time.Minute)
	oldNextAttempt := base.Add(time.Minute)
	releasedAt := base.Add(2 * time.Minute)

	for _, writer := range []struct {
		name            string
		providerMissing bool
	}{
		{name: "recorded handle"},
		{name: "provider missing", providerMissing: true},
	} {
		for _, cleanupStatus := range []CleanupStatus{
			CleanupStatusPending,
			CleanupStatusInProgress,
			CleanupStatusRetryableFailed,
			CleanupStatusPermanentFailed,
			CleanupStatusReleased,
		} {
			t.Run(writer.name+"/"+string(cleanupStatus), func(t *testing.T) {
				suffix := strings.ReplaceAll(writer.name+"_"+string(cleanupStatus), " ", "_")
				sessionID := "sesn_delete_settle_" + suffix
				sandboxID := "sandbox_delete_settle_" + suffix
				seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
				if err := store.CreateSandbox(ctx, &Sandbox{
					ID:                   sandboxID,
					WorkspaceID:          workspace.DefaultID,
					SessionID:            sessionID,
					Status:               StatusFailed,
					Provider:             testPostgreSQLSandboxProvider,
					StartupFailureReason: "startup_interrupted",
					CleanupStatus:        CleanupStatusPending,
					CreatedAt:            base,
					UpdatedAt:            base,
					FailedAt:             timePtr(base),
				}); err != nil {
					t.Fatalf("CreateSandbox: %v", err)
				}

				var providerSandboxID any = "provider_" + suffix
				if writer.providerMissing {
					providerSandboxID = nil
				}
				seedCleanupStatus := cleanupStatus
				if cleanupStatus == CleanupStatusInProgress {
					seedCleanupStatus = CleanupStatusPending
				}
				if _, err := admin.ExecContext(ctx,
					`UPDATE sandboxes
					    SET status = $2,
					        provider_sandbox_id = $3,
					        cleanup_status = $4,
					        cleanup_method = 'cleanup',
					        cleanup_error_kind = 'old_error',
					        cleanup_retryable = TRUE,
					        cleanup_last_attempt_at = $5,
					        cleanup_next_attempt_at = $6,
					        cleanup_attempt_count = 0,
					        cleanup_lease_token = $7,
					        cleanup_lease_expires_at = $8
					  WHERE workspace_id = $1
					    AND id = $9`,
					string(workspace.DefaultID),
					string(StatusFailed),
					providerSandboxID,
					string(seedCleanupStatus),
					oldLastAttempt,
					oldNextAttempt,
					nil,
					nil,
					sandboxID,
				); err != nil {
					t.Fatalf("seed delete settlement state: %v", err)
				}
				if cleanupStatus == CleanupStatusInProgress {
					claim, claimErr := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, 2*time.Minute, 20)
					if claimErr != nil {
						t.Fatalf("ClaimDueStartupCleanup: %v", claimErr)
					}
					if claim.Sandbox.CleanupStatus != CleanupStatusInProgress ||
						claim.Sandbox.CleanupLeaseToken == "" ||
						claim.Sandbox.CleanupLeaseExpiresAt == nil {
						t.Fatalf("startup cleanup claim = %+v; want held lease", claim)
					}
				}

				var (
					got *Sandbox
					err error
				)
				if writer.providerMissing {
					got, err = store.MarkReleasedForDeleteProviderMissing(ctx, workspace.DefaultID, sandboxID, releasedAt)
				} else {
					releasing, claimErr := store.MarkReleasingForDelete(ctx, workspace.DefaultID, sandboxID, releasedAt.Add(-time.Second))
					if claimErr != nil {
						t.Fatalf("MarkReleasingForDelete: %v", claimErr)
					}
					if releasing.Status != StatusReleasing || releasing.CleanupStatus != cleanupStatus {
						t.Fatalf("delete claim = %+v; want releasing with cleanup state preserved", releasing)
					}
					got, err = store.MarkReleased(ctx, workspace.DefaultID, sandboxID, releasedAt)
				}
				if err != nil {
					t.Fatalf("delete release: %v", err)
				}
				if got.Status != StatusReleased {
					t.Fatalf("status = %q; want released", got.Status)
				}

				switch cleanupStatus {
				case CleanupStatusPending, CleanupStatusInProgress, CleanupStatusRetryableFailed:
					if got.CleanupStatus != CleanupStatusReleased ||
						got.CleanupMethod != string(ReleaseReasonDelete) ||
						got.CleanupErrorKind != "" ||
						got.CleanupRetryable ||
						got.CleanupLastAttemptAt == nil ||
						!got.CleanupLastAttemptAt.Equal(releasedAt) ||
						got.CleanupNextAttemptAt != nil ||
						got.CleanupAttemptCount != 1 ||
						got.CleanupLeaseToken != "" ||
						got.CleanupLeaseExpiresAt != nil {
						t.Fatalf("settled cleanup = %+v; want full delete settlement", got)
					}
				case CleanupStatusPermanentFailed, CleanupStatusReleased:
					if got.CleanupStatus != cleanupStatus ||
						got.CleanupMethod != string(ReleaseReasonCleanup) ||
						got.CleanupErrorKind != "old_error" ||
						!got.CleanupRetryable ||
						got.CleanupLastAttemptAt == nil ||
						!got.CleanupLastAttemptAt.Equal(oldLastAttempt) ||
						got.CleanupNextAttemptAt == nil ||
						!got.CleanupNextAttemptAt.Equal(oldNextAttempt) ||
						got.CleanupAttemptCount != 0 {
						t.Fatalf("preserved cleanup = %+v; want terminal verdict unchanged", got)
					}
				}
			})
		}
	}

	var terminalLeaseCount int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*)
		   FROM sandboxes
		  WHERE status IN ('releasing', 'released')
		    AND cleanup_lease_token IS NOT NULL`,
	).Scan(&terminalLeaseCount); err != nil {
		t.Fatalf("count terminal lease markers: %v", err)
	}
	if terminalLeaseCount != 0 {
		t.Fatalf("terminal rows carrying cleanup lease tokens = %d; want 0", terminalLeaseCount)
	}
}

func TestPostgreSQLStoreWakeRefreshCannotSupersedeDeleteReleaseOwner(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	const (
		sessionID = "sesn_wake_refresh_delete_owner"
		sandboxID = "sandbox_wake_refresh_delete_owner"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		Status: StatusFailed, Provider: testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending,
		CreatedAt: base.Add(-2 * time.Minute), UpdatedAt: base.Add(-2 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, sandboxID, ProviderHandle{
		Provider: testPostgreSQLSandboxProvider, SandboxID: "provider_wake_refresh_delete_owner",
	}, base.Add(-2*time.Minute)); err != nil {
		t.Fatalf("SaveProviderHandle: %v", err)
	}
	claim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base.Add(-time.Minute), 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup: %v", err)
	}
	releasing, err := store.MarkReleasingForDelete(ctx, workspace.DefaultID, sandboxID, base)
	if err != nil {
		t.Fatalf("MarkReleasingForDelete: %v", err)
	}
	if releasing.Status != StatusReleasing || releasing.CleanupStatus != CleanupStatusInProgress {
		t.Fatalf("delete owner = %+v; want releasing/in_progress", releasing)
	}

	for _, staleStatus := range []Status{StatusReleased, StatusArchived} {
		if _, err := store.RefreshSandboxState(ctx, workspace.DefaultID, sandboxID, staleStatus, base.Add(time.Second)); err == nil {
			t.Fatalf("stale %s wake refresh superseded delete release owner", staleStatus)
		} else {
			var notFound *NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("%s refresh error = %T %v; want NotFoundError", staleStatus, err, err)
			}
		}
	}
	got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("FindLatestBySessionID: %v", err)
	}
	if got.Status != StatusReleasing ||
		got.CleanupStatus != CleanupStatusInProgress ||
		got.CleanupLeaseToken != claim.Sandbox.CleanupLeaseToken ||
		got.CleanupLeaseExpiresAt == nil {
		t.Fatalf("stale wake refresh changed delete owner = %+v", got)
	}
}

func TestPostgreSQLStoreWakeFailureReleasedRefreshPreservesCleanupFields(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)
	const (
		sessionID = "sesn_wake_failure_cleanup_noop"
		sandboxID = "sandbox_wake_failure_cleanup_noop"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		Status: StatusActive, Provider: testPostgreSQLSandboxProvider,
		CleanupStatus: CleanupStatusPermanentFailed, CleanupMethod: string(ReleaseReasonCleanup),
		CleanupErrorKind: "archive_failed", CleanupRetryable: false, CleanupAttemptCount: 3,
		CleanupLastAttemptAt: timePtr(base.Add(-time.Minute)),
		CleanupNextAttemptAt: timePtr(base.Add(time.Minute)),
		CreatedAt:            base.Add(-2 * time.Minute),
		UpdatedAt:            base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}

	got, err := store.RefreshSandboxState(ctx, workspace.DefaultID, sandboxID, StatusReleased, base)
	if err != nil {
		t.Fatalf("RefreshSandboxState released: %v", err)
	}
	if got.Status != StatusReleased ||
		got.CleanupStatus != CleanupStatusPermanentFailed ||
		got.CleanupMethod != string(ReleaseReasonCleanup) ||
		got.CleanupErrorKind != "archive_failed" ||
		got.CleanupRetryable ||
		got.CleanupAttemptCount != 3 ||
		got.CleanupLastAttemptAt == nil ||
		!got.CleanupLastAttemptAt.Equal(base.Add(-time.Minute)) ||
		got.CleanupNextAttemptAt == nil ||
		!got.CleanupNextAttemptAt.Equal(base.Add(time.Minute)) ||
		got.CleanupLeaseToken != "" ||
		got.CleanupLeaseExpiresAt != nil {
		t.Fatalf("wake-failure release = %+v; want cleanup fields unchanged", got)
	}
}

func TestPostgreSQLStoreMarkReleasingRejectsInFlightLifecycleStates(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name   string
		status Status
	}{
		{name: "creating", status: StatusCreating},
		{name: "resuming", status: StatusResuming},
		{name: "releasing", status: StatusReleasing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := "sesn_mark_releasing_" + tc.name
			sandboxID := "sandbox_mark_releasing_" + tc.name
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			if err := store.CreateSandbox(ctx, &Sandbox{
				ID:          sandboxID,
				WorkspaceID: workspace.DefaultID,
				SessionID:   sessionID,
				Status:      StatusCreating,
				Provider:    testPostgreSQLSandboxProvider,
				CreatedAt:   base,
				UpdatedAt:   base,
			}); err != nil {
				t.Fatalf("CreateSandbox: %v", err)
			}
			if tc.status != StatusCreating {
				if _, err := admin.ExecContext(ctx,
					`UPDATE sandboxes
					    SET status = $1, updated_at = $2
					  WHERE workspace_id = $3
					    AND id = $4`,
					string(tc.status),
					base.Add(time.Minute),
					string(workspace.DefaultID),
					sandboxID,
				); err != nil {
					t.Fatalf("seed sandbox status %s: %v", tc.status, err)
				}
			}

			_, err := store.MarkReleasing(ctx, workspace.DefaultID, sandboxID, base.Add(2*time.Minute))
			var notFound *NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("MarkReleasing from %s err = %T %v; want NotFoundError", tc.status, err, err)
			}
			var gotStatus string
			if err := admin.QueryRowContext(ctx,
				`SELECT status
				   FROM sandboxes
				  WHERE workspace_id = $1
				    AND id = $2`,
				string(workspace.DefaultID),
				sandboxID,
			).Scan(&gotStatus); err != nil {
				t.Fatalf("read sandbox status: %v", err)
			}
			if gotStatus != string(tc.status) {
				t.Fatalf("sandbox status = %q; want unchanged %q", gotStatus, tc.status)
			}
		})
	}
}

func TestPostgreSQLStoreMarkActiveOnlyTransitionsStartupStates(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	for _, status := range []Status{StatusActive, StatusStopped, StatusArchived, StatusReleasing, StatusReleased, StatusFailed} {
		sessionID := "sesn_mark_active_guard_" + string(status)
		sandboxID := "sandbox_mark_active_guard_" + string(status)
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID, Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base}); err != nil {
			t.Fatalf("CreateSandbox %s: %v", status, err)
		}
		if _, err := admin.ExecContext(ctx,
			`UPDATE sandboxes
			    SET status = $2,
			        machine_was_usable = CASE WHEN $2 = 'active' THEN TRUE ELSE machine_was_usable END
			  WHERE id = $1`,
			sandboxID, string(status),
		); err != nil {
			t.Fatalf("seed status %s: %v", status, err)
		}
		if _, err := store.MarkActive(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute)); err == nil {
			t.Fatalf("MarkActive accepted %s; want guarded rejection", status)
		}
	}
}

func TestPostgreSQLStoreEveryActiveWriterRecordsMachineUsability(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

	assertUsable := func(t *testing.T, sandboxID string, got *Sandbox, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("active writer: %v", err)
		}
		if got == nil || got.Status != StatusActive || !got.MachineWasUsable {
			t.Fatalf("active writer result = %+v; want active with machine_was_usable", got)
		}
		var persisted bool
		if err := admin.QueryRowContext(ctx,
			`SELECT machine_was_usable
			   FROM sandboxes
			  WHERE workspace_id = $1 AND id = $2`,
			string(workspace.DefaultID), sandboxID,
		).Scan(&persisted); err != nil {
			t.Fatalf("read machine usability: %v", err)
		}
		if !persisted {
			t.Fatal("persisted machine_was_usable = false; want true")
		}
	}

	t.Run("active insert", func(t *testing.T) {
		const sessionID = "sesn_active_fact_insert"
		const sandboxID = "sandbox_active_fact_insert"
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		err := store.CreateSandbox(ctx, &Sandbox{
			ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		})
		if err != nil {
			t.Fatalf("CreateSandbox active: %v", err)
		}
		got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
		assertUsable(t, sandboxID, got, err)
	})

	t.Run("readiness completion", func(t *testing.T) {
		const sessionID = "sesn_active_fact_completion"
		const sandboxID = "sandbox_active_fact_completion"
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{
			ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.MarkActive(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute))
		assertUsable(t, sandboxID, got, err)
	})

	t.Run("provider state refresh", func(t *testing.T) {
		const sessionID = "sesn_active_fact_refresh"
		const sandboxID = "sandbox_active_fact_refresh"
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{
			ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.RefreshSandboxState(ctx, workspace.DefaultID, sandboxID, StatusActive, base.Add(time.Minute))
		assertUsable(t, sandboxID, got, err)
	})

	t.Run("stale startup adoption refresh", func(t *testing.T) {
		const sessionID = "sesn_active_fact_stale"
		const sandboxID = "sandbox_active_fact_stale"
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{
			ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.RefreshStaleStartup(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute), StaleStartupRefreshUpdate{
			RefreshedAt: base.Add(2 * time.Minute),
			Status:      StatusActive,
		})
		assertUsable(t, sandboxID, got, err)
	})

	t.Run("provider refresh cannot reclaim release owner", func(t *testing.T) {
		const sessionID = "sesn_active_fact_release_owner"
		const sandboxID = "sandbox_active_fact_release_owner"
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{
			ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		if _, err := store.MarkReleasing(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute)); err != nil {
			t.Fatalf("MarkReleasing: %v", err)
		}
		if _, err := store.RefreshSandboxState(ctx, workspace.DefaultID, sandboxID, StatusActive, base.Add(2*time.Minute)); err == nil {
			t.Fatal("RefreshSandboxState reclaimed releasing row")
		}
		released, err := store.MarkReleased(ctx, workspace.DefaultID, sandboxID, base.Add(3*time.Minute))
		if err != nil {
			t.Fatalf("MarkReleased after rejected refresh: %v", err)
		}
		if released.Status != StatusReleased {
			t.Fatalf("release owner result = %+v; want released", released)
		}
	})
}

func TestPostgreSQLStoreStaleReconcilerClaimWinsLateMarkActiveRace(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	const sessionID = "sesn_mark_active_reconciler_race"
	const sandboxID = "sandbox_mark_active_reconciler_race"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID, Status: StatusCreating,
		Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, sandboxID, ProviderHandle{Provider: testPostgreSQLSandboxProvider, SandboxID: "provider_reconciler_race"}, base); err != nil {
		t.Fatalf("SaveProviderHandle: %v", err)
	}
	claimed, err := store.ClaimStaleCreating(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute), StartupFailureUpdate{
		FailedAt: base.Add(2 * time.Minute), StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending,
	})
	if err != nil {
		t.Fatalf("ClaimStaleCreating: %v", err)
	}
	if claimed.Status != StatusFailed || claimed.CleanupStatus != CleanupStatusPending {
		t.Fatalf("reconciler claim = %+v; want failed/pending cleanup owner", claimed)
	}
	if _, err := store.MarkActive(ctx, workspace.DefaultID, sandboxID, base.Add(3*time.Minute)); err == nil {
		t.Fatal("late MarkActive won after stale reconciler claim")
	}
	var status, cleanupStatus, providerID string
	if err := admin.QueryRowContext(ctx, `SELECT status,cleanup_status,provider_sandbox_id FROM sandboxes WHERE id=$1`, sandboxID).Scan(&status, &cleanupStatus, &providerID); err != nil {
		t.Fatalf("read race result: %v", err)
	}
	if status != "failed" || cleanupStatus != "pending" || providerID != "provider_reconciler_race" {
		t.Fatalf("race result=%q/%q/%q; want failed/pending with provider identity retained", status, cleanupStatus, providerID)
	}
}

func TestPostgreSQLStoreMarkReleasingClaimsFailedRowsWithPendingCleanup(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 12, 10, 0, 0, time.UTC)
	failedAt := base.Add(-time.Minute)

	for _, tc := range []struct {
		name          string
		reason        string
		cleanupStatus CleanupStatus
		wantClaim     bool
	}{
		{name: "mount failed pending", reason: string(SandboxErrorMountFailed), cleanupStatus: CleanupStatusPending, wantClaim: true},
		{name: "startup interrupted retryable", reason: "startup_interrupted", cleanupStatus: CleanupStatusRetryableFailed, wantClaim: true},
		{name: "mount failed already released", reason: string(SandboxErrorMountFailed), cleanupStatus: CleanupStatusReleased, wantClaim: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sessionID := "sesn_failed_releasing_" + strings.ReplaceAll(tc.name, " ", "_")
			sandboxID := "sandbox_failed_releasing_" + strings.ReplaceAll(tc.name, " ", "_")
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			if err := store.CreateSandbox(ctx, &Sandbox{
				ID:                   sandboxID,
				WorkspaceID:          workspace.DefaultID,
				SessionID:            sessionID,
				Status:               StatusFailed,
				Provider:             testPostgreSQLSandboxProvider,
				StartupFailureReason: tc.reason,
				CleanupStatus:        tc.cleanupStatus,
				CreatedAt:            base.Add(-2 * time.Minute),
				UpdatedAt:            failedAt,
				FailedAt:             &failedAt,
			}); err != nil {
				t.Fatalf("CreateSandbox: %v", err)
			}

			got, err := store.MarkReleasing(ctx, workspace.DefaultID, sandboxID, base)
			if tc.wantClaim {
				if err != nil {
					t.Fatalf("MarkReleasing failed row: %v", err)
				}
				if got.Status != StatusReleasing {
					t.Fatalf("failed row status = %s; want releasing", got.Status)
				}
				return
			}

			var notFound *NotFoundError
			if !errors.As(err, &notFound) {
				t.Fatalf("MarkReleasing terminal cleanup err = %T %v; want NotFoundError", err, err)
			}
			latest, latestErr := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
			if latestErr != nil {
				t.Fatalf("FindLatestBySessionID: %v", latestErr)
			}
			if latest.Status != StatusFailed || latest.CleanupStatus != tc.cleanupStatus {
				t.Fatalf("terminal cleanup row = %+v; want failed/%s unchanged", latest, tc.cleanupStatus)
			}
		})
	}
}

func TestPostgreSQLStoreMarkReleasingExcludesStartupCleanupLeaseHolder(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 12, 10, 0, 0, time.UTC)
	const (
		sessionID = "sesn_failed_releasing_in_progress"
		sandboxID = "sandbox_failed_releasing_in_progress"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		Status: StatusFailed, Provider: testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending,
		CreatedAt: base.Add(-2 * time.Minute), UpdatedAt: base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	claim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup: %v", err)
	}
	if _, err := store.MarkReleasing(ctx, workspace.DefaultID, sandboxID, base.Add(time.Second)); err == nil {
		t.Fatal("MarkReleasing claimed a row held by startup cleanup")
	}
	latest, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("FindLatestBySessionID: %v", err)
	}
	if latest.CleanupStatus != CleanupStatusInProgress ||
		latest.CleanupLeaseToken != claim.Sandbox.CleanupLeaseToken ||
		latest.Status != StatusFailed {
		t.Fatalf("leased cleanup row changed through release surface: %+v", latest)
	}
}

func TestPostgreSQLStoreReadsMemoryStoreSnapshot(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_memory_snapshot")
	seedSandboxMemoryStore(t, admin, string(workspace.DefaultID), "memstore_snapshot")
	seedSandboxMemory(t, admin, string(workspace.DefaultID), "memstore_snapshot", "mem_snapshot_a", "/a.md", "alpha", false)
	seedSandboxMemory(t, admin, string(workspace.DefaultID), "memstore_snapshot", "mem_snapshot_b", "/b.md", "beta", false)
	seedSandboxMemory(t, admin, string(workspace.DefaultID), "memstore_snapshot", "mem_snapshot_deleted", "/deleted.md", "gone", true)

	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	files, err := store.ReadMemoryStoreSnapshot(ctx, workspace.DefaultID, "memstore_snapshot")
	if err != nil {
		t.Fatalf("ReadMemoryStoreSnapshot: %v", err)
	}
	want := []MemorySnapshotFile{
		{Path: "/a.md", Content: "alpha", ContentSHA256: sandboxTestSHA256Hex("alpha")},
		{Path: "/b.md", Content: "beta", ContentSHA256: sandboxTestSHA256Hex("beta")},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("snapshot = %+v; want %+v", files, want)
	}
}

func TestPostgreSQLStoreWithMemoryStoreMutationLocksHoldsLockThroughCallback(t *testing.T) {
	runtime, _ := newSandboxStoreTestDB(t)
	ctx := context.Background()
	client := dbconnect.NewClientForTesting(runtime)
	store := NewPostgreSQLStore(client)

	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.WithMemoryStoreMutationLocks(ctx, workspace.DefaultID, []string{"memstore_b", "memstore_a", "memstore_a"}, func(context.Context) error {
			close(locked)
			<-release
			return nil
		})
	}()

	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for memory-store lock callback")
	}

	err := client.WithWorkspaceTx(ctx, string(workspace.DefaultID), "sandbox.test_blocked_memory_store_lock", func(tx *dbconnect.Tx) error {
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '50ms'"); err != nil {
			return err
		}
		return storage.AcquireMemoryStoreMutationLock(ctx, tx, string(workspace.DefaultID), "memstore_a")
	})
	if err == nil {
		close(release)
		t.Fatal("second transaction acquired memory store mutation lock while callback held it")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("WithMemoryStoreMutationLocks: %v", err)
	}
}

func TestPostgreSQLStoreMarkFailedRemovesSandboxFromLiveLookup(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_failed")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	createdAt := time.Date(2026, 5, 22, 11, 30, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_failed",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_failed",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	failedAt := createdAt.Add(time.Minute)
	failed, err := store.MarkFailed(ctx, workspace.DefaultID, "sandbox_failed", failedAt)
	if err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	if failed.Status != StatusFailed || failed.FailedAt == nil || !failed.FailedAt.Equal(failedAt) {
		t.Fatalf("failed sandbox = %+v; want failed_at %s", failed, failedAt)
	}
	if _, err := store.FindLiveBySessionID(ctx, workspace.DefaultID, "sesn_failed"); err == nil {
		t.Fatal("FindLiveBySessionID found failed sandbox; want not found")
	} else {
		var notFound *NotFoundError
		if !errors.As(err, &notFound) {
			t.Fatalf("err = %T %v; want NotFoundError", err, err)
		}
	}
}

func TestPostgreSQLStoreClaimStaleCreatingOnlyClaimsExpiredCreatingRows(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 11, 40, 0, 0, time.UTC)

	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_stale_claim")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_stale_claim",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_stale_claim",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base,
		UpdatedAt:   base,
	}); err != nil {
		t.Fatalf("CreateSandbox stale: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_stale_claim", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_stale_claim",
		Metadata:  map[string]string{"region": "iad"},
	}, base); err != nil {
		t.Fatalf("SaveProviderHandle stale: %v", err)
	}

	claimed, err := store.ClaimStaleCreating(ctx, workspace.DefaultID, "sandbox_stale_claim", base.Add(time.Minute), StartupFailureUpdate{
		FailedAt:             base.Add(2 * time.Minute),
		StartupFailureReason: "startup_interrupted",
		CleanupStatus:        CleanupStatusPending,
	})
	if err != nil {
		t.Fatalf("ClaimStaleCreating stale: %v", err)
	}
	if claimed.Status != StatusFailed || claimed.StartupFailureReason != "startup_interrupted" || claimed.CleanupStatus != CleanupStatusPending {
		t.Fatalf("claimed stale sandbox = %+v; want failed startup_interrupted pending", claimed)
	}
	if claimed.ProviderHandle.SandboxID != "provider_stale_claim" {
		t.Fatalf("provider handle = %+v; want persisted handle available for cleanup", claimed.ProviderHandle)
	}

	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_fresh_claim")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_fresh_claim",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_fresh_claim",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base,
		UpdatedAt:   base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox fresh: %v", err)
	}
	if _, err := store.ClaimStaleCreating(ctx, workspace.DefaultID, "sandbox_fresh_claim", base.Add(time.Minute), StartupFailureUpdate{
		FailedAt:             base.Add(4 * time.Minute),
		StartupFailureReason: "startup_interrupted",
		CleanupStatus:        CleanupStatusPending,
	}); err == nil {
		t.Fatal("ClaimStaleCreating fresh row succeeded; want not found")
	}
	fresh, err := store.FindLiveBySessionID(ctx, workspace.DefaultID, "sesn_fresh_claim")
	if err != nil {
		t.Fatalf("FindLiveBySessionID fresh: %v", err)
	}
	if fresh.Status != StatusCreating {
		t.Fatalf("fresh sandbox status = %s; want still creating", fresh.Status)
	}
}

func TestPostgreSQLStoreClaimDueStartupCleanupOnlyClaimsPendingOrDueRetryableRows(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 11, 50, 0, 0, time.UTC)

	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_cleanup_pending")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                   "sandbox_cleanup_pending",
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_cleanup_pending",
		Status:               StatusFailed,
		Provider:             testPostgreSQLSandboxProvider,
		StartupFailureReason: string(SandboxErrorBaseTemplateFailed),
		CleanupStatus:        CleanupStatusPending,
		CreatedAt:            base.Add(-10 * time.Minute),
		UpdatedAt:            base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox pending: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_cleanup_pending", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_cleanup_pending",
		Metadata:  map[string]string{"region": "iad"},
	}, base); err != nil {
		t.Fatalf("SaveProviderHandle pending: %v", err)
	}
	pendingClaim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, "sandbox_cleanup_pending", base, 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup pending: %v", err)
	}
	pending := pendingClaim.Sandbox
	if !pendingClaim.ProviderAttemptReserved || pending.CleanupStatus != CleanupStatusInProgress ||
		pending.CleanupAttemptCount != 1 || pending.CleanupLeaseToken == "" ||
		pending.CleanupLeaseExpiresAt == nil || !pending.CleanupLeaseExpiresAt.Equal(base.Add(2*time.Minute)) ||
		pending.ProviderHandle.SandboxID != "provider_cleanup_pending" {
		t.Fatalf("pending cleanup claim = %+v; want one leased provider attempt", pendingClaim)
	}

	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_cleanup_due")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                   "sandbox_cleanup_due",
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_cleanup_due",
		Status:               StatusFailed,
		Provider:             testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted",
		CleanupStatus:        CleanupStatusRetryableFailed,
		CleanupRetryable:     true,
		CleanupNextAttemptAt: timePtr(base.Add(-time.Minute)),
		CreatedAt:            base.Add(-10 * time.Minute),
		UpdatedAt:            base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox due: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_cleanup_due", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_cleanup_due",
		Metadata:  map[string]string{"region": "iad"},
	}, base); err != nil {
		t.Fatalf("SaveProviderHandle due: %v", err)
	}
	dueClaim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, "sandbox_cleanup_due", base, 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup due: %v", err)
	}
	claimed := dueClaim.Sandbox
	if !dueClaim.ProviderAttemptReserved || claimed.CleanupStatus != CleanupStatusInProgress ||
		claimed.CleanupRetryable || claimed.CleanupNextAttemptAt != nil || claimed.CleanupAttemptCount != 1 {
		t.Fatalf("claimed cleanup = %+v; want in-progress lease with retry fields cleared", dueClaim)
	}
	if claimed.ProviderHandle.SandboxID != "provider_cleanup_due" {
		t.Fatalf("provider handle = %+v; want persisted handle", claimed.ProviderHandle)
	}

	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_cleanup_future")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                   "sandbox_cleanup_future",
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_cleanup_future",
		Status:               StatusFailed,
		Provider:             testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted",
		CleanupStatus:        CleanupStatusRetryableFailed,
		CleanupNextAttemptAt: timePtr(base.Add(time.Minute)),
		CreatedAt:            base.Add(-10 * time.Minute),
		UpdatedAt:            base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox future: %v", err)
	}
	if _, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, "sandbox_cleanup_future", base, 2*time.Minute, 20); err == nil {
		t.Fatal("ClaimDueStartupCleanup future row succeeded; want not found")
	}

	for _, status := range []CleanupStatus{
		CleanupStatusReleased,
		CleanupStatusPermanentFailed,
	} {
		sessionID := "sesn_cleanup_terminal_" + string(status)
		sandboxID := "sandbox_cleanup_terminal_" + string(status)
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{
			ID:                   sandboxID,
			WorkspaceID:          workspace.DefaultID,
			SessionID:            sessionID,
			Status:               StatusFailed,
			Provider:             testPostgreSQLSandboxProvider,
			StartupFailureReason: "startup_interrupted",
			CleanupStatus:        status,
			CreatedAt:            base.Add(-10 * time.Minute),
			UpdatedAt:            base.Add(-time.Minute),
		}); err != nil {
			t.Fatalf("CreateSandbox terminal %s: %v", status, err)
		}
		if _, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, 2*time.Minute, 20); err == nil {
			t.Fatalf("ClaimDueStartupCleanup terminal %s succeeded; want not found", status)
		}
		got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
		if err != nil {
			t.Fatalf("FindLatestBySessionID terminal %s: %v", status, err)
		}
		if got.CleanupStatus != status {
			t.Fatalf("terminal cleanup status = %s; want unchanged %s", got.CleanupStatus, status)
		}
	}
}

func TestPostgreSQLStoreStartupCleanupLeaseExcludesConcurrentClaimAndRejectsLateCompletion(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	const (
		sessionID = "sesn_cleanup_concurrent_cap"
		sandboxID = "sandbox_cleanup_concurrent_cap"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 11, 55, 0, 0, time.UTC)
	retryAt := base.Add(time.Minute)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                   sandboxID,
		WorkspaceID:          workspace.DefaultID,
		SessionID:            sessionID,
		Status:               StatusFailed,
		Provider:             testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted",
		CleanupStatus:        CleanupStatusPending,
		CleanupAttemptCount:  0,
		CreatedAt:            base.Add(-time.Minute),
		UpdatedAt:            base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	start := make(chan struct{})
	type claimResult struct {
		claim *StartupCleanupClaim
		err   error
	}
	results := make(chan claimResult, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			claim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, 2*time.Minute, 20)
			results <- claimResult{claim: claim, err: err}
		}()
	}
	close(start)
	var first *StartupCleanupClaim
	successes := 0
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err == nil {
			successes++
			first = result.claim
		}
	}
	if successes != 1 || first == nil || first.Sandbox == nil {
		t.Fatalf("concurrent claim successes = %d, first = %+v; want exactly one lease holder", successes, first)
	}
	got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("FindLatestBySessionID: %v", err)
	}
	if got.CleanupAttemptCount != 1 || got.CleanupStatus != CleanupStatusInProgress ||
		got.CleanupLeaseToken != first.Sandbox.CleanupLeaseToken {
		t.Fatalf("concurrent cleanup claim result = %+v; want one durable reservation", got)
	}

	reclaimed, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base.Add(2*time.Minute), 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("reclaim expired cleanup lease: %v", err)
	}
	if !reclaimed.ProviderAttemptReserved || reclaimed.Sandbox.CleanupAttemptCount != 2 ||
		reclaimed.Sandbox.CleanupLeaseToken == first.Sandbox.CleanupLeaseToken {
		t.Fatalf("reclaimed cleanup = %+v; want fresh token and second reservation", reclaimed)
	}

	late, err := store.MarkStartupCleanupAttempt(ctx, workspace.DefaultID, sandboxID, CleanupAttemptUpdate{
		AttemptedAt:          base.Add(2*time.Minute + time.Second),
		CleanupLeaseToken:    first.Sandbox.CleanupLeaseToken,
		CleanupStatus:        CleanupStatusRetryableFailed,
		CleanupErrorKind:     "late_timeout",
		CleanupRetryable:     true,
		CleanupNextAttemptAt: &retryAt,
	})
	if err != nil {
		t.Fatalf("late cleanup completion: %v", err)
	}
	if late.CleanupStatus != CleanupStatusInProgress ||
		late.CleanupLeaseToken != reclaimed.Sandbox.CleanupLeaseToken ||
		late.CleanupAttemptCount != 2 {
		t.Fatalf("late completion changed reclaimed lease: %+v", late)
	}

	completed, err := store.MarkStartupCleanupAttempt(ctx, workspace.DefaultID, sandboxID, CleanupAttemptUpdate{
		AttemptedAt:       base.Add(2*time.Minute + 2*time.Second),
		CleanupLeaseToken: reclaimed.Sandbox.CleanupLeaseToken,
		CleanupStatus:     CleanupStatusReleased,
		CleanupMethod:     string(ReleaseReasonCleanup),
	})
	if err != nil {
		t.Fatalf("complete reclaimed cleanup: %v", err)
	}
	if completed.CleanupStatus != CleanupStatusReleased || completed.CleanupLeaseToken != "" ||
		completed.CleanupLeaseExpiresAt != nil || completed.CleanupAttemptCount != 2 {
		t.Fatalf("completed cleanup = %+v; want released row with lease cleared", completed)
	}
}

func TestPostgreSQLStoreCleanupClaimAtAttemptCapLeasesReadOnlyObservationWithoutIncrement(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	const (
		sessionID = "sesn_cleanup_cap_observation"
		sandboxID = "sandbox_cleanup_cap_observation"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 11, 55, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		Status: StatusFailed, Provider: testPostgreSQLSandboxProvider,
		StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending,
		CleanupAttemptCount: 20, CreatedAt: base.Add(-time.Minute), UpdatedAt: base.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	claim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup: %v", err)
	}
	if claim.ProviderAttemptReserved || claim.Sandbox.CleanupAttemptCount != 20 ||
		claim.Sandbox.CleanupStatus != CleanupStatusInProgress ||
		claim.Sandbox.CleanupLeaseToken == "" {
		t.Fatalf("cap claim = %+v; want leased read-only observation without increment", claim)
	}
}

func TestPostgreSQLStoreCapObservationTokenWinsBothCompletionOrderings(t *testing.T) {
	for _, test := range []struct {
		name             string
		lateSuccessFirst bool
	}{
		{name: "late success before cap settlement", lateSuccessFirst: true},
		{name: "cap settlement before late success", lateSuccessFirst: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := newSandboxStoreTestDB(t)
			ctx := context.Background()
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_cleanup_cap_race_" + suffix
			sandboxID := "sandbox_cleanup_cap_race_" + suffix
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			base := time.Date(2026, 5, 22, 11, 55, 0, 0, time.UTC)
			if err := store.CreateSandbox(ctx, &Sandbox{
				ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
				Status: StatusFailed, Provider: testPostgreSQLSandboxProvider,
				StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending,
				CleanupAttemptCount: 19, CreatedAt: base.Add(-time.Minute), UpdatedAt: base.Add(-time.Minute),
			}); err != nil {
				t.Fatalf("CreateSandbox: %v", err)
			}
			oldClaim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base, time.Minute, 20)
			if err != nil {
				t.Fatalf("claim twentieth attempt: %v", err)
			}
			capClaim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute), time.Minute, 20)
			if err != nil {
				t.Fatalf("claim cap observation: %v", err)
			}
			if capClaim.ProviderAttemptReserved || capClaim.Sandbox.CleanupAttemptCount != 20 {
				t.Fatalf("cap claim = %+v; want increment-free observation lease", capClaim)
			}
			lateSuccess := CleanupAttemptUpdate{
				AttemptedAt: base.Add(time.Minute + time.Second), CleanupLeaseToken: oldClaim.Sandbox.CleanupLeaseToken,
				CleanupStatus: CleanupStatusReleased, CleanupMethod: string(ReleaseReasonCleanup),
			}
			capTerminal := CleanupAttemptUpdate{
				AttemptedAt: base.Add(time.Minute + 2*time.Second), CleanupLeaseToken: capClaim.Sandbox.CleanupLeaseToken,
				CleanupStatus: CleanupStatusPermanentFailed,
			}
			apply := func(update CleanupAttemptUpdate) {
				t.Helper()
				if _, err := store.MarkStartupCleanupAttempt(ctx, workspace.DefaultID, sandboxID, update); err != nil {
					t.Fatalf("complete cleanup token %q: %v", update.CleanupLeaseToken, err)
				}
			}
			if test.lateSuccessFirst {
				apply(lateSuccess)
				apply(capTerminal)
			} else {
				apply(capTerminal)
				apply(lateSuccess)
			}
			got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
			if err != nil {
				t.Fatalf("FindLatestBySessionID: %v", err)
			}
			if got.CleanupStatus != CleanupStatusPermanentFailed || got.CleanupAttemptCount != 20 ||
				got.CleanupLeaseToken != "" || got.CleanupLeaseExpiresAt != nil {
				t.Fatalf("race result = %+v; want cap owner terminal result", got)
			}
		})
	}
}

func TestPostgreSQLStorePrepareSandboxReplacementClearsAttemptMachineUsability(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	const (
		sessionID = "sesn_replacement_machine_fact"
		sandboxID = "sandbox_replacement_machine_fact"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                    sandboxID,
		WorkspaceID:           workspace.DefaultID,
		SessionID:             sessionID,
		Status:                StatusArchived,
		Provider:              testPostgreSQLSandboxProvider,
		EnvironmentID:         "env_old",
		EnvironmentGeneration: 1,
		CreatedAt:             base,
		UpdatedAt:             base,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sandboxes
		    SET machine_was_usable = TRUE,
		        startup_failure_reason = 'old_failure',
		        cleanup_status = 'permanent_failed',
		        cleanup_attempt_count = 20
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), sandboxID,
	); err != nil {
		t.Fatalf("seed machine usability fact: %v", err)
	}

	got, err := store.PrepareSandboxReplacement(ctx, workspace.DefaultID, sandboxID, "env_new", 2, base.Add(time.Minute))
	if err != nil {
		t.Fatalf("PrepareSandboxReplacement: %v", err)
	}
	if got.Status != StatusCreating || got.MachineWasUsable ||
		got.StartupFailureReason != "" || got.CleanupStatus != CleanupStatusNone || got.CleanupAttemptCount != 0 {
		t.Fatalf("replacement sandbox = %+v; want creating with no inherited machine or cleanup history", got)
	}
	var persisted sql.NullBool
	if err := admin.QueryRowContext(ctx,
		`SELECT machine_was_usable
		   FROM sandboxes
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), sandboxID,
	).Scan(&persisted); err != nil {
		t.Fatalf("read persisted machine usability fact: %v", err)
	}
	if persisted.Valid {
		t.Fatalf("replacement machine_was_usable = %v; want NULL until this attempt produces a fact", persisted)
	}
}

func TestPostgreSQLStoreDerivesPreparationFailureSandboxSettlementFromUsabilityFact(t *testing.T) {
	base := time.Date(2026, 7, 18, 10, 0, 0, 0, time.UTC)

	t.Run("never usable remains failed after cleanup", func(t *testing.T) {
		runtime, admin := newSandboxStoreTestDB(t)
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
		seedSandboxSession(t, admin, workspace.DefaultID, "sesn_never_usable")
		if err := store.CreateSandbox(context.Background(), &Sandbox{
			ID: "sandbox_never_usable", WorkspaceID: workspace.DefaultID, SessionID: "sesn_never_usable",
			Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, "sandbox_never_usable", StartupFailureUpdate{
			FailedAt: base.Add(time.Minute), StartupFailureReason: "provider_create_failed",
			CleanupStatus: CleanupStatusReleased, CleanupAttempted: true,
		})
		if err != nil {
			t.Fatalf("MarkStartupFailed: %v", err)
		}
		if got.Status != StatusFailed || got.MachineWasUsable {
			t.Fatalf("settlement = %+v; want failed and never usable", got)
		}
	})

	t.Run("usable archive success becomes archived", func(t *testing.T) {
		runtime, admin := newSandboxStoreTestDB(t)
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
		seedSandboxSession(t, admin, workspace.DefaultID, "sesn_usable_archived")
		if err := store.CreateSandbox(context.Background(), &Sandbox{
			ID: "sandbox_usable_archived", WorkspaceID: workspace.DefaultID, SessionID: "sesn_usable_archived",
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, "sandbox_usable_archived", StartupFailureUpdate{
			FailedAt: base.Add(time.Minute), StartupFailureReason: "resource_preparation_failed",
			CleanupStatus: CleanupStatusReleased, CleanupAttempted: true,
		})
		if err != nil {
			t.Fatalf("MarkStartupFailed: %v", err)
		}
		if got.Status != StatusArchived || !got.MachineWasUsable {
			t.Fatalf("settlement = %+v; want archived usable sandbox", got)
		}
	})

	t.Run("usable provider missing becomes released", func(t *testing.T) {
		runtime, admin := newSandboxStoreTestDB(t)
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
		seedSandboxSession(t, admin, workspace.DefaultID, "sesn_usable_missing")
		if err := store.CreateSandbox(context.Background(), &Sandbox{
			ID: "sandbox_usable_missing", WorkspaceID: workspace.DefaultID, SessionID: "sesn_usable_missing",
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		got, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, "sandbox_usable_missing", StartupFailureUpdate{
			FailedAt: base.Add(time.Minute), StartupFailureReason: "resource_preparation_failed",
			CleanupStatus: CleanupStatusReleased, CleanupErrorKind: string(ProviderErrorNotFound),
			CleanupAttempted: true,
		})
		if err != nil {
			t.Fatalf("MarkStartupFailed: %v", err)
		}
		if got.Status != StatusReleased || got.CleanupStatus != CleanupStatusReleased ||
			got.ReleasedAt == nil || !got.MachineWasUsable {
			t.Fatalf("settlement = %+v; want released provider-missing sandbox", got)
		}
	})

	t.Run("usable retryable archive failure reconciles to archived", func(t *testing.T) {
		runtime, admin := newSandboxStoreTestDB(t)
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
		seedSandboxSession(t, admin, workspace.DefaultID, "sesn_usable_retry")
		if err := store.CreateSandbox(context.Background(), &Sandbox{
			ID: "sandbox_usable_retry", WorkspaceID: workspace.DefaultID, SessionID: "sesn_usable_retry",
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		retryAt := base.Add(2 * time.Minute)
		failed, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, "sandbox_usable_retry", StartupFailureUpdate{
			FailedAt: base.Add(time.Minute), StartupFailureReason: "resource_preparation_failed",
			CleanupStatus: CleanupStatusRetryableFailed, CleanupRetryable: true,
			CleanupAttempted: true, CleanupNextAttemptAt: &retryAt,
		})
		if err != nil {
			t.Fatalf("MarkStartupFailed: %v", err)
		}
		if failed.Status != StatusFailed || !failed.MachineWasUsable {
			t.Fatalf("failed settlement = %+v; want retryable failed usable row", failed)
		}
		claim, err := store.ClaimDueStartupCleanup(context.Background(), workspace.DefaultID, failed.ID, retryAt.Add(time.Second), 2*time.Minute, 20)
		if err != nil {
			t.Fatalf("ClaimDueStartupCleanup: %v", err)
		}
		archived, err := store.MarkStartupCleanupAttempt(context.Background(), workspace.DefaultID, failed.ID, CleanupAttemptUpdate{
			AttemptedAt: retryAt.Add(2 * time.Second), CleanupStatus: CleanupStatusReleased,
			CleanupMethod: string(ReleaseReasonCleanup), CleanupLeaseToken: claim.Sandbox.CleanupLeaseToken,
		})
		if err != nil {
			t.Fatalf("MarkStartupCleanupAttempt: %v", err)
		}
		if archived.Status != StatusArchived || !archived.MachineWasUsable {
			t.Fatalf("reconciled settlement = %+v; want archived usable row", archived)
		}
		late, err := store.MarkStartupCleanupAttempt(context.Background(), workspace.DefaultID, failed.ID, CleanupAttemptUpdate{
			AttemptedAt: retryAt.Add(3 * time.Second), CleanupStatus: CleanupStatusRetryableFailed,
			CleanupErrorKind: "late_timeout", CleanupRetryable: true,
			CleanupNextAttemptAt: timePtr(retryAt.Add(time.Minute)), CleanupLeaseToken: claim.Sandbox.CleanupLeaseToken,
		})
		if err != nil {
			t.Fatalf("late cleanup completion after archive: %v", err)
		}
		if late.Status != StatusArchived || late.CleanupStatus != CleanupStatusReleased ||
			late.CleanupAttemptCount != archived.CleanupAttemptCount {
			t.Fatalf("late cleanup completion changed archived row: before=%+v after=%+v", archived, late)
		}
	})

	t.Run("usable retryable cleanup observes provider missing", func(t *testing.T) {
		runtime, admin := newSandboxStoreTestDB(t)
		store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
		seedSandboxSession(t, admin, workspace.DefaultID, "sesn_usable_retry_missing")
		if err := store.CreateSandbox(context.Background(), &Sandbox{
			ID: "sandbox_usable_retry_missing", WorkspaceID: workspace.DefaultID, SessionID: "sesn_usable_retry_missing",
			Status: StatusActive, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
		}); err != nil {
			t.Fatalf("CreateSandbox: %v", err)
		}
		retryAt := base.Add(2 * time.Minute)
		failed, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, "sandbox_usable_retry_missing", StartupFailureUpdate{
			FailedAt: base.Add(time.Minute), StartupFailureReason: "resource_preparation_failed",
			CleanupStatus: CleanupStatusRetryableFailed, CleanupRetryable: true,
			CleanupAttempted: true, CleanupNextAttemptAt: &retryAt,
		})
		if err != nil {
			t.Fatalf("MarkStartupFailed: %v", err)
		}
		claim, err := store.ClaimDueStartupCleanup(context.Background(), workspace.DefaultID, failed.ID, retryAt.Add(time.Second), 2*time.Minute, 20)
		if err != nil {
			t.Fatalf("ClaimDueStartupCleanup: %v", err)
		}
		released, err := store.MarkStartupCleanupAttempt(context.Background(), workspace.DefaultID, failed.ID, CleanupAttemptUpdate{
			AttemptedAt: retryAt.Add(2 * time.Second), CleanupStatus: CleanupStatusReleased,
			CleanupErrorKind: string(ProviderErrorNotFound), CleanupLeaseToken: claim.Sandbox.CleanupLeaseToken,
		})
		if err != nil {
			t.Fatalf("MarkStartupCleanupAttempt: %v", err)
		}
		if released.Status != StatusReleased || released.CleanupStatus != CleanupStatusReleased ||
			released.ReleasedAt == nil || !released.MachineWasUsable {
			t.Fatalf("reconciled settlement = %+v; want released provider-missing sandbox", released)
		}
	})

	t.Run("late failure writer cannot rewrite settled lifecycle", func(t *testing.T) {
		for _, status := range []Status{StatusFailed, StatusArchived, StatusReleasing, StatusReleased} {
			runtime, admin := newSandboxStoreTestDB(t)
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			sessionID := "sesn_settled_" + string(status)
			sandboxID := "sandbox_settled_" + string(status)
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			if err := store.CreateSandbox(context.Background(), &Sandbox{
				ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
				Status: status, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base,
			}); err != nil {
				t.Fatalf("CreateSandbox %s: %v", status, err)
			}
			got, err := store.MarkStartupFailed(context.Background(), workspace.DefaultID, sandboxID, StartupFailureUpdate{
				FailedAt: base.Add(time.Minute), StartupFailureReason: "late_failure",
				CleanupStatus: CleanupStatusPermanentFailed,
			})
			if err != nil {
				t.Fatalf("MarkStartupFailed %s: %v", status, err)
			}
			if got.Status != status || got.StartupFailureReason != "" {
				t.Fatalf("late writer changed %s row: %+v", status, got)
			}
		}
	})
}

func TestPostgreSQLStoreDeletedSessionStaleStartupBecomesFailedButStartupCleanupCannotClaim(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 14, 16, 0, 0, 0, time.UTC)
	for _, status := range []Status{StatusCreating, StatusResuming} {
		sessionID := "sesn_deleted_stale_" + string(status)
		sandboxID := "sandbox_deleted_stale_" + string(status)
		seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
		if err := store.CreateSandbox(ctx, &Sandbox{ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID, Status: status, Provider: testPostgreSQLSandboxProvider, CreatedAt: base, UpdatedAt: base}); err != nil {
			t.Fatalf("CreateSandbox %s: %v", status, err)
		}
		if _, err := admin.ExecContext(ctx, `UPDATE sessions SET lifecycle_state='deleted', delete_cleanup_id=$2 WHERE id=$1`, sessionID, "delcln_"+string(status)); err != nil {
			t.Fatalf("tombstone %s: %v", status, err)
		}
		claimed, err := store.ClaimStaleCreating(ctx, workspace.DefaultID, sandboxID, base.Add(time.Minute), StartupFailureUpdate{FailedAt: base.Add(2 * time.Minute), StartupFailureReason: "startup_interrupted", CleanupStatus: CleanupStatusPending})
		if err != nil {
			t.Fatalf("ClaimStaleCreating %s: %v", status, err)
		}
		if claimed.Status != StatusFailed || claimed.CleanupStatus != CleanupStatusPending {
			t.Fatalf("claimed %s = %+v; want failed/pending for delete owner", status, claimed)
		}
		if _, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, base.Add(3*time.Minute), 2*time.Minute, 20); err == nil {
			t.Fatalf("ClaimDueStartupCleanup %s succeeded for tombstoned session", status)
		}
		got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
		if err != nil {
			t.Fatalf("FindLatestBySessionID %s: %v", status, err)
		}
		if got.Status != StatusFailed || got.CleanupStatus != CleanupStatusPending || got.CleanupAttemptCount != 0 {
			t.Fatalf("startup cleanup mutated deleted %s = %+v", status, got)
		}
	}
}

func TestPostgreSQLStoreStartupCleanupClaimSerializesBehindSessionTombstone(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 7, 18, 18, 0, 0, 0, time.UTC)
	const (
		sessionID = "sesn_cleanup_tombstone_race"
		sandboxID = "sandbox_cleanup_tombstone_race"
	)
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		Status: StatusCreating, Provider: testPostgreSQLSandboxProvider,
		CreatedAt: base, UpdatedAt: base,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	if _, err := store.MarkStartupFailed(ctx, workspace.DefaultID, sandboxID, StartupFailureUpdate{
		FailedAt: base.Add(time.Minute), StartupFailureReason: "startup_interrupted",
		CleanupStatus: CleanupStatusPending,
	}); err != nil {
		t.Fatalf("MarkStartupFailed: %v", err)
	}

	deleteTx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delete transaction: %v", err)
	}
	defer func() { _ = deleteTx.Rollback() }()
	if _, err := deleteTx.ExecContext(ctx,
		`UPDATE sessions
		    SET lifecycle_state = 'deleted', delete_cleanup_id = $2
		  WHERE id = $1`,
		sessionID,
		"delcln_cleanup_tombstone_race",
	); err != nil {
		t.Fatalf("stage session tombstone: %v", err)
	}

	claimResult := make(chan error, 1)
	go func() {
		_, claimErr := store.ClaimDueStartupCleanup(
			context.Background(),
			workspace.DefaultID,
			sandboxID,
			base.Add(2*time.Minute),
			2*time.Minute,
			20,
		)
		claimResult <- claimErr
	}()

	waitDeadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := admin.QueryRowContext(ctx,
			`SELECT EXISTS (
				SELECT 1
				  FROM pg_stat_activity
				 WHERE wait_event_type = 'Lock'
				   AND query LIKE '%FOR UPDATE OF s%'
			)`,
		).Scan(&waiting); err != nil {
			t.Fatalf("inspect claim lock wait: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("startup cleanup claim did not wait on the session tombstone lock")
		}
		time.Sleep(time.Millisecond)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit session tombstone: %v", err)
	}
	select {
	case claimErr := <-claimResult:
		var notFound *NotFoundError
		if !errors.As(claimErr, &notFound) {
			t.Fatalf("ClaimDueStartupCleanup error = %T %v; want NotFoundError", claimErr, claimErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup cleanup claim did not finish after tombstone commit")
	}

	got, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("FindLatestBySessionID: %v", err)
	}
	if got.CleanupStatus != CleanupStatusPending || got.CleanupLeaseToken != "" ||
		got.CleanupAttemptCount != 0 {
		t.Fatalf("tombstone-race sandbox = %+v; want untouched pending cleanup", got)
	}
}

func TestPostgreSQLStoreMarkStartupCleanupAttemptPreservesStartupFailureFields(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 11, 55, 0, 0, time.UTC)
	failedAt := base.Add(-5 * time.Minute)
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_cleanup_preserve")
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:                   "sandbox_cleanup_preserve",
		WorkspaceID:          workspace.DefaultID,
		SessionID:            "sesn_cleanup_preserve",
		Status:               StatusFailed,
		Provider:             testPostgreSQLSandboxProvider,
		StartupFailureReason: string(SandboxErrorMountFailed),
		CleanupStatus:        CleanupStatusRetryableFailed,
		CreatedAt:            base.Add(-10 * time.Minute),
		UpdatedAt:            failedAt,
		FailedAt:             &failedAt,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}

	claim, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, "sandbox_cleanup_preserve", base, 2*time.Minute, 20)
	if err != nil {
		t.Fatalf("ClaimDueStartupCleanup: %v", err)
	}
	got, err := store.MarkStartupCleanupAttempt(ctx, workspace.DefaultID, "sandbox_cleanup_preserve", CleanupAttemptUpdate{
		AttemptedAt:       base,
		CleanupLeaseToken: claim.Sandbox.CleanupLeaseToken,
		CleanupStatus:     CleanupStatusReleased,
		CleanupMethod:     string(ReleaseReasonCleanup),
	})
	if err != nil {
		t.Fatalf("MarkStartupCleanupAttempt: %v", err)
	}
	if got.StartupFailureReason != string(SandboxErrorMountFailed) || got.FailedAt == nil || !got.FailedAt.Equal(failedAt) {
		t.Fatalf("startup fields = reason %q failed_at %v; want preserved", got.StartupFailureReason, got.FailedAt)
	}
	if got.CleanupStatus != CleanupStatusReleased || got.CleanupAttemptCount != 1 || got.CleanupLastAttemptAt == nil || !got.CleanupLastAttemptAt.Equal(base) {
		t.Fatalf("cleanup fields = %+v; want released attempt at %s", got, base)
	}
}

func TestServiceReleaseForSessionUsesPersistedProviderHandle(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_release_handle")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	provider := newSuccessfulRecordingProvider()
	service := NewService(store, provider,
		WithProviderName(testPostgreSQLSandboxProvider),
		WithIDStrategy(func() string { return "sandbox_release_handle" }),
		WithClock(func() time.Time { return fixedSandboxTime }),
	)

	createdAt := time.Date(2026, 5, 22, 11, 45, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_release_handle",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_release_handle",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}
	handle := ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_release_handle",
		Metadata:  map[string]string{"region": "iad", "release_group": "blue"},
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_release_handle", handle, createdAt.Add(time.Minute)); err != nil {
		t.Fatalf("SaveProviderHandle: %v", err)
	}
	if _, err := store.MarkActive(ctx, workspace.DefaultID, "sandbox_release_handle", createdAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkActive: %v", err)
	}

	if err := service.ReleaseForSession(ctx, workspace.DefaultID, "sesn_release_handle", ReleaseReasonDelete); err != nil {
		t.Fatalf("ReleaseForSession: %v", err)
	}
	if len(provider.releaseHandles) != 1 || !reflect.DeepEqual(provider.releaseHandles[0], handle) {
		t.Fatalf("release handles = %#v; want persisted handle %#v", provider.releaseHandles, handle)
	}
}

func TestPostgreSQLStoreEnforcesOneSandboxRowPerSession(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_unique")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	base := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_unique_active",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_unique",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base,
		UpdatedAt:   base,
	}); err != nil {
		t.Fatalf("CreateSandbox first: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_unique_active", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_unique_active",
		Metadata:  map[string]string{"region": "iad"},
	}, base); err != nil {
		t.Fatalf("SaveProviderHandle first: %v", err)
	}
	if _, err := store.MarkActive(ctx, workspace.DefaultID, "sandbox_unique_active", base); err != nil {
		t.Fatalf("MarkActive first: %v", err)
	}

	err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_unique_second",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_unique",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base.Add(time.Minute),
		UpdatedAt:   base.Add(time.Minute),
	})
	if err == nil {
		t.Fatal("CreateSandbox second current row succeeded; want conflict")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %T %v; want ConflictError", err, err)
	}

	if _, err := store.MarkReleasing(ctx, workspace.DefaultID, "sandbox_unique_active", base.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkReleasing: %v", err)
	}
	if _, err := store.MarkReleased(ctx, workspace.DefaultID, "sandbox_unique_active", base.Add(3*time.Minute)); err != nil {
		t.Fatalf("MarkReleased: %v", err)
	}
	err = store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_unique_after_release",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_unique",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base.Add(4 * time.Minute),
		UpdatedAt:   base.Add(4 * time.Minute),
	})
	if err == nil {
		t.Fatal("CreateSandbox after released historical row succeeded; want conflict")
	}
	if !errors.As(err, &conflict) {
		t.Fatalf("after-release err = %T %v; want ConflictError", err, err)
	}
}

func TestPostgreSQLStoreRejectsSuspiciousProviderMetadataBeforePersistence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_secret")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 13, 0, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_secret",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_secret",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base,
		UpdatedAt:   base,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}

	_, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_secret", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_secret",
		Metadata:  map[string]string{"authorization": "Bearer super-secret"},
	}, base)
	if err == nil {
		t.Fatal("SaveProviderHandle accepted authorization metadata; want validation error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}

	row := admin.QueryRowContext(ctx, `SELECT provider_sandbox_id, provider_metadata_json FROM sandboxes WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sandbox_secret")
	var providerSandboxID sql.NullString
	var metadataJSON string
	if scanErr := row.Scan(&providerSandboxID, &metadataJSON); scanErr != nil {
		t.Fatalf("scan sandbox after rejected metadata: %v", scanErr)
	}
	if providerSandboxID.Valid || metadataJSON != "{}" {
		t.Fatalf("persisted rejected provider data: provider_sandbox_id=%v metadata=%s", providerSandboxID, metadataJSON)
	}
}

func TestPostgreSQLStoreRejectsCredentialURLProviderMetadataBeforePersistence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_credential_url")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 13, 30, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_credential_url",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_credential_url",
		Status:      StatusCreating,
		Provider:    testPostgreSQLSandboxProvider,
		CreatedAt:   base,
		UpdatedAt:   base,
	}); err != nil {
		t.Fatalf("CreateSandbox: %v", err)
	}

	_, err := store.SaveProviderHandle(ctx, workspace.DefaultID, "sandbox_credential_url", ProviderHandle{
		Provider:  testPostgreSQLSandboxProvider,
		SandboxID: "provider_credential_url",
		Metadata:  map[string]string{"endpoint": "sandbox endpoint=HTTPS://unit:opaque@example.test/api"},
	}, base)
	if err == nil {
		t.Fatal("SaveProviderHandle accepted credential URL metadata; want validation error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}

	row := admin.QueryRowContext(ctx, `SELECT provider_sandbox_id, provider_metadata_json FROM sandboxes WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sandbox_credential_url")
	var providerSandboxID sql.NullString
	var metadataJSON string
	if scanErr := row.Scan(&providerSandboxID, &metadataJSON); scanErr != nil {
		t.Fatalf("scan sandbox after rejected credential URL metadata: %v", scanErr)
	}
	if providerSandboxID.Valid || metadataJSON != "{}" {
		t.Fatalf("persisted rejected provider data: provider_sandbox_id=%v metadata=%s", providerSandboxID, metadataJSON)
	}
}

func TestPostgreSQLStoreRejectsSuspiciousCreateMetadataBeforePersistence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_create_secret")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 14, 0, 0, 0, time.UTC)

	err := store.CreateSandbox(ctx, &Sandbox{
		ID:               "sandbox_create_secret",
		WorkspaceID:      workspace.DefaultID,
		SessionID:        "sesn_create_secret",
		Status:           StatusCreating,
		Provider:         testPostgreSQLSandboxProvider,
		ProviderMetadata: map[string]string{"provider_hint": `{"token":"abc"}`},
		CreatedAt:        base,
		UpdatedAt:        base,
	})
	if err == nil {
		t.Fatal("CreateSandbox accepted secret-shaped initial metadata; want validation error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}

	var count int
	if scanErr := admin.QueryRowContext(ctx, `SELECT count(*) FROM sandboxes WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sandbox_create_secret").Scan(&count); scanErr != nil {
		t.Fatalf("count rejected sandbox: %v", scanErr)
	}
	if count != 0 {
		t.Fatalf("persisted rejected sandbox rows = %d; want 0", count)
	}
}

func TestPostgreSQLStoreRejectsCreateProviderSandboxIDBeforePersistence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_create_handle")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 15, 0, 0, 0, time.UTC)

	err := store.CreateSandbox(ctx, &Sandbox{
		ID:                "sandbox_create_handle",
		WorkspaceID:       workspace.DefaultID,
		SessionID:         "sesn_create_handle",
		Status:            StatusCreating,
		Provider:          testPostgreSQLSandboxProvider,
		ProviderSandboxID: "api_key=abc",
		ProviderMetadata:  map[string]string{"region": "iad"},
		CreatedAt:         base,
		UpdatedAt:         base,
	})
	if err == nil {
		t.Fatal("CreateSandbox accepted initial provider_sandbox_id; want validation error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	var count int
	if scanErr := admin.QueryRowContext(ctx, `SELECT count(*) FROM sandboxes WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sandbox_create_handle").Scan(&count); scanErr != nil {
		t.Fatalf("count rejected sandbox: %v", scanErr)
	}
	if count != 0 {
		t.Fatalf("persisted rejected sandbox rows = %d; want 0", count)
	}
}

func TestPostgreSQLStoreRejectsSecretShapedProviderBeforePersistence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_provider_secret")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	base := time.Date(2026, 5, 22, 16, 0, 0, 0, time.UTC)

	err := store.CreateSandbox(ctx, &Sandbox{
		ID:          "sandbox_provider_secret",
		WorkspaceID: workspace.DefaultID,
		SessionID:   "sesn_provider_secret",
		Status:      StatusCreating,
		Provider:    "Bearer raw-token",
		CreatedAt:   base,
		UpdatedAt:   base,
	})
	if err == nil {
		t.Fatal("CreateSandbox accepted secret-shaped provider name; want validation error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("err = %T %v; want ValidationError", err, err)
	}
	var count int
	if scanErr := admin.QueryRowContext(ctx, `SELECT count(*) FROM sandboxes WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sandbox_provider_secret").Scan(&count); scanErr != nil {
		t.Fatalf("count rejected sandbox: %v", scanErr)
	}
	if count != 0 {
		t.Fatalf("persisted rejected sandbox rows = %d; want 0", count)
	}
}

func TestPostgreSQLStoreClaimsPreparationLoadsResourcesAndMarksReady(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_prepare_pg")
	seedSessionPreparation(t, admin, workspace.DefaultID, "sesn_prepare_pg", "prep_pg", "sandbox_prepare_pg", "pending")
	seedFilePreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_file_pg", "file_source_pg", "file_session_pg", "obj_prepare_pg", "/mnt/session/uploads/file_session_pg")
	seedFilePreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_deleted_pg", "file_deleted_source_pg", "file_deleted_session_pg", "obj_deleted_pg", "/workspace/deleted.csv")
	seedFilePreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_detached_pg", "file_detached_source_pg", "file_detached_session_pg", "obj_detached_pg", "/workspace/detached.csv")
	seedMemoryPreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_memory_deleted_pg", "memstore_deleted_pg", "/mnt/memory/deleted")
	seedSandboxSkillVersion(t, admin, workspace.DefaultID, "skill_prepare_pg", "2026-07-01", "finance", "skills/default/skill_prepare_pg/2026-07-01/package.zip", 42, strings.Repeat("c", 64))
	seedSandboxSessionAgentSkills(t, admin, workspace.DefaultID, "sesn_prepare_pg", []sessionPreparationSkillRef{{SkillID: "skill_prepare_pg", Version: "latest"}})
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_resources
		    SET delete_requested_at = '2026-05-23T09:55:00Z', updated_at = '2026-05-23T09:55:00Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_pg", "sesrsc_deleted_pg",
	); err != nil {
		t.Fatalf("request file resource delete: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_resources
		    SET detached_at = '2026-05-23T09:50:00Z', updated_at = '2026-05-23T09:50:00Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_pg", "sesrsc_detached_pg",
	); err != nil {
		t.Fatalf("detach non-materialized file resource: %v", err)
	}
	seedGitHubPreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_repo_pg", "https://github.com/tetral-ai/tetral", "/workspace/tetral", "branch", "main")
	seedGitHubPreparationResource(t, admin, workspace.DefaultID, "sesn_prepare_pg", "sesrsc_repo_deleted_pg", "https://github.com/tetral-ai/old", "/workspace/old", "", "")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_resources SET delete_requested_at = '2026-05-23T09:56:00Z', updated_at = '2026-05-23T09:56:00Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND resource_id IN ($3, $4)`,
		string(workspace.DefaultID), "sesn_prepare_pg", "sesrsc_memory_deleted_pg", "sesrsc_repo_deleted_pg",
	); err != nil {
		t.Fatalf("request memory/GitHub resource delete: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)

	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, "sesn_prepare_pg", "prep_pg", now)
	if err != nil {
		t.Fatalf("ClaimSessionPreparation: %v", err)
	}
	if !claimed || preparation.Status != "preparing" || preparation.SandboxID != "sandbox_prepare_pg" || preparation.EnvironmentID != "env_sandbox" {
		t.Fatalf("preparation = %+v claimed=%v; want preparing env/sandbox", preparation, claimed)
	}
	if preparation.ProviderArtifactRef != "artifact_sandbox" || preparation.Network.Type != "unrestricted" {
		t.Fatalf("preparation artifact/network = %q/%+v; want ready artifact with unrestricted network", preparation.ProviderArtifactRef, preparation.Network)
	}
	resources, err := store.ListSessionPreparationResources(ctx, workspace.DefaultID, "sesn_prepare_pg")
	if err != nil {
		t.Fatalf("ListSessionPreparationResources: %v", err)
	}
	if len(resources.GitHubRepositories) != 1 ||
		resources.GitHubRepositories[0].URL != "https://github.com/tetral-ai/tetral" ||
		resources.GitHubRepositories[0].CheckoutType != "branch" ||
		resources.GitHubRepositories[0].ResourceID != "sesrsc_repo_pg" {
		t.Fatalf("resources = %+v; want GitHub repository setup", resources)
	}
	if len(resources.Files) != 1 ||
		resources.Files[0].ResourceID != "sesrsc_file_pg" ||
		resources.Files[0].SourceFileID != "file_source_pg" ||
		resources.Files[0].SessionFileID != "file_session_pg" ||
		resources.Files[0].ObjectID != "obj_prepare_pg" ||
		resources.Files[0].MountPath != "/mnt/session/uploads/file_session_pg" ||
		!resources.Files[0].ReadOnly {
		t.Fatalf("file resources = %+v; want source_file_id resolved to canonical object_id", resources.Files)
	}
	if len(resources.DeletedFiles) != 1 ||
		resources.DeletedFiles[0].ResourceID != "sesrsc_deleted_pg" ||
		resources.DeletedFiles[0].SessionFileID != "file_deleted_session_pg" ||
		resources.DeletedFiles[0].MountPath != "/workspace/deleted.csv" {
		t.Fatalf("deleted file resources = %+v; want only delete-requested cleanup input", resources.DeletedFiles)
	}
	if len(resources.DeletedMemoryStores) != 1 || resources.DeletedMemoryStores[0].MemoryStoreID != "memstore_deleted_pg" || resources.DeletedMemoryStores[0].MountPath != "/mnt/memory/deleted" {
		t.Fatalf("deleted memory resources = %+v; want typed cleanup input", resources.DeletedMemoryStores)
	}
	if len(resources.DeletedGitHubRepositories) != 1 || resources.DeletedGitHubRepositories[0].ResourceID != "sesrsc_repo_deleted_pg" || resources.DeletedGitHubRepositories[0].MountPath != "/workspace/old" {
		t.Fatalf("deleted GitHub resources = %+v; want typed cleanup input", resources.DeletedGitHubRepositories)
	}
	if len(resources.Skills) != 1 ||
		resources.Skills[0].SkillID != "skill_prepare_pg" ||
		resources.Skills[0].Name != "Test Skill" ||
		resources.Skills[0].Description != "Use for financial analysis." ||
		resources.Skills[0].Version != "2026-07-01" ||
		resources.Skills[0].Directory != "finance" ||
		resources.Skills[0].BlobKey != "skills/default/skill_prepare_pg/2026-07-01/package.zip" ||
		resources.Skills[0].SizeBytes != 42 ||
		resources.Skills[0].SHA256 != strings.Repeat("c", 64) {
		t.Fatalf("skill resources = %+v; want latest skill ref resolved to immutable version blob", resources.Skills)
	}
	var skillsIndexJSON sql.NullString
	if err := admin.QueryRowContext(ctx,
		`SELECT skills_index_json
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_pg", "prep_pg",
	).Scan(&skillsIndexJSON); err != nil {
		t.Fatalf("read persisted skill guidance index: %v", err)
	}
	if !skillsIndexJSON.Valid || skillsIndexJSON.String != `[{"skill_id":"skill_prepare_pg","skill_version_id":"skv_skill_prepare_pg_2026-07-01","version":"2026-07-01","name":"Test Skill","description":"Use for financial analysis.","directory":"finance"}]` {
		t.Fatalf("skills_index_json = %v; want resolved mounted skill metadata", skillsIndexJSON)
	}
	for _, resourceID := range []string{"sesrsc_deleted_pg", "sesrsc_memory_deleted_pg", "sesrsc_repo_deleted_pg"} {
		if err := store.DetachSessionPreparationResource(ctx, workspace.DefaultID, "sesn_prepare_pg", "prep_pg", resourceID, now.Add(30*time.Second)); err != nil {
			t.Fatalf("DetachSessionPreparationResource(%s): %v", resourceID, err)
		}
	}
	var detachedAt, deletedAt sql.NullString
	if err := admin.QueryRowContext(ctx,
		`SELECT sr.detached_at, f.deleted_at
		   FROM session_resources sr
		   JOIN session_file_resources sfr
		     ON sfr.workspace_id = sr.workspace_id
		    AND sfr.session_id = sr.session_id
		    AND sfr.resource_id = sr.resource_id
		   JOIN files f
		     ON f.workspace_id = sr.workspace_id
		    AND f.file_id = sfr.file_id
		  WHERE sr.workspace_id = $1
		    AND sr.session_id = $2
		    AND sr.resource_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_pg", "sesrsc_deleted_pg",
	).Scan(&detachedAt, &deletedAt); err != nil {
		t.Fatalf("read finalized deleted resource: %v", err)
	}
	if !detachedAt.Valid || detachedAt.String != now.Add(30*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("detached_at = %v; want cleanup finalizer timestamp", detachedAt)
	}
	if !deletedAt.Valid || deletedAt.String != now.Add(30*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("session file deleted_at = %v; want cleanup finalizer timestamp", deletedAt)
	}
	var generalizedDetached int
	if err := admin.QueryRowContext(ctx,
		`SELECT count(*) FROM session_resources
		  WHERE workspace_id = $1 AND session_id = $2
		    AND resource_id IN ($3, $4) AND detached_at IS NOT NULL`,
		string(workspace.DefaultID), "sesn_prepare_pg", "sesrsc_memory_deleted_pg", "sesrsc_repo_deleted_pg",
	).Scan(&generalizedDetached); err != nil {
		t.Fatalf("read generalized detached resources: %v", err)
	}
	if generalizedDetached != 2 {
		t.Fatalf("generalized detached resources = %d; want 2", generalizedDetached)
	}
	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, "sesn_prepare_pg", "prep_pg", SessionPreparationReadyUpdate{ReadyAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}
	var status string
	var readyAt sql.NullString
	if err := admin.QueryRowContext(ctx,
		`SELECT status, ready_at FROM session_preparations WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_pg", "prep_pg",
	).Scan(&status, &readyAt); err != nil {
		t.Fatalf("read ready preparation: %v", err)
	}
	if status != "ready" || !readyAt.Valid || readyAt.String != now.Add(time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("ready row status=%q ready_at=%v; want ready timestamp", status, readyAt)
	}
	if got := len(readSandboxQueueJobs(t, admin, "sesn_prepare_pg")); got != 0 {
		t.Fatalf("queue jobs = %d; want none without pending inbound events", got)
	}
	_, claimed, err = store.ClaimSessionPreparation(ctx, workspace.DefaultID, "sesn_prepare_pg", "prep_pg", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("Claim ready preparation: %v", err)
	}
	if claimed {
		t.Fatal("ready preparation was claimed again; want no-op")
	}
}

func TestPostgreSQLSessionPreparationRemovalFailureRetryDetachesEachResourceType(t *testing.T) {
	for _, resourceType := range []string{"file", "memory_store", "github_repository"} {
		t.Run(resourceType, func(t *testing.T) {
			runtime, admin := newSandboxStoreTestDB(t)
			ctx := context.Background()
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			suffix := strings.ReplaceAll(resourceType, "_", "")
			sessionID := "sesn_pg_remove_retry_" + suffix
			preparationID := "prep_pg_remove_retry_" + suffix
			sandboxID := "sandbox_pg_remove_retry_" + suffix
			resourceID := "sesrsc_pg_remove_retry_" + suffix
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, preparationID, sandboxID, "pending")
			switch resourceType {
			case "file":
				seedFilePreparationResource(t, admin, workspace.DefaultID, sessionID, resourceID, "file_source_"+suffix, "file_session_"+suffix, "obj_"+suffix, "/workspace/project")
			case "memory_store":
				seedMemoryPreparationResource(t, admin, workspace.DefaultID, sessionID, resourceID, "memstore_"+suffix, "/mnt/memory/project")
			case "github_repository":
				seedGitHubPreparationResource(t, admin, workspace.DefaultID, sessionID, resourceID, "https://github.com/tetral-ai/old", "/workspace/project", "", "")
			}
			if _, err := admin.ExecContext(ctx, `UPDATE session_resources SET delete_requested_at=$4,updated_at=$4 WHERE workspace_id=$1 AND session_id=$2 AND resource_id=$3`, string(workspace.DefaultID), sessionID, resourceID, "2026-05-23T09:55:00Z"); err != nil {
				t.Fatalf("request deletion: %v", err)
			}
			now := time.Date(2026, 5, 23, 10, 0, 0, 0, time.UTC)
			if _, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, preparationID, now); err != nil || !claimed {
				t.Fatalf("ClaimSessionPreparation claimed=%v err=%v", claimed, err)
			}
			coordinator := &sessionPreparationResourceCleanupCoordinator{store: store, workspaceID: workspace.DefaultID, sessionID: sessionID, preparationAttemptID: preparationID, clock: func() time.Time { return now.Add(time.Minute) }}
			removalCalls := 0
			successorMaterializerCalls := 0
			removeErr := errors.New("injected " + resourceType + " removal failure")
			remove := func(context.Context) error {
				removalCalls++
				if removalCalls == 1 {
					return removeErr
				}
				return nil
			}
			if err := coordinator.CleanupSessionResource(ctx, resourceID, remove); !errors.Is(err, removeErr) {
				t.Fatalf("first cleanup error=%v; want %v", err, removeErr)
			}
			var detachedAt, readyAt sql.NullString
			var preparationStatus string
			if err := admin.QueryRowContext(ctx, `SELECT detached_at FROM session_resources WHERE session_id=$1 AND resource_id=$2`, sessionID, resourceID).Scan(&detachedAt); err != nil {
				t.Fatalf("read failed detach: %v", err)
			}
			if err := admin.QueryRowContext(ctx, `SELECT status,ready_at FROM session_preparations WHERE session_id=$1 AND preparation_attempt_id=$2`, sessionID, preparationID).Scan(&preparationStatus, &readyAt); err != nil {
				t.Fatalf("read preparation after failure: %v", err)
			}
			if detachedAt.Valid || readyAt.Valid || preparationStatus != "preparing" || successorMaterializerCalls != 0 || removalCalls != 1 {
				t.Fatalf("failure state detached=%v ready=%v status=%q successor=%d removals=%d; want NULL/NULL/preparing/0/1", detachedAt, readyAt, preparationStatus, successorMaterializerCalls, removalCalls)
			}
			if err := coordinator.CleanupSessionResource(ctx, resourceID, remove); err != nil {
				t.Fatalf("cleanup retry: %v", err)
			}
			if err := admin.QueryRowContext(ctx, `SELECT detached_at FROM session_resources WHERE session_id=$1 AND resource_id=$2`, sessionID, resourceID).Scan(&detachedAt); err != nil {
				t.Fatalf("read committed detach: %v", err)
			}
			if !detachedAt.Valid || removalCalls != 2 {
				t.Fatalf("retry state detached=%v removals=%d; want committed/2", detachedAt, removalCalls)
			}
			if err := coordinator.CleanupSessionResource(ctx, resourceID, func(context.Context) error { removalCalls++; return nil }); err != nil {
				t.Fatalf("post-detach replay: %v", err)
			}
			if removalCalls != 2 {
				t.Fatalf("post-detach replay removal calls=%d; want 2", removalCalls)
			}
		})
	}
}

func TestPostgreSQLPrepareSessionClaimRefreshesCurrentEnvironmentGenerationBeforeReplacement(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_current_generation"
	preparationID := "prep_prepare_current_generation"
	sandboxID := "sandbox_prepare_current_generation"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, preparationID, sandboxID, "pending")
	if _, err := admin.ExecContext(ctx,
		`UPDATE environments SET current_generation = 2, updated_at = $2
		  WHERE workspace_id = $1 AND id = 'env_sandbox'`,
		string(workspace.DefaultID), "2026-07-14T12:00:00Z",
	); err != nil {
		t.Fatalf("advance environment generation: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, 'env_sandbox', 2, 'ready', 'tetral', 'artifact_sandbox_generation_2',
			'hash_config_sandbox_2', 'hash_packages_sandbox_2', '{"type":"unrestricted"}', '{}', $2, $2)`,
		string(workspace.DefaultID), "2026-07-14T12:00:00Z",
	); err != nil {
		t.Fatalf("seed current environment artifact: %v", err)
	}

	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	oldAt := time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC)
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		EnvironmentID: "env_sandbox", EnvironmentGeneration: 1,
		Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: oldAt, UpdatedAt: oldAt,
	}); err != nil {
		t.Fatalf("seed generation-1 sandbox: %v", err)
	}
	if _, err := store.SaveProviderHandle(ctx, workspace.DefaultID, sandboxID, ProviderHandle{
		Provider: testPostgreSQLSandboxProvider, SandboxID: "provider_generation_1",
	}, oldAt); err != nil {
		t.Fatalf("seed generation-1 provider handle: %v", err)
	}
	if _, err := store.MarkActive(ctx, workspace.DefaultID, sandboxID, oldAt); err != nil {
		t.Fatalf("mark generation-1 sandbox active: %v", err)
	}
	if _, err := store.RefreshSandboxState(ctx, workspace.DefaultID, sandboxID, StatusStopped, oldAt); err != nil {
		t.Fatalf("mark generation-1 sandbox stopped: %v", err)
	}
	provider := newSuccessfulRecordingProvider()
	provider.handle.Provider = testPostgreSQLSandboxProvider
	service := NewService(store, provider,
		WithProviderName(testPostgreSQLSandboxProvider),
		WithClock(func() time.Time { return time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC) }),
		WithSessionPreparationStore(store),
		WithSessionResourcePreparer(&recordingSessionResourcePreparer{}),
	)

	result, err := service.PrepareSession(ctx, SessionPrepareRequest{
		WorkspaceID: workspace.DefaultID, SessionID: sessionID, PreparationAttemptID: preparationID,
	})
	if err != nil || result.Status != SessionPrepareStatusReady {
		t.Fatalf("PrepareSession current generation = %+v err=%v; want ready", result, err)
	}
	if got := provider.calls; len(got) < 2 || got[0] != "release" || got[1] != "create" {
		t.Fatalf("provider calls = %v; want generation-1 delete before generation-2 create", got)
	}
	if len(provider.createRequests) != 1 || provider.createRequests[0].Setup.ProviderArtifactRef != "artifact_sandbox_generation_2" {
		t.Fatalf("create requests = %+v; want current generation-2 artifact", provider.createRequests)
	}
	var persistedGeneration int64
	if err := admin.QueryRowContext(ctx,
		`SELECT environment_generation FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), sessionID, preparationID,
	).Scan(&persistedGeneration); err != nil {
		t.Fatalf("read persisted preparation generation: %v", err)
	}
	if persistedGeneration != 2 {
		t.Fatalf("persisted preparation generation = %d; want current generation 2", persistedGeneration)
	}
	replaced, err := store.FindLatestBySessionID(ctx, workspace.DefaultID, sessionID)
	if err != nil {
		t.Fatalf("read replaced sandbox: %v", err)
	}
	if replaced.EnvironmentGeneration != 2 || replaced.ProviderSandboxID != provider.handle.SandboxID {
		t.Fatalf("replaced sandbox = %+v; want generation-2 provider", replaced)
	}
}

func TestPostgreSQLPreparationClaimMovesToCurrentBuildingGenerationWaitingForFanout(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_current_building"
	preparationID := "prep_prepare_current_building"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, preparationID, "sandbox_prepare_current_building", "pending")
	if _, err := admin.ExecContext(ctx,
		`UPDATE environments SET current_generation = 2 WHERE workspace_id = $1 AND id = 'env_sandbox'`,
		string(workspace.DefaultID),
	); err != nil {
		t.Fatalf("advance environment generation: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, 'env_sandbox', 2, 'building', 'tetral', NULL,
			'hash_config_sandbox_waiting_2', 'hash_packages_sandbox_waiting_2',
			'{"type":"unrestricted"}', '{}', $2, $2)`,
		string(workspace.DefaultID), "2026-07-14T12:00:00Z",
	); err != nil {
		t.Fatalf("seed building current artifact: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, preparationID, time.Date(2026, 7, 14, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ClaimSessionPreparation: %v", err)
	}
	if claimed || preparation.Status != "waiting_environment" || preparation.EnvironmentGeneration != 2 {
		t.Fatalf("claim = %+v claimed=%v; want durable generation-2 waiting state", preparation, claimed)
	}
	var (
		persistedGeneration int64
		persistedStatus     string
	)
	if err := admin.QueryRowContext(ctx,
		`SELECT environment_generation, status FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), sessionID, preparationID,
	).Scan(&persistedGeneration, &persistedStatus); err != nil {
		t.Fatalf("read waiting preparation: %v", err)
	}
	if persistedGeneration != 2 || persistedStatus != "waiting_environment" {
		t.Fatalf("persisted generation/status = %d/%q; want 2/waiting_environment", persistedGeneration, persistedStatus)
	}
}

func TestPostgreSQLPostCreateTombstoneSettlementIsTerminalAndIdempotent(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_post_create_tombstone_pg"
	preparationID := "prep_post_create_tombstone_pg"
	sandboxID := "sandbox_post_create_tombstone_pg"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, preparationID, sandboxID, "pending")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, 7, 14, 13, 0, 0, 0, time.UTC)
	if _, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, preparationID, now); err != nil || !claimed {
		t.Fatalf("claim preparation claimed=%v err=%v", claimed, err)
	}
	if err := store.CreateSandbox(ctx, &Sandbox{
		ID: sandboxID, WorkspaceID: workspace.DefaultID, SessionID: sessionID,
		EnvironmentID: "env_sandbox", EnvironmentGeneration: 1,
		Status: StatusCreating, Provider: testPostgreSQLSandboxProvider, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create durable startup row: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE sessions SET lifecycle_state = 'deleted', delete_cleanup_id = $3
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID), sessionID, "delcln_post_create_tombstone_pg",
	); err != nil {
		t.Fatalf("tombstone session: %v", err)
	}

	saved, disposition, err := store.SaveProviderHandleForSessionPreparation(ctx, workspace.DefaultID, sessionID, preparationID, sandboxID, ProviderHandle{
		Provider: testPostgreSQLSandboxProvider, SandboxID: "provider_post_create_tombstone_pg",
	}, now.Add(time.Second))
	if err != nil || disposition != postCreatePreparationDeleted || saved != nil {
		t.Fatalf("post-create fence saved=%+v disposition=%q err=%v; want deleted without handle persistence", saved, disposition, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := store.SettleDeletedSessionPreparationAfterCreate(ctx, workspace.DefaultID, sessionID, preparationID, sandboxID, now.Add(2*time.Second)); err != nil {
			t.Fatalf("settle deleted preparation attempt %d: %v", attempt+1, err)
		}
	}
	var (
		preparationStatus string
		failureReason     string
		sandboxStatus     string
		providerID        sql.NullString
		cleanupStatus     string
		cleanupMethod     sql.NullString
		cleanupAttempts   int
	)
	if err := admin.QueryRowContext(ctx,
		`SELECT sp.status, sp.failure_reason, sb.status, sb.provider_sandbox_id,
		        sb.cleanup_status, sb.cleanup_method, sb.cleanup_attempt_count
		   FROM session_preparations sp
		   JOIN sandboxes sb ON sb.workspace_id = sp.workspace_id AND sb.id = sp.sandbox_id
		  WHERE sp.workspace_id = $1 AND sp.session_id = $2 AND sp.preparation_attempt_id = $3`,
		string(workspace.DefaultID), sessionID, preparationID,
	).Scan(&preparationStatus, &failureReason, &sandboxStatus, &providerID, &cleanupStatus, &cleanupMethod, &cleanupAttempts); err != nil {
		t.Fatalf("read post-create terminal state: %v", err)
	}
	if preparationStatus != "failed" || failureReason != sessionDeletedPreparationFailureReason || sandboxStatus != string(StatusFailed) || providerID.Valid || cleanupStatus != string(CleanupStatusReleased) || !cleanupMethod.Valid || cleanupMethod.String != "delete" || cleanupAttempts != 1 {
		t.Fatalf("terminal state preparation=%q/%q sandbox=%q provider=%v cleanup=%q/%v/%d", preparationStatus, failureReason, sandboxStatus, providerID, cleanupStatus, cleanupMethod, cleanupAttempts)
	}
	if _, err := store.ClaimDueStartupCleanup(ctx, workspace.DefaultID, sandboxID, now.Add(3*time.Second), 2*time.Minute, 20); err == nil {
		t.Fatal("startup cleanup claimed post-create tombstone state; want delete-owner fence")
	}
}

func TestPostgreSQLPrepareSessionPostCreateTombstoneDeletesOnceAndReplaysWithoutProviderCall(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_post_create_tombstone_service_pg"
	preparationID := "prep_post_create_tombstone_service_pg"
	sandboxID := "sandbox_post_create_tombstone_service_pg"
	deleteCleanupID := "delcln_post_create_tombstone_service_pg"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, preparationID, sandboxID, "pending")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	provider := newSuccessfulRecordingProvider()
	provider.handle.Provider = testPostgreSQLSandboxProvider
	provider.createHook = func() {
		if _, err := admin.ExecContext(ctx,
			`UPDATE sessions SET lifecycle_state = 'deleted', delete_cleanup_id = $3
			  WHERE workspace_id = $1 AND id = $2`,
			string(workspace.DefaultID), sessionID, deleteCleanupID,
		); err != nil {
			t.Errorf("post-create tombstone: %v", err)
		}
	}
	service := NewService(store, provider,
		WithProviderName(testPostgreSQLSandboxProvider),
		WithClock(func() time.Time { return time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC) }),
		WithSessionPreparationStore(store),
		WithSessionResourcePreparer(&recordingSessionResourcePreparer{}),
	)
	request := SessionPrepareRequest{WorkspaceID: workspace.DefaultID, SessionID: sessionID, PreparationAttemptID: preparationID}

	result, err := service.PrepareSession(ctx, request)
	if err != nil || result.Status != SessionPrepareStatusFailed || result.FailureReason != sessionDeletedPreparationFailureReason {
		t.Fatalf("post-create tombstone result = %+v err=%v; want session-deleted failure", result, err)
	}
	if len(provider.createRequests) != 1 || len(provider.releaseReasons) != 1 || provider.releaseReasons[0] != ReleaseReasonDelete {
		t.Fatalf("provider create/release = %d/%v; want same job to delete exact created machine", len(provider.createRequests), provider.releaseReasons)
	}
	provider.createHook = nil
	replay, err := service.PrepareSession(ctx, request)
	if err != nil || replay.Status != SessionPrepareStatusNoop {
		t.Fatalf("deleted preparation replay = %+v err=%v; want claim-time tombstone noop", replay, err)
	}
	if len(provider.createRequests) != 1 || len(provider.releaseReasons) != 1 {
		t.Fatalf("provider calls after replay create=%d release=%v; want no duplicate", len(provider.createRequests), provider.releaseReasons)
	}
	var (
		preparationStatus string
		failureReason     string
		sandboxStatus     string
		providerID        sql.NullString
		cleanupStatus     string
	)
	if err := admin.QueryRowContext(ctx,
		`SELECT sp.status, sp.failure_reason, sb.status, sb.provider_sandbox_id, sb.cleanup_status
		   FROM session_preparations sp
		   JOIN sandboxes sb ON sb.workspace_id = sp.workspace_id AND sb.id = sp.sandbox_id
		  WHERE sp.workspace_id = $1 AND sp.session_id = $2 AND sp.preparation_attempt_id = $3`,
		string(workspace.DefaultID), sessionID, preparationID,
	).Scan(&preparationStatus, &failureReason, &sandboxStatus, &providerID, &cleanupStatus); err != nil {
		t.Fatalf("read post-create replay state: %v", err)
	}
	if preparationStatus != "failed" || failureReason != sessionDeletedPreparationFailureReason || sandboxStatus != string(StatusFailed) || providerID.Valid || cleanupStatus != string(CleanupStatusReleased) {
		t.Fatalf("post-create replay state preparation=%q/%q sandbox=%q provider=%v cleanup=%q", preparationStatus, failureReason, sandboxStatus, providerID, cleanupStatus)
	}
}

func TestPostgreSQLStoreStaleSessionPrepareAttemptNoops(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_stale_attempt"
	seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_stale_attempt", "sandbox_stale_attempt", "pending")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations
		    SET superseded_at = '2026-05-22T10:04:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = 'prep_stale_attempt'`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark stale preparation superseded: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		) VALUES ($1, $2, 'prep_fresh_attempt', 'env_sandbox', 1, 'sandbox_stale_attempt', 'pending',
			'2026-05-22T10:05:00Z', '2026-05-22T10:05:00Z')`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("seed fresh preparation attempt: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, "prep_stale_attempt", time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ClaimSessionPreparation stale: %v", err)
	}
	if claimed || preparation.PreparationAttemptID != "prep_stale_attempt" {
		t.Fatalf("stale claim preparation=%+v claimed=%v; want stale no-op", preparation, claimed)
	}
	if preparation.IsCurrent {
		t.Fatalf("stale claim preparation=%+v; want non-current attempt", preparation)
	}
	var status string
	if err := admin.QueryRowContext(ctx,
		`SELECT status
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = 'prep_stale_attempt'`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&status); err != nil {
		t.Fatalf("read stale preparation status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("stale preparation status = %q; want unchanged pending", status)
	}
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations
		    SET status = 'preparing'
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = 'prep_stale_attempt'`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("mark stale attempt preparing: %v", err)
	}
	err = store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_stale_attempt", SessionPreparationReadyUpdate{ReadyAt: time.Date(2026, 5, 23, 12, 1, 0, 0, time.UTC)})
	if !errors.Is(err, ErrStalePreparationAttempt) {
		t.Fatalf("MarkSessionPreparationReady stale err = %v; want ErrStalePreparationAttempt", err)
	}
	if err := admin.QueryRowContext(ctx,
		`SELECT status
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = 'prep_stale_attempt'`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&status); err != nil {
		t.Fatalf("read stale preparation status after mark ready: %v", err)
	}
	if status != "preparing" {
		t.Fatalf("stale preparation status = %q; want unchanged preparing", status)
	}
}

func TestPostgreSQLStoreDeletedSessionPreparationDoesNotClaim(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	const sessionID = "sesn_prepare_deleted_gate"
	seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_deleted_gate", "sandbox_deleted_gate", "pending")
	if _, err := admin.ExecContext(ctx, `UPDATE sessions SET lifecycle_state='deleted' WHERE id=$1`, sessionID); err != nil {
		t.Fatalf("mark session deleted: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, "prep_deleted_gate", time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ClaimSessionPreparation deleted: %v", err)
	}
	if claimed || preparation.PreparationAttemptID != "" {
		t.Fatalf("deleted preparation = %+v claimed=%v; want silent no-op", preparation, claimed)
	}
	var status string
	if err := admin.QueryRowContext(ctx, `SELECT status FROM session_preparations WHERE session_id=$1`, sessionID).Scan(&status); err != nil {
		t.Fatalf("read preparation status: %v", err)
	}
	if status != "pending" {
		t.Fatalf("deleted preparation status = %q; want pending without provider admission", status)
	}
}

func TestPostgreSQLStoreDeletionPendingResourceDetachLifecycle(t *testing.T) {
	tests := []struct {
		name              string
		resourceType      string
		seed              func(*testing.T, *sql.DB, string, string, string)
		assertFileDeleted bool
	}{
		{
			name:         "file",
			resourceType: "file",
			seed: func(t *testing.T, admin *sql.DB, sessionID string, deletedID string, successorID string) {
				seedFilePreparationResource(t, admin, workspace.DefaultID, sessionID, deletedID, "file_source_deleted_"+sessionID, "file_session_deleted_"+sessionID, "obj_deleted_"+sessionID, "/workspace/replacement")
				seedFilePreparationResource(t, admin, workspace.DefaultID, sessionID, successorID, "file_source_successor_"+sessionID, "file_session_successor_"+sessionID, "obj_successor_"+sessionID, "/workspace/replacement")
			},
			assertFileDeleted: true,
		},
		{
			name:         "github_repository",
			resourceType: "github_repository",
			seed: func(t *testing.T, admin *sql.DB, sessionID string, deletedID string, successorID string) {
				seedGitHubPreparationResource(t, admin, workspace.DefaultID, sessionID, deletedID, "https://github.com/tetral-ai/old", "/workspace/replacement", "", "")
				seedGitHubPreparationResource(t, admin, workspace.DefaultID, sessionID, successorID, "https://github.com/tetral-ai/new", "/workspace/replacement", "", "")
			},
		},
		{
			name:         "memory_store",
			resourceType: "memory_store",
			seed: func(t *testing.T, admin *sql.DB, sessionID string, deletedID string, successorID string) {
				seedMemoryPreparationResource(t, admin, workspace.DefaultID, sessionID, deletedID, "memstore_deleted_"+sessionID, "/mnt/memory/replacement")
				seedMemoryPreparationResource(t, admin, workspace.DefaultID, sessionID, successorID, "memstore_successor_"+sessionID, "/mnt/memory/replacement")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := newSandboxStoreTestDB(t)
			ctx := context.Background()
			sessionID := "sesn_detach_" + tc.name
			attemptID := "prep_detach_" + tc.name
			deletedID := "sesrsc_deleted_" + tc.name
			successorID := "sesrsc_successor_" + tc.name
			seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
			seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, attemptID, "sandbox_"+tc.name, "preparing")
			tc.seed(t, admin, sessionID, deletedID, successorID)
			if _, err := admin.ExecContext(ctx,
				`UPDATE session_resources
				    SET delete_requested_at = '2026-07-13T10:00:00Z', updated_at = '2026-07-13T10:00:00Z'
				  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
				string(workspace.DefaultID), sessionID, deletedID,
			); err != nil {
				t.Fatalf("request resource deletion: %v", err)
			}
			store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
			coordinator := &sessionPreparationResourceCleanupCoordinator{
				store:                store,
				workspaceID:          workspace.DefaultID,
				sessionID:            sessionID,
				preparationAttemptID: attemptID,
				clock:                func() time.Time { return time.Date(2026, 7, 13, 10, 1, 0, 0, time.UTC) },
			}

			resources, err := store.ListSessionPreparationResources(ctx, workspace.DefaultID, sessionID)
			if err != nil {
				t.Fatalf("ListSessionPreparationResources before detach: %v", err)
			}
			assertDeletionPendingResourceSplit(t, tc.resourceType, resources, deletedID, successorID)

			pending, err := store.CheckSessionPreparationResourceCleanup(ctx, workspace.DefaultID, sessionID, attemptID, deletedID)
			if err != nil {
				t.Fatalf("CheckSessionPreparationResourceCleanup: %v", err)
			}
			if !pending {
				t.Fatal("cleanup pending = false; want deletion-pending owner eligible for removal")
			}

			// A provider/materializer removal failure never reaches durable detach
			// or permits the successor path to be treated as cleaned up.
			removalFailure := errors.New("removal failed")
			if err := coordinator.CleanupSessionResource(ctx, deletedID, func(context.Context) error { return removalFailure }); !errors.Is(err, removalFailure) {
				t.Fatalf("failed cleanup err = %v; want removal failure", err)
			}
			assertSessionResourceDetached(t, admin, sessionID, deletedID, false)
			resources, err = store.ListSessionPreparationResources(ctx, workspace.DefaultID, sessionID)
			if err != nil {
				t.Fatalf("ListSessionPreparationResources after simulated removal failure: %v", err)
			}
			assertDeletionPendingResourceSplit(t, tc.resourceType, resources, deletedID, successorID)

			detachedAt := time.Date(2026, 7, 13, 10, 1, 0, 0, time.UTC)
			removed := false
			if err := coordinator.CleanupSessionResource(ctx, deletedID, func(context.Context) error {
				removed = true
				return nil
			}); err != nil {
				t.Fatalf("CleanupSessionResource: %v", err)
			}
			if !removed {
				t.Fatal("provider/materializer removal was not invoked before detach")
			}
			assertSessionResourceDetached(t, admin, sessionID, deletedID, true)
			assertSessionResourceDetached(t, admin, sessionID, successorID, false)
			assertSessionFileTombstone(t, admin, sessionID, deletedID, tc.assertFileDeleted)

			// Crash retry after detach sees no cleanup owner and cannot detach the
			// active same-path successor.
			pending, err = store.CheckSessionPreparationResourceCleanup(ctx, workspace.DefaultID, sessionID, attemptID, deletedID)
			if err != nil {
				t.Fatalf("CheckSessionPreparationResourceCleanup after detach: %v", err)
			}
			if pending {
				t.Fatal("cleanup pending = true after detach; old removal could erase successor")
			}
			removed = false
			if err := coordinator.CleanupSessionResource(ctx, deletedID, func(context.Context) error {
				removed = true
				return nil
			}); err != nil {
				t.Fatalf("crash retry cleanup: %v", err)
			}
			if removed {
				t.Fatal("crash retry re-ran removal after durable detach; successor path is at risk")
			}
			if err := store.DetachSessionPreparationResource(ctx, workspace.DefaultID, sessionID, attemptID, successorID, detachedAt.Add(time.Second)); err != nil {
				t.Fatalf("Detach active successor: %v", err)
			}
			assertSessionResourceDetached(t, admin, sessionID, successorID, false)

			if _, err := admin.ExecContext(ctx,
				`UPDATE session_resources
				    SET delete_requested_at = '2026-07-13T10:02:30Z', updated_at = '2026-07-13T10:02:30Z'
				  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
				string(workspace.DefaultID), sessionID, successorID,
			); err != nil {
				t.Fatalf("request successor deletion before stale retry: %v", err)
			}
			removed = false
			if err := coordinator.CleanupSessionResource(ctx, successorID, func(context.Context) error {
				removed = true
				if _, err := admin.ExecContext(ctx,
					`UPDATE session_preparations
					    SET status = 'ready', ready_at = '2026-07-13T10:02:00Z', superseded_at = '2026-07-13T10:03:00Z', updated_at = '2026-07-13T10:03:00Z'
					  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
					string(workspace.DefaultID), sessionID, attemptID,
				); err != nil {
					return err
				}
				seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_successor_"+tc.name, "sandbox_"+tc.name, "ready")
				return nil
			}); !errors.Is(err, ErrStalePreparationAttempt) {
				t.Fatalf("post-ACK stale cleanup err = %v; want ErrStalePreparationAttempt", err)
			}
			if !removed {
				t.Fatal("removal ACK was not observed before the detach stale fence")
			}
			assertSessionResourceDetached(t, admin, sessionID, successorID, false)

			removed = false
			if err := coordinator.CleanupSessionResource(ctx, successorID, func(context.Context) error {
				removed = true
				return nil
			}); !errors.Is(err, ErrStalePreparationAttempt) {
				t.Fatalf("stale cleanup preflight err = %v; want ErrStalePreparationAttempt", err)
			}
			if removed {
				t.Fatal("stale old attempt invoked removal against the successfully bound successor")
			}
			if err := store.DetachSessionPreparationResource(ctx, workspace.DefaultID, sessionID, attemptID, successorID, detachedAt.Add(2*time.Second)); !errors.Is(err, ErrStalePreparationAttempt) {
				t.Fatalf("stale detach err = %v; want ErrStalePreparationAttempt", err)
			}
			assertSessionResourceDetached(t, admin, sessionID, successorID, false)
		})
	}
}

func assertDeletionPendingResourceSplit(t *testing.T, resourceType string, resources ResourceSetup, deletedID string, successorID string) {
	t.Helper()
	switch resourceType {
	case "file":
		if len(resources.DeletedFiles) != 1 || resources.DeletedFiles[0].ResourceID != deletedID || len(resources.Files) != 1 || resources.Files[0].ResourceID != successorID {
			t.Fatalf("file resources active=%+v deleted=%+v; want successor active and cleanup owner separate", resources.Files, resources.DeletedFiles)
		}
	case "github_repository":
		if len(resources.DeletedGitHubRepositories) != 1 || resources.DeletedGitHubRepositories[0].ResourceID != deletedID || len(resources.GitHubRepositories) != 1 || resources.GitHubRepositories[0].ResourceID != successorID {
			t.Fatalf("GitHub resources active=%+v deleted=%+v; want successor active and cleanup owner separate", resources.GitHubRepositories, resources.DeletedGitHubRepositories)
		}
	case "memory_store":
		if len(resources.DeletedMemoryStores) != 1 || resources.DeletedMemoryStores[0].ResourceID != deletedID || len(resources.MemoryStores) != 1 || resources.MemoryStores[0].ResourceID != successorID {
			t.Fatalf("memory resources active=%+v deleted=%+v; want successor active and cleanup owner separate", resources.MemoryStores, resources.DeletedMemoryStores)
		}
	default:
		t.Fatalf("unsupported resource type %q", resourceType)
	}
}

func assertSessionResourceDetached(t *testing.T, admin *sql.DB, sessionID string, resourceID string, want bool) {
	t.Helper()
	var detachedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT detached_at FROM session_resources
		  WHERE workspace_id = $1 AND session_id = $2 AND resource_id = $3`,
		string(workspace.DefaultID), sessionID, resourceID,
	).Scan(&detachedAt); err != nil {
		t.Fatalf("read resource %s detach: %v", resourceID, err)
	}
	if detachedAt.Valid != want {
		t.Fatalf("resource %s detached=%v; want %v", resourceID, detachedAt, want)
	}
}

func assertSessionFileTombstone(t *testing.T, admin *sql.DB, sessionID string, resourceID string, want bool) {
	t.Helper()
	var resourceType string
	var deletedAt sql.NullString
	err := admin.QueryRowContext(context.Background(),
		`SELECT sr.type, f.deleted_at
		   FROM session_resources sr
		   LEFT JOIN session_file_resources sfr
		     ON sfr.workspace_id = sr.workspace_id AND sfr.session_id = sr.session_id AND sfr.resource_id = sr.resource_id
		   LEFT JOIN files f ON f.workspace_id = sfr.workspace_id AND f.file_id = sfr.file_id
		  WHERE sr.workspace_id = $1 AND sr.session_id = $2 AND sr.resource_id = $3`,
		string(workspace.DefaultID), sessionID, resourceID,
	).Scan(&resourceType, &deletedAt)
	if err != nil {
		t.Fatalf("read resource %s file tombstone: %v", resourceID, err)
	}
	if deletedAt.Valid != want {
		t.Fatalf("resource %s type=%s file tombstone=%v; want %v", resourceID, resourceType, deletedAt, want)
	}
}

func TestPostgreSQLStoreMarkReadyFanoutQueuesPendingRuntimeInput(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_fanout"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_ready_fanout", "sandbox_ready_fanout", "preparing")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_msg_1", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"hello"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_msg_2", 2, eventTypeUserMessage, `{"content":[{"type":"text","text":"again"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_interrupt", 3, eventTypeUserInterrupt, `{}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_done", 4, eventTypeUserMessage, `{"content":[{"type":"text","text":"done"}]}`, "2026-05-22T10:30:00Z")
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_ready_fanout", SessionPreparationReadyUpdate{ReadyAt: now}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("queue jobs = %#v; want messages and interrupt fanout jobs", jobs)
	}
	assertSandboxRuntimeInputJob(t, findSandboxRuntimeInputJob(t, jobs, runtimeInputKindMessages), sessionID, threadID, runtimeInputKindMessages, 0, []string{"sevt_ready_msg_1", "sevt_ready_msg_2"}, 1, 2)
	assertSandboxRuntimeInputJob(t, findSandboxRuntimeInputJob(t, jobs, runtimeInputKindInterruptControl), sessionID, threadID, runtimeInputKindInterruptControl, runtimeInputInterruptQueuePriority, []string{"sevt_ready_interrupt"}, 3, 3)
	if got := countUnprocessedSandboxEvents(t, admin, sessionID); got != 3 {
		t.Fatalf("unprocessed runtime input events = %d; want fanout to leave three events for Bridge commit", got)
	}
}

func TestPostgreSQLStoreMarkReadyFanoutDoesNotMixRecordedBirths(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_birth_segments"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_birth_current", "sandbox_birth_segments", "preparing")
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at, superseded_at
		) SELECT workspace_id, session_id, 'prep_birth_earlier', environment_id, environment_generation,
		         sandbox_id, 'failed', '2026-05-22T09:59:00Z', '2026-05-22T09:59:00Z', '2026-05-22T10:00:00Z'
		    FROM session_preparations
		   WHERE workspace_id = $1
		     AND session_id = $2
		     AND preparation_attempt_id = 'prep_birth_current'`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("seed earlier birth attempt: %v", err)
	}
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_birth_earlier", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"earlier"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_birth_current", 2, eventTypeUserMessage, `{"content":[{"type":"text","text":"current"}]}`, "")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_events
		    SET preparation_attempt_id = 'prep_birth_earlier'
		  WHERE workspace_id = $1
		    AND event_id = 'sevt_birth_earlier'`,
		string(workspace.DefaultID),
	); err != nil {
		t.Fatalf("stamp earlier event birth: %v", err)
	}

	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_birth_current", SessionPreparationReadyUpdate{
		ReadyAt: time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("queue jobs = %#v; want one segment per recorded birth", jobs)
	}
	got := make(map[string][]string, len(jobs))
	for _, job := range jobs {
		var payload struct {
			EventIDs             []string `json:"event_ids"`
			PreparationAttemptID string   `json:"preparation_attempt_id"`
		}
		if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
			t.Fatalf("decode birth-segment payload: %v", err)
		}
		got[payload.PreparationAttemptID] = payload.EventIDs
	}
	if !reflect.DeepEqual(got["prep_birth_earlier"], []string{"sevt_birth_earlier"}) ||
		!reflect.DeepEqual(got["prep_birth_current"], []string{"sevt_birth_current"}) {
		t.Fatalf("birth-segment payloads = %#v; want separate earlier/current segments", got)
	}
}

func TestPostgreSQLStoreMarkReadyFanoutUsesGlobalInsertOrderAcrossThreads(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_global_order"
	mainThreadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	childThreadID := "thread_prepare_ready_child"
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status, task_name,
			created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, $4, 'subagent', 'public', 'idle', 'ready-order-child', $5, $5, $5)`,
		string(workspace.DefaultID), childThreadID, sessionID, mainThreadID, "2026-05-22T10:00:00Z"); err != nil {
		t.Fatalf("seed child thread: %v", err)
	}
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_ready_global_order", "sandbox_ready_global_order", "preparing")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, mainThreadID, "sevt_global_first", 9, eventTypeUserMessage, `{"content":[{"type":"text","text":"first"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, childThreadID, "sevt_global_second", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"second"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, mainThreadID, "sevt_global_third", 10, eventTypeUserMessage, `{"content":[{"type":"text","text":"third"}]}`, "")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_ready_global_order", SessionPreparationReadyUpdate{ReadyAt: time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 3 {
		t.Fatalf("queue jobs = %#v; want three globally ordered segments", jobs)
	}
	assertSandboxRuntimeInputJob(t, jobs[0], sessionID, mainThreadID, runtimeInputKindMessages, 0, []string{"sevt_global_first"}, 9, 9)
	assertSandboxRuntimeInputJob(t, jobs[1], sessionID, childThreadID, runtimeInputKindMessages, 0, []string{"sevt_global_second"}, 1, 1)
	assertSandboxRuntimeInputJob(t, jobs[2], sessionID, mainThreadID, runtimeInputKindMessages, 0, []string{"sevt_global_third"}, 10, 10)
}

func TestPostgreSQLStoreMarkReadyFanoutChunksLargeBacklogInStreamOrder(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_large_backlog"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_ready_large_backlog", "sandbox_ready_large_backlog", "preparing")
	eventIDs := make([]string, queue.MaxRuntimeInputEventRefsPerJob+1)
	for i := range eventIDs {
		eventIDs[i] = fmt.Sprintf("sevt_large_backlog_%04d", i+1)
		seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, eventIDs[i], int64(i+1), eventTypeUserMessage, `{"content":[{"type":"text","text":"queued"}]}`, "")
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_ready_large_backlog", SessionPreparationReadyUpdate{
		ReadyAt: time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("queue jobs = %d; want two ordered chunks", len(jobs))
	}
	assertSandboxRuntimeInputJob(t, jobs[0], sessionID, threadID, runtimeInputKindMessages, 0, eventIDs[:queue.MaxRuntimeInputEventRefsPerJob], 1, queue.MaxRuntimeInputEventRefsPerJob)
	assertSandboxRuntimeInputJob(t, jobs[1], sessionID, threadID, runtimeInputKindMessages, 0, eventIDs[queue.MaxRuntimeInputEventRefsPerJob:], queue.MaxRuntimeInputEventRefsPerJob+1, queue.MaxRuntimeInputEventRefsPerJob+1)
	if jobs[0].dedupeKey == jobs[1].dedupeKey {
		t.Fatalf("chunk dedupe keys are equal: %q", jobs[0].dedupeKey)
	}
}

func TestPostgreSQLStoreMarkReadyFanoutSkipsMessagesSupersededByProcessedInterrupt(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_interrupt_fence"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_ready_interrupt_fence", "sandbox_ready_interrupt_fence", "preparing")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_old_msg_1", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"old 1"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_old_msg_2", 2, eventTypeUserMessage, `{"content":[{"type":"text","text":"old 2"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_processed_interrupt", 3, eventTypeUserInterrupt, `{}`, "2026-05-22T10:30:00Z")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_new_msg", 4, eventTypeUserMessage, `{"content":[{"type":"text","text":"new"}]}`, "")
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_ready_interrupt_fence", SessionPreparationReadyUpdate{ReadyAt: now}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want only post-fence message fanout", jobs)
	}
	assertSandboxRuntimeInputJob(t, findSandboxRuntimeInputJob(t, jobs, runtimeInputKindMessages), sessionID, threadID, runtimeInputKindMessages, 0, []string{"sevt_ready_new_msg"}, 4, 4)
}

func TestPostgreSQLStoreMarkReadyFanoutSkipsEventsWithActiveRuntimeInput(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_fanout_dedupe"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_ready_fanout_dedupe", "sandbox_ready_fanout_dedupe", "preparing")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_already_queued", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"queued"}]}`, "")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_ready_orphan", 2, eventTypeUserMessage, `{"content":[{"type":"text","text":"orphan"}]}`, "")
	seedSandboxRuntimeInputJob(t, admin, workspace.DefaultID, sessionID, threadID, "rin_existing_ready", []string{"sevt_ready_already_queued"}, 1, 1, runtimeInputKindMessages)
	now := time.Date(2026, 5, 23, 12, 30, 0, 0, time.UTC)
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	if err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_ready_fanout_dedupe", SessionPreparationReadyUpdate{ReadyAt: now}); err != nil {
		t.Fatalf("MarkSessionPreparationReady: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 2 {
		t.Fatalf("queue jobs = %#v; want existing runtime_input plus one fanout job", jobs)
	}
	assertSandboxRuntimeInputJob(t, findSandboxRuntimeInputJobByEvent(t, jobs, "sevt_ready_already_queued"), sessionID, threadID, runtimeInputKindMessages, 0, []string{"sevt_ready_already_queued"}, 1, 1)
	assertSandboxRuntimeInputJob(t, findSandboxRuntimeInputJobByEvent(t, jobs, "sevt_ready_orphan"), sessionID, threadID, runtimeInputKindMessages, 0, []string{"sevt_ready_orphan"}, 2, 2)
}

func TestPostgreSQLStoreMarkReadyRequiresRuntimeStatusFence(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_ready_missing_runtime_status"
	seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_missing_runtime_status", "sandbox_missing_runtime_status", "preparing")
	if _, err := admin.ExecContext(ctx,
		`DELETE FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("delete runtime status: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	err := store.MarkSessionPreparationReady(ctx, workspace.DefaultID, sessionID, "prep_missing_runtime_status", SessionPreparationReadyUpdate{ReadyAt: time.Date(2026, 5, 23, 13, 0, 0, 0, time.UTC)})
	if err == nil {
		t.Fatal("MarkSessionPreparationReady accepted a session without runtime status fence")
	}
	if !strings.Contains(err.Error(), "session_runtime_status invariant missing") {
		t.Fatalf("error = %T %v; want missing runtime status invariant", err, err)
	}
	var status string
	if err := admin.QueryRowContext(ctx,
		`SELECT status
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID),
		sessionID,
		"prep_missing_runtime_status",
	).Scan(&status); err != nil {
		t.Fatalf("read preparation status: %v", err)
	}
	if status != "preparing" {
		t.Fatalf("preparation status = %q; want preparing after missing runtime status", status)
	}
	if got := len(readSandboxQueueJobs(t, admin, sessionID)); got != 0 {
		t.Fatalf("queue jobs = %d; want none without runtime status fence", got)
	}
}

func TestPostgreSQLStoreMarksPreparationFailedWithReason(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	seedSandboxSession(t, admin, workspace.DefaultID, "sesn_prepare_failed")
	seedSessionPreparation(t, admin, workspace.DefaultID, "sesn_prepare_failed", "prep_failed", "sandbox_prepare_failed", "pending")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, 5, 23, 11, 0, 0, 0, time.UTC)
	if _, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, "sesn_prepare_failed", "prep_failed", now); err != nil || !claimed {
		t.Fatalf("ClaimSessionPreparation claimed=%v err=%v; want claimed", claimed, err)
	}
	if err := store.MarkSessionPreparationFailed(ctx, workspace.DefaultID, "sesn_prepare_failed", "prep_failed", SessionPreparationFailureUpdate{
		FailedAt:      now.Add(time.Minute),
		FailureStage:  "resource_preparation",
		LastErrorKind: GitHubCredentialRequiredReason,
		FailureReason: GitHubCredentialRequiredReason,
		Retryable:     false,
	}); err != nil {
		t.Fatalf("MarkSessionPreparationFailed: %v", err)
	}
	var status string
	var failureReason sql.NullString
	var retryable sql.NullBool
	if err := admin.QueryRowContext(ctx,
		`SELECT status, failure_reason, retryable FROM session_preparations WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), "sesn_prepare_failed", "prep_failed",
	).Scan(&status, &failureReason, &retryable); err != nil {
		t.Fatalf("read failed preparation: %v", err)
	}
	if status != "failed" || !failureReason.Valid || failureReason.String != GitHubCredentialRequiredReason || !retryable.Valid || retryable.Bool {
		t.Fatalf("failed row status=%q reason=%v retryable=%v; want terminal github_credential_required", status, failureReason, retryable)
	}
	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, "sesn_prepare_failed", "prep_failed", now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("Claim failed preparation: %v", err)
	}
	if claimed || preparation.Status != "failed" || preparation.FailureReason != GitHubCredentialRequiredReason {
		t.Fatalf("failed replay preparation = %+v claimed=%v; want no claim with reason", preparation, claimed)
	}
}

func TestPostgreSQLStoreMarkFailedFanoutQueuesPendingRuntimeInputForBridgeSettlement(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_failed_fanout"
	threadID := seedSandboxSessionRuntimeFence(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_failed_fanout", "sandbox_failed_fanout", "preparing")
	seedSandboxSessionEvent(t, admin, workspace.DefaultID, sessionID, threadID, "sevt_failed_fanout", 1, eventTypeUserMessage, `{"content":[{"type":"text","text":"clone"}]}`, "")
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))
	failedAt := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)

	if err := store.MarkSessionPreparationFailed(ctx, workspace.DefaultID, sessionID, "prep_failed_fanout", SessionPreparationFailureUpdate{
		FailedAt:           failedAt,
		FailureStage:       "github_repository_clone",
		LastErrorKind:      GitHubCredentialRequiredReason,
		FailureReason:      GitHubCredentialRequiredReason,
		FailureResourceID:  "sesrsc_repo",
		FailureResourceURL: "https://github.com/tetral-ai/private",
		Retryable:          false,
	}); err != nil {
		t.Fatalf("MarkSessionPreparationFailed: %v", err)
	}

	jobs := readSandboxQueueJobs(t, admin, sessionID)
	if len(jobs) != 1 {
		t.Fatalf("queue jobs = %#v; want one runtime input for Bridge failure settlement", jobs)
	}
	assertSandboxRuntimeInputJob(t, jobs[0], sessionID, threadID, runtimeInputKindMessages, 0, []string{"sevt_failed_fanout"}, 1, 1)
	var payload struct {
		PreparationAttemptID string `json:"preparation_attempt_id"`
	}
	if err := json.Unmarshal([]byte(jobs[0].payloadJSON), &payload); err != nil {
		t.Fatalf("decode fenced runtime input: %v", err)
	}
	if payload.PreparationAttemptID != "prep_failed_fanout" {
		t.Fatalf("birth preparation attempt = %q; want prep_failed_fanout", payload.PreparationAttemptID)
	}
	var resourceID sql.NullString
	var resourceURL sql.NullString
	if err := admin.QueryRowContext(ctx,
		`SELECT failure_resource_id, failure_resource_url
		   FROM session_preparations
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspace.DefaultID), sessionID, "prep_failed_fanout",
	).Scan(&resourceID, &resourceURL); err != nil {
		t.Fatalf("read failure identity: %v", err)
	}
	if resourceID.String != "sesrsc_repo" || resourceURL.String != "https://github.com/tetral-ai/private" {
		t.Fatalf("failure identity = %v/%v; want repository identity", resourceID, resourceURL)
	}
}

func newSandboxStoreTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}

func seedSandboxMemoryStore(t *testing.T, admin *sql.DB, workspaceID string, storeID string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		 VALUES ($1, $2, $2, '2026-05-22T10:00:00Z', '2026-05-22T10:00:00Z')`,
		workspaceID,
		storeID,
	); err != nil {
		t.Fatalf("seed memory store: %v", err)
	}
}

func seedSandboxMemory(t *testing.T, admin *sql.DB, workspaceID string, storeID string, memoryID string, memoryPath string, content string, deleted bool) {
	t.Helper()
	tx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin seed memory: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	versionID := memoryID + "_ver"
	now := "2026-05-22T10:00:00Z"
	if deleted {
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO memories (
				workspace_id, memory_store_id, memory_id, current_version_id, path,
				content_sha256, content_size_bytes, created_at, updated_at, deleted_at
			) VALUES ($1, $2, $3, $4, $5, NULL, NULL, $6, $6, $6)`,
			workspaceID,
			storeID,
			memoryID,
			versionID,
			memoryPath,
			now,
		); err != nil {
			t.Fatalf("seed deleted memory head: %v", err)
		}
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO memory_versions (
				workspace_id, memory_store_id, memory_id, memory_version_id, operation, path,
				created_at, created_actor_type, created_session_id
			) VALUES ($1, $2, $3, $4, 'deleted', $5, $6, 'session_actor', 'sesn_memory_snapshot')`,
			workspaceID,
			storeID,
			memoryID,
			versionID,
			memoryPath,
			now,
		); err != nil {
			t.Fatalf("seed deleted memory version: %v", err)
		}
	} else {
		hash := sandboxTestSHA256Hex(content)
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO memories (
				workspace_id, memory_store_id, memory_id, current_version_id, path,
				content_sha256, content_size_bytes, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)`,
			workspaceID,
			storeID,
			memoryID,
			versionID,
			memoryPath,
			hash,
			len([]byte(content)),
			now,
		); err != nil {
			t.Fatalf("seed active memory head: %v", err)
		}
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO memory_versions (
				workspace_id, memory_store_id, memory_id, memory_version_id, operation, path, content,
				content_sha256, content_size_bytes, created_at, created_actor_type, created_session_id
			) VALUES ($1, $2, $3, $4, 'created', $5, $6, $7, $8, $9, 'session_actor', 'sesn_memory_snapshot')`,
			workspaceID,
			storeID,
			memoryID,
			versionID,
			memoryPath,
			content,
			hash,
			len([]byte(content)),
			now,
		); err != nil {
			t.Fatalf("seed active memory version: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed memory: %v", err)
	}
}

func TestPostgreSQLStoreClaimPreparationLoadsPreviousResourceRoots(t *testing.T) {
	runtime, admin := newSandboxStoreTestDB(t)
	ctx := context.Background()
	sessionID := "sesn_prepare_previous_roots"
	seedSandboxSession(t, admin, workspace.DefaultID, sessionID)
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_previous_roots_old", "sandbox_previous_roots", "ready")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations
		    SET ready_at = '2026-05-22T10:01:00Z',
		        resource_cred_expires_at = '2026-05-22T11:00:00Z',
		        resource_roots_json = '[{"path":"/mnt/session/uploads/file_existing","mode":"read"}]',
		        superseded_at = '2026-05-22T10:02:00Z',
		        updated_at = '2026-05-22T10:02:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = 'prep_previous_roots_old'`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("seed previous ready roots: %v", err)
	}
	seedSessionPreparation(t, admin, workspace.DefaultID, sessionID, "prep_previous_roots_new", "sandbox_previous_roots", "pending")
	if _, err := admin.ExecContext(ctx,
		`UPDATE session_preparations
		    SET resource_cred_expires_at = '2026-05-22T11:00:00Z',
		        created_at = '2026-05-22T10:03:00Z',
		        updated_at = '2026-05-22T10:03:00Z'
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = 'prep_previous_roots_new'`,
		string(workspace.DefaultID),
		sessionID,
	); err != nil {
		t.Fatalf("seed fresh incremental attempt: %v", err)
	}
	store := NewPostgreSQLStore(dbconnect.NewClientForTesting(runtime))

	preparation, claimed, err := store.ClaimSessionPreparation(ctx, workspace.DefaultID, sessionID, "prep_previous_roots_new", time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ClaimSessionPreparation: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimSessionPreparation did not claim fresh preparation")
	}
	if got, want := preparation.ResourceRootsJSON, `[{"path":"/mnt/session/uploads/file_existing","mode":"read"}]`; got != want {
		t.Fatalf("ResourceRootsJSON = %q; want previous ready roots %q", got, want)
	}
}

func seedSessionPreparation(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, preparationID string, sandboxID string, status string) {
	t.Helper()
	now := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_preparations (
			workspace_id, session_id, preparation_attempt_id, environment_id, environment_generation,
			sandbox_id, status, created_at, updated_at
		) VALUES ($1, $2, $3, 'env_sandbox', 1, $4, $5, $6, $6)`,
		string(ws), sessionID, preparationID, sandboxID, status, now,
	); err != nil {
		t.Fatalf("seed session preparation: %v", err)
	}
}

func seedSandboxSessionRuntimeFence(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string) string {
	t.Helper()
	seedSandboxSession(t, admin, ws, sessionID)
	threadID := "thr_" + sessionID
	createdAt := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET main_thread_id = $3,
		        lifecycle_state = 'active'
		  WHERE workspace_id = $1
		    AND id = $2`,
		string(ws),
		sessionID,
		threadID,
	); err != nil {
		t.Fatalf("seed session main thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_threads (
			workspace_id, id, session_id, role, visibility, status, created_at, last_active_at, updated_at
		) VALUES ($1, $2, $3, 'main', 'public', 'idle', $4, $4, $4)`,
		string(ws),
		threadID,
		sessionID,
		createdAt,
	); err != nil {
		t.Fatalf("seed session thread: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, created_at, updated_at
		) VALUES ($1, $2, 'idle', $3, $3)
		ON CONFLICT (workspace_id, session_id) DO UPDATE
		    SET status = EXCLUDED.status,
		        updated_at = EXCLUDED.updated_at`,
		string(ws),
		sessionID,
		createdAt,
	); err != nil {
		t.Fatalf("seed session runtime status: %v", err)
	}
	return threadID
}

func seedSandboxSessionEvent(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, threadID string, eventID string, sequence int64, eventType string, payload string, processedAt string) {
	t.Helper()
	now := "2026-05-22T10:00:00Z"
	var preparationAttemptID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		  ORDER BY created_at DESC, preparation_attempt_id DESC
		  LIMIT 1`,
		string(ws),
		sessionID,
	).Scan(&preparationAttemptID); err != nil {
		t.Fatalf("read sandbox session event birth: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			preparation_attempt_id, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)`,
		string(ws),
		sessionID,
		threadID,
		eventID,
		sequence,
		eventType,
		payload,
		preparationAttemptID,
		now,
		nullableEmptyString(processedAt),
	); err != nil {
		t.Fatalf("seed session event: %v", err)
	}
	var insertPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, 1, 'public', TRUE, $5)
		RETURNING stream_position`,
		string(ws), sessionID, eventID, threadID, now).Scan(&insertPosition); err != nil {
		t.Fatalf("seed session event stream change: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events
		    SET insert_stream_position = $3
		  WHERE workspace_id = $1 AND session_id = $2 AND event_id = $4`,
		string(ws), sessionID, insertPosition, eventID); err != nil {
		t.Fatalf("set session event insert position: %v", err)
	}
}

func seedSandboxRuntimeInputJob(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, threadID string, runtimeInputID string, eventIDs []string, sequenceFrom int64, sequenceTo int64, inputKind string) {
	t.Helper()
	var preparationAttemptID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		  ORDER BY created_at DESC, preparation_attempt_id DESC
		  LIMIT 1`,
		string(ws),
		sessionID,
	).Scan(&preparationAttemptID); err != nil {
		t.Fatalf("read runtime input birth preparation attempt: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"workspace_id":           string(ws),
		"session_id":             sessionID,
		"session_thread_id":      threadID,
		"runtime_input_id":       runtimeInputID,
		"event_ids":              eventIDs,
		"sequence_from":          sequenceFrom,
		"sequence_to":            sequenceTo,
		"input_kind":             inputKind,
		"preparation_attempt_id": preparationAttemptID,
	})
	if err != nil {
		t.Fatalf("marshal runtime input job: %v", err)
	}
	ctx := context.Background()
	sqlTx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin runtime input job seed: %v", err)
	}
	defer func() { _ = sqlTx.Rollback() }()
	queueTx := dbconnect.NewTxForTesting(
		sqlTx,
		dbconnect.NewClientForTesting(admin),
		"sandbox.seed_runtime_input_job",
	)
	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	if _, err := queue.EnqueueTx(ctx, queueTx, queue.EnqueueRequest{
		WorkspaceID:  ws,
		Kind:         queue.KindRuntimeInput,
		PartitionKey: queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:    queue.FormatRuntimeInputDedupeKey(ws, sessionID, runtimeInputID),
		PayloadJSON:  payload,
		Now:          now,
	}); err != nil {
		t.Fatalf("seed runtime input job: %v", err)
	}
	if err := sqlTx.Commit(); err != nil {
		t.Fatalf("commit runtime input job seed: %v", err)
	}
}

func seedMemoryPreparationResource(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, resourceID string, memoryStoreID string, mountPath string) {
	t.Helper()
	now := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO memory_stores (workspace_id, memory_store_id, name, created_at, updated_at)
		 VALUES ($1, $2, $2, $3, $3)`, string(ws), memoryStoreID, now); err != nil {
		t.Fatalf("seed memory store: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'memory_store', $4, $4)`, string(ws), sessionID, resourceID, now); err != nil {
		t.Fatalf("seed memory session resource: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_memory_store_resources (
			workspace_id, session_id, resource_id, memory_store_id, access, instructions, name, description, mount_path
		) VALUES ($1, $2, $3, $4, 'read_write', '', $4, '', $5)`,
		string(ws), sessionID, resourceID, memoryStoreID, mountPath); err != nil {
		t.Fatalf("seed memory session resource detail: %v", err)
	}
}

func seedGitHubPreparationResource(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, resourceID string, repoURL string, mountPath string, checkoutType string, checkoutRef string) {
	t.Helper()
	now := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'github_repository', $4, $4)`,
		string(ws), sessionID, resourceID, now,
	); err != nil {
		t.Fatalf("seed github session resource: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_github_repository_resources (
			workspace_id, session_id, resource_id, url, mount_path, checkout_type, checkout_ref,
			authorization_token_encrypted
		) VALUES ($1, $2, $3, $4, $5, $6, $7, decode('00', 'hex'))`,
		string(ws), sessionID, resourceID, repoURL, mountPath, nullableEmptyString(checkoutType), nullableEmptyString(checkoutRef),
	); err != nil {
		t.Fatalf("seed github session resource detail: %v", err)
	}
}

func seedFilePreparationResource(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, resourceID string, sourceFileID string, sessionFileID string, objectID string, mountPath string) {
	t.Helper()
	now := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, 12, $4, $5)`,
		string(ws), objectID, "files/"+string(ws)+"/"+objectID, sandboxTestSHA256Hex(objectID), now,
	); err != nil {
		t.Fatalf("seed file object: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ($1, $2, $3, 'source.txt', 'text/plain', TRUE, $4)`,
		string(ws), sourceFileID, objectID, now,
	); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at)
		 VALUES ($1, $2, $3, 'session.txt', 'text/plain', TRUE, 'session', $4, $5)`,
		string(ws), sessionFileID, objectID, sessionID, now,
	); err != nil {
		t.Fatalf("seed session file: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_resources (workspace_id, session_id, resource_id, type, created_at, updated_at)
		 VALUES ($1, $2, $3, 'file', $4, $4)`,
		string(ws), sessionID, resourceID, now,
	); err != nil {
		t.Fatalf("seed file session resource: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_file_resources (
			workspace_id, session_id, resource_id, source_file_id, file_id, mount_path
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		string(ws), sessionID, resourceID, sourceFileID, sessionFileID, mountPath,
	); err != nil {
		t.Fatalf("seed file session resource detail: %v", err)
	}
}

type sandboxQueueJobRow struct {
	id             string
	kind           string
	partitionKey   string
	dedupeKey      string
	status         string
	payloadVersion int
	payloadJSON    string
	priority       int
	attemptCount   int
	maxAttempts    int
}

func readSandboxQueueJobs(t *testing.T, db *sql.DB, sessionID string) []sandboxQueueJobRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, kind, partition_key, dedupe_key, status, payload_version,
		        payload_json, priority, attempt_count, max_attempts
		   FROM queue_jobs
		  WHERE workspace_id = $1 AND partition_key = $2
		  ORDER BY created_at, id`,
		string(workspace.DefaultID),
		queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
	)
	if err != nil {
		t.Fatalf("query queue jobs: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var got []sandboxQueueJobRow
	for rows.Next() {
		var row sandboxQueueJobRow
		if err := rows.Scan(
			&row.id,
			&row.kind,
			&row.partitionKey,
			&row.dedupeKey,
			&row.status,
			&row.payloadVersion,
			&row.payloadJSON,
			&row.priority,
			&row.attemptCount,
			&row.maxAttempts,
		); err != nil {
			t.Fatalf("scan queue job: %v", err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("queue jobs: %v", err)
	}
	return got
}

func findSandboxRuntimeInputJob(t *testing.T, jobs []sandboxQueueJobRow, inputKind string) sandboxQueueJobRow {
	t.Helper()
	for _, job := range jobs {
		if job.kind != queue.KindRuntimeInput {
			continue
		}
		var payload struct {
			InputKind string `json:"input_kind"`
		}
		if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
			t.Fatalf("decode runtime_input payload: %v", err)
		}
		if payload.InputKind == inputKind {
			return job
		}
	}
	t.Fatalf("queue jobs = %#v; missing runtime_input kind %s", jobs, inputKind)
	return sandboxQueueJobRow{}
}

func findSandboxRuntimeInputJobByEvent(t *testing.T, jobs []sandboxQueueJobRow, eventID string) sandboxQueueJobRow {
	t.Helper()
	for _, job := range jobs {
		if job.kind != queue.KindRuntimeInput {
			continue
		}
		var payload struct {
			EventIDs []string `json:"event_ids"`
		}
		if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
			t.Fatalf("decode runtime_input payload: %v", err)
		}
		for _, got := range payload.EventIDs {
			if got == eventID {
				return job
			}
		}
	}
	t.Fatalf("queue jobs = %#v; missing runtime_input for event %s", jobs, eventID)
	return sandboxQueueJobRow{}
}

func assertSandboxRuntimeInputJob(t *testing.T, job sandboxQueueJobRow, sessionID string, threadID string, inputKind string, priority int, eventIDs []string, sequenceFrom int64, sequenceTo int64) {
	t.Helper()
	if !strings.HasPrefix(job.id, queue.JobIDPrefix) {
		t.Fatalf("queue job id = %q; want %s prefix", job.id, queue.JobIDPrefix)
	}
	if job.kind != queue.KindRuntimeInput || job.status != "pending" || job.payloadVersion != 1 {
		t.Fatalf("queue job control fields = %#v; want pending runtime_input payload v1", job)
	}
	if job.priority != priority || job.attemptCount != 0 || job.maxAttempts != 0 {
		t.Fatalf("queue job fields = priority %d attempts %d/%d; want priority %d and durable unset 0/0", job.priority, job.attemptCount, job.maxAttempts, priority)
	}
	var payload struct {
		WorkspaceID     string   `json:"workspace_id"`
		SessionID       string   `json:"session_id"`
		SessionThreadID string   `json:"session_thread_id"`
		RuntimeInputID  string   `json:"runtime_input_id"`
		EventIDs        []string `json:"event_ids"`
		SequenceFrom    int64    `json:"sequence_from"`
		SequenceTo      int64    `json:"sequence_to"`
		InputKind       string   `json:"input_kind"`
	}
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime_input payload: %v", err)
	}
	if strings.Contains(job.payloadJSON, "hello") || strings.Contains(job.payloadJSON, "again") {
		t.Fatalf("queue payload copies user content: %s", job.payloadJSON)
	}
	if payload.WorkspaceID != string(workspace.DefaultID) || payload.SessionID != sessionID || payload.SessionThreadID != threadID || payload.InputKind != inputKind {
		t.Fatalf("runtime_input payload identity = %#v; want session/thread/input kind", payload)
	}
	if !strings.HasPrefix(payload.RuntimeInputID, runtimeInputIDPrefix) {
		t.Fatalf("runtime_input_id = %q; want %s prefix", payload.RuntimeInputID, runtimeInputIDPrefix)
	}
	if !reflect.DeepEqual(payload.EventIDs, eventIDs) {
		t.Fatalf("event_ids = %v; want %v", payload.EventIDs, eventIDs)
	}
	if payload.SequenceFrom != sequenceFrom || payload.SequenceTo != sequenceTo {
		t.Fatalf("sequence range = %d..%d; want %d..%d", payload.SequenceFrom, payload.SequenceTo, sequenceFrom, sequenceTo)
	}
	if job.partitionKey != queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID) {
		t.Fatalf("partition key = %q; want session partition", job.partitionKey)
	}
	wantDedupe := queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, payload.RuntimeInputID)
	if job.dedupeKey != wantDedupe {
		t.Fatalf("dedupe key = %q; want %q", job.dedupeKey, wantDedupe)
	}
}

func countUnprocessedSandboxEvents(t *testing.T, db *sql.DB, sessionID string) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND processed_at IS NULL
		    AND type IN ('user.message', 'user.interrupt', 'user.tool_confirmation')`,
		string(workspace.DefaultID),
		sessionID,
	).Scan(&count); err != nil {
		t.Fatalf("count unprocessed session events: %v", err)
	}
	return count
}

func sandboxTestSHA256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func seedSandboxSession(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string) {
	t.Helper()
	createdAt := "2026-05-22T10:00:00Z"
	if _, err := admin.Exec(
		`INSERT INTO agents (workspace_id, id, name, description, version, created_at, updated_at)
		 VALUES ($1, 'agent_sandbox', 'Sandbox Agent', '', 1, $2, $2)
		 ON CONFLICT (id) DO NOTHING`,
		string(ws), createdAt,
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := admin.Exec(
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agentver_sandbox', 'agent_sandbox', 1, '{}', 'hash', $2)
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(ws), createdAt,
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := admin.Exec(
		`INSERT INTO environments (workspace_id, id, name, description, config_json, metadata_json, created_at, updated_at)
		 VALUES ($1, 'env_sandbox', 'Sandbox Env', '', '{"type":"container","networking":{"type":"unrestricted"},"packages":{}}', '{}', $2, $2)
		 ON CONFLICT (id) DO NOTHING`,
		string(ws), createdAt,
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
	if _, err := admin.Exec(
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, 'env_sandbox', 1, 'ready', 'tetral', 'artifact_sandbox',
			'hash_config_sandbox', 'hash_packages_sandbox', '{"type":"unrestricted"}', '{}', $2, $2)
		ON CONFLICT (workspace_id, environment_id, generation) DO NOTHING`,
		string(ws), createdAt,
	); err != nil {
		t.Fatalf("seed environment artifact: %v", err)
	}
	if _, err := admin.Exec(
		`INSERT INTO sessions (
			workspace_id, id, type, title, metadata_json, status, agent_id, agent_version,
			environment_id, vault_ids_json, created_at, updated_at
		) VALUES ($1, $2, 'session', NULL, '{}', 'idle', 'agent_sandbox', 1, 'env_sandbox', '[]', $3, $3)`,
		string(ws), sessionID, createdAt,
	); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := admin.Exec(
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, idle_since, created_at, updated_at
		) VALUES ($1, $2, 'idle', $3, $3, $3)`,
		string(ws), sessionID, createdAt,
	); err != nil {
		t.Fatalf("seed session runtime status: %v", err)
	}
}

func seedSandboxSkillVersion(t *testing.T, admin *sql.DB, ws workspace.ID, skillID string, version string, directory string, blobKey string, sizeBytes int64, sha256Hex string) {
	t.Helper()
	createdAt := "2026-05-22T10:00:00Z"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO skills (workspace_id, skill_id, display_title, latest_version, created_at, updated_at)
		 VALUES ($1, $2, 'Test Skill', $3, $4, $4)`,
		string(ws), skillID, version, createdAt,
	); err != nil {
		t.Fatalf("seed skill: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO skill_versions (
			workspace_id, skill_id, skill_version_id, version, name, description,
			directory, blob_key, size_bytes, sha256, created_at
		) VALUES ($1, $2, $3, $4, 'Test Skill', 'Use for financial analysis.', $5, $6, $7, $8, $9)`,
		string(ws), skillID, "skv_"+skillID+"_"+version, version, directory, blobKey, sizeBytes, sha256Hex, createdAt,
	); err != nil {
		t.Fatalf("seed skill version: %v", err)
	}
}

func seedSandboxSessionAgentSkills(t *testing.T, admin *sql.DB, ws workspace.ID, sessionID string, skills []sessionPreparationSkillRef) {
	t.Helper()
	config, err := json.Marshal(struct {
		Skills []sessionPreparationSkillRef `json:"skills"`
	}{Skills: skills})
	if err != nil {
		t.Fatalf("marshal agent skills config: %v", err)
	}
	hash := sandboxTestSHA256Hex(string(config))
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE agent_versions av
		    SET config_json = $3,
		        config_hash = $4
		   FROM sessions s
		  WHERE s.workspace_id = $1
		    AND s.id = $2
		    AND av.workspace_id = s.workspace_id
		    AND av.id = s.agent_version_id`,
		string(ws), sessionID, string(config), hash,
	); err != nil {
		t.Fatalf("seed session agent skills config: %v", err)
	}
}
