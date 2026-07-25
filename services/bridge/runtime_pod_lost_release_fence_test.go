package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossSettlesAndClaimsBeforeRelease(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID  = "sesn_pod_loss_release_fence"
		threadID   = "thr_pod_loss_release_fence"
		bindingID  = "bind_pod_loss_release_fence"
		generation = int64(17)
		taskID     = "task_pod_loss_release_fence"
	)
	seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, threadID, bindingID, generation, taskID)

	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	claimID := "runtime_pod_lost:" + bindingID + ":17"
	releaser := &recordingSandboxReleaseClient{
		result: SandboxReleaseResult{Status: SandboxReleaseReleased, SandboxStatus: "released"},
		beforeRelease: func(request SandboxReleaseRequest) error {
			if request.IdempotencyKey != claimID {
				return fmt.Errorf("release idempotency key = %q; want %q", request.IdempotencyKey, claimID)
			}
			if !request.DurableCleanupFence {
				return errors.New("release request did not declare durable cleanup fence")
			}
			var taskStatus string
			var cleanupJobID sql.NullString
			var cleanupClaimedAt sql.NullString
			var cleanupAfter sql.NullString
			var runtimeStatus string
			var statusEventID sql.NullString
			var statusBindingID sql.NullString
			var statusGeneration sql.NullInt64
			var bindingCount int
			var requestEndCount int
			var toolResultCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_background_tasks
				  WHERE workspace_id = 'default' AND session_id = $1 AND task_id = $2`,
				sessionID, taskID).Scan(&taskStatus); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status, status_event_id, cleanup_after, cleanup_job_id, cleanup_claimed_at, binding_id, binding_generation
				   FROM session_runtime_status
				  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(
				&runtimeStatus, &statusEventID, &cleanupAfter, &cleanupJobID, &cleanupClaimedAt, &statusBindingID, &statusGeneration,
			); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_runtime_bindings
				  WHERE workspace_id = 'default' AND session_id = $1 AND binding_id = $2 AND binding_generation = $3`,
				sessionID, bindingID, generation).Scan(&bindingCount); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1
				    AND type = 'span.model_request_end' AND model_request_id = $2`,
				sessionID, "mrq_"+sessionID).Scan(&requestEndCount); err != nil {
				return err
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1
				    AND type = 'agent.tool_result'
				    AND payload_json::jsonb ->> 'tool_use_event_id' = $2`,
				sessionID, "sevt_orphan_tool_"+sessionID).Scan(&toolResultCount); err != nil {
				return err
			}
			if taskStatus != "cancelled_by_cleanup" || runtimeStatus != "idle" || !statusEventID.Valid || !cleanupAfter.Valid ||
				!cleanupJobID.Valid || cleanupJobID.String != claimID ||
				!cleanupClaimedAt.Valid || !statusBindingID.Valid || statusBindingID.String != bindingID ||
				!statusGeneration.Valid || statusGeneration.Int64 != generation || bindingCount != 1 ||
				requestEndCount != 1 || toolResultCount != 1 {
				return fmt.Errorf("release fence observed task=%q runtime_status=%q status_event=%v cleanup_after=%v claim=%v claimed_at=%v status_binding=%v/%v binding_count=%d request_ends=%d tool_results=%d",
					taskStatus, runtimeStatus, statusEventID, cleanupAfter, cleanupJobID, cleanupClaimedAt, statusBindingID, statusGeneration, bindingCount, requestEndCount, toolResultCount)
			}
			return nil
		},
	}
	store.SandboxReleaser = releaser

	err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID, runtimeBindingForDelivery{
		BindingID:         bindingID,
		BindingGeneration: generation,
		Namespace:         "tetral-agent-runtime",
		PodName:           "runtime-pod-0",
		PodUID:            "pod_uid_" + sessionID,
		PodIP:             "10.0.0.10",
	}, store.Clock())
	if err != nil {
		t.Fatalf("repairLostRuntimeBinding: %v", err)
	}
	var cleanupAfter, cleanupJobID, bindingIDAfter sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT cleanup_after, cleanup_job_id, binding_id
		   FROM session_runtime_status
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(
		&cleanupAfter, &cleanupJobID, &bindingIDAfter,
	); err != nil {
		t.Fatalf("read finalized runtime status: %v", err)
	}
	if cleanupAfter.Valid || cleanupJobID.Valid || bindingIDAfter.Valid {
		t.Fatalf("finalized runtime status retained cleanup/binding fields: cleanup_after=%v cleanup_job_id=%v binding_id=%v", cleanupAfter, cleanupJobID, bindingIDAfter)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossClaimConflictsRollbackSettlements(t *testing.T) {
	t.Run("foreign claim", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_pod_loss_foreign_claim"
		seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, "thr_pod_loss_foreign_claim", "bind_pod_loss_foreign_claim", 3, "task_pod_loss_foreign_claim")
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_runtime_status
			    SET cleanup_job_id = 'cleanup_session:foreign', cleanup_claimed_at = '2026-01-01T00:04:00Z'
			  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
			t.Fatalf("seed foreign cleanup claim: %v", err)
		}
		releaser := &recordingSandboxReleaseClient{}
		store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
		err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
			runtimePodLostReleaseFenceBinding(sessionID, "bind_pod_loss_foreign_claim", 3), store.Clock())
		assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_claim_stale")
		assertRuntimePodLostTaskAndFence(t, admin, sessionID, "task_pod_loss_foreign_claim", "running", "cleanup_session:foreign", true)
		assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 0)
		if len(releaser.requests) != 0 {
			t.Fatalf("foreign claim release requests = %d; want 0", len(releaser.requests))
		}
	})

	t.Run("partial claimed-at state", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_pod_loss_partial_claim"
		seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, "thr_pod_loss_partial_claim", "bind_pod_loss_partial_claim", 5, "task_pod_loss_partial_claim")
		if _, err := admin.ExecContext(context.Background(),
			`UPDATE session_runtime_status
			    SET cleanup_job_id = NULL, cleanup_claimed_at = '2026-01-01T00:04:00Z'
			  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
			t.Fatalf("seed malformed partial cleanup claim: %v", err)
		}
		releaser := &recordingSandboxReleaseClient{}
		store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
		err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
			runtimePodLostReleaseFenceBinding(sessionID, "bind_pod_loss_partial_claim", 5), store.Clock())
		assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_claim_stale")
		assertRuntimePodLostTaskStatus(t, admin, sessionID, "task_pod_loss_partial_claim", "running")
		assertRuntimePodLostMalformedPartialClaim(t, admin, sessionID)
		assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 0)
		if len(releaser.requests) != 0 {
			t.Fatalf("partial claim release requests = %d; want 0", len(releaser.requests))
		}
	})

	t.Run("missing runtime status", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_pod_loss_missing_status"
		const bindingID = "bind_pod_loss_missing_status"
		seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, "thr_pod_loss_missing_status", bindingID, 6, "task_pod_loss_missing_status")
		if _, err := admin.ExecContext(context.Background(),
			`DELETE FROM session_runtime_status
			  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
			t.Fatalf("delete runtime status fence: %v", err)
		}
		releaser := &recordingSandboxReleaseClient{}
		store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
		err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
			runtimePodLostReleaseFenceBinding(sessionID, bindingID, 6), store.Clock())
		assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_claim_stale")
		assertRuntimePodLostTaskStatus(t, admin, sessionID, "task_pod_loss_missing_status", "running")
		assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 0)
		assertRuntimePodLostBindingCount(t, admin, sessionID, bindingID, 6, 1)
		var statusCount int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_runtime_status
			  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&statusCount); err != nil {
			t.Fatalf("count missing runtime status: %v", err)
		}
		if statusCount != 0 {
			t.Fatalf("runtime status rows after missing-fence repair = %d; want 0", statusCount)
		}
		if len(releaser.requests) != 0 {
			t.Fatalf("missing status release requests = %d; want 0", len(releaser.requests))
		}
	})

	t.Run("generation mismatch", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		const sessionID = "sesn_pod_loss_generation_mismatch"
		seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, "thr_pod_loss_generation_mismatch", "bind_pod_loss_generation_mismatch", 8, "task_pod_loss_generation_mismatch")
		releaser := &recordingSandboxReleaseClient{}
		store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
		err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
			runtimePodLostReleaseFenceBinding(sessionID, "bind_pod_loss_generation_mismatch", 7), store.Clock())
		assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_claim_stale")
		assertRuntimePodLostTaskAndFence(t, admin, sessionID, "task_pod_loss_generation_mismatch", "running", "", false)
		assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 0)
		if len(releaser.requests) != 0 {
			t.Fatalf("generation mismatch release requests = %d; want 0", len(releaser.requests))
		}
	})
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossNonACKRetainsClaimAndBinding(t *testing.T) {
	tests := []struct {
		name       string
		result     SandboxReleaseResult
		releaseErr error
		errorKind  string
		retryable  bool
	}{
		{name: "transport", releaseErr: errors.New("lost transport ACK"), errorKind: "sandbox_release_unavailable", retryable: true},
		{name: "retry_later", result: SandboxReleaseResult{Status: SandboxReleaseRetryLater}, errorKind: "sandbox_release_retry_later", retryable: true},
		{name: "failed", result: SandboxReleaseResult{Status: SandboxReleaseFailed}, errorKind: "sandbox_release_failed", retryable: false},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := fmt.Sprintf("sesn_pod_loss_non_ack_%d", index)
			threadID := fmt.Sprintf("thr_pod_loss_non_ack_%d", index)
			bindingID := fmt.Sprintf("bind_pod_loss_non_ack_%d", index)
			taskID := fmt.Sprintf("task_pod_loss_non_ack_%d", index)
			seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, threadID, bindingID, 4, taskID)
			releaser := &recordingSandboxReleaseClient{result: test.result, err: test.releaseErr}
			store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
			err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
				runtimePodLostReleaseFenceBinding(sessionID, bindingID, 4), store.Clock())
			var prepareErr runtimeDeliveryPrepareError
			if !errors.As(err, &prepareErr) || prepareErr.kind != test.errorKind || prepareErr.retryable != test.retryable {
				t.Fatalf("repair error = %#v; want kind %q retryable=%v", err, test.errorKind, test.retryable)
			}
			claimID := "runtime_pod_lost:" + bindingID + ":4"
			assertRuntimePodLostTaskAndFence(t, admin, sessionID, taskID, "cancelled_by_cleanup", claimID, true)
			assertRuntimePodLostBindingCount(t, admin, sessionID, bindingID, 4, 1)
			assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 1)
		})
	}
}

type ackLossSandboxReleaseClient struct {
	requests []SandboxReleaseRequest
}

func (c *ackLossSandboxReleaseClient) ReleaseSandbox(_ context.Context, request SandboxReleaseRequest) (SandboxReleaseResult, error) {
	c.requests = append(c.requests, request)
	if len(c.requests) == 1 {
		return SandboxReleaseResult{}, errors.New("provider released sandbox but ACK was lost")
	}
	return SandboxReleaseResult{Status: SandboxReleaseAlreadyReleased, SandboxStatus: "released"}, nil
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossFreshStoreRetriesLostACK(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_ack_loss"
		threadID  = "thr_pod_loss_ack_loss"
		bindingID = "bind_pod_loss_ack_loss"
		taskID    = "task_pod_loss_ack_loss"
	)
	seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, threadID, bindingID, 6, taskID)
	releaser := &ackLossSandboxReleaseClient{}
	firstStore := newRuntimePodLostReleaseFenceStore(runtime, releaser)
	binding := runtimePodLostReleaseFenceBinding(sessionID, bindingID, 6)
	firstErr := firstStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, binding, firstStore.Clock())
	assertRuntimePodLostRetryableError(t, firstErr, "sandbox_release_unavailable")
	claimID := "runtime_pod_lost:" + bindingID + ":6"
	assertRuntimePodLostTaskAndFence(t, admin, sessionID, taskID, "cancelled_by_cleanup", claimID, true)
	assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 1)

	freshStore := newRuntimePodLostReleaseFenceStore(runtime, releaser)
	if err := freshStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, binding, freshStore.Clock()); err != nil {
		t.Fatalf("fresh-store repair retry: %v", err)
	}
	if len(releaser.requests) != 2 || releaser.requests[0].IdempotencyKey != claimID || releaser.requests[1].IdempotencyKey != claimID {
		t.Fatalf("ACK-loss release identities = %+v; want two %q requests", releaser.requests, claimID)
	}
	var terminalEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`, sessionID).Scan(&terminalEventCount); err != nil {
		t.Fatalf("count ACK-loss terminal events: %v", err)
	}
	if terminalEventCount != 1 {
		t.Fatalf("ACK-loss terminal event count = %d; want 1", terminalEventCount)
	}
	assertRuntimePodLostBindingCount(t, admin, sessionID, bindingID, 6, 0)
	assertRuntimePodLostTaskAndFence(t, admin, sessionID, taskID, "cancelled_by_cleanup", "", false)
	assertRuntimePodLostTerminalFactCounts(t, admin, sessionID, 1)
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossRearmsCompletionMailOnlyOnFreshClaim(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_pod_loss_mail_ack_loss"
		mainID    = "thr_pod_loss_mail_ack_loss"
		childID   = "thr_pod_loss_mail_ack_loss_child"
		bindingID = "bind_pod_loss_mail_ack_loss"
		taskID    = "task_pod_loss_mail_ack_loss"
		delivery  = "delivery_pod_loss_mail_ack_loss"
	)
	seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, mainID, bindingID, 6, taskID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedCompletionMailSentAt(t, admin, sessionID, mainID, childID, delivery, 1, "2026-01-01T00:00:02Z")

	releaser := &ackLossSandboxReleaseClient{}
	binding := runtimePodLostReleaseFenceBinding(sessionID, bindingID, 6)
	firstStore := newRuntimePodLostReleaseFenceStore(runtime, releaser)
	firstErr := firstStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, binding, firstStore.Clock())
	assertRuntimePodLostRetryableError(t, firstErr, "sandbox_release_unavailable")
	assertActiveCompletionWake(t, admin, sessionID, delivery, true)

	dedupeKey := queue.FormatRuntimeInputDedupeKey(
		workspace.ID("default"),
		sessionID,
		completionRuntimeInputID(delivery),
	)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE queue_jobs
		    SET status = 'acknowledged',
		        updated_at = '2026-01-01T00:05:01Z'
		  WHERE workspace_id = 'default'
		    AND dedupe_key = $1
		    AND status = 'pending'`,
		dedupeKey,
	); err != nil {
		t.Fatalf("acknowledge first completion wake: %v", err)
	}

	freshStore := newRuntimePodLostReleaseFenceStore(runtime, releaser)
	if err := freshStore.repairLostRuntimeBinding(context.Background(), "default", sessionID, binding, freshStore.Clock()); err != nil {
		t.Fatalf("fresh-store repair retry: %v", err)
	}
	var wakeCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND dedupe_key = $1`,
		dedupeKey,
	).Scan(&wakeCount); err != nil {
		t.Fatalf("count completion wakes after lost-ACK retry: %v", err)
	}
	if wakeCount != 1 {
		t.Fatalf("completion wakes after lost-ACK retry = %d; want original wake only", wakeCount)
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossFinalizationCASRejectsLateChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB, string, string, int64)
		check  func(*testing.T, *sql.DB, string, string, int64, string)
	}{
		{
			name: "replacement binding",
			mutate: func(t *testing.T, db *sql.DB, sessionID string, _ string, _ int64) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(),
					`UPDATE session_runtime_bindings
					    SET binding_id = 'bind_late_replacement', binding_generation = 99,
					        agent_runtime_pod_uid = 'pod_uid_late_replacement'
					  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
					t.Fatalf("inject replacement binding: %v", err)
				}
			},
			check: func(t *testing.T, db *sql.DB, sessionID string, _ string, _ int64, claimID string) {
				assertRuntimePodLostBindingCount(t, db, sessionID, "bind_late_replacement", 99, 1)
				assertRuntimePodLostClaim(t, db, sessionID, claimID, true)
			},
		},
		{
			name: "different claim",
			mutate: func(t *testing.T, db *sql.DB, sessionID string, _ string, _ int64) {
				t.Helper()
				if _, err := db.ExecContext(context.Background(),
					`UPDATE session_runtime_status
					    SET cleanup_job_id = 'cleanup_session:late_foreign_claim'
					  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
					t.Fatalf("inject different cleanup claim: %v", err)
				}
			},
			check: func(t *testing.T, db *sql.DB, sessionID string, bindingID string, generation int64, _ string) {
				assertRuntimePodLostBindingCount(t, db, sessionID, bindingID, generation, 1)
				assertRuntimePodLostClaim(t, db, sessionID, "cleanup_session:late_foreign_claim", true)
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := fmt.Sprintf("sesn_pod_loss_finalize_%d", index)
			bindingID := fmt.Sprintf("bind_pod_loss_finalize_%d", index)
			taskID := fmt.Sprintf("task_pod_loss_finalize_%d", index)
			seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, fmt.Sprintf("thr_pod_loss_finalize_%d", index), bindingID, 12, taskID)
			releaser := &recordingSandboxReleaseClient{
				result: SandboxReleaseResult{Status: SandboxReleaseReleased},
				beforeRelease: func(SandboxReleaseRequest) error {
					test.mutate(t, admin, sessionID, bindingID, 12)
					return nil
				},
			}
			store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
			err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
				runtimePodLostReleaseFenceBinding(sessionID, bindingID, 12), store.Clock())
			assertRuntimePodLostRetryableError(t, err, "runtime_pod_lost_finalize_stale")
			test.check(t, admin, sessionID, bindingID, 12, "runtime_pod_lost:"+bindingID+":12")
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreAvailabilityVisibilityDoesNotRepairPodLoss(t *testing.T) {
	tests := []enginekubernetes.BindingVisibilityState{
		enginekubernetes.BindingVisibilitySnapshotNotReady,
		enginekubernetes.BindingVisibilityNotReady,
		enginekubernetes.BindingVisibilityNotServing,
		enginekubernetes.BindingVisibilityTerminating,
	}
	for index, visibility := range tests {
		t.Run(string(visibility), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := fmt.Sprintf("sesn_pod_loss_visibility_%d", index)
			threadID := fmt.Sprintf("thr_pod_loss_visibility_%d", index)
			bindingID := fmt.Sprintf("bind_pod_loss_visibility_%d", index)
			taskID := fmt.Sprintf("task_pod_loss_visibility_%d", index)
			seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, threadID, bindingID, 15, taskID)
			eventID := fmt.Sprintf("sevt_pod_loss_visibility_%d", index)
			seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, eventID, 3, "user.message", `{"content":[{"type":"text","text":"visibility"}]}`)
			bound := enginekubernetes.BoundRuntimePod{Namespace: "tetral-agent-runtime", PodName: "runtime-pod-0", PodUID: "pod_uid_" + sessionID, PodIP: "10.0.0.10"}
			snapshot := enginekubernetes.NewBindingVisibilitySnapshotStateWithCandidatesForTest(true, bound, visibility, nil)
			releaser := &recordingSandboxReleaseClient{}
			store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
			store.TargetResolver = KubernetesRuntimeTargetResolver{Snapshot: func() enginekubernetes.BindingVisibilitySnapshot { return snapshot }, Clock: store.Clock}
			_, err := store.PrepareRuntimeCommand(context.Background(), RuntimeJob{
				JobID: "qjob_" + sessionID, LeaseToken: "lease_" + sessionID, Kind: queue.KindRuntimeInput,
				WorkspaceID: "default", SessionID: sessionID, SessionThreadID: threadID,
				PreparationAttemptID: "prep_" + sessionID,
				RuntimeInputID:       "rin_" + sessionID, EventIDs: []string{eventID}, SequenceFrom: 3, SequenceTo: 3,
				InputKind: "messages", CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES,
				PayloadJSON: `{"input_kind":"messages"}`,
			})
			assertRuntimePodLostRetryableError(t, err, "runtime_binding_not_available")
			assertRuntimePodLostTaskAndFence(t, admin, sessionID, taskID, "running", "", false)
			assertRuntimePodLostBindingCount(t, admin, sessionID, bindingID, 15, 1)
			if len(releaser.requests) != 0 {
				t.Fatalf("availability visibility release requests = %d; want 0", len(releaser.requests))
			}
		})
	}
}

func TestPostgreSQLRuntimeDeliveryStoreRuntimePodLossLateTaskCompletionAndFailureLoseCAS(t *testing.T) {
	for index, terminalStatus := range []string{"completed", "failed"} {
		t.Run(terminalStatus, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := fmt.Sprintf("sesn_pod_loss_late_task_%d", index)
			threadID := fmt.Sprintf("thr_pod_loss_late_task_%d", index)
			bindingID := fmt.Sprintf("bind_pod_loss_late_task_%d", index)
			taskID := fmt.Sprintf("task_pod_loss_late_task_%d", index)
			sourceToolUseEventID := "sevt_tool_" + taskID
			seedRuntimePodLostReleaseFenceFixture(t, admin, sessionID, threadID, bindingID, 1, taskID)
			releaser := &recordingSandboxReleaseClient{result: SandboxReleaseResult{Status: SandboxReleaseRetryLater}}
			store := newRuntimePodLostReleaseFenceStore(runtime, releaser)
			err := store.repairLostRuntimeBinding(context.Background(), "default", sessionID,
				runtimePodLostReleaseFenceBinding(sessionID, bindingID, 1), store.Clock())
			assertRuntimePodLostRetryableError(t, err, "sandbox_release_retry_later")

			runtimeInputID := "rin_late_" + taskID
			seedBridgeAPITaskNotificationInbox(t, admin, "default", sessionID, threadID, runtimeInputID, bindingID, "pod_uid_"+sessionID)
			apiStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			response, err := apiStore.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{
				Scope:          bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+sessionID),
				RuntimeInputId: runtimeInputID,
				TaskId:         taskID,
				ResultJson: fmt.Sprintf(`{"task_id":%q,"source_tool_use_event_id":%q,"status":%q,"stdout":{"text":"late","truncated":false},"stderr":{"text":"","truncated":false}}`,
					taskID, sourceToolUseEventID, terminalStatus),
			})
			if err != nil {
				t.Fatalf("late CommitTaskNotificationResult: %v", err)
			}
			if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || response.GetAck().GetErrorCode() != "task_notification_stale" {
				t.Fatalf("late task result ACK = %#v; want task_notification_stale", response.GetAck())
			}
			var taskStatus string
			var terminalEventCount int
			var projectionCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status FROM session_background_tasks
				  WHERE workspace_id = 'default' AND session_id = $1 AND task_id = $2`, sessionID, taskID).Scan(&taskStatus); err != nil {
				t.Fatalf("read late task status: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`, sessionID).Scan(&terminalEventCount); err != nil {
				t.Fatalf("count late task terminal events: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_messages
				  WHERE workspace_id = 'default' AND session_id = $1 AND kind = 'runtime_notification'`, sessionID).Scan(&projectionCount); err != nil {
				t.Fatalf("count late task projections: %v", err)
			}
			if taskStatus != "cancelled_by_cleanup" || terminalEventCount != 1 || projectionCount != 1 {
				t.Fatalf("late task state = %q events=%d projections=%d; want cancelled_by_cleanup/1/1", taskStatus, terminalEventCount, projectionCount)
			}
		})
	}
}

func seedRuntimePodLostReleaseFenceFixture(t *testing.T, db *sql.DB, sessionID string, threadID string, bindingID string, generation int64, taskID string) {
	t.Helper()
	seedBridgeAPISession(t, db, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, db, "default", sessionID, bindingID, generation, "pod_uid_"+sessionID)
	seedBridgeAPIPreparationReady(t, db, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIActiveSandbox(t, db, "default", sessionID, "2026-01-01T00:04:59Z")
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, $3, 1, 'span.model_request_start',
		 $4, 'internal', false, $5, $4, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'),
		('default', $1, $2, $6, 2, 'agent.tool_use',
		 $7, 'public', true, $5, $8, '2026-01-01T00:00:01Z', '2026-01-01T00:00:01Z')`,
		sessionID,
		threadID,
		"sevt_request_start_"+sessionID,
		fmt.Sprintf(`{"type":"span.model_request_start","model_request_id":%q,"request_kind":"agent_provider_request"}`, "mrq_"+sessionID),
		"mrq_"+sessionID,
		"sevt_orphan_tool_"+sessionID,
		`{"type":"agent.tool_use","name":"Write","input":{"file_path":"src/a.ts"},"evaluated_permission":"allow"}`,
		`{"type":"runtime_tool_projection","model_tool_call_id":"tool-call-pod-loss","tool_name":"Write","input":{"file_path":"src/a.ts"},"state":"running"}`,
	); err != nil {
		t.Fatalf("seed runtime pod-loss terminal facts: %v", err)
	}
	seedBridgeAPIBackgroundTask(t, db, "default", sessionID, threadID, bindingID, taskID, "sevt_tool_"+taskID)
	seedRuntimePodLostStatusFence(t, db, sessionID, bindingID, generation)
}

func seedRuntimePodLostStatusFence(t *testing.T, db *sql.DB, sessionID string, bindingID string, generation int64) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, binding_id, binding_generation, created_at, updated_at
		) VALUES ('default', $1, 'running', $2, $3, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		sessionID, bindingID, generation); err != nil {
		t.Fatalf("seed runtime pod-loss status: %v", err)
	}
}

func newRuntimePodLostReleaseFenceStore(runtime *sql.DB, releaser SandboxReleaseClient) *PostgreSQLRuntimeDeliveryStore {
	store := NewPostgreSQLRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), 9090)
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC) }
	store.SandboxReleaser = releaser
	return store
}

func runtimePodLostReleaseFenceBinding(sessionID string, bindingID string, generation int64) runtimeBindingForDelivery {
	return runtimeBindingForDelivery{
		BindingID:         bindingID,
		BindingGeneration: generation,
		Namespace:         "tetral-agent-runtime",
		PodName:           "runtime-pod-0",
		PodUID:            "pod_uid_" + sessionID,
		PodIP:             "10.0.0.10",
	}
}

func assertRuntimePodLostRetryableError(t *testing.T, err error, kind string) {
	t.Helper()
	var prepareErr runtimeDeliveryPrepareError
	if !errors.As(err, &prepareErr) || prepareErr.kind != kind || !prepareErr.retryable {
		t.Fatalf("repair error = %#v; want retryable %q", err, kind)
	}
}

func assertRuntimePodLostTaskAndFence(t *testing.T, db *sql.DB, sessionID string, taskID string, wantTaskStatus string, wantClaimID string, wantClaimed bool) {
	t.Helper()
	assertRuntimePodLostTaskStatus(t, db, sessionID, taskID, wantTaskStatus)
	assertRuntimePodLostClaim(t, db, sessionID, wantClaimID, wantClaimed)
}

func assertRuntimePodLostTaskStatus(t *testing.T, db *sql.DB, sessionID string, taskID string, wantTaskStatus string) {
	t.Helper()
	var taskStatus string
	if err := db.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks
		  WHERE workspace_id = 'default' AND session_id = $1 AND task_id = $2`, sessionID, taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("read pod-loss task status: %v", err)
	}
	if taskStatus != wantTaskStatus {
		t.Fatalf("pod-loss task status = %q; want %q", taskStatus, wantTaskStatus)
	}
}

func assertRuntimePodLostMalformedPartialClaim(t *testing.T, db *sql.DB, sessionID string) {
	t.Helper()
	var cleanupJobID sql.NullString
	var cleanupClaimedAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT cleanup_job_id, cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&cleanupJobID, &cleanupClaimedAt); err != nil {
		t.Fatalf("read malformed partial pod-loss claim: %v", err)
	}
	if cleanupJobID.Valid || !cleanupClaimedAt.Valid || cleanupClaimedAt.String != "2026-01-01T00:04:00Z" {
		t.Fatalf("malformed partial claim changed to job=%v claimed_at=%v; want NULL/2026-01-01T00:04:00Z", cleanupJobID, cleanupClaimedAt)
	}
}

func assertRuntimePodLostClaim(t *testing.T, db *sql.DB, sessionID string, wantClaimID string, wantClaimed bool) {
	t.Helper()
	var cleanupJobID sql.NullString
	var cleanupClaimedAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT cleanup_job_id, cleanup_claimed_at
		   FROM session_runtime_status
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&cleanupJobID, &cleanupClaimedAt); err != nil {
		t.Fatalf("read pod-loss claim: %v", err)
	}
	if wantClaimed {
		if !cleanupJobID.Valid || cleanupJobID.String != wantClaimID || !cleanupClaimedAt.Valid {
			t.Fatalf("pod-loss claim = %v claimed_at=%v; want %q claimed", cleanupJobID, cleanupClaimedAt, wantClaimID)
		}
		return
	}
	if cleanupJobID.Valid || cleanupClaimedAt.Valid {
		t.Fatalf("pod-loss claim = %v claimed_at=%v; want absent", cleanupJobID, cleanupClaimedAt)
	}
}

func assertRuntimePodLostBindingCount(t *testing.T, db *sql.DB, sessionID string, bindingID string, generation int64, want int) {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings
		  WHERE workspace_id = 'default' AND session_id = $1 AND binding_id = $2 AND binding_generation = $3`,
		sessionID, bindingID, generation).Scan(&count); err != nil {
		t.Fatalf("count pod-loss binding: %v", err)
	}
	if count != want {
		t.Fatalf("pod-loss binding %s/%d count = %d; want %d", bindingID, generation, count, want)
	}
}

func assertRuntimePodLostTerminalFactCounts(t *testing.T, db *sql.DB, sessionID string, want int) {
	t.Helper()
	var requestEndCount int
	var toolResultCount int
	var taskNotificationCount int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'span.model_request_end' AND model_request_id = $2`,
		sessionID, "mrq_"+sessionID).Scan(&requestEndCount); err != nil {
		t.Fatalf("count pod-loss request ends: %v", err)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $2`,
		sessionID, "sevt_orphan_tool_"+sessionID).Scan(&toolResultCount); err != nil {
		t.Fatalf("count pod-loss tool results: %v", err)
	}
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`,
		sessionID).Scan(&taskNotificationCount); err != nil {
		t.Fatalf("count pod-loss task notifications: %v", err)
	}
	if requestEndCount != want || toolResultCount != want || taskNotificationCount != want {
		t.Fatalf("pod-loss terminal facts = request_end %d tool_result %d task_notification %d; want %d each",
			requestEndCount, toolResultCount, taskNotificationCount, want)
	}
}
