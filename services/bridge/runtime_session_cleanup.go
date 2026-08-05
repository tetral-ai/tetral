package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns cleanup-session and delete-cleanup state transitions.

func (s *PostgreSQLRuntimeDeliveryStore) cleanupTargetProvenGone(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, claim cleanupSessionClaim) (bool, error) {
	prover, ok := s.TargetResolver.(RuntimeCleanupTargetProver)
	if !ok {
		return false, nil
	}
	return prover.CleanupTargetProvenGone(ctx, tx, job, runtimeBindingForDelivery{
		BindingID:         claim.BindingID,
		BindingGeneration: claim.BindingGeneration,
		Namespace:         claim.Namespace,
		PodName:           claim.PodName,
		PodUID:            claim.PodUID,
		PodIP:             claim.PodIP,
	})
}

type cleanupSessionClaim struct {
	WorkspaceID        string
	SessionID          string
	SessionThreadID    string
	CleanupJobID       string
	IdleEventID        string
	IdleStreamPosition int64
	BindingID          string
	BindingGeneration  int64
	PodUID             string
	PodIP              string
	PodName            string
	Namespace          string
	SandboxID          string
}

type cleanupClaimOptions struct {
	RejectNewerInput bool
	RequireClaimed   bool
	SetClaimed       bool
}

type sessionDeleteCleanupState struct {
	WorkspaceID     string
	SessionID       string
	DeleteCleanupID string
	Binding         runtimeBindingForDelivery
	HasBinding      bool
	SessionThreadID string
}

// Session deletion clears live Runtime custody before it ensures the durable
// Sandbox release operation. The release worker, not Bridge, inspects and
// releases the provider handle. Private Sandbox rows and Blob prefixes become
// eligible for deletion only after release completes and every Sandbox Queue
// job for the Session is closed.
func (s *PostgreSQLRuntimeDeliveryStore) prepareSessionDeleteCleanupCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, port int, now time.Time) (RuntimeCommandPlan, error) {
	state, stale, err := loadSessionDeleteCleanupStateTx(ctx, tx, job, true)
	if err != nil || stale {
		return RuntimeCommandPlan{StaleAccepted: stale}, err
	}
	if !state.HasBinding {
		return RuntimeCommandPlan{CleanupTargetGone: true}, nil
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_job_id = $3,
		        cleanup_claimed_at = COALESCE(cleanup_claimed_at, $4),
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $5
		    AND binding_generation = $6`,
		job.WorkspaceID, job.SessionID, job.DeleteCleanupID, now,
		state.Binding.BindingID, state.Binding.BindingGeneration,
	)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if !rowsAffected(result) {
		return RuntimeCommandPlan{StaleAccepted: true}, nil
	}
	payloadJSON, err := cleanupSessionPayloadJSON(job.DeleteCleanupID)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{Namespace: state.Binding.Namespace, PodName: state.Binding.PodName, PodUID: state.Binding.PodUID, PodIP: state.Binding.PodIP, Port: port},
		Request: &agentruntimev1.RuntimeInputCommandRequest{
			RequestId: job.JobID + ":" + job.LeaseToken, WorkspaceId: job.WorkspaceID, SessionId: job.SessionID,
			SessionThreadId: state.SessionThreadID, BindingId: state.Binding.BindingID,
			BindingGeneration: state.Binding.BindingGeneration, TargetPodNamespace: state.Binding.Namespace,
			TargetPodName: state.Binding.PodName, TargetPodUid: state.Binding.PodUID, TargetPodIp: state.Binding.PodIP,
			RuntimeInputId: job.RuntimeInputID, CommandKind: job.CommandKind, PayloadJson: payloadJSON,
		},
	}, nil
}

func loadSessionDeleteCleanupStateTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, lock bool) (sessionDeleteCleanupState, bool, error) {
	state := sessionDeleteCleanupState{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, DeleteCleanupID: job.DeleteCleanupID}
	query := `SELECT lifecycle_state, delete_cleanup_id, COALESCE(main_thread_id, '')
		          FROM sessions
		         WHERE workspace_id = $1 AND id = $2`
	if lock {
		query += ` FOR UPDATE`
	}
	var lifecycleState string
	var storedDeleteCleanupID sql.NullString
	if err := tx.QueryRow(ctx, query, job.WorkspaceID, job.SessionID).Scan(&lifecycleState, &storedDeleteCleanupID, &state.SessionThreadID); dbconnect.IsNoRows(err) {
		return state, true, nil
	} else if err != nil {
		return state, false, err
	}
	if lifecycleState != "deleted" || !storedDeleteCleanupID.Valid || storedDeleteCleanupID.String != job.DeleteCleanupID {
		return state, true, nil
	}
	binding, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return state, false, err
	}
	state.Binding, state.HasBinding = binding, found
	return state, false, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) prepareCleanupSessionCommandTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, port int, now time.Time) (RuntimeCommandPlan, error) {
	claimed, err := cleanupSessionAlreadyClaimedTx(ctx, tx, job)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	claim, stale, err := claimCleanupSessionTx(ctx, tx, job, now, cleanupClaimOptions{
		RejectNewerInput: !claimed,
		SetClaimed:       true,
	})
	if err != nil || stale {
		return RuntimeCommandPlan{StaleAccepted: stale}, err
	}
	provenGone, err := s.cleanupTargetProvenGone(ctx, tx, job, claim)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	if provenGone {
		return RuntimeCommandPlan{CleanupTargetGone: true}, nil
	}
	payloadJSON, err := cleanupSessionPayloadJSON(job.CleanupJobID)
	if err != nil {
		return RuntimeCommandPlan{}, err
	}
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{
			Namespace: claim.Namespace,
			PodName:   claim.PodName,
			PodUID:    claim.PodUID,
			PodIP:     claim.PodIP,
			Port:      port,
		},
		Request: &agentruntimev1.RuntimeInputCommandRequest{
			RequestId:          job.JobID + ":" + job.LeaseToken,
			WorkspaceId:        job.WorkspaceID,
			SessionId:          job.SessionID,
			SessionThreadId:    claim.SessionThreadID,
			BindingId:          claim.BindingID,
			BindingGeneration:  claim.BindingGeneration,
			TargetPodNamespace: claim.Namespace,
			TargetPodName:      claim.PodName,
			TargetPodUid:       claim.PodUID,
			TargetPodIp:        claim.PodIP,
			RuntimeInputId:     job.RuntimeInputID,
			CommandKind:        job.CommandKind,
			PayloadJson:        payloadJSON,
		},
	}, nil
}

func cleanupSessionAlreadyClaimedTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob) (bool, error) {
	var claimed bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_runtime_status
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND cleanup_job_id = $3
			   AND cleanup_claimed_at IS NOT NULL
		)`,
		job.WorkspaceID,
		job.SessionID,
		job.CleanupJobID,
	).Scan(&claimed)
	return claimed, err
}

func claimCleanupSessionTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time, options cleanupClaimOptions) (cleanupSessionClaim, bool, error) {
	if job.CleanupJobID == "" {
		return cleanupSessionClaim{}, false, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "cleanup job id is required", retryable: false}
	}
	var sessionStatus string
	var lifecycleState string
	var mainThreadID sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT status, lifecycle_state, main_thread_id
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&sessionStatus, &lifecycleState, &mainThreadID)
	if dbconnect.IsNoRows(err) {
		return cleanupSessionClaim{}, true, nil
	}
	if err != nil {
		return cleanupSessionClaim{}, false, err
	}
	if lifecycleState != "active" || sessionStatus == "terminated" {
		return cleanupSessionClaim{}, true, nil
	}

	row := tx.QueryRow(ctx,
		`SELECT status, status_event_id, cleanup_job_id, cleanup_claimed_at, binding_id, binding_generation
		   FROM session_runtime_status
		  WHERE workspace_id = $1
		    AND session_id = $2
		  FOR UPDATE`,
		job.WorkspaceID,
		job.SessionID,
	)
	var runtimeStatus string
	var statusEventID sql.NullString
	var cleanupJobID sql.NullString
	var cleanupClaimedAt sql.NullTime
	var statusBindingID sql.NullString
	var statusBindingGeneration sql.NullInt64
	if err := row.Scan(&runtimeStatus, &statusEventID, &cleanupJobID, &cleanupClaimedAt, &statusBindingID, &statusBindingGeneration); dbconnect.IsNoRows(err) {
		return cleanupSessionClaim{}, true, nil
	} else if err != nil {
		return cleanupSessionClaim{}, false, err
	}
	if runtimeStatus != "idle" || !cleanupJobID.Valid || cleanupJobID.String != job.CleanupJobID || !statusBindingID.Valid || !statusBindingGeneration.Valid {
		return cleanupSessionClaim{}, true, nil
	}
	if options.RequireClaimed && !cleanupClaimedAt.Valid {
		return cleanupSessionClaim{}, true, nil
	}
	// The cleanup alarm is only a hint. The claim and finalize transactions
	// carry the whole-tree proof, while the pod's hot run-slot check remains
	// the final authority when durable thread status lags.
	var treeBusy bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_threads
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND status IN ('running', 'rescheduling')
		)`,
		job.WorkspaceID,
		job.SessionID,
	).Scan(&treeBusy); err != nil {
		return cleanupSessionClaim{}, false, err
	}
	if treeBusy {
		if err := rescheduleBusyCleanupSessionTx(ctx, tx, job, now); err != nil {
			return cleanupSessionClaim{}, false, err
		}
		return cleanupSessionClaim{}, true, nil
	}
	if !statusEventID.Valid || statusEventID.String == "" {
		return cleanupSessionClaim{}, false, runtimeDeliveryPrepareError{kind: "cleanup_idle_fence_invalid", message: "cleanup idle fence is missing", retryable: false}
	}

	var idleThreadID sql.NullString
	var idleStreamPosition int64
	if err := tx.QueryRow(ctx,
		`SELECT session_thread_id, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3
		    AND type = 'session.status_idle'`,
		job.WorkspaceID,
		job.SessionID,
		statusEventID.String,
	).Scan(&idleThreadID, &idleStreamPosition); dbconnect.IsNoRows(err) {
		return cleanupSessionClaim{}, false, runtimeDeliveryPrepareError{kind: "cleanup_idle_fence_invalid", message: "cleanup idle fence event is missing", retryable: false}
	} else if err != nil {
		return cleanupSessionClaim{}, false, err
	}
	if idleStreamPosition <= 0 {
		return cleanupSessionClaim{}, false, runtimeDeliveryPrepareError{kind: "cleanup_idle_fence_invalid", message: "cleanup idle fence stream position is missing", retryable: false}
	}
	threadID := idleThreadID.String
	if threadID == "" {
		threadID = mainThreadID.String
	}
	if threadID == "" {
		return cleanupSessionClaim{}, false, runtimeDeliveryPrepareError{kind: "cleanup_idle_fence_invalid", message: "cleanup thread fence is missing", retryable: false}
	}
	if options.RejectNewerInput {
		newerInput, err := cleanupHasNewerUnprocessedInputTx(ctx, tx, job.WorkspaceID, job.SessionID, idleStreamPosition)
		if err != nil {
			return cleanupSessionClaim{}, false, err
		}
		if newerInput {
			return cleanupSessionClaim{}, true, nil
		}
	}

	binding, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, job.WorkspaceID, job.SessionID)
	if err != nil {
		return cleanupSessionClaim{}, false, err
	}
	if !found || binding.BindingID != statusBindingID.String || binding.BindingGeneration != statusBindingGeneration.Int64 {
		return cleanupSessionClaim{}, true, nil
	}
	if options.SetClaimed && !cleanupClaimedAt.Valid {
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET cleanup_claimed_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND cleanup_job_id = $3
			    AND cleanup_claimed_at IS NULL`,
			job.WorkspaceID,
			job.SessionID,
			job.CleanupJobID,
			now,
		)
		if err != nil {
			return cleanupSessionClaim{}, false, err
		}
		if !rowsAffected(result) {
			return cleanupSessionClaim{}, true, nil
		}
	}
	return cleanupSessionClaim{
		WorkspaceID:        job.WorkspaceID,
		SessionID:          job.SessionID,
		SessionThreadID:    threadID,
		CleanupJobID:       job.CleanupJobID,
		IdleEventID:        statusEventID.String,
		IdleStreamPosition: idleStreamPosition,
		BindingID:          binding.BindingID,
		BindingGeneration:  binding.BindingGeneration,
		PodUID:             binding.PodUID,
		PodIP:              binding.PodIP,
		PodName:            binding.PodName,
		Namespace:          binding.Namespace,
	}, false, nil
}

func rescheduleBusyCleanupSessionTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_after = $4,
		        cleanup_enqueued_at = NULL,
		        cleanup_claimed_at = NULL,
		        cleanup_job_id = NULL,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND cleanup_job_id = $3`,
		job.WorkspaceID,
		job.SessionID,
		job.CleanupJobID,
		now.Add(defaultIdleCleanupDelay),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return runtimeDeliveryPrepareError{kind: "cleanup_busy_reschedule_stale", message: "cleanup busy reschedule fence is stale", retryable: true}
	}
	return nil
}

func cleanupHasNewerUnprocessedInputTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, idleStreamPosition int64) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND latest_stream_position > $3
			   AND processed_at IS NULL
			   AND type IN ('user.message', 'user.interrupt', 'user.tool_confirmation')
		)`,
		workspaceID,
		sessionID,
		idleStreamPosition,
	).Scan(&exists)
	return exists, err
}

func cleanupSessionPayloadJSON(cleanupJobID string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"cleanup_job_id": cleanupJobID,
		"reason":         "expired",
	})
}

// FinalizeRuntimeCleanup settles hot Runtime custody and releases only the
// Runtime binding. Sandbox resources outlive idle cleanup; Session deletion
// records its own durable Sandbox release operation below.
func (s *PostgreSQLRuntimeDeliveryStore) FinalizeRuntimeCleanup(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_reconcile_unavailable",
			ErrorMessage: "runtime delivery store is unavailable",
		}, nil
	}
	if job.Kind == queue.KindSessionDeleteCleanup {
		return s.finalizeSessionDeleteCleanup(ctx, job)
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var claim cleanupSessionClaim
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.cleanup_settle", func(tx *dbconnect.Tx) error {
		loaded, stale, err := loadClaimedCleanupSessionTx(ctx, tx, job, now)
		if err != nil || stale {
			if stale {
				claim = cleanupSessionClaim{}
			}
			return err
		}
		if err := expireCleanupPendingWaitsTx(ctx, tx, loaded, now); err != nil {
			return err
		}
		claim = loaded
		return nil
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if claim.CleanupJobID == "" {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, nil
	}
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.cleanup_finalize", func(tx *dbconnect.Tx) error {
		return finalizeCleanupSessionTx(ctx, tx, claim, now)
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) finalizeSessionDeleteCleanup(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var state sessionDeleteCleanupState
	var stale bool
	var releaseComplete bool
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.session_delete_cleanup_settle", func(tx *dbconnect.Tx) error {
		var err error
		state, stale, err = loadSessionDeleteCleanupStateTx(ctx, tx, job, true)
		if err != nil || stale {
			return err
		}
		if state.HasBinding {
			var streamPosition int64
			if err := tx.QueryRow(ctx,
				`SELECT COALESCE(MAX(latest_stream_position), 0) FROM session_events WHERE workspace_id = $1 AND session_id = $2`,
				job.WorkspaceID, job.SessionID,
			).Scan(&streamPosition); err != nil {
				return err
			}
			claim := cleanupSessionClaim{
				WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, SessionThreadID: state.SessionThreadID,
				CleanupJobID: job.DeleteCleanupID, IdleStreamPosition: streamPosition,
				BindingID: state.Binding.BindingID, BindingGeneration: state.Binding.BindingGeneration,
				PodUID: state.Binding.PodUID, PodIP: state.Binding.PodIP, PodName: state.Binding.PodName,
				Namespace: state.Binding.Namespace,
			}
			if err := expireCleanupPendingWaitsTx(ctx, tx, claim, now); err != nil {
				return err
			}
			if err := finalizeCleanupSessionTx(ctx, tx, claim, now); err != nil {
				return err
			}
		}
		if sessionDeleteCleanupFinalAttempt(job) {
			releaseComplete, err = sandboxrelease.CompleteTx(ctx, tx, job.WorkspaceID, job.SessionID)
		} else {
			_, releaseComplete, err = sandboxrelease.EnsureTx(
				ctx, tx, job.WorkspaceID, job.SessionID, sandboxrelease.SessionDelete, "", now,
			)
			if err == nil {
				var successors int
				successors, err = sandboxrelease.EnsureFailedSessionDeleteTx(ctx, tx, job.WorkspaceID, job.SessionID, now)
				if successors > 0 {
					releaseComplete = false
				}
			}
		}
		return err
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if stale {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}, nil
	}
	if !releaseComplete {
		if sessionDeleteCleanupFinalAttempt(job) {
			return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: false, ErrorKind: "sandbox_release_incomplete", ErrorMessage: "sandbox release is not complete"}, nil
		}
		return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "sandbox_release_retry_later", ErrorMessage: "sandbox release is not complete"}, nil
	}
	return s.finalizeSessionDeleteSandboxCustody(ctx, job, now)
}

func sessionDeleteCleanupFinalAttempt(job RuntimeJob) bool {
	return job.Kind == queue.KindSessionDeleteCleanup && job.MaxAttempts > 0 && job.AttemptCount >= job.MaxAttempts
}

func (s *PostgreSQLRuntimeDeliveryStore) finalizeSessionDeleteSandboxCustody(ctx context.Context, job RuntimeJob, now time.Time) (RuntimeDeliveryResult, error) {
	pending := false
	releaseIncomplete := false
	err := s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.session_delete_cleanup_sandbox_custody", func(tx *dbconnect.Tx) error {
		var err error
		releaseIncomplete, err = hasIncompleteSessionSandboxReleasesTx(ctx, tx, job.WorkspaceID, job.SessionID)
		if err != nil || releaseIncomplete {
			return err
		}
		pending, err = hasOpenSessionSandboxQueueJobsTx(ctx, tx, job.WorkspaceID, job.SessionID)
		if err != nil || pending {
			return err
		}
		pending, err = ensureSessionOutputCaptureCleanupTx(ctx, tx, job.WorkspaceID, job.SessionID, now)
		return err
	})
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	if releaseIncomplete {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "sandbox_release_incomplete",
			ErrorMessage: "sandbox release is not complete",
		}, nil
	}
	if pending {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "sandbox_output_capture_cleanup_pending",
			ErrorMessage: "sandbox output capture cleanup is not complete",
		}, nil
	}
	pending, err = s.deleteSessionSandboxAttachments(ctx, job.WorkspaceID, job.SessionID, now)
	if err != nil {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "sandbox_attachment_cleanup_failed", ErrorMessage: "sandbox attachment cleanup failed"}, nil
	}
	if pending {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryRejected, Retryable: true, ErrorKind: "sandbox_attachment_cleanup_pending", ErrorMessage: "sandbox attachment cleanup is not complete"}, nil
	}
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.session_delete_cleanup_sandbox_rows", func(tx *dbconnect.Tx) error {
		return deleteSessionSandboxRowsTx(ctx, tx, job.WorkspaceID, job.SessionID)
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
}

func hasIncompleteSessionSandboxReleasesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (bool, error) {
	var incomplete bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM sandbox_lifecycle_operations
			 WHERE workspace_id=$1 AND session_id=$2 AND kind='release'
			   AND superseded_by_operation_id IS NULL AND state<>'completed'
		)`,
		workspaceID, sessionID,
	).Scan(&incomplete)
	return incomplete, err
}

func hasOpenSessionSandboxQueueJobsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (bool, error) {
	var open bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM queue_jobs
			 WHERE workspace_id=$1 AND payload_json::jsonb ->> 'session_id'=$2
			   AND kind IN (
			       'sandbox_tool_execute', 'sandbox_activate', 'sandbox_materialize',
			       'sandbox_release', 'sandbox_tool_cancel', 'sandbox_output_capture',
			       'sandbox_output_capture_cleanup', 'sandbox_memory_projection',
			       'sandbox_background_command', 'sandbox_background_reconcile'
			   )
			   AND status IN ('pending','leased')
		)`,
		workspaceID, sessionID,
	).Scan(&open)
	return open, err
}

func (s *PostgreSQLRuntimeDeliveryStore) deleteSessionSandboxAttachments(ctx context.Context, workspaceID string, sessionID string, now time.Time) (bool, error) {
	var rowsToDelete []transientAttachmentGCRow
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.session_delete_cleanup_attachment_claim", func(tx *dbconnect.Tx) error {
		rows, err := tx.Query(ctx,
			`WITH candidates AS (
				SELECT attachment.workspace_id, attachment.attachment_ref, attachment.blob_pointer, attachment.status
				  FROM session_transient_attachments AS attachment
				  JOIN session_runtime_tool_results AS execution
				    ON execution.workspace_id=attachment.workspace_id
				   AND execution.session_id=attachment.session_id
				   AND execution.session_thread_id=attachment.session_thread_id
				   AND execution.tool_use_event_id=attachment.source_tool_use_event_id
				 WHERE attachment.workspace_id=$1 AND attachment.session_id=$2
				   AND execution.tool_kind='sandbox_tool'
				   AND attachment.status IN ('uploading','staged','active','deleting')
				 ORDER BY attachment.storage_sequence
				 LIMIT 100 FOR UPDATE OF attachment SKIP LOCKED
			)
			UPDATE session_transient_attachments AS attachment
			   SET status='deleting', updated_at=$3
			  FROM candidates
			 WHERE attachment.workspace_id=candidates.workspace_id
			   AND attachment.attachment_ref=candidates.attachment_ref
			RETURNING attachment.workspace_id, attachment.attachment_ref,
			          attachment.blob_pointer, candidates.status`,
			workspaceID, sessionID, now.UTC(),
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var row transientAttachmentGCRow
			if err := rows.Scan(&row.WorkspaceID, &row.AttachmentRef, &row.BlobPointer, &row.PreviousStatus); err != nil {
				return err
			}
			rowsToDelete = append(rowsToDelete, row)
		}
		return rows.Err()
	})
	if err != nil {
		return false, err
	}
	if len(rowsToDelete) > 0 && s.AttachmentBlobStore == nil {
		return true, errors.New("sandbox attachment Blob store is unavailable")
	}
	for _, row := range rowsToDelete {
		if err := s.AttachmentBlobStore.Delete(ctx, row.BlobPointer); err != nil {
			var notFound *blob.NotFoundError
			if !errors.As(err, &notFound) {
				return true, err
			}
		}
		if err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.session_delete_cleanup_attachment_deleted", func(tx *dbconnect.Tx) error {
			_, err := tx.Exec(ctx,
				`UPDATE session_transient_attachments SET status='deleted', updated_at=$3
				  WHERE workspace_id=$1 AND attachment_ref=$2 AND status='deleting'`,
				workspaceID, row.AttachmentRef, now.UTC(),
			)
			return err
		}); err != nil {
			return true, err
		}
	}
	var remaining bool
	err = s.Client.WithWorkspaceReadOnlyTx(ctx, workspaceID, "agentruntimebridge.session_delete_cleanup_attachment_remaining", func(tx *dbconnect.Tx) error {
		return tx.QueryRow(ctx,
			`SELECT EXISTS (
				SELECT 1 FROM session_transient_attachments AS attachment
				JOIN session_runtime_tool_results AS execution
				  ON execution.workspace_id=attachment.workspace_id
				 AND execution.session_id=attachment.session_id
				 AND execution.session_thread_id=attachment.session_thread_id
				 AND execution.tool_use_event_id=attachment.source_tool_use_event_id
				WHERE attachment.workspace_id=$1 AND attachment.session_id=$2
				  AND execution.tool_kind='sandbox_tool' AND attachment.status<>'deleted'
			)`, workspaceID, sessionID,
		).Scan(&remaining)
	})
	return remaining, err
}

func deleteSessionSandboxRowsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	statements := []string{
		`DELETE FROM session_transient_attachments AS attachment USING session_runtime_tool_results AS execution
		  WHERE attachment.workspace_id=$1 AND attachment.session_id=$2
		    AND execution.workspace_id=attachment.workspace_id AND execution.session_id=attachment.session_id
		    AND execution.session_thread_id=attachment.session_thread_id
		    AND execution.tool_use_event_id=attachment.source_tool_use_event_id
		    AND execution.tool_kind='sandbox_tool' AND attachment.status='deleted'`,
		`DELETE FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND tool_kind IN ('sandbox_tool','sandbox_background')`,
		`DELETE FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2`,
		`DELETE FROM sandbox_output_capture_blobs WHERE workspace_id=$1 AND session_id=$2`,
		`DELETE FROM sandbox_output_capture_operations WHERE workspace_id=$1 AND session_id=$2`,
		`DELETE FROM sandbox_lifecycle_operations WHERE workspace_id=$1 AND session_id=$2`,
		`DELETE FROM session_sandbox_bindings WHERE workspace_id=$1 AND session_id=$2`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement, workspaceID, sessionID); err != nil {
			return err
		}
	}
	return nil
}

func loadClaimedCleanupSessionTx(ctx context.Context, tx *dbconnect.Tx, job RuntimeJob, now time.Time) (cleanupSessionClaim, bool, error) {
	return claimCleanupSessionTx(ctx, tx, job, now, cleanupClaimOptions{
		RequireClaimed: true,
	})
}

func expireCleanupPendingWaitsTx(ctx context.Context, tx *dbconnect.Tx, claim cleanupSessionClaim, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT p.session_thread_id, p.tool_use_event_id, p.model_tool_call_id, p.tool_name, p.input_json,
		        e.type, COALESCE(e.payload_json::jsonb ->> 'mcp_server_name', '')
		   FROM session_pending_tool_uses p
		   JOIN session_events e
		     ON e.workspace_id = p.workspace_id
		    AND e.session_id = p.session_id
		    AND e.session_thread_id = p.session_thread_id
		    AND e.event_id = p.tool_use_event_id
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.status = 'pending'
		    AND p.kind <> 'approval'
		    AND e.latest_stream_position <= $3
		  ORDER BY e.latest_stream_position ASC, p.tool_use_event_id ASC
		  FOR UPDATE OF p`,
		claim.WorkspaceID,
		claim.SessionID,
		claim.IdleStreamPosition,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var waits []cleanupPendingWait
	for rows.Next() {
		var wait cleanupPendingWait
		if err := rows.Scan(&wait.ThreadID, &wait.ToolUseEventID, &wait.ModelToolCallID, &wait.ToolName, &wait.InputJSON, &wait.EventType, &wait.MCPServerName); err != nil {
			return err
		}
		waits = append(waits, wait)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, wait := range waits {
		scope := &bridgev1.RuntimeScope{
			WorkspaceId:     claim.WorkspaceID,
			SessionId:       claim.SessionID,
			SessionThreadId: wait.ThreadID,
			Binding: &bridgev1.RuntimeBindingRef{
				BindingId:         claim.BindingID,
				BindingGeneration: claim.BindingGeneration,
				TargetPodUid:      claim.PodUID,
			},
		}
		eventID, err := insertPendingToolTerminalResultTx(ctx, tx, scope, wait, pendingToolTerminal{
			PartStatus: "error",
			ErrorType:  "cleanup_expired",
			Message:    "Tool approval expired during session cleanup.",
		}, now)
		if err != nil {
			return err
		}
		if wait.EventType == "agent.tool_use" {
			if err := consumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, wait.ToolUseEventID, eventID, "cleanup_wait_expired", now); err != nil {
				return err
			}
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_pending_tool_uses
			    SET status = 'expired',
			        result_event_id = COALESCE(result_event_id, $5),
			        resolved_at = COALESCE(resolved_at, $6),
			        updated_at = $6
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND tool_use_event_id = $4
			    AND status = 'pending'`,
			claim.WorkspaceID,
			claim.SessionID,
			wait.ThreadID,
			wait.ToolUseEventID,
			eventID,
			now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return runtimeDeliveryPrepareError{kind: "cleanup_pending_wait_expire_stale", message: "cleanup pending wait expiry fence is stale", retryable: false}
		}
	}
	return expireCleanupSandboxExecutionsTx(ctx, tx, claim, now)
}

func expireCleanupSandboxExecutionsTx(ctx context.Context, tx *dbconnect.Tx, claim cleanupSessionClaim, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT r.session_thread_id, r.tool_use_event_id, r.model_tool_call_id, r.tool_name, r.input_json
		   FROM session_runtime_tool_results r
		   JOIN session_events e
		     ON e.workspace_id = r.workspace_id
		    AND e.session_id = r.session_id
		    AND e.session_thread_id = r.session_thread_id
		    AND e.event_id = r.tool_use_event_id
		  WHERE r.workspace_id = $1
		    AND r.session_id = $2
		    AND r.tool_kind = 'sandbox_tool'
		    AND r.execution_state <> 'consumed'
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_pending_tool_uses pending_approval
		         WHERE pending_approval.workspace_id = r.workspace_id
		           AND pending_approval.session_id = r.session_id
		           AND pending_approval.session_thread_id = r.session_thread_id
		           AND pending_approval.tool_use_event_id = r.tool_use_event_id
		           AND pending_approval.kind = 'approval'
		           AND pending_approval.status IN ('pending', 'resolving')
		    )
		    AND e.latest_stream_position <= $3
		  ORDER BY e.latest_stream_position ASC, r.tool_use_event_id ASC`,
		claim.WorkspaceID,
		claim.SessionID,
		claim.IdleStreamPosition,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var executions []cleanupPendingWait
	for rows.Next() {
		var execution cleanupPendingWait
		if err := rows.Scan(
			&execution.ThreadID,
			&execution.ToolUseEventID,
			&execution.ModelToolCallID,
			&execution.ToolName,
			&execution.InputJSON,
		); err != nil {
			return err
		}
		execution.EventType = "agent.tool_use"
		executions = append(executions, execution)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, execution := range executions {
		scope := &bridgev1.RuntimeScope{
			WorkspaceId:     claim.WorkspaceID,
			SessionId:       claim.SessionID,
			SessionThreadId: execution.ThreadID,
			Binding: &bridgev1.RuntimeBindingRef{
				BindingId:         claim.BindingID,
				BindingGeneration: claim.BindingGeneration,
				TargetPodUid:      claim.PodUID,
			},
		}
		eventID, settled, err := toolResultForToolUseExistsTx(ctx, tx, claim.WorkspaceID, claim.SessionID, execution.ThreadID, execution.EventType, execution.ToolUseEventID)
		if err != nil {
			return err
		}
		if !settled {
			eventID, err = insertPendingToolTerminalResultTx(ctx, tx, scope, execution, pendingToolTerminal{
				PartStatus: "error",
				ErrorType:  "cleanup_expired",
				Message:    "Sandbox tool execution expired during session cleanup.",
			}, now)
			if err != nil {
				return err
			}
		}
		if err := markPendingToolResultResolvedTx(ctx, tx, scope, execution.ToolUseEventID, eventID, now); err != nil {
			return err
		}
		if err := consumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, execution.ToolUseEventID, eventID, "cleanup_wait_expired", now); err != nil {
			return err
		}
	}
	return nil
}

func finalizeCleanupSessionTx(ctx context.Context, tx *dbconnect.Tx, claim cleanupSessionClaim, now time.Time) error {
	if claim.CleanupJobID == "" {
		return nil
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4`,
		claim.WorkspaceID,
		claim.SessionID,
		claim.BindingID,
		claim.BindingGeneration,
	); err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET cleanup_after = NULL,
		        cleanup_enqueued_at = NULL,
		        cleanup_claimed_at = NULL,
		        cleanup_job_id = NULL,
		        binding_id = NULL,
		        binding_generation = NULL,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND cleanup_job_id = $3
		    AND binding_id = $4
		    AND binding_generation = $5`,
		claim.WorkspaceID,
		claim.SessionID,
		claim.CleanupJobID,
		claim.BindingID,
		claim.BindingGeneration,
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return runtimeDeliveryPrepareError{kind: "cleanup_finalize_stale", message: "cleanup finalization fence is stale", retryable: false}
	}
	return nil
}

func cleanupRuntimeBoundedJSON(inputJSON string) map[string]any {
	preview := inputJSON
	if preview == "" {
		preview = "{}"
	}
	var value any
	if err := json.Unmarshal([]byte(preview), &value); err != nil {
		value = map[string]any{}
		preview = "{}"
	}
	return map[string]any{
		"value":     value,
		"preview":   preview,
		"truncated": false,
	}
}

func (r KubernetesRuntimeTargetResolver) CleanupTargetProvenGone(_ context.Context, _ *dbconnect.Tx, _ RuntimeJob, binding runtimeBindingForDelivery) (bool, error) {
	if r.Snapshot == nil {
		return false, runtimeDeliveryPrepareError{kind: "runtime_visibility_unavailable", message: "runtime visibility snapshot is unavailable", retryable: true}
	}
	snapshot := r.Snapshot()
	if !snapshot.Ready {
		return false, runtimeDeliveryPrepareError{kind: "runtime_visibility_not_ready", message: "runtime pod visibility is not ready", retryable: true}
	}
	visibility := snapshot.VisibilityFor(enginekubernetes.BoundRuntimePod{
		Namespace: binding.Namespace,
		PodName:   binding.PodName,
		PodUID:    binding.PodUID,
		PodIP:     binding.PodIP,
	})
	switch visibility {
	case enginekubernetes.BindingVisibilityReusable:
		return false, nil
	case enginekubernetes.BindingVisibilityAbsent,
		enginekubernetes.BindingVisibilityDeleted,
		enginekubernetes.BindingVisibilityUIDChanged,
		enginekubernetes.BindingVisibilityIPChanged:
		return true, nil
	case enginekubernetes.BindingVisibilitySnapshotNotReady,
		enginekubernetes.BindingVisibilityNotReady,
		enginekubernetes.BindingVisibilityNotServing,
		enginekubernetes.BindingVisibilityTerminating:
		return false, runtimeDeliveryPrepareError{kind: "runtime_binding_not_available", message: "runtime cleanup target is not currently available: " + string(visibility), retryable: true}
	default:
		return false, runtimeDeliveryPrepareError{kind: "runtime_binding_not_available", message: "runtime cleanup target visibility is not reusable", retryable: true}
	}
}
