package agentruntimebridge

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	enginekubernetes "github.com/tetral-ai/tetral/internal/kubernetes"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/workspace"
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

type RuntimeCleanupDeliveryAuthority struct {
	Active bool
}

func cleanupSessionExactLease(job RuntimeJob) (queue.ExactLeaseRequest, error) {
	if job.Kind != queue.KindCleanupSession || job.WorkspaceID == "" || job.SessionID == "" || job.CleanupJobID == "" ||
		job.JobID == "" || job.LeaseToken == "" || job.PartitionKey == "" || job.DedupeKey == "" {
		return queue.ExactLeaseRequest{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "cleanup delivery authority is incomplete", retryable: false}
	}
	workspaceID := workspace.ID(job.WorkspaceID)
	if job.PartitionKey != queue.FormatSessionPartitionKey(workspaceID, job.SessionID) ||
		job.DedupeKey != queue.FormatCleanupSessionDedupeKey(workspaceID, job.SessionID, job.CleanupJobID) {
		return queue.ExactLeaseRequest{}, runtimeDeliveryPrepareError{kind: "invalid_runtime_job_payload", message: "cleanup delivery authority binding is invalid", retryable: false}
	}
	return queue.ExactLeaseRequest{
		WorkspaceID: workspaceID, JobID: job.JobID, LeaseToken: job.LeaseToken,
		Kind: job.Kind, PartitionKey: job.PartitionKey, DedupeKey: job.DedupeKey,
	}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) RuntimeCleanupDeliveryAuthority(ctx context.Context, job RuntimeJob) (RuntimeCleanupDeliveryAuthority, error) {
	if s == nil || s.Client == nil {
		return RuntimeCleanupDeliveryAuthority{}, runtimeDeliveryPrepareError{kind: "runtime_reconcile_unavailable", message: "runtime cleanup authority store is unavailable", retryable: true}
	}
	lease, err := cleanupSessionExactLease(job)
	if err != nil {
		return RuntimeCleanupDeliveryAuthority{}, err
	}
	authority := RuntimeCleanupDeliveryAuthority{}
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.authorize_cleanup_delivery", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, lease)
		if err != nil {
			return err
		}
		authority.Active = active
		return nil
	})
	return authority, err
}

func (s *PostgreSQLRuntimeDeliveryStore) RescheduleBusyRuntimeCleanup(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, errors.New("runtime cleanup reschedule store is unavailable")
	}
	lease, err := cleanupSessionExactLease(job)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	settled := false
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.reschedule_busy_cleanup", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, lease)
		if err != nil || !active {
			return err
		}
		if err := rescheduleBusyCleanupSessionTx(ctx, tx, job, now); err != nil {
			return err
		}
		settled, err = queue.AckTx(ctx, tx, queue.AckRequest{
			WorkspaceID: lease.WorkspaceID, JobID: lease.JobID, LeaseToken: lease.LeaseToken, Now: now,
		})
		return err
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	if !settled {
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}, nil
	}
	return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted, QueueLeaseSettled: true}, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) FinalizeRuntimeCleanupExhaustion(ctx context.Context, job RuntimeJob, deliveryResult RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	if s == nil || s.Client == nil {
		return RuntimeDeliveryResult{}, errors.New("runtime cleanup exhaustion store is unavailable")
	}
	if job.MaxAttempts <= 0 || job.AttemptCount < job.MaxAttempts {
		return RuntimeDeliveryResult{}, runtimeDeliveryPrepareError{kind: "cleanup_exhaustion_invalid", message: "runtime cleanup attempts are not exhausted", retryable: false}
	}
	lease, err := cleanupSessionExactLease(job)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	errorKind := deliveryResult.ErrorKind
	if errorKind == "" {
		errorKind = "runtime_cleanup_exhausted"
	}
	errorMessage := deliveryResult.ErrorMessage
	if errorMessage == "" {
		errorMessage = "runtime cleanup attempts are exhausted"
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	outcome := RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.finalize_cleanup_exhaustion", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, lease)
		if err != nil {
			return err
		}
		if !active {
			var queueStatus string
			if err := tx.QueryRow(ctx,
				`SELECT status FROM queue_jobs
				  WHERE workspace_id=$1 AND id=$2 AND kind=$3 AND partition_key=$4 AND dedupe_key=$5`,
				job.WorkspaceID, job.JobID, job.Kind, job.PartitionKey, job.DedupeKey,
			).Scan(&queueStatus); err != nil && !dbconnect.IsNoRows(err) {
				return err
			}
			if queueStatus == queue.StatusDeadLettered {
				outcome = RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}
			}
			return nil
		}
		deadLettered, err := queue.DeadLetterTx(ctx, tx, queue.DeadLetterRequest{
			WorkspaceID: lease.WorkspaceID, JobID: lease.JobID, LeaseToken: lease.LeaseToken,
			ErrorKind: errorKind, ErrorMessage: errorMessage, Now: now,
		})
		if err != nil {
			return err
		}
		if !deadLettered {
			return nil
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET cleanup_after=$4, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL,
			        cleanup_job_id=NULL, updated_at=$5
			  WHERE workspace_id=$1 AND session_id=$2 AND cleanup_job_id=$3`,
			job.WorkspaceID, job.SessionID, job.CleanupJobID, now.Add(defaultIdleCleanupDelay), now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return runtimeDeliveryPrepareError{kind: "cleanup_exhaustion_stale", message: "runtime cleanup exhaustion marker is stale", retryable: false}
		}
		outcome = RuntimeDeliveryResult{
			Status: RuntimeDeliveryRejected, Retryable: false,
			ErrorKind: errorKind, ErrorMessage: errorMessage,
			QueueLeaseSettled: true,
		}
		return nil
	})
	return outcome, err
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
	return RuntimeCommandPlan{
		Target:           RuntimePodTarget{Namespace: state.Binding.Namespace, PodName: state.Binding.PodName, PodUID: state.Binding.PodUID, PodIP: state.Binding.PodIP, Port: port},
		AttemptedBinding: RuntimeAttemptedBinding{BindingID: state.Binding.BindingID, Generation: state.Binding.BindingGeneration, TargetPodUID: state.Binding.PodUID},
		CleanupSession: &agentruntimev1.CleanupSessionRequest{
			WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, BindingId: state.Binding.BindingID,
			BindingGeneration: state.Binding.BindingGeneration, TargetPodUid: state.Binding.PodUID,
			CleanupOperationId: job.DeleteCleanupID, Reason: agentruntimev1.CleanupSessionReason_CLEANUP_SESSION_REASON_OPERATOR_REQUESTED,
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
	return RuntimeCommandPlan{
		Target: RuntimePodTarget{
			Namespace: claim.Namespace,
			PodName:   claim.PodName,
			PodUID:    claim.PodUID,
			PodIP:     claim.PodIP,
			Port:      port,
		},
		AttemptedBinding: RuntimeAttemptedBinding{BindingID: claim.BindingID, Generation: claim.BindingGeneration, TargetPodUID: claim.PodUID},
		CleanupSession: &agentruntimev1.CleanupSessionRequest{
			WorkspaceId: job.WorkspaceID, SessionId: job.SessionID, BindingId: claim.BindingID,
			BindingGeneration: claim.BindingGeneration, TargetPodUid: claim.PodUID,
			CleanupOperationId: job.CleanupJobID, Reason: agentruntimev1.CleanupSessionReason_CLEANUP_SESSION_REASON_EXPIRED,
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
	lease, err := cleanupSessionExactLease(job)
	if err != nil {
		return RuntimeDeliveryResult{}, err
	}
	now := storage.Now()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	outcome := RuntimeDeliveryResult{Status: RuntimeDeliveryAuthorityLost}
	err = s.Client.WithWorkspaceTx(ctx, job.WorkspaceID, "agentruntimebridge.cleanup_settle", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		active, err := queue.AssertExactLeaseTx(ctx, tx, lease)
		if err != nil || !active {
			return err
		}
		loaded, stale, err := loadClaimedCleanupSessionTx(ctx, tx, job, now)
		if err != nil {
			return err
		}
		if stale {
			settled, err := queue.AckTx(ctx, tx, queue.AckRequest{
				WorkspaceID: lease.WorkspaceID, JobID: lease.JobID, LeaseToken: lease.LeaseToken, Now: now,
			})
			if err != nil {
				return err
			}
			if settled {
				outcome = RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate, QueueLeaseSettled: true}
			}
			return nil
		}
		if err := expireCleanupSandboxExecutionsTx(ctx, tx, loaded, now); err != nil {
			return err
		}
		if err := finalizeCleanupSessionTx(ctx, tx, loaded, now); err != nil {
			return err
		}
		settled, err := queue.AckTx(ctx, tx, queue.AckRequest{
			WorkspaceID: lease.WorkspaceID, JobID: lease.JobID, LeaseToken: lease.LeaseToken, Now: now,
		})
		if err != nil {
			return err
		}
		if settled {
			outcome = RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted, QueueLeaseSettled: true}
		}
		return nil
	})
	if err != nil {
		return runtimeDeliveryResultFromPrepareError(err), nil
	}
	return outcome, nil
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
			if err := expireCleanupSandboxExecutionsTx(ctx, tx, claim, now); err != nil {
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

func expireCleanupSandboxExecutionsTx(ctx context.Context, tx *dbconnect.Tx, claim cleanupSessionClaim, now time.Time) error {
	rows, err := tx.Query(ctx,
		`SELECT r.session_thread_id, r.tool_use_event_id, e.model_request_id, r.model_tool_call_id
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
			&execution.ModelRequestID,
			&execution.ModelToolCallID,
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
				ErrorType: "cleanup_expired",
				Message:   "Sandbox tool execution expired during session cleanup.",
			}, now)
			if err != nil {
				return err
			}
		}
		if err := resolveSettledToolRouteTx(ctx, tx, scope, execution.ToolUseEventID, eventID, now); err != nil {
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
