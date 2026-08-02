// Package release owns provider-neutral Sandbox release declarations. It
// records Session fences, closes pre-provider execution waiters, and creates
// durable Queue work; provider inspection and mutation remain in Sandbox
// Service.
package release

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	lifecycleOperationIDPrefix   = "sop_"
	backgroundCommandMaxAttempts = 5
)

// MaxAttempts is the bounded Queue delivery budget for provider release work.
const MaxAttempts = 5

type Reason string

const (
	SessionDelete  Reason = "session_delete"
	ReplacedHandle Reason = "replaced_handle"
)

type binding struct {
	logicalSandboxID   string
	providerResourceID string
	releaseReason      string
}

// EnsureTx records one current-handle release fence and its durable provider
// operation inside the caller's Session transaction.
func EnsureTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason Reason, targetHandle string, now time.Time) (string, bool, error) {
	return ensureTx(ctx, tx, workspaceID, sessionID, reason, targetHandle, now, applyQueueChangesTx)
}

// DeclareTx records a release and returns its Queue changes without applying
// them, so a caller with other outputs can preserve the transaction lock order.
func DeclareTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason Reason, targetHandle string, now time.Time) (string, bool, []queue.EnqueueRequest, []queue.TargetedCancelRequest, error) {
	var enqueueRequests []queue.EnqueueRequest
	var cancelRequests []queue.TargetedCancelRequest
	operationID, complete, err := ensureTx(
		ctx, tx, workspaceID, sessionID, reason, targetHandle, now,
		func(_ context.Context, _ *dbconnect.Tx, enqueues []queue.EnqueueRequest, cancels []queue.TargetedCancelRequest) error {
			enqueueRequests = append(enqueueRequests, enqueues...)
			cancelRequests = append(cancelRequests, cancels...)
			return nil
		},
	)
	return operationID, complete, enqueueRequests, cancelRequests, err
}

type queueChangeApplier func(context.Context, *dbconnect.Tx, []queue.EnqueueRequest, []queue.TargetedCancelRequest) error

func ensureTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason Reason, targetHandle string, now time.Time, applyQueueChanges queueChangeApplier) (string, bool, error) {
	if tx == nil || workspaceID == "" || sessionID == "" || !validReason(reason) {
		return "", false, errors.New("sandbox release identity is invalid")
	}
	if now.IsZero() {
		return "", false, errors.New("sandbox release time is required")
	}
	if err := lockSession(ctx, tx, workspaceID, sessionID); err != nil {
		return "", false, err
	}
	currentBinding, exists, err := lockBinding(ctx, tx, workspaceID, sessionID)
	if err != nil || !exists {
		return "", true, err
	}
	currentBindingRelease := reason != ReplacedHandle
	var cancelRequests []queue.TargetedCancelRequest
	if currentBindingRelease {
		if targetHandle == "" {
			targetHandle = currentBinding.providerResourceID
		}
		strongest := strongerReason(Reason(currentBinding.releaseReason), reason)
		if _, err := tx.Exec(ctx,
			`UPDATE session_sandbox_bindings
			    SET release_requested_at=COALESCE(release_requested_at,$3), release_reason=$4, updated_at=$3
			  WHERE workspace_id=$1 AND session_id=$2`,
			workspaceID, sessionID, now.UTC(), strongest,
		); err != nil {
			return "", false, err
		}
		reason = strongest
		if err := lockLifecycleOperationsTx(ctx, tx, workspaceID, currentBinding.logicalSandboxID); err != nil {
			return "", false, err
		}
		if err := settleWaitersTx(ctx, tx, workspaceID, sessionID, reason, now); err != nil {
			return "", false, err
		}
		cancelRequests, err = abandonDependenciesTx(ctx, tx, workspaceID, currentBinding.logicalSandboxID, now)
		if err != nil {
			return "", false, err
		}
	}
	if targetHandle == "" {
		if err := applyQueueChanges(ctx, tx, nil, cancelRequests); err != nil {
			return "", false, err
		}
		unresolved, err := hasUnresolvedProviderCreationTx(ctx, tx, workspaceID, currentBinding.logicalSandboxID)
		return "", !unresolved, err
	}

	var operationID, state, storedReason string
	err = tx.QueryRow(ctx,
		`SELECT operation_id, state, release_reason
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND logical_sandbox_id=$2 AND kind='release'
		    AND target_provider_resource_id=$3 AND superseded_by_operation_id IS NULL
		  ORDER BY created_at DESC, operation_id DESC LIMIT 1 FOR UPDATE`,
		workspaceID, currentBinding.logicalSandboxID, targetHandle,
	).Scan(&operationID, &state, &storedReason)
	if err != nil && !dbconnect.IsNoRows(err) {
		return "", false, err
	}
	if err == nil {
		strongest := strongerReason(Reason(storedReason), reason)
		switch state {
		case "pending", "running":
			if strongest != Reason(storedReason) {
				if _, err := tx.Exec(ctx,
					`UPDATE sandbox_lifecycle_operations SET release_reason=$3, updated_at=$4
					  WHERE workspace_id=$1 AND operation_id=$2`,
					workspaceID, operationID, strongest, now.UTC(),
				); err != nil {
					return "", false, err
				}
			}
			var requests []queue.EnqueueRequest
			if currentBindingRelease {
				backgroundRequests, err := ensureBackgroundCancelsTx(ctx, tx, workspaceID, sessionID, operationID, targetHandle, now)
				if err != nil {
					return "", false, err
				}
				requests = append(requests, backgroundRequests...)
			}
			if state == "pending" {
				releaseRequest, ready, err := pendingReleaseEnqueueRequestTx(
					ctx, tx, workspaceID, sessionID, currentBinding.logicalSandboxID, operationID, targetHandle, now,
				)
				if err != nil {
					return "", false, err
				}
				if ready {
					requests = append(requests, releaseRequest)
				}
			}
			if err := applyQueueChanges(ctx, tx, requests, cancelRequests); err != nil {
				return "", false, err
			}
			return operationID, false, nil
		case "completed":
			if err := ApplyPostconditionTx(ctx, tx, workspaceID, sessionID, currentBinding.providerResourceID, targetHandle, strongest, now); err != nil {
				return "", false, err
			}
			return operationID, true, applyQueueChanges(ctx, tx, nil, cancelRequests)
		case "failed":
			predecessorID := operationID
			operationID, releaseRequest, err := insertOperationTx(ctx, tx, workspaceID, sessionID, currentBinding.logicalSandboxID, targetHandle, strongest, now)
			if err != nil {
				return "", false, err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE sandbox_lifecycle_operations SET superseded_by_operation_id=$3, updated_at=$4
				  WHERE workspace_id=$1 AND operation_id=$2 AND state='failed' AND superseded_by_operation_id IS NULL`,
				workspaceID, predecessorID, operationID, now.UTC(),
			); err != nil {
				return "", false, err
			}
			requests := []queue.EnqueueRequest{releaseRequest}
			if currentBindingRelease {
				cancelRequests, err := ensureBackgroundCancelsTx(ctx, tx, workspaceID, sessionID, operationID, targetHandle, now)
				if err != nil {
					return "", false, err
				}
				requests = append(requests, cancelRequests...)
			}
			if err := applyQueueChanges(ctx, tx, requests, cancelRequests); err != nil {
				return "", false, err
			}
			return operationID, false, nil
		default:
			return "", false, errors.New("sandbox release operation has an invalid state")
		}
	}

	operationID, releaseRequest, err := insertOperationTx(ctx, tx, workspaceID, sessionID, currentBinding.logicalSandboxID, targetHandle, reason, now)
	if err != nil {
		return "", false, err
	}
	requests := []queue.EnqueueRequest{releaseRequest}
	if currentBindingRelease {
		cancelRequests, err := ensureBackgroundCancelsTx(ctx, tx, workspaceID, sessionID, operationID, targetHandle, now)
		if err != nil {
			return "", false, err
		}
		requests = append(requests, cancelRequests...)
	}
	err = applyQueueChanges(ctx, tx, requests, cancelRequests)
	return operationID, false, err
}

// EnsureFailedSessionDeleteTx gives each failed, unsuperseded handle release
// one successor under the caller's finite Session-delete cleanup delivery.
func EnsureFailedSessionDeleteTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time) (int, error) {
	if tx == nil || workspaceID == "" || sessionID == "" || now.IsZero() {
		return 0, errors.New("sandbox release identity is invalid")
	}
	rows, err := tx.Query(ctx,
		`SELECT target_provider_resource_id
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND session_id=$2 AND kind='release'
		    AND state='failed' AND superseded_by_operation_id IS NULL
		  ORDER BY target_provider_resource_id, operation_id
		  FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return 0, err
	}
	var handles []string
	for rows.Next() {
		var handle string
		if err := rows.Scan(&handle); err != nil {
			_ = rows.Close()
			return 0, err
		}
		handles = append(handles, handle)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, handle := range handles {
		if _, _, err := EnsureTx(ctx, tx, workspaceID, sessionID, SessionDelete, handle, now); err != nil {
			return 0, err
		}
	}
	return len(handles), nil
}

func BackgroundCancelRequestID(operationID string, taskID string, generation int64) string {
	return operationID + ":cancel:" + taskID + ":" + strconv.FormatInt(generation, 10)
}

func BackgroundCancelEnqueueRequest(workspaceID string, sessionID string, taskID string, requestID string, now time.Time) (queue.EnqueueRequest, error) {
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "session_id": sessionID, "task_id": taskID, "request_id": requestID,
	})
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	ws := workspace.ID(workspaceID)
	return queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: ws, Kind: queue.KindSandboxBackgroundCommand,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundCommandDedupeKey(ws, sessionID, taskID, requestID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: backgroundCommandMaxAttempts, Now: now.UTC(),
	}, nil
}

func ApplyPostconditionTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, currentHandle string, targetHandle string, reason Reason, now time.Time) error {
	if currentHandle != targetHandle {
		return nil
	}
	switch reason {
	case SessionDelete:
		_, err := tx.Exec(ctx,
			`UPDATE session_sandbox_bindings
			    SET provider_resource_id=NULL, binding_revision=binding_revision+1,
			        materialized_resource_revision=0, resource_credential_expires_at=NULL,
			        resource_roots_json='[]', helper_verified_at=NULL,
			        provider_metadata_json='{}', updated_at=$3
			  WHERE workspace_id=$1 AND session_id=$2`,
			workspaceID, sessionID, now.UTC(),
		)
		return err
	case ReplacedHandle:
		return nil
	default:
		return errors.New("sandbox release reason is invalid")
	}
}

func pendingReleaseEnqueueRequestTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	logicalSandboxID string,
	operationID string,
	targetHandle string,
	now time.Time,
) (queue.EnqueueRequest, bool, error) {
	blocked, err := BlockedTx(ctx, tx, workspaceID, sessionID, logicalSandboxID, operationID, targetHandle)
	if err != nil || blocked {
		return queue.EnqueueRequest{}, false, err
	}

	var currentJobID sql.NullString
	if err := tx.QueryRow(ctx,
		`SELECT queue_job_id FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND operation_id=$2 AND kind='release' AND state='pending'
		  FOR UPDATE`,
		workspaceID, operationID,
	).Scan(&currentJobID); err != nil {
		return queue.EnqueueRequest{}, false, err
	}
	if currentJobID.Valid {
		var status string
		err := tx.QueryRow(ctx,
			`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`,
			workspaceID, currentJobID.String,
		).Scan(&status)
		if err != nil && !dbconnect.IsNoRows(err) {
			return queue.EnqueueRequest{}, false, err
		}
		if err == nil && (status == queue.StatusPending || status == queue.StatusLeased) {
			return queue.EnqueueRequest{}, false, nil
		}
	}

	jobID := queue.NewJobID()
	partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(workspaceID), logicalSandboxID)
	dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxRelease, workspace.ID(workspaceID), logicalSandboxID, operationID)
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "session_id": sessionID,
		"logical_sandbox_id": logicalSandboxID, "operation_id": operationID,
	})
	if err != nil {
		return queue.EnqueueRequest{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE sandbox_lifecycle_operations
		    SET queue_job_id=$3, queue_kind=$4, queue_partition_key=$5, queue_dedupe_key=$6,
		        lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL,
		        attempt_count=0, updated_at=$7
		  WHERE workspace_id=$1 AND operation_id=$2 AND state='pending'`,
		workspaceID, operationID, jobID, queue.KindSandboxRelease, partitionKey, dedupeKey, now.UTC(),
	); err != nil {
		return queue.EnqueueRequest{}, false, err
	}
	return queue.EnqueueRequest{
		ID: jobID, WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxRelease,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: payload, MaxAttempts: MaxAttempts, Now: now.UTC(),
	}, true, nil
}

// BlockedTx reports whether provider work still owns the handle targeted by a
// release operation. Callers hold the Session and lifecycle locks that define
// the snapshot before asking this shared predicate.
func BlockedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, logicalSandboxID string, operationID string, targetHandle string) (bool, error) {
	var blocked bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_runtime_tool_results
			 WHERE workspace_id=$1 AND session_id=$2 AND tool_kind='sandbox_tool'
			   AND execution_state IN ('preparing','running')
			   AND authorized_provider_resource_id=$3
			UNION ALL
			SELECT 1 FROM session_background_tasks
			 WHERE workspace_id=$1 AND session_id=$2 AND status='running'
			   AND provider_session_id=$3
			UNION ALL
			SELECT 1 FROM sandbox_lifecycle_operations
			 WHERE workspace_id=$1 AND logical_sandbox_id=$4 AND operation_id<>$5
			   AND kind IN ('create','start','replace','materialize') AND state='running'
		)`,
		workspaceID, sessionID, targetHandle, logicalSandboxID, operationID,
	).Scan(&blocked)
	return blocked, err
}

// CompleteTx reports whether Session-delete release custody is closed without
// creating or superseding a release operation. Callers use it on their final
// bounded cleanup delivery, where another provider attempt is forbidden.
func CompleteTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (bool, error) {
	if tx == nil || workspaceID == "" || sessionID == "" {
		return false, errors.New("sandbox release identity is invalid")
	}
	if err := lockSession(ctx, tx, workspaceID, sessionID); err != nil {
		return false, err
	}
	currentBinding, exists, err := lockBinding(ctx, tx, workspaceID, sessionID)
	if err != nil || !exists {
		return !exists, err
	}
	if currentBinding.providerResourceID == "" {
		unresolved, err := hasUnresolvedProviderCreationTx(ctx, tx, workspaceID, currentBinding.logicalSandboxID)
		return !unresolved, err
	}
	var state string
	err = tx.QueryRow(ctx,
		`SELECT state FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND logical_sandbox_id=$2 AND kind='release'
		    AND target_provider_resource_id=$3 AND superseded_by_operation_id IS NULL
		  ORDER BY created_at DESC, operation_id DESC LIMIT 1 FOR UPDATE`,
		workspaceID, currentBinding.logicalSandboxID, currentBinding.providerResourceID,
	).Scan(&state)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	return state == "completed", err
}

func hasUnresolvedProviderCreationTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, logicalSandboxID string) (bool, error) {
	var unresolved bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM sandbox_lifecycle_operations
			 WHERE workspace_id=$1 AND logical_sandbox_id=$2
			   AND kind IN ('create','replace')
			   AND (state='running' OR (state='failed' AND outcome_effect_boundary='outcome_unknown'))
		)`,
		workspaceID, logicalSandboxID,
	).Scan(&unresolved)
	return unresolved, err
}

func lockSession(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) error {
	var exists bool
	return tx.QueryRow(ctx,
		`SELECT TRUE FROM sessions WHERE workspace_id=$1 AND id=$2 FOR UPDATE`,
		workspaceID, sessionID,
	).Scan(&exists)
}

func lockBinding(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string) (binding, bool, error) {
	var current binding
	var providerResourceID, releaseReason sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT logical_sandbox_id, provider_resource_id, release_reason
		   FROM session_sandbox_bindings
		  WHERE workspace_id=$1 AND session_id=$2 FOR UPDATE`,
		workspaceID, sessionID,
	).Scan(&current.logicalSandboxID, &providerResourceID, &releaseReason)
	if dbconnect.IsNoRows(err) {
		return binding{}, false, nil
	}
	if err != nil {
		return binding{}, false, err
	}
	current.providerResourceID = providerResourceID.String
	current.releaseReason = releaseReason.String
	return current, true, nil
}

func lockLifecycleOperationsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, logicalSandboxID string) error {
	rows, err := tx.Query(ctx,
		`SELECT operation_id FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND logical_sandbox_id=$2
		  ORDER BY operation_id FOR UPDATE`,
		workspaceID, logicalSandboxID,
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			_ = rows.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func ensureBackgroundCancelsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, operationID string, targetHandle string, now time.Time) ([]queue.EnqueueRequest, error) {
	rows, err := tx.Query(ctx,
		`SELECT session_thread_id, task_id, reconcile_generation, release_operation_id
		   FROM session_background_tasks
		  WHERE workspace_id=$1 AND session_id=$2 AND provider_session_id=$3 AND status='running'
		  ORDER BY session_thread_id, task_id FOR UPDATE`,
		workspaceID, sessionID, targetHandle,
	)
	if err != nil {
		return nil, err
	}
	type task struct {
		threadID, taskID   string
		generation         int64
		releaseOperationID sql.NullString
	}
	var tasks []task
	for rows.Next() {
		var item task
		if err := rows.Scan(&item.threadID, &item.taskID, &item.generation, &item.releaseOperationID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		tasks = append(tasks, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	requests := make([]queue.EnqueueRequest, 0, len(tasks))
	for _, item := range tasks {
		if item.releaseOperationID.Valid && item.releaseOperationID.String != operationID {
			return nil, errors.New("sandbox background task is fenced by another release operation")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_background_tasks SET release_operation_id=$4, updated_at=$5
			  WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND status='running'
			    AND (release_operation_id IS NULL OR release_operation_id=$4)`,
			workspaceID, sessionID, item.taskID, operationID, now.UTC(),
		); err != nil {
			return nil, err
		}
		requestID := BackgroundCancelRequestID(operationID, item.taskID, item.generation)
		var exists bool
		if err := tx.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM session_runtime_tool_results
			  WHERE workspace_id=$1 AND session_id=$2 AND tool_kind='sandbox_background'
			    AND background_request_id=$3)`,
			workspaceID, sessionID, requestID,
		).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			continue
		}
		inputJSON := `{"reason":"sandbox_release"}`
		inputDigest := sha256.Sum256([]byte(inputJSON))
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_runtime_tool_results (
				workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
				normalized_input_hash, tool_name, input_json, ack_status,
				background_operation_kind, background_operation_state, background_request_id,
				background_task_id, background_max_output_tokens, created_at, updated_at
			) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'cancel_command',$6,'committed',
				'cancel','pending',$7,$8,0,$9,$9)`,
			workspaceID, sessionID, item.threadID, "background_receipt:"+requestID,
			hex.EncodeToString(inputDigest[:]), inputJSON, requestID, item.taskID, now.UTC(),
		); err != nil {
			return nil, err
		}
		request, err := BackgroundCancelEnqueueRequest(workspaceID, sessionID, item.taskID, requestID, now)
		if err != nil {
			return nil, err
		}
		requests = append(requests, request)
	}
	return requests, nil
}

func insertOperationTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, logicalSandboxID string, targetHandle string, reason Reason, now time.Time) (string, queue.EnqueueRequest, error) {
	operationID := id.New(lifecycleOperationIDPrefix)
	jobID := queue.NewJobID()
	partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(workspaceID), logicalSandboxID)
	dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxRelease, workspace.ID(workspaceID), logicalSandboxID, operationID)
	payload, err := json.Marshal(map[string]string{
		"workspace_id": workspaceID, "session_id": sessionID,
		"logical_sandbox_id": logicalSandboxID, "operation_id": operationID,
	})
	if err != nil {
		return "", queue.EnqueueRequest{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO sandbox_lifecycle_operations (
			workspace_id, operation_id, session_id, logical_sandbox_id, kind, state,
			target_provider_resource_id, release_reason, queue_job_id, queue_kind,
			queue_partition_key, queue_dedupe_key, created_at, updated_at
		) VALUES ($1,$2,$3,$4,'release','pending',$5,$6,$7,$8,$9,$10,$11,$11)`,
		workspaceID, operationID, sessionID, logicalSandboxID, targetHandle, reason,
		jobID, queue.KindSandboxRelease, partitionKey, dedupeKey, now.UTC(),
	); err != nil {
		return "", queue.EnqueueRequest{}, err
	}
	return operationID, queue.EnqueueRequest{
		ID: jobID, WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxRelease,
		PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
		PayloadJSON: payload, MaxAttempts: MaxAttempts, Now: now.UTC(),
	}, nil
}

func settleWaitersTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason Reason, now time.Time) error {
	if reason != SessionDelete {
		return errors.New("current binding release reason is invalid")
	}
	rows, err := tx.Query(ctx,
		`SELECT session_thread_id, tool_use_event_id, execution_attempt_generation
		   FROM session_runtime_tool_results
		  WHERE workspace_id=$1 AND session_id=$2 AND tool_kind='sandbox_tool'
		    AND execution_state IN ('pending','waiting_activation','waiting_materialization')
		  ORDER BY session_thread_id, tool_use_event_id FOR UPDATE`,
		workspaceID, sessionID,
	)
	if err != nil {
		return err
	}
	type waiter struct {
		threadID, toolUseEventID string
		generation               int64
	}
	var waiters []waiter
	for rows.Next() {
		var item waiter
		if err := rows.Scan(&item.threadID, &item.toolUseEventID, &item.generation); err != nil {
			_ = rows.Close()
			return err
		}
		waiters = append(waiters, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	resultJSON := `{"error":{"kind":"session_deleted","message":"sandbox execution is no longer available"},"status":"error"}`
	digest := sha256.Sum256([]byte(resultJSON))
	for _, item := range waiters {
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET execution_state='terminal_unconsumed', result_json=$6, result_digest=$7,
			        provider_command_reference_json=NULL, background_task_started=FALSE,
			        task_id=NULL, updated_at=$8
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			    AND tool_use_event_id=$4 AND execution_attempt_generation=$5
			    AND execution_state IN ('pending','waiting_activation','waiting_materialization')`,
			workspaceID, sessionID, item.threadID, item.toolUseEventID, item.generation,
			resultJSON, hex.EncodeToString(digest[:]), now.UTC(),
		); err != nil {
			return err
		}
	}
	return nil
}

func abandonDependenciesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, logicalSandboxID string, now time.Time) ([]queue.TargetedCancelRequest, error) {
	rows, err := tx.Query(ctx,
		`SELECT operation_id, queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key
		   FROM sandbox_lifecycle_operations
		  WHERE workspace_id=$1 AND logical_sandbox_id=$2
		    AND kind IN ('create','start','replace','materialize')
		    AND state IN ('pending','waiting_artifact','waiting_activation')
		  ORDER BY operation_id FOR UPDATE`,
		workspaceID, logicalSandboxID,
	)
	if err != nil {
		return nil, err
	}
	type dependency struct {
		operationID string
		jobID       sql.NullString
		kind        sql.NullString
		partition   sql.NullString
		dedupe      sql.NullString
	}
	var dependencies []dependency
	for rows.Next() {
		var item dependency
		if err := rows.Scan(&item.operationID, &item.jobID, &item.kind, &item.partition, &item.dedupe); err != nil {
			_ = rows.Close()
			return nil, err
		}
		dependencies = append(dependencies, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	cancelRequests := make([]queue.TargetedCancelRequest, 0, len(dependencies))
	for _, item := range dependencies {
		if item.jobID.Valid && item.kind.Valid && item.partition.Valid && item.dedupe.Valid {
			cancelRequests = append(cancelRequests, queue.TargetedCancelRequest{
				WorkspaceID: workspace.ID(workspaceID), JobID: item.jobID.String, Kind: item.kind.String,
				PartitionKey: item.partition.String, DedupeKey: item.dedupe.String, Now: now.UTC(),
			})
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state='abandoned', completed_at=$3, updated_at=$3
			  WHERE workspace_id=$1 AND operation_id=$2
			    AND state IN ('pending','waiting_artifact','waiting_activation')`,
			workspaceID, item.operationID, now.UTC(),
		); err != nil {
			return nil, err
		}
	}
	return cancelRequests, nil
}

func applyQueueChangesTx(ctx context.Context, tx *dbconnect.Tx, enqueueRequests []queue.EnqueueRequest, cancelRequests []queue.TargetedCancelRequest) error {
	if len(enqueueRequests) > 0 {
		if _, err := queue.EnqueueBatchTx(ctx, tx, enqueueRequests); err != nil {
			return err
		}
	}
	sort.Slice(cancelRequests, func(i, j int) bool {
		return cancelRequests[i].JobID < cancelRequests[j].JobID
	})
	for _, request := range cancelRequests {
		if _, err := queue.CancelTx(ctx, tx, request); err != nil {
			return err
		}
	}
	return nil
}

func validReason(reason Reason) bool {
	return reason == SessionDelete || reason == ReplacedHandle
}

func strongerReason(current Reason, incoming Reason) Reason {
	if reasonRank(incoming) > reasonRank(current) {
		return incoming
	}
	return current
}

func reasonRank(reason Reason) int {
	switch reason {
	case SessionDelete:
		return 3
	case ReplacedHandle:
		return 1
	default:
		return 0
	}
}
