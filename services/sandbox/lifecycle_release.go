package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/workspace"
)

const (
	sandboxReleaseMaxAttempts                  = 5
	releaseBackgroundCancelSuccessorBackoffCap = 30 * time.Second
)

// EnsureSandboxReleaseTx records the provider-neutral release declaration in
// the caller's Session transaction. Provider work remains in Sandbox Service.
func EnsureSandboxReleaseTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason SandboxReleaseReason, targetHandle string, now time.Time) (string, bool, error) {
	return sandboxrelease.EnsureTx(ctx, tx, workspaceID, sessionID, reason, targetHandle, now)
}

func declareSandboxReleaseTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, reason SandboxReleaseReason, targetHandle string, now time.Time) ([]queue.EnqueueRequest, []queue.TargetedCancelRequest, error) {
	_, _, enqueues, cancels, err := sandboxrelease.DeclareTx(ctx, tx, workspaceID, sessionID, reason, targetHandle, now)
	return enqueues, cancels, err
}

func releaseBackgroundCancelRequestID(operationID string, taskID string, generation int64) string {
	return sandboxrelease.BackgroundCancelRequestID(operationID, taskID, generation)
}

func releaseBackgroundCancelSuccessorEnqueueRequest(workspaceID string, sessionID string, taskID string, requestID string, now time.Time) (queue.EnqueueRequest, error) {
	request, err := sandboxrelease.BackgroundCancelEnqueueRequest(workspaceID, sessionID, taskID, requestID, now)
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	request.AvailableAt = now.UTC().Add(releaseBackgroundCancelSuccessorBackoffCap)
	return request, nil
}

func enqueueReadySandboxReleasesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time) error {
	requests, err := readySandboxReleaseRequestsTx(ctx, tx, workspaceID, sessionID, now, nil)
	if err != nil {
		return err
	}
	_, err = queue.EnqueueBatchTx(ctx, tx, requests)
	return err
}

func readySandboxReleaseRequestsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time, stagedJobIDs map[string]struct{}) ([]queue.EnqueueRequest, error) {
	// Callers lock the Session binding and the complete lifecycle set first. This
	// helper only reads that snapshot before acquiring outgoing Queue locks.
	rows, err := tx.Query(ctx,
		`SELECT o.operation_id, o.logical_sandbox_id, o.target_provider_resource_id,
		        o.queue_job_id, o.queue_kind, o.queue_partition_key, o.queue_dedupe_key
		   FROM sandbox_lifecycle_operations o
		  WHERE o.workspace_id=$1 AND o.session_id=$2 AND o.kind='release' AND o.state='pending'
		    AND o.superseded_by_operation_id IS NULL
		  ORDER BY o.operation_id`,
		workspaceID, sessionID,
	)
	if err != nil {
		return nil, err
	}
	type pendingRelease struct {
		operationID, logicalSandboxID, targetHandle string
		jobID, kind, partition, dedupe              sql.NullString
	}
	var pending []pendingRelease
	for rows.Next() {
		var item pendingRelease
		if err := rows.Scan(&item.operationID, &item.logicalSandboxID, &item.targetHandle,
			&item.jobID, &item.kind, &item.partition, &item.dedupe); err != nil {
			_ = rows.Close()
			return nil, err
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	requests := make([]queue.EnqueueRequest, 0, len(pending))
	for _, item := range pending {
		blocked, err := sandboxReleaseBlockedTx(ctx, tx, workspaceID, sessionID, item.logicalSandboxID, item.operationID, item.targetHandle)
		if err != nil {
			return nil, err
		}
		if blocked {
			continue
		}
		if item.jobID.Valid {
			if _, staged := stagedJobIDs[item.jobID.String]; staged {
				continue
			}
			var status string
			err := tx.QueryRow(ctx,
				`SELECT status FROM queue_jobs WHERE workspace_id=$1 AND id=$2`, workspaceID, item.jobID.String,
			).Scan(&status)
			if err != nil && !dbconnect.IsNoRows(err) {
				return nil, err
			}
			if err == nil && (status == queue.StatusPending || status == queue.StatusLeased) {
				continue
			}
		}
		jobID := queue.NewJobID()
		partitionKey := queue.FormatSandboxLifecyclePartitionKey(workspace.ID(workspaceID), item.logicalSandboxID)
		dedupeKey := queue.FormatSandboxLifecycleDedupeKey(queue.KindSandboxRelease, workspace.ID(workspaceID), item.logicalSandboxID, item.operationID)
		payload, err := json.Marshal(map[string]string{
			"workspace_id": workspaceID, "session_id": sessionID,
			"logical_sandbox_id": item.logicalSandboxID, "operation_id": item.operationID,
		})
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET queue_job_id=$3, queue_kind=$4, queue_partition_key=$5, queue_dedupe_key=$6,
			        lease_owner=NULL, lease_token=NULL, lease_expires_at=NULL, attempt_count=0, updated_at=$7
			  WHERE workspace_id=$1 AND operation_id=$2 AND state='pending'`,
			workspaceID, item.operationID, jobID, queue.KindSandboxRelease, partitionKey, dedupeKey, now.UTC(),
		); err != nil {
			return nil, err
		}
		requests = append(requests, queue.EnqueueRequest{
			ID: jobID, WorkspaceID: workspace.ID(workspaceID), Kind: queue.KindSandboxRelease,
			PartitionKey: partitionKey, DedupeKey: dedupeKey, PayloadVersion: 1,
			PayloadJSON: payload, MaxAttempts: sandboxReleaseMaxAttempts, Now: now.UTC(),
		})
	}
	return requests, nil
}

func flushSandboxOutputsAndReadyReleasesOnSuccess(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	now time.Time,
	enqueueRequests *[]queue.EnqueueRequest,
	cancelRequests *[]queue.TargetedCancelRequest,
	txErr *error,
) {
	if txErr == nil || *txErr != nil {
		return
	}
	stagedJobIDs := make(map[string]struct{}, len(*enqueueRequests))
	for _, request := range *enqueueRequests {
		if request.ID != "" {
			stagedJobIDs[request.ID] = struct{}{}
		}
	}
	releaseRequests, err := readySandboxReleaseRequestsTx(ctx, tx, workspaceID, sessionID, now, stagedJobIDs)
	if err != nil {
		*txErr = err
		return
	}
	all := append([]queue.EnqueueRequest(nil), (*enqueueRequests)...)
	all = append(all, releaseRequests...)
	if _, err := queue.EnqueueBatchTx(ctx, tx, all); err != nil {
		*txErr = err
		return
	}
	sort.Slice(*cancelRequests, func(left, right int) bool {
		return (*cancelRequests)[left].JobID < (*cancelRequests)[right].JobID
	})
	for _, request := range *cancelRequests {
		if _, err := queue.CancelTx(ctx, tx, request); err != nil {
			*txErr = err
			return
		}
	}
}

// A transaction that removes a release blocker must enqueue newly ready work
// before it commits; a later maintenance pass is not a substitute for custody.
func wakeReadySandboxReleasesOnSuccess(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, now time.Time, txErr *error) {
	if txErr == nil || *txErr != nil {
		return
	}
	*txErr = enqueueReadySandboxReleasesTx(ctx, tx, workspaceID, sessionID, now)
}
