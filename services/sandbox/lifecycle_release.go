package tetralsandbox

import (
	"context"
	"sort"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
)

const (
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
	return sandboxrelease.ReadyRequestsTx(ctx, tx, workspaceID, sessionID, now, stagedJobIDs)
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
