package tetralsandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	SandboxQueueOverLimitInterval  = 30 * time.Second
	SandboxQueueOverLimitBatchSize = 100
)

type SandboxQueueOverLimitReader interface {
	ListPendingAtOrOverBudget(context.Context, queue.ListPendingAtOrOverBudgetRequest) ([]queue.PendingAtOrOverBudgetJob, error)
}

type SandboxQueueOverLimitFinalizer interface {
	FinalizePendingAtOrOverBudget(context.Context, queue.PendingAtOrOverBudgetJob, time.Time) (bool, error)
}

// SandboxQueueOverLimitReconciler closes Sandbox business work whose worker
// died on its last permitted Queue attempt. Provider calls are deliberately
// absent: the durable business state determines the terminal result.
type SandboxQueueOverLimitReconciler struct {
	Queue     SandboxQueueOverLimitReader
	Finalizer SandboxQueueOverLimitFinalizer
	Clock     func() time.Time
}

type PostgreSQLSandboxQueueOverLimitFinalizer struct {
	client *dbconnect.Client
}

func NewPostgreSQLSandboxQueueOverLimitFinalizer(client *dbconnect.Client) *PostgreSQLSandboxQueueOverLimitFinalizer {
	return &PostgreSQLSandboxQueueOverLimitFinalizer{client: client}
}

func (r *SandboxQueueOverLimitReconciler) RunOnce(ctx context.Context) (int, error) {
	if r == nil || r.Queue == nil || r.Finalizer == nil {
		return 0, errors.New("sandbox over-limit reconciler dependencies are required")
	}
	candidates, err := r.Queue.ListPendingAtOrOverBudget(ctx, queue.ListPendingAtOrOverBudgetRequest{Limit: SandboxQueueOverLimitBatchSize})
	if err != nil {
		return 0, err
	}
	now := r.now()
	processed := 0
	for _, candidate := range candidates {
		updated, err := r.Finalizer.FinalizePendingAtOrOverBudget(ctx, candidate, now)
		if err != nil {
			return processed, err
		}
		if updated {
			processed++
		}
	}
	return processed, nil
}

func RunSandboxQueueOverLimitLoop(ctx context.Context, reconciler *SandboxQueueOverLimitReconciler, interval time.Duration) {
	if reconciler == nil {
		return
	}
	if interval <= 0 {
		interval = SandboxQueueOverLimitInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = reconciler.RunOnce(ctx)
		}
	}
}

func (r *SandboxQueueOverLimitReconciler) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

var errSandboxOverLimitCandidateChanged = errors.New("sandbox over-limit candidate changed")

func (f *PostgreSQLSandboxQueueOverLimitFinalizer) FinalizePendingAtOrOverBudget(ctx context.Context, candidate queue.PendingAtOrOverBudgetJob, now time.Time) (bool, error) {
	if f == nil || f.client == nil {
		return false, errors.New("sandbox over-limit finalizer database is required")
	}
	if candidate.WorkspaceID == "" || candidate.JobID == "" || candidate.AttemptCount <= 0 || candidate.MaxAttempts <= 0 || candidate.AttemptCount < candidate.MaxAttempts {
		return false, errors.New("sandbox over-limit candidate is invalid")
	}
	if now.IsZero() {
		now = storage.Now()
	}
	now = now.UTC()

	var finalizeBusiness func(context.Context, *dbconnect.Tx) error
	errorKind := "sandbox_queue_attempts_exhausted"
	errorMessage := "sandbox queue attempt budget exhausted"
	switch candidate.Kind {
	case queue.KindSandboxToolExecute:
		job, err := decodeSandboxExecutionQueueTransportIdentity(
			candidate.JobID, candidate.WorkspaceID.String(), candidate.PartitionKey,
			candidate.DedupeKey,
		)
		if err != nil {
			return false, err
		}
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			return finalizeExhaustedSandboxExecutionTx(ctx, tx, job, now)
		}
		errorKind = "sandbox_execution_attempts_exhausted"
		errorMessage = "sandbox execution attempt budget exhausted"
	case queue.KindSandboxToolCancel:
		job, err := decodeSandboxToolCancelQueueTransportIdentity(&queuev1.QueueJob{
			Id: candidate.JobID, WorkspaceId: candidate.WorkspaceID.String(), Kind: candidate.Kind,
			PartitionKey: candidate.PartitionKey, DedupeKey: candidate.DedupeKey,
			LeaseToken: "over-limit-finalizer",
		})
		if err != nil {
			return false, err
		}
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			return finalizeToolCancellationTx(ctx, tx, job, now)
		}
		errorKind = "sandbox_tool_cancel_attempts_exhausted"
		errorMessage = "sandbox tool cancellation attempt budget exhausted"
	case queue.KindSandboxActivate, queue.KindSandboxMaterialize, queue.KindSandboxRelease:
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			job, found, err := lookupSandboxLifecycleJobByQueueIdentity(
				ctx, tx, candidate.WorkspaceID.String(), candidate.JobID, candidate.Kind,
				candidate.PartitionKey, candidate.DedupeKey,
			)
			if err != nil || !found {
				return err
			}
			_, err = finalizeExhaustedSandboxLifecycleTx(ctx, tx, job, candidate.Kind, "", "", now)
			return err
		}
		switch candidate.Kind {
		case queue.KindSandboxActivate:
			errorKind = "sandbox_activation_attempts_exhausted"
			errorMessage = "sandbox activation attempt budget exhausted"
		case queue.KindSandboxMaterialize:
			errorKind = "sandbox_materialization_attempts_exhausted"
			errorMessage = "sandbox materialization attempt budget exhausted"
		case queue.KindSandboxRelease:
			errorKind = "sandbox_release_attempts_exhausted"
			errorMessage = "sandbox release attempt budget exhausted"
		}
	case queue.KindSandboxBackgroundReconcile, queue.KindSandboxBackgroundCommand:
		identity, err := decodeBackgroundQueueIdentity(
			candidate.WorkspaceID.String(), candidate.Kind, candidate.PartitionKey, candidate.DedupeKey,
		)
		if err != nil {
			return false, err
		}
		switch candidate.Kind {
		case queue.KindSandboxBackgroundReconcile:
			finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
				return finalizeExhaustedBackgroundReconcileTx(ctx, tx, identity, now)
			}
			errorKind = "sandbox_background_reconcile_attempts_exhausted"
			errorMessage = "sandbox background reconcile attempt budget exhausted"
		case queue.KindSandboxBackgroundCommand:
			finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
				return finalizeExhaustedBackgroundCommandTx(ctx, tx, identity, now)
			}
			errorKind = "sandbox_background_command_attempts_exhausted"
			errorMessage = "sandbox background command attempt budget exhausted"
		}
	case queue.KindSandboxMemoryProjection:
		queueJob := &queuev1.QueueJob{
			Id: candidate.JobID, WorkspaceId: candidate.WorkspaceID.String(), Kind: candidate.Kind,
			PartitionKey: candidate.PartitionKey, DedupeKey: candidate.DedupeKey,
		}
		_, memoryWriteID, err := decodeMemoryProjectionTransportIdentity(queueJob)
		if err != nil {
			return false, err
		}
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			return settleMemoryProjectionTx(ctx, tx, memoryWriteID, "failed", "memory projection attempt budget exhausted", now)
		}
		errorKind = "sandbox_memory_projection_attempts_exhausted"
		errorMessage = "sandbox memory projection attempt budget exhausted"
	case queue.KindSandboxOutputCapture:
		queueJob := &queuev1.QueueJob{
			Id: candidate.JobID, WorkspaceId: candidate.WorkspaceID.String(), Kind: candidate.Kind,
			PartitionKey: candidate.PartitionKey, DedupeKey: candidate.DedupeKey,
		}
		identity, err := decodeSandboxOutputCaptureTransportIdentity(queueJob)
		if err != nil {
			return false, err
		}
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			return finalizeOutputCaptureExhaustionTx(ctx, tx, identity, now)
		}
		errorKind = "sandbox_output_capture_attempts_exhausted"
		errorMessage = "sandbox output capture attempt budget exhausted"
	case queue.KindSandboxOutputCaptureCleanup:
		queueJob := &queuev1.QueueJob{
			Id: candidate.JobID, WorkspaceId: candidate.WorkspaceID.String(), Kind: candidate.Kind,
			PartitionKey: candidate.PartitionKey, DedupeKey: candidate.DedupeKey,
		}
		identity, err := decodeSandboxOutputCaptureCleanupTransportIdentity(queueJob)
		if err != nil {
			return false, err
		}
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			return advanceOutputCaptureCleanupTx(ctx, tx, identity, now)
		}
		errorKind = "sandbox_output_capture_cleanup_attempts_exhausted"
		errorMessage = "sandbox output capture cleanup attempt budget exhausted"
	default:
		return false, errors.New("sandbox over-limit job kind is not owned by an installed finalizer")
	}

	updated := false
	err := f.client.WithWorkspaceTx(ctx, candidate.WorkspaceID.String(), "sandbox.queue.finalize_over_limit", func(tx *dbconnect.Tx) error {
		if err := finalizeBusiness(ctx, tx); err != nil {
			return err
		}
		var err error
		updated, err = queue.DeadLetterExhaustedTx(ctx, tx, queue.DeadLetterExhaustedRequest{
			WorkspaceID: candidate.WorkspaceID, JobID: candidate.JobID,
			ObservedAttemptCount: candidate.AttemptCount,
			ErrorKind:            errorKind, ErrorMessage: errorMessage, Now: now,
		})
		if err != nil {
			return err
		}
		if !updated {
			return errSandboxOverLimitCandidateChanged
		}
		return nil
	})
	if errors.Is(err, errSandboxOverLimitCandidateChanged) {
		return false, nil
	}
	return updated, err
}
