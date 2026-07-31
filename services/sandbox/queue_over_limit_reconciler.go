package tetralsandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
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
	case queue.KindSandboxActivate, queue.KindSandboxMaterialize:
		finalizeBusiness = func(ctx context.Context, tx *dbconnect.Tx) error {
			job, found, err := lookupSandboxLifecycleJobByQueueIdentity(
				ctx, tx, candidate.WorkspaceID.String(), candidate.JobID, candidate.Kind,
				candidate.PartitionKey, candidate.DedupeKey,
			)
			if err != nil || !found {
				return err
			}
			return finalizeExhaustedSandboxLifecycleTx(ctx, tx, job, candidate.Kind, "", "", now)
		}
		if candidate.Kind == queue.KindSandboxActivate {
			errorKind = "sandbox_activation_attempts_exhausted"
			errorMessage = "sandbox activation attempt budget exhausted"
		} else {
			errorKind = "sandbox_materialization_attempts_exhausted"
			errorMessage = "sandbox materialization attempt budget exhausted"
		}
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
