package tetralsandbox

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type sandboxQueueAuthority struct {
	mu          sync.RWMutex
	workspaceID string
	jobID       string
	leaseToken  string
	leasedUntil time.Time
}

func (a *sandboxQueueAuthority) updateLeasedUntil(leasedUntil time.Time) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.leasedUntil = leasedUntil.UTC()
	a.mu.Unlock()
}

func (a *sandboxQueueAuthority) currentLeasedUntil() time.Time {
	if a == nil {
		return time.Time{}
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.leasedUntil
}

type sandboxQueueAuthorityContextKey struct{}

func withSandboxQueueAuthority(ctx context.Context, authority *sandboxQueueAuthority) context.Context {
	return context.WithValue(ctx, sandboxQueueAuthorityContextKey{}, authority)
}

func sandboxQueueAuthorityFromContext(ctx context.Context) *sandboxQueueAuthority {
	authority, _ := ctx.Value(sandboxQueueAuthorityContextKey{}).(*sandboxQueueAuthority)
	return authority
}

func decodeSandboxQueueAuthority(job *queuev1.QueueJob) (*sandboxQueueAuthority, error) {
	if job == nil || job.GetWorkspaceId() == "" || job.GetId() == "" || job.GetLeaseToken() == "" || job.GetLeasedUntil() == "" {
		return nil, errors.New("sandbox Queue authority is incomplete")
	}
	leasedUntil, err := time.Parse(time.RFC3339Nano, job.GetLeasedUntil())
	if err != nil {
		return nil, errors.New("sandbox Queue lease expiry is invalid")
	}
	return &sandboxQueueAuthority{
		workspaceID: job.GetWorkspaceId(),
		jobID:       job.GetId(),
		leaseToken:  job.GetLeaseToken(),
		leasedUntil: leasedUntil,
	}, nil
}

// assertSandboxQueueAuthorityTx is the final lock in a live Sandbox job's
// business transaction. Missing custody is authority loss, not an unfenced
// alternate path.
func assertSandboxQueueAuthorityTx(ctx context.Context, tx *dbconnect.Tx) error {
	authority := sandboxQueueAuthorityFromContext(ctx)
	if authority == nil {
		return errQueueLeaseLost
	}
	active, err := queue.AssertActiveLeaseTx(ctx, tx, queue.ActiveLeaseRequest{
		WorkspaceID: workspace.ID(authority.workspaceID),
		JobID:       authority.jobID,
		LeaseToken:  authority.leaseToken,
	})
	if err != nil {
		return err
	}
	if !active {
		return errQueueLeaseLost
	}
	return nil
}

func finishSandboxQueueAuthorityTx(ctx context.Context, tx *dbconnect.Tx, txErr *error) {
	if txErr == nil || *txErr != nil {
		return
	}
	*txErr = assertSandboxQueueAuthorityTx(ctx, tx)
}
