package tetralsandbox

import (
	"context"
	"errors"
	"time"

	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// Acquisition closes immediately on quiesce. Active workers retain their work
// context and heartbeat until the shared drain cutoff, then cancel and join.
type acquisitionContextKey struct{}

func WithAcquisitionContext(work, acquisition context.Context) context.Context {
	return context.WithValue(work, acquisitionContextKey{}, acquisition)
}
func acquisitionContext(work context.Context) context.Context {
	if ctx, ok := work.Value(acquisitionContextKey{}).(context.Context); ok {
		return ctx
	}
	return work
}

type acquisitionQueue struct{ SandboxQueueClient }

// WithQueueAcquisition applies the same admission boundary to every Sandbox
// Queue family; provider and settlement methods retain the caller work context.
func WithQueueAcquisition(client SandboxQueueClient) SandboxQueueClient {
	return acquisitionQueue{client}
}
func (q acquisitionQueue) Lease(work context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	acquire := acquisitionContext(work)
	if err := acquire.Err(); err != nil {
		return &queuev1.LeaseResponse{}, err
	}
	leaseCtx, cancelLease := context.WithCancel(work)
	stopAcquire := context.AfterFunc(acquire, cancelLease)
	defer func() { stopAcquire(); cancelLease() }()
	response, err := q.SandboxQueueClient.Lease(leaseCtx, request)
	if acquire.Err() == nil {
		return response, err
	}
	// The reply may race quiesce after Queue committed. Return only these exact
	// capabilities; never execute the newly observed jobs after admission closes.
	var transitionErrors []error
	for _, job := range response.GetJobs() {
		deferred, deferErr := q.Defer(work, &queuev1.DeferRequest{WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken()})
		if transitionErr := transitionUpdated(deferred, deferErr); transitionErr != nil {
			transitionErrors = append(transitionErrors, transitionErr)
		}
	}
	return &queuev1.LeaseResponse{}, errors.Join(append(transitionErrors, acquire.Err())...)
}
func JoinSandboxWorkers(done <-chan struct{}, cancel context.CancelFunc, drain, join time.Duration) error {
	timer := time.NewTimer(drain)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancel()
	}
	timer.Reset(join)
	select {
	case <-done:
		return nil
	case <-timer.C:
		// A pool cannot be closed while its owner still uses it. Record the failed
		// bound, then finish joining; deployment process termination is the last fuse.
		<-done
		return errors.New("sandbox worker cancellation join exceeded its budget")
	}
}
