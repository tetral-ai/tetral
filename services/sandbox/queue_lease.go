package tetralsandbox

import (
	"context"
	"errors"
	"sync"
	"time"

	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

var errQueueLeaseLost = errors.New("sandbox queue lease lost")

func wireRoundedQueueLeaseDuration(duration time.Duration) time.Duration {
	return time.Duration(duration.Milliseconds()) * time.Millisecond
}

func queueHeartbeatDeadline(now time.Time, localExpiry time.Time, heartbeatInterval time.Duration) (time.Time, bool) {
	remaining := localExpiry.Sub(now)
	if remaining <= 0 {
		return time.Time{}, false
	}
	callBudget := heartbeatInterval
	if halfRemaining := remaining / 2; halfRemaining < callBudget {
		callBudget = halfRemaining
	}
	if callBudget <= 0 {
		return time.Time{}, false
	}
	return now.Add(callBudget), true
}

type sandboxQueueLeaseGuard struct {
	cancelHeartbeat context.CancelFunc
	cancelWork      context.CancelFunc
	done            chan struct{}
	authority       *sandboxQueueAuthority

	mu                 sync.Mutex
	err                error
	localExpiry        time.Time
	transitionTimer    *time.Timer
	transitionFinished bool
	once               sync.Once
}

type sandboxQueueLeaseGuardContextKey struct{}

func startQueueLeaseGuard(
	ctx context.Context,
	client SandboxQueueClient,
	queueJob *queuev1.QueueJob,
	localExpiry time.Time,
	heartbeatInterval time.Duration,
	leaseDuration time.Duration,
) (context.Context, func() error, error) {
	if client == nil || queueJob == nil || queueJob.GetWorkspaceId() == "" || queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" {
		return nil, nil, errors.New("sandbox queue lease identity is required")
	}
	if heartbeatInterval <= 0 || leaseDuration <= 0 || localExpiry.IsZero() {
		return nil, nil, errors.New("sandbox queue lease timing is required")
	}
	if time.Until(localExpiry) <= heartbeatInterval {
		return nil, nil, errQueueLeaseLost
	}
	authority, err := decodeSandboxQueueAuthority(queueJob)
	if err != nil {
		return nil, nil, err
	}

	workCtx, cancelWork := context.WithCancel(ctx)
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	guard := &sandboxQueueLeaseGuard{
		cancelHeartbeat: cancelHeartbeat,
		cancelWork:      cancelWork,
		done:            make(chan struct{}),
		authority:       authority,
		localExpiry:     localExpiry,
	}
	go guard.run(heartbeatCtx, client, queueJob, localExpiry, heartbeatInterval, leaseDuration)
	workCtx = context.WithValue(workCtx, sandboxQueueLeaseGuardContextKey{}, guard)
	workCtx = withSandboxQueueAuthority(workCtx, authority)
	return workCtx, guard.finish, nil
}

func (g *sandboxQueueLeaseGuard) run(
	ctx context.Context,
	client SandboxQueueClient,
	queueJob *queuev1.QueueJob,
	localExpiry time.Time,
	heartbeatInterval time.Duration,
	leaseDuration time.Duration,
) {
	defer close(g.done)
	for {
		remaining := time.Until(localExpiry)
		if remaining <= 0 {
			g.loseAuthority()
			return
		}
		delay := heartbeatInterval
		if half := remaining / 2; half < delay {
			delay = half
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}

		heartbeatStartedAt := time.Now()
		deadline, ok := queueHeartbeatDeadline(heartbeatStartedAt, localExpiry, heartbeatInterval)
		if !ok {
			g.loseAuthority()
			return
		}
		heartbeatCtx, cancel := context.WithDeadline(ctx, deadline)
		sentAt := heartbeatStartedAt
		response, err := client.Heartbeat(heartbeatCtx, &queuev1.HeartbeatRequest{
			WorkspaceId:     queueJob.GetWorkspaceId(),
			JobId:           queueJob.GetId(),
			LeaseToken:      queueJob.GetLeaseToken(),
			LeaseDurationMs: leaseDuration.Milliseconds(),
		})
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil || response == nil || !response.GetUpdated() {
			g.loseAuthority()
			return
		}
		databaseExpiry, err := time.Parse(time.RFC3339Nano, response.GetLeasedUntil())
		if err != nil {
			g.loseAuthority()
			return
		}
		localExpiry = sentAt.Add(time.Duration(leaseDuration.Milliseconds()) * time.Millisecond)
		g.mu.Lock()
		g.localExpiry = localExpiry
		g.mu.Unlock()
		g.authority.updateLeasedUntil(databaseExpiry)
	}
}

func (g *sandboxQueueLeaseGuard) loseAuthority() {
	g.mu.Lock()
	g.err = errQueueLeaseLost
	g.mu.Unlock()
	g.cancelWork()
	g.cancelHeartbeat()
}

func (g *sandboxQueueLeaseGuard) stopHeartbeat() error {
	g.once.Do(func() {
		g.cancelHeartbeat()
		<-g.done
		g.armTransitionExpiry()
	})
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}

func (g *sandboxQueueLeaseGuard) armTransitionExpiry() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil || g.transitionFinished {
		return
	}
	remaining := time.Until(g.localExpiry)
	if remaining <= 0 {
		g.err = errQueueLeaseLost
		g.cancelWork()
		return
	}
	g.transitionTimer = time.AfterFunc(remaining, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.transitionFinished {
			return
		}
		g.err = errQueueLeaseLost
		g.cancelWork()
	})
}

func (g *sandboxQueueLeaseGuard) finish() error {
	_ = g.stopHeartbeat()
	g.mu.Lock()
	g.transitionFinished = true
	if g.transitionTimer != nil {
		g.transitionTimer.Stop()
	}
	err := g.err
	g.mu.Unlock()
	g.cancelWork()
	return err
}

func stopQueueLeaseGuard(ctx context.Context) error {
	guard, _ := ctx.Value(sandboxQueueLeaseGuardContextKey{}).(*sandboxQueueLeaseGuard)
	if guard == nil {
		return nil
	}
	return guard.stopHeartbeat()
}
