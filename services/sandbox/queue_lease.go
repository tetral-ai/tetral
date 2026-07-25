package tetralsandbox

import (
	"context"
	"errors"
	"time"

	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

var errQueueLeaseLost = errors.New("sandbox queue lease lost")

func startQueueLeaseGuard(
	ctx context.Context,
	client SessionPrepareQueueClient,
	workspaceID string,
	jobID string,
	leaseToken string,
	heartbeatInterval time.Duration,
	leaseDuration time.Duration,
) (context.Context, func() error) {
	workCtx, cancelWork := context.WithCancel(ctx)
	if heartbeatInterval <= 0 || leaseDuration <= 0 {
		return workCtx, func() error {
			cancelWork()
			return nil
		}
	}
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	done := make(chan struct{})
	var heartbeatErr error
	go func() {
		defer close(done)
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				response, err := client.Heartbeat(heartbeatCtx, &queuev1.HeartbeatRequest{
					WorkspaceId:     workspaceID,
					JobId:           jobID,
					LeaseToken:      leaseToken,
					LeaseDurationMs: leaseDuration.Milliseconds(),
				})
				if heartbeatCtx.Err() != nil {
					return
				}
				if err != nil || !response.GetUpdated() {
					heartbeatErr = errQueueLeaseLost
					cancelWork()
					cancelHeartbeat()
					return
				}
			}
		}
	}()
	return workCtx, func() error {
		cancelHeartbeat()
		<-done
		cancelWork()
		return heartbeatErr
	}
}
