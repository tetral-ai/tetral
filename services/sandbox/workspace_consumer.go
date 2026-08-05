package tetralsandbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/pollbackoff"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type WorkspaceLister interface {
	ListIDs(context.Context) ([]workspace.ID, error)
}

type WorkspaceConsumer func(context.Context, workspace.ID) (bool, error)

// WorkspaceConsumerPool bounds aggregate work across every Queue consumer that
// shares it. A contender acquires a slot before it can lease a Queue job and
// holds that slot through the job's durable disposition.
type WorkspaceConsumerPool struct {
	slots chan struct{}
}

func NewWorkspaceConsumerPool(size int) (*WorkspaceConsumerPool, error) {
	if size <= 0 {
		return nil, errors.New("sandbox workspace consumer pool size must be positive")
	}
	return &WorkspaceConsumerPool{slots: make(chan struct{}, size)}, nil
}

func (p *WorkspaceConsumerPool) run(ctx context.Context, consume WorkspaceConsumer, workspaceID workspace.ID) (bool, error) {
	if p == nil {
		return false, errors.New("sandbox workspace consumer pool is required")
	}
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
		return consume(ctx, workspaceID)
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func RunWorkspaceConsumerCycle(ctx context.Context, lister WorkspaceLister, consume WorkspaceConsumer) (bool, error) {
	if lister == nil {
		return false, errors.New("sandbox workspace lister is required")
	}
	if consume == nil {
		return false, errors.New("sandbox workspace consumer is required")
	}
	workspaceIDs, err := lister.ListIDs(ctx)
	if err != nil {
		return false, err
	}
	hadWork := false
	for _, workspaceID := range workspaceIDs {
		if workspaceID == "" {
			return hadWork, errors.New("sandbox discovered an empty workspace id")
		}
		workspaceHadWork, err := consume(ctx, workspaceID)
		hadWork = hadWork || workspaceHadWork
		if err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

func RunWorkspaceConsumerLoop(ctx context.Context, lister WorkspaceLister, pollInterval time.Duration, consume WorkspaceConsumer, wake *queue.WakeSignal, logger *slog.Logger) error {
	return runWorkspaceConsumerLoop(ctx, lister, pollInterval, consume, wake, logger, wake.Wait)
}

// RunWorkspaceConsumerGroup starts enough long-lived contenders to expose the
// shared pool's capacity. Contenders blocked on the pool hold no Queue lease.
func RunWorkspaceConsumerGroup(
	ctx context.Context,
	contenders int,
	pool *WorkspaceConsumerPool,
	lister WorkspaceLister,
	pollInterval time.Duration,
	consume WorkspaceConsumer,
	wake *queue.WakeSignal,
	logger *slog.Logger,
) error {
	if contenders <= 0 {
		return errors.New("sandbox workspace contender count must be positive")
	}
	if pool == nil {
		return errors.New("sandbox workspace consumer pool is required")
	}
	if consume == nil {
		return errors.New("sandbox workspace consumer is required")
	}
	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsByContender := make(chan error, contenders)
	var contendersDone sync.WaitGroup
	contendersDone.Add(contenders)
	for range contenders {
		go func() {
			defer contendersDone.Done()
			errorsByContender <- RunWorkspaceConsumerLoop(groupCtx, lister, pollInterval, func(cycleCtx context.Context, workspaceID workspace.ID) (bool, error) {
				return pool.run(cycleCtx, consume, workspaceID)
			}, wake, logger)
		}()
	}
	firstErr := <-errorsByContender
	cancel()
	contendersDone.Wait()
	return firstErr
}

func runWorkspaceConsumerLoop(
	ctx context.Context,
	lister WorkspaceLister,
	pollInterval time.Duration,
	consume WorkspaceConsumer,
	wake *queue.WakeSignal,
	logger *slog.Logger,
	wait func(context.Context, time.Duration, queue.WakeSnapshot) error,
) error {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	backoff := pollbackoff.New(pollInterval, 30*pollInterval)
	for {
		wakeSnapshot := wake.Snapshot()
		hadWork, err := RunWorkspaceConsumerCycle(ctx, lister, consume)
		delay := backoff.Next(hadWork)
		if logger != nil {
			logger.Debug("sandbox.queue.wait",
				slog.String("operation", "sandbox.queue.wait"),
				slog.String("event.kind", "queue_wait"),
				slog.Int64("duration.ms", delay.Milliseconds()),
				slog.Bool("poll.had_work", hadWork),
				slog.Bool("notification.enabled", wake != nil),
			)
		}
		waitErr := wait(ctx, delay, wakeSnapshot)
		if waitErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return waitErr
		}
		if err != nil {
			continue
		}
	}
}
