package tetralsandbox

import (
	"context"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/pollbackoff"
	"github.com/tetral-ai/tetral/internal/workspace"
)

type WorkspaceLister interface {
	ListIDs(context.Context) ([]workspace.ID, error)
}

type WorkspaceConsumer func(context.Context, workspace.ID) (bool, error)

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

func RunWorkspaceConsumerLoop(ctx context.Context, lister WorkspaceLister, pollInterval time.Duration, consume WorkspaceConsumer) error {
	return runWorkspaceConsumerLoop(ctx, lister, pollInterval, consume, waitForWorkspacePoll)
}

func runWorkspaceConsumerLoop(
	ctx context.Context,
	lister WorkspaceLister,
	pollInterval time.Duration,
	consume WorkspaceConsumer,
	wait func(context.Context, time.Duration) error,
) error {
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	if wait == nil {
		wait = waitForWorkspacePoll
	}
	backoff := pollbackoff.New(pollInterval, 30*pollInterval)
	for {
		hadWork, err := RunWorkspaceConsumerCycle(ctx, lister, consume)
		if waitErr := wait(ctx, backoff.Next(hadWork)); waitErr != nil {
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

func waitForWorkspacePoll(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
