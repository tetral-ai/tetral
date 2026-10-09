package jobrunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
)

// RunJobRunnerLoop runs the acquisition coordinator and, when configured, the
// independent Pod-loss repair owner until ctx ends. Cancelling ctx closes
// acquisition and stops repair; active jobs then drain within
// Config.DrainTimeout before their work context is cancelled, and both owners
// are joined before this returns.
func RunJobRunnerLoop(ctx context.Context, runner *JobRunner, logger *slog.Logger, wake *queue.WakeSignal) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runner == nil {
		return nil
	}
	owned := *runner
	owned.AcquisitionContext = ctx
	owned.wake = wake
	owned.slots = nil
	if owned.Logger == nil {
		owned.Logger = logger
	}
	repairDone := make(chan struct{})
	if owned.Repair != nil {
		go func() {
			defer close(repairDone)
			owned.Repair.Run(ctx)
		}()
	} else {
		close(repairDone)
	}
	defer func() { <-repairDone }()
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	done := make(chan error, 1)
	go func() {
		done <- runJobRunnerLoop(workCtx, &owned, logger)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	workload.BeginProcessShutdown(ctx)
	drain := owned.Config.DrainTimeout
	if drain <= 0 {
		drain = 30 * time.Second
	}
	join := owned.Config.CancelJoinTimeout
	if join <= 0 {
		join = 5 * time.Second
	}
	timer := time.NewTimer(drain)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		cancelWork()
	}
	joinTimer := time.NewTimer(join)
	defer joinTimer.Stop()
	select {
	case err := <-done:
		return err
	case <-joinTimer.C:
		// Resource closure must still follow the actual join. A misbehaving owned
		// dependency is a failed termination bound, never permission to close its DB.
		if logger != nil {
			logger.Error("job_runner.shutdown.join_timeout", slog.String("component", ServiceNameJobRunner))
		}
		<-done
		return fmt.Errorf("job runner cancellation join exceeded configured budget")
	}
}

// runJobRunnerLoop acquires work whenever a slot is free and nothing is in
// flight. A response with jobs is followed at once by a request for the
// remaining free slots; an empty response waits for Queue's retry hint, a
// committed-work wake or a slot completion; a failed request waits the capped
// backoff. After acquisition closes it joins every dispatched job.
func runJobRunnerLoop(ctx context.Context, runner *JobRunner, logger *slog.Logger) error {
	cfg := runner.effectiveConfig()
	slots := runner.jobSlots(cfg.MaxJobs)
	failures := 0
	for {
		free, _, _, closed := slots.snapshot()
		if closed {
			break
		}
		if free == 0 {
			// No acquisition while full: only a completion or closure proceeds.
			if !runner.waitForAcquisition(ctx, slots, 0, queue.WakeSnapshot{}) {
				break
			}
			continue
		}
		acquisition, err := runner.AcquireAndDispatch(ctx)
		if errors.Is(err, ErrJobRunnerAcquisitionClosed) {
			break
		}
		delay := acquisition.RetryAfter
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			logJobRunnerAcquisitionFailure(logger)
			delay = jobRunnerAcquisitionBackoff[min(failures, len(jobRunnerAcquisitionBackoff)-1)]
			failures++
		} else {
			failures = 0
			if acquisition.Dispatched > 0 {
				continue
			}
			if delay <= 0 {
				delay = jobRunnerDefaultRetryAfter
			}
		}
		if !runner.waitForAcquisition(ctx, slots, delay, acquisition.wake) {
			break
		}
	}
	// Drain joins every dispatched job even after the drain deadline cancelled
	// its work context; each failed job was already logged by its slot.
	_ = runner.JoinDispatched(context.WithoutCancel(ctx))
	return nil
}

func logJobRunnerAcquisitionFailure(logger *slog.Logger) {
	if logger == nil {
		return
	}
	logger.Warn("job_runner.acquisition_failed",
		slog.String("operation", "job_runner.acquisition"),
		slog.String("event.kind", "acquisition_failed"),
		slog.String("component", ServiceNameJobRunner),
		slog.Bool("retryable", true),
		slog.Bool("terminal", false),
		slog.String("error.class", "job_runner_error"),
		slog.String("error.code", "acquisition_failed"),
		slog.String("error.message_safe", "job runner queue acquisition failed"),
	)
}
