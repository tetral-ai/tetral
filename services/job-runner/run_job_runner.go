package jobrunner

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/pollbackoff"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
)

func RunJobRunnerLoop(ctx context.Context, runner *JobRunner, logger *slog.Logger, wake *queue.WakeSignal) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runner == nil {
		return nil
	}
	owned := *runner
	owned.AcquisitionContext = ctx
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	done := make(chan error, 1)
	go func() {
		done <- runJobRunnerLoop(workCtx, &owned, logger, wake, func(_ context.Context, d time.Duration, s queue.WakeSnapshot) error { return wake.Wait(ctx, d, s) })
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

func runJobRunnerLoop(
	ctx context.Context,
	runner *JobRunner,
	logger *slog.Logger,
	wake *queue.WakeSignal,
	wait func(context.Context, time.Duration, queue.WakeSnapshot) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runner == nil {
		return nil
	}
	if runner.Logger == nil {
		runner.Logger = logger
	}
	interval := runner.Config.PollInterval
	if interval <= 0 {
		interval = defaultJobRunnerPollInterval
	}
	backoff := pollbackoff.New(interval, 30*interval)
	for {
		wakeSnapshot := wake.Snapshot()
		hadWork, err := runner.RunOnceWithActivity(ctx)
		if err != nil && ctx.Err() == nil && logger != nil {
			logger.Warn("job_runner.poll_failed",
				slog.String("operation", "job_runner.poll"),
				slog.String("event.kind", "poll_failed"),
				slog.String("component", ServiceNameJobRunner),
				slog.Bool("retryable", true),
				slog.Bool("terminal", false),
				slog.String("error.class", "job_runner_error"),
				slog.String("error.code", "poll_failed"),
				slog.String("error.message_safe", "job runner poll failed"),
			)
		}
		delay := backoff.Next(hadWork)
		waitErr := wait(ctx, delay, wakeSnapshot)
		if waitErr != nil {
			if ctx.Err() != nil || (runner.AcquisitionContext != nil && runner.AcquisitionContext.Err() != nil) {
				return nil
			}
			return waitErr
		}
	}
}
