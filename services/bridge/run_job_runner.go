package agentruntimebridge

import (
	"context"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/pollbackoff"
	"github.com/tetral-ai/tetral/internal/queue"
)

func RunJobRunnerLoop(ctx context.Context, runner *JobRunner, logger *slog.Logger, wake *queue.WakeSignal) error {
	return runJobRunnerLoop(ctx, runner, logger, wake, wake.Wait)
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
			logger.Warn("bridge.job_runner.poll_failed",
				slog.String("operation", "bridge.job_runner.poll"),
				slog.String("event.kind", "poll_failed"),
				slog.String("component", ServiceNameJobRunner),
				slog.Bool("retryable", true),
				slog.Bool("terminal", false),
				slog.String("error.class", "bridge_job_runner_error"),
				slog.String("error.code", "poll_failed"),
				slog.String("error.message_safe", "job runner poll failed"),
			)
		}
		delay := backoff.Next(hadWork)
		waitErr := wait(ctx, delay, wakeSnapshot)
		if waitErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return waitErr
		}
	}
}
