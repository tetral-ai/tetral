package agentruntimebridge

import (
	"context"
	"log/slog"
	"time"

	"github.com/tetral-ai/tetral/internal/pollbackoff"
)

func RunJobRunnerLoop(ctx context.Context, runner *JobRunner, logger *slog.Logger) error {
	return runJobRunnerLoop(ctx, runner, logger, waitForJobRunnerPoll)
}

func runJobRunnerLoop(
	ctx context.Context,
	runner *JobRunner,
	logger *slog.Logger,
	wait func(context.Context, time.Duration) error,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runner == nil {
		return nil
	}
	interval := runner.Config.PollInterval
	if interval <= 0 {
		interval = defaultJobRunnerPollInterval
	}
	if wait == nil {
		wait = waitForJobRunnerPoll
	}
	backoff := pollbackoff.New(interval, 30*interval)
	for {
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
		if err := wait(ctx, backoff.Next(hadWork)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func waitForJobRunnerPoll(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
