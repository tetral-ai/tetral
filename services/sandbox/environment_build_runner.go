package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type EnvironmentBuildStore interface {
	ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error)
	MarkEnvironmentBuildReady(context.Context, EnvironmentBuildJob, string, time.Time) error
	MarkEnvironmentBuildRetryableFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, time.Time) error
	MarkEnvironmentBuildTerminalFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, time.Time) error
}

type EnvironmentBuildJobRunner struct {
	Queue   SessionPrepareQueueClient
	Store   EnvironmentBuildStore
	Builder sandbox.ArtifactBuilder
	Config  EnvironmentRunnerConfig
	Clock   func() time.Time
}

type EnvironmentRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
}

type EnvironmentBuildJob struct {
	JobID         string
	LeaseToken    string
	WorkspaceID   string
	EnvironmentID string
	Generation    int64
}

func (r *EnvironmentBuildJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *EnvironmentBuildJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("sandbox environment_build queue client is required")
	}
	if r.Store == nil {
		return false, errors.New("sandbox environment_build store is required")
	}
	if r.Builder == nil {
		return false, errors.New("sandbox environment artifact builder is required")
	}
	cfg := normalizedEnvironmentRunnerConfig(r.Config)
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     cfg.WorkspaceID,
		Kinds:           []string{queue.KindEnvironmentBuild},
		LeaseOwner:      cfg.LeaseOwner,
		MaxJobs:         int32(cfg.MaxJobs),
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	hadWork := len(lease.GetJobs()) > 0
	for _, queueJob := range lease.GetJobs() {
		if err := r.processJob(ctx, queueJob, cfg); err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

func (r *EnvironmentBuildJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg EnvironmentRunnerConfig) error {
	job, err := DecodeEnvironmentBuildJob(queueJob)
	if err != nil {
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_environment_build_payload",
			ErrorMessage: "environment_build queue payload is invalid",
		}))
	}
	now := r.now()
	input, claimed, err := r.Store.ClaimEnvironmentBuild(ctx, job, now)
	if err != nil {
		return r.retryEnvironmentBuild(ctx, job, cfg, "environment_build_store_error", "environment build store claim failed")
	}
	if !claimed {
		return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
			WorkspaceId: job.WorkspaceID,
			JobId:       job.JobID,
			LeaseToken:  job.LeaseToken,
		}))
	}
	workCtx, stopHeartbeat := startQueueLeaseGuard(ctx, r.Queue, job.WorkspaceID, job.JobID, job.LeaseToken, cfg.HeartbeatInterval, cfg.LeaseDuration)
	result, buildErr := r.Builder.BuildArtifact(workCtx, sandbox.BuildArtifactRequest{
		WorkspaceID:        input.WorkspaceID,
		EnvironmentID:      input.EnvironmentID,
		Generation:         input.Generation,
		ArtifactInputHash:  input.ArtifactInputHash,
		NormalizedPackages: input.NormalizedPackages,
	})
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
		return heartbeatErr
	}
	if buildErr != nil {
		return r.handleBuildError(ctx, queueJob, job, cfg, buildErr)
	}
	if err := r.Store.MarkEnvironmentBuildReady(ctx, job, result.ProviderArtifactRef, r.now()); err != nil {
		return r.retryEnvironmentBuild(ctx, job, cfg, "environment_build_store_error", "environment build ready commit failed")
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
}

func (r *EnvironmentBuildJobRunner) handleBuildError(ctx context.Context, queueJob *queuev1.QueueJob, job EnvironmentBuildJob, cfg EnvironmentRunnerConfig, buildErr error) error {
	failure := environmentArtifactFailureForError(buildErr)
	if !retryableEnvironmentBuildError(buildErr) {
		failure.Retryable = false
		if err := r.Store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, r.now()); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    valueOrDefault(failure.LastErrorKind, "environment_build_error"),
			ErrorMessage: "environment build failed",
		}))
	}
	if environmentRetryWillExhaust(queueJob) {
		failure.Retryable = false
		if err := r.Store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, r.now()); err != nil {
			return err
		}
	} else {
		failure.Retryable = true
		if err := r.Store.MarkEnvironmentBuildRetryableFailure(ctx, job, failure, r.now()); err != nil {
			return err
		}
	}
	return r.retryEnvironmentBuild(ctx, job, cfg, valueOrDefault(failure.LastErrorKind, "environment_build_error"), "environment build failed")
}

func (r *EnvironmentBuildJobRunner) retryEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob, cfg EnvironmentRunnerConfig, errorKind string, errorMessage string) error {
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId:  job.WorkspaceID,
		JobId:        job.JobID,
		LeaseToken:   job.LeaseToken,
		ErrorKind:    errorKind,
		ErrorMessage: errorMessage,
	}))
}

func DecodeEnvironmentBuildJob(queueJob *queuev1.QueueJob) (EnvironmentBuildJob, error) {
	if queueJob == nil {
		return EnvironmentBuildJob{}, errors.New("queue job is required")
	}
	if queueJob.GetKind() != queue.KindEnvironmentBuild {
		return EnvironmentBuildJob{}, errors.New("queue job kind is not environment_build")
	}
	workspaceID, environmentID, generation, err := decodeEnvironmentJobPayload(queueJob)
	if err != nil {
		return EnvironmentBuildJob{}, err
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" {
		return EnvironmentBuildJob{}, errors.New("environment_build queue identity is incomplete")
	}
	return EnvironmentBuildJob{
		JobID:         queueJob.GetId(),
		LeaseToken:    queueJob.GetLeaseToken(),
		WorkspaceID:   workspaceID,
		EnvironmentID: environmentID,
		Generation:    generation,
	}, nil
}

func decodeEnvironmentJobPayload(queueJob *queuev1.QueueJob) (string, string, int64, error) {
	var payload struct {
		WorkspaceID   string `json:"workspace_id"`
		EnvironmentID string `json:"environment_id"`
		Generation    string `json:"generation"`
	}
	decoder := json.NewDecoder(strings.NewReader(queueJob.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return "", "", 0, err
	}
	generation, err := strconv.ParseInt(payload.Generation, 10, 64)
	if err != nil || generation <= 0 {
		return "", "", 0, errors.New("environment queue payload generation must be a positive integer string")
	}
	if payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() || payload.EnvironmentID == "" {
		return "", "", 0, errors.New("environment queue payload has missing identity fields")
	}
	return payload.WorkspaceID, payload.EnvironmentID, generation, nil
}

func (r *EnvironmentBuildJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

func normalizedEnvironmentRunnerConfig(cfg EnvironmentRunnerConfig) EnvironmentRunnerConfig {
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceName
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = 1
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = time.Minute
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	return cfg
}

func environmentRetryWillExhaust(queueJob *queuev1.QueueJob) bool {
	return queueJob != nil && queueJob.GetMaxAttempts() > 0 && queueJob.GetAttemptCount() >= queueJob.GetMaxAttempts()
}

func retryableEnvironmentBuildError(err error) bool {
	if err == nil {
		return false
	}
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.Retryable || providerErr.Kind == sandbox.ProviderErrorTimeout || providerErr.Kind == sandbox.ProviderErrorUnavailable
	}
	var validation *sandbox.ValidationError
	if errors.As(err, &validation) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return true
}

func environmentArtifactFailureForError(err error) EnvironmentArtifactFailure {
	failure := EnvironmentArtifactFailure{
		Stage:         "environment_build",
		LastErrorKind: "environment_build_error",
		Reason:        "environment_build_error",
		Retryable:     retryableEnvironmentBuildError(err),
	}
	var providerErr *sandbox.ProviderError
	if errors.As(err, &providerErr) {
		failure.Stage = string(providerErr.Stage)
		failure.LastErrorKind = string(providerErr.Kind)
		failure.Reason = valueOrDefault(providerErr.SafeMessage, string(providerErr.Kind))
		failure.Retryable = providerErr.Retryable
	}
	return failure
}
