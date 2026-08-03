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
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type EnvironmentBuildStore interface {
	ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error)
	AuthorizeEnvironmentArtifactCreate(context.Context, EnvironmentBuildJob, time.Time) (bool, error)
	MarkEnvironmentBuildReady(context.Context, EnvironmentBuildJob, string, time.Time) error
	MarkEnvironmentBuildRetryableFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, bool, time.Time) error
	MarkEnvironmentBuildTerminalFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, time.Time) error
}

type EnvironmentBuildJobRunner struct {
	Queue     SandboxQueueClient
	Store     EnvironmentBuildStore
	Providers *ProviderRegistry
	Config    EnvironmentRunnerConfig
	Clock     func() time.Time
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
	AttemptCount  int
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
	if r.Providers == nil {
		return false, errors.New("sandbox provider registry is required")
	}
	cfg := normalizedEnvironmentRunnerConfig(r.Config)
	leaseSentAt := time.Now()
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
		if err := r.processJob(ctx, queueJob, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return hadWork, err
		}
	}
	return hadWork, nil
}

func (r *EnvironmentBuildJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg EnvironmentRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, stopHeartbeat, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if heartbeatErr := stopHeartbeat(); resultErr == nil && heartbeatErr != nil {
			resultErr = heartbeatErr
		}
	}()
	ctx = workCtx
	if queueJob.GetMaxAttempts() <= 0 || queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if transportJob, identityErr := decodeEnvironmentBuildTransportIdentity(queueJob); identityErr == nil {
			failure := EnvironmentArtifactFailure{Stage: "build_artifact", LastErrorKind: "environment_build_attempts_exhausted", Reason: "environment build attempt budget exhausted"}
			if queueJob.GetMaxAttempts() <= 0 {
				failure.LastErrorKind = "sandbox_queue_integrity_error"
				failure.Reason = "environment build job has no attempt budget"
			}
			if err := r.finalizePrecheckedEnvironmentBuild(ctx, transportJob, failure); err != nil {
				return err
			}
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		errorKind := "environment_build_attempts_exhausted"
		errorMessage := "environment build attempt budget exhausted"
		if queueJob.GetMaxAttempts() <= 0 {
			errorKind = "sandbox_queue_integrity_error"
			errorMessage = "environment build job has no attempt budget"
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: errorKind, ErrorMessage: errorMessage,
		}))
	}
	job, err := DecodeEnvironmentBuildJob(queueJob)
	if err != nil {
		if transportJob, identityErr := decodeEnvironmentBuildTransportIdentity(queueJob); identityErr == nil {
			if err := r.finalizePrecheckedEnvironmentBuild(ctx, transportJob, EnvironmentArtifactFailure{
				Stage: "build_artifact", LastErrorKind: "invalid_environment_build_payload", Reason: "environment_build queue payload is invalid",
			}); err != nil {
				return err
			}
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
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
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		return r.retryEnvironmentBuild(ctx, job, cfg, "environment_build_store_error", "environment build store claim failed")
	}
	if !claimed {
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
			WorkspaceId: job.WorkspaceID,
			JobId:       job.JobID,
			LeaseToken:  job.LeaseToken,
		}))
	}
	builder, ok := r.Providers.ResolveEnvironmentArtifacts(input.Provider)
	if !ok {
		if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
			return heartbeatErr
		}
		return r.retryEnvironmentBuild(ctx, job, cfg, "provider_configuration_invalid", "environment artifact provider is unavailable")
	}
	outcome := builder.BuildEnvironmentArtifact(ctx, sandbox.BuildArtifactRequest{
		WorkspaceID:        input.WorkspaceID,
		EnvironmentID:      input.EnvironmentID,
		Generation:         input.Generation,
		ArtifactInputHash:  input.ArtifactInputHash,
		NormalizedPackages: input.NormalizedPackages,
		AuthorizeProviderCreate: func(ctx context.Context) (bool, error) {
			return r.Store.AuthorizeEnvironmentArtifactCreate(ctx, job, r.now())
		},
	})
	if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
		return heartbeatErr
	}
	if outcome.Failed() {
		return r.handleBuildFailure(ctx, queueJob, job, cfg, outcome)
	}
	if err := r.Store.MarkEnvironmentBuildReady(ctx, job, outcome.Value.ProviderArtifactRef, r.now()); err != nil {
		return r.retryEnvironmentBuild(ctx, job, cfg, "environment_build_store_error", "environment build ready commit failed")
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
}

func (r *EnvironmentBuildJobRunner) finalizePrecheckedEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob, failure EnvironmentArtifactFailure) error {
	now := r.now()
	_, claimed, err := r.Store.ClaimEnvironmentBuild(ctx, job, now)
	if err != nil || !claimed {
		return err
	}
	return r.Store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, now)
}

func decodeEnvironmentBuildTransportIdentity(queueJob *queuev1.QueueJob) (EnvironmentBuildJob, error) {
	if queueJob == nil || queueJob.GetKind() != queue.KindEnvironmentBuild ||
		queueJob.GetWorkspaceId() == "" || queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || queueJob.GetAttemptCount() <= 0 {
		return EnvironmentBuildJob{}, errors.New("environment_build Queue identity is incomplete")
	}
	ws := workspace.ID(queueJob.GetWorkspaceId())
	partitionPrefix := strings.TrimSuffix(queue.FormatEnvironmentPartitionKey(ws, "identity"), "identity")
	if !strings.HasPrefix(queueJob.GetPartitionKey(), partitionPrefix) {
		return EnvironmentBuildJob{}, errors.New("environment_build partition identity is invalid")
	}
	environmentID := strings.TrimPrefix(queueJob.GetPartitionKey(), partitionPrefix)
	if environmentID == "" {
		return EnvironmentBuildJob{}, errors.New("environment_build environment identity is missing")
	}
	dedupePrefix := strings.TrimSuffix(queue.FormatEnvironmentBuildDedupeKey(ws, environmentID, "identity"), "identity")
	if !strings.HasPrefix(queueJob.GetDedupeKey(), dedupePrefix) {
		return EnvironmentBuildJob{}, errors.New("environment_build dedupe identity is invalid")
	}
	generationText := strings.TrimPrefix(queueJob.GetDedupeKey(), dedupePrefix)
	generation, err := strconv.ParseInt(generationText, 10, 64)
	if err != nil || generation <= 0 || queueJob.GetDedupeKey() != queue.FormatEnvironmentBuildDedupeKey(ws, environmentID, generationText) {
		return EnvironmentBuildJob{}, errors.New("environment_build generation identity is invalid")
	}
	return EnvironmentBuildJob{
		JobID: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(), AttemptCount: int(queueJob.GetAttemptCount()),
		WorkspaceID: queueJob.GetWorkspaceId(), EnvironmentID: environmentID, Generation: generation,
	}, nil
}

func (r *EnvironmentBuildJobRunner) handleBuildFailure(ctx context.Context, queueJob *queuev1.QueueJob, job EnvironmentBuildJob, cfg EnvironmentRunnerConfig, outcome ProviderOutcome[sandbox.BuildArtifactResult]) error {
	failure := EnvironmentArtifactFailure{
		Stage: "build_artifact", LastErrorKind: valueOrDefault(outcome.ErrorKind, "environment_build_error"),
		Reason: valueOrDefault(outcome.SafeMessage, "environment build failed"), Retryable: outcome.Disposition == ProviderRetryable,
	}
	if outcome.Disposition != ProviderRetryable {
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
		rearmCreate := outcome.EffectBoundary == ProviderProvedNotStarted
		if err := r.Store.MarkEnvironmentBuildRetryableFailure(ctx, job, failure, rearmCreate, r.now()); err != nil {
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
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || queueJob.GetAttemptCount() <= 0 {
		return EnvironmentBuildJob{}, errors.New("environment_build queue identity is incomplete")
	}
	return EnvironmentBuildJob{
		JobID:         queueJob.GetId(),
		LeaseToken:    queueJob.GetLeaseToken(),
		AttemptCount:  int(queueJob.GetAttemptCount()),
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
