package tetralsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

const (
	DefaultEnvironmentBuildWarnAfter = 10 * time.Minute
	DefaultEnvironmentBuildTimeout   = 30 * time.Minute
	environmentBuildQueryTimeout     = 45 * time.Second
)

type EnvironmentBuildStore interface {
	ClaimEnvironmentBuild(context.Context, EnvironmentBuildJob, time.Time) (EnvironmentArtifactBuildInput, bool, error)
	AuthorizeEnvironmentArtifactCreate(context.Context, EnvironmentBuildJob, time.Time) (bool, error)
	MarkEnvironmentBuildReady(context.Context, EnvironmentBuildJob, string, time.Time) error
	MarkEnvironmentBuildWaiting(context.Context, EnvironmentBuildJob, sandbox.BuildArtifactResult, time.Time) error
	MarkEnvironmentBuildRetryableFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, bool, time.Time) error
	MarkEnvironmentBuildTerminalFailure(context.Context, EnvironmentBuildJob, EnvironmentArtifactFailure, time.Time) error
}

type EnvironmentBuildJobRunner struct {
	Queue     SandboxQueueClient
	Store     EnvironmentBuildStore
	Providers *ProviderRegistry
	Config    EnvironmentRunnerConfig
	Clock     func() time.Time
	Logger    *slog.Logger
}

type EnvironmentRunnerConfig struct {
	WorkspaceID       string
	LeaseOwner        string
	MaxJobs           int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	BuildWarnAfter    time.Duration
	BuildTimeout      time.Duration
}

type EnvironmentBuildJob struct {
	JobID          string
	LeaseToken     string
	AttemptCount   int
	WorkspaceID    string
	EnvironmentID  string
	Generation     int64
	BuildWarnAfter time.Duration
	BuildTimeout   time.Duration
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
	jobIdentity := SandboxLifecycleJob{JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId()}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindEnvironmentBuild, writer)
		}
	}()
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
	jobIdentity.JobID = job.JobID
	jobIdentity.WorkspaceID = job.WorkspaceID
	job.BuildWarnAfter, job.BuildTimeout = cfg.BuildWarnAfter, cfg.BuildTimeout
	now := r.now()
	input, claimed, err := r.Store.ClaimEnvironmentBuild(ctx, job, now)
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("environment_build_claim", err)
		}
		// Retain the notification until business state can be reconciled. A
		// Retry at the last attempt could dead-letter while leaving waiters
		// pending forever; reclaim re-enters the fenced exhaustion finalizer.
		return err
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
	if input.WarningDue {
		r.logEnvironmentBuild(job, input, "waiting_overdue", sandbox.BuildArtifactResult{})
	}
	if !input.DeadlineAt.IsZero() && !r.now().Before(input.DeadlineAt) {
		return r.timeoutEnvironmentBuild(ctx, job, input)
	}
	builder, ok := r.Providers.ResolveEnvironmentArtifacts(input.Provider)
	if !ok {
		return r.failEnvironmentBuild(ctx, job, EnvironmentArtifactFailure{
			Stage: "build_artifact", LastErrorKind: "provider_configuration_invalid", Reason: "environment artifact provider is unavailable",
		})
	}
	// Bound one external call, not the asynchronous installation. The original
	// artifact deadline also bounds in-flight calls; settlement uses the still
	// live parent Queue lease after this child context is canceled.
	queryTimeout := environmentBuildQueryTimeout
	if !input.DeadlineAt.IsZero() && input.DeadlineAt.Sub(r.now()) < queryTimeout {
		queryTimeout = input.DeadlineAt.Sub(r.now())
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, queryTimeout)
	defer cancelQuery()
	outcome, controlErr := builder.BuildEnvironmentArtifact(queryCtx, sandbox.BuildArtifactRequest{
		WorkspaceID:        input.WorkspaceID,
		EnvironmentID:      input.EnvironmentID,
		Generation:         input.Generation,
		ArtifactInputHash:  input.ArtifactInputHash,
		NormalizedPackages: input.NormalizedPackages,
		AuthorizeProviderCreate: func(ctx context.Context) (bool, error) {
			authorized, err := r.Store.AuthorizeEnvironmentArtifactCreate(ctx, job, r.now())
			if err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					err = queueAuthorityLostBy("environment_build_authorize_create", err)
				}
				return false, &environmentArtifactControlError{err: err}
			}
			return authorized, err
		},
	})
	if err := queueLeaseGuardError(ctx); err != nil {
		return err
	}
	if controlErr != nil {
		return controlErr
	}
	if !input.DeadlineAt.IsZero() && !r.now().Before(input.DeadlineAt) {
		return r.timeoutEnvironmentBuild(ctx, job, input)
	}
	if outcome.Failed() {
		r.logEnvironmentBuild(job, input, "observation_failed", sandbox.BuildArtifactResult{})
		return r.handleBuildFailure(ctx, job, outcome)
	}
	switch outcome.Value.State {
	case sandbox.ArtifactBuildWaiting:
		if err := r.Store.MarkEnvironmentBuildWaiting(ctx, job, outcome.Value, r.now()); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("environment_build_waiting", err)
			}
			return err
		}
		phase := "waiting"
		if outcome.Value.Submitted {
			phase = "submitted"
		}
		r.logEnvironmentBuild(job, input, phase, outcome.Value)
		return r.deferEnvironmentBuild(ctx, job)
	case sandbox.ArtifactBuildFailed:
		r.logEnvironmentBuild(job, input, "provider_failed", outcome.Value)
		return r.failEnvironmentBuild(ctx, job, EnvironmentArtifactFailure{
			Stage: "build_artifact", LastErrorKind: "environment_provider_build_failed", Reason: "daytona snapshot build failed",
			ProviderBuildRef: outcome.Value.ProviderBuildRef, ProviderState: outcome.Value.ProviderState,
		})
	case sandbox.ArtifactBuildReady:
		// Ready must still cross the live lease fence before waking dependents.
	default:
		return r.failEnvironmentBuild(ctx, job, EnvironmentArtifactFailure{
			Stage: "build_artifact", LastErrorKind: "provider_response_malformed", Reason: "environment build returned an invalid state",
		})
	}
	if err := r.Store.MarkEnvironmentBuildReady(ctx, job, outcome.Value.ProviderArtifactRef, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("environment_build_mark_ready", err)
		}
		return err
	}
	r.logEnvironmentBuild(job, input, "ready", outcome.Value)
	if heartbeatErr := stopQueueLeaseGuard(ctx); heartbeatErr != nil {
		return heartbeatErr
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
	if errors.Is(err, errQueueLeaseLost) {
		return queueAuthorityLostBy("environment_build_claim", err)
	}
	if err != nil || !claimed {
		return err
	}
	err = r.Store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, now)
	if errors.Is(err, errQueueLeaseLost) {
		return queueAuthorityLostBy("environment_build_terminal_failure", err)
	}
	return err
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

func (r *EnvironmentBuildJobRunner) handleBuildFailure(ctx context.Context, job EnvironmentBuildJob, outcome ProviderOutcome[sandbox.BuildArtifactResult]) error {
	failure := EnvironmentArtifactFailure{
		Stage: "observe_artifact", LastErrorKind: valueOrDefault(outcome.ErrorKind, "environment_build_observation_failed"),
		Reason: valueOrDefault(outcome.SafeMessage, "environment build observation failed"), Retryable: outcome.Disposition == ProviderRetryable,
	}
	if outcome.Disposition != ProviderRetryable {
		return r.failEnvironmentBuild(ctx, job, failure)
	}
	// A transient observation/submission error cannot prove installation failed.
	// Its safe diagnostic persists; the artifact deadline, not poll count,
	// bounds observation. Only proven rejection rearms provider submission.
	if err := r.Store.MarkEnvironmentBuildRetryableFailure(ctx, job, failure, outcome.EffectBoundary == ProviderProvedNotStarted, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("environment_build_retryable_failure", err)
		}
		return err
	}
	return r.deferEnvironmentBuild(ctx, job)
}

func (r *EnvironmentBuildJobRunner) deferEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob) error {
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Defer(ctx, &queuev1.DeferRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
	}))
}

func (r *EnvironmentBuildJobRunner) timeoutEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob, input EnvironmentArtifactBuildInput) error {
	r.logEnvironmentBuild(job, input, "timed_out", sandbox.BuildArtifactResult{})
	return r.failEnvironmentBuild(ctx, job, EnvironmentArtifactFailure{
		Stage: "observe_artifact", LastErrorKind: "environment_build_wait_timeout",
		Reason: "engine timed out waiting for the environment build",
	})
}

func (r *EnvironmentBuildJobRunner) failEnvironmentBuild(ctx context.Context, job EnvironmentBuildJob, failure EnvironmentArtifactFailure) error {
	if err := r.Store.MarkEnvironmentBuildTerminalFailure(ctx, job, failure, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("environment_build_terminal_failure", err)
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken,
		ErrorKind: failure.LastErrorKind, ErrorMessage: failure.Reason,
	}))
}

func (r *EnvironmentBuildJobRunner) logEnvironmentBuild(job EnvironmentBuildJob, input EnvironmentArtifactBuildInput, phase string, result sandbox.BuildArtifactResult) {
	if r.Logger == nil {
		return
	}
	level := slog.LevelInfo
	if phase == "waiting_overdue" || phase == "observation_failed" || phase == "provider_failed" || phase == "timed_out" {
		level = slog.LevelWarn
	}
	r.Logger.Log(context.Background(), level, "sandbox.environment_build.observed",
		slog.String("operation", "sandbox.environment_build"), slog.String("outcome", phase),
		slog.String("queue.job.id", job.JobID), slog.String("workspace.id", job.WorkspaceID),
		slog.String("environment.id", job.EnvironmentID), slog.Int64("environment.generation", job.Generation),
		slog.String("provider.build.ref", valueOrDefault(result.ProviderBuildRef, input.ProviderBuildRef)), slog.String("provider.build.state", valueOrDefault(result.ProviderState, input.ProviderState)),
		slog.Time("build.started_at", input.StartedAt), slog.Time("build.deadline_at", input.DeadlineAt))
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
	if cfg.BuildWarnAfter <= 0 {
		cfg.BuildWarnAfter = DefaultEnvironmentBuildWarnAfter
	}
	if cfg.BuildTimeout <= 0 {
		cfg.BuildTimeout = DefaultEnvironmentBuildTimeout
	}
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
