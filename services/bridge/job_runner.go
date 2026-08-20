package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type QueueClient interface {
	Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error)
	Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error)
	Ack(context.Context, *queuev1.AckRequest) (*queuev1.TransitionResponse, error)
	Retry(context.Context, *queuev1.RetryRequest) (*queuev1.TransitionResponse, error)
	Defer(context.Context, *queuev1.DeferRequest) (*queuev1.TransitionResponse, error)
	DeadLetter(context.Context, *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error)
	Cancel(context.Context, *queuev1.CancelRequest) (*queuev1.CancelResponse, error)
}

type WorkspaceLister interface {
	ListIDs(context.Context) ([]workspace.ID, error)
}

type queueServiceClientAdapter struct {
	client queuev1.QueueServiceClient
}

func QueueClientFromGRPC(client queuev1.QueueServiceClient) QueueClient {
	return queueServiceClientAdapter{client: client}
}

func (a queueServiceClientAdapter) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return a.client.Lease(ctx, request)
}

func (a queueServiceClientAdapter) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	return a.client.Heartbeat(ctx, request)
}

func (a queueServiceClientAdapter) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Ack(ctx, request)
}

func (a queueServiceClientAdapter) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Retry(ctx, request)
}

func (a queueServiceClientAdapter) Defer(ctx context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Defer(ctx, request)
}

func (a queueServiceClientAdapter) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	return a.client.DeadLetter(ctx, request)
}

func (a queueServiceClientAdapter) Cancel(ctx context.Context, request *queuev1.CancelRequest) (*queuev1.CancelResponse, error) {
	return a.client.Cancel(ctx, request)
}

type RuntimeJobDeliverer interface {
	DeliverRuntimeJob(context.Context, RuntimeJob) (RuntimeDeliveryResult, error)
}

type RuntimeDeliveryFinalizer interface {
	FinalizeRuntimeDelivery(context.Context, RuntimeJob, RuntimeDeliveryResult) (RuntimeDeliveryResult, error)
}

type RuntimeDeliveryFinalizationReplayer interface {
	ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error)
}

type malformedRuntimeInputCustodyReplacer interface {
	ReplaceMalformedRuntimeInputCustody(context.Context, RuntimeJob) (queue.ReplaceMalformedRuntimeInputCustodyResult, error)
}

type malformedRuntimeInputCustodyFinalizer interface {
	FinalizeMalformedRuntimeInputCustody(context.Context, MalformedRuntimeInputLease) (MalformedRuntimeInputCustodyResult, error)
}

type RuntimePodLossRepairer interface {
	RepairLostRuntimeBindings(context.Context, string) (int, error)
}

type JobRunner struct {
	Queue      QueueClient
	Workspaces WorkspaceLister
	Deliverer  RuntimeJobDeliverer
	Config     JobRunnerConfig
	Logger     *slog.Logger
}

type RuntimeJob struct {
	JobID                 string
	LeaseToken            string
	Kind                  string
	PartitionKey          string
	DedupeKey             string
	WorkspaceID           string
	SessionID             string
	SessionThreadID       string
	RuntimeInputID        string
	ConfigGeneration      string
	MCPServerName         string
	MCPManifestGeneration string
	CleanupJobID          string
	DeleteCleanupID       string
	EventIDs              []string
	SequenceFrom          int64
	SequenceTo            int64
	InputKind             string
	RejectionReasonCode   string
	RecoverySourceEventID string
	PayloadJSON           string
	AttemptCount          int32
	MaxAttempts           int32
}

type RuntimeDeliveryStatus string

const (
	RuntimeDeliveryAccepted      RuntimeDeliveryStatus = "accepted"
	RuntimeDeliveryDuplicate     RuntimeDeliveryStatus = "duplicate"
	RuntimeDeliveryRejected      RuntimeDeliveryStatus = "rejected"
	RuntimeDeliveryBarrierStale  RuntimeDeliveryStatus = "barrier_stale"
	RuntimeDeliveryAuthorityLost RuntimeDeliveryStatus = "authority_lost"
)

type RuntimeDeliveryResult struct {
	Status            RuntimeDeliveryStatus
	Retryable         bool
	ErrorKind         string
	ErrorMessage      string
	QueueLeaseSettled bool
	// The command plan carries the binding that actually owned this attempt.
	// These process-local fields fence Bridge finalization after a Pod-loss
	// handoff; they are not Queue payload, RPC result, or durable state.
	AttemptedBindingID         string
	AttemptedBindingGeneration int64
	AttemptedTargetPodUID      string
}

func (r *JobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *JobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil {
		return false, errors.New("bridge job runner queue client is required")
	}
	if r.Workspaces == nil {
		return false, errors.New("bridge workspace lister is required")
	}
	if r.Deliverer == nil {
		return false, errors.New("bridge runtime job deliverer is required")
	}
	cfg := r.Config
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = ServiceNameJobRunner
	}
	if cfg.MaxJobs <= 0 {
		cfg.MaxJobs = defaultJobRunnerMaxJobs
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultJobRunnerLeaseDuration
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	workspaceIDs, err := r.Workspaces.ListIDs(ctx)
	if err != nil {
		return false, err
	}
	hadWork := false
	// One workspace's failure must not starve the workspaces after it: the list
	// is deterministically ordered, so aborting the sweep would permanently
	// withhold repair from every workspace sorted later. Failures are collected
	// and reported after the whole list has had its turn. A cancelled context is
	// the exception — it means the runner itself is stopping.
	var sweepErrs []error
	for _, workspaceID := range workspaceIDs {
		if ctx.Err() != nil {
			sweepErrs = append(sweepErrs, ctx.Err())
			break
		}
		if workspaceID == "" {
			sweepErrs = append(sweepErrs, errors.New("bridge discovered an empty workspace id"))
			continue
		}
		workspaceHadWork, err := r.runWorkspaceOnce(ctx, workspaceID.String(), cfg)
		hadWork = hadWork || workspaceHadWork
		if err != nil {
			sweepErrs = append(sweepErrs, fmt.Errorf("workspace %s: %w", workspaceID, err))
		}
	}
	return hadWork, errors.Join(sweepErrs...)
}

func (r *JobRunner) runWorkspaceOnce(ctx context.Context, workspaceID string, cfg JobRunnerConfig) (bool, error) {
	hadWork := false
	var phaseErrs []error
	if repairer, ok := r.Deliverer.(RuntimePodLossRepairer); ok {
		repaired, err := repairer.RepairLostRuntimeBindings(ctx, workspaceID)
		hadWork = repaired > 0
		if err != nil {
			phaseErrs = append(phaseErrs, fmt.Errorf("runtime pod-loss repair: %w", err))
		}
		if ctx.Err() != nil {
			return hadWork, errors.Join(append(phaseErrs, ctx.Err())...)
		}
	}
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     workspaceID,
		Kinds:           []string{queue.KindRuntimeInput, queue.KindRuntimeRecovery, queue.KindRuntimeConfigUpdate, queue.KindCleanupSession, queue.KindSessionDeleteCleanup},
		LeaseOwner:      cfg.LeaseOwner,
		MaxJobs:         int32(cfg.MaxJobs),
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		phaseErrs = append(phaseErrs, fmt.Errorf("queue lease: %w", err))
		return hadWork, errors.Join(phaseErrs...)
	}
	hadWork = hadWork || len(lease.GetJobs()) > 0
	for _, job := range lease.GetJobs() {
		if job.GetWorkspaceId() != workspaceID {
			phaseErrs = append(phaseErrs, errors.New("bridge queue returned a cross-workspace job"))
			break
		}
		if err := r.processRuntimeJob(ctx, job, cfg); err != nil {
			phaseErrs = append(phaseErrs, err)
			break
		}
	}
	return hadWork, errors.Join(phaseErrs...)
}

func (r *JobRunner) processRuntimeJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg JobRunnerConfig) error {
	job, err := DecodeRuntimeJob(queueJob)
	if err != nil {
		if queueJob.GetKind() == queue.KindRuntimeInput {
			finalizer, ok := r.Deliverer.(malformedRuntimeInputCustodyFinalizer)
			if !ok {
				return errors.New("malformed runtime-input custody finalizer is required")
			}
			outcome, finalizeErr := finalizer.FinalizeMalformedRuntimeInputCustody(ctx, malformedRuntimeInputLease(queueJob))
			if finalizeErr != nil {
				return finalizeErr
			}
			if outcome.Retry {
				return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
					WorkspaceId:  queueJob.GetWorkspaceId(),
					JobId:        queueJob.GetId(),
					LeaseToken:   queueJob.GetLeaseToken(),
					ErrorKind:    "invalid_runtime_job_payload",
					ErrorMessage: "runtime queue payload is invalid",
				}))
			}
			if outcome.Handled {
				return nil
			}
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_runtime_job_payload",
			ErrorMessage: "runtime queue payload is invalid",
		}))
	}
	workCtx, stopHeartbeat := startJobRunnerHeartbeat(ctx, r.Queue, job, cfg)
	if job.Kind == queue.KindRuntimeInput {
		replayer, ok := r.Deliverer.(RuntimeDeliveryFinalizationReplayer)
		if !ok {
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return heartbeatErr
			}
			return errors.New("bridge runtime delivery finalization replayer is required")
		}
		replayed, found, err := replayer.ReplayRuntimeDeliveryFinalization(workCtx, job)
		if err != nil {
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return heartbeatErr
			}
			if invalidRuntimeJobPayload(err) {
				return r.settleInvalidRuntimeJobPayload(ctx, job)
			}
			return err
		}
		if found {
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return heartbeatErr
			}
			r.logRuntimeJobAttempt(job, "none", "durable_replay")
			return r.applyRuntimeDeliveryResult(ctx, job, replayed)
		}
		if job.InputKind == "interrupt_control" && runtimeJobFinalAttempt(job) {
			finalized, finalizeErr := r.finalizeRuntimeDelivery(workCtx, job, RuntimeDeliveryResult{
				Status:       RuntimeDeliveryRejected,
				Retryable:    false,
				ErrorKind:    "runtime_delivery_exhausted",
				ErrorMessage: "runtime delivery attempts are exhausted",
			})
			heartbeatErr := stopHeartbeat()
			if heartbeatErr != nil && !finalized.QueueLeaseSettled {
				return heartbeatErr
			}
			if finalizeErr != nil {
				if invalidRuntimeJobPayload(finalizeErr) {
					return r.settleInvalidRuntimeJobPayload(ctx, job)
				}
				return finalizeErr
			}
			r.logRuntimeJobAttempt(job, "none", runtimeJobFinalizationDisposition(finalized))
			return r.applyRuntimeDeliveryResult(ctx, job, finalized)
		}
	}
	result, deliverErr := r.Deliverer.DeliverRuntimeJob(workCtx, job)
	if heartbeatErr := stopHeartbeat(); heartbeatErr != nil && !result.QueueLeaseSettled {
		return heartbeatErr
	}
	if result.QueueLeaseSettled {
		return deliverErr
	}
	if deliverErr != nil {
		if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" {
			replayer := r.Deliverer.(RuntimeDeliveryFinalizationReplayer)
			replayed, found, replayErr := replayer.ReplayRuntimeDeliveryFinalization(ctx, job)
			if replayErr != nil {
				return replayErr
			}
			if found {
				r.logRuntimeJobAttempt(job, "runtime_transport_error", "durable_replay")
				return r.applyRuntimeDeliveryResult(ctx, job, replayed)
			}
		}
		preparationKind := runtimeJobPreparationErrorKind(deliverErr)
		attemptedBindingID := result.AttemptedBindingID
		attemptedBindingGeneration := result.AttemptedBindingGeneration
		attemptedTargetPodUID := result.AttemptedTargetPodUID
		result = RuntimeDeliveryResult{
			Status:                     RuntimeDeliveryRejected,
			Retryable:                  true,
			ErrorKind:                  "runtime_transport_error",
			ErrorMessage:               "runtime command delivery failed",
			AttemptedBindingID:         attemptedBindingID,
			AttemptedBindingGeneration: attemptedBindingGeneration,
			AttemptedTargetPodUID:      attemptedTargetPodUID,
		}
		if job.Kind == queue.KindRuntimeConfigUpdate && !isMCPManifestRuntimeJob(job) {
			r.logRuntimeJobAttempt(job, preparationKind, "deferred")
			return r.deferRuntimeConfig(ctx, job)
		}
		if runtimeJobFinalAttempt(job) && job.InputKind != "interrupt_control" {
			finalized, err := r.finalizeRuntimeDelivery(ctx, job, result)
			if err != nil {
				if invalidRuntimeJobPayload(err) {
					return r.settleInvalidRuntimeJobPayload(ctx, job)
				}
				return err
			}
			if shouldDeferRuntimeConfigResult(job, finalized) {
				r.logRuntimeJobAttempt(job, preparationKind, "deferred")
				return r.deferRuntimeConfig(ctx, job)
			}
			r.logRuntimeJobAttempt(job, preparationKind, runtimeJobFinalizationDisposition(finalized))
			return r.applyRuntimeDeliveryResult(ctx, job, finalized)
		}
		r.logRuntimeJobAttempt(job, preparationKind, "retry_scheduled")
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    result.ErrorKind,
			ErrorMessage: result.ErrorMessage,
		}))
	}
	if shouldDeferRuntimeConfigResult(job, result) {
		r.logRuntimeJobAttempt(job, "none", "deferred")
		return r.deferRuntimeConfig(ctx, job)
	}
	// An interrupt without a durable receipt remains an outcome-unknown Session
	// partition barrier. Queue.Retry preserves that custody even when Runtime
	// returned a nominally terminal rejection; only receipt replay may ACK it.
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" && result.Status == RuntimeDeliveryRejected {
		result.Retryable = true
	}
	preparationKind := "none"
	if result.ErrorKind != "" {
		preparationKind = result.ErrorKind
	}
	if runtimeDeliveryRequiresFinalization(job, result) {
		finalized, err := r.finalizeRuntimeDelivery(ctx, job, result)
		if err != nil {
			if invalidRuntimeJobPayload(err) {
				return r.settleInvalidRuntimeJobPayload(ctx, job)
			}
			return err
		}
		result = finalized
	}
	if shouldDeferRuntimeConfigResult(job, result) {
		r.logRuntimeJobAttempt(job, preparationKind, "deferred")
		return r.deferRuntimeConfig(ctx, job)
	}
	r.logRuntimeJobAttempt(job, preparationKind, runtimeJobFinalizationDisposition(result))
	return r.applyRuntimeDeliveryResult(ctx, job, result)
}

func malformedRuntimeInputLease(job *queuev1.QueueJob) MalformedRuntimeInputLease {
	return MalformedRuntimeInputLease{
		WorkspaceID: job.GetWorkspaceId(), JobID: job.GetId(), LeaseToken: job.GetLeaseToken(),
		Kind: job.GetKind(), PartitionKey: job.GetPartitionKey(), DedupeKey: job.GetDedupeKey(),
	}
}

func runtimeJobPreparationErrorKind(err error) string {
	var preparation runtimeDeliveryPrepareError
	if errors.As(err, &preparation) && preparation.kind != "" {
		return preparation.kind
	}
	return "runtime_transport_error"
}

func runtimeJobFinalizationDisposition(result RuntimeDeliveryResult) string {
	if result.ErrorKind != "" {
		return string(result.Status) + ":" + result.ErrorKind
	}
	if result.Status == "" {
		return "none"
	}
	return string(result.Status)
}

// Runtime delivery telemetry is emitted only after the owning branch has
// selected its Queue disposition. It carries bounded identities and kinds;
// durable Inbox and Queue facts remain the lifecycle authority.
func (r *JobRunner) logRuntimeJobAttempt(job RuntimeJob, preparationKind string, disposition string) {
	if r == nil || r.Logger == nil {
		return
	}
	defer func() { _ = recover() }()
	r.Logger.Info("bridge.runtime_delivery.attempt",
		slog.String("operation", "runtime_delivery.attempt"),
		slog.String("event.kind", "runtime_delivery_attempt"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("workspace.id", job.WorkspaceID),
		slog.String("session.id", job.SessionID),
		slog.String("thread.id", job.SessionThreadID),
		slog.String("queue.job.id", job.JobID),
		slog.String("runtime.input.id", job.RuntimeInputID),
		slog.String("runtime.input.kind", job.InputKind),
		slog.Int("queue.attempt", int(job.AttemptCount)),
		slog.Int("queue.max_attempts", int(job.MaxAttempts)),
		slog.String("preparation.error_kind", preparationKind),
		slog.String("finalization.disposition", disposition),
	)
}

func invalidRuntimeJobPayload(err error) bool {
	var preparation runtimeDeliveryPrepareError
	return errors.As(err, &preparation) && preparation.kind == "invalid_runtime_job_payload" && !preparation.retryable
}

func (r *JobRunner) settleInvalidRuntimeJobPayload(ctx context.Context, job RuntimeJob) error {
	if job.Kind != queue.KindRuntimeInput || job.InputKind != "interrupt_control" {
		return r.deadLetterInvalidRuntimeJob(ctx, job)
	}
	finalizer, ok := r.Deliverer.(malformedRuntimeInputCustodyFinalizer)
	if !ok {
		return errors.New("malformed runtime-input custody finalizer is required")
	}
	outcome, err := finalizer.FinalizeMalformedRuntimeInputCustody(ctx, MalformedRuntimeInputLease{
		WorkspaceID:  job.WorkspaceID,
		JobID:        job.JobID,
		LeaseToken:   job.LeaseToken,
		Kind:         job.Kind,
		PartitionKey: job.PartitionKey,
		DedupeKey:    job.DedupeKey,
	})
	if err != nil {
		return err
	}
	if outcome.Retry {
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    "invalid_runtime_job_payload",
			ErrorMessage: "runtime queue payload is invalid",
		}))
	}
	if outcome.Handled {
		disposition := "invalid_interrupt_terminalized"
		if !outcome.InterruptTerminalized {
			disposition = "invalid_interrupt_settled"
		}
		r.logRuntimeJobAttempt(job, "invalid_runtime_job_payload", disposition)
		return nil
	}
	r.logRuntimeJobAttempt(job, "invalid_runtime_job_payload", "invalid_interrupt_invariant_dead_lettered")
	return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
		WorkspaceId:  job.WorkspaceID,
		JobId:        job.JobID,
		LeaseToken:   job.LeaseToken,
		ErrorKind:    "invalid_runtime_job_payload",
		ErrorMessage: "interrupt runtime custody has no unique canonical Inbox",
	}))
}

func (r *JobRunner) deadLetterInvalidRuntimeJob(ctx context.Context, job RuntimeJob) error {
	if job.Kind != queue.KindRuntimeInput {
		r.logRuntimeJobAttempt(job, "invalid_runtime_job_payload", "invalid_payload_dead_lettered")
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    "invalid_runtime_job_payload",
			ErrorMessage: "runtime queue payload is invalid",
		}))
	}
	replacer, ok := r.Deliverer.(malformedRuntimeInputCustodyReplacer)
	if !ok {
		return errors.New("malformed runtime-input custody replacer is required")
	}
	outcome, err := replacer.ReplaceMalformedRuntimeInputCustody(ctx, job)
	if err != nil {
		return err
	}
	disposition := "invalid_payload_stale_lease"
	if outcome.Replaced {
		disposition = "invalid_payload_replaced"
	} else if outcome.DeadLettered {
		disposition = "invalid_payload_dead_lettered"
	}
	r.logRuntimeJobAttempt(job, "invalid_runtime_job_payload", disposition)
	return nil
}

func shouldDeferRuntimeConfigResult(job RuntimeJob, result RuntimeDeliveryResult) bool {
	if job.Kind != queue.KindRuntimeConfigUpdate || result.Status != RuntimeDeliveryRejected {
		return false
	}
	if !isMCPManifestRuntimeJob(job) {
		return true
	}
	return result.ErrorKind == "control_busy" || !result.Retryable
}

func (r *JobRunner) deferRuntimeConfig(ctx context.Context, job RuntimeJob) error {
	return transitionUpdated(r.Queue.Defer(ctx, &queuev1.DeferRequest{
		WorkspaceId: job.WorkspaceID,
		JobId:       job.JobID,
		LeaseToken:  job.LeaseToken,
	}))
}

func (r *JobRunner) finalizeRuntimeDelivery(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) (RuntimeDeliveryResult, error) {
	finalizer, ok := r.Deliverer.(RuntimeDeliveryFinalizer)
	if !ok {
		return RuntimeDeliveryResult{}, errors.New("bridge runtime delivery finalizer is required")
	}
	return finalizer.FinalizeRuntimeDelivery(ctx, job, result)
}

func runtimeDeliveryRequiresFinalization(job RuntimeJob, result RuntimeDeliveryResult) bool {
	if result.Status == RuntimeDeliveryBarrierStale {
		return job.Kind == queue.KindRuntimeInput && job.InputKind != "interrupt_control"
	}
	if result.Status != RuntimeDeliveryRejected {
		return false
	}
	if job.Kind == queue.KindRuntimeInput {
		if job.InputKind == "interrupt_control" {
			return false
		}
		return !result.Retryable || runtimeJobFinalAttempt(job)
	}
	return isMCPManifestRuntimeJob(job) && runtimeJobFinalAttempt(job)
}

func runtimeJobFinalAttempt(job RuntimeJob) bool {
	return (job.Kind == queue.KindRuntimeInput || isMCPManifestRuntimeJob(job)) &&
		job.MaxAttempts > 0 &&
		job.AttemptCount >= job.MaxAttempts
}

func isMCPManifestRuntimeJob(job RuntimeJob) bool {
	return job.Kind == queue.KindRuntimeConfigUpdate && job.MCPServerName != "" && job.MCPManifestGeneration != ""
}

func startJobRunnerHeartbeat(ctx context.Context, client QueueClient, job RuntimeJob, cfg JobRunnerConfig) (context.Context, func() error) {
	workCtx, cancelWork := context.WithCancel(ctx)
	if cfg.HeartbeatInterval <= 0 || cfg.LeaseDuration <= 0 {
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
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				response, err := client.Heartbeat(heartbeatCtx, &queuev1.HeartbeatRequest{
					WorkspaceId:     job.WorkspaceID,
					JobId:           job.JobID,
					LeaseToken:      job.LeaseToken,
					LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
				})
				if heartbeatCtx.Err() != nil {
					return
				}
				if err != nil || !response.GetUpdated() {
					heartbeatErr = errors.New("bridge queue lease lost")
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

func (r *JobRunner) applyRuntimeDeliveryResult(ctx context.Context, job RuntimeJob, result RuntimeDeliveryResult) error {
	if result.QueueLeaseSettled {
		return nil
	}
	switch result.Status {
	case RuntimeDeliveryAuthorityLost:
		return nil
	case RuntimeDeliveryAccepted, RuntimeDeliveryDuplicate:
		return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{
			WorkspaceId: job.WorkspaceID,
			JobId:       job.JobID,
			LeaseToken:  job.LeaseToken,
		}))
	case RuntimeDeliveryRejected:
		errorKind := valueOrDefault(result.ErrorKind, "runtime_rejected_input")
		errorMessage := valueOrDefault(result.ErrorMessage, "runtime rejected input")
		if result.Retryable {
			return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
				WorkspaceId:  job.WorkspaceID,
				JobId:        job.JobID,
				LeaseToken:   job.LeaseToken,
				ErrorKind:    errorKind,
				ErrorMessage: errorMessage,
			}))
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    errorKind,
			ErrorMessage: errorMessage,
		}))
	case RuntimeDeliveryBarrierStale:
		return errors.New("barrier-stale runtime delivery was not atomically finalized")
	default:
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    "invalid_runtime_response",
			ErrorMessage: "runtime response status is invalid",
		}))
	}
}

func DecodeRuntimeJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	if queueJob == nil {
		return RuntimeJob{}, errors.New("queue job is required")
	}
	switch queueJob.GetKind() {
	case queue.KindRuntimeRecovery:
		return decodeRuntimeRecoveryJob(queueJob)
	case queue.KindRuntimeInput:
		return decodeRuntimeInputJob(queueJob)
	case queue.KindRuntimeConfigUpdate:
		return decodeRuntimeConfigUpdateJob(queueJob)
	case queue.KindCleanupSession:
		return decodeCleanupSessionJob(queueJob)
	case queue.KindSessionDeleteCleanup:
		return decodeSessionDeleteCleanupJob(queueJob)
	default:
		return RuntimeJob{}, fmt.Errorf("queue job kind %q is not a Bridge runtime-facing job", queueJob.GetKind())
	}
}

func decodeRuntimeRecoveryJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	var payload struct {
		SessionID       string `json:"session_id"`
		SessionThreadID string `json:"session_thread_id"`
		SourceEventID   string `json:"source_event_id"`
	}
	if err := json.Unmarshal([]byte(queueJob.GetPayloadJson()), &payload); err != nil {
		return RuntimeJob{}, err
	}
	if queueJob.GetWorkspaceId() == "" || queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" ||
		payload.SessionID == "" || payload.SessionThreadID == "" || payload.SourceEventID == "" {
		return RuntimeJob{}, errors.New("runtime recovery payload has missing identity fields")
	}
	return RuntimeJob{
		JobID: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(), Kind: queue.KindRuntimeRecovery,
		PartitionKey: queueJob.GetPartitionKey(), DedupeKey: queueJob.GetDedupeKey(),
		WorkspaceID: queueJob.GetWorkspaceId(), SessionID: payload.SessionID, SessionThreadID: payload.SessionThreadID,
		RecoverySourceEventID: payload.SourceEventID,
		PayloadJSON:           queueJob.GetPayloadJson(), AttemptCount: queueJob.GetAttemptCount(), MaxAttempts: queueJob.GetMaxAttempts(),
	}, nil
}

func decodeRuntimeInputJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	var payload struct {
		WorkspaceID     string   `json:"workspace_id"`
		SessionID       string   `json:"session_id"`
		SessionThreadID string   `json:"session_thread_id"`
		RuntimeInputID  string   `json:"runtime_input_id"`
		EventIDs        []string `json:"event_ids"`
		SequenceFrom    int64    `json:"sequence_from"`
		SequenceTo      int64    `json:"sequence_to"`
		InputKind       string   `json:"input_kind"`
	}
	if err := json.Unmarshal([]byte(queueJob.GetPayloadJson()), &payload); err != nil {
		return RuntimeJob{}, err
	}
	if payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() {
		return RuntimeJob{}, errors.New("payload workspace_id must match queue job")
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || payload.SessionID == "" || payload.SessionThreadID == "" || payload.RuntimeInputID == "" {
		return RuntimeJob{}, errors.New("runtime input payload has missing identity fields")
	}
	eventless := payload.InputKind == "task_notification" || payload.InputKind == "agent_mail"
	if len(payload.EventIDs) == 0 && !eventless {
		return RuntimeJob{}, errors.New("runtime input payload event_ids is required")
	}
	if !eventless && (payload.SequenceFrom <= 0 || payload.SequenceTo < payload.SequenceFrom) {
		return RuntimeJob{}, errors.New("runtime input payload sequence range is invalid")
	}
	if !validRuntimeInputKind(payload.InputKind) {
		return RuntimeJob{}, fmt.Errorf("unknown runtime input kind %q", payload.InputKind)
	}
	return RuntimeJob{
		JobID:           queueJob.GetId(),
		LeaseToken:      queueJob.GetLeaseToken(),
		Kind:            queue.KindRuntimeInput,
		PartitionKey:    queueJob.GetPartitionKey(),
		DedupeKey:       queueJob.GetDedupeKey(),
		WorkspaceID:     payload.WorkspaceID,
		SessionID:       payload.SessionID,
		SessionThreadID: payload.SessionThreadID,
		RuntimeInputID:  payload.RuntimeInputID,
		EventIDs:        append([]string(nil), payload.EventIDs...),
		SequenceFrom:    payload.SequenceFrom,
		SequenceTo:      payload.SequenceTo,
		InputKind:       payload.InputKind,
		PayloadJSON:     queueJob.GetPayloadJson(),
		AttemptCount:    queueJob.GetAttemptCount(),
		MaxAttempts:     queueJob.GetMaxAttempts(),
	}, nil
}

func decodeRuntimeConfigUpdateJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	var payload struct {
		WorkspaceID        string          `json:"workspace_id"`
		SessionID          string          `json:"session_id"`
		ConfigGeneration   json.RawMessage `json:"config_generation"`
		MCPServerName      string          `json:"mcp_server_name"`
		ManifestGeneration int64           `json:"manifest_generation"`
	}
	if err := json.Unmarshal([]byte(queueJob.GetPayloadJson()), &payload); err != nil {
		return RuntimeJob{}, err
	}
	if payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() {
		return RuntimeJob{}, errors.New("payload workspace_id must match queue job")
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || payload.SessionID == "" {
		return RuntimeJob{}, errors.New("runtime config update payload has missing identity fields")
	}
	var mcpServerName string
	var manifestGeneration string
	var configGeneration string
	var runtimeInputID string
	if payload.MCPServerName != "" || payload.ManifestGeneration != 0 {
		if payload.MCPServerName == "" || payload.ManifestGeneration <= 0 || len(payload.ConfigGeneration) > 0 {
			return RuntimeJob{}, errors.New("runtime config update payload has missing identity fields")
		}
		mcpServerName = payload.MCPServerName
		manifestGeneration = strconv.FormatInt(payload.ManifestGeneration, 10)
		runtimeInputID = runtimeMCPManifestInputID(payload.SessionID, mcpServerName, payload.ManifestGeneration)
	} else {
		var hasConfigGeneration bool
		var err error
		configGeneration, hasConfigGeneration, err = runtimeConfigGenerationString(payload.ConfigGeneration)
		if err != nil {
			return RuntimeJob{}, err
		}
		if !hasConfigGeneration {
			return RuntimeJob{}, errors.New("runtime config update payload has missing identity fields")
		}
		runtimeInputID = runtimeConfigUpdateInputID(payload.SessionID, configGeneration)
	}
	return RuntimeJob{
		JobID:                 queueJob.GetId(),
		LeaseToken:            queueJob.GetLeaseToken(),
		Kind:                  queue.KindRuntimeConfigUpdate,
		WorkspaceID:           payload.WorkspaceID,
		SessionID:             payload.SessionID,
		RuntimeInputID:        runtimeInputID,
		ConfigGeneration:      configGeneration,
		MCPServerName:         mcpServerName,
		MCPManifestGeneration: manifestGeneration,
		PayloadJSON:           queueJob.GetPayloadJson(),
		AttemptCount:          queueJob.GetAttemptCount(),
		MaxAttempts:           queueJob.GetMaxAttempts(),
	}, nil
}

func runtimeConfigUpdateInputID(sessionID string, configGeneration string) string {
	return "runtime_config_update:" + sessionID + ":" + configGeneration
}

func runtimeMCPManifestInputID(sessionID string, mcpServerName string, manifestGeneration int64) string {
	return "runtime_config_update:mcp_manifest:" + sessionID + ":" + mcpServerName + ":" + strconv.FormatInt(manifestGeneration, 10)
}

func runtimeConfigGenerationString(raw json.RawMessage) (string, bool, error) {
	if len(raw) == 0 {
		return "", false, nil
	}
	var generation int64
	if err := json.Unmarshal(raw, &generation); err != nil {
		return "", true, errors.New("runtime config update config_generation must be a positive integer")
	}
	if generation <= 0 {
		return "", true, errors.New("runtime config update config_generation must be a positive integer")
	}
	return strconv.FormatInt(generation, 10), true, nil
}

func decodeCleanupSessionJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	var payload struct {
		WorkspaceID  string `json:"workspace_id"`
		SessionID    string `json:"session_id"`
		CleanupJobID string `json:"cleanup_job_id"`
	}
	if err := json.Unmarshal([]byte(queueJob.GetPayloadJson()), &payload); err != nil {
		return RuntimeJob{}, err
	}
	if payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() {
		return RuntimeJob{}, errors.New("payload workspace_id must match queue job")
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || payload.SessionID == "" || payload.CleanupJobID == "" {
		return RuntimeJob{}, errors.New("cleanup session payload has missing identity fields")
	}
	return RuntimeJob{
		JobID:          queueJob.GetId(),
		LeaseToken:     queueJob.GetLeaseToken(),
		Kind:           queue.KindCleanupSession,
		WorkspaceID:    payload.WorkspaceID,
		SessionID:      payload.SessionID,
		RuntimeInputID: "cleanup_session:" + payload.CleanupJobID,
		CleanupJobID:   payload.CleanupJobID,
		PayloadJSON:    queueJob.GetPayloadJson(),
	}, nil
}

func decodeSessionDeleteCleanupJob(queueJob *queuev1.QueueJob) (RuntimeJob, error) {
	var payload struct {
		WorkspaceID     string `json:"workspace_id"`
		SessionID       string `json:"session_id"`
		DeleteCleanupID string `json:"delete_cleanup_id"`
	}
	if err := json.Unmarshal([]byte(queueJob.GetPayloadJson()), &payload); err != nil {
		return RuntimeJob{}, err
	}
	if payload.WorkspaceID == "" || payload.WorkspaceID != queueJob.GetWorkspaceId() {
		return RuntimeJob{}, errors.New("payload workspace_id must match queue job")
	}
	if queueJob.GetId() == "" || queueJob.GetLeaseToken() == "" || payload.SessionID == "" || payload.DeleteCleanupID == "" {
		return RuntimeJob{}, errors.New("session delete cleanup payload has missing identity fields")
	}
	return RuntimeJob{
		JobID: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(), Kind: queue.KindSessionDeleteCleanup,
		WorkspaceID: payload.WorkspaceID, SessionID: payload.SessionID,
		RuntimeInputID: "session_delete_cleanup:" + payload.DeleteCleanupID,
		CleanupJobID:   payload.DeleteCleanupID, DeleteCleanupID: payload.DeleteCleanupID,
		AttemptCount: queueJob.GetAttemptCount(), MaxAttempts: queueJob.GetMaxAttempts(),
		PayloadJSON: queueJob.GetPayloadJson(),
	}, nil
}

func validRuntimeInputKind(inputKind string) bool {
	switch inputKind {
	case "messages", "interrupt_control", "tool_confirmation", "task_notification", "agent_mail", "rejection":
		return true
	default:
		return false
	}
}

func transitionUpdated(response *queuev1.TransitionResponse, err error) error {
	if err != nil {
		return err
	}
	if response == nil || !response.GetUpdated() {
		return errors.New("queue transition did not update the leased job")
	}
	return nil
}
