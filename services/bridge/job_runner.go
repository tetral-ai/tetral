package agentruntimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
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

type RuntimeInboxRepairer interface {
	RepairRuntimeInbox(context.Context, string, int) (int, error)
}

type CompletionMailRepairer interface {
	RepairCompletionMail(context.Context, string, int) (int, error)
}

type RuntimePodLossRepairer interface {
	RepairLostRuntimeBindings(context.Context, string) (int, error)
}

type JobRunner struct {
	Queue      QueueClient
	Workspaces WorkspaceLister
	Deliverer  RuntimeJobDeliverer
	Config     JobRunnerConfig
}

type RuntimeJob struct {
	JobID                 string
	LeaseToken            string
	Kind                  string
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
	CommandKind           agentruntimev1.RuntimeCommandKind
	PayloadJSON           string
	AttemptCount          int32
	MaxAttempts           int32
}

type RuntimeDeliveryStatus string

const (
	RuntimeDeliveryAccepted  RuntimeDeliveryStatus = "accepted"
	RuntimeDeliveryDuplicate RuntimeDeliveryStatus = "duplicate"
	RuntimeDeliveryRejected  RuntimeDeliveryStatus = "rejected"
)

type RuntimeDeliveryResult struct {
	Status            RuntimeDeliveryStatus
	Retryable         bool
	ErrorKind         string
	ErrorMessage      string
	QueueLeaseSettled bool
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
	if repairer, ok := r.Deliverer.(RuntimeInboxRepairer); ok {
		repaired, err := repairer.RepairRuntimeInbox(ctx, workspaceID, defaultRuntimeInboxRepairBatch)
		hadWork = hadWork || repaired > 0
		if err != nil {
			phaseErrs = append(phaseErrs, fmt.Errorf("runtime inbox repair: %w", err))
		}
		if ctx.Err() != nil {
			return hadWork, errors.Join(append(phaseErrs, ctx.Err())...)
		}
	}
	if repairer, ok := r.Deliverer.(CompletionMailRepairer); ok {
		repaired, err := repairer.RepairCompletionMail(ctx, workspaceID, defaultRuntimeInboxRepairBatch)
		hadWork = hadWork || repaired > 0
		if err != nil {
			phaseErrs = append(phaseErrs, fmt.Errorf("completion-mail repair: %w", err))
		}
		if ctx.Err() != nil {
			return hadWork, errors.Join(append(phaseErrs, ctx.Err())...)
		}
	}
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId:     workspaceID,
		Kinds:           []string{queue.KindRuntimeInput, queue.KindRuntimeConfigUpdate, queue.KindCleanupSession, queue.KindSessionDeleteCleanup},
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
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId:  queueJob.GetWorkspaceId(),
			JobId:        queueJob.GetId(),
			LeaseToken:   queueJob.GetLeaseToken(),
			ErrorKind:    "invalid_runtime_job_payload",
			ErrorMessage: "runtime queue payload is invalid",
		}))
	}
	workCtx, stopHeartbeat := startJobRunnerHeartbeat(ctx, r.Queue, job, cfg)
	if job.Kind == queue.KindRuntimeInput && job.InputKind == "interrupt_control" {
		if _, err := r.Queue.Cancel(workCtx, &queuev1.CancelRequest{
			WorkspaceId:            job.WorkspaceID,
			SessionId:              job.SessionID,
			SessionThreadId:        job.SessionThreadID,
			InterruptFenceSequence: job.SequenceTo,
		}); err != nil {
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return heartbeatErr
			}
			return err
		}
	}
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
			return err
		}
		if found {
			if heartbeatErr := stopHeartbeat(); heartbeatErr != nil {
				return heartbeatErr
			}
			return r.applyRuntimeDeliveryResult(ctx, job, replayed)
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
		result = RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    true,
			ErrorKind:    "runtime_transport_error",
			ErrorMessage: "runtime command delivery failed",
		}
		if job.Kind == queue.KindRuntimeConfigUpdate && !isMCPManifestRuntimeJob(job) {
			return r.deferRuntimeConfig(ctx, job)
		}
		if runtimeJobFinalAttempt(job) {
			finalized, err := r.finalizeRuntimeDelivery(ctx, job, result)
			if err != nil {
				return err
			}
			if shouldDeferRuntimeConfigResult(job, finalized) {
				return r.deferRuntimeConfig(ctx, job)
			}
			return r.applyRuntimeDeliveryResult(ctx, job, finalized)
		}
		return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
			WorkspaceId:  job.WorkspaceID,
			JobId:        job.JobID,
			LeaseToken:   job.LeaseToken,
			ErrorKind:    result.ErrorKind,
			ErrorMessage: result.ErrorMessage,
		}))
	}
	if shouldDeferRuntimeConfigResult(job, result) {
		return r.deferRuntimeConfig(ctx, job)
	}
	if runtimeDeliveryRequiresFinalization(job, result) {
		finalized, err := r.finalizeRuntimeDelivery(ctx, job, result)
		if err != nil {
			return err
		}
		result = finalized
	}
	if shouldDeferRuntimeConfigResult(job, result) {
		return r.deferRuntimeConfig(ctx, job)
	}
	return r.applyRuntimeDeliveryResult(ctx, job, result)
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
	if result.Status != RuntimeDeliveryRejected {
		return false
	}
	if job.Kind == queue.KindRuntimeInput {
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
	switch result.Status {
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
	commandKind, err := RuntimeCommandKindForInputKind(payload.InputKind)
	if err != nil {
		return RuntimeJob{}, err
	}
	return RuntimeJob{
		JobID:           queueJob.GetId(),
		LeaseToken:      queueJob.GetLeaseToken(),
		Kind:            queue.KindRuntimeInput,
		WorkspaceID:     payload.WorkspaceID,
		SessionID:       payload.SessionID,
		SessionThreadID: payload.SessionThreadID,
		RuntimeInputID:  payload.RuntimeInputID,
		EventIDs:        append([]string(nil), payload.EventIDs...),
		SequenceFrom:    payload.SequenceFrom,
		SequenceTo:      payload.SequenceTo,
		InputKind:       payload.InputKind,
		CommandKind:     commandKind,
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
		CommandKind:           agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_RUNTIME_CONFIG_PATCH,
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
		CommandKind:    agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
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
		CommandKind: agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_CLEANUP_SESSION,
		PayloadJSON: queueJob.GetPayloadJson(),
	}, nil
}

func RuntimeCommandKindForInputKind(inputKind string) (agentruntimev1.RuntimeCommandKind, error) {
	switch inputKind {
	case "messages":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES, nil
	case "interrupt_control":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_INTERRUPT_CONTROL, nil
	case "tool_confirmation":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TOOL_CONFIRMATION, nil
	case "task_notification":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_TASK_NOTIFICATION, nil
	case "agent_mail":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_AGENT_MAIL, nil
	case "rejection":
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_MESSAGES, nil
	default:
		return agentruntimev1.RuntimeCommandKind_RUNTIME_COMMAND_KIND_UNSPECIFIED, fmt.Errorf("unknown runtime input kind %q", inputKind)
	}
}

func RuntimeDeliveryResultFromResponse(response *agentruntimev1.RuntimeInputCommandResponse) RuntimeDeliveryResult {
	return RuntimeDeliveryResultFromResponseForRequest(response, nil)
}

func RuntimeDeliveryResultFromResponseForRequest(
	response *agentruntimev1.RuntimeInputCommandResponse,
	request *agentruntimev1.RuntimeInputCommandRequest,
) RuntimeDeliveryResult {
	if response == nil {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			ErrorKind:    "invalid_runtime_response",
			ErrorMessage: "runtime response is missing",
		}
	}
	if request != nil && !runtimeResponseIdentityMatchesRequest(response, request) {
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    false,
			ErrorKind:    "invalid_runtime_response_identity",
			ErrorMessage: "runtime response identity does not match command",
		}
	}
	switch response.GetStatus() {
	case agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}
	case agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_DUPLICATE:
		return RuntimeDeliveryResult{Status: RuntimeDeliveryDuplicate}
	case agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_REJECTED:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			Retryable:    response.GetRetryable(),
			ErrorKind:    runtimeInputErrorKind(response.GetErrorCode()),
			ErrorMessage: "runtime rejected input",
		}
	default:
		return RuntimeDeliveryResult{
			Status:       RuntimeDeliveryRejected,
			ErrorKind:    "invalid_runtime_response",
			ErrorMessage: "runtime response status is invalid",
		}
	}
}

func runtimeResponseIdentityMatchesRequest(response *agentruntimev1.RuntimeInputCommandResponse, request *agentruntimev1.RuntimeInputCommandRequest) bool {
	return response.GetSessionId() == request.GetSessionId() &&
		response.GetRuntimeInputId() == request.GetRuntimeInputId() &&
		response.GetBindingId() == request.GetBindingId() &&
		response.GetBindingGeneration() == request.GetBindingGeneration()
}

func runtimeInputErrorKind(code agentruntimev1.RuntimeInputErrorCode) string {
	switch code {
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_SELECTED_POD_IDENTITY_MISMATCH:
		return "selected_pod_identity_mismatch"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_INPUT_IDENTITY_CONFLICT:
		return "runtime_input_identity_conflict"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BINDING_IDENTITY_MISMATCH:
		return "binding_identity_mismatch"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_UNAVAILABLE:
		return "bridge_commit_unavailable"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BRIDGE_COMMIT_REJECTED:
		return "bridge_commit_rejected"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BRIDGE_TOKEN_UNAVAILABLE:
		return "bridge_token_unavailable"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_BRIDGE_TASK_NOTIFICATION_PROJECTION_INVALID:
		return "bridge_task_notification_projection_invalid"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_CONFLICT:
		return "runtime_control_conflict"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTEXT_LOAD_FAILED:
		return "runtime_context_load_failed"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_CONTROL_NOT_ACCEPTED:
		return "runtime_control_not_accepted"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_CONTROL_BUSY:
		return "control_busy"
	case agentruntimev1.RuntimeInputErrorCode_RUNTIME_INPUT_ERROR_CODE_RUNTIME_REJECTED_INPUT:
		return "runtime_rejected_input"
	default:
		return "runtime_rejected_input"
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
