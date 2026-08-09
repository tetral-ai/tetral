package tetralsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type SandboxToolCancelJob struct {
	QueueJob        *queuev1.QueueJob
	JobID           string
	LeaseToken      string
	WorkspaceID     string
	SessionID       string
	SessionThreadID string
	ToolUseEventID  string
}

type SandboxToolCancellationWork struct {
	Job               SandboxToolCancelJob
	AttemptGeneration int64
	Provider          string
	Reference         sandboxdriver.CommandReference
	State             string
}

type SandboxToolCancellationStore interface {
	ClaimToolCancellation(context.Context, SandboxToolCancelJob, time.Time) (SandboxToolCancellationWork, bool, error)
	MarkToolCancellationSubmitted(context.Context, SandboxToolCancellationWork, time.Time) (bool, error)
	SettleToolCancellation(context.Context, SandboxToolCancellationWork, string, string, string, time.Time) error
	FinalizeToolCancellation(context.Context, SandboxToolCancelJob, time.Time) error
}

type ToolCancellationAdapter interface {
	CancelBackground(context.Context, sandboxdriver.CommandCancel) ProviderOutcome[sandboxdriver.CommandResult]
}

type SandboxToolCancelJobRunner struct {
	Queue     SandboxQueueClient
	Store     SandboxToolCancellationStore
	Providers *ProviderRegistry
	Config    SandboxLifecycleRunnerConfig
	Clock     func() time.Time
	Logger    *slog.Logger
}

func (r *SandboxToolCancelJobRunner) RunOnce(ctx context.Context) error {
	_, err := r.RunOnceWithActivity(ctx)
	return err
}

func (r *SandboxToolCancelJobRunner) RunOnceWithActivity(ctx context.Context) (bool, error) {
	if r == nil || r.Queue == nil || r.Store == nil || r.Providers == nil {
		return false, errors.New("sandbox tool cancellation runner dependencies are required")
	}
	cfg, err := normalizeSandboxLifecycleRunnerConfig(r.Config)
	if err != nil {
		return false, err
	}
	leaseSentAt := time.Now()
	lease, err := r.Queue.Lease(ctx, &queuev1.LeaseRequest{
		WorkspaceId: cfg.WorkspaceID, Kinds: []string{queue.KindSandboxToolCancel},
		LeaseOwner: cfg.LeaseOwner, MaxJobs: int32(cfg.MaxJobs), LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	if err != nil {
		return false, err
	}
	for _, job := range lease.GetJobs() {
		if err := r.processJob(ctx, job, cfg, leaseSentAt.Add(wireRoundedQueueLeaseDuration(cfg.LeaseDuration))); err != nil {
			return len(lease.GetJobs()) > 0, err
		}
	}
	return len(lease.GetJobs()) > 0, nil
}

func (r *SandboxToolCancelJobRunner) processJob(ctx context.Context, queueJob *queuev1.QueueJob, cfg SandboxLifecycleRunnerConfig, localExpiry time.Time) (resultErr error) {
	workCtx, finishLease, err := startQueueLeaseGuard(ctx, r.Queue, queueJob, localExpiry, cfg.HeartbeatInterval, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer func() {
		if leaseErr := finishLease(); resultErr == nil && leaseErr != nil {
			resultErr = leaseErr
		}
	}()
	ctx = workCtx
	jobIdentity := SandboxLifecycleJob{JobID: queueJob.GetId(), WorkspaceID: queueJob.GetWorkspaceId()}
	defer func() {
		if writer := queueAuthorityLossWriter(resultErr); writer != "" {
			logSandboxQueueAuthorityLost(r.Logger, jobIdentity, queue.KindSandboxToolCancel, writer)
		}
	}()
	transportJob, transportErr := decodeSandboxToolCancelQueueTransportIdentity(queueJob)
	if transportErr == nil && queueJob.GetMaxAttempts() <= 0 {
		if err := r.Store.FinalizeToolCancellation(ctx, transportJob, r.now()); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_tool_cancel_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_queue_integrity_error", ErrorMessage: "sandbox tool cancellation job has no attempt budget",
		}))
	}
	if transportErr == nil && queueJob.GetAttemptCount() > queueJob.GetMaxAttempts() {
		if err := r.Store.FinalizeToolCancellation(ctx, transportJob, r.now()); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_tool_cancel_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "sandbox_tool_cancel_attempts_exhausted", ErrorMessage: "sandbox tool cancellation attempt budget exhausted",
		}))
	}
	job, err := DecodeSandboxToolCancelJob(queueJob)
	if err != nil {
		if transportErr == nil {
			if err := r.Store.FinalizeToolCancellation(ctx, transportJob, r.now()); err != nil {
				if errors.Is(err, errQueueLeaseLost) {
					return queueAuthorityLostBy("sandbox_tool_cancel_finalize", err)
				}
				return err
			}
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: queueJob.GetWorkspaceId(), JobId: queueJob.GetId(), LeaseToken: queueJob.GetLeaseToken(),
			ErrorKind: "invalid_sandbox_tool_cancel_payload", ErrorMessage: "sandbox tool cancellation payload is invalid",
		}))
	}
	work, current, err := r.Store.ClaimToolCancellation(ctx, job, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_tool_cancel_claim", err)
		}
		return r.retry(ctx, queueJob, "sandbox_tool_cancel_store_error")
	}
	if !current {
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
	}
	adapter, ok := r.Providers.Resolve(work.Provider)
	if !ok {
		return r.settleUnknown(ctx, work, "provider_not_registered", "sandbox cancellation outcome is unknown")
	}
	canceller, ok := adapter.(ToolCancellationAdapter)
	if !ok {
		return r.settleUnknown(ctx, work, "provider_configuration_invalid", "sandbox cancellation outcome is unknown")
	}
	submitted, err := r.Store.MarkToolCancellationSubmitted(ctx, work, r.now())
	if err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_tool_cancel_mark_submitted", err)
		}
		return r.retry(ctx, queueJob, "sandbox_tool_cancel_store_error")
	}
	if !submitted {
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
	}
	outcome := canceller.CancelBackground(ctx, sandboxdriver.CommandCancel{CommandReference: work.Reference, Reason: "user_interrupt"})
	if outcome.Failed() || strings.TrimSpace(outcome.Value.ResultJSON) == "" || !json.Valid([]byte(outcome.Value.ResultJSON)) {
		kind := valueOrDefault(outcome.ErrorKind, "sandbox_cancellation_outcome_unknown")
		return r.settleUnknown(ctx, work, kind, "sandbox cancellation outcome is unknown")
	}
	resultJSON := outcome.Value.ResultJSON
	if outcome.Value.TerminalStatus == "cancelled" {
		resultJSON = `{"status":"cancelled"}`
	}
	if err := r.Store.SettleToolCancellation(ctx, work, resultJSON, "cancelled", "sandbox execution was cancelled", r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_tool_cancel_settle", err)
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: job.WorkspaceID, JobId: job.JobID, LeaseToken: job.LeaseToken}))
}

func (r *SandboxToolCancelJobRunner) settleUnknown(ctx context.Context, work SandboxToolCancellationWork, kind string, message string) error {
	if err := r.Store.SettleToolCancellation(ctx, work, "", kind, message, r.now()); err != nil {
		if errors.Is(err, errQueueLeaseLost) {
			return queueAuthorityLostBy("sandbox_tool_cancel_settle", err)
		}
		return err
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Ack(ctx, &queuev1.AckRequest{WorkspaceId: work.Job.WorkspaceID, JobId: work.Job.JobID, LeaseToken: work.Job.LeaseToken}))
}

func (r *SandboxToolCancelJobRunner) retry(ctx context.Context, job *queuev1.QueueJob, kind string) error {
	if job.GetAttemptCount() >= job.GetMaxAttempts() {
		decoded, err := decodeSandboxToolCancelQueueTransportIdentity(job)
		if err != nil {
			return err
		}
		if err := r.Store.FinalizeToolCancellation(ctx, decoded, r.now()); err != nil {
			if errors.Is(err, errQueueLeaseLost) {
				return queueAuthorityLostBy("sandbox_tool_cancel_finalize", err)
			}
			return err
		}
		if err := stopQueueLeaseGuard(ctx); err != nil {
			return err
		}
		return transitionUpdated(r.Queue.DeadLetter(ctx, &queuev1.DeadLetterRequest{
			WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken(),
			ErrorKind: "sandbox_tool_cancel_attempts_exhausted", ErrorMessage: "sandbox tool cancellation attempt budget exhausted",
		}))
	}
	if err := stopQueueLeaseGuard(ctx); err != nil {
		return err
	}
	return transitionUpdated(r.Queue.Retry(ctx, &queuev1.RetryRequest{
		WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken(),
		ErrorKind: kind, ErrorMessage: "sandbox tool cancellation will be retried",
	}))
}

func (r *SandboxToolCancelJobRunner) now() time.Time {
	if r != nil && r.Clock != nil {
		return r.Clock().UTC()
	}
	return storage.Now()
}

func DecodeSandboxToolCancelJob(job *queuev1.QueueJob) (SandboxToolCancelJob, error) {
	transport, err := decodeSandboxToolCancelQueueTransportIdentity(job)
	if err != nil {
		return SandboxToolCancelJob{}, err
	}
	var payload struct {
		WorkspaceID     string `json:"workspace_id"`
		SessionID       string `json:"session_id"`
		SessionThreadID string `json:"session_thread_id"`
		ToolUseEventID  string `json:"tool_use_event_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(job.GetPayloadJson()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return SandboxToolCancelJob{}, err
	}
	if payload.WorkspaceID != transport.WorkspaceID || payload.SessionID != transport.SessionID ||
		payload.SessionThreadID != transport.SessionThreadID || payload.ToolUseEventID != transport.ToolUseEventID {
		return SandboxToolCancelJob{}, errors.New("sandbox tool cancellation identity is invalid")
	}
	return transport, nil
}

func decodeSandboxToolCancelQueueTransportIdentity(job *queuev1.QueueJob) (SandboxToolCancelJob, error) {
	if job == nil || job.GetKind() != queue.KindSandboxToolCancel || job.GetId() == "" || job.GetLeaseToken() == "" || job.GetWorkspaceId() == "" {
		return SandboxToolCancelJob{}, errors.New("sandbox tool cancellation transport identity is invalid")
	}
	prefix := "sandbox-cancel:" + job.GetWorkspaceId() + ":"
	if !strings.HasPrefix(job.GetPartitionKey(), prefix) {
		return SandboxToolCancelJob{}, errors.New("sandbox tool cancellation partition identity is invalid")
	}
	parts := strings.Split(strings.TrimPrefix(job.GetPartitionKey(), prefix), ":")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return SandboxToolCancelJob{}, errors.New("sandbox tool cancellation partition identity is invalid")
	}
	ws := workspace.ID(job.GetWorkspaceId())
	if job.GetDedupeKey() != queue.FormatSandboxToolCancelDedupeKey(ws, parts[0], parts[1], parts[2]) {
		return SandboxToolCancelJob{}, errors.New("sandbox tool cancellation dedupe identity is invalid")
	}
	return SandboxToolCancelJob{
		QueueJob: job, JobID: job.GetId(), LeaseToken: job.GetLeaseToken(), WorkspaceID: job.GetWorkspaceId(),
		SessionID: parts[0], SessionThreadID: parts[1], ToolUseEventID: parts[2],
	}, nil
}

func (c *PostgreSQLSandboxExecutionCoordinator) ClaimToolCancellation(ctx context.Context, job SandboxToolCancelJob, now time.Time) (SandboxToolCancellationWork, bool, error) {
	var work SandboxToolCancellationWork
	var current bool
	err := c.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.tool_cancel.claim", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
			return err
		}
		var state string
		var generation int64
		var cancelState, encodedReference sql.NullString
		err := tx.QueryRow(ctx,
			`SELECT execution_state, execution_attempt_generation, cancel_state, provider_command_reference_json
			   FROM session_runtime_tool_results
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
			  FOR UPDATE`,
			job.WorkspaceID, job.SessionID, job.SessionThreadID, job.ToolUseEventID,
		).Scan(&state, &generation, &cancelState, &encodedReference)
		if dbconnect.IsNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if (state != "running" && state != "consumed") || !cancelState.Valid {
			return nil
		}
		if cancelState.String == "submitted" {
			if state == "consumed" {
				return clearConsumedToolCancellationTx(ctx, tx, job, generation, now)
			}
			return settleSandboxExecutionTx(ctx, tx, SandboxExecutionRef{
				WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, SessionThreadID: job.SessionThreadID, ToolUseEventID: job.ToolUseEventID,
			}, generation, SandboxExecutionSettlement{
				Kind: SandboxExecutionUnknownOutcome, ErrorKind: "sandbox_cancellation_outcome_unknown", SafeMessage: "sandbox cancellation outcome is unknown",
			}, now)
		}
		if cancelState.String != "pending" || !encodedReference.Valid {
			return nil
		}
		reference, err := decodeSandboxToolObservationReference(encodedReference.String)
		if err != nil {
			return err
		}
		work = SandboxToolCancellationWork{
			Job: job, AttemptGeneration: generation, Provider: reference.Provider,
			Reference: reference.Observation.Reference, State: cancelState.String,
		}
		current = true
		return nil
	})
	return work, current, err
}

func (c *PostgreSQLSandboxExecutionCoordinator) MarkToolCancellationSubmitted(ctx context.Context, work SandboxToolCancellationWork, now time.Time) (bool, error) {
	var changed bool
	err := c.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.tool_cancel.submit", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: work.Job.WorkspaceID, SessionID: work.Job.SessionID}); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET cancel_state='submitted', cancel_submitted_at=$6, updated_at=$6
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
			    AND execution_attempt_generation=$5 AND execution_state IN ('running','consumed') AND cancel_state='pending'`,
			work.Job.WorkspaceID, work.Job.SessionID, work.Job.SessionThreadID, work.Job.ToolUseEventID,
			work.AttemptGeneration, now.UTC(),
		)
		if err != nil {
			return err
		}
		changed = transitionRowsAffected(result)
		return nil
	})
	return changed, err
}

func (c *PostgreSQLSandboxExecutionCoordinator) SettleToolCancellation(ctx context.Context, work SandboxToolCancellationWork, resultJSON string, kind string, message string, now time.Time) error {
	return c.client.WithWorkspaceTx(ctx, work.Job.WorkspaceID, "sandbox.tool_cancel.settle", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: work.Job.WorkspaceID, SessionID: work.Job.SessionID}); err != nil {
			return err
		}
		if _, _, err := lockSandboxBinding(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID); err != nil {
			return err
		}
		if err := lockSandboxLifecycleOperationsForSession(ctx, tx, work.Job.WorkspaceID, work.Job.SessionID); err != nil {
			return err
		}
		var state string
		if err := tx.QueryRow(ctx,
			`SELECT execution_state FROM session_runtime_tool_results
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
			    AND execution_attempt_generation=$5 AND cancel_state IN ('pending','submitted') FOR UPDATE`,
			work.Job.WorkspaceID, work.Job.SessionID, work.Job.SessionThreadID, work.Job.ToolUseEventID,
			work.AttemptGeneration,
		).Scan(&state); dbconnect.IsNoRows(err) {
			return nil
		} else if err != nil {
			return err
		}
		if state == "consumed" {
			return clearConsumedToolCancellationTx(ctx, tx, work.Job, work.AttemptGeneration, now)
		}
		settlementKind := SandboxExecutionFailed
		if resultJSON == "" {
			settlementKind = SandboxExecutionUnknownOutcome
		}
		return settleSandboxExecutionTx(ctx, tx, SandboxExecutionRef{
			WorkspaceID: work.Job.WorkspaceID, SessionID: work.Job.SessionID,
			SessionThreadID: work.Job.SessionThreadID, ToolUseEventID: work.Job.ToolUseEventID,
		}, work.AttemptGeneration, SandboxExecutionSettlement{
			Kind: settlementKind, ResultJSON: resultJSON, ErrorKind: kind, SafeMessage: message,
		}, now)
	})
}

func (c *PostgreSQLSandboxExecutionCoordinator) FinalizeToolCancellation(ctx context.Context, job SandboxToolCancelJob, now time.Time) error {
	return c.client.WithWorkspaceTx(ctx, job.WorkspaceID, "sandbox.tool_cancel.finalize", func(tx *dbconnect.Tx) (txErr error) {
		defer finishSandboxQueueAuthorityTx(ctx, tx, &txErr)
		return finalizeToolCancellationTx(ctx, tx, job, now)
	})
}

func finalizeToolCancellationTx(ctx context.Context, tx *dbconnect.Tx, job SandboxToolCancelJob, now time.Time) error {
	if err := lockSession(ctx, tx, SandboxExecutionRef{WorkspaceID: job.WorkspaceID, SessionID: job.SessionID}); err != nil {
		return err
	}
	if _, _, err := lockSandboxBinding(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return err
	}
	if err := lockSandboxLifecycleOperationsForSession(ctx, tx, job.WorkspaceID, job.SessionID); err != nil {
		return err
	}
	var generation int64
	err := tx.QueryRow(ctx,
		`SELECT execution_attempt_generation FROM session_runtime_tool_results
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		    AND execution_state IN ('running','consumed') AND cancel_state IN ('pending','submitted') FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, job.ToolUseEventID,
	).Scan(&generation)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var state string
	if err := tx.QueryRow(ctx,
		`SELECT execution_state FROM session_runtime_tool_results
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		    AND execution_attempt_generation=$5 FOR UPDATE`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, job.ToolUseEventID, generation,
	).Scan(&state); err != nil {
		return err
	}
	if state == "consumed" {
		return clearConsumedToolCancellationTx(ctx, tx, job, generation, now)
	}
	return settleSandboxExecutionTx(ctx, tx, SandboxExecutionRef{
		WorkspaceID: job.WorkspaceID, SessionID: job.SessionID, SessionThreadID: job.SessionThreadID, ToolUseEventID: job.ToolUseEventID,
	}, generation, SandboxExecutionSettlement{
		Kind: SandboxExecutionUnknownOutcome, ErrorKind: "sandbox_cancellation_outcome_unknown", SafeMessage: "sandbox cancellation outcome is unknown",
	}, now)
}

// clearConsumedToolCancellationTx closes provider-cancellation custody after
// the conversation terminal writer has consumed the execution. It deliberately
// leaves that writer's Tool result and execution receipt untouched.
func clearConsumedToolCancellationTx(ctx context.Context, tx *dbconnect.Tx, job SandboxToolCancelJob, generation int64, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET cancel_state=NULL, cancel_requested_at=NULL, cancel_submitted_at=NULL,
		        provider_command_reference_json=NULL, updated_at=$6
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
		    AND execution_attempt_generation=$5 AND execution_state='consumed'`,
		job.WorkspaceID, job.SessionID, job.SessionThreadID, job.ToolUseEventID, generation, now.UTC(),
	)
	return err
}
