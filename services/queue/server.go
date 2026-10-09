package tetralqueue

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/tetral-ai/tetral/internal/workload"

	"github.com/tetral-ai/tetral/internal/storage"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store interface {
	Lease(context.Context, queue.LeaseRequest) ([]*queue.Job, error)
	LeaseJobRunnerJobs(context.Context, queue.LeaseJobRunnerJobsRequest) (queue.LeaseJobRunnerJobsResult, error)
	ReleaseUnstartedJob(context.Context, queue.ReleaseUnstartedJobRequest) (bool, error)
	Heartbeat(context.Context, queue.HeartbeatRequest) (queue.HeartbeatResult, error)
	Ack(context.Context, queue.AckRequest) (bool, error)
	Retry(context.Context, queue.RetryRequest) (bool, error)
	Defer(context.Context, queue.DeferRequest) (bool, error)
	DeadLetter(context.Context, queue.DeadLetterRequest) (bool, error)
	Cancel(context.Context, queue.CancelRequest) (int, error)
}

type Server struct {
	queuev1.UnimplementedQueueServiceServer
	store  Store
	now    func() time.Time
	logger *slog.Logger
}

func NewServer(store Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = workload.ComponentLogger("queue")
	}
	return &Server{store: store, now: time.Now, logger: logger}
}

func Register(server *grpc.Server, store Store, logger *slog.Logger) {
	queuev1.RegisterQueueServiceServer(server, NewServer(store, logger))
}

func (s *Server) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	leaseDuration, err := positiveMillisDuration(request.GetLeaseDurationMs(), "lease_duration_ms")
	if err != nil {
		return nil, err
	}
	leaseStarted := s.nowUTC()
	leaseNow := leaseStarted
	jobs, err := s.store.Lease(ctx, queue.LeaseRequest{
		WorkspaceID:   workspace.ID(request.GetWorkspaceId()),
		Kinds:         request.GetKinds(),
		LeaseOwner:    request.GetLeaseOwner(),
		MaxJobs:       int(request.GetMaxJobs()),
		LeaseDuration: leaseDuration,
		Now:           leaseNow,
	})
	if err != nil {
		return nil, mapQueueError(err)
	}
	leaseCompleted := s.nowUTC()
	response := &queuev1.LeaseResponse{Jobs: make([]*queuev1.QueueJob, 0, len(jobs))}
	for _, job := range jobs {
		s.logLease("queue.lease", job, leaseNow, leaseStarted, leaseCompleted)
		response.Jobs = append(response.Jobs, queueJobToProto(job))
	}
	return response, nil
}

// LeaseJobRunnerJobs leases Job Runner work across workspaces. Queue owns
// tenant selection, kind admission and the retry hint; the caller supplies
// only capacity, its label and a lease duration in 5000..300000 ms.
func (s *Server) LeaseJobRunnerJobs(ctx context.Context, request *queuev1.LeaseJobRunnerJobsRequest) (*queuev1.LeaseJobRunnerJobsResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	leaseDuration, err := queue.JobRunnerLeaseDurationFromMillis(request.GetLeaseDurationMs())
	if err != nil {
		return nil, mapQueueError(err)
	}
	leaseRequest := queue.LeaseJobRunnerJobsRequest{
		LeaseOwner:    request.GetLeaseOwner(),
		MaxJobs:       int(request.GetMaxJobs()),
		LeaseDuration: leaseDuration,
	}
	if err := queue.ValidateLeaseJobRunnerJobsRequest(leaseRequest); err != nil {
		return nil, mapQueueError(err)
	}
	leaseStarted := s.nowUTC()
	result, err := s.store.LeaseJobRunnerJobs(ctx, leaseRequest)
	if err != nil {
		return nil, mapQueueError(err)
	}
	leaseCompleted := s.nowUTC()
	s.logJobRunnerLeaseDiagnostics(result)
	response := &queuev1.LeaseJobRunnerJobsResponse{
		Jobs:         make([]*queuev1.QueueJob, 0, len(result.Jobs)),
		RetryAfterMs: int32(result.RetryAfter.Milliseconds()),
	}
	for _, job := range result.Jobs {
		s.logLease("queue.lease_job_runner_jobs", job, leaseStarted, leaseStarted, leaseCompleted)
		response.Jobs = append(response.Jobs, queueJobToProto(job))
	}
	return response, nil
}

// logJobRunnerLeaseDiagnostics emits at most three bounded records per call
// with fixed classes and counts; repeated warnings aggregate in the process
// diagnostic limiter.
func (s *Server) logJobRunnerLeaseDiagnostics(result queue.LeaseJobRunnerJobsResult) {
	if s == nil || s.logger == nil {
		return
	}
	for _, timeout := range []struct {
		kind  string
		count int
	}{{"lock_timeout", result.LockTimeouts}, {"statement_timeout", result.StatementTimeouts}} {
		if timeout.count == 0 {
			continue
		}
		s.logger.Warn("queue.job_runner_lease.candidate_timeout",
			slog.String("operation", "queue.lease_job_runner_jobs"),
			slog.String("event.kind", "queue.job_runner_lease.candidate_timeout"),
			slog.String("component", "queue"),
			slog.String("timeout.kind", timeout.kind),
			slog.Int("failed.count", timeout.count),
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
		)
	}
	if result.StopFailure != nil {
		attrs := []any{
			slog.String("operation", "queue.lease_job_runner_jobs"),
			slog.String("event.kind", "queue.job_runner_lease.partial_failure"),
			slog.String("component", "queue"),
			slog.Int("candidate.count", len(result.Jobs)),
			slog.Bool("retryable", true),
			slog.Bool("terminal", false),
			slog.String("error.class", "queue_lease_error"),
			slog.String("error.code", "job_runner_lease_stopped"),
			slog.String("error.message_safe", "queue job runner lease stopped after committed leases"),
		}
		var pgErr *pgconn.PgError
		if errors.As(result.StopFailure, &pgErr) {
			attrs = append(attrs, slog.String("db.sqlstate", pgErr.Code))
		}
		s.logger.Warn("queue.job_runner_lease.partial_failure", attrs...)
	}
}

// ReleaseUnstartedJob returns one observed but undispatched direct Job Runner
// lease to pending with its saved attempt count.
func (s *Server) ReleaseUnstartedJob(ctx context.Context, request *queuev1.ReleaseUnstartedJobRequest) (*queuev1.ReleaseUnstartedJobResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	updated, err := s.store.ReleaseUnstartedJob(ctx, queue.ReleaseUnstartedJobRequest{
		WorkspaceID: workspace.ID(request.GetWorkspaceId()),
		JobID:       request.GetJobId(),
		LeaseToken:  request.GetLeaseToken(),
	})
	if err != nil {
		return nil, mapQueueError(err)
	}
	return &queuev1.ReleaseUnstartedJobResponse{Updated: updated}, nil
}

func (s *Server) logLease(operation string, job *queue.Job, leaseNow, leaseStarted, leaseCompleted time.Time) {
	if s == nil || s.logger == nil || job == nil {
		return
	}
	duration := leaseCompleted.Sub(leaseStarted)
	if duration < 0 {
		duration = 0
	}
	readyWait := leaseNow.Sub(job.AvailableAt)
	if readyWait < 0 {
		readyWait = 0
	}
	s.logger.Debug("queue.job.leased",
		slog.String("operation", operation),
		slog.String("event.kind", "queue_job_leased"),
		slog.String("workspace.id", job.WorkspaceID.String()),
		slog.String("queue.job.id", job.ID),
		slog.String("queue.job.kind", job.Kind),
		slog.String("queue.partition.key", job.PartitionKey),
		slog.Int64("duration.ms", duration.Milliseconds()),
		slog.Int64("queue.ready_wait.ms", readyWait.Milliseconds()),
	)
}

func (s *Server) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	leaseDuration, err := positiveMillisDuration(request.GetLeaseDurationMs(), "lease_duration_ms")
	if err != nil {
		return nil, err
	}
	result, err := s.store.Heartbeat(ctx, queue.HeartbeatRequest{
		WorkspaceID:   workspace.ID(request.GetWorkspaceId()),
		JobID:         request.GetJobId(),
		LeaseToken:    request.GetLeaseToken(),
		LeaseDuration: leaseDuration,
	})
	if err != nil {
		return nil, mapQueueError(err)
	}
	response := &queuev1.HeartbeatResponse{Updated: result.Updated}
	if result.Updated {
		response.LeasedUntil = result.LeasedUntil.UTC().Format(time.RFC3339Nano)
	}
	return response, nil
}

func (s *Server) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	updated, err := s.store.Ack(ctx, queue.AckRequest{
		WorkspaceID: workspace.ID(request.GetWorkspaceId()),
		JobID:       request.GetJobId(),
		LeaseToken:  request.GetLeaseToken(),
		Now:         s.nowUTC(),
	})
	return transitionResponse(updated, err)
}

func (s *Server) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	updated, err := s.store.Retry(ctx, queue.RetryRequest{
		WorkspaceID:  workspace.ID(request.GetWorkspaceId()),
		JobID:        request.GetJobId(),
		LeaseToken:   request.GetLeaseToken(),
		ErrorKind:    request.GetErrorKind(),
		ErrorMessage: request.GetErrorMessage(),
		Now:          s.nowUTC(),
	})
	return transitionResponse(updated, err)
}

// Defer is the queue's voluntary WAITING transition; the lease-reclaim loop is its
// involuntary recovery counterpart. Both move a job off `leased` back to `pending`.
// Defer is lease-token fenced, so a stale owner cannot disturb a row it no longer
// holds; reclaim is gated on lease expiry instead of a token.
//
//	transition                   writer (code path)                   guard                            attempt_count
//	---------------------------  -----------------------------------  -------------------------------  -------------------
//	leased -> pending (defer)     internal/queue Defer, via this RPC   lease_token match; only          -1, cancelling the
//	                                                                   runtime_config_update            lease-time +1
//	leased -> pending (reclaim)   internal/queue                       status = leased AND              unchanged
//	                              ReclaimExpiredLeases                  leased_until <= now
//
// A lease+defer cycle nets zero budget: a token mint (Lease or
// LeaseJobRunnerJobs) adds one to attempt_count and Defer subtracts one; Defer
// also clears direct-lease provenance, so a deferred token cannot be released. Defer never consults max_attempts, so a runtime configuration
// update may wait on an active Session without approaching its retry budget; every
// other kind is rejected. Reclaim never
// consults max_attempts and never dead-letters on expiry alone: it returns the row to
// pending with available_at = now under last_error_kind = "lease_expired", so the next
// owner re-leases, revalidates the durable business row, and stale-ACKs work a crashed
// consumer already finished. This is the recovery path for runtime_input,
// runtime_config_update, cleanup_session, and Sandbox jobs stranded by a lost worker. A job reaches
// dead_lettered only through Retry, once attempt_count reaches the effective
// max_attempts, through an explicit DeadLetter, or after a Sandbox consumer
// settles the business outcome and conditionally closes a reclaimed,
// over-budget notification with DeadLetterExhaustedTx.
//
// UPDATE-WITH: internal/queue/postgresql_store.go (Defer, ReclaimExpiredLeases,
// leaseCandidate, leaseAttemptCountExpression); internal/queue/job_runner_lease.go
// (leaseJobRunnerCandidate); services/queue/maintenance.go (runStalledLeaseMaintenance).
func (s *Server) Defer(ctx context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	updated, err := s.store.Defer(ctx, queue.DeferRequest{
		WorkspaceID: workspace.ID(request.GetWorkspaceId()),
		JobID:       request.GetJobId(),
		LeaseToken:  request.GetLeaseToken(),
		Now:         s.nowUTC(),
	})
	return transitionResponse(updated, err)
}

func (s *Server) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	updated, err := s.store.DeadLetter(ctx, queue.DeadLetterRequest{
		WorkspaceID:  workspace.ID(request.GetWorkspaceId()),
		JobID:        request.GetJobId(),
		LeaseToken:   request.GetLeaseToken(),
		ErrorKind:    request.GetErrorKind(),
		ErrorMessage: request.GetErrorMessage(),
		Now:          s.nowUTC(),
	})
	return transitionResponse(updated, err)
}

func (s *Server) Cancel(ctx context.Context, request *queuev1.CancelRequest) (*queuev1.CancelResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	cancelled, err := s.store.Cancel(ctx, queue.CancelRequest{
		WorkspaceID:            workspace.ID(request.GetWorkspaceId()),
		SessionID:              request.GetSessionId(),
		SessionThreadID:        request.GetSessionThreadId(),
		InterruptFenceSequence: request.GetInterruptFenceSequence(),
		Now:                    s.nowUTC(),
	})
	if err != nil {
		return nil, mapQueueError(err)
	}
	return &queuev1.CancelResponse{CancelledCount: int32(cancelled)}, nil
}

func (s *Server) nowUTC() time.Time {
	if s != nil && s.now != nil {
		return s.now().UTC()
	}
	return storage.Now()
}

func transitionResponse(updated bool, err error) (*queuev1.TransitionResponse, error) {
	if err != nil {
		return nil, mapQueueError(err)
	}
	return &queuev1.TransitionResponse{Updated: updated}, nil
}

func queueJobToProto(job *queue.Job) *queuev1.QueueJob {
	if job == nil {
		return nil
	}
	return &queuev1.QueueJob{
		Id:             job.ID,
		WorkspaceId:    string(job.WorkspaceID),
		Kind:           job.Kind,
		PartitionKey:   job.PartitionKey,
		DedupeKey:      job.DedupeKey,
		PayloadVersion: int32(job.PayloadVersion),
		PayloadJson:    string(job.PayloadJSON),
		Status:         job.Status,
		Priority:       int32(job.Priority),
		AvailableAt:    formatOptionalTime(&job.AvailableAt),
		LeasedBy:       job.LeasedBy,
		LeaseToken:     job.LeaseToken,
		LeasedAt:       formatOptionalTime(job.LeasedAt),
		LeasedUntil:    formatOptionalTime(job.LeasedUntil),
		AttemptCount:   int32(job.AttemptCount),
		MaxAttempts:    int32(job.MaxAttempts),
	}
}

func positiveMillisDuration(value int64, field string) (time.Duration, error) {
	if value <= 0 {
		return 0, status.Error(codes.InvalidArgument, field+" must be positive")
	}
	if value > int64(math.MaxInt64)/int64(time.Millisecond) {
		return 0, status.Error(codes.InvalidArgument, field+" is too large")
	}
	return time.Duration(value) * time.Millisecond, nil
}

func formatOptionalTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func mapQueueError(err error) error {
	if err == nil {
		return nil
	}
	if queue.IsValidationError(err) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if queue.IsPreconditionError(err) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, queue.ErrJobRunnerSchedulerClosed) {
		return status.Error(codes.Unavailable, "queue job runner scheduler is draining")
	}
	return status.Error(codes.Internal, "queue service operation failed")
}
