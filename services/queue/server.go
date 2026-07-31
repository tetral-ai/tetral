package tetralqueue

import (
	"context"
	"math"
	"time"

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
	Heartbeat(context.Context, queue.HeartbeatRequest) (bool, error)
	Ack(context.Context, queue.AckRequest) (bool, error)
	Retry(context.Context, queue.RetryRequest) (bool, error)
	Defer(context.Context, queue.DeferRequest) (bool, error)
	DeadLetter(context.Context, queue.DeadLetterRequest) (bool, error)
	Cancel(context.Context, queue.CancelRequest) (int, error)
}

type Server struct {
	queuev1.UnimplementedQueueServiceServer
	store Store
	now   func() time.Time
}

func NewServer(store Store) *Server {
	return &Server{store: store, now: time.Now}
}

func Register(server *grpc.Server, store Store) {
	queuev1.RegisterQueueServiceServer(server, NewServer(store))
}

func (s *Server) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	leaseDuration, err := positiveMillisDuration(request.GetLeaseDurationMs(), "lease_duration_ms")
	if err != nil {
		return nil, err
	}
	jobs, err := s.store.Lease(ctx, queue.LeaseRequest{
		WorkspaceID:   workspace.ID(request.GetWorkspaceId()),
		Kinds:         request.GetKinds(),
		LeaseOwner:    request.GetLeaseOwner(),
		MaxJobs:       int(request.GetMaxJobs()),
		LeaseDuration: leaseDuration,
		Now:           s.nowUTC(),
	})
	if err != nil {
		return nil, mapQueueError(err)
	}
	response := &queuev1.LeaseResponse{Jobs: make([]*queuev1.QueueJob, 0, len(jobs))}
	for _, job := range jobs {
		response.Jobs = append(response.Jobs, queueJobToProto(job))
	}
	return response, nil
}

func (s *Server) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	if s == nil || s.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue store is required")
	}
	leaseDuration, err := positiveMillisDuration(request.GetLeaseDurationMs(), "lease_duration_ms")
	if err != nil {
		return nil, err
	}
	updated, err := s.store.Heartbeat(ctx, queue.HeartbeatRequest{
		WorkspaceID:   workspace.ID(request.GetWorkspaceId()),
		JobID:         request.GetJobId(),
		LeaseToken:    request.GetLeaseToken(),
		LeaseDuration: leaseDuration,
		Now:           s.nowUTC(),
	})
	return transitionResponse(updated, err)
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
//	                                                                   session_prepare is deferrable    lease-time +1
//	leased -> pending (reclaim)   internal/queue                       status = leased AND              unchanged
//	                              ReclaimExpiredLeases                  leased_until <= now
//
// A lease+defer cycle nets zero budget: leaseCandidate adds one to attempt_count and
// Defer subtracts one. Defer never consults max_attempts, so a session_prepare job
// may defer without limit -- the loop is bounded by the caller's external readiness
// condition, not the attempt budget -- and any other kind is rejected. Reclaim never
// consults max_attempts and never dead-letters on expiry alone: it returns the row to
// pending with available_at = now under last_error_kind = "lease_expired", so the next
// owner re-leases, revalidates the durable business row, and stale-ACKs work a crashed
// consumer already finished. This is the recovery path for runtime_input,
// session_prepare, and cleanup_session jobs stranded by a lost pod. A job reaches
// dead_lettered only through Retry, once attempt_count reaches the effective
// max_attempts, through an explicit DeadLetter, or after a Sandbox consumer
// settles the business outcome and conditionally closes a reclaimed,
// over-budget notification with DeadLetterExhaustedTx.
//
// UPDATE-WITH: internal/queue/postgresql_store.go (Defer, ReclaimExpiredLeases,
// leaseCandidate); services/queue/maintenance.go (RunStalledLeaseMaintenance).
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
	return status.Error(codes.Internal, "queue service operation failed")
}
