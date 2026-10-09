package jobrunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// Acquisition timing is fixed by the Runner/Queue contract, not configuration.
const (
	// A LeaseJobRunnerJobs call may take Queue's one-second budget plus
	// serialization and transport.
	jobRunnerLeaseRPCDeadline = 2 * time.Second
	// Each undispatched lease returned at quiesce gets its own deadline.
	jobRunnerReleaseRPCDeadline = time.Second
	// A response without a usable hint waits this long before asking again.
	jobRunnerDefaultRetryAfter = time.Second
)

// jobRunnerAcquisitionBackoff spaces consecutive failed acquisitions; any
// successful response resets it.
var jobRunnerAcquisitionBackoff = []time.Duration{
	100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second,
}

// ErrJobRunnerAcquisitionClosed reports that acquisition closed; jobs observed
// afterwards were returned through ReleaseUnstartedJob instead of dispatched.
var ErrJobRunnerAcquisitionClosed = errors.New("job runner acquisition is closed")

// errJobRunnerAcquisitionInFlight rejects a second concurrent acquisition.
var errJobRunnerAcquisitionInFlight = errors.New("job runner acquisition is already in flight")

// jobRunnerSlots owns MaxJobs execution slots. A slot moves FREE -> RESERVED
// (before one Queue request) -> ACTIVE (a returned job runs in it) -> JOINING
// (the job, its heartbeat and its final Queue disposition finished) -> FREE
// (the coordinator collected the completion). Unused reservations return to
// FREE. At most one acquisition is in flight, and a job is dispatched only
// while acquisition is open, decided under the same mutex that closes it.
type jobRunnerSlots struct {
	mu sync.Mutex
	// acquisition, when set, closes acquisition once done. It is read under mu
	// at every reservation and dispatch decision, so a response that arrives
	// after cancellation is never dispatched.
	acquisition context.Context
	capacity    int
	free        int
	reserved    int
	active      int
	joining     int
	inflight    bool
	closed      bool
	closedCh    chan struct{}
	completed   chan error
}

var jobRunnerSlotsInit sync.Mutex

func newJobRunnerSlots(capacity int) *jobRunnerSlots {
	return &jobRunnerSlots{
		capacity:  capacity,
		free:      capacity,
		closedCh:  make(chan struct{}),
		completed: make(chan error, capacity),
	}
}

// jobSlots returns the runner's coordinator, creating it on first use with the
// effective MaxJobs and linking acquisition closure to AcquisitionContext.
func (r *JobRunner) jobSlots(capacity int) *jobRunnerSlots {
	jobRunnerSlotsInit.Lock()
	defer jobRunnerSlotsInit.Unlock()
	if r.slots == nil {
		r.slots = newJobRunnerSlots(capacity)
		if r.AcquisitionContext != nil {
			slots := r.slots
			slots.acquisition = r.AcquisitionContext
			// Wakes a waiting coordinator; decisions also check the context.
			context.AfterFunc(r.AcquisitionContext, slots.close)
		}
	}
	return r.slots
}

func (s *jobRunnerSlots) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeLocked()
}

func (s *jobRunnerSlots) closeLocked() {
	if !s.closed {
		s.closed = true
		close(s.closedCh)
	}
}

// isClosedLocked reports closure, observing acquisition cancellation
// synchronously rather than waiting for its asynchronous notification.
func (s *jobRunnerSlots) isClosedLocked() bool {
	if !s.closed && s.acquisition != nil && s.acquisition.Err() != nil {
		s.closeLocked()
	}
	return s.closed
}

// reserveFree reserves every free slot for one acquisition. It returns zero
// when no slot is free.
func (s *jobRunnerSlots) reserveFree() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isClosedLocked() {
		return 0, ErrJobRunnerAcquisitionClosed
	}
	if s.inflight {
		return 0, errJobRunnerAcquisitionInFlight
	}
	if s.free == 0 {
		return 0, nil
	}
	reserved := s.free
	s.free, s.reserved, s.inflight = 0, reserved, true
	return reserved, nil
}

// dispatch moves count reserved slots to ACTIVE and frees the rest, unless
// acquisition has closed; then every reservation is freed and false returned.
func (s *jobRunnerSlots) dispatch(reserved int, count int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved -= reserved
	s.inflight = false
	if s.isClosedLocked() {
		s.free += reserved
		return false
	}
	s.active += count
	s.free += reserved - count
	return true
}

func (s *jobRunnerSlots) unreserve(reserved int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserved -= reserved
	s.free += reserved
	s.inflight = false
}

func (s *jobRunnerSlots) finish(err error) {
	s.mu.Lock()
	s.active--
	s.joining++
	s.mu.Unlock()
	s.completed <- err
}

func (s *jobRunnerSlots) collect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joining--
	s.free++
}

func (s *jobRunnerSlots) snapshot() (free, active, joining int, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.free, s.active, s.joining, s.isClosedLocked()
}

// JobRunnerAcquisition reports one acquisition: jobs dispatched into slots,
// observed jobs returned through ReleaseUnstartedJob because acquisition had
// closed, and Queue's retry hint for when no job was returned.
type JobRunnerAcquisition struct {
	Dispatched int
	Released   int
	RetryAfter time.Duration
	wake       queue.WakeSnapshot
}

func (r *JobRunner) effectiveConfig() JobRunnerConfig {
	cfg := r.Config
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = defaultJobRunnerLeaseOwner
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
	return cfg
}

// AcquireAndDispatch reserves every free slot, snapshots the wake generation,
// makes one LeaseJobRunnerJobs request for that capacity, and dispatches each
// returned job into its own slot, where processRuntimeJob runs under ctx. If
// acquisition closed while the request was in flight, the observed jobs are
// returned through ReleaseUnstartedJob instead. It does not wait for
// dispatched jobs; JoinDispatched and the slot-completion path own that.
func (r *JobRunner) AcquireAndDispatch(ctx context.Context) (JobRunnerAcquisition, error) {
	if r == nil || r.Queue == nil {
		return JobRunnerAcquisition{}, errors.New("job runner queue client is required")
	}
	if r.Deliverer == nil {
		return JobRunnerAcquisition{}, errors.New("job runner runtime job deliverer is required")
	}
	cfg := r.effectiveConfig()
	slots := r.jobSlots(cfg.MaxJobs)
	reserved, err := slots.reserveFree()
	if err != nil || reserved == 0 {
		return JobRunnerAcquisition{}, err
	}
	acquisition := JobRunnerAcquisition{wake: r.wake.Snapshot()}
	rpcCtx, cancel := context.WithTimeout(ctx, jobRunnerLeaseRPCDeadline)
	response, err := r.Queue.LeaseJobRunnerJobs(rpcCtx, &queuev1.LeaseJobRunnerJobsRequest{
		MaxJobs:         int32(reserved),
		LeaseOwner:      cfg.LeaseOwner,
		LeaseDurationMs: cfg.LeaseDuration.Milliseconds(),
	})
	cancel()
	if err != nil {
		slots.unreserve(reserved)
		return acquisition, err
	}
	jobs := response.GetJobs()
	acquisition.RetryAfter = time.Duration(response.GetRetryAfterMs()) * time.Millisecond
	if len(jobs) > reserved {
		slots.unreserve(reserved)
		acquisition.Released = r.releaseUnstarted(ctx, jobs, cfg.MaxJobs)
		return acquisition, fmt.Errorf("queue returned %d jobs for %d free slots", len(jobs), reserved)
	}
	if !slots.dispatch(reserved, len(jobs)) {
		acquisition.Released = r.releaseUnstarted(ctx, jobs, cfg.MaxJobs)
		return acquisition, ErrJobRunnerAcquisitionClosed
	}
	for _, job := range jobs {
		go r.runSlot(ctx, job, cfg, slots)
	}
	acquisition.Dispatched = len(jobs)
	return acquisition, nil
}

func (r *JobRunner) runSlot(ctx context.Context, job *queuev1.QueueJob, cfg JobRunnerConfig, slots *jobRunnerSlots) {
	err := r.processRuntimeJob(ctx, job, cfg)
	if err != nil && ctx.Err() == nil {
		r.logRuntimeJobFailure(job)
	}
	slots.finish(err)
}

// JoinDispatched waits until every dispatched job has finished and its slot is
// free again, and returns the jobs' errors. The loop's drain uses it after
// acquisition closes.
func (r *JobRunner) JoinDispatched(ctx context.Context) error {
	if r == nil {
		return nil
	}
	jobRunnerSlotsInit.Lock()
	slots := r.slots
	jobRunnerSlotsInit.Unlock()
	if slots == nil {
		return nil
	}
	var errs []error
	for {
		_, active, joining, _ := slots.snapshot()
		if active+joining == 0 {
			return errors.Join(errs...)
		}
		select {
		case err := <-slots.completed:
			slots.collect()
			if err != nil {
				errs = append(errs, err)
			}
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
}

// releaseUnstarted returns observed jobs that were never dispatched, at most
// limit at a time, each under its own deadline. A failed release is left to
// lease expiry; the job is never dispatched afterwards.
func (r *JobRunner) releaseUnstarted(ctx context.Context, jobs []*queuev1.QueueJob, limit int) int {
	if len(jobs) == 0 {
		return 0
	}
	var (
		wait     sync.WaitGroup
		mu       sync.Mutex
		released int
	)
	permits := make(chan struct{}, max(limit, 1))
	for _, job := range jobs {
		permits <- struct{}{}
		wait.Add(1)
		go func(job *queuev1.QueueJob) {
			defer wait.Done()
			defer func() { <-permits }()
			releaseCtx, cancel := context.WithTimeout(ctx, jobRunnerReleaseRPCDeadline)
			response, err := r.Queue.ReleaseUnstartedJob(releaseCtx, &queuev1.ReleaseUnstartedJobRequest{
				WorkspaceId: job.GetWorkspaceId(), JobId: job.GetId(), LeaseToken: job.GetLeaseToken(),
			})
			cancel()
			if err != nil {
				r.logReleaseFailure(job)
				return
			}
			if response.GetUpdated() {
				mu.Lock()
				released++
				mu.Unlock()
			}
		}(job)
	}
	wait.Wait()
	return released
}

// waitForAcquisition waits until the next acquisition should start: the delay
// elapses, a wake broadcast follows snapshot, a slot completes, or acquisition
// closes. It reports false when acquisition closed or ctx ended.
func (r *JobRunner) waitForAcquisition(ctx context.Context, slots *jobRunnerSlots, delay time.Duration, snapshot queue.WakeSnapshot) bool {
	var timer <-chan time.Time
	switch {
	case delay <= 0:
	case r.waitTimer != nil:
		timer = r.waitTimer(delay)
	default:
		t := time.NewTimer(delay)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-timer:
	case <-snapshot.Ready():
	case <-slots.completed:
		// The slot itself logged a failure; collecting frees it for refill.
		slots.collect()
	case <-slots.closedCh:
		return false
	case <-ctx.Done():
		return false
	}
	return true
}

func (r *JobRunner) logRuntimeJobFailure(job *queuev1.QueueJob) {
	if r == nil || r.Logger == nil {
		return
	}
	defer func() { _ = recover() }()
	r.Logger.Warn("job_runner.runtime_job.failed",
		slog.String("operation", "job_runner.runtime_job"),
		slog.String("event.kind", "runtime_job_failed"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("workspace.id", job.GetWorkspaceId()),
		slog.String("queue.job.id", job.GetId()),
		slog.String("queue.job.kind", job.GetKind()),
		slog.Bool("retryable", true),
		slog.Bool("terminal", false),
		slog.String("error.class", "job_runner_error"),
		slog.String("error.code", "runtime_job_failed"),
		slog.String("error.message_safe", "job runner runtime job failed"),
	)
}

func (r *JobRunner) logReleaseFailure(job *queuev1.QueueJob) {
	if r == nil || r.Logger == nil {
		return
	}
	defer func() { _ = recover() }()
	r.Logger.Warn("job_runner.release_unstarted.failed",
		slog.String("operation", "job_runner.release_unstarted"),
		slog.String("event.kind", "release_unstarted_failed"),
		slog.String("component", ServiceNameJobRunner),
		slog.String("workspace.id", job.GetWorkspaceId()),
		slog.String("queue.job.id", job.GetId()),
		slog.Bool("retryable", false),
		slog.Bool("terminal", false),
		slog.String("error.class", "job_runner_error"),
		slog.String("error.code", "release_unstarted_failed"),
		slog.String("error.message_safe", "undispatched queue lease was left to expire"),
	)
}
