package jobrunner

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// slotEventLog orders events recorded by the Queue and Runtime fakes.
type slotEventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *slotEventLog) add(event string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *slotEventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// scriptedAcquisitionQueue answers each LeaseJobRunnerJobs call from a script
// and records the requested capacity.
type scriptedAcquisitionQueue struct {
	*recordingQueueClient
	mu        sync.Mutex
	responses []scriptedLease
	requests  []int32
	events    *slotEventLog
	onLease   func(call int)
}

type scriptedLease struct {
	jobs         []*queuev1.QueueJob
	retryAfterMs int32
	err          error
}

func (q *scriptedAcquisitionQueue) LeaseJobRunnerJobs(_ context.Context, request *queuev1.LeaseJobRunnerJobsRequest) (*queuev1.LeaseJobRunnerJobsResponse, error) {
	q.mu.Lock()
	call := len(q.requests)
	q.requests = append(q.requests, request.GetMaxJobs())
	q.events.add("lease")
	response := scriptedLease{retryAfterMs: int32(time.Hour.Milliseconds())}
	if call < len(q.responses) {
		response = q.responses[call]
	}
	onLease := q.onLease
	q.mu.Unlock()
	if onLease != nil {
		onLease(call)
	}
	if response.err != nil {
		return nil, response.err
	}
	return &queuev1.LeaseJobRunnerJobsResponse{Jobs: response.jobs, RetryAfterMs: response.retryAfterMs}, nil
}

func (q *scriptedAcquisitionQueue) requested() []int32 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int32(nil), q.requests...)
}

// slotDeliverer holds the job of one Thread until released and reports every
// delivery start and completion.
type slotDeliverer struct {
	events    *slotEventLog
	held      string
	release   chan struct{}
	delivered chan string
}

func (d *slotDeliverer) DeliverRuntimeJob(ctx context.Context, job RuntimeJob) (RuntimeDeliveryResult, error) {
	d.delivered <- job.SessionThreadID
	if job.SessionThreadID == d.held {
		select {
		case <-d.release:
		case <-ctx.Done():
			return RuntimeDeliveryResult{}, ctx.Err()
		}
	}
	d.events.add("done:" + job.SessionThreadID)
	return RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}, nil
}

func (*slotDeliverer) ReplayRuntimeDeliveryFinalization(context.Context, RuntimeJob) (RuntimeDeliveryResult, bool, error) {
	return RuntimeDeliveryResult{}, false, nil
}

func slotQueueJob(id string, thread string) *queuev1.QueueJob {
	job := runtimeInputQueueJob()
	job.Id, job.LeaseToken = id, "lease_"+id
	job.PayloadJson = `{"workspace_id":"ws_bridge","session_id":"sesn_1","session_thread_id":"` + thread + `","runtime_input_id":"rin_` + id + `","event_ids":["evt_` + id + `"],"sequence_from":1,"sequence_to":1,"input_kind":"messages"}`
	return job
}

// Trace: with two slots, A keeps running while B finishes; B's completion
// starts the next acquisition for exactly one free slot, and C runs in it
// without waiting for A. No acquisition happens while both slots are busy.
func TestRunJobRunnerLoopRefillsAFreedSlotWhileAnotherJobRuns(t *testing.T) {
	events := &slotEventLog{}
	queueClient := &scriptedAcquisitionQueue{
		recordingQueueClient: &recordingQueueClient{},
		responses: []scriptedLease{
			{jobs: []*queuev1.QueueJob{slotQueueJob("qjob_a", "thr_a"), slotQueueJob("qjob_b", "thr_b")}},
			{jobs: []*queuev1.QueueJob{slotQueueJob("qjob_c", "thr_c")}},
		},
		events: events,
	}
	deliverer := &slotDeliverer{events: events, held: "thr_a", release: make(chan struct{}), delivered: make(chan string, 4)}
	runner := &JobRunner{Queue: queueClient, Deliverer: deliverer, Config: JobRunnerConfig{MaxJobs: 2, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour, DrainTimeout: time.Second}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- RunJobRunnerLoop(ctx, runner, nil, nil) }()
	started := map[string]bool{}
	for len(started) < 3 {
		select {
		case thread := <-deliverer.delivered:
			started[thread] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("deliveries started = %v; want C while A is still held", started)
		}
	}
	requests, order := queueClient.requested(), events.snapshot()
	if !reflect.DeepEqual(requests[:2], []int32{2, 1}) {
		t.Fatalf("requested capacities = %v; want both slots, then the one freed slot", requests)
	}
	secondLease := -1
	bDone := -1
	for index, event := range order {
		if event == "lease" && secondLease < 0 && index > 0 {
			secondLease = index
		}
		if event == "done:thr_b" {
			bDone = index
		}
	}
	if bDone < 0 || secondLease < bDone {
		t.Fatalf("events = %v; want the second acquisition only after B freed its slot", order)
	}
	close(deliverer.release)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunJobRunnerLoop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunJobRunnerLoop did not join")
	}
}

// A response observed after acquisition closed is never dispatched, even when
// returning it through ReleaseUnstartedJob fails.
func TestAcquireAndDispatchReleasesJobsObservedAfterAcquisitionClosed(t *testing.T) {
	acquisition, closeAcquisition := context.WithCancel(context.Background())
	queueClient := &scriptedAcquisitionQueue{
		recordingQueueClient: &recordingQueueClient{releaseErr: errors.New("queue unavailable")},
		responses:            []scriptedLease{{jobs: []*queuev1.QueueJob{runtimeInputQueueJob()}}},
		onLease:              func(int) { closeAcquisition() },
	}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	runner := &JobRunner{Queue: queueClient, Deliverer: deliverer, AcquisitionContext: acquisition, Config: JobRunnerConfig{MaxJobs: 1}}
	result, err := runner.AcquireAndDispatch(context.Background())
	if !errors.Is(err, ErrJobRunnerAcquisitionClosed) || result.Dispatched != 0 || result.Released != 0 {
		t.Fatalf("acquisition after closure = %+v/%v; want no dispatch and a failed release", result, err)
	}
	if err := runner.JoinDispatched(context.Background()); err != nil || len(deliverer.jobs) != 0 {
		t.Fatalf("join = %v with %d deliveries; want none", err, len(deliverer.jobs))
	}
	if got := queueClient.transitionSnapshot(); !reflect.DeepEqual(got, []string{"release:qjob_1"}) {
		t.Fatalf("Queue transitions = %v; want one release attempt", got)
	}
	if _, err := runner.AcquireAndDispatch(context.Background()); !errors.Is(err, ErrJobRunnerAcquisitionClosed) {
		t.Fatalf("later acquisition = %v; want closed", err)
	}
}

// Failed acquisitions back off 100/200/400/800/1000 ms and stay capped; a
// successful empty response waits Queue's hint and resets the backoff; a
// partial response is followed at once by a request for the remaining slots.
func TestRunJobRunnerLoopUsesCappedBackoffAndQueueRetryHint(t *testing.T) {
	failure := errors.New("queue unavailable")
	responses := []scriptedLease{}
	for range 6 {
		responses = append(responses, scriptedLease{err: failure})
	}
	responses = append(responses,
		scriptedLease{retryAfterMs: 250},
		scriptedLease{jobs: []*queuev1.QueueJob{slotQueueJob("qjob_partial", "thr_partial")}},
		scriptedLease{retryAfterMs: 400},
		scriptedLease{err: failure},
	)
	queueClient := &scriptedAcquisitionQueue{recordingQueueClient: &recordingQueueClient{}, responses: responses}
	deliverer := &recordingDeliverer{result: RuntimeDeliveryResult{Status: RuntimeDeliveryAccepted}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delays []time.Duration
	var delaysMu sync.Mutex
	runner := &JobRunner{Queue: queueClient, Deliverer: deliverer, Config: JobRunnerConfig{MaxJobs: 2, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour}}
	runner.waitTimer = func(delay time.Duration) <-chan time.Time {
		delaysMu.Lock()
		delays = append(delays, delay)
		count := len(delays)
		delaysMu.Unlock()
		fired := make(chan time.Time, 1)
		if count < 10 {
			fired <- time.Time{}
		} else {
			cancel()
		}
		return fired
	}
	if err := RunJobRunnerLoop(ctx, runner, nil, nil); err != nil {
		t.Fatalf("RunJobRunnerLoop: %v", err)
	}
	ms := time.Millisecond
	if want := []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, time.Second, time.Second, 250 * ms, 400 * ms, 100 * ms, time.Hour}; !reflect.DeepEqual(delays[:len(want)], want) {
		t.Fatalf("waits = %v; want %v", delays, want)
	}
	requests := queueClient.requested()
	if requests[7] != 2 || requests[8] != 1 {
		t.Fatalf("requested capacities = %v; want a partial response followed at once by the remaining slot", requests)
	}
}
