package integration

import (
	"context"

	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// issuedLeaseQueueFixture returns a previously observed real Queue lease as an
// RPC response. All heartbeat and transition calls still reach the real Queue.
type issuedLeaseQueueFixture struct {
	jobrunner.QueueClient
	job *queuev1.QueueJob
}

func (q *issuedLeaseQueueFixture) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	job := q.job
	q.job = nil
	if job == nil {
		return &queuev1.LeaseResponse{}, nil
	}
	return &queuev1.LeaseResponse{Jobs: []*queuev1.QueueJob{job}}, nil
}

// LeaseJobRunnerJobs returns the issued lease once to the Job Runner.
func (q *issuedLeaseQueueFixture) LeaseJobRunnerJobs(context.Context, *queuev1.LeaseJobRunnerJobsRequest) (*queuev1.LeaseJobRunnerJobsResponse, error) {
	job := q.job
	q.job = nil
	if job == nil {
		return &queuev1.LeaseJobRunnerJobsResponse{RetryAfterMs: 1000}, nil
	}
	return &queuev1.LeaseJobRunnerJobsResponse{Jobs: []*queuev1.QueueJob{job}, RetryAfterMs: 100}, nil
}

func runIssuedLeaseThroughRunner(ctx context.Context, runner *jobrunner.JobRunner, job *queuev1.QueueJob, cfg jobrunner.JobRunnerConfig) error {
	// A separate runner so response replay does not change the next caller's inputs.
	consumer := &jobrunner.JobRunner{
		Queue:              &issuedLeaseQueueFixture{QueueClient: runner.Queue, job: job},
		AcquisitionContext: runner.AcquisitionContext,
		Deliverer:          runner.Deliverer,
		Config:             cfg,
		Logger:             runner.Logger,
	}
	return acquireAndJoinJobRunner(ctx, consumer)
}

// observedRuntimeDeliverer records the result of one actual delivery while the
// production Runner owns heartbeat, finalization and Queue settlement.
type observedRuntimeDeliverer struct {
	jobrunner.RuntimePodDirectDeliverer
	result jobrunner.RuntimeDeliveryResult
	err    error
}

func (d *observedRuntimeDeliverer) DeliverRuntimeJob(ctx context.Context, job jobrunner.RuntimeJob) (jobrunner.RuntimeDeliveryResult, error) {
	d.result, d.err = d.RuntimePodDirectDeliverer.DeliverRuntimeJob(ctx, job)
	return d.result, d.err
}
