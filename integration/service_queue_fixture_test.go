package integration

import (
	"context"

	"github.com/tetral-ai/tetral/internal/workspace"
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
func runIssuedLeaseThroughRunner(ctx context.Context, runner *jobrunner.JobRunner, job *queuev1.QueueJob, cfg jobrunner.JobRunnerConfig) error {
	// Use a value copy so response replay does not change the next caller's inputs.
	consumer := *runner
	consumer.Queue = &issuedLeaseQueueFixture{QueueClient: runner.Queue, job: job}
	consumer.Workspaces = staticWorkspaceLister{workspace.ID(job.GetWorkspaceId())}
	consumer.Config = cfg
	return consumer.RunOnce(ctx)
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
