package tetralsandbox

import (
	"context"
	"errors"

	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type SandboxQueueClient interface {
	Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error)
	Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error)
	Ack(context.Context, *queuev1.AckRequest) (*queuev1.TransitionResponse, error)
	Retry(context.Context, *queuev1.RetryRequest) (*queuev1.TransitionResponse, error)
	Defer(context.Context, *queuev1.DeferRequest) (*queuev1.TransitionResponse, error)
	DeadLetter(context.Context, *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error)
}

type sandboxQueueClientAdapter struct {
	client queuev1.QueueServiceClient
}

func SandboxQueueFromGRPC(client queuev1.QueueServiceClient) SandboxQueueClient {
	return sandboxQueueClientAdapter{client: client}
}

func (a sandboxQueueClientAdapter) Lease(ctx context.Context, request *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	return a.client.Lease(ctx, request)
}

func (a sandboxQueueClientAdapter) Heartbeat(ctx context.Context, request *queuev1.HeartbeatRequest) (*queuev1.HeartbeatResponse, error) {
	return a.client.Heartbeat(ctx, request)
}

func (a sandboxQueueClientAdapter) Ack(ctx context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Ack(ctx, request)
}

func (a sandboxQueueClientAdapter) Retry(ctx context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Retry(ctx, request)
}

func (a sandboxQueueClientAdapter) Defer(ctx context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	return a.client.Defer(ctx, request)
}

func (a sandboxQueueClientAdapter) DeadLetter(ctx context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	return a.client.DeadLetter(ctx, request)
}

func transitionUpdated(response *queuev1.TransitionResponse, err error) error {
	if err != nil {
		return err
	}
	if response != nil && !response.GetUpdated() {
		return errors.New("queue transition was not applied")
	}
	return nil
}
