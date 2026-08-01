package tetralsandbox

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

type recordingSandboxQueue struct {
	leased        []*queuev1.QueueJob
	transitions   []string
	heartbeatLost bool
	heartbeatErr  error
}

func (q *recordingSandboxQueue) Lease(context.Context, *queuev1.LeaseRequest) (*queuev1.LeaseResponse, error) {
	jobs := q.leased
	q.leased = nil
	return &queuev1.LeaseResponse{Jobs: jobs}, nil
}

func (q *recordingSandboxQueue) Heartbeat(context.Context, *queuev1.HeartbeatRequest) (*queuev1.TransitionResponse, error) {
	if q.heartbeatErr != nil {
		return nil, q.heartbeatErr
	}
	return &queuev1.TransitionResponse{Updated: !q.heartbeatLost}, nil
}

func (q *recordingSandboxQueue) Ack(_ context.Context, request *queuev1.AckRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "ack:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) Retry(_ context.Context, request *queuev1.RetryRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "retry:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) Defer(_ context.Context, request *queuev1.DeferRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "defer:"+request.GetJobId())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func (q *recordingSandboxQueue) DeadLetter(_ context.Context, request *queuev1.DeadLetterRequest) (*queuev1.TransitionResponse, error) {
	q.transitions = append(q.transitions, "dead:"+request.GetJobId()+":"+request.GetErrorKind())
	return &queuev1.TransitionResponse{Updated: true}, nil
}

func newSandboxServiceTestDB(t *testing.T) (runtime *sql.DB, admin *sql.DB) {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	return storagetest.NewPostgreSQLDBWithAdmin(t)
}
