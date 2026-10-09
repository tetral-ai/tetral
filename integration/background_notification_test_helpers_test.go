package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/tetral-ai/tetral/internal/queue"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	tetralsandbox "github.com/tetral-ai/tetral/services/sandbox"
)

func startBackgroundNotificationQueueServer(t *testing.T, store *queue.PostgreSQLQueueStore) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(2 * 1024 * 1024)
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, tetralqueue.NewServer(store, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient("passthrough:///background-notification-queue",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatalf("dial background notification Queue: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

type backgroundNotificationMedia struct{}

func (backgroundNotificationMedia) MaterializeResult(_ context.Context, _ tetralsandbox.SandboxExecutionRef, _, _, result string, _ time.Time) (string, error) {
	return result, nil
}

func (backgroundNotificationMedia) RecoverResult(context.Context, tetralsandbox.SandboxExecutionRef) (tetralsandbox.SandboxMediaRecovery, error) {
	return tetralsandbox.SandboxMediaRecovery{}, nil
}
