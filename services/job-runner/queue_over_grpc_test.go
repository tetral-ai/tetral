package jobrunner

import (
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

// queueOverGRPC is the Queue service as Job Runner reaches it when deployed:
// the Queue's server over store on a listener with the Queue's server
// options, and the generated client with the Queue's dial options.
func queueOverGRPC(t *testing.T, store tetralqueue.Store) QueueClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for the Queue service: %v", err)
	}
	server := grpc.NewServer(internalgrpc.QueueRPCServerOptions()...)
	tetralqueue.Register(server, store, nil)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(),
		append(internalgrpc.QueueRPCDialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatalf("connect to the Queue service: %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		server.Stop()
	})
	return QueueClientFromGRPC(queuev1.NewQueueServiceClient(connection))
}
