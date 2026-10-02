package mcpmanifest

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	gatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
)

type manifestFixtureToken struct{}

func (manifestFixtureToken) Token(context.Context) (string, error) {
	return "manifest-owner-token", nil
}

type heldManifestServer struct {
	gatewayv1.UnimplementedMcpConnectorServiceServer
	calls  atomic.Int32
	mu     sync.Mutex
	peers  map[string]bool
	held   chan struct{}
	joined chan struct{}
}

func (s *heldManifestServer) ListMcpTools(ctx context.Context, r *gatewayv1.ListMcpToolsRequest) (*gatewayv1.ListMcpToolsResponse, error) {
	s.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	if len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "bearer manifest-owner-token" {
		return nil, context.Canceled
	}
	remote, _ := peer.FromContext(ctx)
	s.mu.Lock()
	s.peers[remote.Addr.String()] = true
	s.mu.Unlock()
	if r.SessionId == "held" {
		close(s.held)
		<-ctx.Done()
		defer close(s.joined)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &gatewayv1.ListMcpToolsResponse{ManifestEtag: "original", Tools: []*gatewayv1.McpToolDefinition{{Name: "Read", InputSchemaJson: `{}`}}}, nil
}
func TestConnectorListerOwnsOneChannelAndClosesAdmission(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	implementation := &heldManifestServer{peers: map[string]bool{}, held: make(chan struct{}), joined: make(chan struct{})}
	gatewayv1.RegisterMcpConnectorServiceServer(server, implementation)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	lister := NewConnectorLister(listener.Addr().String(), manifestFixtureToken{})
	t.Cleanup(func() {
		if err := lister.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-done
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := lister.ListMCPTools(ctx, ListRequest{WorkspaceID: "workspace", SessionID: "session", MCPServerName: "server"})
			if err != nil || result.ManifestETag != "original" || len(result.Tools) != 1 {
				t.Errorf("manifest=%v/%v", result, err)
			}
		}()
	}
	workers.Wait()
	implementation.mu.Lock()
	peers := len(implementation.peers)
	implementation.mu.Unlock()
	if peers != 1 {
		t.Fatalf("discovery opened %d transports", peers)
	}
	// An accepted business call reaches the server exactly once and a caller
	// timeout joins it. Retaining the channel does not introduce application replay.
	attempt, stop := context.WithTimeout(ctx, 80*time.Millisecond)
	_, err = lister.ListMCPTools(attempt, ListRequest{WorkspaceID: "workspace", SessionID: "held", MCPServerName: "server"})
	stop()
	if err == nil {
		t.Fatal("held manifest call ignored deadline")
	}
	select {
	case <-implementation.joined:
	case <-ctx.Done():
		t.Fatal("manifest handler did not join")
	}
	if implementation.calls.Load() != 21 {
		t.Fatalf("manifest retry count=%d", implementation.calls.Load())
	}
	if err := lister.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lister.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := lister.ListMCPTools(ctx, ListRequest{WorkspaceID: "workspace", SessionID: "session", MCPServerName: "server"}); err == nil {
		t.Fatal("manifest accepted after Close")
	}
	if implementation.calls.Load() != 21 {
		t.Fatal("closed channel dispatched new call")
	}
}
