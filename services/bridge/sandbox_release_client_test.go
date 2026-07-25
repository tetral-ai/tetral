package agentruntimebridge

import (
	"context"
	"net"
	"sync"
	"testing"

	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	tetralsandboxv1 "github.com/tetral-ai/tetral/services/sandbox/gen/tetral/sandbox/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type sandboxReleaseClientTestTokenSource struct{}

func (sandboxReleaseClientTestTokenSource) Token(context.Context) (string, error) {
	return "test-token", nil
}

var _ internalgrpcauth.TokenSource = sandboxReleaseClientTestTokenSource{}

type sandboxReleaseClientTestServer struct {
	tetralsandboxv1.UnimplementedSandboxServiceServer

	mu       sync.Mutex
	requests []*tetralsandboxv1.ReleaseSandboxRequest
}

func (s *sandboxReleaseClientTestServer) ReleaseSandbox(_ context.Context, request *tetralsandboxv1.ReleaseSandboxRequest) (*tetralsandboxv1.ReleaseSandboxResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, request)
	return &tetralsandboxv1.ReleaseSandboxResponse{
		Status:        tetralsandboxv1.ReleaseSandboxStatus_RELEASE_SANDBOX_STATUS_RELEASED,
		SandboxStatus: "released",
	}, nil
}

func (s *sandboxReleaseClientTestServer) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *sandboxReleaseClientTestServer) lastRequest() *tetralsandboxv1.ReleaseSandboxRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return nil
	}
	return s.requests[len(s.requests)-1]
}

func TestSandboxServiceReleaseClientRejectsUnfencedRuntimePodLossBeforeRPC(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &sandboxReleaseClientTestServer{}
	tetralsandboxv1.RegisterSandboxServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	client := NewSandboxServiceReleaseClient(
		"passthrough:///sandbox-release-client-test",
		sandboxReleaseClientTestTokenSource{},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	_, err := client.ReleaseSandbox(context.Background(), SandboxReleaseRequest{
		WorkspaceID:       "default",
		SessionID:         "sesn_release_unfenced",
		SandboxID:         "sandbox_release_unfenced",
		BindingID:         "bind_release_unfenced",
		BindingGeneration: 9,
		Reason:            "runtime_pod_lost",
		IdempotencyKey:    "runtime_pod_lost:bind_release_unfenced:9",
	})
	if err == nil {
		t.Fatal("unfenced runtime_pod_lost release succeeded; want rejection")
	}
	if got := service.requestCount(); got != 0 {
		t.Fatalf("unfenced runtime_pod_lost RPC count = %d; want 0", got)
	}
}

func TestSandboxServiceReleaseClientPreservesDurablyFencedRuntimePodLossReason(t *testing.T) {
	client, service := newSandboxReleaseClientTransport(t)
	const idempotencyKey = "runtime_pod_lost:bind_release_fenced:11"
	result, err := client.ReleaseSandbox(context.Background(), SandboxReleaseRequest{
		WorkspaceID:         "default",
		SessionID:           "sesn_release_fenced",
		SandboxID:           "sandbox_release_fenced",
		BindingID:           "bind_release_fenced",
		BindingGeneration:   11,
		Reason:              "runtime_pod_lost",
		IdempotencyKey:      idempotencyKey,
		DurableCleanupFence: true,
	})
	if err != nil {
		t.Fatalf("ReleaseSandbox: %v", err)
	}
	if result.Status != SandboxReleaseReleased {
		t.Fatalf("release status = %q; want released", result.Status)
	}
	request := service.lastRequest()
	if request == nil || request.GetReason() != "runtime_pod_lost" || request.GetIdempotencyKey() != idempotencyKey {
		t.Fatalf("wire request = %#v; want runtime_pod_lost with idempotency key %q", request, idempotencyKey)
	}
}

func TestSandboxServiceReleaseClientKeepsOrdinaryCleanupIdentity(t *testing.T) {
	client, service := newSandboxReleaseClientTransport(t)
	const idempotencyKey = "cleanup_session:cleanup_job_ordinary"
	_, err := client.ReleaseSandbox(context.Background(), SandboxReleaseRequest{
		WorkspaceID:         "default",
		SessionID:           "sesn_release_cleanup",
		SandboxID:           "sandbox_release_cleanup",
		BindingID:           "bind_release_cleanup",
		BindingGeneration:   5,
		Reason:              "cleanup",
		IdempotencyKey:      idempotencyKey,
		DurableCleanupFence: true,
	})
	if err != nil {
		t.Fatalf("ReleaseSandbox: %v", err)
	}
	request := service.lastRequest()
	if request == nil || request.GetReason() != "cleanup" || request.GetIdempotencyKey() != idempotencyKey {
		t.Fatalf("wire request = %#v; want ordinary cleanup with idempotency key %q", request, idempotencyKey)
	}
}

func newSandboxReleaseClientTransport(t *testing.T) (*SandboxServiceReleaseClient, *sandboxReleaseClientTestServer) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service := &sandboxReleaseClientTestServer{}
	tetralsandboxv1.RegisterSandboxServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return NewSandboxServiceReleaseClient(
		"passthrough:///sandbox-release-client-test",
		sandboxReleaseClientTestTokenSource{},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	), service
}
