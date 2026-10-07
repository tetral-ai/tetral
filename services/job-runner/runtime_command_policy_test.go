package jobrunner

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
)

type policyRuntimeServer struct {
	agentruntimev1.UnimplementedAgentRuntimePodServiceServer
}

type channelFixtureToken struct{}

func (channelFixtureToken) Token(context.Context) (string, error) {
	return "channel-fixture-token", nil
}

func TestRuntimeCommandTrustRetirementDrainsAdmittedCall(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	firstEntered, secondEntered, releaseFirst := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	calls := 0
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		mu.Lock()
		calls++
		index := calls
		mu.Unlock()
		if index == 1 {
			close(firstEntered)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			close(secondEntered)
		}
		return &agentruntimev1.AcceptInputResponse{}, nil
	}))
	agentruntimev1.RegisterAgentRuntimePodServiceServer(server, &policyRuntimeServer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	_, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	client := NewRuntimePodCommandClient(channelFixtureToken{})
	t.Cleanup(func() { _ = client.Close() })
	target := RuntimePodTarget{PodIP: "127.0.0.1", Port: port}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() {
		_, err := client.AcceptInput(ctx, target, &agentruntimev1.AcceptInputRequest{})
		firstDone <- err
	}()
	select {
	case <-firstEntered:
	case <-ctx.Done():
		t.Fatal("first channel did not admit actual RPC")
	}
	client.mutex.Lock()
	var old *runtimeCommandChannel
	for _, channel := range client.channels {
		old = channel
	}
	client.mutex.Unlock()
	client.RetireChannels()
	if old == nil || old.connection.GetState() == connectivity.Shutdown {
		t.Fatal("trust retirement interrupted admitted call")
	}
	if _, err := client.AcceptInput(ctx, target, &agentruntimev1.AcceptInputRequest{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondEntered:
	default:
		t.Fatal("new trust channel did not dispatch")
	}
	client.mutex.Lock()
	for _, channel := range client.channels {
		if channel == old {
			t.Error("new admission reused retired trust channel")
		}
	}
	client.mutex.Unlock()
	close(releaseFirst)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("retirement interrupted original RPC: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("old admitted RPC did not join")
	}
	if old.connection.GetState() != connectivity.Shutdown {
		t.Fatal("retired channel remained open after its users joined")
	}
	client.mutex.Lock()
	retired := len(client.retired)
	client.mutex.Unlock()
	if retired != 0 {
		t.Fatal("retired channel ownership leaked")
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("trust rotation replayed RPC: %d", calls)
	}
}

func TestRuntimeCommandPolicyAndRetainedChannel(t *testing.T) {
	p, err := RuntimeCommandPolicyFromEnv(func(key string) string {
		if strings.HasPrefix(key, "TETRAL_RUNTIME_") {
			return "25"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	methods := agentruntimev1.File_tetral_agent_runtime_v1_agent_runtime_proto.Services().ByName("AgentRuntimePodService").Methods()
	if methods.Len() != len(p.methods()) {
		t.Fatalf("unclassified command descriptor count %d != %d", methods.Len(), len(p.methods()))
	}
	for i := 0; i < methods.Len(); i++ {
		name := string(methods.Get(i).Name())
		if timeout, err := p.timeout(name); err != nil || timeout != 25*time.Millisecond {
			t.Fatalf("method %s not mapped to parsed timer: %s/%v", name, timeout, err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	counts := map[string]int{}
	exits := make(chan struct{}, 16)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, _ any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		mu.Lock()
		counts[info.FullMethod]++
		mu.Unlock()
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 25*time.Millisecond {
			t.Errorf("actual RPC lost parsed attempt timer: %s", info.FullMethod)
		}
		<-ctx.Done()
		exits <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}))
	agentruntimev1.RegisterAgentRuntimePodServiceServer(server, &policyRuntimeServer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	_, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	client := NewRuntimePodCommandClient(&countingRuntimeCommandTokenSource{})
	client.Policy = p
	t.Cleanup(func() { _ = client.Close() })
	target := RuntimePodTarget{PodIP: "127.0.0.1", Port: port}
	calls := []func(context.Context) error{
		func(ctx context.Context) error {
			_, err := client.AcceptInput(ctx, target, &agentruntimev1.AcceptInputRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.RecoverThread(ctx, target, &agentruntimev1.RecoverThreadRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.AcceptAgentMail(ctx, target, &agentruntimev1.AcceptAgentMailRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.AcceptTaskNotification(ctx, target, &agentruntimev1.AcceptTaskNotificationRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.Interrupt(ctx, target, &agentruntimev1.InterruptRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.ResolveToolConfirmation(ctx, target, &agentruntimev1.ResolveToolConfirmationRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.ApplyRuntimeConfig(ctx, target, &agentruntimev1.ApplyRuntimeConfigRequest{})
			return err
		},
		func(ctx context.Context) error {
			_, err := client.CleanupSession(ctx, target, &agentruntimev1.CleanupSessionRequest{})
			return err
		},
	}
	for _, call := range calls {
		if err := call(context.Background()); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("actual parsed attempt deadline: %v", err)
		}
		select {
		case <-exits:
		case <-time.After(time.Second):
			t.Fatal("RPC server context did not exit")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := calls[0](ctx); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	select {
	case <-exits:
	case <-time.After(time.Second):
		t.Fatal("earlier caller did not cancel actual RPC")
	}
	mu.Lock()
	for method, count := range counts {
		want := 1
		if strings.HasSuffix(method, "/AcceptInput") {
			want = 2
		}
		if count != want {
			t.Errorf("method %s generically retried: %d", method, count)
		}
	}
	mu.Unlock()
	if len(client.channels) != 1 {
		t.Fatalf("direct target did not retain one channel: %d", len(client.channels))
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := calls[0](context.Background()); err == nil {
		t.Fatal("closed client reopened a command channel")
	}
}

// An unconfigured client still bounds Interrupt with the default 30-second method policy.
func TestRuntimeCommandClientDefaultInterruptDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remaining := make(chan time.Duration, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, _ any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			remaining <- 0
		} else {
			remaining <- time.Until(deadline)
		}
		return &agentruntimev1.InterruptResponse{}, nil
	}))
	agentruntimev1.RegisterAgentRuntimePodServiceServer(server, &policyRuntimeServer{})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-joined })
	_, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	client := NewRuntimePodCommandClient(&countingRuntimeCommandTokenSource{})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Interrupt(context.Background(), RuntimePodTarget{PodIP: "127.0.0.1", Port: port}, &agentruntimev1.InterruptRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := <-remaining; got <= 29*time.Second || got > 30*time.Second {
		t.Fatalf("default Interrupt attempt deadline=%s want about 30s", got)
	}
}
