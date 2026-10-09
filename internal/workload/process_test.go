package workload_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	providergatewayv1 "github.com/tetral-ai/tetral/services/gateway/gen/tetral/provider_gateway/v1"
	jobrunner "github.com/tetral-ai/tetral/services/job-runner"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
	webconnector "github.com/tetral-ai/tetral/services/web-connector"
)

type processTestSink string

func (s processTestSink) Write(p []byte) (int, error) {
	switch s {
	case "throw":
		panic("sink failed")
	case "blocked":
		select {}
	}
	return len(p), nil
}

// A real executable must leave even if its live handler never cooperates. The
// child cannot reach resource cleanup until the actual HTTP owner joins.
func TestProcessShutdownBoundsExecutable(t *testing.T) {
	if mode := os.Getenv("TETRAL_PROCESS_TEST_CHILD"); mode != "" {
		runProcessTestChild(t, mode)
		return
	}
	for _, mode := range []string{"signal", "silent", "throw", "blocked", "listener-failure", "cooperative", "grpc", "grpc-listener-failure", "grpc-cooperative", "queue", "queue-cooperative", "runner", "runner-cooperative", "web", "web-cooperative"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessShutdownBoundsExecutable$")
			child.Env = append(os.Environ(), "TETRAL_PROCESS_TEST_CHILD="+mode, "TETRAL_PROCESS_TEST_DIR="+dir)
			var output bytes.Buffer
			child.Stdout, child.Stderr = &output, &output
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- child.Wait() }()
			t.Cleanup(func() {
				_ = child.Process.Kill()
			})
			ready := filepath.Join(dir, "ready")
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("child exited before admission: %v %s", err, output.String())
				case <-time.After(10 * time.Millisecond):
				case <-ctx.Done():
					t.Fatal("child not ready")
				}
			}
			started := time.Now()
			if strings.HasSuffix(mode, "listener-failure") {
				if err := os.WriteFile(filepath.Join(dir, "fail"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := child.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			err := <-done
			elapsed := time.Since(started)
			if ctx.Err() != nil {
				t.Fatalf("child needed external watchdog: %s", output.String())
			}
			_, closedErr := os.Stat(filepath.Join(dir, "closed"))
			if strings.HasSuffix(mode, "cooperative") {
				if err != nil || closedErr != nil {
					t.Fatalf("cooperative child did not join/close: %v %v %s", err, closedErr, output.String())
				}
			} else {
				if err == nil || child.ProcessState.ExitCode() != 1 {
					t.Fatalf("held child exit=%v state=%v %s", err, child.ProcessState, output.String())
				}
				if !os.IsNotExist(closedErr) {
					t.Fatalf("dependencies closed under held handler: %v", closedErr)
				}
				if elapsed < 4500*time.Millisecond || elapsed > 7*time.Second {
					t.Fatalf("5s absolute allocation exceeded or cut short: %s", elapsed)
				}
			}
			t.Logf("actual pid=%d mode=%s elapsed=%s exit=%d", child.Process.Pid, mode, elapsed, child.ProcessState.ExitCode())
		})
	}
}
func runProcessTestChild(t *testing.T, mode string) {
	dir := os.Getenv("TETRAL_PROCESS_TEST_DIR")
	var sink io.Writer = os.Stderr
	if mode == "silent" {
		sink = io.Discard
	}
	if mode == "throw" || mode == "blocked" {
		sink = processTestSink(mode)
	}
	owner := workload.NewProcessLogger(sink, "test-process", "test", "test", workload.DefaultDiagnosticConfig())
	err := workload.RunProcess(func(ctx context.Context) error {
		defer owner.CloseWithBudget()
		workload.ConfigureProcessShutdown(ctx, 5*time.Second, owner)
		defer workload.ProcessCleanup(ctx, func() {
			if err := os.WriteFile(filepath.Join(dir, "closed"), nil, 0600); err != nil { //nolint:gosec // G703: isolated parent-owned test directory passed to this child.
				panic(err)
			}
		})
		// A real second listener failure three seconds later cannot reset the
		// process deadline started by the first listener's signal.
		if mode == "signal" {
			second, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				return err
			}
			go func() {
				_ = workload.Run(context.WithoutCancel(ctx), workload.Config{ServiceName: "second", Listener: second, Handler: http.NotFoundHandler(), ShutdownTimeout: 2 * time.Second, Logger: owner.Logger})
			}()
			go func() { <-ctx.Done(); time.Sleep(3 * time.Second); _ = second.Close() }()
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer func() { _ = listener.Close() }()
		entered := make(chan struct{})
		hold := processTestHold(ctx, mode, entered)
		go func() {
			<-entered
			if err := os.WriteFile(filepath.Join(dir, "ready"), []byte(fmt.Sprint(os.Getpid())), 0600); err != nil { //nolint:gosec // G703: isolated parent-owned test directory.
				panic(err)
			}
			if strings.HasSuffix(mode, "listener-failure") {
				for {
					if _, err := os.Stat(filepath.Join(dir, "fail")); err == nil { //nolint:gosec // G703: isolated parent-owned test control file.
						_ = listener.Close()
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
		if strings.HasPrefix(mode, "web") {
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			go func() {
				callCtx := metadata.NewOutgoingContext(context.WithoutCancel(ctx), metadata.Pairs("authorization", "Bearer fixture"))
				_, _ = providergatewayv1.NewProviderGatewayServiceClient(conn).RunWeb(callCtx, &providergatewayv1.RunWebRequest{})
			}()
			return webconnector.Run(ctx, webconnector.Config{GRPCAddress: "rpc", MetricsAddress: "127.0.0.1:0", DrainTimeout: 2 * time.Second, CancelJoinTimeout: 3 * time.Second}, &webconnector.Service{}, webconnector.NewMetrics(), webconnector.RuntimeConfig{Logger: owner.Logger, Authenticator: processWebAuthenticator{hold: hold}, Listen: func(network, address string) (net.Listener, error) {
				if address == "rpc" {
					return listener, nil
				}
				return net.Listen(network, address)
			}})
		}
		if strings.HasPrefix(mode, "queue") {
			store := &processQueueStore{hold: hold}
			return tetralqueue.Run(ctx, tetralqueue.Config{GRPCAddress: "127.0.0.1:0", HTTPAddress: "127.0.0.1:0", LeaseReclaimInterval: time.Millisecond, DrainTimeout: 2 * time.Second, CancelJoinTimeout: 3 * time.Second}, store, tetralqueue.RuntimeConfig{Logger: owner.Logger, MaintenanceStore: store})
		}
		if strings.HasPrefix(mode, "runner") {
			runner := &jobrunner.JobRunner{Queue: &processRunnerQueue{hold: hold}, Deliverer: &processRunnerDeliverer{}, Config: jobrunner.JobRunnerConfig{DrainTimeout: 2 * time.Second, CancelJoinTimeout: 3 * time.Second}}
			return jobrunner.RunJobRunnerLoop(ctx, runner, owner.Logger, queue.NewWakeSignal())
		}
		if strings.HasPrefix(mode, "grpc") {
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			go func() {
				callCtx := metadata.NewOutgoingContext(context.WithoutCancel(ctx), metadata.Pairs("authorization", "Bearer fixture"))
				_ = conn.Invoke(callCtx, "/test.Process/Call", &emptypb.Empty{}, &emptypb.Empty{})
			}()
			return internalgrpc.Run(ctx, internalgrpc.Config{ServiceName: "test-process", Listener: listener, Authenticator: processAuthenticator{}, ShutdownTimeout: 2 * time.Second, CancelJoinTimeout: 3 * time.Second, Logger: owner.Logger, Register: func(server *grpc.Server) { registerProcessCall(server, hold) }})
		}
		go func() {
			response, err := http.Get("http://" + listener.Addr().String())
			if err == nil {
				_ = response.Body.Close()
			}
		}()
		return workload.Run(ctx, workload.Config{ServiceName: "test-process", Listener: listener, Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hold() }), ShutdownTimeout: 2 * time.Second, Logger: owner.Logger})
	})
	if err != nil && !strings.Contains(err.Error(), "context") {
		t.Fatal(err)
	}
}

func processTestHold(ctx context.Context, mode string, entered chan struct{}) func() {
	var admitted sync.Once
	return func() {
		// Maintenance can call again after the cooperative first cycle returns.
		// Readiness witnesses first admission, not a one-call service contract.
		admitted.Do(func() { close(entered) })
		if strings.HasSuffix(mode, "cooperative") {
			<-ctx.Done()
			return
		}
		select {}
	}
}

type processAuthenticator struct{}

func (processAuthenticator) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	return grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "test", Name: "process"}}, nil
}
func registerProcessCall(server *grpc.Server, hold func()) {
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Process", HandlerType: (*interface{})(nil), Methods: []grpc.MethodDesc{{MethodName: "Call", Handler: func(_ any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		request := &emptypb.Empty{}
		if err := decode(request); err != nil {
			return nil, err
		}
		call := func(context.Context, any) (any, error) { hold(); return &emptypb.Empty{}, nil }
		if interceptor == nil {
			return call(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: "/test.Process/Call"}, call)
	}}}}, struct{}{})
}

type processQueueStore struct {
	tetralqueue.Store
	hold func()
}

func (s *processQueueStore) ReclaimExpiredLeases(context.Context, queue.ReclaimExpiredLeasesRequest) (int, error) {
	s.hold()
	return 0, nil
}
func (*processQueueStore) SweepSandboxTerminalJobs(context.Context, queue.SandboxTerminalSweepRequest) (int, error) {
	return 0, nil
}
func (*processQueueStore) SweepEmptyPartitionCounters(context.Context, queue.EmptyPartitionCounterSweepRequest) (int, error) {
	return 0, nil
}
func (*processQueueStore) PruneJobRunnerTerminalJobs(context.Context, queue.JobRunnerTerminalRetentionRequest) (queue.JobRunnerTerminalRetentionResult, error) {
	return queue.JobRunnerTerminalRetentionResult{}, nil
}

type processRunnerQueue struct {
	jobrunner.QueueClient
	hold func()
}
type processRunnerDeliverer struct{ jobrunner.RuntimeJobDeliverer }

// LeaseJobRunnerJobs is the Runner's acquisition dependency; a stalled Queue
// call must hold the process like any other owned dependency.
func (q *processRunnerQueue) LeaseJobRunnerJobs(context.Context, *queuev1.LeaseJobRunnerJobsRequest) (*queuev1.LeaseJobRunnerJobsResponse, error) {
	q.hold()
	return &queuev1.LeaseJobRunnerJobsResponse{}, nil
}

// Authentication is an owned external dependency: a stalled TokenReview must
// retain the Web handler and command dependencies just like a stalled backend.
type processWebAuthenticator struct{ hold func() }

func (a processWebAuthenticator) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	a.hold()
	return grpcauth.Identity{}, context.Canceled
}
