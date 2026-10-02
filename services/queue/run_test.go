package tetralqueue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workload"
	queuev1 "github.com/tetral-ai/tetral/services/queue/gen/tetral/queue/v1"
)

func TestQueueRunJoinsMaintenanceAndRPCBeforeReturning(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "cancel"}[force], func(t *testing.T) {
			watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			ctx, cancel := context.WithCancel(watchdog)
			defer cancel()
			store := &barrierQueueStore{rpcEntered: make(chan struct{}), maintenanceEntered: make(chan struct{}), rpcExited: make(chan struct{}), maintenanceExited: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(store.release) }) }
			defer release()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			httpDraining := make(chan struct{})
			httpRelease := make(chan struct{})
			var httpOnce sync.Once
			releaseHTTP := func() { httpOnce.Do(func() { close(httpRelease) }) }
			defer releaseHTTP()
			done := make(chan error, 1)
			go func() {
				done <- Run(ctx, Config{GRPCAddress: "rpc", HTTPAddress: "127.0.0.1:0", LeaseReclaimInterval: time.Millisecond, DrainTimeout: 200 * time.Millisecond}, store, RuntimeConfig{
					Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaintenanceStore: store,
					Listen: func(network, address string) (net.Listener, error) {
						if address == "rpc" {
							return listener, nil
						}
						return net.Listen(network, address)
					},
					RunHTTP: func(ctx context.Context, _ workload.Config) error {
						<-ctx.Done()
						close(httpDraining)
						select {
						case <-httpRelease:
							return nil
						case <-watchdog.Done():
							return watchdog.Err()
						}
					},
				})
			}()
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			rpcDone := make(chan error, 1)
			go func() {
				_, err := queuev1.NewQueueServiceClient(conn).Lease(watchdog, &queuev1.LeaseRequest{WorkspaceId: "default", Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "barrier", MaxJobs: 1, LeaseDurationMs: 1000})
				rpcDone <- err
			}()
			waitQueueBarrier(watchdog, t, store.rpcEntered, "RPC entered")
			waitQueueBarrier(watchdog, t, store.maintenanceEntered, "maintenance entered")
			cancel()
			waitQueueBarrier(watchdog, t, httpDraining, "HTTP observed shutdown")
			select {
			case err := <-done:
				t.Fatalf("returned before admitted work joined: %v", err)
			default:
			}
			select {
			case <-store.maintenanceExited:
				t.Fatal("active maintenance was cancelled before its drain window")
			default:
			}
			if !force {
				release()
			}
			waitQueueBarrier(watchdog, t, store.maintenanceExited, "maintenance exited")
			waitQueueBarrier(watchdog, t, store.rpcExited, "RPC exited")
			select {
			case err := <-done:
				t.Fatalf("returned before HTTP owner joined: %v", err)
			default:
			}
			releaseHTTP()
			select {
			case err := <-done:
				if force && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("forced drain outcome=%v", err)
				}
				if !force && !errors.Is(err, context.Canceled) {
					t.Fatalf("completed drain outcome=%v", err)
				}
			case <-watchdog.Done():
				t.Fatal("Queue did not join within watchdog")
			}
			select {
			case err := <-rpcDone:
				if !force && err != nil {
					t.Fatalf("admitted RPC did not finish: %v", err)
				}
			case <-watchdog.Done():
				t.Fatal("client RPC remained active")
			}
			if store.cycles.Load() != 1 {
				t.Fatalf("admitted %d maintenance cycles across shutdown; want one", store.cycles.Load())
			}
		})
	}
}

func waitQueueBarrier(ctx context.Context, t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("%s: %v", description, ctx.Err())
	}
}

type barrierQueueStore struct {
	recordingStore
	rpcEntered, maintenanceEntered, rpcExited, maintenanceExited, release chan struct{}
	cycles                                                                atomic.Int64
}

func (s *barrierQueueStore) Lease(ctx context.Context, _ queue.LeaseRequest) ([]*queue.Job, error) {
	close(s.rpcEntered)
	defer close(s.rpcExited)
	select {
	case <-s.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *barrierQueueStore) ReclaimExpiredLeases(ctx context.Context, _ queue.ReclaimExpiredLeasesRequest) (int, error) {
	if s.cycles.Add(1) != 1 {
		return 0, errors.New("unexpected additional maintenance cycle")
	}
	close(s.maintenanceEntered)
	defer close(s.maintenanceExited)
	select {
	case <-s.release:
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
func (s *barrierQueueStore) SweepSandboxTerminalJobs(context.Context, queue.SandboxTerminalSweepRequest) (int, error) {
	return 0, nil
}
func (s *barrierQueueStore) SweepEmptyPartitionCounters(context.Context, queue.EmptyPartitionCounterSweepRequest) (int, error) {
	return 0, nil
}

func TestQueueDrainConfigurationLeavesPodJoinMargin(t *testing.T) {
	cfg, err := ConfigFromEnv(configEnv{})
	if err != nil || cfg.DrainTimeout != 10*time.Second {
		t.Fatalf("default Queue drain=%v/%v", cfg.DrainTimeout, err)
	}
	cfg, err = ConfigFromEnv(configEnv{EnvDrainTimeoutMS: "200"})
	if err != nil || cfg.DrainTimeout != 200*time.Millisecond {
		t.Fatalf("configured Queue drain=%v/%v", cfg.DrainTimeout, err)
	}
	for _, value := range []string{"0", "-1", "25001", "invalid"} {
		if _, err := ConfigFromEnv(configEnv{EnvDrainTimeoutMS: value}); err == nil {
			t.Fatalf("accepted invalid drain budget %q", value)
		}
	}
}
