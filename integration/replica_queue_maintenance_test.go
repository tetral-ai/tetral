package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestPostgreSQLReplicaQueueMaintenanceShutdown(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback_and_replacement"}[force], func(t *testing.T) {
			runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			initial := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(runtimeDB))
			first := enqueueReplicaQueueConfig(t, initial, "default", "maintenance_a", 1)
			second := enqueueReplicaQueueConfig(t, initial, "default", "maintenance_b", 1)
			original, err := initial.Lease(watchdog, queue.LeaseRequest{WorkspaceID: "default", Kinds: []string{queue.KindRuntimeConfigUpdate}, LeaseOwner: "abandoned", MaxJobs: 2, LeaseDuration: 50 * time.Millisecond, Now: time.Now()})
			if err != nil || len(original) != 2 {
				t.Fatalf("initial leases=%d/%v", len(original), err)
			}
			awaitReplicaQueueExpiry(watchdog, t, admin, first)
			awaitReplicaQueueExpiry(watchdog, t, admin, second)
			barrier := &queueReclaimCommitBarrier{entered: make(chan struct{}), exited: make(chan struct{}), release: make(chan struct{})}
			barrier.armed.Store(true)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
			defer release()
			servingPool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, barrier)
			client := dbconnect.NewClientForTesting(servingPool)
			store := queue.NewPostgreSQLStore(client)
			ctx, cancel := context.WithCancel(watchdog)
			defer cancel()
			readiness := make(chan *workload.Readiness, 1)
			done := make(chan error, 1)
			go func() {
				err := tetralqueue.Run(ctx, tetralqueue.Config{GRPCAddress: "127.0.0.1:0", HTTPAddress: "127.0.0.1:0", LeaseReclaimInterval: time.Millisecond, LeaseReclaimBatchLimit: 10, DrainTimeout: 200 * time.Millisecond}, store, tetralqueue.RuntimeConfig{
					MaintenanceStore: store, DBStatsProvider: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
					RunHTTP: func(ctx context.Context, cfg workload.Config) error {
						readiness <- cfg.Readiness
						return workload.Run(ctx, cfg)
					},
				})
				// This is the command's resource order: service joins every user, then the
				// command releases its pool. The test checks both sides of that boundary.
				err = errors.Join(err, client.Close())
				done <- err
			}()
			var ready *workload.Readiness
			select {
			case ready = <-readiness:
			case <-watchdog.Done():
				t.Fatal(watchdog.Err())
			}
			awaitReplicaQueueReadiness(watchdog, t, ready, true)
			awaitReplicaQueueBarrier(watchdog, t, barrier.entered, "first reclaim UPDATE before commit")
			assertReplicaQueueState(watchdog, t, admin, "default", first, queue.StatusLeased)
			assertReplicaQueueState(watchdog, t, admin, "default", second, queue.StatusLeased)
			cancel()
			awaitReplicaQueueReadiness(watchdog, t, ready, false)
			if err := servingPool.PingContext(watchdog); err != nil {
				t.Fatalf("pool closed while maintenance still owned its transaction: %v", err)
			}
			select {
			case err := <-done:
				t.Fatalf("service returned across active maintenance transaction: %v", err)
			default:
			}
			if !force {
				release()
			}
			awaitReplicaQueueBarrier(watchdog, t, barrier.exited, "SQL barrier exited")
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("shutdown outcome=%v", err)
				}
				if force && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("missing forced-drain outcome: %v", err)
				}
			case <-watchdog.Done():
				t.Fatal("maintenance cancellation did not join")
			}
			if err := servingPool.PingContext(watchdog); err == nil {
				t.Fatal("command retained the joined database pool")
			}
			want := queue.StatusPending
			if force {
				want = queue.StatusLeased
			}
			for _, id := range []string{first, second} {
				assertReplicaQueueState(watchdog, t, admin, "default", id, want)
			}
			// A different maintenance owner sees the committed whole batch or reclaims
			// the entirely rolled-back batch. It never repeats the first partial update.
			replacementPool := storagetest.OpenRuntimeRoleDBWithTracer(t, runtimeDB, nil)
			replacement := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(replacementPool))
			observed := &queueObservedMaintenance{PostgreSQLQueueStore: replacement, first: make(chan queueReclaimResult, 1)}
			replacementCtx, cancelReplacement := context.WithCancel(watchdog)
			defer cancelReplacement()
			replacementDone := make(chan error, 1)
			go func() {
				replacementDone <- tetralqueue.Run(replacementCtx, tetralqueue.Config{GRPCAddress: "127.0.0.1:0", HTTPAddress: "127.0.0.1:0", LeaseReclaimInterval: time.Millisecond, DrainTimeout: 200 * time.Millisecond}, replacement, tetralqueue.RuntimeConfig{MaintenanceStore: observed, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			}()
			select {
			case result := <-observed.first:
				wantCount := 0
				if force {
					wantCount = 2
				}
				if result.err != nil || result.count != wantCount {
					t.Fatalf("replacement reclaimed=%d/%v; want %d", result.count, result.err, wantCount)
				}
			case <-watchdog.Done():
				t.Fatal("replacement maintenance did not run")
			}
			cancelReplacement()
			select {
			case <-replacementDone:
			case <-watchdog.Done():
				t.Fatal("replacement owner did not join")
			}
			for _, id := range []string{first, second} {
				assertReplicaQueueState(watchdog, t, admin, "default", id, queue.StatusPending)
			}
			replacementRPC := serveQueueReplica(t, replacement, nil)
			recovered := leaseReplicaQueue(watchdog, t, replacementRPC, time.Second)
			if len(recovered) != 2 {
				t.Fatalf("recovered batch size=%d; want two", len(recovered))
			}
			for _, job := range recovered {
				for _, old := range original {
					if old.ID == job.GetId() && old.LeaseToken == job.GetLeaseToken() {
						t.Fatal("replacement reused abandoned authority")
					}
				}
				ackReplicaQueue(watchdog, t, replacementRPC, job, true)
			}
			t.Logf("maintenance SQL UPDATE held before commit; force=%t; pool remained open through cancellation/join; replacement reclaimed whole batch exactly once", force)
		})
	}
}

type queueReclaimTraceKey struct{}
type queueReclaimCommitBarrier struct {
	armed                    atomic.Bool
	entered, exited, release chan struct{}
}

func (b *queueReclaimCommitBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	matches := strings.Contains(data.SQL, "UPDATE queue_jobs") && strings.Contains(data.SQL, "last_error_kind = $3") && strings.Contains(data.SQL, "leased_until = NULL")
	return context.WithValue(ctx, queueReclaimTraceKey{}, matches)
}
func (b *queueReclaimCommitBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	matches, _ := ctx.Value(queueReclaimTraceKey{}).(bool)
	if matches && data.Err == nil && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
		close(b.exited)
	}
}

type queueReclaimResult struct {
	count int
	err   error
}
type queueObservedMaintenance struct {
	*queue.PostgreSQLQueueStore
	once  sync.Once
	first chan queueReclaimResult
}

func (s *queueObservedMaintenance) ReclaimExpiredLeases(ctx context.Context, r queue.ReclaimExpiredLeasesRequest) (int, error) {
	count, err := s.PostgreSQLQueueStore.ReclaimExpiredLeases(ctx, r)
	s.once.Do(func() { s.first <- queueReclaimResult{count, err} })
	return count, err
}
func awaitReplicaQueueReadiness(ctx context.Context, t *testing.T, r *workload.Readiness, want bool) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for r.Ready() != want {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("readiness transition did not occur")
		}
	}
}
