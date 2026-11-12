package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type queueEnvMap map[string]string

func (m queueEnvMap) Getenv(key string) string { return m[key] }

func TestPostgreSQLQueueCommandJoinsMaintenanceBeforeClosingPool(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousRun := openDatabase, runQueueService
	openDatabase = func(context.Context) (dbconnect.OpenResult, error) { return dbconnect.OpenResult{Client: client}, nil }
	entered, exited := make(chan struct{}), make(chan struct{})
	runQueueService = func(ctx context.Context, cfg tetralqueue.Config, store tetralqueue.Store, runtime tetralqueue.RuntimeConfig) error {
		if runtime.MaintenanceStore == nil {
			return errors.New("command omitted its maintenance owner")
		}
		runtime.MaintenanceStore = &commandMaintenanceBarrier{MaintenanceStore: runtime.MaintenanceStore, entered: entered, exited: exited}
		cfg.LeaseReclaimInterval = time.Millisecond
		return tetralqueue.Run(ctx, cfg, store, runtime)
	}
	t.Cleanup(func() { openDatabase, runQueueService = previousOpen, previousRun })
	watchdog, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(watchdog)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, queueEnvMap{tetralqueue.EnvHTTPAddress: "127.0.0.1:0", tetralqueue.EnvGRPCAddress: "127.0.0.1:0", tetralqueue.EnvDrainTimeoutMS: "200"})
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("command exited before maintenance: %v", err)
	case <-watchdog.Done():
		t.Fatal("maintenance did not start")
	}
	cancel()
	if err := runtimeDB.PingContext(watchdog); err != nil {
		t.Fatalf("command closed the pool with active maintenance: %v", err)
	}
	select {
	case <-exited:
	case <-watchdog.Done():
		t.Fatal("maintenance cancellation was not joined")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("forced command drain=%v", err)
		}
	case <-watchdog.Done():
		t.Fatal("command did not exit after maintenance")
	}
	if err := runtimeDB.PingContext(watchdog); err == nil {
		t.Fatal("command did not close its joined pool")
	}
}

type commandMaintenanceBarrier struct {
	tetralqueue.MaintenanceStore
	entered, exited chan struct{}
}

func (s *commandMaintenanceBarrier) ReclaimExpiredLeases(ctx context.Context, _ queue.ReclaimExpiredLeasesRequest) (int, error) {
	close(s.entered)
	defer close(s.exited)
	<-ctx.Done()
	return 0, ctx.Err()
}
