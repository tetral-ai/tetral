package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

func TestTetralQueueSchemaBehindStopsBeforeStoreAndListener(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify, previousRun := openDatabase, verifySchema, runQueueService
	openDatabase = func(context.Context) (dbconnect.OpenResult, error) { return dbconnect.OpenResult{Client: client}, nil }
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	runQueueService = func(context.Context, tetralqueue.Config, tetralqueue.Store, tetralqueue.RuntimeConfig) error {
		t.Fatal("queue service started after schema-behind failure")
		return nil
	}
	t.Cleanup(func() { openDatabase, verifySchema, runQueueService = previousOpen, previousVerify, previousRun })

	err := run(context.Background(), queueEnvMap{})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestTetralQueueCommandStartupFailureLogUsesSharedFields(t *testing.T) {
	stderr, finish := captureStderr(t)
	err := run(context.Background(), queueEnvMap{
		tetralqueue.EnvLeaseReclaimLimit: "0",
	})
	if err == nil {
		t.Fatal("run returned nil for config failure")
	}
	finish()
	output := stderr.String()
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"queue"`,
		`"component":"queue"`,
		`"error.class":"config_error"`,
		tetralqueue.EnvLeaseReclaimLimit,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type queueEnvMap map[string]string

func (m queueEnvMap) Getenv(key string) string { return m[key] }

func captureStderr(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	previous := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = writeEnd
	var buffer bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = buffer.ReadFrom(readEnd)
		close(done)
	}()
	finish := func() {
		_ = writeEnd.Close()
		os.Stderr = previous
		<-done
		_ = readEnd.Close()
	}
	t.Cleanup(func() {
		if os.Stderr == writeEnd {
			finish()
		}
	})
	return &buffer, finish
}

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
