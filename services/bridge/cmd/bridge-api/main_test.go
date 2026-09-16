package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

// Exercise the command's real store/listener assembly. Server loops are
// controlled here; no RPC or blob request is needed to establish LISTEN.
func TestBridgeAPICommandStartsAndStopsExecutionResultListener(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousWorkload := openDatabase, runWorkload
	previousGRPC, previousTokenReview := runInternalGRPC, newTokenReviewClient
	t.Cleanup(func() {
		openDatabase, runWorkload = previousOpen, previousWorkload
		runInternalGRPC, newTokenReviewClient = previousGRPC, previousTokenReview
	})
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	newTokenReviewClient = func(envReader) (grpcauth.TokenReviewClient, error) { return nil, nil }
	runInternalGRPC = func(ctx context.Context, cfg internalgrpc.Config) error {
		cfg.OnServing()
		<-ctx.Done()
		return nil
	}
	serving := make(chan struct{})
	runWorkload = func(ctx context.Context, _ workload.Config) error {
		close(serving)
		<-ctx.Done()
		return nil
	}
	for key, value := range map[string]string{
		blob.EnvEndpoint:      "https://blob.example.invalid",
		blob.EnvRegion:        "us-east-1",
		blob.EnvBucket:        "bridge-startup-test",
		blob.EnvAccessKey:     "test-access-key",
		blob.EnvSecretKey:     "test-secret-key",
		blob.EnvAllowInsecure: "false",
		blob.EnvLocalTestMode: "false",
	} {
		t.Setenv(key, value)
	}
	env := bridgeEnvMap{
		agentruntimebridge.EnvDatabaseURL:                "postgres://runtime@postgres/tetral",
		agentruntimebridge.EnvBridgeMCPConnectorGRPCAddr: "127.0.0.1:1",
		agentruntimebridge.EnvBridgeGatewayTokenPath:     "/unused/test-token",
		agentruntimebridge.EnvRuntimeBindingTokenHMACKey: strings.Repeat("x", 32),
		agentruntimebridge.EnvBridgeAPIHTTPAddress:       "127.0.0.1:0",
		agentruntimebridge.EnvBridgeAPIGRPCAddress:       "127.0.0.1:0",
		"TETRAL_INTERNAL_GRPC_AUDIENCE":                  grpcauth.Audience,
		"TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS":       "tetral/agent-runtime",
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	done := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("bridge command did not stop")
		}
	})
	go func() {
		defer close(stopped)
		done <- run(ctx, env)
	}()
	select {
	case <-serving:
	case err := <-done:
		t.Fatalf("bridge command stopped before serving: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("bridge command did not start")
	}
	waitForCount := func(query string, want int, args ...any) int {
		t.Helper()
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer probeCancel()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var count, pid int
		for {
			if err := admin.QueryRowContext(probeCtx, query, args...).Scan(&count, &pid); err != nil {
				t.Fatalf("inspect execution result listener: %v", err)
			}
			if count == want {
				return pid
			}
			select {
			case <-probeCtx.Done():
				t.Fatalf("execution result listener count = %d; want %d", count, want)
			case <-ticker.C:
			}
		}
	}
	listenerPID := waitForCount(`
		SELECT count(*), COALESCE(min(pid), 0) FROM pg_stat_activity
		WHERE datname = current_database() AND query = $1 AND state = 'idle'`,
		1, "LISTEN "+sandboxmodel.ExecutionResultNotificationChannel)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bridge command shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bridge command did not finish shutdown")
	}
	waitForCount(`SELECT count(*), COALESCE(min(pid), 0) FROM pg_stat_activity WHERE pid = $1`, 0, listenerPID)
}

func TestBridgeAPISchemaBehindStopsBeforeStoreAndListeners(t *testing.T) {
	runtimeDB := storagetest.NewPostgreSQLDB(t)
	client := dbconnect.NewClientForTesting(runtimeDB)
	previousOpen, previousVerify := openDatabase, verifySchema
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	verifySchema = func(context.Context, *dbconnect.Client) error {
		return &storage.SchemaMigrationError{Kind: storage.SchemaErrorBehind, Version: 1}
	}
	t.Cleanup(func() { openDatabase, verifySchema = previousOpen, previousVerify })

	err := run(context.Background(), bridgeEnvMap{agentruntimebridge.EnvDatabaseURL: "postgres://runtime@postgres/tetral"})
	var schemaErr *storage.SchemaMigrationError
	if !errors.As(err, &schemaErr) || schemaErr.Kind != storage.SchemaErrorBehind {
		t.Fatalf("run error = %v, want schema-behind", err)
	}
}

func TestBridgeAPICommandStartupFailureLogRedactsDependencyError(t *testing.T) {
	previousOpen := openDatabase
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{}, errors.New("postgres://user:secret@db.internal/tetral provider payload")
	}
	t.Cleanup(func() { openDatabase = previousOpen })

	stderr, finish := captureStderr(t)
	err := run(context.Background(), bridgeEnvMap{
		agentruntimebridge.EnvDatabaseURL: "postgres://runtime@postgres/tetral",
	})
	if err == nil {
		t.Fatal("run returned nil for dependency failure")
	}
	finish()
	output := stderr.String()
	for _, forbidden := range []string{"postgres://", "secret@db.internal", "provider payload"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("startup log leaked %q: %s", forbidden, output)
		}
	}
	for _, want := range []string{
		`"msg":"startup.failed"`,
		`"service.name":"bridge"`,
		`"component":"bridge"`,
		`"error.class":"startup_error"`,
		`"error.message_safe":"startup failed"`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("startup log missing %s: %s", want, output)
		}
	}
}

type bridgeEnvMap map[string]string

func (m bridgeEnvMap) Getenv(key string) string { return m[key] }

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
