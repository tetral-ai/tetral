package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc"
	grpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	sandboxmodel "github.com/tetral-ai/tetral/internal/sandbox"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workload"
	bridge "github.com/tetral-ai/tetral/services/bridge"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type bridgeDrainAuthenticator struct{}

func (bridgeDrainAuthenticator) Authenticate(context.Context, string) (grpcauth.Identity, error) {
	return grpcauth.Identity{ServiceAccount: grpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"}, KubernetesPodUID: "drain-pod"}, nil
}

func TestBridgeCommandForcedJoinPreservesListenerAndPool(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	client := dbconnect.NewClientForTesting(runtime)
	oldOpen, oldBlob, oldRPC, oldHTTP, oldReview := openDatabase, newBlobStore, runInternalGRPC, runWorkload, newTokenReviewClient
	t.Cleanup(func() {
		openDatabase, newBlobStore, runInternalGRPC, runWorkload, newTokenReviewClient = oldOpen, oldBlob, oldRPC, oldHTTP, oldReview
	})
	openDatabase = func(context.Context, string, string) (dbconnect.OpenResult, error) {
		return dbconnect.OpenResult{Client: client}, nil
	}
	newBlobStore = blob.NewS3BlobStore
	newTokenReviewClient = func(envReader) (grpcauth.TokenReviewClient, error) { return nil, nil }
	for key, value := range map[string]string{blob.EnvEndpoint: "https://blob.example.invalid", blob.EnvRegion: "us-east-1", blob.EnvBucket: "drain-test", blob.EnvAccessKey: "test-access", blob.EnvSecretKey: "test-secret", blob.EnvAllowInsecure: "false", blob.EnvLocalTestMode: "false"} {
		t.Setenv(key, value)
	}
	env := bridgeEnvMap{bridge.EnvDatabaseURL: "postgres://fixture", bridge.EnvBridgeMCPConnectorGRPCAddr: "127.0.0.1:1", bridge.EnvBridgeGatewayTokenPath: "unused", bridge.EnvRuntimeBindingTokenHMACKey: strings.Repeat("x", 32), bridge.EnvBridgeAPIHTTPAddress: "127.0.0.1:0", bridge.EnvBridgeAPIGRPCAddress: "127.0.0.1:0", "TETRAL_INTERNAL_GRPC_AUDIENCE": grpcauth.Audience, "TETRAL_INTERNAL_ALLOWED_SERVICE_ACCOUNTS": "tetral-agent-runtime/agent-runtime", "TETRAL_DRAIN_TIMEOUT_MS": "40", "TETRAL_CANCEL_JOIN_TIMEOUT_MS": "30", "TETRAL_BRIDGE_ADMISSION_TIMEOUT_MS": "10", "TETRAL_BRIDGE_RELEASE_RUNTIME_BINDING_TIMEOUT_MS": "20"}
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseHandler := func() { once.Do(func() { close(release) }) }
	address := make(chan string, 1)
	runInternalGRPC = func(ctx context.Context, cfg internalgrpc.Config) error {
		if cfg.ShutdownTimeout != 40*time.Millisecond || cfg.CancelJoinTimeout != 30*time.Millisecond {
			t.Errorf("owning lifecycle policy not applied=%s/%s", cfg.ShutdownTimeout, cfg.CancelJoinTimeout)
		}
		cfg.Authenticator = bridgeDrainAuthenticator{}
		cfg.ServerOptions = append(cfg.ServerOptions, grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			response, err := next(ctx, request)
			var one int
			if queryErr := runtime.QueryRowContext(ctx, `SELECT 1`).Scan(&one); queryErr != nil {
				t.Error(queryErr)
			}
			close(entered)
			<-ctx.Done()
			// The pool and result listener remain owned throughout cancellation cleanup.
			if queryErr := runtime.QueryRow(`SELECT 1`).Scan(&one); queryErr != nil {
				t.Errorf("pool closed before handler cleanup=%v", queryErr)
			}
			close(cancelled)
			<-release
			return response, err
		}))
		address <- cfg.Listener.Addr().String()
		return internalgrpc.Run(ctx, cfg)
	}
	runWorkload = func(ctx context.Context, _ workload.Config) error { <-ctx.Done(); return nil }
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	joined := make(chan struct{})
	go func() { defer close(joined); finished <- run(ctx, env) }()
	t.Cleanup(func() {
		cancel()
		releaseHandler()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Bridge process did not join")
		}
	})
	await := func(ch <-chan struct{}, name string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("missing %s", name)
		}
	}
	var target string
	select {
	case target = <-address:
	case err := <-finished:
		t.Fatalf("Bridge startup=%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Bridge listener not started")
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	callCtx, stopCall := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCall()
	callDone := make(chan struct{})
	go func() {
		_, _ = bridgev1.NewAgentRuntimeBridgeServiceClient(conn).LoadContext(metadata.NewOutgoingContext(callCtx, metadata.Pairs("authorization", "bearer drain-fixture")), &bridgev1.LoadContextRequest{})
		close(callDone)
	}()
	defer func() { stopCall(); <-callDone }()
	await(entered, "production RPC admission")
	var listenerPID int
	waitUntil := time.Now().Add(5 * time.Second)
	for listenerPID == 0 && time.Now().Before(waitUntil) {
		if err := admin.QueryRow(`SELECT COALESCE(min(pid),0) FROM pg_stat_activity WHERE datname=current_database() AND query=$1 AND state='idle'`, "LISTEN "+sandboxmodel.ExecutionResultNotificationChannel).Scan(&listenerPID); err != nil {
			t.Fatal(err)
		}
		if listenerPID == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if listenerPID == 0 {
		t.Fatal("Bridge result LISTEN owner absent")
	}
	drainStarted := time.Now()
	cancel()
	await(cancelled, "configured forced cancellation")
	if elapsed := time.Since(drainStarted); elapsed < 35*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("nondefault40ms drain actual=%s", elapsed)
	}
	select {
	case err := <-finished:
		t.Fatalf("process released handler ownership=%v", err)
	case <-time.After(80 * time.Millisecond):
	}
	var listenerCount int
	if err := admin.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, listenerPID).Scan(&listenerCount); err != nil || listenerCount != 1 {
		t.Fatalf("listener closed before handler join=%d/%v", listenerCount, err)
	}
	releaseHandler()
	select {
	case err := <-finished:
		if !errors.Is(err, internalgrpc.ErrCancelJoinTimeout) {
			t.Fatalf("configured30ms cancellation join=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Bridge did not return after handler join")
	}
	if err := runtime.Ping(); err == nil {
		t.Fatal("Bridge command did not close owned pool after join")
	}
	if stats := runtime.Stats(); stats.OpenConnections != 0 || stats.InUse != 0 || stats.Idle != 0 {
		t.Fatalf("Bridge retained local pool ownership after join: %+v", stats)
	}
	// Driver Close sends Terminate and closes the local socket; PostgreSQL's
	// backend exit is a separate observation, not an acknowledgement of Close.
	// Keep the local join strict and bound observation of this exact remote PID.
	observeCtx, stopObservation := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopObservation()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := admin.QueryRowContext(observeCtx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, listenerPID).Scan(&listenerCount); err != nil {
			t.Fatalf("inspect listener after process join=%d/%v", listenerCount, err)
		}
		if listenerCount == 0 {
			break
		}
		select {
		case <-observeCtx.Done():
			t.Fatalf("listener survived bounded observation after process join=%d/%v", listenerCount, observeCtx.Err())
		case <-ticker.C:
		}
	}
}
