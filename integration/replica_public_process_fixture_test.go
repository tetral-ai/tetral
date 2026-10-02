package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	api "github.com/tetral-ai/tetral/services/api"
)

const replicaPublicAPIChildDirEnv = "TETRAL_REPLICA_PUBLIC_API_CHILD_DIR"

type replicaPublicAPIChild struct {
	URL, controlURL string
	logPath         string
	command         *exec.Cmd
	done            chan struct{}
	waitErr         error // Read only after done closes.
}

type replicaPublicAPIReady struct {
	URL, ControlURL, Role, Database string
	PID                             int
}

// Re-exec the already-built integration binary. The child composes the real
// API router/store and owns its listener and runtime-role database pool; only
// the tracer/control listener belongs to the fault apparatus.
func startReplicaPublicAPIChild(ctx context.Context, t *testing.T, runtimeDB *sql.DB, publicKey string) *replicaPublicAPIChild {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	logFile, err := os.Create(filepath.Join(dir, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	command := exec.CommandContext(ctx, executable, "-test.run=^TestPostgreSQLReplicaPublicControlPlane$", "-test.timeout=180s", "-test.v")
	command.Env = append(os.Environ(), replicaPublicAPIChildDirEnv+"="+dir,
		"TETRAL_REPLICA_PUBLIC_API_RUNTIME_URL="+storagetest.RuntimeDatabaseURL(t, runtimeDB),
		"TETRAL_REPLICA_PUBLIC_API_PUBLIC_KEY="+publicKey)
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	child := &replicaPublicAPIChild{command: command, done: make(chan struct{}), logPath: filepath.Join(dir, "child.log")}
	go func() { child.waitErr = command.Wait(); close(child.done) }()
	t.Cleanup(func() { child.killAndJoin(t) })
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, readErr := os.ReadFile(filepath.Join(dir, "ready.json"))
		if readErr == nil {
			var ready replicaPublicAPIReady
			if err := json.Unmarshal(raw, &ready); err != nil {
				t.Fatal(err)
			}
			var role, database string
			if err := runtimeDB.QueryRowContext(ctx, `SELECT current_user,current_database()`).Scan(&role, &database); err != nil {
				t.Fatal(err)
			}
			if ready.Role != role || ready.Database != database || ready.PID != command.Process.Pid || ready.URL == "" || ready.ControlURL == "" {
				t.Fatalf("API child listener/PID/runtime-role clone identity mismatch: %+v", ready)
			}
			child.URL, child.controlURL = ready.URL, ready.ControlURL
			t.Logf("actual API child ready pid=%d clone=%s runtime_role=%s", ready.PID, ready.Database, ready.Role)
			return child
		}
		select {
		case <-child.done:
			data, _ := os.ReadFile(filepath.Join(dir, "child.log"))
			t.Fatalf("API child exited before readiness: %v %s", child.waitErr, data)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("API child readiness report absent")
	return nil
}

func (c *replicaPublicAPIChild) killAndJoin(t *testing.T) {
	t.Helper()
	defer func() {
		raw, err := os.ReadFile(c.logPath)
		if err != nil {
			t.Error(err)
		} else if strings.Contains(string(raw), "WARNING: DATA RACE") {
			t.Errorf("API child race detector reported a race: %s", raw)
		}
	}()
	select {
	case <-c.done:
		return
	default:
	}
	if err := c.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Error(err)
	}
	select {
	case <-c.done:
		state, ok := c.command.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL {
			t.Errorf("API child did not exit by actual SIGKILL: %v", c.waitErr)
		}
	case <-time.After(10 * time.Second):
		t.Error("API child failed to join after kill")
	}
}

func (c *replicaPublicAPIChild) control(t *testing.T, method, path, body string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, c.controlURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("API child control=%d/%v %s", response.StatusCode, err, raw)
	}
	return raw
}

func (c *replicaPublicAPIChild) arm(t *testing.T, contains string) (<-chan struct{}, func()) {
	t.Helper()
	c.control(t, "POST", "/arm", contains)
	reached := make(chan struct{})
	// This polls the child-side query completion barrier, never a fabricated
	// parent-side ACK. The ordinary barrier helper supplies the outer watchdog.
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		client := &http.Client{Timeout: time.Second}
		for time.Now().Before(deadline) {
			response, err := client.Get(c.controlURL + "/reached")
			if err == nil {
				raw, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if string(raw) == "true" {
					close(reached)
					return
				}
			}
			select {
			case <-c.done:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	var once sync.Once
	release := func() {
		once.Do(func() {
			select {
			case <-c.done:
				return
			default:
				c.control(t, "POST", "/release", "")
			}
		})
	}
	t.Cleanup(release)
	return reached, release
}

func (c *replicaPublicAPIChild) assertUncommittedTransaction(ctx context.Context, t *testing.T, admin *sql.DB) uint32 {
	t.Helper()
	var pid uint32
	if err := json.Unmarshal(c.control(t, "GET", "/database-pid", ""), &pid); err != nil {
		t.Fatal(err)
	}
	var state, query string
	var transactionOpen bool
	if err := admin.QueryRowContext(ctx, `SELECT state,query,xact_start IS NOT NULL FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&state, &query, &transactionOpen); err != nil || state != "idle in transaction" || !transactionOpen || !strings.Contains(strings.ToLower(query), "insert into queue_jobs") {
		t.Fatalf("child database precommit boundary state=%q transaction=%t query=%q/%v", state, transactionOpen, query, err)
	}
	t.Logf("API child pid=%d reached queue INSERT before commit; PostgreSQL backend=%d state=%s transaction_open=%t", c.command.Process.Pid, pid, state, transactionOpen)
	return pid
}

func (*replicaPublicAPIChild) awaitDatabaseDisconnect(ctx context.Context, t *testing.T, admin *sql.DB, pid uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		var count int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Logf("killed API child's PostgreSQL backend=%d disconnected before survivor assertions", pid)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("killed API child's PostgreSQL backend remained connected")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type replicaPublicChildTracer struct {
	*replicaPublicTransactionBarrier
	pid atomic.Uint32
}

func (b *replicaPublicChildTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	b.pid.Store(conn.PgConn().PID())
	b.replicaPublicTransactionBarrier.TraceQueryEnd(ctx, conn, data)
}

func runReplicaPublicAPIChild(t *testing.T) {
	dir := os.Getenv(replicaPublicAPIChildDirEnv)
	directory, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tracer := &replicaPublicChildTracer{replicaPublicTransactionBarrier: &replicaPublicTransactionBarrier{}}
	config, err := pgx.ParseConfig(os.Getenv("TETRAL_REPLICA_PUBLIC_API_RUNTIME_URL"))
	if err != nil {
		t.Fatal("parse API child runtime database configuration")
	}
	config.Tracer = tracer
	pool := stdlib.OpenDB(*config)
	pool.SetMaxOpenConns(20)
	defer func() { _ = pool.Close() }()
	verifier, err := auth.NewInternalPrincipalVerifierFromBase64(os.Getenv("TETRAL_REPLICA_PUBLIC_API_PUBLIC_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	router, err := api.BuildRouter(ctx, api.RouterConfig{RuntimeClient: dbconnect.NewClientForTesting(pool), RawDatabase: pool, VaultKey: sdkIntegrationVaultKey, DataDir: dir, Env: sdkIntegrationEnv{"TETRAL_DEFAULT_ENVIRONMENT_ARTIFACT_REF": "artifact_replica_public"}, PrincipalVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := router.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: router, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer func() { _ = server.Close() }()
	var reached <-chan struct{}
	var release func()
	var controlMu sync.Mutex
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controlMu.Lock()
		defer controlMu.Unlock()
		switch r.URL.Path {
		case "/arm":
			raw, err := io.ReadAll(io.LimitReader(r.Body, 256))
			if err != nil {
				http.Error(w, "arm body", 400)
				return
			}
			reached, release = tracer.arm(t, string(raw))
		case "/release":
			if release != nil {
				release()
			}
		case "/reached":
			select {
			case <-reached:
				_, _ = io.WriteString(w, "true")
			default:
				_, _ = io.WriteString(w, "false")
			}
		case "/database-pid":
			_ = json.NewEncoder(w).Encode(tracer.pid.Load())
		default:
			http.NotFound(w, r)
		}
	}))
	defer control.Close()
	ready := replicaPublicAPIReady{URL: "http://" + listener.Addr().String(), ControlURL: control.URL, PID: os.Getpid()}
	if err := pool.QueryRowContext(ctx, `SELECT current_user,current_database()`).Scan(&ready.Role, &ready.Database); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	raw, _ := json.Marshal(ready)
	if err := directory.WriteFile("ready.tmp", raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := directory.Rename("ready.tmp", "ready.json"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
		_ = server.Close()
		<-done
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	}
}
