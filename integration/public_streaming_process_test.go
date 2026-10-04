package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/integration/transporttest"
	"github.com/tetral-ai/tetral/internal/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/testinfra"
	eventstream "github.com/tetral-ai/tetral/services/event-stream"
)

// Actual command processes own independent subscribers, readers and sockets.
// Only discovery is controlled by the fixture's forwarding target.
type publicStreamProcess struct {
	command          *exec.Cmd
	output           lockedBuffer
	joined           chan error
	address, metrics string
	stopOnce         sync.Once
}

func publicFreeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
func buildPublicStreamCommand(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "event-stream")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./services/event-stream/cmd/event-stream") //nolint:gosec // Repository-owned command.
	command.Dir = ".."
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build actual Event Stream: %v: %s", err, output)
	}
	return binary
}
func startPublicStreamProcess(t *testing.T, binary string, database *sql.DB, publicKey string, postgres *transporttest.PostgreSQL, broker testinfra.NATSFixture, overrides map[string]string) *publicStreamProcess {
	t.Helper()
	p := &publicStreamProcess{joined: make(chan error, 1)}
	p.address = "http://" + publicFreeAddress(t)
	p.metrics = "http://" + publicFreeAddress(t)
	values := map[string]string{
		"TETRAL_EVENT_STREAM_HTTP_ADDR":                 strings.TrimPrefix(p.address, "http://"),
		"TETRAL_EVENT_STREAM_METRICS_ADDR":              strings.TrimPrefix(p.metrics, "http://"),
		"TETRAL_EVENT_STREAM_DATABASE_URL":              storagetest.RuntimeDatabaseURL(t, database),
		"TETRAL_DATABASE_TLS_CA_PATH":                   filepath.Join(postgres.Directory, "ca.pem"),
		"TETRAL_DATABASE_TLS_SERVER_NAME":               "postgres.transport.test",
		"TETRAL_AUTH_INTERNAL_PRINCIPAL_PUBLIC_KEY_B64": publicKey,
		"TETRAL_DEPLOYMENT_ENVIRONMENT":                 "test",
		"TETRAL_SERVICE_VERSION":                        "public-streaming-fixture",
		"TETRAL_NATS_SERVERS":                           strings.Join(broker.Servers, ","),
		"TETRAL_NATS_USER_PATH":                         broker.Subscriber.UserPath,
		"TETRAL_NATS_PASSWORD_PATH":                     broker.Subscriber.PasswordPath,
		"TETRAL_EVENT_STREAM_POLL_INTERVAL_MS":          "5",
	}
	if broker.Subscriber.TLS.CAPath != "" {
		values["TETRAL_NATS_TLS_CA_PATH"] = broker.Subscriber.TLS.CAPath
		values["TETRAL_NATS_TLS_CERT_PATH"] = broker.Subscriber.TLS.CertPath
		values["TETRAL_NATS_TLS_KEY_PATH"] = broker.Subscriber.TLS.KeyPath
	}
	for key, value := range overrides {
		values[key] = value
	}
	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, exists := values[key]; !exists {
			environment = append(environment, entry)
		}
	}
	for key, value := range values {
		environment = append(environment, key+"="+value)
	}
	p.command = exec.Command(binary)
	p.command.Env = environment
	p.command.Stderr = &p.output
	p.command.Stdout = &p.output
	if err := p.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.joined <- p.command.Wait() }()
	t.Cleanup(func() { p.stop(t) })
	client := &http.Client{Timeout: time.Second}
	publicWait(t, "actual Event Stream command readiness", func() bool {
		response, err := client.Get(p.address + "/ready")
		if err != nil {
			return false
		}
		defer func() { _ = response.Body.Close() }()
		return response.StatusCode == http.StatusOK
	})
	return p
}
func (p *publicStreamProcess) stop(t *testing.T) {
	t.Helper()
	p.stopOnce.Do(func() {
		if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("signal Event Stream: %v", err)
		}
		select {
		case err := <-p.joined:
			if err != nil {
				t.Errorf("Event Stream command shutdown: %v: %s", err, p.output.String())
			}
		case <-time.After(10 * time.Second):
			_ = p.command.Process.Kill()
			<-p.joined
			t.Error("Event Stream command did not join by existing shutdown budget")
		}
	})
}
func (p *publicStreamProcess) metric(t *testing.T, name string) float64 {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(p.metrics + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("actual Event Stream metric missing: %s", name)
	return 0
}

type publicStreamProcessGroup struct {
	mu            sync.Mutex
	processes     []*publicStreamProcess
	target        int
	binary        string
	database      *sql.DB
	publicKey     string
	postgres      *transporttest.PostgreSQL
	broker        testinfra.NATSFixture
	streamHeaders http.Header
}

func (g *publicStreamProcessGroup) serve(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	target := g.processes[g.target].address
	if strings.HasSuffix(r.URL.Path, "/stream") {
		g.streamHeaders = r.Header.Clone()
	}
	g.mu.Unlock()
	endpoint, err := url.Parse(target)
	if err != nil {
		http.Error(w, "fixture target invalid", 500)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	proxy.FlushInterval = -1
	proxy.ServeHTTP(w, r)
}
func (g *publicStreamProcessGroup) selectProcess(index int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.target = index
}
func newPublicStreamingProcesses(t *testing.T, scenario string, count int, overrides map[string]string) (*publicStreamingHarness, *publicStreamProcessGroup) {
	t.Helper()
	postgres := &transporttest.PostgreSQL{Directory: os.Getenv("TETRAL_PREVIEW_PROCESS_PG_CERTS"), URL: os.Getenv("TETRAL_PREVIEW_PROCESS_PG_URL")}
	if postgres.Directory == "" || postgres.URL == "" {
		t.Fatal("process composition requires isolated TLS PostgreSQL child")
	}
	broker, err := testinfra.LoadNATSFixture()
	if err != nil {
		t.Fatal(err)
	}
	group := &publicStreamProcessGroup{binary: buildPublicStreamCommand(t), postgres: postgres, broker: broker}
	gateway := map[string]any{"holdEveryRequest": true, "followupScenario": scenario}
	if scenario == "public-cycle" {
		gateway["recordContext"] = false
		gateway["sessionScenarioPlans"] = true
	}
	h := newPublicStreamingHarness(t, scenario, publicStreamingOptions{broker: &broker, gateway: gateway, eventsFactory: func(t *testing.T, pools *storagetest.WorkloadDB, _ eventstream.Reader, _ *auth.InternalPrincipalVerifier, publicKey string) http.Handler {
		group.database = pools.OpenWorkload(t, "event_stream", nil)
		group.publicKey = publicKey
		for range count {
			group.processes = append(group.processes, startPublicStreamProcess(t, group.binary, group.database, publicKey, postgres, broker, overrides))
		}
		return http.HandlerFunc(group.serve)
	}})
	t.Logf("actual Event Stream processes=%d database_role=event_stream", count)
	return h, group
}
func (g *publicStreamProcessGroup) restart(t *testing.T, index int) {
	t.Helper()
	g.processes[index].stop(t)
	replacement := startPublicStreamProcess(t, g.binary, g.database, g.publicKey, g.postgres, g.broker, nil)
	g.mu.Lock()
	g.processes[index] = replacement
	g.mu.Unlock()
}
func publicAssertProcessBaseline(t *testing.T, p *publicStreamProcess) {
	t.Helper()
	publicWait(t, "process preview ownership baseline", func() bool {
		return p.metric(t, "event_stream_preview_viewers") == 0 && p.metric(t, "event_stream_preview_subscriptions") == 0 && p.metric(t, "event_stream_preview_pending_bytes") == 0 && p.metric(t, "event_stream_preview_active_requests") == 0 && p.metric(t, "event_stream_active_sse_viewers") == 0
	})
	publicLogAssertion(t, fmt.Sprintf("actual-process-%d-preview-cleanup", p.command.Process.Pid))
}

// storagetest intentionally owns one PostgreSQL registry per Go process. Run
// these command-lifecycle compositions in a fresh test process, with their TLS
// database selected before any store helper initializes its process owner.
func publicIsolatedProcessCase(t *testing.T, body func(*testing.T)) {
	t.Helper()
	if os.Getenv("TETRAL_PREVIEW_PROCESS_CASE") == t.Name() {
		// Select the one database owner before any subcase initializes the
		// process-wide storagetest registry, including in-process readers.
		// Commands additionally verify the CA/hostname via OpenProtectedDSN.
		dsn, err := url.Parse(os.Getenv("TETRAL_PREVIEW_PROCESS_PG_URL"))
		if err != nil || dsn.Host == "" {
			t.Fatal("isolated TLS PostgreSQL URL absent or invalid")
		}
		query := dsn.Query()
		query.Set("sslmode", "require")
		dsn.RawQuery = query.Encode()
		t.Setenv(storagetest.EnvTestDatabaseURL, dsn.String())
		t.Setenv(storagetest.EnvTestRunID, "")
		body(t)
		return
	}
	postgres := transporttest.NewPostgreSQL(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^"+t.Name()+"$", "-test.v") //nolint:gosec // Current immutable test executable and fixed root test.
	command.Env = append(os.Environ(), "TETRAL_PREVIEW_PROCESS_CASE="+t.Name(), "TETRAL_PREVIEW_PROCESS_PG_CERTS="+postgres.Directory, "TETRAL_PREVIEW_PROCESS_PG_URL="+postgres.URL)
	output, err := command.CombinedOutput()
	// Actual child assertions, test names and failures remain in the parent raw
	// Go evidence; credentials exist only in the child's private environment.
	t.Logf("isolated public process case output:\n%s", output)
	if err != nil {
		t.Fatalf("actual public process composition failed: %v", err)
	}
	if !strings.Contains(string(output), "--- PASS: "+t.Name()) || !strings.Contains(string(output), "public_streaming_sdk_assertion=") {
		t.Fatal("isolated composition lacks executed named assertions")
	}
}
