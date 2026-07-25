package workload_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/workload"
)

func TestWorkloadHealthAndReadinessLifecycle(t *testing.T) {
	readiness := workload.NewReadiness()
	handler := workload.HealthRouter(readiness)

	assertWorkloadResponse(t, handler, "/health", http.StatusOK, "ok")
	assertWorkloadResponse(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")

	readiness.MarkReady()
	assertWorkloadResponse(t, handler, "/ready", http.StatusOK, "ready")

	readiness.BeginShutdown()
	assertWorkloadResponse(t, handler, "/ready", http.StatusServiceUnavailable, "shutting down")
}

func TestWorkloadHealthRouterServesRuntimeMetrics(t *testing.T) {
	handler := workload.HealthRouter(workload.NewReadiness())
	request, err := http.NewRequest(http.MethodGet, "/metrics", nil)
	if err != nil {
		t.Fatalf("new metrics request: %v", err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d; want 200", recorder.Code)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("/metrics content-type = %q; want text/plain", contentType)
	}
	body := recorder.Body.String()
	for _, metric := range []string{"go_goroutines", "go_sched_gomaxprocs_threads", "go_heap_alloc_bytes", "go_gc_cycles_total"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("/metrics body missing %q:\n%s", metric, body)
		}
	}
}

func TestWorkloadHealthRouterServesServiceCollectors(t *testing.T) {
	httpMetrics := workload.NewHTTPMetrics()
	httpMetrics.ObserveHTTPRequest(http.MethodPost, http.StatusCreated, 1500*time.Millisecond)
	grpcMetrics := workload.NewGRPCMetrics()
	grpcMetrics.ObserveGRPCRequest("/tetral.test.v1.Service/Call", "OK", 2*time.Second)
	handler := workload.HealthRouter(workload.NewReadiness(),
		workload.WithMetricsCollector("http", httpMetrics.Collector()),
		workload.WithMetricsCollector("grpc", grpcMetrics.Collector()),
		workload.WithMetricsCollector("database", workload.DBStatsMetrics("runtime", fakeDBStatsProvider{stats: sql.DBStats{
			OpenConnections: 4,
			InUse:           2,
			Idle:            2,
			WaitCount:       7,
			WaitDuration:    3 * time.Second,
		}})),
		workload.WithMetricsCollector("queue", func(context.Context) ([]workload.Metric, error) {
			return []workload.Metric{{
				Name:   "queue_pending_jobs",
				Help:   "Pending queue jobs.",
				Type:   "gauge",
				Labels: []workload.MetricLabel{{Name: "kind", Value: "runtime_input"}},
				Value:  5,
			}}, nil
		}),
		workload.WithMetricsCollector("broken", func(context.Context) ([]workload.Metric, error) {
			return nil, errors.New("raw secret diagnostic must not be exposed")
		}),
	)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		`db_pool_open_connections{pool="runtime"} 4`,
		`db_pool_in_use_connections{pool="runtime"} 2`,
		`db_pool_wait_count_total{pool="runtime"} 7`,
		`db_pool_wait_duration_seconds_total{pool="runtime"} 3`,
		`http_request_duration_seconds_count{method="POST",status_code="201"} 1`,
		`http_request_duration_seconds_sum{method="POST",status_code="201"} 1.5`,
		`grpc_request_duration_seconds_count{grpc_code="OK",grpc_method="/tetral.test.v1.Service/Call"} 1`,
		`grpc_request_duration_seconds_sum{grpc_code="OK",grpc_method="/tetral.test.v1.Service/Call"} 2`,
		`queue_pending_jobs{kind="runtime_input"} 5`,
		`tetral_metrics_collector_errors_total{collector="broken"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics body missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "raw secret diagnostic") {
		t.Fatalf("/metrics leaked collector error text:\n%s", body)
	}
}

type fakeDBStatsProvider struct {
	stats sql.DBStats
}

func (p fakeDBStatsProvider) Stats() sql.DBStats { return p.stats }

func TestWorkloadHealthRouterRecordsHTTPMetrics(t *testing.T) {
	metrics := workload.NewHTTPMetrics()
	handler := workload.HealthRouter(workload.NewReadiness(),
		workload.WithHTTPMetrics(metrics),
		workload.WithMetricsCollector("http", metrics.Collector()),
	)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `http_request_duration_seconds_count{method="GET",status_code="200"} 1`) {
		t.Fatalf("/metrics body missing health request count:\n%s", body)
	}
}

func TestWorkloadReadinessDependencyComposesWithLifecycleState(t *testing.T) {
	dependencyReady := false
	readiness := workload.NewReadiness().WithReadinessDependency(func() bool { return dependencyReady })
	handler := workload.HealthRouter(readiness)

	// Lifecycle not yet ready: not ready regardless of dependency.
	assertWorkloadResponse(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")

	// Lifecycle ready but dependency not ready: still not ready.
	readiness.MarkReady()
	if readiness.Ready() {
		t.Fatal("readiness reported ready while dependency was not ready")
	}
	assertWorkloadResponse(t, handler, "/ready", http.StatusServiceUnavailable, "not ready")

	// Dependency ready and lifecycle ready: ready.
	dependencyReady = true
	if !readiness.Ready() {
		t.Fatal("readiness not ready while lifecycle and dependency were both ready")
	}
	assertWorkloadResponse(t, handler, "/ready", http.StatusOK, "ready")

	// Shutdown wins over a ready dependency.
	readiness.BeginShutdown()
	if readiness.Ready() {
		t.Fatal("readiness reported ready during shutdown")
	}
	assertWorkloadResponse(t, handler, "/ready", http.StatusServiceUnavailable, "shutting down")
}

func TestWorkloadInvalidConfigPreventsServing(t *testing.T) {
	listener := newWorkloadTestListener(t)
	closed := make(chan struct{})
	go func() {
		_, err := listener.Accept()
		if err != nil {
			close(closed)
		}
	}()

	err := workload.Run(context.Background(), workload.Config{
		Listener: listener,
		Handler:  http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}),
	})
	if err == nil {
		t.Fatal("Run accepted config without service name")
	}
	select {
	case <-closed:
		t.Fatal("invalid workload config closed or served listener")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWorkloadReadinessFlipsFalseDuringShutdown(t *testing.T) {
	readiness := workload.NewReadiness()
	readiness.MarkReady()
	listener := newWorkloadTestListener(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- workload.Run(ctx, workload.Config{
			ServiceName:     "api",
			Listener:        listener,
			Handler:         workload.HealthRouter(readiness),
			Readiness:       readiness,
			ShutdownTimeout: 2 * time.Second,
		})
	}()

	waitForWorkloadReady(t, listener.Addr().String())
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned error: %v", err)
			}
			if readiness.Ready() {
				t.Fatal("readiness remained true after shutdown")
			}
			return
		case <-deadline:
			t.Fatal("workload did not stop after cancellation")
		}
	}
}

func TestWorkloadDoesNotImportPublicAPIDomainPackages(t *testing.T) {
	for _, forbidden := range workload.ForbiddenImportPrefixes() {
		if strings.Contains(forbidden, "workload") {
			t.Fatalf("forbidden import guard is malformed: %q", forbidden)
		}
	}
	if err := workload.AssertNoForbiddenImports("."); err != nil {
		t.Fatalf("workload import guard failed: %v", err)
	}
}

// TestForbiddenImportPrefixesCoversContractScopedSet pins the explicit
// architecture-owned enumeration as a floor: every domain and workload-specific
// package that the lifecycle package must not import must be covered. Dropping
// any one entry from ForbiddenImportPrefixes() fails CI. The set is a floor,
// not a ceiling; pre-existing extras such as internal/dbconnect are allowed and
// asserted retained separately.
func TestForbiddenImportPrefixesCoversContractScopedSet(t *testing.T) {
	const importPrefix = "github.com/tetral-ai/tetral/internal/"
	// Public-API domain packages.
	domain := []string{
		"agent", "auth", "blob", "environment", "eventstream", "files", "memory", "sandbox",
		"session", "sessionevent", "skill", "vault", "httpapi", "workspace", "encryption",
	}
	// Workload-specific / control-plane packages.
	workloadSpecific := []string{
		"tetralapi", "internalgrpc", "kubernetes",
	}
	serviceSpecific := []string{
		"github.com/tetral-ai/tetral/services/bridge",
		"github.com/tetral-ai/tetral/services/event-stream",
	}

	covered := make(map[string]bool)
	for _, forbidden := range workload.ForbiddenImportPrefixes() {
		covered[forbidden] = true
	}

	for _, group := range [][]string{domain, workloadSpecific} {
		for _, pkg := range group {
			prefix := importPrefix + pkg
			if !covered[prefix] {
				t.Errorf("ForbiddenImportPrefixes() does not cover contract-scoped package %q", prefix)
			}
		}
	}
	for _, prefix := range serviceSpecific {
		if !covered[prefix] {
			t.Errorf("ForbiddenImportPrefixes() does not cover service package %q", prefix)
		}
	}
}

// TestForbiddenImportPrefixesRetainsPreExistingExtras locks the FLOOR-not-ceiling
// rule (M4): the pre-existing internal/dbconnect guard must never be removed by a
// fix that only adds the contract-scoped set.
func TestForbiddenImportPrefixesRetainsPreExistingExtras(t *testing.T) {
	const dbconnect = "github.com/tetral-ai/tetral/internal/dbconnect"
	for _, forbidden := range workload.ForbiddenImportPrefixes() {
		if forbidden == dbconnect {
			return
		}
	}
	t.Fatalf("ForbiddenImportPrefixes() dropped pre-existing guard %q", dbconnect)
}

// TestAssertNoForbiddenImportsRejectsNewlyGuardedDomainPackages proves
// AssertNoForbiddenImports errors on a production .go file that imports a
// guarded domain package.
func TestAssertNoForbiddenImportsRejectsNewlyGuardedDomainPackages(t *testing.T) {
	cases := []struct {
		name       string
		importPath string
	}{
		{name: "workspace", importPath: "github.com/tetral-ai/tetral/internal/workspace"},
		{name: "encryption", importPath: "github.com/tetral-ai/tetral/internal/encryption"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			dir := t.TempDir()
			source := "package syntheticleaf\n\nimport _ \"" + testCase.importPath + "\"\n"
			path := dir + "/synthetic.go"
			if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
				t.Fatalf("write synthetic source: %v", err)
			}
			if err := workload.AssertNoForbiddenImports(dir); err == nil {
				t.Fatalf("AssertNoForbiddenImports accepted a file importing %q", testCase.importPath)
			}
		})
	}
}

func assertWorkloadResponse(t *testing.T, handler http.Handler, path string, wantStatus int, wantBody string) {
	t.Helper()
	server := httptestLikeHandler{handler: handler}
	status, body := server.request(t, path)
	if status != wantStatus || body != wantBody {
		t.Fatalf("%s response = %d %q; want %d %q", path, status, body, wantStatus, wantBody)
	}
}

type httptestLikeHandler struct {
	handler http.Handler
}

func (h httptestLikeHandler) request(t *testing.T, path string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "http://example.invalid"+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	recorder := &workloadResponseRecorder{headers: http.Header{}}
	h.handler.ServeHTTP(recorder, request)
	return recorder.status, strings.TrimSpace(recorder.body.String())
}

type workloadResponseRecorder struct {
	headers http.Header
	status  int
	body    strings.Builder
}

func (r *workloadResponseRecorder) Header() http.Header { return r.headers }

func (r *workloadResponseRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.body.Write(data)
}

func (r *workloadResponseRecorder) WriteHeader(status int) { r.status = status }

func newWorkloadTestListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func waitForWorkloadReady(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + address + "/health")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("workload server did not start")
}
