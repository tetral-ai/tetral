// Package workload owns shared service lifecycle concerns.
package workload

import (
	"context"
	"database/sql"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	stdmetrics "runtime/metrics"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// NewLogger builds the production *slog.Logger every workload command shares.
// Service identity is attached once here via logger.With so every line carries
// service.name/deployment.environment/service.version without per-call repetition.
// A nil writer falls back to os.Stderr; empty identity fields take the same
// defaults the command config layer applies.
func NewLogger(writer io.Writer, serviceName string, deploymentEnvironment string, serviceVersion string) *slog.Logger {
	if writer == nil {
		writer = os.Stderr
	}
	if serviceName == "" {
		serviceName = "unknown"
	}
	if deploymentEnvironment == "" {
		deploymentEnvironment = "local"
	}
	if serviceVersion == "" {
		serviceVersion = "unknown"
	}
	return slog.New(slog.NewJSONHandler(writer, nil)).With(
		slog.String("service.name", serviceName),
		slog.String("deployment.environment", deploymentEnvironment),
		slog.String("service.version", serviceVersion),
	)
}

// Readiness tracks whether a workload can receive traffic.
//
// A workload may register a single readiness dependency (a live ready-check such as a
// Kubernetes cache sync state). When set, the workload is serving-ready only while the
// lifecycle state is ready AND the dependency reports ready. Shutdown state always wins:
// once draining begins the workload is unready regardless of the dependency. The hook is
// generic on purpose — internal/workload owns lifecycle only and holds no workload-
// specific behavior.
type Readiness struct {
	state      atomic.Int32
	dependency func() bool
}

const (
	readinessNotReady int32 = iota
	readinessReady
	readinessShuttingDown
)

// NewReadiness creates a readiness tracker that starts false.
func NewReadiness() *Readiness {
	return &Readiness{}
}

// WithReadinessDependency registers a live ready-check that must also pass for the
// workload to report ready. It is composed with lifecycle state, never replacing it.
func (r *Readiness) WithReadinessDependency(dependency func() bool) *Readiness {
	if r != nil {
		r.dependency = dependency
	}
	return r
}

// MarkReady marks dependencies ready.
func (r *Readiness) MarkReady() {
	if r != nil {
		r.state.Store(readinessReady)
	}
}

// BeginShutdown marks the workload unready for shutdown.
func (r *Readiness) BeginShutdown() {
	if r != nil {
		r.state.Store(readinessShuttingDown)
	}
}

// Ready reports whether the workload is serving-ready.
func (r *Readiness) Ready() bool {
	if r == nil || r.state.Load() != readinessReady {
		return false
	}
	return r.dependency == nil || r.dependency()
}

func (r *Readiness) message() string {
	if r == nil {
		return "not ready"
	}
	switch r.state.Load() {
	case readinessReady:
		if r.dependency != nil && !r.dependency() {
			return "not ready"
		}
		return "ready"
	case readinessShuttingDown:
		return "shutting down"
	default:
		return "not ready"
	}
}

// Metric is one Prometheus/OpenMetrics-compatible sample emitted by a service
// metrics collector. Names, types, and labels are internal constants; values
// must not contain request bodies, credentials, prompts, or other user data.
type Metric struct {
	Name   string
	Help   string
	Type   string
	Labels []MetricLabel
	Value  float64
}

type MetricLabel struct {
	Name  string
	Value string
}

type MetricsCollector func(context.Context) ([]Metric, error)

type healthRouterConfig struct {
	metricsCollectors []namedMetricsCollector
	httpMetrics       *HTTPMetrics
}

type namedMetricsCollector struct {
	name    string
	collect MetricsCollector
}

type HealthRouterOption func(*healthRouterConfig)

// WithMetricsCollector attaches a service-local metrics collector to /metrics.
// The collector name is used only for a safe internal error counter label.
func WithMetricsCollector(name string, collect MetricsCollector) HealthRouterOption {
	return func(cfg *healthRouterConfig) {
		if collect == nil {
			return
		}
		if name == "" {
			name = "unnamed"
		}
		cfg.metricsCollectors = append(cfg.metricsCollectors, namedMetricsCollector{name: name, collect: collect})
	}
}

type DBStatsProvider interface {
	Stats() sql.DBStats
}

type HTTPMetrics struct {
	mu      sync.Mutex
	records map[httpMetricsKey]durationMetricsRecord
}

type httpMetricsKey struct {
	method     string
	statusCode string
}

type GRPCMetrics struct {
	mu      sync.Mutex
	records map[grpcMetricsKey]durationMetricsRecord
}

type grpcMetricsKey struct {
	method string
	code   string
}

type durationMetricsRecord struct {
	count      int64
	sumSeconds float64
}

func NewHTTPMetrics() *HTTPMetrics {
	return &HTTPMetrics{records: map[httpMetricsKey]durationMetricsRecord{}}
}

func (m *HTTPMetrics) ObserveHTTPRequest(method string, statusCode int, duration time.Duration) {
	if m == nil {
		return
	}
	key := httpMetricsKey{
		method:     defaultString(method, "UNKNOWN"),
		statusCode: fmt.Sprintf("%d", statusCode),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.records[key]
	record.count++
	record.sumSeconds += duration.Seconds()
	m.records[key] = record
}

func (m *HTTPMetrics) Collector() MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		keys := make([]httpMetricsKey, 0, len(m.records))
		for key := range m.records {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i int, j int) bool {
			if keys[i].method == keys[j].method {
				return keys[i].statusCode < keys[j].statusCode
			}
			return keys[i].method < keys[j].method
		})
		metrics := make([]Metric, 0, len(keys)*2)
		for _, key := range keys {
			record := m.records[key]
			labels := []MetricLabel{{Name: "method", Value: key.method}, {Name: "status_code", Value: key.statusCode}}
			metrics = append(metrics,
				Metric{Name: "http_request_duration_seconds_count", Help: "Observed HTTP request count.", Type: "counter", Labels: labels, Value: float64(record.count)},
				Metric{Name: "http_request_duration_seconds_sum", Help: "Observed HTTP request duration seconds.", Type: "counter", Labels: labels, Value: record.sumSeconds},
			)
		}
		return metrics, nil
	}
}

func NewGRPCMetrics() *GRPCMetrics {
	return &GRPCMetrics{records: map[grpcMetricsKey]durationMetricsRecord{}}
}

func (m *GRPCMetrics) ObserveGRPCRequest(method string, code string, duration time.Duration) {
	if m == nil {
		return
	}
	key := grpcMetricsKey{
		method: defaultString(method, "unknown"),
		code:   defaultString(code, "Unknown"),
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	record := m.records[key]
	record.count++
	record.sumSeconds += duration.Seconds()
	m.records[key] = record
}

func (m *GRPCMetrics) Collector() MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if m == nil {
			return nil, nil
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		keys := make([]grpcMetricsKey, 0, len(m.records))
		for key := range m.records {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i int, j int) bool {
			if keys[i].method == keys[j].method {
				return keys[i].code < keys[j].code
			}
			return keys[i].method < keys[j].method
		})
		metrics := make([]Metric, 0, len(keys)*2)
		for _, key := range keys {
			record := m.records[key]
			labels := []MetricLabel{{Name: "grpc_code", Value: key.code}, {Name: "grpc_method", Value: key.method}}
			metrics = append(metrics,
				Metric{Name: "grpc_request_duration_seconds_count", Help: "Observed gRPC request count.", Type: "counter", Labels: labels, Value: float64(record.count)},
				Metric{Name: "grpc_request_duration_seconds_sum", Help: "Observed gRPC request duration seconds.", Type: "counter", Labels: labels, Value: record.sumSeconds},
			)
		}
		return metrics, nil
	}
}

func WithHTTPMetrics(metrics *HTTPMetrics) HealthRouterOption {
	return func(cfg *healthRouterConfig) {
		cfg.httpMetrics = metrics
	}
}

func DBStatsMetrics(pool string, provider DBStatsProvider) MetricsCollector {
	return func(context.Context) ([]Metric, error) {
		if provider == nil {
			return nil, nil
		}
		stats := provider.Stats()
		labels := []MetricLabel{{Name: "pool", Value: defaultString(pool, "default")}}
		return []Metric{
			{Name: "db_pool_open_connections", Help: "Open database connections.", Type: "gauge", Labels: labels, Value: float64(stats.OpenConnections)},
			{Name: "db_pool_in_use_connections", Help: "Database connections currently in use.", Type: "gauge", Labels: labels, Value: float64(stats.InUse)},
			{Name: "db_pool_idle_connections", Help: "Idle database connections.", Type: "gauge", Labels: labels, Value: float64(stats.Idle)},
			{Name: "db_pool_wait_count_total", Help: "Total waits for a database connection.", Type: "counter", Labels: labels, Value: float64(stats.WaitCount)},
			{Name: "db_pool_wait_duration_seconds_total", Help: "Cumulative database connection wait duration.", Type: "counter", Labels: labels, Value: stats.WaitDuration.Seconds()},
		}, nil
	}
}

// HealthRouter returns a small router for health/readiness probes.
func HealthRouter(readiness *Readiness, options ...HealthRouterOption) http.Handler {
	cfg := healthRouterConfig{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		message := readiness.message()
		if readiness.Ready() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write([]byte(message + "\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(metricsText(r.Context(), cfg.metricsCollectors)))
	})
	if cfg.httpMetrics == nil {
		return mux
	}
	return observeHTTPMetrics(mux, cfg.httpMetrics)
}

func observeHTTPMetrics(next http.Handler, metrics *HTTPMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tracker := &statusTrackingWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(tracker, r)
		metrics.ObserveHTTPRequest(r.Method, tracker.status, time.Since(start))
	})
}

type statusTrackingWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusTrackingWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusTrackingWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

// RuntimeMetricsTextWith renders Go runtime metrics plus already-collected
// service-local samples for services that own a custom metrics handler.
func RuntimeMetricsTextWith(extra []Metric) string {
	return runtimeMetricsText(extra)
}

func runtimeMetricsText(extra []Metric) string {
	samples := []stdmetrics.Sample{
		{Name: "/sched/goroutines:goroutines"},
		{Name: "/sched/gomaxprocs:threads"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/cpu/classes/gc/pause:cpu-seconds"},
	}
	stdmetrics.Read(samples)
	var builder strings.Builder
	emittedHeaders := map[string]bool{}
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_goroutines", Help: "Current number of goroutines.", Type: "gauge", Value: metricSampleValue(samples[0])})
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_sched_gomaxprocs_threads", Help: "Current runtime GOMAXPROCS scheduler thread setting.", Type: "gauge", Value: metricSampleValue(samples[1])})
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_heap_alloc_bytes", Help: "Bytes allocated and still in use on the Go heap.", Type: "gauge", Value: metricSampleValue(samples[2])})
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_heap_free_bytes", Help: "Free bytes reserved for the Go heap.", Type: "gauge", Value: metricSampleValue(samples[3])})
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_gc_cycles_total", Help: "Completed Go GC cycles.", Type: "counter", Value: metricSampleValue(samples[4])})
	writeMetric(&builder, emittedHeaders, Metric{Name: "go_gc_pause_seconds_total", Help: "Cumulative Go GC pause CPU seconds.", Type: "counter", Value: metricSampleValue(samples[5])})
	for _, metric := range extra {
		writeMetric(&builder, emittedHeaders, metric)
	}
	return builder.String()
}

func metricsText(ctx context.Context, collectors []namedMetricsCollector) string {
	extra := []Metric{}
	for _, collector := range collectors {
		metrics, err := collector.collect(ctx)
		if err != nil {
			extra = append(extra, Metric{
				Name:   "tetral_metrics_collector_errors_total",
				Help:   "Metrics collector failures.",
				Type:   "counter",
				Labels: []MetricLabel{{Name: "collector", Value: collector.name}},
				Value:  1,
			})
			continue
		}
		extra = append(extra, metrics...)
	}
	return runtimeMetricsText(extra)
}

func metricSampleValue(sample stdmetrics.Sample) float64 {
	switch sample.Value.Kind() {
	case stdmetrics.KindUint64:
		return float64(sample.Value.Uint64())
	case stdmetrics.KindFloat64:
		return sample.Value.Float64()
	default:
		return 0
	}
}

func writeMetric(builder *strings.Builder, emittedHeaders map[string]bool, metric Metric) {
	if metric.Name == "" {
		return
	}
	metricType := defaultString(metric.Type, "gauge")
	if !emittedHeaders[metric.Name] {
		_, _ = fmt.Fprintf(builder, "# HELP %s %s\n# TYPE %s %s\n", metric.Name, defaultString(metric.Help, metric.Name), metric.Name, metricType)
		emittedHeaders[metric.Name] = true
	}
	_, _ = fmt.Fprintf(builder, "%s%s %g\n", metric.Name, metricLabels(metric.Labels), metric.Value)
}

func metricLabels(labels []MetricLabel) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := append([]MetricLabel(nil), labels...)
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})
	parts := make([]string, 0, len(sorted))
	for _, label := range sorted {
		if label.Name == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf(`%s="%s"`, label.Name, escapeMetricLabelValue(label.Value)))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeMetricLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// Config controls a workload HTTP lifecycle.
type Config struct {
	ServiceName           string
	DeploymentEnvironment string
	ServiceVersion        string
	ListenAddress         string
	ListenConfigKey       string
	Listen                func(network string, address string) (net.Listener, error)
	Listener              net.Listener
	Handler               http.Handler
	Readiness             *Readiness
	ReadHeaderTimeout     time.Duration
	ShutdownTimeout       time.Duration
	Logger                *slog.Logger
}

// Run validates config, serves HTTP, and gracefully drains on ctx cancellation.
func Run(ctx context.Context, cfg Config) error {
	if cfg.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}
	if cfg.Handler == nil {
		return fmt.Errorf("handler is required")
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = 10 * time.Second
	}
	if cfg.DeploymentEnvironment == "" {
		cfg.DeploymentEnvironment = "local"
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "unknown"
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = ":8080"
	}
	if cfg.ListenConfigKey == "" {
		cfg.ListenConfigKey = "listen.address"
	}
	if cfg.Logger == nil {
		cfg.Logger = NewLogger(os.Stderr, cfg.ServiceName, cfg.DeploymentEnvironment, cfg.ServiceVersion)
	}
	if cfg.Readiness == nil {
		cfg.Readiness = NewReadiness()
	}
	listener := cfg.Listener
	if listener == nil {
		listen := cfg.Listen
		if listen == nil {
			listen = net.Listen
		}
		cfg.Logger.Info("workload.listener.starting",
			slog.String("operation", "workload.listener"),
			slog.String("event.kind", "listener_starting"),
			slog.String("component", "workload"),
			slog.String("listener.transport", "tcp"),
			slog.String("readiness.state", cfg.Readiness.message()),
		)
		created, err := listen("tcp", cfg.ListenAddress)
		if err != nil {
			cfg.Logger.Error("workload.listener.failed", StartupFailureAttrs(err,
				slog.String("component", "workload"),
				slog.String("listener.transport", "tcp"),
				slog.String("readiness.state", cfg.Readiness.message()),
			)...)
			return err
		}
		listener = created
	}
	if ctx == nil {
		ctx = context.Background()
	}
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Handler:           cfg.Handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	cfg.Logger.Info("workload.started",
		slog.String("operation", "workload.lifecycle"),
		slog.String("event.kind", "started"),
		slog.String("component", "workload"),
		slog.String("listener.transport", "tcp"),
		slog.String("readiness.state", cfg.Readiness.message()),
	)
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
		close(serverErr)
	}()

	var serveErr error
	select {
	case <-signalCtx.Done():
		if cfg.Readiness != nil {
			cfg.Readiness.BeginShutdown()
		}
		cfg.Logger.Info("workload.shutdown.starting",
			slog.String("operation", "workload.lifecycle"),
			slog.String("event.kind", "shutdown_starting"),
			slog.String("component", "workload"),
			slog.String("readiness.state", cfg.Readiness.message()),
		)
	case serveErr = <-serverErr:
		// A serve error drains like a signal: flip /ready false before draining so
		// load balancers stop routing during the drain window, matching the signal branch.
		if cfg.Readiness != nil {
			cfg.Readiness.BeginShutdown()
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	if serveErr != nil {
		cfg.Logger.Error("workload.server.failed", StartupFailureAttrs(serveErr,
			slog.String("component", "workload"),
			slog.String("readiness.state", cfg.Readiness.message()),
		)...)
		return serveErr
	}
	cfg.Logger.Info("workload.shutdown.complete",
		slog.String("operation", "workload.lifecycle"),
		slog.String("event.kind", "shutdown_complete"),
		slog.String("component", "workload"),
		slog.String("readiness.state", cfg.Readiness.message()),
	)
	return shutdownErr
}

// ForbiddenImportPrefixes returns packages the lifecycle package must not import.
// internal/workload owns process lifecycle only, never public API domain services
// or workload-specific control-plane behavior. The set covers both contract
// categories: public-API domain packages and workload-specific / control-plane
// packages. internal/dbconnect is a pre-existing pure-infrastructure guard kept on
// top of the contract floor (the floor is a minimum, not a maximum).
func ForbiddenImportPrefixes() []string {
	return []string{
		// Public-API domain packages.
		"github.com/tetral-ai/tetral/internal/agent",
		"github.com/tetral-ai/tetral/internal/auth",
		"github.com/tetral-ai/tetral/internal/blob",
		"github.com/tetral-ai/tetral/internal/encryption",
		"github.com/tetral-ai/tetral/internal/environment",
		"github.com/tetral-ai/tetral/internal/eventstream",
		"github.com/tetral-ai/tetral/internal/files",
		"github.com/tetral-ai/tetral/internal/httpapi",
		"github.com/tetral-ai/tetral/internal/memory",
		"github.com/tetral-ai/tetral/internal/sandbox",
		"github.com/tetral-ai/tetral/internal/session",
		"github.com/tetral-ai/tetral/internal/sessionevent",
		"github.com/tetral-ai/tetral/internal/skill",
		"github.com/tetral-ai/tetral/internal/vault",
		"github.com/tetral-ai/tetral/internal/workspace",
		// Workload-specific / control-plane packages.
		"github.com/tetral-ai/tetral/services/event-stream",
		"github.com/tetral-ai/tetral/internal/internalgrpc",
		"github.com/tetral-ai/tetral/internal/kubernetes",
		"github.com/tetral-ai/tetral/internal/tetralapi",
		"github.com/tetral-ai/tetral/services/bridge",
		// Pre-existing pure-infrastructure guard retained on top of the floor.
		"github.com/tetral-ai/tetral/internal/dbconnect",
	}
}

// AssertNoForbiddenImports scans production Go files in dir for forbidden imports.
func AssertNoForbiddenImports(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := dir + "/" + name
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, forbidden := range ForbiddenImportPrefixes() {
				if strings.HasPrefix(value, forbidden) {
					return fmt.Errorf("%s imports forbidden package %s", name, value)
				}
			}
		}
	}
	return nil
}
