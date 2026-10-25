package workload

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Telemetry is one process's recording owner: its service, logger, tracer and
// the metric families the library records for every service. A process has
// one, created with its ProcessLogger and installed with InstallDefaultLogger.
type Telemetry struct {
	service    string
	logger     *slog.Logger
	tracing    *tracing
	metrics    *Registry
	slowQuery  time.Duration
	operations *Histogram
	long       *Histogram
	phases     *Histogram
	coldStarts *Histogram
	statements *Histogram
	shutdown   func(context.Context)
}

// TelemetryOptions builds a Telemetry outside a ProcessLogger, for tests and
// embedded components.
type TelemetryOptions struct {
	Service     string
	Environment string
	Version     string
	Logger      *slog.Logger
	// Exporter receives sampled spans; nil keeps spans and trace IDs local.
	Exporter sdktrace.SpanExporter
	// SyncExporter receives every ended sampled span before End returns,
	// outside the bounded queue; tests read spans from it.
	SyncExporter sdktrace.SpanExporter
	// SampleRatio is the share of root traces sampled, from 0 to 1; an
	// unsampled trace still gives records its trace ID.
	SampleRatio float64
	SlowQuery   time.Duration
}

func NewTelemetry(options TelemetryOptions) *Telemetry {
	if options.SlowQuery <= 0 {
		options.SlowQuery = defaultSlowQuery
	}
	if options.Logger == nil {
		options.Logger = NewLogger(discardWriter{}, options.Service, options.Environment, options.Version)
	}
	t := &Telemetry{
		service:   options.Service,
		logger:    options.Logger,
		tracing:   newTracing(tracingOptions{service: options.Service, environment: defaultString(options.Environment, DefaultDeploymentEnvironment), version: defaultString(options.Version, DefaultServiceVersion), sampleRatio: options.SampleRatio, exporter: options.Exporter}),
		metrics:   NewRegistry(options.Service),
		slowQuery: options.SlowQuery,
	}
	t.operations = t.metrics.Histogram("tetral_operation_duration_seconds", "Duration of a completed operation.", "service", "operation", "outcome")
	t.long = t.metrics.Histogram("tetral_long_operation_duration_seconds", "Duration of a completed long operation.", "service", "operation", "outcome")
	t.phases = t.metrics.Histogram("cold_start_phase_seconds", "Duration of one completed cold start phase.", "service", "kind", "phase")
	t.coldStarts = t.metrics.Histogram("cold_start_seconds", "Duration of a completed cold start, in total and Engine's part.", "service", "kind", "scope")
	t.statements = t.metrics.Histogram("db_client_operation_duration_seconds", "Duration of one named database statement or lock acquisition.", "service", "operation", "outcome")
	if options.SyncExporter != nil {
		t.tracing.provider.RegisterSpanProcessor(sdktrace.NewSimpleSpanProcessor(options.SyncExporter))
	}
	t.shutdown = t.tracing.shutdown
	return t
}

type discardWriter struct{}

func (discardWriter) Write(line []byte) (int, error) { return len(line), nil }

const defaultSlowQuery = 250 * time.Millisecond

var installedTelemetry atomic.Pointer[Telemetry]

var fallbackTelemetry = NewTelemetry(TelemetryOptions{Service: "unknown"})

// InstallTelemetry makes t the process's recording owner and returns the
// restoration of the previous one.
func InstallTelemetry(t *Telemetry) func() {
	previous := installedTelemetry.Swap(t)
	return func() { installedTelemetry.Store(previous) }
}

// CurrentTelemetry returns the installed owner, or a quiet one that records
// nowhere when none is installed.
func CurrentTelemetry() *Telemetry {
	if t := installedTelemetry.Load(); t != nil {
		return t
	}
	return fallbackTelemetry
}

func (t *Telemetry) Service() string { return t.service }

// Registry is the process's own metric registry; families a service records
// alone are registered on it too, so /metrics renders them once.
func (t *Telemetry) Registry() *Registry { return t.metrics }

// Tracer starts the spans of this process.
func (t *Telemetry) Tracer() trace.Tracer { return t.tracing.tracer }

// SlowQueryThreshold is the database statement duration above which the
// statement writes db.query.slow.
func (t *Telemetry) SlowQueryThreshold() time.Duration { return t.slowQuery }

// Logger writes the library's own records for this process.
func (t *Telemetry) Logger() *slog.Logger { return t.logger }

// Collector renders the library's families and the span drop counter.
func (t *Telemetry) Collector() MetricsCollector {
	return func(ctx context.Context) ([]Metric, error) {
		metrics, _ := t.metrics.Collector()(ctx)
		return append(metrics, Metric{Name: "tetral_diagnostic_spans_dropped_total", Help: "Spans refused because the export queue was full.", Type: "counter", Value: float64(t.tracing.droppedSpans())}), nil
	}
}

// Shutdown exports the spans still queued within ctx's budget.
func (t *Telemetry) Shutdown(ctx context.Context) {
	if t != nil && t.shutdown != nil {
		t.shutdown(ctx)
	}
}

// ObserveStatement records one named database statement, or the wait to
// acquire a lock, that started at started: one duration sample and, above
// the slow-query threshold, one WARN db.query.slow that never carries the
// statement's text or arguments.
func (t *Telemetry) ObserveStatement(ctx context.Context, operation string, started time.Time, err error) {
	elapsed := time.Since(started)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	t.statements.Observe(elapsed.Seconds(), t.service, operation, outcome)
	if elapsed > t.slowQuery {
		t.logger.LogAttrs(ctx, slog.LevelWarn, "db.query.slow", slog.String("operation", operation), slog.Int64("duration_ms", elapsed.Milliseconds()))
	}
}

// LibraryMetricsText renders the installed Telemetry's families, for services
// whose metrics port does not use HealthRouter or RuntimeMetricsTextWith.
func LibraryMetricsText() string {
	samples, _ := CurrentTelemetry().Collector()(context.Background())
	var builder strings.Builder
	headers := map[string]bool{}
	for _, sample := range groupMetricFamilies(samples) {
		writeMetric(&builder, headers, sample)
	}
	return builder.String()
}
