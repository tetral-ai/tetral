// Package workload owns what every Go Engine process shares at its edges:
// listener and shutdown lifetime, readiness, the /metrics port, startup
// failure records, and the recording library through which every service
// writes its records, metric samples and spans. README.md states the
// recording rules; this comment indexes the code.
//
// Design rules:
//   - A process has one Telemetry, built by NewProcessLogger and installed by
//     InstallDefaultLogger; library code reaches it through CurrentTelemetry.
//   - Every event, operation, metric family and span template a process
//     writes is declared in an observability.json table registered with
//     RegisterDeclarations; this package's own table declares the shared
//     boundaries. Writing an undeclared name fails a test binary at once.
//   - Records carry only allowlisted field names (diagnostic_fields.go, which
//     TestDiagnosticFieldVocabularyProjection keeps equal to the TypeScript
//     library's fields.json), and secrets are redacted by key before a record
//     is written.
//   - Only this package imports the OpenTelemetry SDK.
//
// Where each concern lives:
//   - Records: diagnostics.go (ProcessLogger, the JSON handler, limiter,
//     redaction, sink failures), config_error.go (startup failures),
//     http_server_diagnostics.go (net/http server errors).
//   - Declarations: declarations.go and observability.json; inventory_test.go
//     compares every table with deploy/managed's inventories.
//   - Metrics: metric_registry.go (Registry and its families),
//     operation_metrics.go (OperationMetrics: service operations and shutdown
//     phases), workload.go (HealthRouter, HTTPMetrics, GRPCMetrics, process
//     metrics).
//   - Observations and spans: observe.go (Observe, WithFields), tracing.go
//     (sampler, traceparent propagation, the bounded export queue, Span).
//   - Pressure points: pressure.go. Cold starts: cold_start.go.
//   - Process lifetime: process.go (RunProcess, the shutdown budget) and
//     workload.go (Run, Readiness, shutdown phases).
package workload
