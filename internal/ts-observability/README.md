# Shared TypeScript diagnostics

This package is the TypeScript half of Engine's observability library. It owns
process JSON records, severity filtering, bounded repeated-event state,
diagnostic configuration, observability tables, operation and cold-start
metrics, pressure points, tracing and the production stderr adapter. Provider
Gateway, MCP Connector and Runtime keep their safe event builders, their tables
and their operational meaning in their own packages.

Every TypeScript service records by the Go library's
[recording rules](../workload/README.md#recording-rules), with the same names,
fields and semantics. This file states how the TypeScript side implements them.

Entry points:

| Import | Contents |
| --- | --- |
| `@tetral/ts-observability` | Logger, configuration, tables, telemetry, observations, metrics, pressure points, cold starts, trace propagation |
| `@tetral/ts-observability/effect` | `ObservabilityLive` and `observabilityLayer(telemetry)` for Effect programs |
| `@tetral/ts-observability/test-support/recording-telemetry` | Test-only telemetry that keeps records and ended spans in memory |
| `@tetral/ts-observability/test-support/executable-shutdown` | Test-only executable shutdown driver |
| `@tetral/ts-observability/test-support/handled-failure-record.json` | The field names of one handled failure record, which the Go and TypeScript library tests both compare their records with |

`parseWorkloadResourceConfig` preserves each service's existing required
environment/version validation and maximum length; it reads only its supplied
environment projection.

## Configuration

The process reads diagnostic controls once at startup. Empty values keep the
following defaults; malformed, noncanonical and out-of-range values fail
startup. `parseDiagnosticConfigResult` returns the problem, which names the
variable and its accepted values and never the supplied value; a service's
`startup_failed` record carries it in `startup.config_problem` beside the fixed
`error.message_safe` `configuration invalid`.

| Environment | Default | Accepted values |
| --- | --- | --- |
| `TETRAL_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `TETRAL_LOG_MAX_RECORD_BYTES` | `16384` | `1024`–`65536` bytes, including newline |
| `TETRAL_LOG_SUMMARY_INTERVAL_MS` | `30000` | `100`–`3600000` milliseconds |
| `TETRAL_LOG_BURST` | `1` | `1`–`1000` initial records per repeated event/reason window |
| `TETRAL_TRACE_SAMPLE_RATIO` | `1` | a decimal matching `^(0\|1)(\.[0-9]+)?$` and at most `1`: the share of root traces sampled |
| `TETRAL_DB_SLOW_QUERY_MS` | `250` | `1`–`60000` milliseconds |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | an `http` or `https` URL with a host; empty exports no span |

The sample ratio is honoured exactly: `0` samples no root trace. The
slow-statement threshold is validated so that both languages accept the same
environment.

## Records

A record is one newline-terminated JSON object with `time` (UTC, six fractional digits,
the milliseconds from `Date.now()` and only the digits below them from
`performance.now()`), `level` (`DEBUG`, `INFO`,
`WARN` or `ERROR`), `event`, `msg` (equal to `event`), `operation` (the
caller's, otherwise the event's first dot-separated segment), `log.type` (the
event's declared `log_type`, otherwise `app`), `service.name`,
`deployment.environment`, `service.version`, `service.instance.id` and
`process.pid`. An `ERROR` record, or one carrying any member of the failure
tuple, carries the complete tuple: missing members become `operation_failed`,
`operation_failed` and `operation failed`. A record written inside a span
carries `trace_id` and `span_id`, and one written inside `withFields(fields, fn)`
or an observation carries those correlation fields; both follow the
AsyncLocalStorage context that `createTelemetry` installs.

The field rules are Go's. The boundary admits the scalar vocabulary in
[`fields.json`](src/fields.json), which a Go test keeps equal to Go's list; inspects at
most 64 caller fields with keys of at most 128 characters; bounds every string
to 1,024 UTF-8 bytes, replacing a longer one with `[TRUNCATED]`; replaces
control characters with spaces; redacts credential, content and full-URL
values with Go's key and value patterns; and drops records exceeding the byte
ceiling. Resource values are bounded to 253 UTF-8 bytes. Existing safe builders
remain responsible for turning arbitrary errors and content into fixed public
messages or approved bounded classifications. Adding a field requires reviewing
its producer and updating the checked Go vocabulary alongside this file.

Severity describes operational significance; a semantic failure retains
`error.class`, `error.code`, and `error.message_safe` independently of severity.
Healthy high-frequency polling and authenticated gRPC completions use Debug.
First degradation and final failures remain visible at their owning severity.
Repeated warnings, errors, and marked failures aggregate by stable event/reason,
never by workspace, Session, Thread, request or operation identity;
`pressure.rejecting_started` and `pressure.rejecting_ended` aggregate by
pressure point and cause. At most 256 windows retain one bounded correlation
sample each. Summaries contain suppressed count and first/last timestamps, the
complete marked safe failure tuple when present, and approved distinguishing
phase/component context. Samples retain only sanitized bounded scalar values;
severity alone does not mark a failure. Severity escalation emits a new first
record; a recovery marked with `recovery.event` flushes that event's summary
before the recovery record and resets its window.

Info records bypass the limiter unless they mark a semantic failure or opt in
with the control-only boolean `diagnostic.repeat: true`. The marker selects
limiter admission and is never written, because it is outside `fields.json`.
Limiter windows are keyed by event and reason (`error.code`, otherwise `reason`,
otherwise `outcome`), so each distinct outcome emits its first record and later
repeats within the window are summarized. A suppressed Info record still counts
in its suppression summary.

Existing health/metrics servers expose fixed `tetral_diagnostic_*` counters for
emissions, filtering, suppression, dropped records and sink failures, plus a
limiter-entry gauge, and `tetral_diagnostic_spans_dropped_total`. They carry no
request or tenant labels, and never report diagnostic loss through the log sink.
Diagnostics are best effort: observer, encoding and sink faults cannot become
business operation failures. Durable receipts and external effects remain owned
by their business boundary.

## Declarations

Each TypeScript package that records declares its events, operations, metric
families and span templates in `observability.json` at its package root, in the
Go table format. This package's [`observability.json`](observability.json)
declares what the library writes for every TypeScript service: cold starts,
pressure points, diagnostics, command lifecycle and shutdown phases, the shared
metric families, the gRPC call record and histogram, and the `{grpc.method}`
and `{operation}` spans; a service table never
repeats them (`serviceDeclarations` rejects a repeated name). A service's logger
module loads its table and passes it to `createTetralJsonLogger` and
`createTelemetry`. A table may also declare a `summary` family, for the existing
summary families the Go format does not use.

A logger with a table refuses an undeclared event, and `observe` an undeclared
operation. Under `NODE_ENV=test` the call throws `UndeclaredNameError` and the
error is thrown again on a microtask, so a caller that contains logging
failures cannot hide it from the test run; otherwise the record becomes
`diagnostic.invalid_event` with `event.original`, and the operation records as
`unknown_operation`. The `undeclared` option selects either policy explicitly.

`MetricRegistry`, built with a table and a policy, creates a metric family only
from its declaration, as the Go `Registry` does: the type, label names and
buckets come from the table, the code gives the name, help text and label
order, and a sample whose label value is outside a declared values list is
dropped. An undeclared name, or one declared with another type or other
labels, throws under `throw` and under `record` gives a handle that records
nothing.

## Telemetry

`createTelemetry` builds one process's recording owner: its logger, table,
tracer and the library's metric families. Each command creates it once its
configuration is loaded, installs it with `installTelemetry` and, after its
business resources close, calls `shutdown`, which exports the queued spans
within a one-second budget. `currentTelemetry` returns the installed owner or a
quiet one. `operationMetricsText(operations)` renders a service's operation
histograms with the installed owner's families, one header per family.

### Observations

`observe(operation, fields)` starts a span named after the operation;
`Observation.run(fn)` runs `fn` inside it. `end(error)` records the duration in
`tetral_operation_duration_seconds{service,operation,outcome}`, or for an
operation its table marks long in `tetral_long_operation_duration_seconds`,
with outcome `ok`, `rejected` (a `Rejection` from a pressure point), the
error's `errorCode` when the operation declares it, or `error`.
`endHandled(error)` also writes the declared `<operation>.failed` record with
the tuple its event fixes for that code and `duration_ms`; a rejection writes
none. A failed span carries `error.class` and `error.code` only.

A duration histogram uses the request buckets `0.005, 0.01, 0.025, 0.05, 0.1,
0.25, 0.5, 1, 2.5, 5, 10` or the long-operation buckets `0.1, 0.5, 1, 2.5, 5,
10, 30, 60, 120, 300, 600, 1800` (`requestDurationBuckets`,
`longOperationDurationBuckets`), which a table names `request` and `long`. A
`MetricRegistry` family has one label order per process; registering its name
with another order throws.

### Pressure points and cold starts

`PressurePoint` and the cold-start functions follow the Go
[pressure point](../workload/README.md#pressure-points) and
[cold start](../workload/README.md#cold-starts) rules. A command calls
`beginServiceColdStart` once its configuration is loaded, so the `config` phase
runs from process start to that point; `endServiceColdStartPhase("config")`
follows at once and `endServiceColdStartPhase("dependencies")` when every
dependency it opens at start is connected. Readiness first passing calls
`completeServiceColdStart`; a startup failure calls
`failServiceColdStart("startup_error")`, which observes no histogram. A
configuration failure calls `failServiceColdStartAtConfig`, which records the
`config` phase ended by `config_error` through the command's first logger,
since the process's telemetry needs that configuration. Each service README
names its dependency and readiness points.

## Traces

Tracing uses the OpenTelemetry JS SDK through a provider this package keeps
private: nothing registers a global tracer provider or propagator, so no
library can pick up the tracer or write trace headers on its own. Spans and
trace IDs always exist, an unsampled root included. The sampler is
parent-based with root ratio `TETRAL_TRACE_SAMPLE_RATIO`. An OTLP/gRPC exporter
to `OTEL_EXPORTER_OTLP_ENDPOINT` exists only when it is set; it connects
lazily and is fed by a 2,048-span queue that refuses a span when full and
counts it in `tetral_diagnostic_spans_dropped_total`, exporting batches of 256
every second, each within ten seconds. The resource carries `service.name`,
`service.version`, `service.instance.id` and `deployment.environment`.
An empty timer interval leaves the exporter ready for later spans. A flush
joins an active export even when that export has already emptied the queue.

W3C `traceparent` is the only propagated header. Every gRPC client a service
opens to an Engine service carries `traceparentClientInterceptor`, which writes
the caller's span into the call metadata. Every gRPC server registers its
implementation through `continueGrpcTraces(definition, implementation)`, which
records each call as Go's server interceptors do: it runs in a server span named
by the full method path, a child of the caller's `traceparent` or a new trace
without one, with the request's `workspaceId`, `sessionId` and `threadId` as the
correlation fields `workspace.id`, `session.id` and `thread.id`; it ends with
the status in `rpc.grpc.status_name` spelled as Go's `codes.Code` (a handler
error without a numeric code is `Unknown`), one
`grpc_server_request_duration_seconds{service,method,code}` sample and one
`DEBUG` `internal.grpc.request` record, a failed call included. A failed call
adds `error.class` `grpc_error` and the lowercase status name as `error.code`;
the component that handles the failure records it.
HTTP clients get no instrumentation, so no call to a model provider, a remote
MCP server or GitHub carries trace context.

## Effect programs

`ObservabilityLive` is a layer over the installed telemetry that replaces
Effect's loggers, its tracer logger included, with one logger writing this
package's records, and installs a tracer backed by `@effect/opentelemetry` on
the telemetry's tracer. A log message's first part is the event name, checked
against the table; log annotations are the record's fields; a failure cause
adds the tuple the event declares for the failure's `errorCode`, otherwise the
generic tuple, never the failure's own text. `Fatal` records as `ERROR` and
`Trace` as `DEBUG`. Effect spans keep only attributes the field rules admit and
drop every event, recorded exception and status message; a failed span carries
`error.class` and `error.code`. The program's workspace provides `effect` and
`@effect/opentelemetry` 4.0.0-beta.66.

## Stream ownership

`createTetralJsonLogger` accepts a prompt `write` adapter. False rejects a record;
exceptions are contained. Commands use `createDiagnosticStreamSink(process.stderr)`
and pass its nonrecursive failure counter to the logger. The adapter adds no
private queue: it accepts only within a 65536-byte stream-buffer ceiling, stops
writing after backpressure until drain, and rejects after stream failure. One
error listener is retained per stream so accepted asynchronous writes remain safe
after an owner closes; each owner's drain listener is removed on close. Repeated
command construction therefore does not accumulate listeners. Close neither
waits for stream flush nor destroys process-owned stderr.

An emitted counter means the stream accepted the record, not that an external
collector persisted it. Asynchronous stream errors increment sink-failure
counters independently; the stream cannot establish which accepted records were
persisted. Logger `flush` emits bounded suppression summaries and clears its
timer/state; command finally blocks invoke it even on startup failure before
closing the adapter. Business shutdown and diagnostic cleanup retain separate
ownership.

`processFailureLogRecord` supplies fixed command/cleanup phases and a safe
failure tuple, without accepting exception text or stacks. `runProcessEntry`
and `registerProcessSignalHandlers` contain executable and signal rejections
and choose exit status. `runProcessEntry` supplies an explicit executable-only
boundary: each command arms one absolute application shutdown deadline from its
resolved typed policy, for signal and command-finally cleanup. Joined cleanup
disarms it. Expiry reports a fixed incomplete-shutdown diagnostic and exits nonzero
even when the diagnostic sink is silent or throws. The helper never closes business
resources or invents custody transfer; reusable commands receive no exit policy
unless invoked through the executable boundary.
Each command attempts its acquired resources once in its existing shutdown
order, continues after a rejected close, and releases diagnostics afterward.
Programmatic command callers still receive the original run failure, or the
first cleanup failure when the run succeeded. Diagnostic faults add no wait
for stderr. Reusable business owners retain their existing drain and join
behavior; the executable failure fuse does not publish an operation outcome or
claim that an unjoined business operation completed.

## Operation histograms

`OperationMetricsRegistry` is the process-local histogram utility used by the
owning Gateway/MCP/Runtime registries. It opens no listener. Each owner supplies
its static operation domain and its table; other operation input shares
`unknown_method`, and an operation the table marks long goes to
`tetral_long_operation_duration_seconds`. The service is a fixed workload name
and outcomes follow `observe`: `success` from existing callers records as `ok`,
and an outcome other than `ok`, `rejected` or `error` is kept only when the
operation's table declares it as a code, otherwise it records as `error`. IDs, configured MCP tool names and error messages are never labels.
Nonfinite or negative duration observations are excluded rather than presented
as zero. Milliseconds at an owning hook are converted once at ingestion.
Existing service-specific names and units are retained. `_count` describes
completed local observations, with success and non-success populations
separate; it does not stand in for admission or valid Agent-completion
counters. Histogram rendering is read-only and observer failures remain
business-independent.

`observeShutdown` records a completed owning phase in that same registry and
emits `workload.shutdown.phase_completed` through the existing logger. Its
closed `operation`/`outcome`, exact `duration.seconds` and per-operation/outcome
`metric.observation.count` survive field filtering. Resource fields remain
logger-owned, including `service.instance.id` and `process.pid`. The record is
emitted at phase completion before command diagnostic cleanup; a drain cutoff
and the later actual join remain separate observations.

This provides a retrieval path after the metrics listener closes, with no new
listener, network dependency or exit budget. Central log capture must bind the
process and container incarnation and corroborate source phase/lifetime
coverage. Info filtering, diagnostic backpressure/failure, missing records or a
hard exit make evidence unavailable or partial. Count continuity can detect
interior omissions; it does not prove that the last record was final. Stream
sink `close()` does not wait for accepted asynchronous stderr delivery, and a
phase record never proves that all process cleanup completed.

## Tests

Shared contract tests run through Provider Gateway's selected unit-test entry:
`shared-observability` covers the logger and configuration, `shared-telemetry`
the record shape against Go's, the UTF-8 bound, observation outcomes, gRPC
server spans, the span queue, cold starts and pressure points. Runtime's
tests cover the Effect layer and trace continuity from its Gateway client into
a Gateway server span; its real HTTP metrics test also exercises asynchronous
stream failure. The owning service packages typecheck and bundle these file
dependencies. Their selected command-failure tests exercise real Bun executable
and signal exits, acquired-resource cleanup, and diagnostic backpressure.
Their `shutdown-process` tests run each service's own command fixture through
the `executable-shutdown` driver. Held work under a normal, silent or throwing
diagnostic sink, triggered respectively by SIGTERM, SIGINT and command finally,
must exit nonzero between 50 ms before and two seconds after the fixture's
application budget elapses, without a join, dependency close or handoff; only
the normal sink reports the deadline diagnostic. A cooperative run must exit
zero after the service's expected joins and before dependency close. The driver
module documents the fixture arguments and event lines.
