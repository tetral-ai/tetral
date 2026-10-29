# Workload process boundaries

This package owns common Go listener lifecycle, readiness, metrics, safe startup
failure records, process diagnostics and the recording library every Engine
service writes its records, metrics and spans through. Service configuration remains in the
owning service. `ResourceConfigFromEnv` reads deployment environment/version with
Go defaults `local`/`unknown`; `ResourceConfigFromEnvWithTrimPolicy` explicitly preserves Git Proxy's
existing trimming of all metadata values; ordinary callers preserve nonblank
spaces and default only exact empty values.

## Listener and request lifetime

`Run` closes request admission when shutdown begins and marks readiness as
shutting down. A late startup callback cannot restore readiness. Already
admitted HTTP handlers retain their request contexts during the configured
shutdown grace period; cancellation of the process context alone does not
abort them. At the deadline the owner cancels those contexts, closes remaining
connections and joins all admitted handlers and the serving loop before it
returns. Command-owned database, object-store and transport clients can then
close without racing an active handler. Hijacked handlers are included in the
join even though `http.Server.Shutdown` does not wait for them.

The internal gRPC owner uses the same order: withdraw health/admission, attempt
graceful completion, force cancellation at its deadline, then join handlers
and the serving loop. `internalgrpc.RunGRPCWorkload` joins every started gRPC
server, including cancellation before the serving callback. Its outer HTTP
orchestration does not impose a second timeout that could return while gRPC
still uses the command's dependencies. A configured cancellation/join budget
reports an overrun immediately, retains handler ownership until the join, and
then returns an error. A zero budget uses the shared five-second default.
Service-specific consumers separately
own their acquisition, heartbeat, settlement and drain policies.

Executable entrypoints explicitly call `RunProcess`. Their validated service
configuration supplies one absolute application shutdown allocation (drain plus
cancellation/join). The first signal, listener failure or dependency cleanup
starts that deadline; other listeners and later cleanup cannot reset it. If a
producer refuses cancellation, the executable exits with status 1 when that
allocation expires. It does not close dependencies beneath live producers or
claim a completed handoff. A final diagnostic is best effort through the bounded
process logger; silent, blocked and panicking sinks cannot delay exit. Reusable
runners retain join-before-close ownership and cannot exit their caller. Normal
completion disarms the executable guard only after command cleanup returns.

## Diagnostics

The process reads diagnostic controls once at startup. Empty values keep the
following defaults; malformed, noncanonical integers and values outside the
listed ranges fail startup without echoing their supplied value.

| Environment | Default | Accepted values |
| --- | --- | --- |
| `TETRAL_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `TETRAL_LOG_MAX_RECORD_BYTES` | `16384` | `1024`–`65536` bytes, including newline |
| `TETRAL_LOG_SUMMARY_INTERVAL_MS` | `30000` | `100`–`3600000` milliseconds |
| `TETRAL_LOG_BURST` | `1` | `1`–`1000` initial records per repeated event/reason window |
| `TETRAL_TRACE_SAMPLE_RATIO` | `1.0` | a decimal from `0` to `1`: the share of root traces sampled |
| `TETRAL_DB_SLOW_QUERY_MS` | `250` | `1`–`60000` milliseconds; a longer statement writes `db.query.slow` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | empty | an OTLP/gRPC collector URL; empty exports no span |

Severity describes operational significance; a semantic failure retains
`error.class`, `error.code`, and `error.message_safe` independently of severity.
Healthy high-frequency polling and authenticated gRPC completions use Debug.
First degradation and final failures remain visible at their owning severity.
Repeated warnings, errors, and marked failures aggregate by stable event/reason,
never by workspace, Session, Thread, request or operation identity;
`pressure.rejecting_started` and `pressure.rejecting_ended` aggregate by
pressure point and cause. Kubernetes
watch diagnostics additionally distinguish the two fixed resource classes
`pods` and `endpointslices`; a recovery for one resource clears only its windows. At most 256
windows retain one bounded correlation sample each. Summaries contain suppressed
count and first/last timestamps, the complete safe failure tuple when present,
and approved distinguishing phase/component context. Partial failure tuples are
completed per missing member without replacing supplied safe fields or treating
an ordinary Info/Warn record as a failure. Severity escalation emits a new first record;
a recovery marked with `recovery.event` flushes that event's summary before the
recovery record and resets its window.

The shared boundary normalizes event names and protects resource metadata. It
admits the scalar field vocabulary in
[`fields.json`](../ts-observability/src/fields.json), bounds strings to 1024 UTF-8 bytes
and inspects at most 64 caller fields per record, removes controls, redacts credentials,
content fields and full URLs, and drops records exceeding the byte ceiling.
Resource values are sanitized once and bounded to 253 UTF-8 bytes for diagnostics;
this does not change their owning configuration validation. Existing safe builders
remain responsible for turning arbitrary errors and content into fixed public
messages or approved bounded classifications. Adding a field requires reviewing
its producer and updating the checked Go vocabulary alongside this file.

JSON records carry `time` (UTC, six fractional digits), `level` (`DEBUG`,
`INFO`, `WARN` or `ERROR`), `event`, `msg` (equal to `event`), `log.type`,
`service.name`, `deployment.environment`, `service.version`,
`service.instance.id`, and `process.pid`. Existing health/metrics servers expose,
through `DiagnosticMetrics`, fixed `tetral_diagnostic_*` counters for emissions,
filtering, suppression, dropped records and sink failures, plus limiter-entry and
queued-record gauges. The queued-record gauge reports the process logger's queue
and is zero for synchronous writers. The series carry no request or tenant
labels, and never report diagnostic loss through the log sink.
Diagnostics are best effort: observer, encoding and sink faults cannot become
business operation failures. Durable receipts and external effects remain owned
by their business boundary.

`NewProcessLogger` owns one worker and a nonblocking queue of 64 bounded records.
Production commands install that logger as the process default for library
records and close it after business listeners and resources. `Close(ctx)` flushes
bounded suppression summaries and waits only within the caller's budget;
`CloseWithBudget` supplies a one-second diagnostic budget. Excess records drop
under backpressure. A generic injected `io.Writer` cannot be canceled: if its
write never returns, the single worker may remain blocked after Close returns.
There are no per-record goroutines or additional unbounded buffers. Tests that
inject a blocked writer must release it during cleanup. Production stderr has
the same explicit write/backpressure boundary.

`Stats` counts successfully completed writes as emitted and rejected, discarded,
partial or failed writes as loss. `NewLogger` is a synchronous adapter at the
default Info level for writers known to return promptly, such as test buffers;
production commands use the process owner. Embedded components without an
installed process logger remain quiet until the caller injects one. The shared
runners `workload.Run`, `internalgrpc.Run` and `internalgrpc.RunGRPCWorkload`
follow the same rule: without an injected `Logger` they use the installed
process logger and are otherwise quiet. `InstallDefaultLogger` returns a
restoration function so construction and tests do not leak global ownership.

The package tests check literal controls, bounded retained samples, field
projection, severity/recovery transitions and an actual paused OS pipe. Selected
service tests cover real Queue poll/heartbeat and authenticated gRPC caller noise.

## Recording rules

Every Engine service records through this library in Go and
[`@tetral/ts-observability`](../ts-observability/README.md) in TypeScript, with
the same names and semantics; `integration/static` fails any other `slog`
package-level call or TypeScript `console` call in service code.

- **Records.** One JSON object per line on stderr with the fields above. A
  failure adds `error.class`, `error.code` and `error.message_safe`, the fixed
  text its event declares for that code, never an error's own text. A duration
  is an integer `_ms` field from a monotonic clock. A string over 1,024 bytes
  becomes `[TRUNCATED]`; control characters become spaces.
- **Correlation.** `WithFields(ctx, ...)` adds correlation fields (the
  workspace, Session, thread, request, tool call, task, message, connection,
  instance, credential, binding, computer and drive identities); every record
  written with that context, and every span and observation started under it,
  carries them, and a record written under a span also carries `trace_id` and
  `span_id`.
- **Levels.** `ERROR` when an operator must act; `WARN` when work goes on
  degraded, a pressure point starts or stops refusing, a peer breaks protocol,
  or a default replaces a value; `INFO` for one summary per external request
  or turn, one record per durable state change, process start and stop;
  `DEBUG` for single steps. The component that handles a failure records it
  once; one that returns it does not. Nothing is recorded per loop iteration
  or per streamed fragment.
- **Never recorded.** No record, span or metric holds a key, token, ticket,
  connection key, password or `Authorization` value; a prompt, model output,
  tool input or output, command line, environment value, file content or drive
  file path; or an email address, name or other personal attribute.

### Declarations

Each component declares its events, operations, metric families and span
templates in `observability.json` at its package root
(`services/<name>/observability.json`,
`services/<name>/packages/<package>/observability.json`). This package's own
[`observability.json`](observability.json) declares what the library and the
boundaries every Go service shares write: process lifecycle, HTTP, internal
gRPC, database statements, transport security, cold starts, pressure points
and diagnostics; a service table never repeats them. A Go service embeds its
table and calls `RegisterDeclarations` from an `init` function. A logger of a
registered service refuses an undeclared event and `Observe` an undeclared
operation: a test binary fails at once, production writes
`diagnostic.invalid_event`. `Registry` creates a metric family only from its
declaration: the type, label names and buckets (`request`, `long` or a list)
come from the table, the code gives the name, help text and label order, and a
sample whose label value is outside a declared values list is dropped. An
undeclared name, or one declared with another type or other labels, fails a
test binary and in production gives a handle that records nothing. Collectors
and metric text a service writes by hand are checked by the inventory test
alone. `deploy/managed/logging-inventory.json`,
`metrics-inventory.json` and `span-inventory.json` catalogue the same
declarations; `TestObservationInventory` fails on any difference.

### Observations

`Observe(ctx, operation)` starts a span named after the operation;
`Observation.End(err)` records the duration in
`tetral_operation_duration_seconds{service,operation,outcome}`, or for an
operation its table marks long in `tetral_long_operation_duration_seconds`,
with outcome `ok`, `rejected` (a `*Rejection` from a pressure point, which no
error rate counts), the error's `ErrorCode()` when the operation declares it,
or `error`. `EndHandled(err)` also writes the declared `<operation>.failed`
record, for the component that handles the failure. `OperationMetrics`, the
per-owner recorder of shutdown phases and service-named operations, records in
the same families under the same rule: `success` is `ok`, and an outcome such as
`timeout` is kept only when the operation declares it as a code.

### Metrics

A name is lowercase with `_` between words; a counter ends in `_total`, a
duration in `_seconds`, a size in `_bytes`. A family the library records for
every service carries `service` as a label; a family one service records alone
carries the service as its name prefix (`computerhub_`). Label values come
from sets fixed in code and never hold an identity. A duration histogram uses
the request buckets `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`
or the long-operation buckets `0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600,
1800` (`RequestDurationBuckets`, `LongOperationDurationBuckets`). `Registry`
holds a service's own families; the installed `Telemetry`'s registry renders
on every `/metrics`.

Boundaries record without per-call code: the public HTTP middleware observes
`http_server_request_duration_seconds{service,route,method,status_class}`, or
`http_server_stream_duration_seconds` for a long route; internal gRPC
`grpc_server_request_duration_seconds{service,method,code}`; named database
statements and lock acquisitions
`db_client_operation_duration_seconds{service,operation,outcome}`.
For Go queries, the outcome, elapsed time and sampled span finish at result
exhaustion, terminal error or explicit close (at Scan completion for QueryRow).
This includes caller read intervals. The slow-operation warning therefore
describes client database access time, not isolated server execution time.

### Pressure points

A place where work waits or is refused exposes, named after the point,
`<point>_in_progress` and `<point>_limit` where it holds limited work,
`<point>_queued`, `<point>_oldest_queued_seconds` and
`<point>_queue_wait_seconds` where work waits, and
`<point>_rejected_total{cause}` where it refuses work. `NewPressurePoint`
counts refusals and records them by episode: `pressure.rejecting_started` at
the first refusal of a cause after 60 quiet seconds, `pressure.rejecting_ended`
with `pressure.rejected`, the episode's count, at the first admission or
refusal 60 seconds or more after its last refusal.

### Cold starts

A cold start has a kind and phases that follow one another; each phase
records `cold_start.phase_started` and `cold_start.phase_completed` with its
`<phase>_ms`, and the start one `cold_start.completed` with
`cold_start.start_kind`, every phase's `_ms` and `outcome`. A completed start
observes `cold_start_phase_seconds{service,kind,phase}` and
`cold_start_seconds{service,kind,scope}`, whose `engine` scope leaves out the
phases `ColdStart.External` marks. `BeginServiceColdStart` records each
service process's `service_pod` start of kind `new`, from process start
through `config`, `dependencies` and `ready`; the first `Readiness` to pass, or
`MarkProcessReady` for a service with its own readiness, completes it, and
`LogStartupFailure` fails it with the startup error code.

### Traces

Spans and trace IDs always exist; an OTLP/gRPC exporter to
`OTEL_EXPORTER_OTLP_ENDPOINT` exists only when it is set, fed by a 2,048-span
queue that refuses a span when full and counts it in
`tetral_diagnostic_spans_dropped_total`. W3C `traceparent` is the only
propagator; the sampler is parent-based with root ratio
`TETRAL_TRACE_SAMPLE_RATIO`. A developer API request starts a new trace and an
incoming `traceparent` becomes only its link; an internal gRPC call continues
its caller's trace from `traceparent` metadata, which every internal dial
writes; a queued job carries its producer's `traceparent`, and a CLI's Session
instruction its issuing work's, which the hub's first write of it continues.
No call to a model provider, a remote MCP server or GitHub carries trace
context. A span holds identities, sizes, durations, outcomes and its
template's attributes; a failed span `error.class` and `error.code`. Only this
package imports the tracing SDK; others use `Span`, `StartRequestSpan`,
`StartServerSpan`, `StartRemoteChildSpan` and `StartSpan`.

## Shutdown phases

Shutdown samples bracket actual `shutdown_http_drain`, `shutdown_http_join`,
`shutdown_grpc_drain` and `shutdown_grpc_cancel_join` operations, which every
registry recognizes and the library table declares long. Owners register their
own phases, such as Queue's `shutdown_queue_drain` and
`shutdown_queue_cancel_join`. A drain sample measures the actual graceful wait
and records `timeout` when its cutoff expires. A join sample is recorded only
after the actual owner joins, including a join that outlives its budget. HTTP
drain and join share the same absolute `ShutdownTimeout` deadline; a join's
`timeout` can inherit the exhausted drain budget even when the final join is
brief. Instrumentation changes no admission, deadline or dependency closure
order. `ObserveShutdown` also sends the same completed observation to the
process logger as `workload.shutdown.phase_completed`, with closed
`operation`/`outcome`, exact `duration.seconds` and `metric.observation.count`
for that registry's operation/outcome. Count continuity helps detect omitted
records; the last observed count alone cannot prove that no later phase
occurred. Info filtering, backpressure, sink failure or process termination
before a join leave phase evidence unavailable or partial; they never
manufacture zero duration.

Pool waits retain `db_pool_wait_count_total` and
`db_pool_wait_duration_seconds_total`; `db_pool_open_connections`,
`db_pool_in_use_connections` and `db_pool_idle_connections` describe actual Go
SQL pools. They do not measure query execution time.
