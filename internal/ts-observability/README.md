# Shared TypeScript diagnostics

This package owns process JSON records, severity filtering, bounded repeated-event
state, diagnostic configuration and the production stderr adapter. Provider
Gateway, MCP Connector and Runtime keep their safe event builders and operational
meaning in their own packages. `parseWorkloadResourceConfig` preserves each
service's existing required environment/version validation and maximum length;
it reads only its supplied environment projection.

## Record contract

The process reads diagnostic controls once at startup. Empty values keep the
following defaults; malformed, noncanonical integers and values outside the
listed ranges fail startup without echoing their supplied value.

| Environment | Default | Accepted values |
| --- | --- | --- |
| `TETRAL_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `TETRAL_LOG_MAX_RECORD_BYTES` | `16384` | `1024`–`65536` bytes, including newline |
| `TETRAL_LOG_SUMMARY_INTERVAL_MS` | `30000` | `100`–`3600000` milliseconds |
| `TETRAL_LOG_BURST` | `1` | `1`–`1000` initial records per repeated event/reason window |

Severity describes operational significance; a semantic failure retains
`error.class`, `error.code`, and `error.message_safe` independently of severity.
Healthy high-frequency polling and authenticated gRPC completions use Debug.
First degradation and final failures remain visible at their owning severity.
Repeated warnings, errors, and marked failures aggregate by stable event/reason,
never by workspace, Session, Thread, request or operation identity. At most 256
windows retain one bounded correlation sample each. Summaries contain suppressed
count and first/last timestamps, the complete marked safe failure tuple when
present, and approved distinguishing phase/component context. Samples retain
only sanitized bounded scalar values; severity alone does not mark a failure. Severity escalation emits a new first record;
a recovery marked with `recovery.event` flushes that event's summary before the
recovery record and resets its window.

Info records bypass the limiter unless they mark a semantic failure or opt in
with the control-only boolean `diagnostic.repeat: true`. The marker selects
limiter admission and is never written, because it is outside `fields.json`.
Limiter windows are keyed by event and reason (`error.code`, otherwise `reason`,
otherwise `outcome`), so each distinct outcome emits its first record and later
repeats within the window are summarized. A suppressed Info record still counts
in its suppression summary.

The shared boundary normalizes event names and protects resource metadata. It
admits the scalar field vocabulary in
[`fields.json`](src/fields.json), bounds strings to 1024 Go bytes or TypeScript UTF-16 code units
and inspects at most 64 caller fields per record, removes controls, redacts credentials,
content fields and full URLs, and drops records exceeding the byte ceiling.
Resource values are sanitized once and bounded to 253 Go bytes or TypeScript UTF-16 code units for diagnostics;
this does not change their owning configuration validation. Existing safe builders
remain responsible for turning arbitrary errors and content into fixed public
messages or approved bounded classifications. Adding a field requires reviewing
its producer and updating the checked Go vocabulary alongside this file.

JSON records carry `service.name`, `deployment.environment`, `service.version`,
`service.instance.id`, and `process.pid`. Existing health/metrics servers expose
fixed `tetral_diagnostic_*` counters for emissions, filtering, suppression,
dropped records and sink failures, plus a limiter-entry gauge. They carry no
request or tenant labels, and never report diagnostic loss through the log sink.
Diagnostics are best effort: observer, encoding and sink faults cannot become
business operation failures. Durable receipts and external effects remain owned
by their business boundary.

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

Shared contract tests run through Provider Gateway's selected unit-test entry;
Runtime's real HTTP metrics test also exercises asynchronous stream failure.
The owning service packages typecheck and bundle these file dependencies.
Their selected command-failure tests exercise real Bun executable and signal
exits, acquired-resource cleanup, and diagnostic backpressure.

## Operation histograms

`OperationMetricsRegistry` is the process-local histogram utility used by the
owning Gateway/MCP/Runtime registries. It opens no listener. Each owner supplies
its static operation domain; other operation input shares `unknown_method`.
The service is a fixed workload name and outcomes are a closed union. IDs,
configured MCP tool names and error messages are never labels. Nonfinite or
negative duration observations are excluded rather than presented as zero.

The public `tetral_operation_duration_seconds` family uses seconds and the
[shared fixed bucket contract](../workload/README.md#operation-durations).
Milliseconds at an owning hook are converted once at ingestion. Existing
service-specific names and units are retained. `_count` describes completed
local observations, with success and non-success populations separate; it does
not stand in for admission or valid Agent-completion counters. Histogram
rendering is read-only and observer failures remain business-independent.

`observeShutdown` records a completed owning phase in that same public registry
and emits `workload.shutdown.phase_completed` through the existing logger. Its
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
