# Workload process boundaries

This package owns common Go listener lifecycle, readiness, metrics, safe startup
failure records and process diagnostics. Service configuration remains in the
owning service. `ResourceConfigFromEnv` reads deployment environment/version with
Go defaults `local`/`unknown`; `ResourceConfigFromEnvWithTrimPolicy` explicitly preserves Git Proxy's
existing trimming of all metadata values; ordinary callers preserve nonblank
spaces and default only exact empty values.

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

Severity describes operational significance; a semantic failure retains
`error.class`, `error.code`, and `error.message_safe` independently of severity.
Healthy high-frequency polling and authenticated gRPC completions use Debug.
First degradation and final failures remain visible at their owning severity.
Repeated warnings, errors, and marked failures aggregate by stable event/reason,
never by workspace, Session, Thread, request or operation identity. Kubernetes
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
[`fields.json`](../ts-observability/src/fields.json), bounds strings to 1024 Go bytes or TypeScript UTF-16 code units
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
partial or failed writes as loss. `Metrics` adds the queue gauge. `NewLogger` and
`NewLoggerWithLevel` are synchronous adapters for writers known to return
promptly, such as test buffers; production commands use the process owner.
Embedded components without an installed process logger remain quiet until the
caller injects one. `InstallDefaultLogger` returns a restoration function so
construction and tests do not leak global ownership.

The package tests check literal controls, bounded retained samples, field
projection, severity/recovery transitions and an actual paused OS pipe. Selected
service tests cover real Queue poll/heartbeat and authenticated gRPC caller noise.
