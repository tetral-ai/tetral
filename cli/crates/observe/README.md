# observe

How the CLI records belongs here: structured records on stderr and in the log
file, the declared catalogue of events and metric families, the metrics
registry, trace context and stopwatches.

## Design rules

- It depends on no other workspace crate.
- No crate records any other way; `protocol`, `state` and `sdk`, which may not
  depend on it, record nothing, and their callers record for them.
- A record is one JSON line: the fields of `RECORD_FIELDS`, the correlation
  fields of the spans it is recorded in and of the event itself, which are the
  identities and trace ID of the work it was recorded for
  (`CORRELATION_FIELDS`), and the fields its catalogue entry declares. Only events of workspace crates become records
  (`RecordLayer`).
- Text reaches a record only as an identity (`Id`), an enum field's declared
  value, `error.message_safe`, the catalogue's fixed text for the event's
  `error.code`, or `error.message`, the cause of a failure whose error the
  catalogue marks `cause` (`Cause`): no string type is a `RecordValue`. A
  cause's text comes only from the CLI's own code, the operating system, a
  linked library, or a helper process the CLI starts for its own work, up to
  the moment that helper starts a Session's work, as the sandbox launcher does
  before the command it launches runs; of a helper's stderr it is the last
  nonempty line. Text that Session work produced, the output of a command, a
  tool, an MCP server, a model or a remote Git server, is never a cause, and
  neither is what Engine answered: its refusals carry their code alone.
  `Cause::system` keeps of each URL its scheme, host and path, without its
  userinfo and query; keeps the text up to the first Drive mount directory
  (`sessions/<session_id>/drives/<drive_id>`, behind any prefix, each
  identity running to the next `/`) that a `/` follows and replaces the rest
  with `[…]`; then it keeps the first `STRING_LIMIT_BYTES` at a character
  boundary, followed by `[TRUNCATED]` when cut. Any other string value
  longer than `STRING_LIMIT_BYTES` is replaced without being copied. A
  control character in any string value becomes a space. A duration field
  holds only an `Ms`, read by a `Stopwatch` or by `Startup`.
- Every event and metric family is declared in `cli/observability.json`, each
  event with its level and error codes. An event at `ERROR`, or with an
  `error.*` field, declares at least one error, and each error declares
  exactly one of `"cause": true` and `"no_cause": "<source>"`, the source
  being `command_output`, `tool_output`, `mcp_server_response`,
  `remote_git_output` or `engine_response` (`check_event`). Recording an
  undeclared event, metric series or error code, a declared event at another
  level, an `error.message` that is not the `Cause` of an error marked
  `cause`, an event that declares errors without its `error.code`, or an
  error marked `cause` without its `error.message`, fails
  the test that does it, whatever the level set, the sinks' room and the
  subscriber: observe's macros check an event where it is recorded (`check_declared`),
  and the record layer checks every workspace event it gets. The failure
  names the event, the error code when the rule is about one, the rule, and
  the file and line that wrote the record (`BrokenRule::problem`). A release
  build writes `diagnostic.invalid_event` in its place, which keeps its spans'
  correlation fields and, of the rejected event's own fields, only its name,
  in `event.original`, when that is a well-formed event name; a failure that
  only lacks its code or its cause is written as it is, and one that carries
  a cause its code declares it has not is written without that cause, since
  replacing either would lose the failure.
- Recording never makes a task wait (`Sink`), and a record is built only after
  a sink has room for it (`Sinks::write_with`).
- The registry, `Startup` and the sinks are atomics and bounded queues that
  hold no App Server state, so the workspace's state-owner rule does not apply
  to them.
- A pressure point is a place where work waits or is refused; its series carry
  the point's name as their prefix, and the catalogue checks that each has its
  siblings (`check_pressure_point`). Its owner records its rejections by
  episode, one per cause (`pressure::Episodes`).

- Logs use nonblocking, no-follow opens and accept only regular files after
  inspecting the opened descriptor. A FIFO cannot stall initialization.
- Rotation remembers completed steps. After moving history it retries only
  the current-file move, then only exclusive creation of the new current
  file. A failed reopen drops that record and leaves history in place; the
  next record retries. An unexpected current path is not adopted or removed.
- Both sinks share one flush deadline. Flushing completes queued writes or
  their normal failure handling; it adds no per-record fsync. Exit at the
  deadline may leave queued records unwritten and not individually counted.

## Entry points

- `install(level, clock)` makes the record layer the process's subscriber,
  writing to stderr. `Observer::hold_records_for_log_file` keeps every later record, up
  to the sink's queue, for the log file that `Observer::open_log_file` opens
  and writes them to it first; a log file that cannot be opened counts each of
  them, and every later record, as one the file could not take.
  `Observer::flush` waits for the sinks at exit, at most `EXIT_FLUSH_LIMIT`
  and never past the end that `Observer::end_flush_within` set, both on the
  `FlushClock` that `install` was given, which it reads again at least once
  per its recheck interval (`sinks`).
- `info!("event.name", field = value, ...)`, and `debug!`, `warn!` and
  `error!`, record a declared event at its declared level.
- `correlation_span` gives a span whose records carry the given identities and
  the `trace_id` of a `TraceContext`.
- `registry()` hands out the declared series; `render()` writes them in the
  Prometheus text format 0.0.4.
- `Level::from_env` reads `TETRAL_LOG_LEVEL`; `TraceContext::parse` accepts a
  `traceparent` value.
- `Stopwatch` measures a duration, and `STARTUP` the startup phases of a
  server start, on the monotonic clock their caller passes in.

## Where things live

| File | Holds |
|---|---|
| `src/record.rs` | `RecordLayer`, the record and correlation fields, `RecordValue`, `Cause`, `correlation_span` and the sanitising of string values |
| `src/sinks.rs` | `Sink`, `Sinks`, and `LogFile` with its rotation |
| `src/catalogue.rs` | The parsed catalogue and its checks against the record and metric rules, with the two bucket sets |
| `src/metrics.rs` | `Counter`, `Gauge`, `Histogram`, `Registry` and the text format |
| `src/level.rs` | `Level` and `TETRAL_LOG_LEVEL` |
| `src/trace_context.rs` | `TraceContext` |
| `src/pressure.rs` | `Episodes`, the rejection episodes of a pressure point, and `QUIET_INTERVAL` |
| `src/stopwatch.rs` | `Stopwatch`, `Ms` and `Startup`, which records the `cold_start` events |
| `tests/string_fields.rs` | The compile-fail cases of `tests/ui/`: a `String` or a `&str` field does not compile |
| `tests/level_variable.rs` | `Level::from_env` reading `TETRAL_LOG_LEVEL` |
