# mcp

The **MCP manager** belongs here, one task per App Server, which owns the local
MCP servers this machine runs: their processes and MCP connections, their
listings, each server's latest state and its tool-list reports, and every
local MCP call in flight. The only local server is Computer Use, `cua`, which
runs as `<driver> mcp`.

## Design rules

- The manager follows the owner rules of `cli/README.md`: the App Server, the
  local API and the Connection owner reach it only through `McpManager`
  messages and `protocol::ports::McpPort`, and it handles them in arrival
  order. Each MCP connection runs in its own task, which tells the manager
  when it opened and, exactly once, how it ended; a request to the server is
  sent from a task of its own, so no handler waits for a server.
- The MCP client is rmcp `=3.5.1` over the transport of `transport.rs`, which
  writes every request without `params._meta` and hands the manager each
  tagged request's answer as its raw JSON, so a listing keeps every value as
  the server sent it.
- A server starts when the App Server starts with Computer Use recorded
  enabled and at each enable; it never restarts after a failed start or an
  exit. Its first process is its server connection, in `/`; the
  `initialize` result's `protocolVersion` is not checked.
- Before that process, the server connection's task runs
  `<driver> --version` (`driver_version.rs`) and hands the manager the
  version it reported, or none, and the start goes on either way. The
  manager publishes `cua_driver` of `GET /v1/status`
  (`McpManager::cua_driver`): none unless Computer Use is recorded enabled,
  its version none until that read; a version read as other than
  `CUA_DRIVER_TESTED_VERSION`, none included, writes one
  `mcp.driver_version_untested` warning, naming the version only when there
  is one, and changes nothing else.
- Per server, at most one tool-list report is in flight on the online
  connection and only the newest unsent one waits behind it
  (`tool_lists.rs`); a report the outbox refuses as too large becomes the
  failure `mcp_list_failed` with the same `seq`.
- Each binding calls through its own MCP connection, a `<driver> mcp`
  process spawned in the binding's directory at its first call and used by
  no other binding (`binding.rs`). The manager writes `tools/call` and
  `notifications/cancelled` itself under string IDs `tetral-<n>`, which
  never meet rmcp's numeric ones, and the answers to them reach only the
  manager. An opened binding connection whose process ends unstopped is the
  server's exit; one the manager closes for a stdout line over its bound
  ends only its own calls.
- Every call that a binding owner hands over ends in exactly one final
  report, submitted to its Session's Report outbox, or in none when its
  Engine connection ends (`call.rs`). A terminal report takes only the
  outbox's ordinary capacity: one that does not fit becomes the small error
  `resource_exhausted`, which the reserve admits, so a full report budget
  ends that call and never the connection. From its handover to that end it
  feeds the App Server's active-task gauge and idle-exit activity
  (`ManagerConfig::activity`). A binding's release, at its close or
  its connection's end, is answered once its connections' processes have
  exited, and a close reports `closed` only after that answer.
- The driver the manager last recorded is published to the binding owners
  (`McpManager::recorded_driver`), so a task admitted after an enable keeps
  that driver read-only.
- A process is stopped with SIGTERM to its process group and, when any
  member that has not exited remains after the stop grace, SIGKILL to the
  group. Every time is on the seam clock; each reading of it for the
  version read's bound is the test seam counter `mcp.version_checked`, each
  for the start bound `mcp.start_checked`, each for a listing's deadline
  `mcp.listing_checked`, each for a call's bound `mcp.call_checked`, each
  for a stop's grace `mcp.stop_checked`, and the end of each listing is
  `mcp.listed`. A setting admitted behind an active setting reports
  `mcp.setting_queued`.
- The log gets a server's stderr and its `notifications/message` only as
  their sizes and level, never their content, as `observe` requires.
- `POST /v1/computer-use` requests are handled one at a time: the manager
  writes `[computer_use]` of `config.toml`, stops the running server and, for
  an enable, starts the driver and answers once its `initialize` succeeded.
  A `config.toml` that is missing or does not parse, or an operating-system
  error on it, changes nothing and answers `SetError::InvalidConfiguration`
  or `SetError::Io`. Once the shutdown starts, every request not yet
  answered gets `SetError::ShuttingDown`, which the local API maps to an explicit
  `500 io_failed` shutdown/retry response; an active request may already
  have written configuration; a queued request has not written it. Requests
  received after shutdown begins get the same error.

For a local MCP call, `queued_ms` includes waiting for the binding's MCP
connection to open and ends after the replay-ledger check, immediately before
the call is handed to the connection writer. `run_ms` and the 300-second call
deadline begin when `tools/call` is written. A call cancelled before that
handoff has no run phase and counts its wait in `queued_ms`.

## Where things are

- `lib.rs`: `spawn`, `McpManager`, the manager task, its messages and the
  record of `[computer_use]`.
- `limits.rs`: the named bounds and `Limits`.
- `driver_version.rs`: `CUA_DRIVER_TESTED_VERSION` and the `--version` read.
- `process.rs`: spawning and stopping `<driver> mcp`, and its stderr.
- `transport.rs`: the stdio transport, its raw answers and its message bound.
- `server.rs`: one MCP connection's task: start, the server's messages and
  how it ends.
- `listing.rs`: paging, the `ListToolsResult` checks, the limits and member
  filtering.
- `tool_lists.rs`: the latest state and the per-connection report queue.
- `call.rs`: `Calls` and its messages, check 3 (`check`), checks 4 and 5,
  `tools/call`, the call bound, cancellation, the result checks, the
  `mcp_error` bounds and the reports with their messages.
- `binding.rs`: binding connections: their opening, the calls waiting for
  it, their end and a binding's release.
- `media.rs`: the base64 check of media strings, the replacement of refused
  images and the measured sizes.
