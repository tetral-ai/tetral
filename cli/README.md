# Tetral CLI

`tetral` is one program for Linux and macOS that runs on users' own machines
and on cloud machines alike. It connects out to Engine and executes, on the
machine, the work that Engine sends for the Sessions that use it. This
directory is its Cargo workspace, with its own `Cargo.lock` and its own
toolchain in `rust-toolchain.toml`; it is built, tested and released apart from
the repository's Go module, and depends on no Engine code.

## Terms

- **App Server**: the long-running `tetral` process on a machine. It holds the
  connection to Engine and every piece of the CLI's state on that machine;
  other processes reach it only through its local API, HTTP and JSON over a
  Unix socket.
- **State root**: the CLI's private directory, one per app account, which
  holds its configuration, credentials, log file and the data it keeps for
  Sessions and drives. The local API's socket and the projects' working
  directories lie outside it. One App Server at a time owns it.
- **Session record**: what the CLI keeps on this machine about one Engine
  Session that uses the machine as one of its computers.
- **Directory probe**: the short-lived child process that checks a Session's
  working directory before its preparation is reported: it resolves symbolic
  links, checks that the target is a directory outside the state root that
  the App Server's user can enter, and enters it.
- **Binding**: one preparation of this machine for one Session on one
  connection to Engine, issued by Engine. A resumed event stream keeps its
  bindings; a new connection needs new ones.
- **Instruction** and **report**: an instruction is a message that Engine sends
  on the event stream, such as a Session's preparation, its close or a tool
  call; a report is the CLI's answer to an instruction, or a later message
  about it, sent as an HTTP request. A report is **frozen** once its body is
  final and written to disk; every send of it, retries included, carries
  those bytes.
- **Ready**: the message the CLI sends on each new connection to declare its
  versions and capabilities, before Engine sends it any Session instruction.
- **Task**: one execution of one tool call on this machine, with its process
  tree.
- **Drive**: a cloud folder of the workspace that Sessions bind and machines
  mount. **Catch-up** applies to a mounted drive the changes committed
  elsewhere since this machine last saw it.

## Crates

A crate depends only on crates of lower layers, and crates of one layer never
depend on each other. The test package `tetral-tests` (`tests/`) may depend on
every crate, and no crate depends on it. No package may come from a path
outside `cli/`, as a path dependency or through a `[patch]`, and no package of
the resolved graph, development and build dependencies included, may be
licensed under the AGPL. The normal dependencies of `tetral`, followed to the
end, must hold neither `tetral-tests` nor the test-only `jsonschema`. Test
seams must sit behind a crate feature named `test-seams`, which no crate may
enable by default and no normal dependency may enable, directly or through
other features, so that the released binary holds none.
`tests/tests/crate_graph.rs` checks these rules over `cargo metadata`.

| Layer | Crate | Responsible for |
|---|---|---|
| 1 | `tetral` | The binary: its commands, the App Server assembly, and the internal entry points the App Server runs as child processes |
| 2 | `appserver` | The state-root lock, the local API server, startup, shutdown and the lifecycle task |
| 2 | `sdk` | The typed client of the local API; it depends on `protocol` and `state` only |
| 3 | `connection` | The Connection owner, the Report outbox, the HTTP and SSE transport to Engine and the drive transfer client |
| 3 | `session` | The Session binding owners, the directory probe and repository clones |
| 4 | `execution` | The Task supervisors, the translation of `sandbox_policy`, each call's permissions, into process isolation, and the tools |
| 4 | `drive` | The Drive sync owners, the Content cache, the drive mounts and the `DriveTransfer` interface |
| 4 | `mcp` | The MCP manager |
| 5 | `protocol` | The wire types, the local API types and the interfaces of `protocol::ports` |
| 5 | `state` | Paths, private-file checks, the configuration and credential files, atomic writes, the clock and the test seams |
| 5 | `observe` | Records, the log file, the catalogue of events and metrics, the metrics registry, trace context, stopwatches and the startup phases |

Each crate's `README.md` is its orientation and its crate documentation.
Library crates return typed errors made with `thiserror`; `tetral` turns each
into the error code of its command's result (`output::CommandError`).

## State owners

Every piece of App Server state has exactly one owner: one of the owners in the
table below, or the lifecycle task of `appserver`, which holds whether the App
Server is running or stopping, the shutdown sequence and the idle timer. An
owner is one tokio task with one unbounded mailbox; it handles its messages one
at a time, in arrival order, and no state is shared behind a lock. A handler
never awaits another owner's reply: it sends its request with a reply channel,
and a spawned task awaits the reply and posts it to the owner's mailbox as a
new message. Stream readers, request tasks and local API handlers are not
owners; they send messages and await replies.

| Owner | How many | Crate | Owns |
|---|---|---|---|
| Connection | One per App Server | `connection` | The connection to Engine: authentication, the event stream and its resume, ready, heartbeats, credential renewal, and the routing of Session instructions |
| Session binding | One per Session on this machine | `session` | The Session record, its bindings and their states, and the admission of new work |
| Task supervisor | One per task | `execution` | One task's process tree |
| Report outbox | One per App Server | `connection` | The frozen reports, their sending and their retries |
| Drive sync | One per Session on this machine | `drive` | The Session's drive sync state, the checks before a command, uploads and catch-up |
| Content cache | One per machine | `drive` | The cached file contents and the slots that bound concurrent uploads and downloads |
| MCP manager | One per App Server | `mcp` | The local MCP servers and each binding's MCP connections |

Peer crates reach each other only through interfaces. `session`,
`execution`, `drive` and `mcp` may send no HTTP request: their reports go to
`protocol::ports::ReportSink` and their drive transfer requests to
`drive::DriveTransfer`. The Connection owner reaches Session binding owners
through `protocol::ports::SessionFactory` and `SessionHandle`, and Session
binding and Drive sync owners reach the Connection owner through
`protocol::ports::ConnectionPort`. Each interface's documentation names the
crate its implementation belongs to; joining the implementations to the owners
that use them belongs to `appserver`.

## Records and metrics

`observe` writes every record and holds every metric; its README states the
record rules. A record is one JSON line, written by two sinks: stderr, and the
log file once it is opened. `TETRAL_LOG_LEVEL` sets the lowest level for both.
`observability.json` is the catalogue of every event and metric family the CLI
records, which `observe::catalogue` embeds and checks; a change that records a
new one adds it there in the same change. No library crate prints, and only
`tetral` may write to stdout, where a command's result object goes: library
crates deny the print lints, and `tests/tests/crate_graph.rs` fails on a print
or `dbg!` call in their sources.

## Reference control server and cross-process tests

`reference/` is the reference control server, Engine's side of the client
connection for the CLI's tests, with fault-injection controls on a second
listener; its README states its rules and controls. It is Go in the
repository's root Go module, not part of this workspace, and
`reference/cmd/tetral-reference` runs it. `reference/wire/` holds its own
types for the protocol of `protocol/` at the repository root, written apart
from the Rust types of `protocol` and tested against the same fixtures;
neither set imports the other or any Engine package.

The cross-process tests of `tests/tests/` run `tetral`, built with the feature
`test-seams`, and the reference control server as separate processes; the
harness in `tests/src/` builds both, drives the clock and barriers of
`state::seams`, and writes each test file's acceptance record. A release
build enables no test seam, and `tests/tests/release_seams.rs` checks that
its binary holds none.

## Dependencies

The CLI depends on no Engine service, database, queue or internal package, on
no provider SDK and on no provider credential. `UPSTREAM.md` lists every
outside source with its pinned version and licence.

## Build and test

Every `cargo` command runs with `cli/` as its working directory, so that
`rust-toolchain.toml` selects the pinned toolchain; none passes
`--manifest-path cli/Cargo.toml` from the repository root.

- `cargo build` and `cargo test` in `cli/` build and test the workspace.
  `cargo test` needs Go, which builds the reference control server and whose
  version each acceptance record names, and `TETRAL_ACCEPTANCE_DIR` naming
  the directory that each test file's acceptance record,
  `<test>_<os>-<arch>_<commit>.md`, is written to; a test fails at once
  without it.
- `go test -race ./cli/reference/...` at the repository root tests the Go
  packages under `reference/`.
- `make test-affected` at the repository root runs the evidence group `cli`
  (the same two commands, with the acceptance records under its output
  directory) for a change under `cli/`; a change under `cli/reference/`, which
  the cross-process tests build, also runs the Go groups.
  A change under `protocol/` makes it fall back to the full profile, which runs
  the `cli` group and every Go package.

## Continuous integration

`.github/workflows/cli-verification.yml` (**CLI Verification**) runs on every
pull request. Its job `changed` finds whether the pull request changes a path
under `cli/` or `protocol/` or the workflow file; only then does the job
`linux` build and test the workspace on `ubuntu-24.04`, with unprivileged user
namespaces enabled. The job `CLI results` is the workflow's result: it passes
when nothing of the CLI changed, and otherwise only when every job that ran
passed.
