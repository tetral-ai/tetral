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
  final and admitted to the retained-report memory budget; every send,
  retries included, carries
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
enable by default and no product crate's normal dependency may enable,
directly or through other features, so that the released binary holds none.
The test-only harness may enable seams for explicit in-process test contexts;
it remains forbidden from the release dependency graph.
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
at a time, in arrival order, and no state is shared behind a lock. An owner
that keeps deadlines acts on every one that has passed before it handles the
message it woke for, so a message taken late never undoes a deadline. Apart from
the three exceptions below, a handler never awaits another owner's reply: it sends its request with a reply channel,
and a spawned task awaits the reply and posts it to the owner's mailbox as a
new message. Three existing serialized paths are exceptions: Drive sync awaits
Content cache adoption after an accepted commit; Drive sync awaits Connection's
local lost-change recording attempt before dropping abandoned pending changes;
and the App Server lifecycle queries Connection's Drive-sync list and Drive-sync
tables when beginning idle exit. These callees do not await the waiting caller.
The caller cannot process other messages until the reply; these exceptions
preserve error handling and make no guarantee of successful persistence on a
callback or bounded queue/filesystem latency. They authorize no new inline waits
or waits for network completion. Stream readers, request tasks and local API
handlers are not owners; they send messages and await replies.

| Owner | How many | Crate | Owns |
|---|---|---|---|
| Connection | One per App Server | `connection` | The connection to Engine: authentication, the event stream and its resume, ready, heartbeats, credential renewal, and the routing of Session instructions; the count of Session records, the directory probe slots and the task pressure point `tetral_tasks` |
| Session binding | One per Session on this machine | `session` | The Session record, its bindings and their states, and the admission of new work |
| Task supervisor | One per task | `execution` | One task's process tree |
| Report outbox | One per App Server | `connection` | Immutable encoded report bodies, their retained-memory charges, sending and retries |
| Drive sync | One per Session on this machine | `drive` | The Session's drive sync state, the checks before a command, uploads and catch-up |
| Content cache | One per machine | `drive` | The cached file contents and the slots that bound concurrent uploads and downloads |
| MCP manager | One per App Server | `mcp` | The local MCP servers and each binding's MCP connections |
| Earlier task records | One per App Server | `execution` | The records under `execution/` that an earlier App Server left, their confirmation checks and the set of Sessions they keep `cleanup_pending`, which Session binding owners and the local API read through a `watch` channel |

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
new one adds it there in the same change. The App Server serves its metrics
at `GET /v1/metrics` in the Prometheus text format 0.0.4, and `tetral diag
--app <app> --output <path>` bundles the log files, `config.toml`, the
system's facts and the App Server's status and metrics into a `tar.gz` for
the app's support, never `credentials/credentials.json` or a key. No library crate
prints, and only `tetral` may write to stdout, where a command's result
object goes: library crates deny the print lints, and
`tests/tests/crate_graph.rs` fails on a print or `dbg!` call in their
sources.

Pressure metrics belong to admission boundaries. A wait inside admitted work
does not create another family: upload progress, sync-point/final-upload waits
and Git completion use their existing owners and diagnostics.

| Boundary | Existing observation | Owning evidence |
|---|---|---|
| Directory probe slots | `tetral_probes_*` | Connection owner and preparation process tests |
| Task admission and staging | `tetral_tasks_*`, rejection-episode records | Task admission and local-service process tests |
| Upload slots and throttling | `tetral_uploads_*` | Cache/DriveSync scheduling and HTTP upload through the local metrics route |
| Download slots and throttling | `tetral_downloads_*` | Cache FIFO, cancellation and clock-driven queue-age tests |
| Report body budget | `report.memory_exhausted`, `drive.report_deferred` | Outbox accounting and durable-notice refusal/recovery tests |

## Reference control server and cross-process tests

`reference/` is the reference control server, Engine's side of the client
connection for the CLI's tests, with fault-injection controls on a second
listener; its README states its rules and controls. It is Go in the
repository's root Go module, not part of this workspace, and
`reference/cmd/tetral-reference` runs it. `reference/wire/` holds its own
types for the protocol of `protocol/` at the repository root, written apart
from the Rust types of `protocol` and tested against the same fixtures;
neither set imports the other or any Engine package but, for the Go set,
`internal/pathvalidation`, the repository's one owner of the NFC check.

The cross-process tests of `tests/tests/` run `tetral`, built with the feature
`test-seams`, and, where a test needs Engine's side, the reference control
server as separate processes; the harness in `tests/src/` builds both, drives
the clock and barriers of `state::seams`, and writes the acceptance record of
each test file of `tests/tests/`. A release
build enables no test seam, and `tests/tests/release_seams.rs` checks that
its binary holds none.

## Computer Use

Computer Use is the local MCP server `cua`: the official, unmodified
cua-driver release that `UPSTREAM.md` pins, run as `<driver> mcp`.

- Installing the release authorizes nothing. On a user's computer the
  developer's app installs it, explains the risks, and the user enables
  Computer Use and grants the system permissions; on a cloud computer the
  image installs it and the deployment enables it with no interactive step.
  Both run `tetral computer-use enable --driver <absolute path of the
  cua-driver executable>`, and `tetral computer-use disable` to turn it off.
  On a Mac one `/Applications/CuaDriver.app` serves every program that uses
  cua-driver, so the developer's app replaces an existing one only after
  telling the user and the user agrees.
- Each time `cua` starts, the App Server first runs `<driver> --version` for
  at most 5 seconds and keeps the `<v>` of a single output line
  `cua-driver <v>`, or null for any other answer; `cua` starts either way.
  While `config.toml` records Computer Use enabled, `GET /v1/status` shows
  `cua_driver` as `{version, tested_version}`; `tested_version` is
  `CUA_DRIVER_TESTED_VERSION` in `crates/mcp/src/driver_version.rs`, which a
  test keeps equal to the cua-driver row of `UPSTREAM.md`. A `version` other
  than `tested_version`, null included, writes one warning record,
  `mcp.driver_version_untested`, with `cua_driver.tested_version`, and
  `cua_driver.version` when it is not null, and changes nothing else; on a
  Mac a different version shows that another program's install replaced the
  tested `CuaDriver.app`.
- The App Server runs the driver only as `<driver> --version` and
  `<driver> mcp`, with no other argument, and sets, changes or removes no
  `CUA_DRIVER_*` variable. The driver's permission mode, risk
  acknowledgement and policy files come from the environment of the process
  that owns the driver's runtime, an unset mode being the driver's
  `standard`: on Linux `<driver> mcp`, whose environment is that of
  `tetral server run`; on macOS `CuaDriver.app`, as it was launched.
- On Linux the driver needs a desktop session reachable through that
  environment. A machine without one, or without the permissions, shows it
  through the driver itself: a failed start, a shorter tool list or call
  errors, each reported unchanged.
- The driver's `session` input is the driver's own lifecycle label. The CLI
  forwards it inside `arguments` like any other input; it is neither a
  Tetral Session nor an authorization, which is Engine's decision for each
  call.
- Each binding's calls go through its own `<driver> mcp` process, so the
  driver keeps a session per binding. On Linux each such process runs its own
  driver, unless the driver's installed configuration routes `mcp` through
  its own service; on macOS the processes share `CuaDriver.app`. What they
  share decides by its own rules whether concurrent actions wait for each
  other, run at once or are refused: on macOS a call that names a window
  waits on the driver's own locks, a text input into a process that is
  already receiving one is refused at once with the driver's `input_busy`
  refusal, and foreground typing into different processes runs one after
  the other. The CLI adds no lock, never
  gives one Session the whole desktop and passes the driver's results and
  errors unchanged. Separate driver sessions are not separate desktops.
- Desktop effects already made stay after a cancel, a timeout, a disable, a
  binding close or a disconnection, and no desktop operation is replayed.

## Versions

`cli_version` is `[workspace.package] version` in `Cargo.toml`, a SemVer
version that every crate inherits; `tetral --version`, the ready message and
`GET /v1/status` report it. Its prerelease part names the stage:
`-alpha.<n>`, then `-beta.<n>`, and none for the final release. A release is
one commit on `main`, its version and the tag `cli-v<version>` on that
commit. What each release speaks:

| Interface | Version a release speaks |
|---|---|
| Connection `protocol_version` | `1` |
| Tool contract, `capabilities.tool_execution.schema_version` | `1` |
| Local MCP, `capabilities.local_mcp.schema_version` | `1` |

Engine's minimum and recommended CLI versions decide whether it accepts a
release; a CLI below the minimum stops at `cli_upgrade_required`.

## Releases

`scripts/package.sh <target>`, run on a machine of that target, writes
`target/dist/tetral-<target>.tar.gz` and prints its SHA-256. Every archive
holds `tetral` and `THIRD-PARTY-NOTICES`, which lists each crate built into
that `tetral` other than the workspace's own, with its version and licence,
followed by the licence texts. cargo-about writes it from `about.hbs` and
`about.toml`, whose accepted licences are those of `deny.toml`; a crate whose
licence cannot be identified or is not accepted stops the script before any
archive is written.

- For `x86_64-apple-darwin` and `aarch64-apple-darwin` the archive holds
  only these two.
- For `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` it also
  holds `bwrap`, bubblewrap's licence `COPYING`, libcap's licence
  `COPYING.libcap` and OpenSSL's licence `COPYING.openssl`. The script
  checks the libcap and OpenSSL tarballs that `UPSTREAM.md` pins against
  their SHA-256, and the Codex tag that `Cargo.toml` pins against the commit
  `Cargo.lock` records, before it builds anything. It builds `bwrap` from
  that tag against a static libcap, then `tetral` against a static OpenSSL
  and with `bwrap`'s SHA-256, so that this `tetral` accepts only that
  `bwrap`.

Build prerequisites, beside the toolchain of `rust-toolchain.toml` with the
target added (`rustup target add <target>`) and cargo-about (`cargo install
--locked --features cli cargo-about --version <CARGO_ABOUT_VERSION>`):

| Target | Prerequisites |
|---|---|
| Linux | A musl C compiler, `<arch>-linux-musl-gcc` or `musl-gcc`; the build machine's Linux kernel headers in `/usr/include`; `make`, `ar`, `ranlib`, `perl`, `pkg-config`, `curl`, `xz`, `git`, `sha256sum` and `nproc`. On Ubuntu: the packages `musl-tools pkg-config make perl curl xz-utils git` beside `gcc` and `libc6-dev` |
| macOS | The Xcode command line tools |

`cargo test --test release_archive -- --ignored`, with `TETRAL_TEST_ARCHIVE`
naming an archive of the machine's target and `TETRAL_TEST_ARCHIVE_SHA256`
the digest the script printed, checks the archive's entries and digest,
checks that `THIRD-PARTY-NOTICES` names every package that `cargo tree`
lists as built into `tetral` for that target, and runs its `tetral` against
the reference control server; on Linux it also
checks that `tetral` and `bwrap` are static, runs a command under the
archive's `bwrap`, and checks that a changed `bwrap` is refused.

Apps that embed the CLI follow the integration guide,
[`docs/app-integration.md`](docs/app-integration.md);
[`examples/app-integration.sh`](examples/app-integration.sh) performs an
app's side of it.

## Dependencies

The CLI depends on no Engine service, database, queue or internal package, on
no provider SDK and on no provider credential. `UPSTREAM.md` lists every
outside source with its pinned version and licence, and how each is raised:
the CLI's owner raises Codex by hand when they choose, with no fixed
schedule, and a raise whose gate fails is not made.

`cargo deny check advisories licenses sources` in `cli/` checks every
dependency against the RustSec advisories, the licence list of `deny.toml`,
which holds no AGPL licence, and its sources, which admit crates.io and only
the Codex crates' Git sources; a finding fails it.

## Build and test

Every `cargo` command runs with `cli/` as its working directory, so that
`rust-toolchain.toml` selects the pinned toolchain; none passes
`--manifest-path cli/Cargo.toml` from the repository root.

- `cargo build` and `cargo test` in `cli/` build and test the workspace.
  `cargo test` needs Python 3 for Linux live-probe fixtures and Go, with which
  the harness builds the reference control
  server for the tests that run it and whose version each acceptance record
  names, and `TETRAL_ACCEPTANCE_DIR` naming the
  directory that the acceptance record of each test file of `tests/tests/`,
  `<test>_<os>-<arch>_<commit>.md`, is written to; a test fails at once
  without it.
- `go test -race ./cli/reference/...` at the repository root tests the Go
  packages under `reference/`.
- `make test-affected` at the repository root runs the evidence group `cli`
  (the same two commands, with the acceptance records under its output
  directory) for a change under `cli/`; a change under `cli/reference/`, which
  the harness builds for the cross-process tests, also runs the Go groups.
  A change under `protocol/` makes it fall back to the full profile, which runs
  the `cli` group and every Go package.

## Continuous integration

`.github/workflows/cli-verification.yml` (**CLI Verification**) runs on every
pull request. Its `changed` job selects changes to `cli/`, `protocol/`, the
reference server's dependencies, the workflow, and paths with unknown ownership.
Three independent Linux matrix jobs cover ordinary functionality, release seam
exclusion, and the serial 5,000-task regression on `ubuntu-24.04`, with
unprivileged user namespaces enabled. The `macos` job runs `cargo build` and
`cargo test` natively on `macos-15`. `CLI results` requires every selected job
to pass, or accepts them skipped when verification was not selected.

Adding `cli-build-benchmark` can trigger verification and compilation measurement.
Other label additions skip all verification jobs and use a separate workflow
concurrency group, so they cannot cancel running or pending verification. Normal
code updates and benchmark-label additions share the pull request's usual group.
The main-only cache producer and read-only pull request restore are described in
[testing](../docs/testing.md); remote cache savings require measurement, and
requiring `CLI results` depends on the workflow first landing on `main`.

`.github/workflows/cli-dependency-check.yml` (**CLI Dependency Check**) runs
the advisory, licence and source check in its job `cargo-deny`, with the
cargo-deny version it pins, on each pull request that changes `cli/Cargo.toml`,
`cli/Cargo.lock`, `cli/deny.toml` or the workflow, and daily at 06:41 UTC.

`.github/workflows/cli-release.yml` (**CLI Release**) publishes releases. A
pushed tag `cli-v<version>` runs `tag-check`, which accepts only a tag that
names this `Cargo.toml`'s version on a commit of `main`; then one `build`
job per target, which installs the cargo-about version that
`scripts/package.sh` pins and runs the script and the archive test on a
GitHub runner of that target; then `release`, which writes `SHA256SUMS` and
publishes the GitHub Release `cli-v<version>` with the four archives,
`SHA256SUMS` and the notes `cli_version: <version>` and
`cua-driver (tested): <version>`, a prerelease while the version has a
prerelease part. Adding the label `cli-release-check` to a pull request runs
the four `build` jobs on its merge commit and publishes nothing; removing
the label and adding it again runs them again.
