# sandbox

The service that builds and manages the machines agent sessions work in. It
turns Environment definitions into provider artifacts, creates or wakes or
replaces each session's sandbox machine, materializes the session's resources
into it, and owns every provider lifecycle call — start, stop,
checkpoint-archive, delete. It has no public API; it consumes queue jobs.

Two shared subsystems under `engine/internal/sandbox` back the service and are
documented here because this service is their primary owner:

- `internal/sandbox/driver` — the only package that imports the Daytona SDK. It
  turns Tetral lifecycle and tool calls into provider operations and reads the
  results back.
- `internal/sandbox/helper` — source of the static `sandbox` binary installed
  at `/usr/local/bin/sandbox` in the base template. It is the only executor of
  sandbox-backed tool semantics. It runs inside sandboxes, never inside a
  service pod. The Sandbox Service invokes it through the selected provider
  adapter for queued tool execution and resource materialization.

## Responsibilities

Consume Environment, Sandbox lifecycle, resource-materialization, and Sandbox
tool-execution queue jobs, plus the resource-prefix GC poll loop. The service
holds the sole authority over provider lifecycle: it builds deterministic
Environment artifacts, creates/wakes/replaces/releases each Session's Sandbox
from a pinned artifact, projects the Session's resources, and submits approved
Sandbox tool commands. Business truth lives in `environment_artifacts`,
`session_sandbox_bindings`, `sandbox_lifecycle_operations`,
`session_runtime_tool_results`, and the resource stores — never in the Queue
row. Queue jobs carry only stable references to that truth.

The provider API key is a deployment-owned control-plane credential and never
enters public rows, Queue payloads, Runtime payloads, events, model-visible
context, or logs. Runtime Pod, API, Auth, Queue, and Gateway receive no Sandbox
provider credentials.

## States & lifecycle

### Queue kinds consumed

| Kind | Trigger | Effect |
| --- | --- | --- |
| `environment_build` | new/changed Environment `packages` | build deterministic artifact, persist `provider_artifact_ref`, mark generation `ready` or record failure |
| `environment_ready_fanout` | build success | advance `waiting_environment` preparations to `pending`, enqueue their `session_prepare` |
| `environment_failed_fanout` | terminal build failure | settle per-session inputs gated on the build so none is left silently blocked |
| `session_prepare` | session admitted / cold return | run the two preparation stages below |
| `sandbox_tool_execute` | an approved Sandbox Tool Use has a durable execution row | converge the Session binding and materialization receipt, then submit the helper command once and store its normalized result |
| `sandbox_activate` | an execution or materialization observes a missing, stopped, or archived provider resource | resolve/adopt by stable name and labels, create/start/replace as required, then wake durable waiters |
| `sandbox_materialize` | the binding's Environment generation, resource revision, credential, or helper receipt is not current | project resources against the current provider handle and persist one complete materialization receipt |
| resource-prefix GC poll | timer | reclaim orphaned resource prefixes |

An Environment's `networking` is runtime policy and never triggers a build.
Snapshot creation is never called while holding a database transaction.
Artifact reuse is bounded to the same environment lineage with an earlier
`ready` generation and the same input hash.

### Durable Sandbox tool execution

An approved Tool Use owns one full-scope execution row keyed by workspace,
Session, thread, and Tool Use event. The row, not its Queue notification, is
the authority for execution state and result custody.

```text
pending
  -> waiting_activation -> pending
  -> waiting_materialization -> pending
  -> preparing -> running -> terminal_unconsumed -> consumed
```

`preparing` covers provider-neutral payload staging under a persisted deadline;
the user command has not started. The `preparing -> running` transaction
rechecks the current binding, release/cancellation fences, exact materialized
resource revision, credential lifetime, helper verification, and the database
clock. Only after that transaction commits may the adapter make the one
user-facing helper call. A lost worker that finds `running` never submits the
command again; without a durable provider result it records
`unknown_outcome`.

Activation and materialization never hold an execution Queue lease while they
wait. They attach the execution to a durable lifecycle operation and ACK the
execution job. Lifecycle completion issues a new refs-only execution job for
the same Tool Use and increments the business attempt generation. Queue
transport attempts and business execution generations are separate counters.

Every Sandbox job has an explicit positive attempt budget. The last permitted
attempt still runs. A worker receiving a lease beyond that budget finalizes the
business row before dead-lettering the Queue row. A separate bounded
maintenance pass closes jobs whose final worker died: it writes the same
state-specific business outcome and conditionally dead-letters the still-pending
Queue row in one transaction, without calling a provider.

### `session_prepare` — two ordered stages

| Stage | Precondition | Work | Failure rule |
| --- | --- | --- | --- |
| 1. sandbox readiness | environment generation re-resolved; session lifecycle re-checked under the sessions-row lock (a tombstoned session gets no machine — checked before any provider call and again after create) | create the provider sandbox from the pinned artifact with `Name = sandbox_id` (deterministic, so a crash-orphaned machine is rediscoverable by name); persist `provider_sandbox_id` guarded by `WHERE status IN ('creating','resuming')` | a tombstone landing mid-create makes the same job release its own just-created machine |
| 2. resource preparation | stage 1 done; never allocates a new machine | resolve and validate every mount path globally first (collision fails before the first write — fail-before-partial-write), then materialize each resource class with its own idempotent convergence | a failed attempt is not re-driven in place; recovery is a new input allocating a fresh attempt |

Resource classes and their convergence: read-only file projection at mount
paths and memory projected by whole-store swap are the two seams detailed
below (Seam: file-resource projection; Seam: memory projection); GitHub
repositories cloned through the git proxy after sandbox git constants are
written (clone-if-absent); skills extracted under a read-only projection with
the resolved index persisted in the same transaction. A
GitHub clone failure is classified by manifestation: an explicit-credential
manifestation and a not-found manifestation are terminal for the attempt (no
automatic retry), with the failing repository's identity captured onto the
preparation row; everything else takes the ordinary retryable path.

### `sandboxes.status`

`sandbox.Status` (`internal/sandbox/sandbox.go`) tracks the machine; refreshed
from provider state through the driver. A machine is usable only when `active`
and fresh within the freshness window.

| Status | Meaning |
| --- | --- |
| `creating` | provider create in flight |
| `active` | usable when fresh |
| `stopped` / `archived` | sleeping: billing stopped, disk survives |
| `resuming` | start in flight |
| `releasing` | release in flight |
| `released` | provider-deleted, wake failed, or provider-missing observed — never routine TTL cleanup |
| `failed` | provider create/start failure awaiting terminalization |

### Sleep, wake, replace

The outcome the platform promises for an idle session is sleep, not death. TTL
cleanup is driven by Bridge and executed at the provider by this service
through `ReleaseSandbox`.

| `ReleaseReason` (`sandbox.go`) | Provider action |
| --- | --- |
| `cleanup` | stop + checkpoint-archive |
| `runtime_pod_lost` | stop + checkpoint-archive |
| `delete` | provider-delete the machine (session-delete path) |
| `archive` | checkpoint-archive |

A cold return (new input for a sleeping session) routes through one closed
wake-vs-replace decision:

| # | Condition | Outcome |
| --- | --- | --- |
| 1 | row `released` | REPLACE |
| 2 | row's environment generation differs from the freshly resolved current generation | REPLACE (a machine that slept through an upgrade is never woken) |
| 3 | `stopped`/`archived`, generations match, start succeeds | WAKE — same machine/handle; incremental preparation reconciles drift |
| 4 | `stopped`/`archived` but start answers provider-missing or another unrecoverable error | settle row `released`, then REPLACE from durable rows — never a user-visible error; a retryable start error retries in place |
| 5 | `failed` row previously usable with post-failure archive still in flight | DEFER (attempt-neutral wait) until the archive lands and row 3 wakes it |

REPLACE obeys delete-old-first: if the row still records a provider handle, that
machine is provider-deleted (idempotently) before the new create is issued —
the new machine reuses `Name = sandbox_id`, so the old one must be gone first.
Conversation context always comes from durable history; sandbox state never
captured into durable resources is not restored.

Two reconcilers patrol the edges. The stale-startup reconciler covers
`creating`/`resuming` rows gone stale: it inspects before destroying (probing
the recorded handle or the deterministic name) and only claims `failed` what is
provably terminal or absent. The startup-failure cleanup checkpoint-archives
failed machines under an exclusive lease (claim CAS, fresh lease token, attempt
counted at claim) with a finite attempt cap; at the cap it makes one read-only
provider observation before terminalizing, so an already-archived machine is
recorded as success rather than failure.

### Helper result envelope

Every helper subcommand emits exactly one JSON envelope on stdout
(`protocol.Envelope`). Exit code 0 means the envelope is authoritative
(including `status = error`); a non-zero exit means the helper failed before
emitting one, and Bridge synthesizes a `helper_failure` result.

| `status` | Meaning |
| --- | --- |
| `success` | operation completed; `result` carries facts |
| `error` | operation failed; `error.kind` is one of the shared error kinds; may carry a partial `result` |
| `running` | `exec`/`stdin`/`poll` only — the underlying detached task is still alive |

### Helper background-task state discovery

A detached command task is owned by a supervisor process, not by the helper
invocation that started it. `stdin`/`poll`/`cancel` discover state by
`task_id`:

| Observation | Meaning | Behavior |
| --- | --- | --- |
| no task directory | unknown task | `task_not_found` |
| `control.sock` live | running | serve the operation |
| sock dead, `exit.json` present | terminal | serve terminal state |
| sock dead, no `exit.json` | supervisor died abnormally | `task_lost` |

`exit.json` is written before `control.sock` is unlinked, so a client that
finds the socket dead re-checks `exit.json` before concluding `task_lost`.

## Seams

### Seam: sandbox driver (`internal/sandbox/driver`)

The provider-specific boundary. Confining Daytona to one package is what keeps
every layer above the driver free of Daytona SDK types, and it is where the
credential-isolation invariants are enforced. Sandbox execution and lifecycle
resolve a provider-neutral `ProviderAdapter` from the service registry. The
alpha registry is deliberately closed to one installed provider, Daytona;
adding another provider means implementing the complete adapter contract and
registering it at startup.

| Provider-backed role | Constructed at | How it is selected |
| --- | --- | --- |
| `ProviderAdapter` | `services/sandbox/wiring.go` (`NewDaytonaAdapter`) | registered under the configured provider; today only `daytona` is accepted |
| `sandbox.ArtifactBuilder` | `services/sandbox/cmd/tetral-sandbox/main.go` (`NewDaytonaArtifactBuilder`) | constructed as Daytona directly |
| memory materializer / memory-projection refresher / preparation-command runner / preparation file stager (one `DaytonaHelperExecutor`) | `services/sandbox/cmd/tetral-sandbox/main.go` (`NewDaytonaHelperExecutor`) | constructed as Daytona directly |

A replacement provider must cover all of these construction sites, not only
the `cfg.SandboxDriver` switch.

**Interface contract.** The service depends on consumer-defined interfaces the
driver implements; nothing above the driver names Daytona.

| Interface | Location | Implemented by | Purpose |
| --- | --- | --- | --- |
| `ProviderAdapter` | `services/sandbox/provider_adapter.go` | `DaytonaAdapter` | inspect/resolve/activate/materialize/prepare/execute/release with normalized effect boundaries and dispositions |
| `sandbox.LifecycleProvider` | `internal/sandbox/sandbox.go` | `driver.DaytonaLifecycleProvider` | `CreateSandbox`, `StartSandbox`, `CheckBaseTemplateHealth`, `ApplyNetworkPolicy`, `PrepareBaseDirectories`, `GetStatus`, `ReleaseSandbox` |
| `sandbox.ArtifactBuilder` | `internal/sandbox/sandbox.go` | driver artifact builder (`artifact_builder.go`) | `BuildArtifact(normalized packages) -> provider_artifact_ref` |
| `driver.ToolExecutor` | `internal/sandbox/driver/types.go` | `driver.DaytonaHelperExecutor` | `CheckHealth`, `RunTool`, `ReadCommandResult`, `SendCommandInput`, `CancelCommand` |
| `driver.OutputCapturer` | `driver/types.go` | `DaytonaHelperExecutor` | `CaptureOutputs` (Bridge-driven idle output capture) |
| `driver.MemoryProjectionRefresher`, `PreparationCommandRunner`, `PreparationFileStager` | `driver/types.go` | `DaytonaHelperExecutor` | preparation-time transport |

Callers above the adapter pass and receive only Tetral identifiers and normalized results
(`ToolTarget`, `ToolInvocation`, `ToolExecution`, `CommandReference`,
`CommandResult`, `ProviderHandle`, `sandbox.Status`). Provider command IDs,
process-session IDs, and sandbox IDs stay behind this boundary.

**Lifecycle.** Service wiring constructs the driver through the package
(`NewDaytonaLifecycleProvider`, `NewDaytonaHelperExecutor`), passing typed
config — endpoint, target, timeouts, credentials. The driver maps provider
state to `sandbox.Status` (availability and retryable siblings) and provider
errors to `sandbox.ProviderError` with a stage and retryable classification.
For a helper call it stages one payload file, sets it root-owned `0600` in a
`0700` directory, runs `sandbox <sub> --payload <path>` as root, parses the
stdout envelope, and strips provider metadata. A foreground command resolves
inside one bounded invocation (self-capped at the 50 s transport budget, no
polling) and returns a single envelope. A command that would exceed that budget
takes the driver's detach composition instead — `exec` with detach-on-expiry,
then a loop of bounded `poll` calls whose head+tail-bounded output the driver
accumulates across rounds until the task settles.

**Invariants a replacement must preserve.**

- *Credentials never enter the sandbox.* Driver code takes config through typed
  construction only; it must not call `os.Getenv`, read secret files ad hoc, or
  define hidden provider defaults. The provider API key is control-plane and
  never reaches sandbox environments, logs, or public responses. GitHub
  repository clone auth is a separate write-only per-session token injected only
  by `git-proxy` — never placed in the sandbox. Read-only file-resource
  projection mints scoped, expiring credentials
  (`services/sandbox/internal/resourceprojection`); the durable expiry
  drives re-preparation before a tool runs against stale material.
- *Helper privilege boundaries.* Bridge's helper exec runs as root so the
  helper can read the root-owned payload, unlink it, then drop to the sandbox
  runtime user (`driver.RuntimeUser`) for tool effects. The model's runtime user
  can exec the on-PATH binary but cannot become root; the Bridge-internal
  capture mode therefore gates on `euid == 0` and refuses otherwise. Payload
  directories are `0700`, files `0600`; hidden names are never the boundary.
  `driver.RuntimeUser` (`internal/sandbox/driver/types.go`, currently `daytona`)
  is a base-template convention, not a name the provider SDK forces: it is the
  non-root account baked into the base image that the helper drops privilege to.
  A replacement may rename it, but the base image, this constant, and the
  privilege-drop target must stay in agreement, so it is changed as one unit with
  the base template, never independently.
- *Artifact-ref opacity.* `ArtifactBuilder.BuildArtifact` returns a
  `provider_artifact_ref` that the service stores and later pins a sandbox from;
  the service treats it as an opaque provider-defined string and never parses it.
  The contract is only that the ref deterministically reproduces the built
  environment — it need not be a Daytona snapshot id. A provider without a
  snapshot/image primitive may back the ref with any durable handle (an image
  tag, a content digest, a build id) as long as pinning from it is reproducible.
  Snapshot creation runs outside any database transaction; a replacement's
  build/publish step must keep that same no-transaction boundary.
- *Preparation fence.* `CheckBaseTemplateHealth` must pass before any tool
  executes; `PrepareBaseDirectories` runs before any resource is staged; every
  tool/command target carries `workspace_id`/`session_id`/`binding_id`/
  `binding_generation`, which Bridge verifies against the active binding before
  the driver acts. Lifecycle timing and the lease/command safety inequality
  (queue lease minus heartbeat must cover the preparation command timeout plus a
  margin) are typed configuration validated at startup.

**Conformance tests.** `internal/sandbox/driver` — `provider_status_test.go`
(status/availability/retryable mapping), `daytona_test.go` and
`daytona_preparation_test.go` (lifecycle + health + payload staging),
`daytona_output_capture_test.go`, `memory_projection_test.go`,
`github_repository_test.go`, `artifact_builder_test.go`. The driver is the only
production importer of the Daytona SDK — every importer lives under this package;
the complementary static gate `internal/sandbox/helper/contract_static_test.go`
proves the other side of that boundary, that the helper tree stays Daytona-free
(no static gate asserts the driver-only-importer direction itself). The service
uses `sandbox.LifecycleProvider` fakes (`internal/sandbox/service_test.go`) so no
lifecycle test touches a provider.

The driver-only-importer direction — production imports of the provider SDK
live only under `internal/sandbox/driver` — is held by convention, not by a
static gate today. Onboarding a replacement SDK keeps that confinement; adding
a static gate that mirrors the helper-side one is recommended hardening, not a
current requirement. The `external-smoke` CI job runs the driver against a
live provider; it is parameterized by the same config the service reads
(`TETRAL_SANDBOX_DRIVER` plus that provider's credential env). Pointing it at a
different live provider therefore needs the new driver-switch case and the new
provider's config env, not a smoke-specific mechanism.

### Seam: sandbox helper tools (`internal/sandbox/helper`)

The single executor of sandbox-backed tool semantics. A replacement is a
different `/usr/local/bin/sandbox` binary that honors the same payload/envelope
contract; Bridge and this service are indifferent to its internals.

**Interface contract.** `sandbox <subcommand> --payload <path>` for every
operation except `health` (no payload). Subcommands: `exec`, `stdin`, `poll`,
`cancel`, `read`, `write`, `edit`, `apply_patch`, `grep`, `glob`, `view_image`,
`health`. The helper does not know provider tool names; Bridge maps routes onto
subcommands. Payload and envelope Go types live in `helper/protocol`
(`Payload`, `Root`, `Limits`, `Envelope`, `ToolError`, capture types) — the one
package importable by Bridge and by this service; nothing under
`helper/internal/...` is imported outside the helper tree.

**Lifecycle of one call.**

```text
Bridge uploads payload.json under /tmp/tetral-runtime/tool-payloads/<id>/ (0700 dir, 0600 file, root-owned)
helper reads it fully into memory (4 MiB cap), validates tool == argv, then unlinks it before the operation
helper drops to the runtime user and executes the one operation
helper writes exactly one envelope to stdout; stderr is redirected to a log file (never the transport)
Bridge parses the envelope and best-effort removes the payload directory
```

stdout purity is an invariant: children get their own pipes; detached-task
supervisors fully detach their stdio. Every operation self-bounds its
wait/block deadline to 50 s (the per-invocation transport budget), with a
bounded closeout overrun.

**Invariants a replacement must preserve.**

- *Path containment is the security core.* Every path-taking operation
  (`read`, `write`, `edit`, every path in an `apply_patch`, `grep`/`glob`
  roots, `view_image`) resolves against the payload `roots` set, not against a
  single hardcoded root: relative paths join to `workspace_root`; the path is
  lexically cleaned; symlinks are physically resolved (the final target, never
  "the symlink itself"); the resolved path must sit at or under one allowed
  root, else `path_escape`. The most-specific containing root decides the
  mode; a write-class target whose deciding root is `read`-mode is
  `path_escape`. Independently, any path under `/tmp/tetral-runtime` or
  `/dev/shm/tetral-runtime` is `forbidden_path`. Kind checks run after
  containment, so escape probing cannot distinguish an outside file that exists
  from one that is missing. Projected read-only files are enforced twice: as
  their own most-specific `read` root at containment, and by mount-level
  permission as the backstop for `exec`/Bash, which is deliberately exempt from
  workspace containment (a shell can `cd` anywhere; only forbidden roots gate
  its cwd). TOCTOU between resolve and I/O is accepted for model-facing tools —
  the sandbox is single-tenant and the only racing writer is the model racing
  itself.
- *`apply_patch` two-phase atomicity is the other core.* The Codex-compatible
  freeform patch language is parsed into `AddFile`/`DeleteFile`/`UpdateFile`
  ops; two ops resolving to the same target path (including a move destination)
  are rejected before any write. Phase 1 verifies with zero writes — resolve
  and containment-check every source and destination, read targets (32 MiB
  cap), and compute each update to a complete in-memory result via the `seek`
  matcher (exact → right-trim → trim → Unicode-normalize passes, each scanning
  the whole remaining range before the next, an advancing cursor making chunk
  order meaningful). Any semantic failure leaves the filesystem untouched.
  Phase 2 applies in patch order through the shared atomic temp+rename write;
  only a raw I/O failure can produce partial application, reported exactly with
  the committed-path list, never silently.
- *Atomic whole-file writes.* `write`, `edit`, and `apply_patch` phase 2 write
  through one temp+`fsync`+rename sequence, so a crash never leaves a torn
  file and racing same-file writers produce one intact version or the other —
  never interleaved bytes.
  The helper takes no cross-invocation locks; upstream `conflictKey`
  exclusivity is assumed. The one exception is a `flock` reservation guarding
  the 16-live-supervisor detached-task cap.
- *Bounding while capturing.* Every producer is bounded where bytes enter helper
  memory, never buffered whole and truncated after. Command streams keep a
  head+tail ring and keep counting/draining after the cap (to keep the child
  unblocked and totals truthful); file/grep reads stop pulling at the cap.
  Pipe readers drain to EOF before the process is reaped, and every
  kill/timeout path force-closes the read ends so a blocked reader always
  unblocks within that path's budget.
- *Envelope discipline.* Exactly one envelope per call; total size self-capped
  at 256 KiB (`view_image`, Read media, and the capture envelope are the
  exemptions); no payload paths, task-directory paths, supervisor pids, or
  provider identifiers in any envelope. Every detached task has a hard lifetime
  (default 30 min); unattended sandboxes must not accumulate immortal
  processes. Error kinds are a stable enum shared across tools; adding one is a
  contract change.
- *Dependency confinement.* stdlib, `golang.org/x/sys/unix`, the pinned image
  decode dependency, and the pure-Go PDF library only — never the Daytona SDK,
  any `engine/internal/...` service package, or any Kubernetes/DB/blob package.
  The helper knows nothing about Bridge, Runtime, Daytona, events, or
  scheduling.

**Base-template invariants checked by `health`** (preparation fails on any
failure): `sandbox` and `rg` on PATH, the apply_patch engine, `/tmp/tetral-runtime`
at mode `0700`, `rclone` and `fusermount3` present, `/etc/fuse.conf`
containing `user_allow_other`, and `/proc` mounted (the capture mode's
same-inode reopen goes through `/proc/self/fd`).

**Conformance tests.** Per package under `internal/sandbox/helper/internal`:
`pathsafe` (relative join, `..` and absolute escape, symlink file/dir escape,
dangling symlink, forbidden roots, multi-root mode selection, reporting shape),
`patch` (grammar corpus and golden errors, per-pass matcher order, cursor
advance, EOF-anchored and pure-insertion chunks, verify-phase leaves a
snapshot-identical tree, injected phase-2 failure reports the exact applied
list), `filetool` (atomic rename, `expected` guard, edit match/ambiguity/
curly-quote fallback), `bound` (counting readers prove capture stops,
head+tail seam, envelope self-cap trim), `task` (exit codes,
stdout purity, PDEATHSIG, detach/poll/stdin/cancel, lifetime cap, `task_limit`,
`task_lost`), `search`, `media`, `health`. `helper/protocol` proves
payload/envelope round-trip and required-field rejection. `contract_static_test.go`
proves package layout, the static-binary build, dependency confinement, and the
`health` base-template invariant list. `supervisor_privilege_integration_test.go`
(the `helper-privilege` CI job) proves the supervisor keeps its authorization
after privilege drop and that hidden entrypoints reject forged capabilities.
Helper tests run with no network and no Daytona.

### Seam: file-resource projection (`internal/resourceprojection`)

Materializes each declared read-only file resource as a real regular file at
its `mount_path`, backed by canonical bytes in the blob store — readable by the
model, write-rejected, with a writable parent. A replacement must preserve the
object layout, the credential scope, and the five checks; the sandbox and
Bridge see only `mount_path`s and a durable expiry, never a provider or storage
credential.

**Object layout.** One bucket (name from typed blob config, never hardcoded).
Two key families:

| Family | Key | Written by |
| --- | --- | --- |
| Canonical | `files/<workspace_id>/<object_id>` (`CanonicalObjectKey`) | the Files API only; this service reads it, never writes it |
| Per-session copy | `workspaces/<workspace_id>/sessions/<session_id>/resources/<resource_id>/file` (`SessionResourceKey`) | this service, per materialization |

The copy key is deterministic from `(workspace_id, session_id, resource_id)` —
recomputed everywhere (`SessionPrefix`, `SessionResourcesPrefix`), never stored
in a lookup table. The per-session copy is load-bearing, not an optimization:
the mount credential is scoped to the session's `resources/` prefix (the sandbox
sees only this session's declared resources — never the workspace's whole
`files/`, never another session), the prefix presents a listable tree to mount,
the copy stays immutable under the model even if the canonical object is
overwritten mid-session, and the physical prefix holds one session so a storage
bug leaks one session, not the workspace.

**Pipeline (`planner.go` pure plan, then effectful execute).**
`BuildPlan(PlanRequest) -> (Plan, error)` resolves every file resource to
`(resource_id, source_file_id, session_file_id, mount_path)` — mount_path defaulting to
`/mnt/session/uploads/<session_file_id>` — runs the collision planner, and emits ordered
`Action`s: `copy_object`, `mint_credential`, `mount`, `bind`[], `verify`[]. A
collision produces a plan error and zero actions (fail before the first write).
For N file resources the executor issues N server-side copies
(`CopyExecutor.CopyIfNeeded` — copy-if-absent via HeadObject, create-only) and
one mint, neither inside the sandbox, then one mount, N binds, and N verifies in
one driver preparation-exec.

**Collision planner (`planner.go`, pure, lexical over resolved absolute paths).**
Rejected before any action:

| Code | Rejects |
| --- | --- |
| `duplicate_mount_path` | two file resources at the same path |
| `nested_mount_path` | one path a path-component prefix of another (`/a` vs `/a/b`; not `/a` vs `/a/bc`) |
| `duplicate_github_mount_path` / `nested_github_mount_path` | file/GitHub-repo path overlap |
| `reserved_mount_path` | overlap with any `reservedSubtrees()` entry: `/mnt/tetral/r2`, `/tmp/tetral-runtime`, `/dev/shm/tetral-runtime`, `/tmp/tetral/session-prepare`, `/mnt/memory`, `/skills`, `/mnt/session/outputs`; or the `/workspace` root itself (a dedicated branch — a file resource may nest under `/workspace`, but must not claim the GitHub-repo root or repo subtree) |
| `invalid_mount_path` | a path that is not absolute and lexically clean, or is `/` |

**Credential (`credential.go`, `CredentialMinter` over `cloudflare-go/v7`).** One
temporary object-storage credential per materialization: `object-read-only`, one
prefix entry equal to the session `resources/` prefix, TTL from
`TETRAL_RESOURCE_CRED_TTL` (default 24h). The minter asserts the prefix list is
non-empty and exactly the session prefix before every mint — an empty prefix
would silently grant whole-bucket read and list. Only the minted triple enters
the sandbox (rclone SigV4, `RcloneEnv`); the parent credential stays in service
config and never reaches the sandbox, logs, or responses (`CredentialMintError`
redacts, including the mint-error path).

**Mount and bind (`mount.go`).** One generated script per driver
preparation-exec: rclone mounts the session prefix read-only at `/mnt/tetral/r2`
(`RcloneStagingRoot`) with `--read-only --allow-other --vfs-cache-mode full`,
the cache bounded by `TETRAL_RCLONE_VFS_CACHE_MAX_SIZE` /
`TETRAL_RCLONE_VFS_MIN_FREE` (an unbounded cache fills the sandbox disk and
starves `/mnt/session/outputs`). Each resource is bind-projected from
`/mnt/tetral/r2/<resource_id>/file` to its `mount_path` then remounted
read-only; the parent directory is created writable as the runtime user.
Idempotency is explicit: `mountpoint -q` skips a live mount
(`MountAliveCommand`), and a per-target `/proc/self/mountinfo` guard skips an
existing bind and rebinds a stale one, so add / cold-return / rotation / retry
converge instead of stacking binds.

**Degradation ladder (`resource_projection_preparer.go`).** The projection level
is a base-template property discovered by health, not chosen per request:

| `ResourceProjectionLevel` | Mechanism |
| --- | --- |
| `fuse_bind` (default) | rclone read-only mount + per-file bind + remount,ro |
| `local_copy` (fallback) | driver stages bytes to a `0444` local file at `mount_path` (`ResourceLocalCopyStagingRoot`); bytes transit the driver, no in-sandbox credential |

**Five verification checks (`verify.go`, run as the runtime user; any failure
fails prepare):** the path exists and is a regular file; its first byte reads
(exercises the `--allow-other` cross-user surface); `open(O_WRONLY)` fails
EROFS; a temp file in the parent creates and unlinks; the copy is present
(proved by copy success before any bind).

**The hard boundary is the credential, not in-sandbox read-only.** The runtime
user has passwordless sudo, so bind / remount / ownership are soft. Every
integrity claim rests on the credential: object-read-only rejects every write at
the store however the sandbox reaches the mount; the prefix scope makes the
canonical `files/` prefix and every other session unreadable and un-listable;
the TTL bounds an exfiltrated credential; the parent credential (the
cross-tenant root of trust, never in the sandbox) is the only thing that could
exceed a session's scope. Because a minted temporary credential can never exceed
its parent, the parent must itself be scoped `object-read-write` to the single
configured blob bucket (`TETRAL_BLOB_BUCKET`) and rotated — never account-level
or admin. An account-admin parent would turn a parent-token leak into an
all-tenant compromise; the bucket scope is what caps that blast radius to one
bucket.
Cross-session and cross-workspace isolation is enforced here, at the storage
credential — not by any in-sandbox guard.

**Idle add / delete (`teardown.go`).** Add at idle re-runs the pipeline
incrementally (guards skip existing copies/mount/binds; a live mount reuses the
existing credential and carries its expiry forward via `MountAliveCommand`).
Delete at idle unbinds `mount_path`, removes the placeholder, deletes the
session copy object, and tombstones the row (`DeletedFileCleanupTarget`,
`RunDeletedFileCleanup`); deleting the last file resource also unmounts
`/mnt/tetral/r2`. Add or delete on a running session is a 409 at the API.
Canonical bytes are never touched.

**Cold return.** Re-materialization is the pipeline again: copies skip (session
copies survive the sandbox), the credential is re-minted, the mount and binds
are re-established, the five checks re-run.

**Credential expiry and rotation (`rotation.go`).** The mint response carries no
expiry, so expiry is computed as `now() + ttl` and persisted as the
preparation's `resource_cred_expires_at`. A preparation is stale for tool
execution once `now() > resource_cred_expires_at -
TETRAL_RESOURCE_CRED_REFRESH_MARGIN` (default 30m); Bridge (which reads the
durable expiry, never mints) resets the preparation and re-enqueues
`session_prepare`. Live rotation is the rare fallback for a session that
outlives its credential: no live mount can re-take a fresh SigV4 credential, so
rotation tears down every bind, lazy-unmounts the staging mount, re-mints,
remounts, rebinds, and re-verifies in order — distinct from cold return, which
assumes the mount is already gone.

**Failure matrix.**

| Condition | Result |
| --- | --- |
| mount_path collision | prepare fails at plan; zero mutation |
| canonical object missing at copy | prepare fails; no bind |
| mint error / empty-prefix assertion / bad parent key | prepare fails retryable; nothing mounted |
| rclone mount fails | prepare fails (degrade per ladder only if the template lacks FUSE/caps) |
| a bind or a check fails | prepare fails; no attempt-scoped teardown — partial copies/binds linger unobservably on the session's own sandbox and the next attempt reconciles idempotently |
| credential expired mid-run | remote read → auth error → retryable transient; readiness gate re-preps; never wrong bytes |
| add / delete on running session | 409 |

No condition yields a partially-materialized success: either every declared
resource verifies or the preparation is unready.

**Conformance tests.** `internal/resourceprojection` — `planner_test.go` (plan
golden tests, default mount_path resolution, deterministic keys,
incremental-skip, collision table for each code and reserved path),
`credential_test.go` (bucket/permission/prefix builder, the non-empty-prefix
assertion, secret redaction on the error path), `copy_test.go` (copy-if-absent,
HEAD-skip, recopy-on-mismatch, canonical-missing), `teardown_test.go`
(delete/unbind/last-resource unmount). Service-level
`resource_projection_preparer_test.go` and `resource_projection_live_test.go`
cover the preparer orchestration and the level fallback.

### Seam: memory projection (`internal/sandbox/memory_projection.go`, driver `internal/sandbox/driver/memory_projection.go`)

Projects each attached memory store into the sandbox as a read-only directory
tree. PostgreSQL is the source of truth; the projection is disposable and
re-materialized, never read back into durable state.

**Layout.** For every `session_memory_store_resources` row (both `read_write`
and `read_only`), each active memory becomes `<mount_path><memories.path>` — a
`root:root 0644` file; directories are `0755`; a store with no active memories
is an empty directory. The mount path is read verbatim from the row (written
once at session create), never re-derived. Store `name` / `description` /
`instructions` are never written as files — they render into the system prompt
elsewhere; the projection holds memory content and nothing else.

**Transport (driver command sequence — not a helper command, no object store, no
FUSE).** Memory truth is KB-scale text in PostgreSQL, so bytes flow directly:
`FileSystem.CreateFolder` + `FileSystem.UploadFileStream` stage, then one root
`Process.ExecuteCommand` moves them into place — the same physical leg the
helper payload path uses. The staging root is `/mnt/memory/.staging/<opaque-id>`
(`memoryProjectionStagingRoot`), a sibling of the store mounts on the same
filesystem, so every `mv` is an atomic `rename(2)`. `/tmp` is never used for
staging: a tmpfs `/tmp` degrades `mv` to copy+unlink, where a reader can observe
partial bytes. Command strings carry only shell-quoted paths, never content.

**Whole-store swap at prepare (`MaterializeMemoryProjections`).** The prepare
step builds an in-memory `tar.gz` plus a `SHA256SUMS` manifest from
`memories.content_sha256`, uploads both, then extracts, runs `sha256sum --check`
inside the sandbox as the materialization verification, normalizes permissions,
and swaps with `rm -rf MOUNT && mv -T STAGE/extract MOUNT` (`-T` fails loudly
rather than nesting if `MOUNT` reappears). An empty store yields a bare `0755`
directory. A prefix-freeness scan runs first: a filesystem cannot host `/a` and
`/a/b` both as files, so a snapshot with an ancestor/descendant pair fails the
step before any materializer call — the scan walks each path's `/`-boundary
prefixes, not an adjacent-pair sort, because byte order places `/a.txt` between
`/a` and `/a/b`.

**Live refresh (per-file).** Post-mutation refresh (driven by Bridge's
`RunMemory`, orchestration owned there) never swaps the whole directory while a
model may be reading: the same driver surface pushes per-file staged upload +
`mv -f` (atomic rename) for an upsert, and `rm -f` plus `rmdir` of
exactly-emptied ancestors for a remove, never touching the mount root
(`MaterializeMemoryStore` and `RefreshMemoryProjection` are one driver method
family, two callers).

**Seams and serialization.** The prepare unit is pure planning + orchestration
with no Daytona import, depending on three consumer-side interfaces beside it:
`MemorySnapshotReader` (loads the durable snapshot), `MemoryStoreMaterializer`
(the driver swap), and `MemoryStoreMutationLocker`. The per-store mutation lock
is held across both the snapshot read and the swap (a read-modify-write), and
the same lock key serializes the prepare-time swap against a concurrent live
refresh, so a live re-preparation and a memory mutation never clobber
`/mnt/memory/<store>` at once.

**Invariants a replacement must preserve.** Projected files are exactly
`memory_versions.content` (verbatim UTF-8, no BOM, no newline normalization);
every file `root:root 0644`, every directory `0755`; no metadata files; staging
on the projection filesystem so swaps and per-file moves are atomic renames; the
prepare swap and live refresh share the per-store lock; a failed command fails
the memory step and thereby `session_prepare` (fail before partial write), with
the next attempt's `rm -rf MOUNT` swap the only cleanup needed.

**Conformance tests.** `internal/sandbox/memory_projection_test.go` (per-store
materialization regardless of access, verbatim mount path, empty-store branch,
prefix-conflict rejection including the non-adjacent `{/a, /a.txt, /a/b}` case);
driver `internal/sandbox/driver/memory_projection_test.go` (staged-rename
upsert, remove without touching the mount root, whole-store snapshot with
`sha256sum --check` and the empty-store branch, non-zero exit propagates,
directory-remove is a no-op).

## Testing guide

| Suite (file) | Proves |
|---|---|
| `internal/sandbox/service_test.go` | the create/start orchestration: provider setup keeps a stable handle, active is marked before resource materialization, the committed row plus final network policy is what start reads, and a failed stage releases the provider handle while preserving the stage error |
| `internal/sandbox/session_prepare_test.go` | preparation claims through ready: resource credentials survive without implicit rotation, a deleted GitHub checkout is removed before a same-path file projection, and the exact-path replacement matrix |
| `internal/sandbox/postgresql_store_test.go` | the store's durable behavior against PostgreSQL: provider handle and release states, delete-release settling a superseded startup cleanup, wake-refresh never superseding a delete-release owner, wake-failure preserving cleanup fields |
| `internal/sandbox/github_preparation_test.go`, `github_preparation_postgresql_test.go` | repository preparation: mount-path collisions rejected before ticket rotation, default and explicit mount materialization, live-before-clone phase order, and retry safety at each phase boundary |
| `internal/sandbox/memory_projection_test.go` | memory projection builds a verified snapshot per store, removes deleted stores under the mutation lock, retries removal failure before detach, and rejects a prefix-conflicting snapshot |
| `internal/sandbox/helper/contract_static_test.go`, `supervisor_privilege_integration_test.go` | the helper contract and privilege model: package layout, static build artifact, no service/provider imports, detached-task authorization surviving privilege drop, hidden entrypoints rejecting forged capabilities, non-root identity constants |
| `services/sandbox/environment_artifact_store_test.go`, `environment_runner_test.go` | environment build and fan-out: ready enqueues fan-out and advances same-input followers, terminal failure fails waiting preparations, lease loss cancels the build without a durable outcome |
| `services/sandbox/provider_adapter_test.go`, `tool_execution_runner_test.go` | provider outcome normalization, stable activation adoption, pre-submission preparation, one authorized helper submission, and no replay after an unknown outcome |
| `services/sandbox/execution_store_test.go`, `lifecycle_store_test.go`, `lifecycle_runner_test.go` | binding convergence, exact materialization receipts, database-clock authorization fences, activation/materialization waiter handoff, and lifecycle exhaustion fan-out |
| `services/sandbox/queue_over_limit_reconciler_test.go` | bounded over-budget discovery and atomic business-result plus Queue dead-letter settlement after worker loss |
| `services/sandbox/session_prepare_runner_test.go` | the prepare runner's durable transitions, retry without ack, failure before retry-exhaustion dead-letter, and dead-lettering invalid payloads before the handler |
| `services/sandbox/release_handler_test.go`, `server_test.go` | release idempotency on key and fence, runtime-pod-lost using the binding fence without a cleanup claim, unclaimed cleanup jobs rejected, and the gRPC surface's reason/identity/status mapping |
| `services/sandbox/resource_prefix_gc_runner_test.go` | the prefix GC runner: due unbound prefixes deleted and marked, bound/active markers skipped, retryable failure keeping the marker due later, workspace ID required |
| `services/sandbox/resource_projection_preparer_test.go`, `resource_projection_live_test.go` | file-resource projection: copy/mint/command ordering and returned metadata, twenty files batched under one mount and credential, credential-mint rejection stopping before mount, skill package hash mismatch rejected; the live suite covers the FUSE bind, local-copy fallback, oversized reads, and temp-credential prefix isolation |
| `services/sandbox/stale_creating_reconciler_test.go`, `startup_cleanup_reconciler_test.go`, `workspace_consumer_test.go` | the reconcilers scan only their due rows, and the workspace consumer visits every discovered workspace with backoff across consecutive empty polls |
| `services/sandbox/config_test.go`, `authz_test.go`, `k8s_manifest_test.go` | `ConfigFromEnv` pins the lease/cleanup budgets and rejects concurrency above transport capacity; release is scoped to Bridge; the manifest carries the static internal-gRPC token and projection knobs |

Run every suite that opens PostgreSQL with the race detector on. The live
resource-projection suite needs a real sandbox provider and is opt-in. If a PR
changes Sandbox execution states, binding or materialization receipts, the
release/terminal-status mapping, the reconcilers, the provider-adapter surface,
the helper contract, or either projection layout, it updates the matching row
here.

## Boundaries

This service never delivers Runtime input, never talks to Runtime Pod, and
never writes conversation events or messages. It stores a normalized terminal
Sandbox result for a durable Tool Use; Runtime authors the corresponding Tool
Result and Bridge commits that conversation write before marking the stored
result consumed.
Lifecycle timing (stop timeouts, auto-stop/auto-archive/auto-delete intervals)
and the lease/command safety inequality are typed configuration of this service
alone, validated at startup as whole seconds; the auto intervals are
required-positive because they are the sole re-sleep mechanism for a machine
woken without a runtime binding. If a PR changes the job kinds, Sandbox
execution states, binding or materialization receipts, the wake-vs-replace
decision, the release/terminal-status mapping, the reconcilers, the
provider-adapter surface, the helper
payload/envelope contract, the file-resource projection object layout /
credential scope / five checks, or the memory projection layout / transport, it
updates the matching section here.
