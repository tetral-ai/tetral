# sandbox

The Sandbox Service is the queue-driven control plane for provider-backed
execution environments. It activates and releases provider resources,
materializes durable Session resources, executes approved sandbox tools,
captures outputs, reconciles background commands, and builds Environment
artifacts. It exposes health and metrics over HTTP and has no product or
internal command RPC surface.

## Responsibilities

The service owns provider interaction. Runtime declares an approved Tool Use;
Bridge records the durable execution request and its Queue notification; the
Sandbox Service resolves the selected provider adapter and performs the work.
Bridge never imports a provider SDK, invokes the helper, or decides provider
lifecycle transitions.

Business truth lives in PostgreSQL, not in Queue leases or worker memory:

- `session_sandbox_bindings` records the current logical Sandbox binding,
  provider handle, materialized revisions, credential expiry, and release
  fence for a Session.
- `sandbox_lifecycle_operations` records activation, materialization, and
  release operations.
- `session_runtime_tool_results` records Sandbox execution custody, provider
  submission state, and normalized results.
- the output-capture, background-command, and resource tables hold their own
  durable state.

Queue rows carry stable references to these facts. A worker may disappear and
another worker may resume from PostgreSQL without inheriting process state.

## Queue Work

| Kind | Durable operation |
| --- | --- |
| `environment_build` | build a provider artifact for an Environment generation |
| `environment_ready_fanout` | release lifecycle operations waiting for that artifact |
| `sandbox_activate` | inspect, adopt, create, replace, or start one provider resource |
| `sandbox_materialize` | converge the current Environment and Session resource revision |
| `sandbox_release` | inspect and release one fenced provider handle |
| `sandbox_tool_execute` | prepare, submit, observe, and settle one approved Tool Use |
| `sandbox_tool_cancel` | apply a durable business cancellation to one execution |
| `sandbox_output_capture` | scan and stage one FinishIdle output generation |
| `sandbox_output_capture_cleanup` | remove an expired unadopted capture generation |
| `sandbox_memory_projection` | reconcile live memory-store changes |
| `sandbox_background_command` | send input to or cancel a provider command |
| `sandbox_background_reconcile` | observe a detached command and record completion |

Sandbox execution and lifecycle kinds have explicit positive attempt budgets.
Environment build/fanout notifications use the Queue default when unset.
Queue attempt counts govern transport only; lifecycle-operation and execution generations govern business
re-entry. A worker that receives an over-budget job settles the referenced
business row before dead-lettering the Queue row. The bounded reconciler safely
logs a candidate that cannot be settled and continues through the rest of the
batch.

Provider authorization, Blob custody, terminal settlement, and live exhaustion
remain under the source Queue lease. Workers keep heartbeating through the last
fenced business transaction, which locks the source Queue row after business
rows and rejects an expired or replaced token before commit. The heartbeat is
stopped before the final Queue transition, so stale workers cannot acknowledge,
retry, or dead-letter work after losing execution authority.
Environment artifact workers apply the same lease guard before budget checks,
payload decoding, or business claims; malformed and exhausted work settles the
addressed artifact generation before its Queue row is dead-lettered.

## Environment builds

`environment_build` submits or observes one deterministic provider snapshot for
an Environment generation. `pending`, `building`, and `pulling` snapshot states
are successful progress observations. They do not consume a failure attempt.
Each worker records progress, releases artifact custody, calls Queue `Defer`,
and returns. Queue persists a 30-second `available_at`, refunds that lease's
attempt, and clears its lease. The existing consumer loop picks it up when due;
there is no per-build cron or worker held open for installation.

Artifact `status=building` means a worker owns the artifact; `status=pending`
can mean scheduled observation of a build still running at the provider.
`provider_build_state` and `provider_build_ref` preserve the provider lifecycle
independently of that custody. The immutable workspace/Environment/generation/
package-hash identity reconstructs the original snapshot name after restart.
The persisted create guard permits another submission only after proven provider
rejection; an ambiguous result is observed until visible or timed out.

On the first claim, the artifact persists `build_started_at`, `build_warn_at`,
and `build_deadline_at`. Previously submitted live builds use their submission
time. These values never reset on deferral, lease handoff, or configuration
changes. `TETRAL_SANDBOX_ENVIRONMENT_BUILD_WARN_AFTER` defaults to `10m`;
`TETRAL_SANDBOX_ENVIRONMENT_BUILD_TIMEOUT` defaults to `30m` and must exceed the
warning interval. Helm exposes these as `sandbox.environmentBuildWarnAfter`
and `sandbox.environmentBuildTimeout`; canonical manifests read them from
`sandbox-config`. Deployment changes apply only to builds without saved timing.
The first observation at/after the warning threshold persists
`build_warned_at` and logs `waiting_overdue`. The first processing at/after the
hard deadline settles the artifact and waiting activation/tool calls with
`environment_build_wait_timeout`, then dead-letters the QUEUEJOB. Individual
provider calls are bounded by 45 seconds or the remaining build time, whichever
is shorter; an Active response received at/after the deadline cannot activate.
The deadline bounds acceptance of an observation, not just dispatch of a query:
late Ready or Failed responses settle as Engine wait timeout. This keeps the
same cutoff regardless of whether time expires before or during a provider call.
Availability is a minimum scheduling time, not a guaranteed execution time.

An Active snapshot with matching identity and a provider ID becomes a ready
artifact. The same transaction enqueues ready fanout for waiting generations
with identical package input; changed package input is untouched. Fanout
re-enqueues waiting activation operations. Explicit provider `error` or
`build_failed` settles dependents with `environment_provider_build_failed`.
Transient query/submission errors persist their safe diagnostic under
`observe_artifact` and defer observation until recovery or the original deadline.
Permanent configuration/protocol errors retain their distinct terminal reasons.
A missing artifact provider is immediately terminal (`provider_configuration_invalid`),
including on the first lease. Production startup rejects incomplete Daytona
adapters before starting consumers; retrying a known invalid provider selection
is not part of the observation policy. The adapter classifies malformed provider
states and Ready results missing either the snapshot ID or snapshot name as
`provider_response_malformed`. An invalid adapter outcome reaching the runner is a control-plane
contract violation and retains custody for reclaim rather than settling it as
a provider build failure.
Control/store errors leave custody for reclaim and fenced business settlement
rather than dead-lettering a notification before its dependents can be settled.

Timeout stops Engine observation; it does not cancel the provider build. Existing
failed artifacts and already-settled tool results are not reopened, even if the
provider later becomes Active. No historical recovery or implicit replay is
performed. Migration V4 adds nullable observation fields without rewriting old
terminal records.

Logs distinguish `submitted`, `waiting`, `ready`, `provider_failed`,
`observation_failed`, `waiting_overdue`, and `timed_out`, including the safe
provider build reference and original deadline. `provider_build_ref` always
records the adopted snapshot name (also for same-input followers);
`provider_artifact_ref` records its usable snapshot ID. The name is the diagnostic
handle; unbounded installation logs are not collected or published.

Tests use real PostgreSQL and the actual Queue server/store, builder, adapter,
and artifact/activation writers, with deterministic Daytona responses and
controlled scheduling time. `environment_build_integration_test.go` proves
waiting beyond the failure budget, deadline persistence, late-response rejection,
query recovery, first-claim custom timing, pre-V4 live-build handoff, terminal
redelivery, and stale lease rejection. The artifact-store tests own same-input
reuse and isolation of changed input. Driver tests prove
state classification and the single-submission guard. MinIO and live Daytona
are not dependencies of these focused tests.

## Lifecycle

Sessions are admitted without creating a provider resource. The first approved
sandbox-backed Tool Use records an execution row and a Queue job. The Sandbox
worker performs a fresh provider inspection and normalizes the result. That
first tool may include provider inspection and activation latency, or return a
typed tool failure when the artifact or provider resource cannot be made usable:

```text
ready      -> authorize the execution against the current binding
stopped    -> join or create sandbox_activate, then re-enter execution
archived   -> join or create sandbox_activate, then re-enter execution
not_found  -> join or create sandbox_activate, then re-enter execution
transition -> retry the same durable operation without a provider side effect
```

Activation is single-flight per logical Sandbox. Concurrent executions attach
to the same unfinished operation. Completion records the provider handle and
re-enqueues refs-only execution jobs; each released execution inspects the
provider again before authorization.
Sandbox activation and lifecycle transactions lock the Environment row while
selecting or validating its generation. The Sandbox database role therefore
needs `environments UPDATE` for `SELECT ... FOR UPDATE`, even though API remains
the owner of Environment configuration changes. Granting the row-lock capability
does not change that business ownership.
If a lifecycle notification is redelivered after a release fence but no
provider submission was recorded, the worker abandons that operation without a
provider call. Once a submission boundary exists, recovery continues by
observation instead.

Release uses a separate existence inspection: only a provider `not_found`
response proves absence, while every successful Get is a present handle that
must receive provider Release regardless of its execution state. Provider-side
outcomes commit only while their durable operation lease is still current.
Environment artifact rows carry the current build job, lease token, and attempt
number for business re-entry, while the live Queue row remains the final
authority for every artifact and fanout mutation.

Provider adoption resolves the logical Sandbox name against exactly the stable
workspace, Session, Environment, Sandbox, and lifecycle-owner labels. Durable
lifecycle operation IDs correlate Queue work and logs; they are not provider
resource ownership, so a later operation can adopt the same stable resource.

Resource materialization is a separate single-flight gate. It converges the
binding's Environment generation, Session resource revision, bounded resource
credential, helper health, and resource-root receipt. A resource revision that
changes during materialization creates a successor operation; waiters are
released only by the operation matching the current revision.

Network policy is applied once, when the provider Sandbox is created for the
binding's Environment generation. Materialization does not reapply it, and an
Environment update does not mutate a running Sandbox; the update takes effect
when a later Sandbox is created from the new generation.

Tool execution follows this durable state machine:

```text
pending
  -> waiting_activation -> pending
  -> waiting_materialization -> pending
  -> preparing -> running -> terminal_unconsumed -> consumed
```

`terminal_unconsumed` is durable staging, not conversation history. The Bridge
turns it into a conversation Tool Result only when Runtime commits that event.
If another terminal writer wins first, the staged body is cleared and only its
digest and settlement receipt remain, so a late provider result cannot rewrite
the terminal conversation.

`preparing` stages a provider command under a persisted deadline but does not
run the user-authored command. The transition to `running` rechecks the current
binding revision, exact provider handle, release and cancellation fences,
materialized revisions, credential lifetime, helper receipt, and database
clock. Only after that transaction commits may the adapter submit the command.
A worker that recovers a `running` row observes the durable provider reference;
it never blindly submits the command again. Foreground observations are spaced
by a cancellation-aware 500 ms wait so recovery cannot spin against the
provider API.

Memory projection stays bound to the exact attached writable store named by
its durable job. Detaching that store terminalizes the projection even when a
different writable store is attached later; projection work never crosses
store identity.

Background settlement checks the shared child-close fence before publishing a
task notification. `session_threads` and `session_events` require `SELECT` and
`UPDATE` because the fence takes `FOR SHARE` locks; Sandbox does not become an
author of their business columns. A committed close input with no terminal source
Tool Result also requires reading `session_bridge_operations` for the close
receipt. A closing or closed target receives a parked notification without a
runnable notification job; task result and notification commit atomically.

## Provider Adapter

`ProviderAdapter` is the complete provider boundary. The composition root
constructs exactly one `DaytonaAdapter`, registers it under `daytona`, and gives
the registry to every Sandbox runner. The same adapter instance supplies:

- provider inspection, activation, and release;
- Environment artifact building;
- resource materialization and credential minting;
- helper-backed tool execution and background-command control;
- output capture and memory projection.

Provider-native states, SDK request types, command identifiers, mount
mechanics, and helper transport remain behind that adapter. Queue payloads,
Bridge, Runtime, and binding rows use only Tetral identities and normalized
outcomes.

Create-time Daytona disk, CPU, and memory capacity rejections are one provider
failure family. Classification uses only the first logical line of the SDK's
structured validation message and requires the anchored family grammar plus
the resource's exact limit shape. It remains a proved-not-started operation and
therefore consumes the existing activation Queue budget; other validation
responses and non-Create stages retain terminal `invalid_request` semantics.
Provider completion logs record the normalized `quota_exceeded` category and
fixed safe message, never the resource, response text, or capacity value.

Tool preparation has separate filesystem diagnostics. With the pinned Daytona
SDK, directory creation returns a `message` while streaming upload returns a
bulk-upload `errors` array. The driver recognizes Linux filesystem errors only
for the Engine-owned payload path and these specific operations; HTTP 400 alone
does not imply exhausted storage. Known no-space, permission, and read-only
failures become distinct English Tool Result messages. These SDK preparation
failures state that the tool operation was not started. A failure after helper
submission retains unknown-outcome semantics and does not make that claim.
Unknown messages keep a generic caller-facing failure. This path uses the
existing Tool Result delivery and does not create a separate Session error
event. See the pinned
[directory handler](https://github.com/daytonaio/daytona/blob/8c07569d1f4f88c4b84a8859c905da8b9cb7573f/apps/daemon/pkg/toolbox/fs/create_folder.go)
and [bulk-upload handler](https://github.com/daytonaio/daytona/blob/8c07569d1f4f88c4b84a8859c905da8b9cb7573f/apps/daemon/pkg/toolbox/fs/upload_files.go)
for the upstream response shapes.

New package snapshots allocate 4 vCPU, 8 GiB memory, and 10 GiB disk. Existing
snapshot lookup identities stay stable so in-flight builds can adopt a
previously-created snapshot, including its previous allocation. Prebuilt default
snapshots must be registered with the same allocation during release preparation; see
[bootstrap](../../docs/bootstrap.md#6-register-the-sandbox-snapshot-with-daytona)
for registration and existing Environment compatibility.
Each 4/8/10 allocation consumes the organization's regional quota and reduces
concurrent Sandbox capacity; verify actual limits and remaining headroom before
rollout. Exhaustion continues through the existing `quota_exceeded` activation
path, while Session admission remains independent of Sandbox allocation.

The Daytona adapter owns the R2/rclone/FUSE path and the Daytona Linux Helper.
File resources are copied into a Session-scoped Blob prefix, mounted read-only,
bound to their declared paths, and verified as the runtime user. GitHub
repositories, memory stores, skills, credentials, and helper health are
converged in the same materialization operation. There is no alternate mount
path or provider fallback.

GitHub repository checkout carries the mount's declared commit identity from
the durable materialization snapshot. After a fresh clone — and again whenever
the already-admitted origin is recognized during recovery or
rematerialization — the checkout installs a declared `git_identity` as
repository-local `user.name`/`user.email`, so disposable Sandbox recreation
reasserts it and one Session can mount repositories with different identities.
The driver rejects malformed identity snapshots before running a command,
including values Git would trim or remove from commit headers; admitted values
are installed unchanged as the default author and committer identity.
A mount without a declared identity keeps the session-scoped platform identity
from the Sandbox-global Git configuration, which per-repository configuration
never rewrites.

## Release

Release is a durable lifecycle operation. Its only producers are Session
deletion and displacement of a recorded provider handle. API and Bridge may
declare Session-deletion release through the provider-neutral internal release
boundary; only Sandbox Service inspects or mutates the provider resource.
Runtime Pod loss and ordinary idle cleanup do not release a Sandbox.

The release fence prevents new execution authorization and activation work.
The release worker waits for executions, lifecycle operations, and background
commands targeting the handle to become terminal. It then inspects the exact
handle and performs the provider release. A transport loss after submission is
observation-only; it never causes a blind second release call.

When a release is blocked by unfinished work, one transaction acknowledges the
current Queue lease and clears the operation's Queue identity without spending
a release attempt. The transaction that settles the final blocker creates the
next release job, including successful activation/materialization completion
and execution reinspection back to pending. Provider failures use the finite
Queue attempt budget; a pre-submission background-cancel exhaustion creates one
backoff-delayed successor rather than a hot loop. An
exhausted Session-deletion release becomes a named dead letter and preserves
the binding, operation, and Blob pointers for operator inspection.

After release is complete and all Sandbox Queue jobs for a deleted Session are
closed, cleanup drains private Blob prefixes and removes private lifecycle and
execution rows. Public Session history and Tool Results retain their ordinary
custody rules.

## Interruption And Results

Transport cancellation does not cancel durable Sandbox work. Only an accepted
user interruption records business cancellation. Pending or waiting work can
settle without a provider call; preparing work waits for the pre-submission
deadline fence; running work uses the adapter's cancellation capability and
still preserves an unknown outcome when the provider cannot prove a result.

Provider results are normalized and stored before Runtime receives a refs-only
delivery. Media bytes are staged in Blob storage and become public attachments
only in Bridge's Tool Result commit transaction. Execution receipts do not
retain a second permanent copy of the result body after that commit.

Activation-budget exhaustion is private lifecycle state. Its Sandbox
settlement and Bridge wait response carry the internal exhaustion kind with the
fixed message `sandbox activation could not be completed`; Runtime maps every
Sandbox Tool family to one provider-neutral error text block containing exactly
`The requested operation could not be completed.` Provider capacity diagnosis,
Sandbox architecture, route envelopes, partial output, attempt counts, and
Runtime error codes are not projected into public Tool Results.

## Configuration

The service requires PostgreSQL, Queue, a Daytona Tier 3 or higher account,
Blob/R2, and git-proxy configuration. `TETRAL_SANDBOX_WORKER_CONCURRENCY` is
one process-wide slot budget shared by the Sandbox business Queue runners;
each occupied slot leases at most one job. Environment build/fanout and
maintenance loops retain their separate named limits. Queue lease duration,
provider command timeout, late-command margin, credential lifetime, and
artifact construction are also service-owned settings. The process listens
only on `TETRAL_SANDBOX_HTTP_ADDR` for health and metrics.

## Queue and provider diagnostics

A committed Queue insert emits a PostgreSQL notification containing only the
consumer class. Each Bridge or Sandbox process broadcasts that hint to its
local polling loops; they still call Queue `Lease`, which remains the sole
assignment authority. A disconnected listener reconnects and triggers a
catch-up poll, while the existing timer polling remains the fallback.

Execution results have a dedicated wake channel,
`tetral_sandbox_execution_result`. Both production writers that transition an
execution to `terminal_unconsumed` emit a refs-only hint — workspace, Session,
Thread, and Tool Use event IDs, never a result body — inside the settling
transaction: ordinary settlement in `execution_store.go`
(`settleSandboxExecutionTx`, reached by every settlement path including
failure, cancellation, and unknown outcomes) and Session-deletion waiter
settlement in `internal/sandbox/release` (`settleWaitersTx`). A stale
generation or replayed settlement affects no row and emits nothing; rollback
publishes neither the result nor the hint. Bridge API waiters treat the hint
only as a wake signal and re-verify the durable row; a one-second fallback in
the wait covers missed or coalesced hints and listener downtime.

Three retention/maintenance loops are deliberately poll-only because their
latency is not user-facing: over-limit Queue reconciliation, the expired
output-capture sweep, and resource-prefix garbage collection. All business
Queue runners, including environment build/fanout, tool execution,
activation/materialization/release, cancellation, background commands,
memory projection, and output capture/cleanup, receive the shared wake hint.

`queue.job.leased` records `queue.job.id`, `queue.job.kind`, the partition, and
`duration.ms` for the database Lease call and `queue.ready_wait.ms` from a
job becoming available to being leased. Sandbox provider completion lines
use `sandbox.provider.operation_completed` with `operation`, `outcome`,
`duration.ms`, available workspace/Session/thread/operation/provider IDs, and a
normalized error class and code. Activation lifecycle lines separately record
stable-name resolution, final Queue-authority loss, and the durable outcome,
normalized error code, and Queue attempt N/M of each current attempt.
Materialization arm operations identify helper health,
base directories, credential mint, file staging, mount/bind verification,
skills, memory projection, and repository checkout separately.

Failed workspace consumer cycles emit one `sandbox.queue.consume.failed` warning
per cycle, including failures before a job is leased. It records the operation,
Sandbox component, a fixed error class (`database_permission_denied` for SQLSTATE
`42501`), and the next retry delay. It omits raw error text and database
diagnostics. Existing polling backoff and wake behavior govern retries; normal
shutdown cancellation emits no failure warning.

Tool SDK failures additionally carry `provider.operation` (the failed SDK
sub-operation) and `provider.error_detail` in private provider completion logs.
Details replace known payload paths, pass the existing secret/internal-path
checks, and are bounded; unsafe details are explicitly redacted. These
diagnostics are not persisted in the public Tool Result.

Provider failure messages pass the provider-message validator and are bounded.
Credentials, headers, request bodies,
tool JSON, commands, mount URLs, tokens, and raw stacks are omitted. Startup
failure categories distinguish configuration, schema, listener, dependency
readiness, and unknown failures. `TETRAL_SANDBOX_DEBUG_LOGGING=true` enables
Sandbox-only debug diagnostics; info-level defaults and completion logs are
unchanged.

## Testing

Focused tests live in `services/sandbox`, `internal/sandbox`, and
`services/sandbox/internal/resourceprojection`. Database-backed lifecycle and
execution tests require `TETRAL_TEST_DATABASE_URL`. The Kubernetes and Helm
packages verify that the canonical, service-local, and rendered deployment
surfaces stay aligned.

`TestSandboxLifecycleRunnersDeliverGitIdentityFromDurableResources` starts with
persisted resources and drives activation and materialization through real
Queue RPCs, PostgreSQL stores and `RunOnce`. It checks declared and omitted
identities at the provider adapter boundary; provider responses are fixtures,
so this does not prove remote Git execution. Driver tests separately execute
clone/configuration commands and inspect actual local Git commits. Admission
and driver snapshot checks use the shared `internal/gitidentity` rules.

`TestDaytonaToolPreparationPreservesFilesystemFailureThroughSDK` exercises the
pinned SDK against a local HTTP server using Daytona's directory and bulk-upload
error formats. It checks the real helper preparation, adapter, and completion
logger, including English messages, no tool submission, and diagnostic redaction.

Driver tests additionally check payload-path scope and the four upstream
bulk-upload error wrappers.
A nonzero exit from the payload permission script still uses the existing
generic retryable preparation error, with no script-output diagnostic; SDK
request failures are the scope of this mapping.

Repository CI builds the unmodified Sandbox Dockerfile and exercises the local
image and Helper without Daytona credentials. Published-image Daytona behavior
belongs to the separately operated release rehearsal, which records the exact
candidate image and Chart digests before promotion.

## Boundaries

- No public SDK shape selects a Sandbox provider in this release.
- Session create does not allocate, inspect, or materialize a Sandbox.
- Runtime and Bridge do not contain provider lifecycle or helper execution.
- Queue does not own Sandbox business state.
- The alpha provider registry contains only `daytona`.
