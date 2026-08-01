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

Every kind has an explicit positive attempt budget. Queue attempt counts govern
transport only; lifecycle-operation and execution generations govern business
re-entry. A worker that receives an over-budget job settles the referenced
business row before dead-lettering the Queue row.

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

Release uses a separate existence inspection: only a provider `not_found`
response proves absence, while every successful Get is a present handle that
must receive provider Release regardless of its execution state. Provider-side
outcomes commit only while their durable operation lease is still current.
Environment artifact rows carry the current build job, lease token, and attempt
number so a successor claim fences every stale authorization and outcome write
without locking a Queue row.

Resource materialization is a separate single-flight gate. It converges the
binding's Environment generation, Session resource revision, bounded resource
credential, helper health, and resource-root receipt. A resource revision that
changes during materialization creates a successor operation; waiters are
released only by the operation matching the current revision.

Tool execution follows this durable state machine:

```text
pending
  -> waiting_activation -> pending
  -> waiting_materialization -> pending
  -> preparing -> running -> terminal_unconsumed -> consumed
```

`preparing` stages a provider command under a persisted deadline but does not
run the user-authored command. The transition to `running` rechecks the current
binding revision, exact provider handle, release and cancellation fences,
materialized revisions, credential lifetime, helper receipt, and database
clock. Only after that transaction commits may the adapter submit the command.
A worker that recovers a `running` row observes the durable provider reference;
it never blindly submits the command again.

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

The Daytona adapter owns the R2/rclone/FUSE path and the Daytona Linux Helper.
File resources are copied into a Session-scoped Blob prefix, mounted read-only,
bound to their declared paths, and verified as the runtime user. GitHub
repositories, memory stores, skills, credentials, and helper health are
converged in the same materialization operation. There is no alternate mount
path or provider fallback.

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
next release job. Provider failures use the finite Queue attempt budget; an
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

## Configuration

The service requires PostgreSQL, Queue, Daytona, Blob/R2, and git-proxy
configuration. `TETRAL_SANDBOX_WORKER_CONCURRENCY` is one process-wide slot
budget shared by the Sandbox business Queue runners; each occupied slot leases
at most one job. Environment build/fanout and maintenance loops retain their
separate named limits. Queue lease duration, provider command timeout,
late-command margin, credential lifetime, and artifact construction are also
service-owned settings. The process listens only on
`TETRAL_SANDBOX_HTTP_ADDR` for health and metrics.

## Testing

Focused tests live in `services/sandbox`, `internal/sandbox`, and
`services/sandbox/internal/resourceprojection`. Database-backed lifecycle and
execution tests require `TETRAL_TEST_DATABASE_URL`. The Kubernetes and Helm
packages verify that the canonical, service-local, and rendered deployment
surfaces stay aligned.

## Boundaries

- No public SDK shape selects a Sandbox provider in this release.
- Session create does not allocate, inspect, or materialize a Sandbox.
- Runtime and Bridge do not contain provider lifecycle or helper execution.
- Queue does not own Sandbox business state.
- The alpha provider registry contains only `daytona`.
