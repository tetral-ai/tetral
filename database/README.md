# PostgreSQL contracts

This directory is the language-neutral owner of Tetral's PostgreSQL security
contracts.

- `postgresql.json` enumerates the live Version 1 Workspace-RLS surface used by
  Go and TypeScript readiness checks. Workspace is the sole database tenant
  dimension; Session and Thread isolation remains explicit relational and
  lifecycle ownership inside a Workspace. Ordinary Workspace tables get one
  read/write isolation policy. The append-only Workspace tables
  (`session_file_attachment_consumptions` and `session_message_parts`) get only
  a Workspace SELECT policy and a Workspace INSERT policy, so no serving role
  can update or delete their rows even where a grant would allow it.
- `roles.json` declares the exact table and sequence privileges, and the
  allowlisted SECURITY DEFINER function grants, for each serving workload. The
  allowlist admits the runtime process lock for Bridge and Job Runner; the
  runtime process liveness lock and the binding-discovery functions
  `tetral_job_runner_binding_upper()` and
  `tetral_job_runner_binding_page(text, text, text, text, integer)` for Job
  Runner only; for Auth only, `tetral_auth_lookup_key`,
  `tetral_auth_lookup_token`, `tetral_auth_lookup_grants`,
  `tetral_auth_lock_authority` and `tetral_auth_prune_tokens`; for Cleanup
  only, `tetral_cleanup_due_sessions(timestamptz, timestamptz, text, integer)`,
  `tetral_prune_event_idempotency` and `tetral_prune_event_changes`; and, for
  Queue only, `tetral_prune_job_runner_jobs`.
  Operator-selected role names and credentials are inputs to the installer and
  never belong in this repository.
- `ApplyRoleContract` owns role attributes, public-privilege revocation, schema
  ownership, and explicit serving grants. It repairs only this declared role
  boundary; application startup verifies schema and role posture but does not
  repair either.

Run `tetral-db-prepare` before a fresh installation. It initializes the current
canonical schema in an empty database, then installs the exact role contract.
Repeating preparation for the exact current version/checksum verifies the schema
and preserves its identity stamp. An unexpected object without a schema registry,
an empty registry, a predecessor checksum or another version is rejected before
schema mutation. The emptiness check covers all objects dependent on the active
initialization namespace, including standalone types and collations, without
changing unrelated namespaces. There is no predecessor upgrade or compatibility
fallback.

The administrative connection must authenticate as a PostgreSQL superuser
(`rolsuper=true`); `CREATEROLE` or migration-role membership is insufficient.
Role declarations are validated before schema or role changes. The canonical
schema includes Git identities, durable MCP discovery budgets and Environment
build observation directly in its initial tables.

`internal/schemaidentity` owns the sole current version/checksum without database
or driver dependencies. Storage binds it to the exact ordered initial DDL payload.
Release metadata and Go/Bun readiness consume that same current identity.
Initialization serializes on the schema advisory lock and commits DDL plus its
stamp in one transaction. Failed DDL rolls back; an unacknowledged commit remains
unknown until an independent connection observes the identity.

Runtime process custody has three non-tenant global tables, with no Workspace
column or RLS policy: `runtime_process_pods` owns registration allocation and
the last promoted order, `runtime_processes` retains candidate/current/retired
boot identities and lifecycle timestamps, and `runtime_process_liveness` holds
one row per process with its last report time (NULL until the first report,
kept after retirement). Bridge alone can register, report and promote, and is
the only writer of liveness. Bridge and the separate `job_runner` role, which
only reads these facts, can execute the fixed lock-only
`public.tetral_lock_runtime_process(text,text,text)` function; only
`job_runner` can execute `public.tetral_lock_runtime_process_liveness(text,text,text)`,
which its final loss classification uses. Both migration-owned
security-definer bodies use a fixed search path and qualified tables; each holds
`FOR SHARE` through the calling transaction without granting mutation. Public
execution is revoked at creation and installation. Other serving roles cannot
execute them or use them to acquire Workspace authority.

Cleanup scheduling has one non-tenant table, `cleanup_schedule_cursor`: a
singleton row, inserted by initialization, holding the elected scheduler's
owner generation and its position in the current discovery cycle. Cleanup can
only SELECT and UPDATE it; no serving role can insert or delete it, and no other
role can read it. Cleanup's cross-Workspace scheduling read is the
migration-owned security-definer
`public.tetral_cleanup_due_sessions(timestamptz,timestamptz,text,integer)`,
which has a fixed search path, qualified objects and revoked public execution.
It sets `tetral.cleanup_discovery`, and the SELECT-only `cleanup_discovery`
policy on `session_runtime_status` admits a row only when that flag is set and
`current_user` is the table owner, so a serving role that sets the flag itself
still sees only its own Workspace. The function returns Workspace, Session and
due time, never Session content.

Retention uses the same pattern with the flag `tetral.retention_maintenance`.
Cleanup alone executes
`public.tetral_prune_event_idempotency(timestamptz,timestamptz,text,text,bytea,integer)`
and `public.tetral_prune_event_changes(timestamptz,timestamptz,text,text,bigint,integer)`;
Queue alone executes `public.tetral_prune_job_runner_jobs(text,timestamptz,integer)`.
Each is owned by the migration role, has `search_path = pg_catalog`, qualified
objects and revoked public execution, and takes only a cutoff it caps at its own
database clock minus the fixed age, a continuation key or terminal state, and a
limit it clamps to 0..256; none accepts a table, role, duration or SQL text.
Each returns only counts, the last examined key (Cleanup) or a bounded
`NULL`-timestamp count (Queue), and `more_remaining` from one indexed
`LIMIT 1` probe for another eligible row, never content. The
owner-checked `retention_maintenance_select`, `_insert`, `_update` and
`_delete` policies require the flag and `current_user` equal to the table owner
and grant exactly: SELECT/UPDATE/DELETE on `session_event_idempotency_keys`,
`session_event_stream_changes` and `queue_jobs` (UPDATE is needed for
`FOR UPDATE`); SELECT/INSERT/UPDATE on `session_event_feed_retention`; SELECT on
`session_events` and `session_threads` for feed eligibility, reading only scope,
visibility and type or role columns. Serving roles setting the flag gain nothing,
and Cleanup has no direct grant on receipts, changes or feed metadata.

`session_event_feed_retention` is a Workspace-RLS table holding, per Session
feed (`feed_key = 'session'`) and per Thread feed (`'thread:' || session_thread_id`),
the highest pruned stream position that feed's reader would have returned. Its
CHECK ties the key to the nullable Thread column, and its composite foreign keys
cascade only from a future Session or Thread purge, so watermarks outlive the
change rows they summarize. Event Stream may only SELECT it; only the change
retention function writes it. Its `(workspace_id, session_id, pruned_through
DESC)` index serves Event Stream's shared idle check, which reads a Session's
highest watermark across its feeds.

`session_runtime_handoffs` and `session_runtime_handoff_threads` are internal
Workspace-RLS receipts retained through Session lifetime. They preserve exact
old binding/process release identity, immutable all-Thread continuation decisions
and Queue-job identities; Session deletion cascades them. They are not public
events. Ordinary receipt replay still requires the unchanged exact live binding;
release replay uses separately authenticated old-owner proof.

Runtime services use only their serving DSNs. No serving process, including API,
receives an administrative or migration-owner DSN or runs schema/role repair.
The migration role remains the owner of schema objects; it is not an API login.
The executable entrypoint lives in `cmd/tetral-db-prepare`; schema implementation
stays in `internal/storage`, and role implementation stays in this directory.
The separate `cmd/tetral-bootstrap` command seeds the initial workspace using
the API serving role. Auth still owns startup refresh of its bootstrap API key.

The preparation command validates all role declarations before touching the
database, then initializes and applies grants in that order. It exits zero only
when both stages succeed. These are separate transactions: a role-installation
failure does not undo an already committed initialization. Stop the release on a
nonzero exit, correct the reported stage and rerun the same revision with the
same declarations. Serialize database preparation and workload rollout; the
schema and role locks do not serialize the entire deployment.

A workload revision must match the prepared schema identity. Preparation does
not stop workloads, perform an upgrade, or change Kubernetes resources.

### Migration diagnostics

The migrator emits `schema.migration.started` and `schema.migration.completed`
for the initial schema, or `schema.migration.failed` when migration fails.
Records include `operation=database.migrate`, `schema.version` when known,
`schema.step`, `duration_ms`, and `transaction.outcome`. Outcomes describe the
initialization transaction, not the complete deployment:

- `not_started`: no transaction was established for this attempt.
- `committed`: `Commit` acknowledged success. A later lock-release failure does
  not undo it.
- `rolled_back`: an explicit rollback before any commit attempt succeeded.
- `unknown`: no acknowledgement establishes the transaction result, including a
  failed `Commit` or an unconfirmed rollback. Reconnect and check history before
  deciding whether to retry; never infer rollback from a connection error.

Failure records contain a constant safe message and classification, plus
`db.sqlstate` when the PostgreSQL driver supplies a valid code. Raw driver
messages, SQL, parameters, connection strings and error details are not logged.
An exact-current database produces no per-version migration records.

The preparation command emits JSON on stderr with `service.name=db-prepare`.
`database.prepare.started`, `.completed`, and `.failed` describe the whole
command; failures identify `step` and `error.message_safe` without serializing
input or raw errors. Safe reasons distinguish invalid input, insufficient
administrative privileges, and role conflicts; unclassified driver failures use
a generic message for the failed stage.
Detailed `schema.migration.failed` records precede the command failure summary.
A role-stage failure may follow successfully committed initialization records.
Container stderr can be collected by the deployment's log agent; an arbitrary
local or SSH command needs its own log collection. This code does not send
requests to Loki, alter PostgreSQL server logging, perform backups or downgrade
a committed schema.

Tests use a different capability model. `internal/storage/storagetest` creates
one immutable schema template per exact current schema identity, then gives
every native test a private cloned database and unique NOBYPASSRLS login. Those broad
test-only grants never define production privileges. Production authorization
tests use `storagetest.OpenWorkloadDB`: it applies the real installer contract
to a private clone and authenticates as the selected workload's unique login.
`OpenWorkload` opens another declared workload's login from the same
installation, so a cross-owner composition such as the separated Bridge and
Job Runner suites runs each owner with only its production grants.
The administrative connection only seeds fixtures, injects missing privileges,
and inspects results. `RequirePrivilege` revokes one privilege, requires the
owning operation to fail with SQLSTATE `42501`, and restores the contract through
the installer; the test then asserts the successful durable outcome.
Workload roles are reserved as NOLOGIN roles together with their names, OIDs and
installer authority comments in the control registry before installation. Normal
cleanup and expired-run recovery verify that identity, reclaim the private
database, and remove all registered roles; a changed role identity fails closed.

The supported database and test baseline is PostgreSQL 18, including when a
focused test receives an administrative `TETRAL_TEST_DATABASE_URL`. That override
selects the server, not an older-version compatibility profile. The catalog
privilege check includes `MAINTAIN`, which is unavailable before PostgreSQL 17.

## Serving privilege validation

Review privileges at the production caller, including shared stores and helpers.
Reads include every joined table. Row locks can require `UPDATE` even without
changing a column; `INSERT ... RETURNING` and `ON CONFLICT` can require reads and
updates as well. Table-owned sequences receive `USAGE` with `INSERT`; independent
sequences must be declared explicitly. A package or table name does not establish
which serving role executes it.

These owner tests pin the serving paths repaired after role restriction:

| Role | Production boundary | Required grants and evidence owner |
|------|---------------------|------------------------------------|
| sandbox | First activation and lifecycle claims | `environments UPDATE`; `services/sandbox/execution_store_test.go` retains concurrent single-activation custody |
| sandbox | Lifecycle resource snapshot | `session_file_resources`, `session_github_repository_resources`, `agent_versions`, `skill_versions SELECT`; `services/sandbox/lifecycle_store_test.go` uses the real resource reader through activation and materialization |
| sandbox | Git repository preparation and recovery | `session_git_tickets SELECT/INSERT/UPDATE`; `internal/sandbox/github_preparation_postgresql_test.go` proves live ticket before clone and pending-ticket recovery |
| sandbox | Media publication | `session_transient_attachments INSERT`; `services/sandbox/tool_media_test.go` checks staged Blob bytes and recovery |
| sandbox | Background task/command settlement and child-close fence | `session_threads SELECT/UPDATE`, `session_events SELECT/UPDATE`, `session_bridge_operations SELECT`; `services/sandbox/background_command_store_test.go` uses the Sandbox role for all store cases; `background_settlement_role_test.go` checks committed control without a close receipt, atomic parking and replay |
| bridge / job_runner | Process custody promotion, liveness reports and Session process fencing | Bridge alone writes registry/arbitration and `runtime_process_liveness SELECT/INSERT/UPDATE`; Runner reads the registry and liveness tables and executes the two lock-only functions; `services/bridge/runtime_process_test.go`, `services/job-runner/runtime_visibility_test.go` and installer role tests check locks, search-path shadowing and denied mutation |
| bridge | Placement and Session cleanup are Job Runner-owned | no binding-generation sequence, no `session_runtime_bindings INSERT`, no Session-cleanup `DELETE` and no lifecycle-operation `INSERT`; Bridge keeps binding `UPDATE/DELETE` for release and termination; `database/apply_roles_postgresql_test.go` requires SQLSTATE `42501` for each removed statement |
| provider_gateway / mcp_connector | Separate Gateway workload credentials | Provider Gateway reads `session_provider_auth` and `platform_provider_keys` and rotates `credentials`; MCP Connector reads Session vault references and reads/rotates its `credentials` with no provider binding or platform-key access; `database/apply_roles_postgresql_test.go` runs Bun and command readiness under both roles and requires SQLSTATE `42501` for MCP Connector's provider credential reads |
| bridge | Durable Memory mutation | `memory_stores UPDATE`; `services/bridge/bridge_api_tools_test.go` checks committed content and idempotent replay |
| bridge | Compaction Request End | `session_thread_context_prefixes UPDATE`; `services/bridge/runtime_compaction_role_test.go` checks main-thread checkpoint, child-prefix consumption, rollback and replay |
| bridge / job_runner | Assistant part append | `session_message_parts SELECT/INSERT` only, with the existing `session_messages UPDATE` for the header counters; Bridge alone inserts messages. `internal/runtimecontrol/message_parts_test.go` appends through the shared primitive under both roles and requires SQLSTATE `42501` for part UPDATE/DELETE and a foreign-workspace INSERT, and `23503` for a part outside its message's Thread or under an embedded message |
| api / bridge / job_runner | First event revision and processed revision | `session_event_stream_changes INSERT` with the derived `USAGE` on its identity sequence, used by `nextval` and an explicit `OVERRIDING SYSTEM VALUE` position; `internal/sessioneventwrite/sessioneventwrite_test.go` writes the first revision under all three roles and the processed revision under Bridge and Job Runner |
| bridge | Runtime context skill index | `skill_versions SELECT`; `services/bridge/bridge_api_context_test.go` checks configured version metadata |
| api | Session deletion through shared Sandbox release | `session_runtime_tool_results SELECT/INSERT/UPDATE`, `session_background_tasks SELECT/UPDATE`; `internal/session/postgresql_store_controlplane_test.go` checks atomic release and background cancellation custody |
| api | Child tool-confirmation admission during close | `session_bridge_operations SELECT`; `internal/sessionevent/closing_role_test.go` checks missing-grant failure, intended conflict with no receipt, and admission/replay after the source Tool Result is terminal; the event-store suite uses the API role |
| job_runner | Pod-loss repair discovery across Workspaces | `EXECUTE` on the two binding-discovery functions only, with no `workspaces` grant and no direct cross-Workspace `SELECT`; the migration-owned functions set `tetral.runner_discovery`, and the `runner_discovery` SELECT policies on `session_runtime_bindings`, `session_runtime_status`, `sessions` and `session_runtime_inbox` also require the actual table owner, so a caller that sets the flag itself still sees only its Workspace; `services/job-runner/runtime_binding_discovery_test.go` uses the installed role for paging, argument rejection, flag spoofing, other workloads, search-path shadowing and ownership |
| job_runner | Lost Runtime closes an open model request | `request_usage_details SELECT/INSERT` (explicit `ON CONFLICT` key target and audit append); `services/job-runner/runtime_pod_loss_role_test.go` uses the installed role for the fenced closeout, verifies one matching zero-token terminal audit, unchanged replay, and whole repair rollback when either privilege is individually revoked |
| cleanup | Receipt and change retention | `tetral_prune_event_idempotency` and `tetral_prune_event_changes` EXECUTE only, no table grant on receipts, changes or feed metadata; `services/cleanup/retention_test.go` runs the installed role through both functions and checks denied direct access, other roles' denied execution, spoofed flags, search-path shadowing and non-owner definers |
| queue | Job Runner terminal retention | `tetral_prune_job_runner_jobs` EXECUTE beside its existing `queue_jobs` grants; `internal/queue/job_runner_retention_test.go` runs the installed role and checks the same denials |
| event_stream | Feed head, retention gap and shared idle signal | `session_event_feed_retention SELECT`; `internal/eventstream/retention_test.go` reads heads and gaps and `internal/eventstream/session_signals_test.go` reads Session signals through the installed role |
| job_runner | Session deletion retires output-capture custody after Sandbox release | `sandbox_output_capture_operations SELECT/UPDATE/DELETE` (row lock, cleanup scheduling, terminal retirement), `sandbox_output_capture_blobs SELECT/DELETE` (scoped deletion), with no capture `INSERT`; `services/job-runner/runtime_output_capture_role_test.go` uses the installed role, preserves pending Queue work and foreign custody, and checks rollback when required grants are revoked |
| cleanup | Global due-Session scheduling | `tetral_cleanup_due_sessions` EXECUTE, `cleanup_schedule_cursor SELECT/UPDATE`, `session_runtime_status SELECT/UPDATE` and Queue admission, with no `workspaces` read; `services/cleanup/scheduler_test.go` runs the installed role through discovery, claims and fenced cursor writes, and `discovery_boundary_test.go` checks its denied direct reads and cursor `INSERT`/`DELETE` and every other role's denied execution |

The installer test independently inspects all live public tables, sequences and declared function capabilities,
including undeclared ones, and verifies that reapplication repairs missing grants
and removes overgrants. This establishes the upper permission bound; owner tests
establish that intended operations remain possible. Declaration equality or
empty-table SQL alone cannot establish the latter.

Owner suites must cover the states that change their SQL dependencies, including
shared helpers: open, closed, and committed-control-without-receipt are distinct
background settlement paths. Single-grant revocations prove sensitivity only for
the paths executed. A green grant-equality check cannot establish that the grant
manifest itself is complete. Broad test roles remain useful for shared-store
behavior and RLS tests, but do not establish serving authorization.

Authorization changes must also inspect other callers: API control-plane stores,
Bridge Runtime APIs, Job Runner placement/delivery/recovery and Session cleanup,
Sandbox lifecycle/execution/projection and
background command settlement, API event admission's child-close fence, Queue
lease and maintenance, Auth key management, Cleanup admission, Provider Gateway and MCP
Connector credential resolution, Git Proxy ticket/credential reads, and Event Stream's dedicated
read-only store. Agent Runtime has no database role and persists through Bridge.
This inventory guides review; the tests above do not claim dynamic coverage of
every branch of every workload.
