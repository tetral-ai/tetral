# PostgreSQL contracts

This directory is the language-neutral owner of Tetral's PostgreSQL security
contracts.

- `postgresql.json` enumerates the live Version 1 Workspace-RLS surface used by
  Go and TypeScript readiness checks. Workspace is the sole database tenant
  dimension; Session and Thread isolation remains explicit relational and
  lifecycle ownership inside a Workspace.
- `roles.json` declares the exact table and sequence privileges for each
  serving workload. Operator-selected role names and credentials are inputs to
  the installer and never belong in this repository.
- `ApplyRoleContract` owns role attributes, public-privilege revocation, schema
  ownership, and explicit serving grants. It repairs only this declared role
  boundary; application startup verifies schema and role posture but does not
  repair either.

Run `tetral-db-prepare` before a fresh installation and before starting
updated workloads whenever the schema or role contract changes. The command
applies pending schema migrations with the administrative connection, then
applies the role contract. Repeating the command preserves applied migrations.

The deployed Alpha 1 schema is immutable V1. V2 adds nullable
`git_identity_name` / `git_identity_email` columns and their paired-value
constraint to `session_github_repository_resources`. Existing repository data
and V1 history remain unchanged; NULL identities retain the default Git
identity. Fresh databases apply V1 then V2; existing V1 databases apply only V2.
The V2 DDL and its history entry commit in one transaction, so a failed V2 can be
retried without partial columns. Go and Gateway readiness require both versions.
Do not edit stored checksums to bypass a mismatch: a database created from a
rewritten V1 is not the deployed Alpha 1 baseline and is rejected as drift.

This is a forward migration, with no automatic downgrade. An older binary
rejects the V2 schema as ahead; upgrading the schema therefore also changes the
rollback requirements. Preserve a database backup before an upgrade that may
need to return to an older binary.

Runtime services use only their serving DSNs. No serving process, including API,
receives an administrative or migration-owner DSN or runs schema/role repair.
The migration role remains the owner of schema objects; it is not an API login.
The executable entrypoint lives in `cmd/tetral-db-prepare`; schema implementation
stays in `internal/storage`, and role implementation stays in this directory.
The separate `cmd/tetral-bootstrap` command seeds the initial workspace using
the API serving role. Auth still owns startup refresh of its bootstrap API key.

The preparation command validates all role declarations before touching the
database, then migrates and applies grants in that order. It exits zero only
when both stages succeed. These are separate transactions: a role-installation
failure does not undo an already committed migration. Stop the release on a
nonzero exit, correct the reported stage and rerun the same revision with the
same declarations. Serialize database preparation and workload rollout; the
schema and role locks do not serialize the entire deployment.

For schema-changing upgrades without a verified compatibility guarantee, stop
application traffic and workers (including scheduled cleanup and autoscaling)
before preparation, preserving the database. Resume only with the matching
workload revision after preparation succeeds. This command does not stop
workloads or change Kubernetes resources. See the
[upgrade procedure](../deploy/helm/tetral/README.md#upgrade-and-rollback).

### Migration diagnostics

The migrator emits `schema.migration.started` and `schema.migration.completed`
for each pending version, or `schema.migration.failed` when migration fails.
Records include `operation=database.migrate`, `schema.version` when known,
`schema.step`, `duration_ms`, and `transaction.outcome`. Outcomes describe the
individual migration transaction, not the complete deployment:

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
An up-to-date database produces no per-version migration records.

The preparation command emits JSON on stderr with `service.name=db-prepare`.
`database.prepare.started`, `.completed`, and `.failed` describe the whole
command; failures identify `step` without serializing input or raw errors.
Detailed `schema.migration.failed` records precede the command failure summary.
A role-stage failure may follow successfully committed migration records.
Container stderr can be collected by the deployment's log agent; an arbitrary
local or SSH command needs its own log collection. This code does not send
requests to Loki, alter PostgreSQL server logging, perform backups or downgrade
committed migrations.

Tests use a different capability model. `internal/storage/storagetest` creates
one immutable schema template per exact migration-history identity, then gives
every native test a private cloned database and unique NOBYPASSRLS login. Those broad
test-only grants never define production privileges. Production authorization
tests use `storagetest.OpenWorkloadDB`: it applies the real installer contract
to a private clone and authenticates as the selected workload's unique login.
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
| bridge | Durable Memory mutation | `memory_stores UPDATE`; `services/bridge/bridge_api_tools_test.go` checks committed content and idempotent replay |
| bridge | Compaction Request End | `session_thread_context_prefixes UPDATE`; `services/bridge/runtime_compaction_role_test.go` checks main-thread checkpoint, child-prefix consumption, rollback and replay |
| bridge | Runtime context skill index | `skill_versions SELECT`; `services/bridge/bridge_api_context_test.go` checks configured version metadata |
| api | Session deletion through shared Sandbox release | `session_runtime_tool_results SELECT/INSERT/UPDATE`, `session_background_tasks SELECT/UPDATE`; `internal/session/postgresql_store_controlplane_test.go` checks atomic release and background cancellation custody |
| api | Child tool-confirmation admission during close | `session_bridge_operations SELECT`; `internal/sessionevent/closing_role_test.go` checks missing-grant failure, intended conflict with no receipt, and admission/replay after the source Tool Result is terminal; the event-store suite uses the API role |

The installer test independently inspects all live public tables and sequences,
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
Bridge Runtime APIs and cleanup, Sandbox lifecycle/execution/projection and
background command settlement, API event admission's child-close fence, Queue
lease and maintenance, Auth key management, Cleanup admission, Gateway credential
resolution, Git Proxy ticket/credential reads, and Event Stream's dedicated
read-only store. Agent Runtime has no database role and persists through Bridge.
This inventory guides review; the tests above do not claim dynamic coverage of
every branch of every workload.
