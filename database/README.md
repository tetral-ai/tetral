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

Run `tetral-postgresql-roles` once before a fresh installation and whenever the
repository-owned role contract changes. The command constructs the current V1
schema with the administrative connection, applies the role contract, and is
idempotent. Runtime services then use only their serving DSNs; API alone also
receives the separate migration-owner DSN for its pinned migration transaction.

Tests use a different capability model. `internal/storage/storagetest` creates
one immutable schema template per exact baseline identity, then gives every
native test a private cloned database and unique NOBYPASSRLS login. Those broad
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
| bridge | Durable Memory mutation | `memory_stores UPDATE`; `services/bridge/bridge_api_tools_test.go` checks committed content and idempotent replay |
| bridge | Compaction Request End | `session_thread_context_prefixes UPDATE`; `services/bridge/runtime_compaction_role_test.go` checks main-thread checkpoint, child-prefix consumption, rollback and replay |
| bridge | Runtime context skill index | `skill_versions SELECT`; `services/bridge/bridge_api_context_test.go` checks configured version metadata |
| api | Session deletion through shared Sandbox release | `session_runtime_tool_results SELECT/INSERT/UPDATE`, `session_background_tasks SELECT/UPDATE`; `internal/session/postgresql_store_controlplane_test.go` checks atomic release and background cancellation custody |

The installer test independently inspects all live public tables and sequences,
including undeclared ones, and verifies that reapplication repairs missing grants
and removes overgrants. This establishes the upper permission bound; owner tests
establish that intended operations remain possible. Declaration equality or
empty-table SQL alone cannot establish the latter.

Authorization changes must also inspect other callers: API control-plane stores,
Bridge Runtime APIs and cleanup, Sandbox lifecycle/execution/projection, Queue
lease and maintenance, Auth key management, Cleanup admission, Gateway credential
resolution, Git Proxy ticket/credential reads, and Event Stream's dedicated
read-only store. Agent Runtime has no database role and persists through Bridge.
This inventory guides review; the tests above do not claim dynamic coverage of
every branch of every workload.
