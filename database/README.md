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
test-only grants never define production privileges.
