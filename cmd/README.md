# Operational commands

These one-shot commands prepare deployment-owned state and then exit. Serving
entrypoints remain under `services/<service>/cmd`; they verify readiness and do
not run database migrations or install roles.

- `tetral-db-prepare`: validate role declarations, apply pending schema versions,
  then install the declared role permissions using an administrative connection.
- `tetral-bootstrap`: seed the initial workspace using the API serving role.

Both ship in the Tetral image. See the [database contract](../database/README.md)
for ownership and transaction guarantees and [bootstrap instructions](../docs/bootstrap.md)
for invocation and deployment ordering.
