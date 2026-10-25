# internal/hubflow

The `computer-hub` steps that span packages: each one runs in its caller's
transaction, so its writes commit or roll back with the request that runs
it. The package owns no table; the rows belong to `internal/instance`.

`doc.go` lists the steps, their callers and the package's invariant.

## Steps

- `AcceptReady` (`ready.go`) accepts a connection's ready and notifies the pod
  that holds the connection's stream, which then starts its heartbeats.
- `EndConnection` (`end.go`) is the one end step of a connection for every
  cause: `lease_expired` when a new connection replaces one whose lease
  passed, `revoked` when the live connection's credential is revoked, and
  `deregistered` when its instance is deregistered. It writes the end,
  deletes the replay ledger and notifies the stream's pod.

## Design rule

The caller holds the connection's row lock `FOR UPDATE`, so a step commits
wholly before or after any request that holds the row `FOR SHARE`.
