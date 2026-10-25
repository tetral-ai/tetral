# internal/instance

The rows behind Engine's machines: CLI registrations (instances), their
connection credentials, the idempotency records of the issuing routes, and
each instance's connections with their replay ledger, protocol-error reports
and Session instructions. `computer-hub` is the only service that writes them;
`auth` resolves a connection key through one security-definer lookup.

`doc.go` lists every table, state, transition and the writer of each
transition, with the package's invariants.

## Design rules

- One writer per transition. Each change of a row's state has exactly one
  function here, and every function runs in its caller's transaction scoped to
  the row's Workspace (`dbconnect.Client.WithWorkspaceTx`).
- Times are the database clock (`Now`), never a process clock; leases and
  grants are the database clock plus at most `LeaseGrant`, capped by the
  connection credential's expiry (`Grant`).
- A key's plaintext leaves the package once, in the issuing or fetching
  answer: rows hold its SHA-256 digest, and `ConnectionKey` prints a
  placeholder (`key.go`).
- A transaction that locks several rows of an instance locks them in the order
  instance, credentials, connection, each in its final mode at its first lock
  (`LockInstance`, `LockInstanceNoKeyUpdate`, `ShareInstance`,
  `LockCredential`, `ShareCredential`, `LockLiveConnection`,
  `ShareConnection`).
- A writer that changes what a connection's stream must write notifies the
  pod that holds the stream in its transaction (`NotifyConnection`); the pod
  reads the rows itself.
- An idempotency record commits with its effect, so a failed request leaves
  none (`SaveIdempotencyRecord`).

## Files

| File | Holds |
|---|---|
| `store.go` | the `Instance` and `Credential` rows and their plain reads |
| `instance.go` | the database clock, instance locks, creation, deregistration, lists and the instance object |
| `credential.go` | issuance, stream renewals, the key fetch, activation, reuse records, revocation and the credential limit |
| `key.go` | the connection key format and its generation |
| `idempotency.go` | idempotency records and the request fingerprint |
| `connection.go` | connections, leases and grants, ready and heartbeat records, ends, resumes and the connection notice |
| `ledger.go` | the replay ledger of upstream messages |
| `protocol_error.go` | protocol-error reports |
| `instruction.go` | Session instructions, their writes and reports, and the instruction pressure read |
| `audit.go` | the audit record of each credential operation |
