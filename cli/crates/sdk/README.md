# sdk

The typed client of the App Server's local API belongs here, for apps that run
`tetral` on a user's machine.

## Design rules

- It depends on `protocol` and `state` only, reaches the App Server only
  through the local API, never starts one and never reads `config.toml` or
  `credentials.json`.
- `Client::new(root)` derives the root's socket path with
  `state::paths::socket_path` and checks `S` and the socket as the root
  checks do (`state::private::check_socket`); a path that is not private is
  an `Io` error of kind `PermissionDenied`.
- Every endpoint returns its `protocol::local` type, `shutdown` nothing on
  the body `{}`, `status_and_metrics` the status and the metrics' text
  within one five-second deadline, or an `Error`:
  `NotRunning` when the socket is missing or refuses the connection,
  `NotResponding` when status, shutdown or status and metrics exceed their
  five-second total
  deadline (connect through the complete response body), `Http`
  with the status and body of any answer but the endpoint's success,
  `Decode` for a body that does not decode or exceeds the response limit, and
  `Io` for every other failure.
- A response body is read up to `RESPONSE_MAX_BYTES`, or the limit given to
  `Client::with_response_limit`, and refused past it.

## Where things are

- `lib.rs`: this documentation, the print lints that `cli/README.md` states,
  `Client` with `status`, `sessions`, `shutdown` and `status_and_metrics`,
  and `Error`.

Status and shutdown cancel their connection driver on completion or timeout.
A timeout does not establish whether shutdown was accepted or completed.

The optional `test-seams` feature exposes `Client::with_test_socket_context`
for harness clients. It accepts an explicit `state::paths::TestSocketContext`
and response limit, retaining the normal socket derivation and permission
checks. Tests need no process-wide environment changes; production builds
have no such constructor.
