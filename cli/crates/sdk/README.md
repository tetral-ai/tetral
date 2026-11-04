# sdk

The typed client of the App Server's local API belongs here, for apps that run
`tetral` on a user's machine.

## Design rules

- It depends on `protocol` and `state` only, reaches the App Server only
  through the local API, never starts one and never reads `config.toml` or
  `credentials/credentials.json`.
- `Client::new(root)` derives the root's socket path with
  `state::paths::socket_path` and checks `S` and the socket as the root
  checks do (`state::private::check_socket`); a path that is not private is
  an `Io` error of kind `PermissionDenied`.
- Every endpoint returns its `protocol::local` type, `shutdown` and
  `set_computer_use` nothing on the body `{}`, `status_and_metrics` the
  status and the metrics' text within one five-second deadline, or an
  `Error`:
  `NotRunning` when the socket is missing or refuses the connection,
  `NotResponding` when status, shutdown or status and metrics exceed their
  five-second total deadline (connect through the complete response body),
  `NoAnswer` when the App Server closed the connection before any answer,
  as it does to a computer-use change still waiting when its shutdown
  starts,
  `Http` with the status and body of any answer but the endpoint's success,
  such as the `400` and `500` of `POST /v1/computer-use`,
  `Decode` for a body that does not decode or exceeds the response limit,
  and `Io` for every other failure. Tests construct the client with a shorter
  private response budget; the public client always uses five seconds.
- `set_computer_use(enabled, driver)` sends `POST /v1/computer-use` with
  `{enabled, driver}`, `driver` as `null` when absent, and has no deadline of
  its own: an enable answers once the driver's `initialize` succeeded or its
  30-second start bound ended it.
- A response body is read up to `RESPONSE_MAX_BYTES`, or the limit given to
  `Client::with_response_limit`, and refused past it.

## Where things are

- `lib.rs`: this documentation, the print lints that `cli/README.md` states,
  `Client` with `status`, `sessions`, `shutdown`, `status_and_metrics` and
  `set_computer_use`, and `Error`.

Status and shutdown cancel their connection driver on completion or timeout.
A timeout does not establish whether shutdown was accepted or completed.

The optional `test-seams` feature exposes `Client::with_test_socket_context`
for harness clients. It accepts an explicit `state::paths::TestSocketContext`
and response limit, retaining the normal socket derivation and permission
checks. Tests need no process-wide environment changes; production builds
have no such constructor.
