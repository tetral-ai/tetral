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
- Every endpoint returns its `protocol::local` type or an `Error`:
  `NotRunning` when the socket is missing or refuses the connection, `Http`
  with the status and body of any answer but the endpoint's success,
  `Decode` for a body that does not decode or exceeds the response limit, and
  `Io` for every other failure.
- A response body is read up to `RESPONSE_MAX_BYTES`, or the limit given to
  `Client::with_response_limit`, and refused past it.

## Where things are

- `lib.rs`: this documentation, the print lints that `cli/README.md` states,
  `Client` with `status`, `sessions` and `shutdown`, and `Error`.
