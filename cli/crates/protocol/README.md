# protocol

The types that the other crates share belong here: the wire types of the
protocol with Engine, the bootstrap that `tetral configure` installs, the
bodies of the local API, and, in `ports`, the
interfaces through which peer crates reach each other. The protocol's own
description, schemas and fixtures are in `protocol/` at the repository root.

## Design rules

- It depends on no other workspace crate and records nothing.
- `decode` reads a message in four steps: its size against the type's
  `Message::MAX_BYTES`, refused before anything is parsed; `strict::check`
  over the raw bytes, which refuses a repeated member name, a `\u` escape of
  a lone surrogate, a number that is not finite as a 64-bit float, an
  integer outside the signed 64-bit range, and objects and arrays nested
  deeper than `strict::MAX_NESTING` (127) levels; typed decoding; then the
  rules of `Message::check`.
- Each type has the members of its schema definition, under their exact
  names. Unknown members are dropped, except in the objects the schema
  closes, such as `sandbox_policy` and a commit request, which refuse them.
- A member keeps the difference between `null` and absent: `Nullable` is
  present and may be `null`, `Maybe` may be absent, `null` or a value, an
  `Option` read with `field::present` may be absent but never `null`, and
  `Absent` must be absent.
- Integers are read as signed 64-bit integers, never through floating point
  (`field`); the integer `-0` reads as 0, as the schemas read it
  (`decode_value`).
- Only the `state` crate turns a `SafeId` into a path component (`id`).
- Beyond the schema, a decoder checks the rules of `Message::check`: the
  narrower size limit of a downstream event, `connection::MESSAGE_MAX_BYTES`
  or, for a tool instruction, its type's own limit through
  `tool::check_instruction`, and relations between members. A tool-status
  report is read under the media limit whatever it holds (`tool::ToolStatus`).
  A response is read
  with `decode_answer` against the request it answers, and an error body
  together with its HTTP status and endpoint.
- Messages are written as compact UTF-8 JSON by `encode`.
- A connection key prints as `<redacted>` in `Debug` and `Display`. There are
  two types, one per rule the protocol gives a key: `bootstrap::BootstrapKey`,
  the bootstrap's key, any nonempty text, whose plaintext leaves it only
  through `expose`, for the `Authorization` header, and `Bootstrap::encode`,
  which writes `credentials.json`; and `connection::ConnectionKey`, a key
  that Engine issues in a credential response, `tetral_ck_` and its text,
  whose plaintext leaves it only through `expose`, for the `Authorization`
  header and the credential file. A bootstrap's decoding failure names the
  member and its rule, never its value (`BootstrapError`).

## Where things are

- `lib.rs`: `Message`, `decode`, `decode_answer`, `encode`, `DecodeError`
  and `PROTOCOL_VERSION`.
- The size limits and bounds are named constants beside the messages they
  bound: `connection::MESSAGE_MAX_BYTES` and the constants after it,
  `tool::limits` and `drive_transfer::limits`.
- `strict.rs`: the strict pass.
- `field.rs`: member presence, constant members and the checked values that
  members share (`Text` with its rules, `Bounded`, `List`, `Unique`).
- `id.rs`: `Id` and `SafeId`. `time.rs`: `Rfc3339Utc` and `system_time`,
  the system time it names. `path.rs`: drive paths and their case folding.
- `connection.rs`: the messages of `connection-v1.schema.json`: the
  event-stream query, `Downstream` and its payloads, ready, heartbeats,
  Session reports, the local MCP tool list, credentials and Git tickets; and
  `ToolDownstream`, the envelope of a tool instruction.
- `tool.rs`: the messages of `tool-execution-v1.schema.json`: tool
  instructions, `sandbox_policy`, tool-status and drive sync reports, ACKs,
  protocol-error reports and the tool part of a ready's capabilities.
- `drive_transfer.rs`: the messages of `drive-transfer-v1.schema.json`:
  listings, uploads, downloads with their headers, and commits.
- `errors.rs`: the client error table (`ErrorCode` with its status),
  `ErrorBody`, the endpoint table `DriveTransferEndpoint`, and the readers of
  an error body, `decode_client_error` and `decode_drive_transfer_error`.
- `bootstrap.rs`: `Bootstrap`, the object that a launcher gives
  `tetral configure` on stdin and that `credentials.json` holds, its member
  rules (`ServerUrl` among them), `BootstrapKey` and `BootstrapError`.
- `local.rs`: the bodies of the local API.
- `ports.rs`: `ReportSink`, `SessionFactory`, `SessionHandle` and
  `ConnectionPort`.
- `tests/fixtures.rs` decodes every fixture of `protocol/`;
  `tests/strict.rs` holds the strict pass's table and, built from edited
  fixtures, the tables of members sent as `null`, refused or, where the
  schema allows it and no fixture sends it, kept, and of unknown members in
  every closed object; `tests/common/` reads the schema and fixture files and
  decodes a fixture as the type of its schema.
