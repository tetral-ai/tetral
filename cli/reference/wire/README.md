# wire

The reference control server's own types for the protocol between `tetral`
and Engine belong here, one Go type for each schema definition of
`protocol/` that a fixture names. They are written apart from the CLI's Rust
types in `cli/crates/protocol`, import neither those nor any Engine package,
and are tested against the same fixtures, so a drift in either set fails its
own tests.

## Design rules

- `Decode` reads a message in two passes: `readStrict` over the raw bytes,
  then typed decoding of the tree it returns. The strict pass reads with
  `json.Decoder.Token` and `UseNumber` and refuses a repeated member name, a
  `\u` escape of a lone surrogate, a number that is not finite as a 64-bit
  float and an integer outside the signed 64-bit range.
- The decoder reads members by the exact names of their `json` tags; an
  unknown member is dropped, except in a struct with a `Closed` field, which
  refuses it. A `wire` tag adds `min` and `max` (an integer's value or an
  array's length), `unique` and `const`.
- A member keeps the difference between `null` and absent: `Nullable` is
  present and may be `null`, `Maybe` may be absent, `null` or a value, `Opt`
  may be absent but never `null`, and `Absent` must be absent. Every other
  member is required and not `null`.
- Integers are `int64`, read from the integer token; `7.0` and `7e0` are
  refused.
- Each value's `check` holds its own rule or the relations between its
  members, at every depth. A message's `checkReceived` reads it as received,
  for its size or members no type keeps, and a response's `checkAnswer`
  relates it to its request (`DecodeAnswer`).
- A tagged union, such as `NetworkPolicy` or `CommitChange`, holds one
  variant; `decodeVariant` picks it by its tag member and `encodeVariant`
  writes the tag first.
- `Encode` writes compact UTF-8 JSON with `encoding/json`; `omitzero` leaves
  out an absent member.

## Where things are

- `wire.go`: `Message`, `Decode`, `DecodeAnswer`, `Encode`, `DecodeError`,
  the reflective decoder, the presence types and the union helpers.
- `strict.go`: the strict pass and `StrictError`.
- `text.go`: the string values that members share: identities, paths,
  digests, drive paths, RFC 3339 times and decimals.
- `connection.go`: the messages of `connection-v1.schema.json`, and
  `ToolDownstream`, the envelope of a tool instruction.
- `tool.go`: the messages of `tool-execution-v1.schema.json`.
- `drive_transfer.go`: the messages of `drive-transfer-v1.schema.json`.
- `errors.go`: the client error table, `ErrorBody`, the drive transfer
  endpoint table, and the readers of an error body, `DecodeClientError` and
  `DecodeDriveTransferError`.
- `fixtures_test.go` decodes every fixture of `protocol/`; `strict_test.go`
  holds the strict pass's table and, built from edited fixtures, the tables
  of members sent as `null` and of unknown members in every closed object.
