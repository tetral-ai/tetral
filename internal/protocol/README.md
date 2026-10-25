# internal/protocol

Engine's Go types for the client protocol between Engine and the CLI: the
tool execution messages and the client connection messages, bodies and
query. The JSON Schema and
fixture files in [`protocol/`](../../protocol) define the protocol. Every type
here is written by hand from those schemas; the package imports no generated
code and no other set of protocol types. The Engine services that speak the
protocol, computer-hub and Bridge, decode and encode its messages only through
this package.

The files in `protocol/` are copies that nobody edits here: a needed
difference is a change of the protocol itself, made together with its
fixtures.

## Decoding

Each `Decode<Type>` function decodes the schema definition of the same name:
`[]byte` for JSON and `url.Values` for a query. It applies these rules in this order and returns the
first failure:

1. The input is UTF-8 JSON, or the query parameters that the schema models as
   strings. Invalid UTF-8, an escaped lone surrogate, a duplicate object key at
   any depth (equal after unescaping) and a query parameter that the
   definition models given more than once are rejected; a parameter that the
   definition does not model is not read.
2. Integers are parsed from their exact decimal text into `int64`; a value
   outside that range, a fraction and an exponent are rejected, and no integer
   passes through `float64`. A fraction or exponent beyond the `float64` range
   is rejected, since the protocol has no nonfinite number.
3. Every constraint of the schema definition is checked by hand-written code:
   required members, types, `const` and `enum`, `pattern`, string, array and
   numeric bounds, the conditional requirements and the members a definition
   marks absent.
4. Unknown members are skipped where the definition allows additional
   properties and rejected where it does not. `SandboxPolicy` and its parts
   are closed.
5. A schema `default` never fills a value: `Optional` and `Nullable` keep an
   omitted member apart from `null` and from the default.
6. The relations beyond the schema run last: `protocol_version` and
   `report_seq` as integer tokens wherever a tool execution message carries
   them, also where the definition does not model the member; the policy,
   budget and size rules of tool execution; and the connection rules. A
   request body's complete limit is its first relation.

Sizes are measured on the compact JSON with every string and number token
counted as written, escapes included. A failure is a `*DecodeError` holding
the JSON pointer of the first failing value and the status and code the
protocol answers it with: `400 invalid_message`, also for a member over its
own limit inside a body within its limit, except a request body over its
complete limit (`413 request_too_large`: an ordinary or media tool-status, a
drive sync report, a local MCP tool-list report, and the 64 KiB of
every other request). A message Engine sends or a response it gives that is
over its limit fails with `400 invalid_message`, since no request of Engine's
is answered with it. `DecodeErrorBody` rejects an error code that
`ClientErrors` does not pair with the response's status.

## Encoding

`Encode` writes only the members a value holds, `null` only for a member set to
`null`, integers as their exact text, the schema's constants, and strings with
minimal escaping, so that the length of an encoding is the size its receiver
measures.

## Terms

- **Drive set**: the drives, each with its access, that Engine last sent to a
  binding in `session.prepare` or `session.drives`; it authorizes the binding's
  drive transfer requests.
- **Sync point**: the step in which the CLI uploads and commits a Session's
  drive changes before it reports an `exec_command`, `write_stdin` or `edit`
  result; the result's drive sync outcome reports what it did.
