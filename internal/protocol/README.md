# internal/protocol

Engine's Go types for the client protocol between Engine and the CLI: the
tool execution messages, the client connection messages and bodies, and the
drive transfer queries, headers, bodies and responses. The JSON Schema and
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
`[]byte` for JSON, `url.Values` for a query, `http.Header` for headers and a
map for path parameters. It applies these rules in this order and returns the
first failure:

1. The input is UTF-8 JSON, or the query parameters, path parameters or
   headers that the schema models as strings. Invalid UTF-8, an escaped lone
   surrogate, a duplicate object key at any depth (equal after unescaping) and
   a parameter or header that the definition models given more than once are
   rejected; a parameter or header that the definition does not model is not
   read.
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
   them, and `protocol_version` wherever a drive transfer body does, also where
   the definition does not model the member; the policy, budget and size rules
   of tool execution; the connection rules; and the drive transfer path, order
   and size rules. The decoders of drive transfer responses and response
   headers take the request they answer as a context and also check the
   relations to it. A request body's complete limit is its first relation.

The token parser keeps one incremental path of unescaped segments, rendering a
complete JSON pointer only for an error. Path bookkeeping therefore does not
retain a full ancestor pointer at every nesting level. The existing maximum
of 10,000 nested arrays/objects remains unchanged.

Sizes are measured on the compact JSON with every string and number token
counted as written, escapes included. A failure is a `*DecodeError` holding
the JSON pointer of the first failing value and the status and code the
protocol answers it with: `400 invalid_message`, also for a member over its
own limit inside a body within its limit, except a request body over its
complete limit (`413 request_too_large`: an ordinary or media tool-status, a
drive sync report, a local MCP tool-list report, and the 64 KiB of every other
request), a download `Range` naming more than one range (`416
range_not_satisfiable`) and a commit body over 16 MiB (`413
request_too_large`), which `DecodeCommitRequest` answers before any other
rule. A message Engine sends or a response it gives that is over its limit
fails with `400 invalid_message`, since no request of Engine's is answered
with it. `DecodeCommitRequest` leaves the change count to
`CheckCommitChanges`, which answers more than 10,000 changes with `413
request_too_large`. `DecodeUploadContentRequestHeaders` checks only the
headers' form and leaves the length to `CheckUploadLength`, which answers a
missing `Content-Length` with `400 invalid_message` and one above 500,000,000
with `413 file_too_large`. The hub applies both checks after its connection,
binding and drive access checks, so a stale connection is answered before any
size. `DecodeErrorBody` rejects an error code that `ClientErrors` does not
pair with the response's status, and `DecodeDriveTransferError` a status and
code pair that `DriveTransferEndpoints` does not list for the endpoint.

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
