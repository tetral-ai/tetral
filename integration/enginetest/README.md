# integration/enginetest

Test clients that stand in for Engine's remote peers in cross-process tests.

`CLI` (`cli.go`) is an HTTP and SSE client in the CLI's place: it speaks the
client connection protocol of `internal/protocol` to `computer-hub`, directly
or through the public edge. It sends the connection key as a bearer token,
as the CLI does through the edge, unless its `Authorize` hook sets the
request's credentials instead, such as a signed internal principal in tests
that reach `computer-hub` without the edge.

`Runtime` (`runtime_client.go`) stands in for one Runtime pod. It calls
Bridge through the client its test gives it, whose connection sends the
bearer token that the test's identity stand-in maps to the service account
`tetral-agent-runtime/agent-runtime`: it registers its process
(`RegisterRuntimeProcess`, then an accepting `ReportRuntimeProcess`), takes a
binding of a Session through Job Runner's delivery store
(`PrepareRuntimeCommand` of a configuration update, the store seeing this
client as its only Runtime pod and reading its placement load from an
endpoint the client serves) and loads it with `LoadContext`. Before each
machine-bound call it writes the model request start and the tool-use event
with decision `allow` and route capability `machine_tool` (`machine_control`
for `write_stdin`), as the Runtime does, then calls `AdmitToolCall`,
`AdmitToolControl` or `CancelToolCall`. It runs no SQL of its own; the
delivery store runs Job Runner's.

`Developer` (`developer.go`) is a client of the public API in a developer's
or a Controller's place, held to Engine's OpenAPI file (`api/`); see "The
developer client" below.

## Design rule

The clients send exactly what a test gives them and decode what Engine
answers. They apply no retry and never repair a message, so a test controls
every byte that reaches Engine. Beyond the Runtime process's registration
receipt, the only protocol state they keep is what the stream rules below
need.

## The contract check

A `CLI` is made by `NewCLI(t)`. It holds every request it sends and every
answer and event it receives to the contract of `protocol/`, and fails `t`
with `Errorf` for each one outside it; it never changes what is sent or
returned. A copy of a `CLI` reports to the same test.

Each message is checked against three things:

- **The schemas.** The definition `protocol/README.md` names for the part
  (query, path parameters, headers, body, event), in `protocol/`'s JSON
  Schema files, read by a schema validator (`contract.go`).
- **The decoders.** The rules `protocol/README.md` adds to a schema, through
  the decoder of `internal/protocol` for the part, given the request it
  belongs to: an error body with its status and its endpoint, a list page
  with its query, a commit's results with its changes, an upload's answer
  with its URL and length, a returned range with the requested one. The check
  itself holds an ACK to its request's `connection_id` and `message_id`, a
  credential or git-ticket answer to `Cache-Control: no-store`, and a success
  status to the ones the endpoint has (`exchange.go`). The endpoints are the
  table of `protocol/README.md`, "Endpoints", and the drive transfer schema's
  `DriveTransferEndpoints` (`routes.go`). An answer of status 500 or above has
  no defined body and is not checked.
- **The stream rules** (`rules.go`), each with its section of
  `protocol/README.md`:

  | Rule | Section |
  |---|---|
  | The first event of a stream is `connected` | Endpoints; Downstream messages |
  | An event is one `data` line that holds one envelope; no `event` or `id` field, no comment | Downstream messages |
  | An event is within 64 KiB, or within its own limit for a tool instruction; a request body is within 64 KiB, or within its own limit for a tool report, a tool list and a commit | Resource limits; Message sizes and encoding; Common rules |
  | A message is on one stream once | Downstream messages; Resource limits (replay ledger) |
  | Before the first Session instruction of a stream comes one `drive.changed` for every drive that has a commit in the drive set of a prepared or closing binding of the connection, when its ready declared drive mounting | Drive change notice; Drive binding changes |

  For the last rule the `CLI` follows what it has seen: the ready Engine
  accepted, the `session.prepare` and `session.drives` it received, the
  Session reports it sent, and the drives a `drive.changed`, a commit or a
  first list page showed a commit of. It asks only for the notices Engine
  certainly owes: a drive counts from the ACK of its binding's `prepared`
  report until the `applied` or `closed` report that removes it is sent, and
  a connection or binding another `CLI` value prepared is unknown to it.

**Requests outside the contract on purpose.** A test that sends such a
request sends it through `cli.Invalid()`. The request is then not held to the
contract, and fails the test when it is within it, so a mark never outlives
its reason; its answer is checked as any other. An unmarked request to a
method and path the contract does not have fails the test. A request body
the transport did not send whole (the test stalls or cuts it) cannot be
checked and is not reported.

## Behaviour cases

`cases.go` reads `protocol/behaviour-v1.cases.json` (`protocol/README.md`,
"Behaviour cases") and holds what every runner of the cases needs: the file's
form check, the symbols and captured values of one run, and the matcher of an
expected value. Engine's runner of the Engine-direction cases is
`integration/behaviour_cases_test.go`: it brings each state about and runs
each stimulus against `computer-hub` with real PostgreSQL, sends the case's
requests through a `CLI`, and fails on a state or stimulus it does not
implement.

## The scripted machine

A `Machine` (`machine.go`) is a `CLI` with the connection's default script:
`Connect` opens the stream and sends ready, and each heartbeat is answered. A
test sends every other report itself. What a test sends as the real CLI's
behaviour goes through `React`, or is checked with `HoldReaction`, against the
CLI-direction behaviour case that states it: the instruction is the case's
trigger and the reports are its requests, one each and in its order, so the
sequence is read from the case file and a report the case does not state is
refused before it is sent. The CLI's own tests run the same cases against the
real CLI.

A test that needs a step late, twice, changed or missing replaces `Ready` or
`Heartbeat`, or sends through the `CLI` directly; such a step needs no case.
`integration`'s fixture holds its ready and prepared reports to their cases in
the same way, and a test holds each closed or applied report and each
command's terminal report it sends as what the real CLI sends, with the
command's running report before it when there is one.

## The developer client

A `Developer` sends a request to a public route and holds the request, the
answer with its headers and every stream event to `api/engine-v1.openapi.json`.
A request outside the file fails the test as the test's defect; an answer or
an event outside it fails the test as Engine's defect. It reads what the file
states and nothing else:

- the request's path, query and header parameters, its body (JSON or a
  form; a JSON body without a media type is taken as JSON, but on the token
  exchange, which requires `application/json`), and, on an operation with
  `x-closed-query`, that every query name is declared;
- the answer's status, the headers its status declares with the values their
  descriptions give (`Cache-Control: no-store`, `Retry-After` in seconds,
  `x-should-retry: false`), its media type and its body's schema, and under
  an error status that `error.type` is one of the status's `x-error-types`
  and `request_id` is the answer's `request-id`;
- on a stream answer, that it is `text/event-stream`, that each frame is one
  `event` line and one `data` line, that each event's name is one of the
  answer's `x-events`, and each event's `data` its schema. A frame of
  comment lines carries no event;
- for a `/v1` path the file does not declare for the request's method, the
  status `x-undeclared-paths` gives it: 401 `authentication_error` for any
  path, else 400 `invalid_request_error` for an `unsupported` surface, 405
  for a path declared with other methods, and 404 for any other.

A request made invalid on purpose, or sent to a `/v1` path the file does not
declare, is marked: only its answer is checked, and the answer must be a
refusal. A path outside `/v1`, such as a health probe, and the machine's
routes under `/v1/client`, which the `CLI` holds to `protocol/`, are no part of
the file and pass unchecked.

A test reaches the check in one of four ways, which check alike:

- `Do` and `Stream` send a request the client builds from the test's method,
  target, headers and body; `Invalid()` marks one.
- `Handler(router)` wraps a router a test calls in process with requests it
  builds itself; `InvalidRequest(request)` marks one.
- `Transport(base)` wraps the transport of a test's own HTTP client.
- `Exchange(request, response)` holds a request a test wrote to a socket
  itself and the response it read back.

A report names the test line the request was sent from. Tests of handlers
over fake services (layer 1) build their own routers and are not held to the
file: a fake's answer is not Engine's.

When `TETRAL_API_COVERAGE_FILE` names a file, each checked operation with its
status, error type and stream event is appended to it, one JSON object per
line; the gate compares the records of a full run with the file.
