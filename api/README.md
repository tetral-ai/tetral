# Engine's public API

`engine-v1.openapi.json` is the contract between Engine and its clients, the
SDK included: every public operation, the developers' and the Controllers'.
The operations are the route table of `internal/auth/operations.go` and `POST
/v1/oauth/token`. The machine's routes under `/v1/client/` are not here; their
contract is [`protocol/`](../protocol/README.md).

The file is OpenAPI 3.1. It started from the upstream description the SDK was
generated from: what Engine serves was kept, the rest dropped, Engine's own
operations added. Where Engine differs from upstream, the file states what
Engine does.

## What the file states

- **Each operation**: path and method, `operationId` (the operation's name in
  the route table), its path, query and header parameters, its request body,
  every success answer, every error answer, and the response headers a client
  acts on (`Retry-After`, `x-should-retry`, `request-id`, and `Cache-Control`
  on the token exchange). `x-closed-query: true` marks an operation that
  refuses a query name it does not declare.
- **Errors**: an error answer is `ErrorResponse`. Each error status of an
  operation lists, in `x-error-types`, every `error.type` the operation
  answers under that status.
- **Closed objects**: an object with fixed fields has `additionalProperties:
  false`; a field the file does not name appears in no request, answer or
  event. A position whose keys are not fixed is a dictionary with its value
  type: a `metadata` value is a string, and on an update also null.
- **Streams**: `x-event-streams`, at the top of the file, states the one form
  of a stream answer. A stream operation's `200` answer is `text/event-stream`
  with `x-events`: every event name the stream sends, each with the schema of
  its `data`. Comment lines carry no event.
- **Other paths**: `x-undeclared-paths` states what Engine answers for a
  `/v1` path the file does not declare.
- **Paths**: `x-path-handling` states how a path is normalized before it is
  routed, and which paths are refused. A path parameter with
  `x-rest-of-path: true` is the path's last segment and takes the rest of
  the path, one segment or more, `/` included; the route table names that
  route with `*` in its place.
- **The edge's own answers**: `x-edge-answers` states what a client reads for
  a refused path and while a service behind the edge cannot be reached.

## Keeping it true

A handler and the file change in the same pull request.

- `api_test.go` compares the file's operations with the route table and the
  token exchange, and holds the file to its form: closed objects, resolved
  references, typed error answers, named stream events.
- Engine's tests send every request to a public route through
  `enginetest.Developer` (`integration/enginetest`), which holds each request,
  answer, header and stream event to the file. An answer or event outside the
  file fails the test as Engine's defect; a request outside it as the test's.
- The test runner names a record file, and the client appends what it
  checked. After Engine's full suite the merge gate fails on every operation
  with no checked success answer, every declared error type checked on no
  operation and every declared stream event checked on no stream.
  `unreachable.json` lists what no test can reach, each with its reason; an
  entry a test did check fails the gate too.

The SDK keeps a copy of this file, named by the Engine commit it was taken
from, and checks its Tetral contract tests against that copy.
