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

## Design rule

The clients send exactly what a test gives them and decode what Engine
answers. They keep no protocol state beyond the Runtime process's
registration receipt, apply no retry and never repair a message, so a test
controls every byte that reaches Engine.
