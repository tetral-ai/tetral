# integration/enginetest

Test clients that stand in for Engine's remote peers in cross-process tests.

`CLI` (`cli.go`) is an HTTP and SSE client in the CLI's place: it speaks the
client connection protocol of `internal/protocol` to `computer-hub`, directly
or through the public edge. It sends the connection key as a bearer token,
as the CLI does through the edge, unless its `Authorize` hook sets the
request's credentials instead, such as a signed internal principal in tests
that reach `computer-hub` without the edge.

## Design rule

The clients send exactly what a test gives them and decode what Engine
answers. They keep no protocol state, apply no retry and never repair a
message, so a test controls every byte that reaches Engine.
