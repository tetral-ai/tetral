// Package reference is the reference control server: Engine's side of the
// client connection between tetral and Engine, which the CLI's
// cross-process tests run tetral against.
//
// Server serves the protocol's endpoints (Handler) over HTTP and SSE with
// its state in memory, and every time it reads comes from its Clock, which
// only the clock control moves. ControlHandler serves the fault-injection
// controls on a second listener. serve holds the steps of each upstream
// request, and due what an advance of the clock makes happen.
package reference
