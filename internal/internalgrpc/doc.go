// Package internalgrpc serves and dials Engine's internal gRPC: every
// in-cluster call between Engine services goes through its server and its
// dial options.
//
// Design rules:
//   - The server authenticates each call's workload identity and authorizes
//     its method before the handler runs (server.go); a panic becomes an
//     Internal status and one internal.grpc.recovered record.
//   - Each call is a span that continues the caller's trace from the
//     traceparent metadata key, carries workspace.id, session.id and
//     thread.id read from the request message (a stream's first message),
//     observes grpc_server_request_duration_seconds, and writes one DEBUG
//     internal.grpc.request; the component that handles a failure records it.
//   - Every dial option set (session_rpc_options.go) installs the traceparent
//     client interceptors of internal/internalgrpc/auth; the dial sites add
//     their workload credentials.
//
// Entry points: Run and NewServerWithHealth build the server; RunGRPCWorkload
// (grpc_workload.go) runs a gRPC service process with its health and metrics
// listeners; the *RPCDialOptions and *RPCServerOptions functions carry each
// service pair's message-size and keepalive bounds.
package internalgrpc
