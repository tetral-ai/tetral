// Package httpapi serves Engine's public HTTP API: the /v1 routes, their
// handlers and middleware, and the public error envelope.
//
// Design rules:
//   - RequestLogMiddleware (middleware.go) records the boundary: one
//     http.request.completed per request with its route pattern from
//     matchedRoute, "unmatched" when no route serves it, and never its path
//     or query; one http_server_request_duration_seconds sample, or a stream
//     sample for a route that WithLongRoutes names; and a server span that
//     starts a new trace, an incoming traceparent becoming only a link of it.
//     Health checks and /metrics are never recorded (quietPath);
//     WithQuietRequests and WithUntracedRequests name the other requests that
//     observe nothing or start no span.
//   - RequestIDMiddleware gives each request one req_ ID, written to the
//     request-id header and carried as request.id by the request's record and
//     its authentication audit record.
//   - Each route declares its public operation (authorization.go) on the
//     handler the router registers, so coverage tests read the router itself.
//
// Where each concern lives: router.go (NewRouter, the middleware order and
// route registration), middleware.go (request ID, recording, authentication
// and panic recovery), one <resource>_handler.go per API resource,
// request_text.go and beta_query.go (request text and query checks), and
// errors.go and respond.go (the error envelope and JSON responses).
package httpapi
