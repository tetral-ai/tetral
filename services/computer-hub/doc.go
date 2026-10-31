// Package computerhub is the service between Engine and the machines it
// uses: the client connection routes under /v1/client/, which a CLI calls
// with its connection key, and the Controller and developer routes under
// /v1/instance_requests and /v1/instances. It owns no table: it reads and
// writes rows only through internal/instance (instances, credentials and
// connections), internal/computer (computers, requests and bindings) and
// internal/gitticket (git tickets), and runs the steps that span packages
// through internal/hubflow. README.md is the service's contract.
//
// DESIGN RULES:
//   - A connection key is accepted only under /v1/client/ and a workspace
//     credential only on the management routes; the edge decides the class
//     first and NewRouter again (routes.go).
//   - A client request runs one transaction that checks size, shape,
//     identity, authority, connection and replay in that order before it
//     changes anything, and a refusal changes nothing (client_checks.go).
//   - A stream is the only writer of its response. A connection notice only
//     wakes it; the stream reads its connection and instructions itself, in
//     write passes that each choose in one transaction and write after it
//     commits (stream.go, dispatch.go, notice.go).
//   - Any pod serves any request and holds only the streams it serves, so a
//     CLI resumes on another pod when its pod drains or dies (Streams.Drain).
//   - Every time a request or pass compares is the database clock
//     (instance.Now), never the pod's.
//
// ENTRY POINTS:
//
//	cmd/tetral-computer-hub  the process: settings, the database and the blob
//	                         store, then Run.
//	Run                      both listeners, the streams, the instance event
//	                         fan-out and the drain at shutdown (run.go).
//	NewRouter                the public route table (routes.go).
//
// FILES:
//
//	config.go               every TETRAL_COMPUTER_HUB_* setting (ConfigFromEnv).
//	routes.go, errors.go    route groups, the principal check and the client
//	                        and API error bodies.
//	instance_routes.go      instances, credentials and their idempotency.
//	request_routes.go       the Controller's requests: list, claim, release
//	                        and fail.
//	pagination.go           list pages and their signed page tokens.
//	instance_stream.go      the instance event stream (InstanceEvents).
//	request_stream.go       the request event stream (RequestEvents).
//	client_checks.go        the client checks and their transaction.
//	client_events.go        GET /v1/client/events: new, resumed or refused.
//	client_session_status.go  preparation reports (computer.StoreReport).
//	client_git_ticket.go    git tickets of a cloud computer's binding.
//	client_ready.go, client_heartbeat.go, client_protocol_error.go,
//	client_credential.go    the other client routes, one each.
//	stream.go               held streams, heartbeats, the age limit and the
//	                        drain.
//	dispatch.go             the write pass: renewals and Session instructions.
//	notice.go               the connection notice listener (Streams.Run).
//	metrics.go              the hub's metric families and pressure point.
//	declarations.go         the records and metrics of observability.json.
package computerhub
