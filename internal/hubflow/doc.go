// Package hubflow holds the computer-hub steps that span packages. Each step
// runs in its caller's transaction, so its writes commit or roll back with
// the request that runs it. It owns no table; the rows belong to
// internal/instance and internal/computer.
//
// STEPS:
//
//	AcceptReady (ready.go) accepts a connection's ready (instance.AcceptReady),
//	               fulfils the Controller requests it answers
//	               (computer.FulfilRequests) and notices the pod holding the
//	               connection's stream, which then starts its heartbeats.
//	               Caller: the ready route.
//	EndConnection (end.go) the single end step of a connection, for every cause:
//	               lease_expired (a new connection replaces one whose lease
//	               passed), revoked (the live connection's credential is
//	               revoked) and deregistered (its instance is deregistered).
//	               It runs instance.EndConnection, which writes the end,
//	               deletes the replay ledger and notices the stream's pod.
//	               Callers: the events route, the credential revocation and
//	               instance deregistration routes, and the two request steps.
//	ReturnRequestToPending, FailRequest (request.go) run computer.ReturnToPending
//	               or computer.Fail and then EndConnection with cause revoked
//	               for the live connection of the claim credential they
//	               revoked; internal/computer cannot end a connection itself,
//	               since this package imports it. Callers: the request release
//	               and fail routes.
//
// INVARIANTS:
//   - The caller holds the connection's row lock FOR UPDATE, so a step
//     commits wholly before or after any request that holds the row FOR
//     SHARE.
//
// UPDATE-WITH:
//
//	internal/instance and internal/computer (the writers each step runs);
//	services/computer-hub (the routes that call the steps and the notice
//	  handling of the stream's pod).
package hubflow
