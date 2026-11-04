// Package hubflow holds the computer-hub steps that span packages. Each step
// runs in its caller's transaction, so its writes commit or roll back with
// the request that runs it. It owns no table; the rows belong to
// internal/instance and internal/computer.
//
// STEPS:
//
//	AcceptReady (ready.go) accepts a connection's ready (instance.AcceptReady),
//	               writes the errors of the lost_changes it carries
//	               (computer.RecordLostChanges), fulfils the Controller
//	               requests it answers (computer.FulfilRequests), issues a
//	               session.prepare for every attached computer on the
//	               instance (computer.IssuePreparationsOn) and a late close
//	               for every detached one whose closure is unknown
//	               (computer.IssueLateCloses), and notices the pod holding
//	               the connection's stream, which then starts its heartbeats.
//	               Caller: the ready route.
//	EndConnection (end.go) the single end step of a connection, for every cause:
//	               lease_expired (its lease passed: the Cleanup computer
//	               phase, or a new connection that replaces it), revoked (the
//	               live connection's credential is revoked) and deregistered
//	               (its instance is deregistered). It runs
//	               instance.EndConnection, which writes the end, deletes the
//	               replay ledger and notices the stream's pod; then
//	               runtimecontrol.LockConnectionTaskSessionsTx; then
//	               computer.InvalidateConnection, which invalidates the
//	               connection's active and closing bindings and sets their
//	               attached computers waiting and detached ones unknown; then
//	               computer.RefreshUnused for the instance; then
//	               runtimecontrol.EndConnectionCallsTx,
//	               which settles or ends the connection's calls and ends its
//	               running background commands. Callers: the events route,
//	               the credential revocation route, Deregister, the request
//	               steps and services/cleanup.
//	Deregister (deregister.go) the whole of an instance's deregistration:
//	               instance.Deregister, then, holding the instance's
//	               connection that has not ended and the runtime mutation
//	               lock of every Session with an attached computer on the
//	               instance or an open wake request for it, EndConnection with
//	               cause deregistered; computer.Detach with a removed machine
//	               message of each attached computer of a user instance, or
//	               computer.RetireCurrentInstance of each attached computer a
//	               cloud instance serves; FailRequest of each open wake
//	               request for the instance; computer.RefreshUnused. Caller:
//	               the instance deregistration route.
//	ReturnRequestToPending, ReturnTimedOutClaim, FailRequest (request.go) run
//	               computer.ReturnToPending, computer.ReturnTimedOutClaim or
//	               computer.Fail and then EndConnection with cause revoked
//	               for the live connection of the claim credential they
//	               revoked; internal/computer cannot end a connection itself,
//	               since this package imports it. Callers: the request release
//	               and fail routes; services/cleanup for ReturnTimedOutClaim.
//
// INVARIANTS:
//   - The caller holds the connection's row lock FOR UPDATE, so a step
//     commits wholly before or after any request that holds the row FOR
//     SHARE.
//   - AcceptReady takes the runtime mutation lock of each Session it writes
//     a lost-change error to, in Session ID order, before any computer lock.
//     Deregister takes the instance's row, then its connection's, then its
//     Sessions' locks in Session ID order, before EndConnection; it reads
//     the computers and requests it changes again under those locks.
//   - EndConnection then takes the Session runtime mutation lock of each
//     Session with a running background command on the connection, then
//     locks the connection's computers in id order, their bindings and its
//     calls: Session before computer, as in Bridge. A tool report takes its
//     Session lock before the connection, so the two can deadlock; the
//     database then aborts one transaction: the report fails with 503 and
//     its client sends it again, or the Cleanup row waits for the next run.
//
// UPDATE-WITH:
//
//	internal/instance and internal/computer (the writers each step runs);
//	services/computer-hub (the routes that call the steps and the notice
//	  handling of the stream's pod); services/cleanup (the computer phase);
//	internal/runtimecontrol (the call and task writers EndConnection runs).
package hubflow
