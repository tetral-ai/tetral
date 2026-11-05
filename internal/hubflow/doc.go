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
//	               Caller: the ready route, after LockReadySessions.
//	LockSessions, LockReadySessions, LockPredecessorSessions,
//	RetryLockSetChange (sessions.go) the Session locks a step's transaction
//	               takes before any row, and the route's rerun when the
//	               second read finds a Session it does not hold.
//	EndConnection (end.go) the single end step of a connection, for every cause:
//	               lease_expired (its lease passed: EndExpiredConnection for
//	               the Cleanup computer phase, or a new connection that
//	               replaces it), revoked (the live connection's credential
//	               is revoked: RevokeCredential and the request steps) and
//	               deregistered (Deregister). It reads the connection's
//	               Sessions again; then runs instance.EndConnection, which
//	               writes the end, deletes the replay ledger and notices the
//	               stream's pod; then computer.InvalidateConnection, which
//	               invalidates the connection's active and closing bindings
//	               and sets their attached computers waiting and detached
//	               ones unknown; then computer.RefreshUnused for the
//	               instance; then runtimecontrol.EndConnectionCallsTx, which
//	               settles or ends the connection's calls and ends its
//	               running background commands. Callers: the events route
//	               (after LockPredecessorSessions), the credential revocation
//	               route (RevokeCredential), Deregister, the request steps and
//	               services/cleanup (EndExpiredConnection).
//	Deregister (deregister.go) the whole of an instance's deregistration:
//	               the locks of the Sessions on the instance and on its
//	               connection that has not ended, instance.Deregister, that
//	               connection's lock, the Sessions read again, then
//	               EndConnection with cause deregistered; computer.Detach
//	               with a removed machine message of each attached computer
//	               of a user instance, or computer.RetireCurrentInstance of
//	               each attached computer a cloud instance serves;
//	               FailRequest of each open wake request for the instance;
//	               computer.RefreshUnused. Caller: the instance
//	               deregistration route.
//	LockRequest, ReturnRequestToPending, ReturnTimedOutClaim, FailRequest
//	               (request.go): LockRequest takes a request's locks; the
//	               others run computer.ReturnToPending,
//	               computer.ReturnTimedOutClaim or computer.Fail and then
//	               EndConnection with cause revoked for the live connection
//	               of the claim credential they revoked; internal/computer
//	               cannot end a connection itself, since this package imports
//	               it. Callers: the request release and fail routes;
//	               services/cleanup for LockRequest, ReturnTimedOutClaim and,
//	               after LockRequest, computer.Expire.
//
// INVARIANTS:
//   - Lock order. A transaction takes the runtime mutation locks of all the
//     Sessions it changes first, in Session ID order (LockSessions), and no
//     Session lock after any row lock. Then come the instance's row, its
//     credentials' and its connection's, in that order. A Session's own rows (its
//     computers, bindings, threads and dispatch records) are locked only
//     under that Session's lock, so they cannot close a cycle among
//     themselves. Bridge and the client report routes name their Session
//     and take its lock first.
//   - A step that finds its Sessions through a connection or an instance
//     reads them without locks, locks them, takes its rows and reads them
//     again: the end of a connection (the Sessions of its bindings in state
//     preparing, prepared, failed or closing, of its calls and of its
//     running background commands), a deregistration (also those with a
//     computer or request on the instance), a ready (those with a computer
//     or request on the instance or a lost change it records) and a
//     computer request's release, failure or expiry (also the request's own
//     Session; LockRequest). The
//     connection's row lock, held FOR UPDATE by the end, keeps new bindings,
//     calls and commands off it. A Session found only on the second read
//     ends the transaction with LockSetChangedError: a route runs it again
//     up to three times (RetryLockSetChange) and then answers 503; Cleanup
//     retries at its next run.
//   - The caller holds the connection's row lock FOR UPDATE, so a step
//     commits wholly before or after any request that holds the row FOR
//     SHARE.
//
// UPDATE-WITH:
//
//	internal/instance and internal/computer (the writers each step runs);
//	services/computer-hub (the routes that call the steps and the notice
//	  handling of the stream's pod); services/cleanup (the computer phase);
//	internal/runtimecontrol (the call and task writers EndConnection runs).
package hubflow
