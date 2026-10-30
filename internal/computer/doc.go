// Package computer owns a Session's computers: the machines its tools run
// on. A cloud computer is defined by an Environment and gets a cloud instance
// from a Controller; a user computer is defined by a registered user instance.
// Each write runs in its caller's Workspace-scoped transaction.
//
// OWNS:
//   - Table computers: one row per computer of a Session, kept as long as the
//     Session. storage_sequence orders a Session's computers by addition.
//     networking, working_directory and drive_exclude are fixed when the
//     computer is added; a cloud computer copies its Environment's networking
//     then. current_instance_id is a user computer's instance_id and a cloud
//     computer's instance once one serves it. cwd, connection_id, binding_id
//     and error_code/error_message describe the latest preparation;
//     last_failed_code is the code of the failure since the last success, so
//     a repeated failure writes no second computer_preparation_error.
//   - Table computer_bindings: one row per preparation of a computer on one
//     connection, kind prepare for a session.prepare (late_close for the
//     close of a Session a machine still holds). At most one binding of a
//     computer on a connection is active (preparing, prepared or failed).
//     created_at is the database clock of the transaction that issues the
//     session.prepare and reported_at that of the one that stores its report;
//     the session_computer cold start is their difference.
//   - Table computer_idempotency_records: one row per (principal credential,
//     route, Idempotency-Key) of Session creation or a computer addition,
//     holding the fingerprint, with repository tokens hashed, and the Session
//     and computer the request produced.
//   - Table session_machine_messages: one row per machine message, written by
//     WriteMachineMessageTx in the transaction that changes the computer, with
//     the change (added, removed, replaced) and the computer's Runtime row at
//     that moment; its runtime_input_id names the machine_message input of the
//     Session's main thread that carries it to the Runtime. Rows are never
//     updated.
//   - Table computer_requests: one row per Controller request of a cloud
//     computer without a usable instance, new (no current instance) or wake
//     (an offline current instance). At most one request of a computer is
//     open (pending or claimed); every waiting call shares it. Each
//     transition notifies RequestEventChannel, which every hub pod turns into
//     request stream events.
//   - The rule of an instance's unused (RefreshUnused), which
//     internal/instance.SetUnused stores.
//   - The repositories of a cloud computer are Session resources
//     (internal/session) whose rows name the computer and the resolved path;
//     this package reads them for the computer object.
//
// STATES:
//
//	request            pending -> claimed (ClaimRequest) -> fulfilled (FulfilRequests),
//	                   claimed -> pending (ReturnToPending) | failed (Fail),
//	                   pending -> fulfilled (a wake's ready) | expired (Expire)
//	                   | failed (Fail of a wake whose instance is deregistered)
//	association_state  attached -> detached (removal, archive)
//	execution_state    waiting -> preparing -> ready | failed, then
//	                   closing -> closed | unknown after detachment
//	binding            preparing -> prepared | failed (StoreReport),
//	                   failed -> invalid (PrepareAgain); a report on any other
//	                   state changes nothing
//
// Preparation: IssuePreparation issues one session.prepare through the
// connection's instruction store when the computer is attached, its current
// instance has an online connection and it holds no active binding there; it
// locks the connection before the computer, the order of the ready route.
// StoreReport applies a stored report in the report's transaction and writes
// its session.error events (WriteSessionErrors, under the Session's runtime
// mutation advisory lock). CheckPrepareSize refuses a computer whose
// session.prepare would exceed PrepareEnvelopeLimit, one SSE event.
//
// Machine messages: WriteMachineMessageTx is the one writer of
// session_machine_messages; it reads the computer through ReadRuntimeRow,
// which projects any computer of the Session, removed ones included.
//
// A computer is created attached and waiting by AddComputers, the one writer
// of new computers. The read route serves an attached computer and a detached
// one whose closure is closing or unknown.
//
// CALLERS:
//   - api, through internal/session: CheckEntries and AddComputers at Session
//     creation and at POST /v1/sessions/{session_id}/computers, ListAttached
//     for the Session object, Get for the computer read route, and the
//     idempotency records of both writes. The add route alone writes an added
//     machine message; Session creation writes none.
//   - api, through internal/session: WriteMachineMessageTx for a computer
//     addition.
//   - api, through AddComputers: IssuePreparation for each new computer;
//     only a user computer, which has no repositories, can be online then.
//     internal/session calls CheckPrepareSize once a computer's repositories
//     are resolved.
//   - computer-hub: IssuePreparationsOn through hubflow.AcceptReady after
//     fulfilment; LockPreparation, StoreReport and PreparationColdStart for
//     POST /v1/client/session-status; LockGitTicketBinding for POST
//     /v1/client/git-ticket.
//   - computer-hub, through internal/instance: the attached computers of an
//     instance give its computer_count, and the bindings prepared on its live
//     connection its binding_count.
//   - computer-hub: ListPendingRequests, ReadRequest and ClaimRequest for the
//     request routes; FulfilRequests through hubflow.AcceptReady;
//     ReturnToPending and Fail only through hubflow.ReturnRequestToPending and
//     hubflow.FailRequest, which end the revoked credential's connection.
//
// Every writer that changes an input of the unused rule calls RefreshUnused
// in its transaction.
package computer
