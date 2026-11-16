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
//     close of a detached computer whose close was lost). At most one
//     binding of a computer on a connection is active (preparing, prepared
//     or failed). close_message_id is the session.close issued for it.
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
//   - Table computer_lost_changes: one row per (session.close or
//     session.drives message, drive) whose lost changes a closed or applied
//     report or a ready carried, so each writes one
//     drive_changes_lost_error. Rows are never updated.
//   - The rule of an instance's unused (RefreshUnused), which
//     internal/instance.SetUnused stores.
//   - The repositories of a cloud computer are Session resources
//     (internal/session) whose rows name the computer and the resolved path;
//     this package reads them for the computer object. Likewise it reads the
//     Session's drive bindings (session_drive_bindings) for its
//     preparations, and records each binding's drive sends through
//     internal/drive.
//
// STATES:
//
//	request            pending -> claimed (ClaimRequest) -> fulfilled (FulfilRequests),
//	                   claimed -> pending (ReturnToPending, ReturnTimedOutClaim)
//	                   | failed (Fail),
//	                   pending -> fulfilled (a wake's ready) | expired (Expire)
//	                   | failed (Fail of a wake whose instance is deregistered)
//	association_state  attached -> detached (Detach: removal, archive,
//	                   deletion, deregistration)
//	execution_state    waiting -> preparing -> ready | failed while attached;
//	                   at detachment closing (a session.close was issued),
//	                   closed (no binding ever) or unknown (Detach);
//	                   closing -> closed (StoreClosed) | unknown when its
//	                   connection ends (InvalidateConnection); an attached
//	                   computer -> waiting when its binding's connection ends
//	binding            preparing -> prepared | failed (StoreReport),
//	                   failed -> invalid (PrepareAgain); preparing, prepared,
//	                   failed -> closing (Detach); closing -> closed
//	                   (StoreClosed); preparing, prepared, failed, closing
//	                   -> invalid when its connection ends
//	                   (InvalidateConnection); late_close bindings are
//	                   created closing (IssueLateCloses); a report on any
//	                   other state changes nothing
//
// Preparation: IssuePreparation issues one session.prepare through the
// connection's instruction store when the computer is attached, its current
// instance has an online connection and it holds no active binding there; it
// locks the connection before the computer, the order of the ready route.
// StoreReport applies a stored report in the report's transaction and writes
// its session.error events (WriteSessionErrors, under the Session's runtime
// mutation advisory lock). ToolReportErrors are the session.error events of
// a stored tool report, which computer-hub writes in the report's
// transaction. CheckPrepareSize refuses a computer whose
// session.prepare would exceed PrepareEnvelopeLimit, one SSE event.
//
// Drives: a session.prepare carries the drive set of the computer's latest
// binding on its instance whose prepared report is stored, and the Session's
// drive bindings when it has none (drives.go). A binding change of an idle
// Session sends, through SendDrives, one session.drives with the new bindings
// to every prepared binding of the Session whose connection is live; a
// prepared report whose session.prepare carried other drives than the
// Session's bindings sends one before the report releases the computer's
// waiting calls. The hub accepts applied only for a session.drives issued on
// the connection to that binding (LockDrivesReport); StoreApplied makes the
// binding's drive set that message's and writes its mount and lost-changes
// errors. CheckDrivesFit refuses new bindings with which an attached
// computer's session.prepare would exceed PrepareEnvelopeLimit, alone or with
// its latest prepared drive set or that of a preparation not yet answered. A
// binding transfers its drives while its
// prepared report is stored and its closed one is not (TransferBinding); the
// connections whose ready declared drives.mount and whose such bindings hold
// a drive receive its drive.changed (DriveListeners).
//
// Preparation orders repository ancestors before descendants, preserving
// creation order among eligible entries. Stored/API order is unchanged. This
// order does not permit overwriting an occupied inner checkout destination.
//
// Removal and close: removal.go owns Detach, the one writer of detached
// computers, and its callers Remove (the removal route: an attached computer
// of an idle Session, never its last attached cloud computer, with a removed
// machine message), DetachAll (archive and deletion: no machine message,
// no last-cloud rule) and hubflow.Deregister for a user instance (a removed
// machine message, whatever the Session's run state). Detach locks the
// computer FOR UPDATE, then its binding, then reads the live connection; the
// session.close goes through the connection's instruction store (close.go)
// and records the binding's close_message_id. A computer's open request is
// left to its own
// transitions. A close lost with its connection is issued again by
// IssueLateCloses when the instance's next ready is accepted, as a
// late_close binding that becomes the computer's binding. The hub accepts
// closed only for the recorded instruction (LockClose) and StoreClosed
// writes the terminal cleanup error and, through RecordLostChanges, one
// drive_changes_lost_error per new (close, drive), with the state
// transition; repeated reports of one close write no further events. Closed
// means execution and access ended; residual local files need operator
// cleanup.
//
// Replacement: RetireCurrentInstance, run by hubflow.Deregister for a cloud
// instance, is the one writer that clears an attached computer's current
// instance. The computer keeps its ID and becomes waiting, its Session gets a
// replaced machine message and one session.computer_replaced, and its next
// call records a new request (EnsureRequest).
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
//   - api, through internal/session: ReadRemoval and Remove for DELETE
//     /v1/sessions/{session_id}/computers/{computer_id}; DetachAll for
//     Session archive and deletion.
//   - api, through internal/session: WriteMachineMessageTx for a computer
//     addition.
//   - api, through AddComputers: IssuePreparation for each new computer;
//     only a user computer, which has no repositories, can be online then.
//     internal/session calls CheckPrepareSize once a computer's repositories
//     are resolved.
//   - api, through internal/session: CheckDrivesFit and SendDrives for
//     the Session drive binding routes.
//   - computer-hub: RecordLostChanges, IssuePreparationsOn and
//     IssueLateCloses through hubflow.AcceptReady; LockPreparation,
//     StoreReport and PreparationColdStart for POST
//     /v1/client/session-status; LockClose and StoreClosed for its closed
//     reports, LockDrivesReport and StoreApplied for its applied reports;
//     LockGitTicketBinding for POST /v1/client/git-ticket; TransferBinding
//     for the drive transfer routes; ConnectionDrives and DriveListeners for
//     its drive.changed notices; IssuedBindingComputer for POST
//     /v1/client/drive-sync.
//   - computer-hub: AttachedOn, OpenWakeRequests, Detach and
//     RetireCurrentInstance through hubflow.Deregister for DELETE
//     /v1/instances/{instance_id}.
//   - computer-hub, through internal/instance: the attached computers of an
//     instance give its computer_count, and the bindings prepared on its live
//     connection its binding_count.
//   - computer-hub: ListPendingRequests, ReadRequest and ClaimRequest for the
//     request routes; FulfilRequests through hubflow.AcceptReady;
//     ReturnToPending and Fail only through hubflow.ReturnRequestToPending and
//     hubflow.FailRequest, which end the revoked credential's connection;
//     InvalidateConnection only through hubflow.EndConnection.
//   - cleanup: ReturnTimedOutClaim through hubflow.ReturnTimedOutClaim, and
//     Expire, each after hubflow.LockRequest, for the claim timeout and
//     pending expiry of the computer phase.
//   - hubflow, before it takes its rows and again under them:
//     ConnectionSessions, InstanceSessions, LostChangeSessions and
//     BindingSession, the Sessions a connection's end, a deregistration, a
//     ready or a git ticket fetch changes.
//
// Session and thread running/idle status belongs to internal/runtimecontrol
// (doc.go); this package owns computer association and execution state.
//
// Every writer that changes an input of the unused rule calls RefreshUnused
// in its transaction.
//
// Cold-start transition facts leave their transaction as created requests,
// fulfilled requests and issued bindings. Service boundaries record them only
// after commit; unchanged/replayed transitions carry no new facts. cold_start.go
// records phase endpoints without owning transactions or summary histograms.
//
// UPDATE-WITH:
//
//	internal/storage/postgresql_computer_schema.go (the computer, binding,
//	  request, lost-change and idempotency tables, the machine message
//	  table's step and the request pressure function), internal/storage/postgresql_schema.go
//	  (the machine message table's DDL), internal/schemaidentity,
//	  database/postgresql.json and database/roles.json;
//	internal/protocol (the session.prepare and session.drives payloads and
//	  the session-status report that bindings carry);
//	internal/drive (the binding drive sets);
//	services/api/README.md and services/computer-hub/README.md (the routes
//	  these writers serve).
package computer
