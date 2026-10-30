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
//     and error_code/error_message describe the latest preparation.
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
//   - The repositories of a cloud computer are Session resources
//     (internal/session) whose rows name the computer and the resolved path;
//     this package reads them for the computer object.
//
// STATES:
//
//	association_state  attached -> detached (removal, archive)
//	execution_state    waiting -> preparing -> ready | failed, then
//	                   closing -> closed | unknown after detachment
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
//   - api and computer-hub: WriteMachineMessageTx for each computer change the
//     Runtime must learn (an addition; removals and replacements as their
//     routes land).
//   - computer-hub, through internal/instance: the attached computers of an
//     instance give its computer_count.
package computer
