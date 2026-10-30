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
// A computer is created attached and waiting by AddComputers, the one writer
// of new computers. The read route serves an attached computer and a detached
// one whose closure is closing or unknown.
//
// CALLERS:
//   - api, through internal/session: CheckEntries and AddComputers at Session
//     creation and at POST /v1/sessions/{session_id}/computers, ListAttached
//     for the Session object, Get for the computer read route, and the
//     idempotency records of both writes.
//   - computer-hub, through internal/instance: the attached computers of an
//     instance give its computer_count.
package computer
