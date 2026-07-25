// Package sessionevent admits public session events (user messages, interrupts,
// tool confirmations), persists them, and enqueues the runtime-input work that
// carries each admitted runtime-input-bearing event into a prepared session.
//
// OWNS:
//   - session_events.preparation_attempt_id: the runtime-input birth column that
//     binds a runtime-input-bearing event row to the session preparation attempt
//     that must consume it. It is NULL for events that carry no runtime input.
//   - queue.KindRuntimeInput jobs: each job payload carries a copy of that birth
//     id (runtimeInputQueuePayload.PreparationAttemptID).
//   - The events-admission bounds MaxProviderRequestAttachments and
//     MaxEventsPerRequest, enforced as hard rejections before any row is written.
//
// STATE MACHINE: session_events.preparation_attempt_id
//
//	transition                          writer / site
//	unset -> unset                      admission INSERT of a non-runtime-input
//	                                    event; the column stays NULL for life.
//	unset -> stamped(latest attempt)    admission INSERT of a runtime-input event
//	                                    stamps the latest preparation attempt id
//	                                    in the same row write.
//	stamped(failed) -> stamped(fresh)   admission re-drive: when the latest
//	                                    preparation is failed and new runtime
//	                                    input arrives, a fresh preparation attempt
//	                                    is allocated and the just-inserted rows
//	                                    are UPDATEd to it inside the same
//	                                    transaction (stampRuntimeInputEventBirthTx).
//
//	Writer: the public admission transaction (AppendClientEvents) only.
//	Readers: runtimeInputSegments and enqueueRuntimeInputJobs copy the value into
//	         segment boundaries and queue payloads; they never write it.
//
// INVARIANTS:
//   - The birth column has exactly one runtime writer: the public admission
//     transaction. Both the INSERT stamp and the failed-preparation re-drive
//     UPDATE run inside that one transaction; no other request path assigns it.
//     The sole exception is the one-time schema backfill migration step
//     session_events_preparation_attempt_id_backfill
//     (internal/storage/postgresql_migrator.go), which stamps legacy pre-column
//     rows once at migration time and never runs on the admission path.
//   - Enqueue copies the birth id into every runtime-input job payload and never
//     assigns the column. An empty birth id on a runtime-input segment is a hard
//     error at enqueue, not a fallback that fills the value in.
//   - The re-drive branch writes the freshly-allocated attempt as a
//     post-allocation UPDATE within the same admission transaction, so an
//     admitted runtime-input row is never enqueued bearing a stale failed-attempt
//     birth id.
//
// UPDATE-WITH:
//   - internal/sessionevent/postgresql_store.go (admission INSERT,
//     stampRuntimeInputEventBirthTx, createFreshPreparationAttemptForNewInput,
//     enqueueRuntimeInputJobs, runtimeInputSegments).
package sessionevent
