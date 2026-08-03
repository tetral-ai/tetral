// Package sessionevent admits public session events and appends them to the
// durable event ledger. The admission transaction validates the Session and
// target thread, allocates ledger sequences, records idempotency, settles
// approval decisions, and enqueues each runtime-input-bearing segment.
//
// Runtime-input Queue jobs are direct projections of committed ledger rows.
// Their payload names the workspace, Session, thread, source event ids, ledger
// sequence range, and input kind. Queue enqueue is part of the same transaction
// as the event append, so Runtime never receives input that lacks durable
// custody and a replayable source identity.
//
// The package also owns the public admission bounds for event count and file
// attachments. Invalid input is rejected before any ledger, idempotency, or
// Queue row is written.
package sessionevent
