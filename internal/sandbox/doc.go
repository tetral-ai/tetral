// Package sandbox owns the provider-neutral session sandbox lifecycle: it turns
// a claimed preparation attempt into a ready, resource-materialized machine and
// carries that machine through wake, replace, release, and failure settlement.
//
// OWNS:
//   - sandboxes: the per-session machine row (one live sandbox per session) and
//     its status / cleanup_status / machine_was_usable columns.
//   - session_preparations: the per-attempt preparation ledger (claim, waiting,
//     ready, failed, superseded).
//   - The provider-neutral LifecycleProvider, resource-preparer, and cleanup
//     interfaces. Provider vocabulary is translated to these types at
//     internal/sandbox/driver; nothing above this package sees provider states.
//
// STATE MACHINE (sandboxes.status; the authoritative transition table, terminal-
// status derivation, and the machine_was_usable invariant live at the status
// writers in postgresql_store.go):
//
//	creating|resuming -> active -> stopped|archived -> released, plus failed.
//
// Cold return re-enters an existing row: a live row wakes (resuming->active) or
// is replaced; a released row is always replaced; a failed row that is still
// checkpoint-archiving defers. session_prepare.go holds the closed decision table.
//
// INVARIANTS:
//   - Cross-executor fence: before any successor job (a re-driven session_prepare
//     or a session_delete_cleanup) can lease a session partition, every remote
//     command dispatched by the prior lease holder has provably terminated on the
//     machine. The worst-case bound is measured from the last successful lease
//     renewal, so it holds for crash, hang, and heartbeat-blip alike; there is no
//     per-resource removal lease or epoch. This is what lets a superseded command
//     never erase a successor's files.
//   - Deterministic provider Name = sandbox_id: a crash-orphaned, not-yet-recorded
//     machine is rediscoverable by name, so the stale-startup reconciler can adopt
//     or release it instead of leaking it.
//   - The stale-startup reconciler covers both creating and resuming rows and is
//     two-phase: it probes provider or by-name state before destroying anything,
//     and it skips sessions whose lifecycle_state = 'deleted' (those machines have
//     a single owner in session_delete_cleanup).
//   - REPLACE obeys DELETE-OLD-FIRST: a recorded provider handle is provider-
//     deleted (idempotent; not-found counts as success) before the new create
//     reuses Name = sandbox_id and before the row's handle is overwritten, so a
//     crash between delete and overwrite re-deletes on retry and never loses a
//     live machine's identity.
//
// UPDATE-WITH:
//   - internal/sandbox/postgresql_store.go (status writers, terminal-status
//     derivation, machine_was_usable).
//   - internal/sandbox/session_prepare.go (readiness stages, cold-return routing).
//   - internal/sandbox/driver/provider.go (provider-state translation, release).
package sandbox
