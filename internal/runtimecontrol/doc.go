// Package runtimecontrol holds the durable Runtime control rules that Bridge
// and Job Runner share; README.md describes the package and its lock graph.
//
// OWNS:
//   - The Session's status in session_runtime_status.status: running while at
//     least one of its threads, of any role, is running or rescheduling, idle
//     when none is (a thread idle with requires_action counts as idle).
//
// STATE MACHINE (session_runtime_status.status):
//
//	(none) -> idle | running   Session creation's first row ((*postgresqlTransaction).CreateSession in internal/session)
//	idle   -> running          RefreshSessionStatusTx, writing the Session's session.status_running
//	running -> idle            RefreshSessionStatusTx, writing the Session's session.status_idle and arming cleanup_after
//	(any)  -> idle             appendRuntimeTerminatedStatusTx when the main thread terminates the Session
//
// Callers of RefreshSessionStatusTx, each after it changes a thread's status
// and holding the Session's runtime mutation lock:
//   - Bridge: WriteEvent of a running event, FinishIdle, the reschedule of a
//     request (WriteRequestEnd), a child thread's close.
//   - Job Runner: the pod-loss settlement, a failed delivery, the recovery
//     that sets a thread running again, an MCP discovery input.
//   - This package: a child thread's termination.
//
// Inputs that never start a run (queue.IsNonStartingRuntimeInputKind):
// AppendNonStartingInputTx is the one writer of a new Inbox row of such a kind,
// queued, unbound and without events, together with its runtime_input job. Its
// caller is internal/computer.WriteMachineMessageTx (api). Such a row may be
// accepted with no binding;
// HandBackQuiescedRuntimeInputsTx leaves an accepted one as it is, and
// HandBackRuntimeInputsTx (pod loss) hands it back with a new job built by
// RuntimeInputEnqueueRequest and hands back a delivering one with its own job.
//
// INVARIANTS:
//   - No other writer sets session_runtime_status.status or writes the
//     Session's session.status_running or session.status_idle (the main
//     thread's own status events carry its thread and stay off the primary
//     stream); TestSessionStatusHasOneWriter in integration/static enforces it.
//   - A terminated Session's status is never changed.
//
// UPDATE-WITH:
//   - session_status.go (RefreshSessionStatusTx), termination.go, events.go
//     (PublicProjection of the main thread's status events).
//   - non_starting_input.go (AppendNonStartingInputTx), input_custody.go,
//     input.go, internal/queue (the input kinds that never start a run) and
//     services/job-runner/runtime_delivery.go (their preparation).
//   - services/bridge (bridge_api_events.go, bridge_api_settlement.go,
//     bridge_api_children.go) and services/job-runner (runtime_pod_lost.go,
//     runtime_delivery.go, mcp_discovery_input.go).
//   - internal/session (the idle check of operations allowed only while the
//     Session is idle) and integration/static/session_status_writer_static_test.go.
package runtimecontrol
