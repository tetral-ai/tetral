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
// EnsureIdleCleanupScheduledTx in idle_cleanup.go restores a missing cleanup
// timer after terminal input settlement without changing Session status. Its
// callers are Job Runner's discovery/delivery finalizers and Bridge's interrupt
// input commit; it retains the current idle event and binding as the fence.
// When no status event has ever been published, it writes the first aggregate
// idle only after all cleanup guards hold, retaining the original idle_since.
//
// Inputs that never start a run (queue.IsNonStartingRuntimeInputKind):
// AppendNonStartingInputTx is the one writer of a new Inbox row of such a kind,
// queued, unbound and without events, together with its runtime_input job. Its
// caller is internal/computer.WriteMachineMessageTx (api). Such a row may be
// accepted with no binding;
// HandBackQuiescedRuntimeInputsTx (a Runtime's release) and
// HandBackCleanedRuntimeInputsTx (an idle cleanup the Runtime accepted) leave
// an accepted one as it is and return a delivering one to queued with its job;
// HandBackRuntimeInputsTx (pod loss) hands it back with a new job built by
// RuntimeInputEnqueueRequest and hands back a delivering one with its own job.
//
// Machine-bound tool calls: a call of exec_command, edit or view_image has a
// session_runtime_tool_results row of tool kind machine_tool, a write_stdin
// one of machine_control. The row is pending until settled, then
// terminal_unconsumed with its settlement record, then consumed once a result
// event of the call commits. Each downstream tool instruction of a call has a
// session_tool_dispatches record (tool.execute, tool.stdin or tool.cancel),
// issued under its message_id once it is ready. Writers, one per transition:
//
//	row pending -> terminal_unconsumed    SettleToolCallTx, inside every writer below that settles; it
//	                                      issues the call's resultnotify notice in the same transaction
//	row terminal_unconsumed -> consumed   ConsumeToolCallTx: Bridge's interrupt closeout
//	(none) -> execute waiting_instance, waiting_preparation or ready, or the row settled at once
//	                                      AdmitToolCallTx: Bridge AdmitToolCall
//	execute waiting_instance -> waiting_preparation
//	                                      AwaitPreparationTx: internal/computer.FulfilRequests (computer-hub ready)
//	execute waiting_preparation -> ready, or ended when the CLI lacks the capability
//	                                      ReleasePreparedTx: internal/computer.StoreReport of a prepared report
//	execute waiting_preparation -> ended  EndPreparationWaitersTx: internal/computer.StoreReport of a failed report
//	execute waiting_instance -> ended     EndRequestWaitersTx: internal/computer.Fail and Expire
//	execute waiting_* -> ended            CancelToolCallTx: Bridge CancelToolCall (turn end) and interrupt closeout
//	(none) -> stdin ready                 AdmitToolControlTx: Bridge AdmitToolControl
//	(none) -> cancel ready, or ended when its route is gone
//	                                      CancelToolCallTx, and CancelRunningTaskTx for a stored running
//	                                      report at Bridge's interrupt closeout; at most one per task
//
// Every writer that moves a record or issues one locks the computer row first
// (FOR NO KEY UPDATE), then the row and the record; in Bridge after its
// thread, route and Session locks, in computer-hub after its connection
// locks. Connection reads under that lock are unlocked
// (instance.ReadOnlineConnection, instance.ReadLiveConnection).
// ComputerRouting is the seam to internal/computer, which imports this
// package: admission reaches a computer's request and preparation writers
// through it, and Bridge passes internal/computer.AdmissionRouting.
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
//   - tool_dispatch.go and tool_settlement.go, the session_tool_dispatches
//     DDL in internal/storage/postgresql_runtime_schema.go, internal/computer
//     (request.go, binding.go, routing.go) and services/bridge
//     (bridge_api_machine_tools.go, bridge_api_interrupt_tools.go).
package runtimecontrol
