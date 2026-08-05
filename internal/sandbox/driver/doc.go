// Package driver owns the Daytona sandbox driver: the provider-specific surface
// that turns tetral sandbox lifecycle and tool calls into Daytona SDK and
// in-sandbox helper operations, and reads the helper's results back.
//
// OWNS:
//   - Sandbox lifecycle calls: DaytonaLifecycleProvider (provider.go) —
//     CreateSandbox, StartSandbox, CheckBaseTemplateHealth,
//     PrepareBaseDirectories, InspectState, ReleaseSandbox against the Daytona
//     SDK; creation-time network-policy translation, auto stop/archive/delete
//     interval policy, and the Daytona SDK error ->
//     sandbox.ProviderError classification (mapDaytonaError).
//   - Helper transport: DaytonaHelperExecutor (daytona.go) — PrepareTool,
//     SubmitPreparedTool, ObserveForegroundTool, ReadCommandResult,
//     SendCommandInput, CancelCommand, CheckHealth,
//     RunDaytonaCommand, StageDaytonaFile. Each helper subcommand stages
//     one payload file under payloadRootPath, chowns it root-owned and chmods it
//     0600 (its directory 0700), then runs it via
//     `sudo -n -u root <helperPath> <sub> --payload <path>`; the reply stdout is
//     parsed as a protocol.Envelope, has its provider metadata stripped, and, for
//     a foreground task, returns durable observation state that callers persist
//     before advancing it through bounded ObserveForegroundTool calls.
//   - Detached-task settlement authority: terminalStatusFromResult,
//     synthesizeHelperBackgroundTask, and runningTaskIDFromResult — turning a
//     running helper envelope into a BackgroundTask handle for later
//     poll/stdin/cancel, and a settled envelope into a terminal status.
//   - Supporting driver surfaces in the same package: idle-output capture
//     (daytona_output_capture.go) invokes capture mode through the same
//     passwordless sudo root-helper form; memory projection refresh
//     (memory_projection.go), GitHub repository preparation
//     (github_repository.go), helper payload and limit construction
//     (helper_payload.go), and the provider artifact reference builder
//     (artifact_builder.go).
//
// SETTLEMENT (a helper result envelope -> terminal status; terminalStatusFromResult):
//
//	envelope shape                                     terminal status
//	------------------------------------------------   ---------------
//	status = running                                   "" (not settled; keep polling)
//	result.cancelled = true                            cancelled
//	result.timed_out = true                            expired
//	result.exit_code = 0                              completed
//	result.exit_code != 0, or result.signal set        failed
//	unparseable JSON, or none of the above             "" (no terminal signal)
//
// Precedence is top-to-bottom: cancelled outranks timed_out, which outranks the
// exit_code/signal split.
//
// helper_failure synthesis: the helper process exit code, not the envelope,
// decides whether an authoritative result exists. executeHelper returns a
// HelperFailureError (helper_failure.go) before any envelope in two cases — a
// non-zero helper exit code, and stdout that fails envelope validation — and the
// caller (Sandbox Service) maps that marker to a helper_failure tool result. An
// exit-0 envelope whose status is error is the opposite case: authoritative,
// not a helper_failure.
//
// INVARIANTS:
//   - The helper is invoked as root (helperUser) through sudo; the driver never
//     runs tool or capture logic as the workspace runtime user. Root ownership
//     lets tool commands read and unlink their payload before dropping
//     privilege, while capture mode retains root for descriptor-safe reads.
//   - Once the per-invocation payload directory is created, every return path
//     removes it: the explicit deletes on the upload and permission-command
//     failures, and the deferred delete around the helper run.
//   - A non-zero helper exit is never surfaced as a tool result; it becomes a
//     HelperFailureError, kept distinct from an exit-0 status = error envelope.
//   - Provider-metadata keys (provider_sandbox_id, provider_session_id,
//     background_task, and their siblings) are stripped recursively from every
//     parsed result before it leaves the driver (stripProviderMetadataValue);
//     separately, an envelope whose top level carries a reserved field
//     (error_kind, result_json, terminal_status, background_task) is rejected as
//     invalid.
//   - terminalStatusFromResult returns "" for a running envelope, so a task that
//     is still running is never recorded as settled.
//   - Foreground observation accumulates bounded stdout/stderr across calls;
//     the provider command is submitted once and its durable observation state
//     is advanced without replaying the command.
//   - InspectState returns the provider-native state. DaytonaAdapter owns the
//     single fail-closed conversion from that state to an execution outcome.
//
// UPDATE-WITH:
//   - internal/sandbox/driver/provider.go (DaytonaLifecycleProvider methods and
//     mapDaytonaError) and services/sandbox/provider_adapter.go (the native
//     Daytona-state to execution-outcome table).
//   - internal/sandbox/driver/daytona.go (DaytonaHelperExecutor; executeHelper
//     payload staging; SubmitPreparedTool; ObserveForegroundTool; terminalStatusFromResult;
//     synthesizeHelperBackgroundTask; runningTaskIDFromResult).
//   - internal/sandbox/driver/helper_failure.go (HelperFailureError).
//   - internal/sandbox/helper/protocol/envelope.go and payload.go (the envelope
//     status/tool/result shape and schema_version the driver parses).
//   - internal/sandbox/helper/internal/task/exec.go and supervisor.go (the
//     exit_code/signal/timed_out/cancelled result fields settlement reads).
//   - services/sandbox/tool_execution_runner.go (consumes BackgroundTask and
//     TerminalStatus and synthesizes the helper_failure tool result).
package driver
