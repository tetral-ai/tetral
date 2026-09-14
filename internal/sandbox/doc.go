// Package sandbox defines provider-neutral Sandbox identities, durable resource
// declarations, normalized provider outcomes, and shared materialization data.
//
// Provider lifecycle and command execution are owned by the Sandbox Service.
// This package supplies the stable vocabulary used by the Session store and the
// Daytona adapter without exposing provider SDK types to Bridge, Runtime, Queue,
// or public APIs.
//
// Durable lifecycle state lives in session_sandbox_bindings and
// sandbox_lifecycle_operations. Session creation does not allocate a provider
// resource. An approved Sandbox Tool Use drives lazy activation and
// materialization through Queue jobs; release is produced only by Session
// deletion or displacement of a recorded provider handle.
//
// Execution-result wake hints share this boundary: the
// tetral_sandbox_execution_result channel and its refs-only ExecutionResultHint
// payload (result_notification.go) are produced by every transaction that
// terminalizes an execution and consumed by Bridge AwaitSandboxExecution
// waiters as wake signals only — never as result authority.
//
// UPDATE-WITH: services/sandbox/provider_adapter.go,
// services/sandbox/lifecycle_store.go, services/sandbox/execution_store.go, and
// internal/session/materialization_snapshot.go.
package sandbox
