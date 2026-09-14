package sandbox

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// ExecutionResultNotificationChannel carries refs-only wake hints from
// Sandbox execution terminal writers to Bridge AwaitSandboxExecution waiters.
// A notification is never correctness state: it is emitted inside the
// transaction that transitions one execution to terminal_unconsumed, so
// commit publishes both and rollback publishes neither, and receivers must
// re-read the durable row before trusting it.
const ExecutionResultNotificationChannel = "tetral_sandbox_execution_result"

// ExecutionResultHint identifies one terminalized Sandbox execution. It is
// the entire notification payload: durable references only, never a result
// body or credentials.
type ExecutionResultHint struct {
	WorkspaceID     string `json:"workspace_id"`
	SessionID       string `json:"session_id"`
	SessionThreadID string `json:"session_thread_id"`
	ToolUseEventID  string `json:"tool_use_event_id"`
}

// Valid reports whether the hint carries its complete durable identity.
func (hint ExecutionResultHint) Valid() bool {
	return hint.WorkspaceID != "" && hint.SessionID != "" &&
		hint.SessionThreadID != "" && hint.ToolUseEventID != ""
}

// ParseExecutionResultHint decodes one notification payload. Malformed or
// identity-incomplete payloads are rejected so they can never wake or
// terminate a consumer.
func ParseExecutionResultHint(payload string) (ExecutionResultHint, bool) {
	var hint ExecutionResultHint
	if err := json.Unmarshal([]byte(payload), &hint); err != nil || !hint.Valid() {
		return ExecutionResultHint{}, false
	}
	return hint, true
}

// NotifyExecutionResultTx emits the execution-result wake hint inside the
// caller's transaction. Call it only when the same transaction actually
// transitioned the execution to terminal_unconsumed; a stale or replayed
// update affecting no row must not notify.
func NotifyExecutionResultTx(ctx context.Context, tx *dbconnect.Tx, hint ExecutionResultHint) error {
	if !hint.Valid() {
		return errors.New("sandbox execution result notification identity is incomplete")
	}
	payload, err := json.Marshal(hint)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, ExecutionResultNotificationChannel, string(payload))
	return err
}
