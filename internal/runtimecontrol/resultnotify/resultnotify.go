// Package resultnotify carries the wake notice of a settled tool call from the
// transaction that settles it to Bridge's waiters. It imports only the database
// client, so every settling writer can import it.
package resultnotify

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/tetral-ai/tetral/internal/dbconnect"
)

// ExecutionResultNotificationChannel carries refs-only wake hints from the
// writers that settle a tool call, a Sandbox execution's or a machine-bound
// call's, to Bridge's waiters. A notice is never result state: it is issued
// in the transaction that moves the call's row to terminal_unconsumed, so
// commit publishes both and rollback neither, and a receiver reads the row
// again before trusting it.
const ExecutionResultNotificationChannel = "tetral_tool_execution_result"

// ExecutionResultHint names one settled call. It is the whole notice:
// durable references only, never a result body or a credential.
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

// ParseExecutionResultHint decodes one notice payload. A malformed or
// incomplete payload is refused, so it never wakes a waiter.
func ParseExecutionResultHint(payload string) (ExecutionResultHint, bool) {
	var hint ExecutionResultHint
	if err := json.Unmarshal([]byte(payload), &hint); err != nil || !hint.Valid() {
		return ExecutionResultHint{}, false
	}
	return hint, true
}

// NotifyExecutionResultTx issues the hint's notice in the caller's
// transaction. Call it only when that transaction settled the call; an update
// that settled nothing issues none.
func NotifyExecutionResultTx(ctx context.Context, tx *dbconnect.Tx, hint ExecutionResultHint) error {
	if !hint.Valid() {
		return errors.New("tool execution result notice identity is incomplete")
	}
	payload, err := json.Marshal(hint)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, $2)`, ExecutionResultNotificationChannel, string(payload))
	return err
}
