package runtimecontrol

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
)

// AppendNonStartingInputTx is the one writer of a new Inbox row of an input
// kind that never starts a run (queue.IsNonStartingRuntimeInputKind). In the
// caller's transaction it inserts the row queued, unbound and without events,
// and enqueues its runtime_input job; Job Runner delivers the input only to a
// Runtime that already holds the Session.
func AppendNonStartingInputTx(ctx context.Context, tx *dbconnect.Tx, workspaceID, sessionID, sessionThreadID, runtimeInputID, inputKind string, now time.Time) error {
	if !queue.IsNonStartingRuntimeInputKind(inputKind) {
		return PreparationError{Kind: "runtime_inbox_invariant", Message: "input kind starts a run", Retryable: false}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
		workspace_id, session_id, session_thread_id, runtime_input_id, input_kind, event_ids_json, status, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, '[]', 'queued', $6, $6)`,
		workspaceID, sessionID, sessionThreadID, runtimeInputID, inputKind, now); err != nil {
		return err
	}
	request, err := RuntimeInputEnqueueRequest(workspaceID, sessionID, AcceptedRuntimeInput{
		SessionThreadID: sessionThreadID, RuntimeInputID: runtimeInputID, InputKind: inputKind, EventIDsJSON: "[]",
	}, now)
	if err != nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, tx, request)
	return err
}

func nonStartingInputQueuePayloadJSON(workspaceID, sessionID, sessionThreadID, runtimeInputID, inputKind string) ([]byte, error) {
	return json.Marshal(InputQueuePayload{
		WorkspaceID: workspaceID, SessionID: sessionID, SessionThreadID: sessionThreadID,
		RuntimeInputID: runtimeInputID, EventIDs: []string{}, InputKind: inputKind,
	})
}
