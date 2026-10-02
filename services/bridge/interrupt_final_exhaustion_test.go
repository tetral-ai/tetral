package agentruntimebridge

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func enqueueInterruptExhaustionJob(
	t *testing.T,
	store *queue.PostgreSQLQueueStore,
	sessionID string,
	threadID string,
	inputID string,
	inputKind string,
	eventID string,
	sequence int64,
	maxAttempts int,
	now time.Time,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": inputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": inputKind,
	})
	if err != nil {
		t.Fatalf("marshal runtime input %s: %v", inputID, err)
	}
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, inputID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: maxAttempts, Now: now,
	}); err != nil {
		t.Fatalf("enqueue runtime input %s: %v", inputID, err)
	}
}
