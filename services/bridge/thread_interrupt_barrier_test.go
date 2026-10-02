package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func seedActiveInterruptQueueCustody(
	t *testing.T,
	db *sql.DB,
	sessionID string,
	threadID string,
	runtimeInputID string,
	eventID string,
	sequence int64,
) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"workspace_id": "default", "session_id": sessionID, "session_thread_id": threadID,
		"runtime_input_id": runtimeInputID, "event_ids": []string{eventID},
		"sequence_from": sequence, "sequence_to": sequence, "input_kind": "interrupt_control",
	})
	if err != nil {
		t.Fatalf("marshal interrupt Queue custody: %v", err)
	}
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspace.DefaultID, Kind: queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(workspace.DefaultID, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(workspace.DefaultID, sessionID, runtimeInputID),
		PayloadVersion: 1, PayloadJSON: payload, Priority: 100,
		MaxAttempts: queue.DefaultMaxAttempts, Now: time.Now().UTC().Add(-time.Second),
	}); err != nil {
		t.Fatalf("seed active interrupt Queue custody: %v", err)
	}
}
