package integration

import (
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
)

func completionMailEnvelope(taskName string, sender string, payload string) string {
	return "Message Type: FINAL_ANSWER\nTask name: " + taskName + "\nSender: " + sender + "\nPayload:\n" + payload
}

func completionMailTestStore(t *testing.T, runtime *sql.DB) *agentruntimebridge.PostgreSQLBridgeAPIStore {
	t.Helper()
	store := agentruntimebridge.NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	return store
}

func completionTestSessionID(suffix string) string {
	return "sesn_bridge_child_finish_idle_" + suffix
}
