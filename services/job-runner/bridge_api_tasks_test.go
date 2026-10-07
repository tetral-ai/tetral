package jobrunner

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

func settleBridgeAPIBackgroundTask(t *testing.T, admin *sql.DB, sessionID string, taskID string, terminalStatus string, resultJSON string) {
	t.Helper()
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_background_tasks
		SET status=$3, terminal_result_json=$4, terminal_result_digest=$5,
		    terminal_at='2026-01-01T00:00:30Z', next_poll_at=NULL,
		    reconcile_generation=reconcile_generation+1, updated_at='2026-01-01T00:00:30Z'
		WHERE workspace_id='default' AND session_id=$1 AND task_id=$2 AND status='running'`,
		sessionID, taskID, terminalStatus, resultJSON, runtimecontrol.RequestHash(resultJSON)); err != nil {
		t.Fatalf("settle background task: %v", err)
	}
}

func TestRuntimeTaskNotificationPayloadAcceptsSandboxFailureEnvelope(t *testing.T) {
	payload, err := runtimeTaskNotificationPayloadJSON(&RuntimeTaskNotificationPlan{
		TaskID: "task_failed_delivery", SourceToolUseEventID: "evt_failed_delivery",
	}, "failed", `{"status":"failed","error":{"kind":"sandbox_provider_unavailable","message":"provider unavailable"},"result":{"stdout":{"text":"","truncated":false},"stderr":{"text":"provider unavailable","truncated":false}}}`)
	if err != nil {
		t.Fatalf("runtimeTaskNotificationPayloadJSON: %v", err)
	}
	var decoded struct {
		Status string `json:"status"`
		Stderr struct {
			Text      string `json:"text"`
			Truncated bool   `json:"truncated"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatalf("decode task failure payload: %v", err)
	}
	if decoded.Status != "failed" || decoded.Stderr.Text != "provider unavailable" || decoded.Stderr.Truncated {
		t.Fatalf("task failure payload = %s", payload)
	}
}

func TestPostgreSQLRuntimeDeliveryReplayKeepsGenuineTaskNotificationExhaustionTerminal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", "sesn_task_exhaustion", "thr_task_exhaustion")
	job := RuntimeJob{
		Kind: queue.KindRuntimeInput, WorkspaceID: "default", SessionID: "sesn_task_exhaustion",
		SessionThreadID: "thr_task_exhaustion", RuntimeInputID: "task_notification:task_exhaustion", InputKind: "task_notification",
	}
	seedRuntimeInboxBirthForJob(t, admin, job)
	if _, err := admin.ExecContext(context.Background(), `UPDATE session_runtime_inbox SET status='dead_lettered'
		WHERE workspace_id='default' AND session_id=$1 AND runtime_input_id=$2`, job.SessionID, job.RuntimeInputID); err != nil {
		t.Fatalf("terminalize task notification Inbox: %v", err)
	}
	store := fixtureRuntimeDeliveryStore(dbconnect.NewClientForTesting(runtime), admin, 9090)

	replayed, found, err := store.ReplayRuntimeDeliveryFinalization(context.Background(), job)
	if err != nil || !found || replayed.Status != RuntimeDeliveryRejected || replayed.Retryable || replayed.ErrorKind != "runtime_delivery_exhausted" {
		t.Fatalf("genuine task notification exhaustion replay = %#v/%t/%v", replayed, found, err)
	}
}
