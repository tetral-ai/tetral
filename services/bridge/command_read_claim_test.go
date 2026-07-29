package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc"
)

func TestReadCommandResultOwnerIdentityStableAndDistinct(t *testing.T) {
	requestID, key, payload := readCommandResultOwnerIdentity("evt_read_owner_a", "task_read_owner", false, 0)
	replayRequestID, replayKey, replayPayload := readCommandResultOwnerIdentity("evt_read_owner_a", "task_read_owner", false, 0)
	if requestID == "" || requestID != replayRequestID || key != replayKey || payload != replayPayload {
		t.Fatalf("same logical poll identity = (%q, %q, %q) then (%q, %q, %q); want stable", requestID, key, payload, replayRequestID, replayKey, replayPayload)
	}

	tests := []struct {
		name     string
		sourceID string
		taskID   string
		deferred bool
		max      int
	}{
		{name: "source event", sourceID: "evt_read_owner_b", taskID: "task_read_owner"},
		{name: "task", sourceID: "evt_read_owner_a", taskID: "task_read_owner_other"},
		{name: "deferred", sourceID: "evt_read_owner_a", taskID: "task_read_owner", deferred: true},
		{name: "max output", sourceID: "evt_read_owner_a", taskID: "task_read_owner", max: 64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nextRequestID, nextKey, nextPayload := readCommandResultOwnerIdentity(tc.sourceID, tc.taskID, tc.deferred, tc.max)
			if nextKey == key {
				t.Fatalf("distinct logical poll key = %q; want different from %q", nextKey, key)
			}
			if tc.sourceID == "evt_read_owner_a" && nextRequestID != requestID {
				t.Fatalf("request id = %q; want source-derived %q", nextRequestID, requestID)
			}
			if (tc.deferred || tc.max != 0) && nextPayload == payload {
				t.Fatalf("normalized payload = %q; want different from %q", nextPayload, payload)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultRedrainsAbandonedSameOwner(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_read_abandoned_owner"
	threadID := "thr_read_abandoned_owner"
	bindingID := "bind_read_abandoned_owner"
	taskID := "task_read_abandoned_owner"
	sourceID := "evt_read_abandoned_owner"
	seedReadCommandClaimFixture(t, admin, sessionID, threadID, bindingID, taskID, "evt_read_abandoned_task")

	requestID, key, payload := readCommandResultOwnerIdentity(sourceID, taskID, false, 23)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
			request_hash, ack_status, result_json, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $3, $4, $5, 'committed', $6,
			'2026-01-01T00:00:30Z', '2026-01-01T00:00:30Z')`,
		sessionID, threadID, bridgeOpReadCommandResult, key,
		bridgeRequestHash(bridgeOpReadCommandResult, key, payload), pendingCommandReadJSON()); err != nil {
		t.Fatalf("seed abandoned read claim: %v", err)
	}

	executor := &recordingSandboxToolExecutor{commandResult: SandboxCommandResult{ResultJSON: `{"status":"running","stdout":{"text":"recovered"}}`}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 31, 0, time.UTC) }
	store.SandboxToolExecutor = executor
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+sessionID)
	scope.RequestId = "random_attempt_request_must_not_own_the_poll"
	response, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:           scope,
		TaskId:          taskID,
		MaxOutputTokens: 23,
		ToolUseEventId:  sourceID,
	})
	if err != nil {
		t.Fatalf("ReadCommandResult abandoned owner successor: %v", err)
	}
	if len(executor.reads) != 1 || !strings.Contains(response.GetResultJson(), "recovered") {
		t.Fatalf("read calls=%d result=%s; want one successor drain", len(executor.reads), response.GetResultJson())
	}
	if executor.reads[0].OwnerRequestID != requestID {
		t.Fatalf("helper owner request id = %q; want %q", executor.reads[0].OwnerRequestID, requestID)
	}

	var operationCount int
	var storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), max(result_json)
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND operation = $2`, sessionID, bridgeOpReadCommandResult).Scan(&operationCount, &storedResult); err != nil {
		t.Fatalf("read recovered operation: %v", err)
	}
	if operationCount != 1 || storedResult != response.GetResultJson() || strings.Contains(storedResult, "_tetral_pending_command_read") {
		t.Fatalf("operations=%d stored=%s; want one finalized canonical claim", operationCount, storedResult)
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultDistinctOwnersDoNotContend(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_read_distinct_owners"
	threadID := "thr_read_distinct_owners"
	bindingID := "bind_read_distinct_owners"
	taskID := "task_read_distinct_owners"
	seedReadCommandClaimFixture(t, admin, sessionID, threadID, bindingID, taskID, "evt_read_distinct_task")

	executor := &recordingSandboxToolExecutor{commandResult: SandboxCommandResult{ResultJSON: `{"status":"running"}`}}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 31, 0, time.UTC) }
	store.SandboxToolExecutor = executor
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+sessionID)
	scope.RequestId = "same_per_attempt_request"
	for _, sourceID := range []string{"evt_read_distinct_a", "evt_read_distinct_b"} {
		if _, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
			Scope:          scope,
			TaskId:         taskID,
			ToolUseEventId: sourceID,
		}); err != nil {
			t.Fatalf("ReadCommandResult %s: %v", sourceID, err)
		}
	}
	if len(executor.reads) != 2 {
		t.Fatalf("helper reads = %d; want two independent logical polls", len(executor.reads))
	}
	var operationCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND operation = $2`, sessionID, bridgeOpReadCommandResult).Scan(&operationCount); err != nil {
		t.Fatalf("count distinct read operations: %v", err)
	}
	if operationCount != 2 {
		t.Fatalf("read operations = %d; want two distinct owner claims", operationCount)
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandResultPendingOwnerRecoversSupersededSandboxPoll(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_read_owner_stale_sandbox"
	threadID := "thr_read_owner_stale_sandbox"
	bindingID := "bind_read_owner_stale_sandbox"
	taskID := "task_read_owner_stale_sandbox"
	sourceID := "evt_read_owner_stale_sandbox"
	seedReadCommandClaimFixture(t, admin, sessionID, threadID, bindingID, taskID, "evt_read_owner_stale_task")

	_, key, payload := readCommandResultOwnerIdentity(sourceID, taskID, false, 0)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind, idempotency_key,
			request_hash, ack_status, result_json, created_at, updated_at
		) VALUES ('default', $1, $2, $3, $3, $4, $5, 'committed', $6,
			'2026-01-01T00:00:30Z', '2026-01-01T00:00:30Z')`,
		sessionID, threadID, bridgeOpReadCommandResult, key,
		bridgeRequestHash(bridgeOpReadCommandResult, key, payload), pendingCommandReadJSON()); err != nil {
		t.Fatalf("seed pending stale-sandbox read claim: %v", err)
	}
	setBridgeAPISandboxStatus(t, admin, "default", sessionID, "released")

	executor := &recordingSandboxToolExecutor{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 31, 0, time.UTC) }
	store.SandboxToolExecutor = executor
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+sessionID)
	response, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          scope,
		TaskId:         taskID,
		ToolUseEventId: sourceID,
	})
	if err != nil || !strings.Contains(response.GetResultJson(), `"status":"running"`) {
		t.Fatalf("ReadCommandResult stale sandbox response = %s err=%v; want stored-task recovery poll", response.GetResultJson(), err)
	}
	if len(executor.reads) != 1 || executor.reads[0].Target.SandboxID == "" || executor.reads[0].Task.ProviderCommandID == "" {
		t.Fatalf("helper reads = %+v; want original launch/provider recovery identity", executor.reads)
	}
}

func TestBridgeAPITaskNotificationReaderUsesDurableSourceOwnerIdentity(t *testing.T) {
	client := &recordingTaskNotificationBridgeClient{resultJSON: `{"status":"completed"}`}
	reader := &BridgeAPITaskNotificationReader{Client: client}
	scope := bridgeAPIScope("sesn_task_reader_owner", "thr_task_reader_owner", "bind_task_reader_owner", 1, "pod_task_reader_owner")
	scope.RequestId = "lease_scoped_request_must_not_escape"

	if _, err := reader.ReadTaskNotificationResult(context.Background(), scope, "task_reader_owner", "evt_task_reader_owner"); err != nil {
		t.Fatalf("ReadTaskNotificationResult: %v", err)
	}
	if client.request == nil {
		t.Fatal("ReadCommandResult request missing")
	}
	wantRequestID, _, _ := readCommandResultOwnerIdentity("evt_task_reader_owner", "task_reader_owner", true, 0)
	if client.request.GetScope().GetRequestId() != wantRequestID ||
		client.request.GetToolUseEventId() != "evt_task_reader_owner" ||
		!client.request.GetDeferTerminalSettlement() {
		t.Fatalf("task notification read = %+v; want durable source owner %q", client.request, wantRequestID)
	}
	if scope.GetRequestId() != "lease_scoped_request_must_not_escape" {
		t.Fatalf("caller scope request id mutated to %q", scope.GetRequestId())
	}
}

func seedReadCommandClaimFixture(t *testing.T, admin *sql.DB, sessionID string, threadID string, bindingID string, taskID string, taskSourceID string) {
	t.Helper()
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_"+sessionID)
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_"+sessionID)
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, taskSourceID)
}

type recordingTaskNotificationBridgeClient struct {
	bridgev1.AgentRuntimeBridgeServiceClient
	request    *bridgev1.ReadCommandResultRequest
	resultJSON string
}

func (c *recordingTaskNotificationBridgeClient) ReadCommandResult(_ context.Context, request *bridgev1.ReadCommandResultRequest, _ ...grpc.CallOption) (*bridgev1.ReadCommandResultResponse, error) {
	c.request = request
	return &bridgev1.ReadCommandResultResponse{ResultJson: c.resultJSON}, nil
}
