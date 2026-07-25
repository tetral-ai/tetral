package agentruntimebridge

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestStripInternalProviderFieldsRemovesNestedProviderMetadata(t *testing.T) {
	result := stripInternalProviderFields(`{"status":"completed","provider_metadata":{"raw":"secret"},"nested":{"provider_command_id":"cmd_1","provider_metadata_json":"{\"raw\":\"secret\"}","items":[{"provider_session_id":"sess_1","text":"ok"}]}}`)
	if strings.Contains(result, "provider_command_id") ||
		strings.Contains(result, "provider_session_id") ||
		strings.Contains(result, "provider_metadata") ||
		strings.Contains(result, "provider_metadata_json") {
		t.Fatalf("provider metadata leaked after recursive strip: %s", result)
	}
	if !strings.Contains(result, `"text":"ok"`) {
		t.Fatalf("recursive strip removed safe content: %s", result)
	}
}

func TestPostgreSQLBridgeAPIStoreRequeueResetsExpiredLeasedSessionPrepareJob(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_stale_leased", "thr_bridge_tool_stale_leased")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_stale_leased", "bind_bridge_tool_stale_leased", 1, "pod_uid_tool_stale_leased")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_tool_stale_leased", "prep_bridge_tool_stale_leased")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_tool_stale_leased", "2026-01-01T00:00:00Z")

	ws := workspace.ID("default")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO queue_jobs (
			id, workspace_id, kind, partition_key, dedupe_key, status,
			payload_json, lease_token, leased_by, leased_at, leased_until,
			available_at, created_at, updated_at
		) VALUES (
			'qjob_bridge_tool_stale_leased_existing', 'default', 'session_prepare', $1, $2, 'leased',
			'{"preparation_attempt_id":"old_payload"}', 'lease_old_prepare', 'sandbox-service',
			'2026-01-01T00:00:10Z', '2026-01-01T00:01:10Z',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', '2026-01-01T00:00:10Z'
		)`,
		queue.FormatSessionPartitionKey(ws, "sesn_bridge_tool_stale_leased"),
		queue.FormatSessionPrepareDedupeKey(ws, "sesn_bridge_tool_stale_leased", "prep_bridge_tool_stale_leased"),
	); err != nil {
		t.Fatalf("seed leased session_prepare job: %v", err)
	}

	executor := &recordingSandboxToolExecutor{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	store.SandboxToolExecutor = executor

	response, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_tool_stale_leased", "thr_bridge_tool_stale_leased", "bind_bridge_tool_stale_leased", 1, "pod_uid_tool_stale_leased"),
		ToolUseEventId:      "evt_tool_stale_leased",
		NormalizedInputHash: sha256Hex(`{"cmd":"pwd"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"pwd"}`,
	})
	if err != nil {
		t.Fatalf("RunTool stale sandbox with leased prepare job: %v", err)
	}
	assertRuntimeToolErrorCode(t, response.GetResultJson(), "sandbox_not_ready")
	assertSessionPrepareRequeued(t, admin, "default", "sesn_bridge_tool_stale_leased", "prep_bridge_tool_stale_leased")

	var status string
	var leaseToken sql.NullString
	var leasedBy sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, lease_token, leased_by
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND id = 'qjob_bridge_tool_stale_leased_existing'`,
	).Scan(&status, &leaseToken, &leasedBy); err != nil {
		t.Fatalf("read reset leased session_prepare job: %v", err)
	}
	if status != "leased" || !leaseToken.Valid || leaseToken.String != "lease_old_prepare" || !leasedBy.Valid || leasedBy.String != "sandbox-service" {
		t.Fatalf("leased session_prepare status=%q lease=%v leased_by=%v; want old leased job unchanged", status, leaseToken, leasedBy)
	}
}

func TestBridgeSessionMessageProjectionInsertsAreConflictSafe(t *testing.T) {
	assertConflictSafeInsert := func(t *testing.T, source string, signature string) {
		t.Helper()
		body := sourceFunctionBody(t, source, signature)
		for _, fragment := range []string{
			"ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)",
			"WHERE source_event_id IS NOT NULL",
			"DO NOTHING",
		} {
			if !strings.Contains(body, fragment) {
				t.Fatalf("%s missing source_event_id conflict protection fragment %q in:\n%s", signature, fragment, body)
			}
		}
	}

	for filename, signatures := range map[string][]string{
		"bridge_api_children.go": {"func insertForkSeedMessageTx"},
		"bridge_api_events.go": {
			"func insertToolResultMessageProjectionTx",
			"func insertSessionMessageProjectionTx",
		},
	} {
		bridgeSourceBytes, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("read %s: %v", filename, err)
		}
		for _, signature := range signatures {
			assertConflictSafeInsert(t, string(bridgeSourceBytes), signature)
		}
	}

	runtimeSourceBytes, err := os.ReadFile("runtime_delivery.go")
	if err != nil {
		t.Fatalf("read runtime_delivery.go: %v", err)
	}
	assertConflictSafeInsert(t, string(runtimeSourceBytes), "func insertPendingToolTerminalMessageTx")
}
