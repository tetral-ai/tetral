package agentruntimebridge

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Snapshot every tenant table, including timestamps and projection/claim state.
// A replay that hides a touch or renewal fails even when its response matches.
func receiptTenantSnapshot(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	rows, err := db.Query(`SELECT table_name FROM information_schema.columns WHERE table_schema='public' AND column_name='workspace_id' ORDER BY table_name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	result := map[string]string{}
	for _, table := range tables {
		quoted := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		var data string
		if err := db.QueryRow(`SELECT COALESCE(jsonb_agg(data ORDER BY data::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(row) AS data FROM public.` + quoted + ` row WHERE workspace_id='default') snapshot`).Scan(&data); err != nil {
			t.Fatal(err)
		}
		result[table] = data
	}
	if len(result) < 20 {
		t.Fatal("tenant snapshot omitted schema owners")
	}
	return result
}

func TestPostgreSQLRuntimeExecutorReceiptRetirement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope, sandboxTool := seedAwaitExecutionNotificationFixture(t, store, admin, "retiredreceipts")
	podUID := scope.GetBinding().GetTargetPodUid()
	seedBridgeAPIWritableMemoryStore(t, admin, "default", scope.GetSessionId(), "memory_retired_receipts")
	memoryRequest := durableMemoryRequestForTest(t, admin, scope, "evt_retired_memory", `{"action":"create","path":"notes/replay.md","content":"original"}`)
	client := processRegistryRPC(t, dbconnect.NewClientForTesting(runtime), podUID)
	memoryFirst, err := client.RunMemory(context.Background(), memoryRequest)
	if err != nil || memoryFirst.GetCommitted() == nil {
		t.Fatalf("memory admission=%v/%v", memoryFirst, err)
	}
	memoryBytes := memoryFirst.GetCommitted().GetResultJson()
	const sandboxBytes = `{"status":"success","result":{"stdout":"original"}}`
	commitAwaitExecutionSettlement(t, admin, scope, sandboxTool, sandboxBytes, false)
	sandboxRequest := &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: sandboxTool}
	// Prove committed stdin custody before losing the real caller's response.
	// Its existing 5s fixture guard exceeds the unchanged 3s admission phase
	// and precedes the 30s result wait; deadline expiry alone is not admission.
	const task = "task_retired_stdin"
	const stdinTool = "evt_retired_stdin"
	seedBridgeAPIEvent(t, admin, "default", scope.GetSessionId(), scope.GetSessionThreadId(), stdinTool, nextBridgeAPIEventSequenceForTest(t, admin, scope.GetSessionId(), scope.GetSessionThreadId()), "agent.tool_use", `{"name":"write_stdin","input":{"session_id":"task_retired_stdin","chars":"once"},"evaluated_permission":"allow"}`)
	seedBridgeAPIToolDeclarationProjection(t, admin, "default", scope.GetSessionId(), scope.GetSessionThreadId(), stdinTool, "call_retired_stdin", "write_stdin", `{"session_id":"task_retired_stdin","chars":"once"}`, "background_command")
	seedBridgeAPIAllowedToolRoute(t, admin, "default", scope.GetSessionId(), scope.GetSessionThreadId(), stdinTool)
	seedBridgeAPIBackgroundTask(t, admin, "default", scope.GetSessionId(), scope.GetSessionThreadId(), scope.GetBinding().GetBindingId(), task, "evt_retired_stdin_source")
	stdinRequest := &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: task, ToolUseEventId: stdinTool, OperationId: "op_retired_stdin"}
	tracer := &bridgeExecutionQueryTracer{}
	traced := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer)))
	stdinClient, stdinReturned := receiptJoinedRPC(t, traced, podUID)
	admitPendingBackgroundReceipt(t, admin, tracer, stdinReturned, backgroundReceiptAdmission{
		scope: scope, receiptID: stdinTool, taskID: task, requestID: stdinRequest.OperationId, kind: "stdin", totalJobs: 1,
		call: func(ctx context.Context) error {
			_, err := stdinClient.SendCommandInput(ctx, stdinRequest)
			return err
		},
	})
	const stdinBytes = `{"status":"accepted","original":true}`
	updated, err := admin.Exec(`UPDATE session_runtime_tool_results SET background_operation_state='terminal',result_json=$1,result_digest='fixture_terminal_digest' WHERE workspace_id='default' AND tool_use_event_id=$2`, stdinBytes, stdinTool)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := updated.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("terminal stdin receipt=%d/%v; want 1", count, err)
	}

	newMemory := durableMemoryRequestForTest(t, admin, scope, "evt_retired_memory_unseen", `{"action":"create","path":"notes/unseen.md","content":"forbidden"}`)
	repairRequest := &bridgev1.CommitInternalToolRepairRequest{Scope: scope, ModelRequestId: "mreq_exec_notify_retiredreceipts", ModelToolCallId: "call_retired_repair", ToolName: "unknown", RepairKey: internalToolRepairKey("mreq_exec_notify_retiredreceipts", "call_retired_repair", "unknown"), CanonicalInputJson: `{"q":"original"}`, Error: &bridgev1.RuntimeToolError{ErrorJson: `{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}`}}
	repairFirst, err := client.CommitInternalToolRepair(context.Background(), repairRequest)
	if err != nil || repairFirst.GetCommitted() == nil {
		t.Fatalf("repair admission=%v/%v", repairFirst, err)
	}
	mcpScope := bridgeAPIScope("session_retired_mcp", "thread_retired_mcp", "binding_retired_mcp", 1, podUID)
	seedBridgeAPISession(t, admin, "default", mcpScope.SessionId, mcpScope.SessionThreadId)
	seedBridgeAPIRuntimeBinding(t, admin, "default", mcpScope.SessionId, mcpScope.Binding.BindingId, 1, podUID)
	mcpTool := writeDurableMCPToolUseForTest(t, store, mcpScope)
	mcp := processRegistryRPCWithIdentity(t, store, auth.Identity{ServiceAccount: auth.ServiceAccount{Namespace: "tetral-system", Name: "mcp-connector"}}, nil)
	claimRequest := &bridgev1.ClaimMcpToolResultRequest{Scope: mcpScope, ToolUseEventId: mcpTool, ClaimId: "claim_retired_mcp"}
	claimed, err := mcp.ClaimMcpToolResult(context.Background(), claimRequest)
	if err != nil || claimed.GetAcquired() == nil {
		t.Fatalf("MCP claim=%v/%v", claimed, err)
	}
	const mcpBytes = `{"response":{"status":1,"result_text":"original","attachments":[]},"content_items":1,"refresh_triggered":false}`
	mcpCommit := &bridgev1.CommitMcpToolResultRequest{Scope: mcpScope, ToolUseEventId: mcpTool, ClaimId: claimRequest.ClaimId, ResultJson: mcpBytes}
	committedMCP, err := mcp.CommitMcpToolResult(context.Background(), mcpCommit)
	if err != nil || committedMCP.GetCommitted() == nil {
		t.Fatalf("MCP commit=%v/%v", committedMCP, err)
	}
	// Separate in-flight custody exercises the hidden lease-renewal mutation.
	mcpInflightScope := bridgeAPIScope("session_retired_mcp_inflight", "thread_retired_mcp_inflight", "binding_retired_mcp_inflight", 1, podUID)
	seedBridgeAPISession(t, admin, "default", mcpInflightScope.SessionId, mcpInflightScope.SessionThreadId)
	seedBridgeAPIRuntimeBinding(t, admin, "default", mcpInflightScope.SessionId, mcpInflightScope.Binding.BindingId, 1, podUID)
	inflightTool := writeDurableMCPToolUseForTest(t, store, mcpInflightScope)
	inflightClaim := &bridgev1.ClaimMcpToolResultRequest{Scope: mcpInflightScope, ToolUseEventId: inflightTool, ClaimId: "claim_inflight_retired"}
	inflightFirst, err := mcp.ClaimMcpToolResult(context.Background(), inflightClaim)
	if err != nil || inflightFirst.GetAcquired() == nil {
		t.Fatalf("MCP inflight admission=%v/%v", inflightFirst, err)
	}

	// An independent Bridge promotes a same-Pod boot. The old binding deliberately
	// remains until reconciliation so exact receipt replay is still authorized.
	replacement := processRegistryRPC(t, dbconnect.NewClientForTesting(runtime), podUID)
	registered, err := replacement.RegisterRuntimeProcess(context.Background(), &bridgev1.RegisterRuntimeProcessRequest{RuntimeProcessId: "replacement_retired_receipts"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.ReportRuntimeProcess(context.Background(), &bridgev1.ReportRuntimeProcessRequest{RuntimeProcessId: registered.RuntimeProcessId, RegistrationReceipt: registered.RegistrationReceipt, Phase: bridgev1.RuntimeProcessPhase_RUNTIME_PROCESS_PHASE_ACCEPTING}); err != nil {
		t.Fatal(err)
	}
	before := receiptTenantSnapshot(t, admin)
	for batch := 0; batch < 3; batch++ {
		repair, err := client.CommitInternalToolRepair(context.Background(), repairRequest)
		if err != nil || repair.GetDuplicate().GetRepairEventId() != repairFirst.GetCommitted().GetRepairEventId() {
			t.Fatalf("retired repair=%v/%v", repair, err)
		}
		mcpReplay, err := mcp.CommitMcpToolResult(context.Background(), mcpCommit)
		if err != nil || mcpReplay.GetDuplicate() == nil {
			t.Fatalf("retired MCP commit=%v/%v", mcpReplay, err)
		}
		mcpStored, err := mcp.ClaimMcpToolResult(context.Background(), claimRequest)
		if err != nil || mcpStored.GetAlreadyCompleted().GetResultJson() != mcpBytes {
			t.Fatalf("retired MCP stored=%v/%v", mcpStored, err)
		}
		mcpRenew, err := mcp.ClaimMcpToolResult(context.Background(), inflightClaim)
		if err != nil || mcpRenew.GetStale() == nil {
			t.Fatalf("retired MCP renewed claim=%v/%v", mcpRenew, err)
		}
		got, err := client.RunMemory(context.Background(), memoryRequest)
		if err != nil || got.GetDuplicate().GetResultJson() != memoryBytes {
			t.Fatalf("retired memory receipt=%v/%v", got, err)
		}
		accepted, err := client.AcceptSandboxExecution(context.Background(), sandboxRequest)
		if err != nil || accepted.GetDuplicate() == nil {
			t.Fatalf("retired Sandbox receipt=%v/%v", accepted, err)
		}
		awaited, err := client.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{Scope: scope, ToolUseEventId: sandboxTool})
		if err != nil || awaited.GetCompleted().GetResultJson() != sandboxBytes {
			t.Fatalf("retired result=%v/%v", awaited, err)
		}
		stdin, err := client.SendCommandInput(context.Background(), stdinRequest)
		if err != nil || stdin.GetDuplicate().GetResultJson() != stdinBytes {
			t.Fatalf("retired stdin=%v/%v", stdin, err)
		}
	}
	newDenied, err := client.RunMemory(context.Background(), newMemory)
	if err != nil || newDenied.GetStale() == nil {
		t.Fatalf("retired fresh memory mutation=%v/%v", newDenied, err)
	}
	changedMCP := proto.Clone(mcpCommit).(*bridgev1.CommitMcpToolResultRequest)
	changedMCP.ResultJson = `{"response":{"status":1,"result_text":"changed","attachments":[]},"content_items":1,"refresh_triggered":false}`
	if _, err := mcp.CommitMcpToolResult(context.Background(), changedMCP); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed MCP payload=%v", err)
	}
	changed := proto.Clone(stdinRequest).(*bridgev1.SendCommandInputRequest)
	changed.OperationId = "conflicting_operation"
	if _, err := client.SendCommandInput(context.Background(), changed); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("changed receipt identity=%v", err)
	}
	forged := proto.Clone(memoryRequest).(*bridgev1.RunMemoryRequest)
	forged.Scope.Binding.RuntimeProcessId = registered.RuntimeProcessId
	denied, err := client.RunMemory(context.Background(), forged)
	if err == nil && denied.GetStale() == nil {
		t.Fatal("replacement used old binding receipt")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(before, after) {
		for table, rows := range before {
			if after[table] != rows {
				t.Errorf("receipt mutated %s", table)
			}
		}
	}
	if _, err := admin.Exec(`DELETE FROM session_runtime_bindings WHERE workspace_id='default'`); err != nil {
		t.Fatal(err)
	}
	afterRelease := receiptTenantSnapshot(t, admin)
	mcpAfterUnbind, err := mcp.CommitMcpToolResult(context.Background(), mcpCommit)
	if err != nil || mcpAfterUnbind.GetStale() == nil {
		t.Fatalf("MCP receipt disclosed afterunbind=%v/%v", mcpAfterUnbind, err)
	}
	repairAfterUnbind, err := client.CommitInternalToolRepair(context.Background(), repairRequest)
	if err == nil && repairAfterUnbind.GetStale() == nil {
		t.Fatal("repair receipt disclosed afterunbind")
	}

	memoryDenied, err := client.RunMemory(context.Background(), memoryRequest)
	if err == nil && memoryDenied.GetStale() == nil {
		t.Fatal("ordinary memory replay disclosed after unbind")
	}
	sandboxDenied, err := client.AcceptSandboxExecution(context.Background(), sandboxRequest)
	if err == nil && sandboxDenied.GetStale() == nil {
		t.Fatal("ordinary Sandbox replay disclosed after unbind")
	}
	awaitDenied, err := client.AwaitSandboxExecution(context.Background(), &bridgev1.AwaitSandboxExecutionRequest{Scope: scope, ToolUseEventId: sandboxTool})
	if err == nil && awaitDenied.GetStale() == nil {
		t.Fatal("ordinary wait disclosed after unbind")
	}
	stdinDenied, err := client.SendCommandInput(context.Background(), stdinRequest)
	if err == nil && stdinDenied.GetStale() == nil {
		t.Fatal("ordinary stdin replay disclosed after unbind")
	}
	if after := receiptTenantSnapshot(t, admin); !reflect.DeepEqual(afterRelease, after) {
		t.Fatal("denied receipt mutated tenant data")
	}
}
