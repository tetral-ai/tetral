package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tasks protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreRunToolPersistsBackgroundTaskBeforeAck(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool", "thr_bridge_tool")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_tool", "prep_bridge_tool")
	seedBridgeAPIResourceRootsJSON(t, admin, "default", "sesn_bridge_tool", "prep_bridge_tool", `[{"path":"/workspace/data/report.csv","mode":"read"}]`)
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_tool", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_tool"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_tool",
				ProviderSessionID:           "provider_session_tool",
				ProviderCommandID:           "provider_command_tool",
				ProviderCommandMetadataJSON: `{"driver":"unit"}`,
			},
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	request := &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool"),
		ToolUseEventId:      "evt_tool_run",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 1"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 1"}`,
	}
	response, err := store.RunTool(context.Background(), request)
	if err != nil {
		t.Fatalf("RunTool: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		!response.GetBackgroundTaskStarted() ||
		response.GetTaskId() != "task_bridge_tool" {
		t.Fatalf("RunTool response = %+v; want committed background task", response)
	}
	if len(executor.invocations) != 1 {
		t.Fatalf("executor calls = %d; want 1", len(executor.invocations))
	}
	if len(executor.healthChecks) != 1 || executor.healthChecks[0].ProviderSandboxID != "provider_sesn_bridge_tool" {
		t.Fatalf("health checks = %+v; want one check before tool execution", executor.healthChecks)
	}
	if executor.invocations[0].Target.SandboxID != "sandbox_sesn_bridge_tool" ||
		executor.invocations[0].Target.ProviderSandboxID != "provider_sesn_bridge_tool" ||
		executor.invocations[0].Target.BindingID != "bind_bridge_tool" ||
		executor.invocations[0].Target.ResourceRootsJSON != `[{"path":"/workspace/data/report.csv","mode":"read"}]` {
		t.Fatalf("executor target = %+v; want active sandbox and binding", executor.invocations[0].Target)
	}

	var providerSessionID string
	var providerCommandID string
	var taskStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_session_id, provider_command_id, status
		   FROM session_background_tasks
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool' AND task_id = 'task_bridge_tool'`).Scan(&providerSessionID, &providerCommandID, &taskStatus); err != nil {
		t.Fatalf("read background task: %v", err)
	}
	if providerSessionID != "provider_session_tool" || providerCommandID != "provider_command_tool" || taskStatus != "running" {
		t.Fatalf("background task = session %q command %q status %q; want provider metadata/running", providerSessionID, providerCommandID, taskStatus)
	}
	var resultJSON string
	var background bool
	var taskID, claimStatus, claimOwner, claimLease sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json, background_task_started, task_id,
		        mcp_claim_status, mcp_claim_owner_request_id, mcp_claim_lease_expires_at
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool' AND tool_use_event_id = 'evt_tool_run'`).Scan(&resultJSON, &background, &taskID, &claimStatus, &claimOwner, &claimLease); err != nil {
		t.Fatalf("read runtime tool result: %v", err)
	}
	if resultJSON != response.GetResultJson() || !background || !taskID.Valid || taskID.String != "task_bridge_tool" {
		t.Fatalf("runtime tool result = json %q background %v task %v; want persisted running task", resultJSON, background, taskID)
	}
	if claimStatus.Valid || claimOwner.Valid || claimLease.Valid {
		t.Fatalf("running MCP claim fields = %+v/%+v/%+v; want all NULL", claimStatus, claimOwner, claimLease)
	}

	replay, err := store.RunTool(context.Background(), request)
	if err != nil {
		t.Fatalf("RunTool replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetTaskId() != response.GetTaskId() {
		t.Fatalf("RunTool replay = %+v; want duplicate same task", replay)
	}
	if len(executor.invocations) != 1 {
		t.Fatalf("executor calls after replay = %d; want 1", len(executor.invocations))
	}
	if len(executor.healthChecks) != 1 {
		t.Fatalf("health checks after replay = %d; want unchanged duplicate replay", len(executor.healthChecks))
	}
	reorderedReplayRequest := proto.Clone(request).(*bridgev1.RunToolRequest)
	reorderedReplayRequest.InputJson = `{"cmd": "sleep 1"}`
	reorderedReplayRequest.Scope.RequestId = "req_bridge_tool_reordered_replay"
	reorderedReplay, err := store.RunTool(context.Background(), reorderedReplayRequest)
	if err != nil {
		t.Fatalf("RunTool normalized replay: %v", err)
	}
	if reorderedReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || reorderedReplay.GetTaskId() != response.GetTaskId() {
		t.Fatalf("RunTool normalized replay = %+v; want duplicate same task", reorderedReplay)
	}
	if len(executor.invocations) != 1 {
		t.Fatalf("executor calls after normalized replay = %d; want 1", len(executor.invocations))
	}

	sendScope := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	sendScope.RequestId = "req_bridge_tool_stdin_1"
	sendResponse, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:           sendScope,
		TaskId:          "task_bridge_tool",
		MaxOutputTokens: 123,
		InputJson:       `{"chars":"hello\n","max_output_tokens":123}`,
		ToolUseEventId:  "evt_bridge_tool_stdin_1",
	})
	if err != nil {
		t.Fatalf("SendCommandInput: %v", err)
	}
	if sendResponse.GetResultJson() != `{"status":"accepted"}` {
		t.Fatalf("SendCommandInput result = %s; want accepted", sendResponse.GetResultJson())
	}
	if sendResponse.GetWriteSeq() != 1 {
		t.Fatalf("SendCommandInput write_seq = %d; want 1", sendResponse.GetWriteSeq())
	}
	if len(executor.inputs) != 1 {
		t.Fatalf("stdin executor calls = %d; want 1", len(executor.inputs))
	}
	if got := testJSONPathString(t, executor.inputs[0].InputJSON, "chars"); got != "hello\n" {
		t.Fatalf("stdin chars = %q; want hello newline", got)
	}
	if executor.inputs[0].MaxOutputTokens != 123 {
		t.Fatalf("stdin max output tokens = %d; want 123", executor.inputs[0].MaxOutputTokens)
	}
	if executor.inputs[0].ToolUseEventID != "evt_bridge_tool_stdin_1" {
		t.Fatalf("stdin tool use event id = %q; want current follow-up event", executor.inputs[0].ToolUseEventID)
	}
	if got := testJSONPathInt(t, executor.inputs[0].InputJSON, "max_output_tokens"); got != 123 {
		t.Fatalf("stdin max_output_tokens = %d; want 123", got)
	}
	if got := testJSONPathInt(t, executor.inputs[0].InputJSON, "write_seq"); got != 1 {
		t.Fatalf("stdin write_seq = %d; want 1", got)
	}
	sendReplay, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:           sendScope,
		TaskId:          "task_bridge_tool",
		MaxOutputTokens: 123,
		InputJson:       `{"chars":"hello\n","max_output_tokens":123}`,
		ToolUseEventId:  "evt_bridge_tool_stdin_1",
	})
	if err != nil {
		t.Fatalf("SendCommandInput replay: %v", err)
	}
	if sendReplay.GetResultJson() != sendResponse.GetResultJson() || sendReplay.GetWriteSeq() != 1 || len(executor.inputs) != 1 {
		t.Fatalf("send replay = %s write_seq=%d calls=%d; want durable replay without helper", sendReplay.GetResultJson(), sendReplay.GetWriteSeq(), len(executor.inputs))
	}
	sendScope2 := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	sendScope2.RequestId = "req_bridge_tool_stdin_2"
	restartedStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	restartedStore.Clock = store.Clock
	restartedStore.SandboxToolExecutor = executor
	sendResponse2, err := restartedStore.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:          sendScope2,
		TaskId:         "task_bridge_tool",
		InputJson:      `{"chars":"again\n","write_seq":99}`,
		ToolUseEventId: "evt_bridge_tool_stdin_2",
	})
	if err != nil {
		t.Fatalf("SendCommandInput second write: %v", err)
	}
	if sendResponse2.GetWriteSeq() != 2 {
		t.Fatalf("second SendCommandInput write_seq = %d; want 2", sendResponse2.GetWriteSeq())
	}
	if len(executor.inputs) != 2 {
		t.Fatalf("stdin executor calls after second write = %d; want 2", len(executor.inputs))
	}
	if got := testJSONPathInt(t, executor.inputs[1].InputJSON, "write_seq"); got != 2 {
		t.Fatalf("second stdin write_seq = %d; want Bridge-owned 2 after store restart", got)
	}
	var durableWriteSequence int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT stdin_write_sequence
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool'
		    AND task_id = 'task_bridge_tool'`).Scan(&durableWriteSequence); err != nil {
		t.Fatalf("read durable stdin sequence: %v", err)
	}
	if durableWriteSequence != 2 {
		t.Fatalf("durable stdin sequence = %d; want 2", durableWriteSequence)
	}

	pendingPayload := `{"chars":"lost\n"}`
	pendingInputJSON, err := injectCommandInputWriteSeq(pendingPayload, 3)
	if err != nil {
		t.Fatalf("prepare pending stdin input: %v", err)
	}
	pendingKey := "task_bridge_tool:req_bridge_tool_stdin_pending"
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_background_tasks
		    SET stdin_write_sequence = 3
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool'
		    AND task_id = 'task_bridge_tool'`); err != nil {
		t.Fatalf("seed pending stdin metadata: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, idempotency_key,
			request_hash, ack_status, result_json, stdin_write_seq, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_tool', 'thr_bridge_tool', $1, $2, $3,
			'committed', $4, 3, '2026-01-01T00:00:31Z', '2026-01-01T00:00:31Z'
		)`,
		bridgeOpSendCommandInput,
		pendingKey,
		bridgeRequestHash(bridgeOpSendCommandInput, pendingKey, "task_bridge_tool:"+pendingPayload),
		pendingCommandInputJSON(pendingInputJSON),
	); err != nil {
		t.Fatalf("seed pending stdin operation: %v", err)
	}
	pendingScope := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	pendingScope.RequestId = "req_bridge_tool_stdin_pending"
	pendingResponse, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:          pendingScope,
		TaskId:         "task_bridge_tool",
		InputJson:      pendingPayload,
		ToolUseEventId: "evt_bridge_tool_stdin_pending",
	})
	if err != nil {
		t.Fatalf("SendCommandInput pending replay: %v", err)
	}
	if pendingResponse.GetResultJson() != `{"status":"accepted"}` {
		t.Fatalf("pending replay result = %s; want accepted", pendingResponse.GetResultJson())
	}
	if pendingResponse.GetWriteSeq() != 3 {
		t.Fatalf("pending replay write_seq = %d; want reused 3", pendingResponse.GetWriteSeq())
	}
	if len(executor.inputs) != 3 {
		t.Fatalf("stdin executor calls after pending replay = %d; want 3", len(executor.inputs))
	}
	if got := testJSONPathInt(t, executor.inputs[2].InputJSON, "write_seq"); got != 3 {
		t.Fatalf("pending replay stdin write_seq = %d; want reused 3", got)
	}
	pendingReplay, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:          pendingScope,
		TaskId:         "task_bridge_tool",
		InputJson:      pendingPayload,
		ToolUseEventId: "evt_bridge_tool_stdin_pending",
	})
	if err != nil {
		t.Fatalf("SendCommandInput completed pending replay: %v", err)
	}
	if pendingReplay.GetResultJson() != pendingResponse.GetResultJson() || pendingReplay.GetWriteSeq() != 3 || len(executor.inputs) != 3 {
		t.Fatalf("completed pending replay = %s write_seq=%d calls=%d; want stored result without helper", pendingReplay.GetResultJson(), pendingReplay.GetWriteSeq(), len(executor.inputs))
	}
	var pendingStoredResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool'
		    AND operation = $1
		    AND idempotency_key = $2`,
		bridgeOpSendCommandInput,
		pendingKey,
	).Scan(&pendingStoredResult); err != nil {
		t.Fatalf("read pending stdin operation: %v", err)
	}
	if strings.Contains(pendingStoredResult, "_tetral_pending_command_input") {
		t.Fatalf("pending stdin operation result = %s; want final helper result", pendingStoredResult)
	}
	sendScope4 := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	sendScope4.RequestId = "req_bridge_tool_stdin_4"
	sendResponse4, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:          sendScope4,
		TaskId:         "task_bridge_tool",
		InputJson:      `{"chars":"after\n"}`,
		ToolUseEventId: "evt_bridge_tool_stdin_4",
	})
	if err != nil {
		t.Fatalf("SendCommandInput after pending replay: %v", err)
	}
	if sendResponse4.GetWriteSeq() != 4 {
		t.Fatalf("post-pending SendCommandInput write_seq = %d; want 4", sendResponse4.GetWriteSeq())
	}
	if len(executor.inputs) != 4 {
		t.Fatalf("stdin executor calls after post-pending write = %d; want 4", len(executor.inputs))
	}
	if got := testJSONPathInt(t, executor.inputs[3].InputJSON, "write_seq"); got != 4 {
		t.Fatalf("post-pending stdin write_seq = %d; want 4", got)
	}

	terminalHelperResult := `{"status":"success","result":{"task_id":"task_bridge_tool","exit_code":0,"stdout":{"text":"done","total_bytes":4,"total_lines":1,"returned_bytes":4,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false}}}`
	executor.commandResult = SandboxCommandResult{ResultJSON: terminalHelperResult, TerminalStatus: "completed"}
	readResponse, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:           bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool"),
		TaskId:          "task_bridge_tool",
		MaxOutputTokens: 77,
		ToolUseEventId:  "evt_bridge_tool_poll_1",
	})
	if err != nil {
		t.Fatalf("ReadCommandResult: %v", err)
	}
	if testJSONPathString(t, readResponse.GetResultJson(), "status") != "success" ||
		testJSONPathString(t, readResponse.GetResultJson(), "result.task_id") != "task_bridge_tool" ||
		testJSONPathString(t, readResponse.GetResultJson(), "result.stdout.text") != "done" ||
		testJSONPathInt(t, readResponse.GetResultJson(), "result.stdout.total_bytes") != 4 {
		t.Fatalf("ReadCommandResult result = %s; want helper terminal envelope facts", readResponse.GetResultJson())
	}
	if len(executor.reads) != 1 || executor.reads[0].Task.ProviderCommandID != "provider_command_tool" {
		t.Fatalf("read executor calls = %+v; want provider command metadata", executor.reads)
	}
	if executor.reads[0].MaxOutputTokens != 77 {
		t.Fatalf("read max output tokens = %d; want 77", executor.reads[0].MaxOutputTokens)
	}
	if executor.reads[0].ToolUseEventID != "evt_bridge_tool_poll_1" {
		t.Fatalf("read tool use event id = %q; want current follow-up event", executor.reads[0].ToolUseEventID)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool' AND task_id = 'task_bridge_tool'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read terminal task status: %v", err)
	}
	if taskStatus != "completed" {
		t.Fatalf("task status after read = %q; want completed", taskStatus)
	}
	readReplay, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:           bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool"),
		TaskId:          "task_bridge_tool",
		MaxOutputTokens: 77,
		ToolUseEventId:  "evt_bridge_tool_poll_1",
	})
	if err != nil {
		t.Fatalf("ReadCommandResult replay: %v", err)
	}
	if readReplay.GetResultJson() != readResponse.GetResultJson() || len(executor.reads) != 1 {
		t.Fatalf("read replay = %s calls=%d; want durable replay without helper", readReplay.GetResultJson(), len(executor.reads))
	}
	freshReadScope := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	freshReadScope.RequestId = "req_bridge_tool_fresh_read_terminal"
	freshRead, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          freshReadScope,
		TaskId:         "task_bridge_tool",
		ToolUseEventId: "evt_bridge_tool_poll_terminal",
	})
	if err != nil {
		t.Fatalf("ReadCommandResult fresh terminal: %v", err)
	}
	if freshRead.GetResultJson() == readResponse.GetResultJson() {
		t.Fatalf("fresh terminal read returned operation replay %s; want canonical terminal event result", freshRead.GetResultJson())
	}
	if testJSONPathString(t, freshRead.GetResultJson(), "status") != "completed" ||
		testJSONPathString(t, freshRead.GetResultJson(), "source_tool_use_event_id") != "evt_tool_run" ||
		testJSONPathString(t, freshRead.GetResultJson(), "stdout.text") != "done" ||
		testJSONPathInt(t, freshRead.GetResultJson(), "stdout.original_bytes") != 4 ||
		testJSONPathInt(t, freshRead.GetResultJson(), "stdout.original_lines") != 1 {
		t.Fatalf("fresh terminal read = %s; want canonical terminal facts from terminal_event_id", freshRead.GetResultJson())
	}
	assertNoTaskOutputPaths(t, freshRead.GetResultJson())
	cancelAfterTerminalScope := bridgeAPIScope("sesn_bridge_tool", "thr_bridge_tool", "bind_bridge_tool", 1, "pod_uid_tool")
	cancelAfterTerminalScope.RequestId = "req_bridge_tool_cancel_after_terminal"
	cancelAfterTerminal, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope:          cancelAfterTerminalScope,
		TaskId:         "task_bridge_tool",
		Reason:         "user_cancel_after_done",
		ToolUseEventId: "evt_bridge_tool_cancel_terminal",
	})
	if err != nil {
		t.Fatalf("CancelCommand after terminal: %v", err)
	}
	if cancelAfterTerminal.GetResultJson() != freshRead.GetResultJson() {
		t.Fatalf("cancel after terminal = %s; want existing terminal facts %s", cancelAfterTerminal.GetResultJson(), freshRead.GetResultJson())
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool' AND task_id = 'task_bridge_tool'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read task status after cancel terminal: %v", err)
	}
	if taskStatus != "completed" {
		t.Fatalf("task status after cancel terminal = %q; want still completed", taskStatus)
	}
}

func TestValidateSandboxBackgroundTaskBoundsRecoveryMetadataObject(t *testing.T) {
	base := SandboxBackgroundTask{TaskID: "task_metadata", ProviderSessionID: "provider_session", ProviderCommandID: "provider_command"}
	valid := base
	valid.ProviderCommandMetadataJSON = "{\"x\":\"" + strings.Repeat("a", 4088) + "\"}"
	if err := validateSandboxBackgroundTask(valid); err != nil {
		t.Fatalf("4096-byte metadata rejected: %v", err)
	}
	for _, metadata := range []string{
		"{\"x\":\"" + strings.Repeat("a", 4089) + "\"}",
		`[]`,
		`null`,
		`{"x":`,
	} {
		candidate := base
		candidate.ProviderCommandMetadataJSON = metadata
		if err := validateSandboxBackgroundTask(candidate); err == nil {
			t.Fatalf("metadata %q accepted; want object/4KiB rejection", metadata[:min(len(metadata), 32)])
		}
	}
}

func TestPostgreSQLBridgeAPIStorePersistsStartedBackgroundTaskAcrossCommitTargetChange(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_stale_commit", "thr_bridge_tool_stale_commit")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_stale_commit", "bind_bridge_tool_stale_commit", 1, "pod_uid_tool_stale_commit")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_tool_stale_commit", "prep_bridge_tool_stale_commit")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_tool_stale_commit", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_stale_commit"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:            "task_bridge_stale_commit",
				ProviderSessionID: "provider_session_stale_commit",
				ProviderCommandID: "provider_command_stale_commit",
			},
		},
		onRun: func(SandboxToolInvocation) {
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sandboxes SET status = 'released', provider_sandbox_id = NULL
				  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_stale_commit'`); err != nil {
				t.Fatalf("replace launch provider handle: %v", err)
			}
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	request := &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_tool_stale_commit", "thr_bridge_tool_stale_commit", "bind_bridge_tool_stale_commit", 1, "pod_uid_tool_stale_commit"),
		ToolUseEventId:      "evt_tool_stale_commit",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 1"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 1"}`,
	}
	response, err := store.RunTool(context.Background(), request)
	if err != nil {
		t.Fatalf("RunTool stale commit race: %v", err)
	}
	if !response.GetBackgroundTaskStarted() || response.GetTaskId() != "task_bridge_stale_commit" || !strings.Contains(response.GetResultJson(), `"status":"running"`) {
		t.Fatalf("RunTool response = %+v; want stable running task after commit race", response)
	}
	replay, err := store.RunTool(context.Background(), proto.Clone(request).(*bridgev1.RunToolRequest))
	if err != nil {
		t.Fatalf("RunTool stale commit replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetResultJson() != response.GetResultJson() || replay.GetTaskId() != response.GetTaskId() || len(executor.invocations) != 1 {
		t.Fatalf("RunTool stale replay = %+v calls=%d; want field-identical duplicate with zero new helper calls", replay, len(executor.invocations))
	}

	var backgroundRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*)
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_stale_commit'`).Scan(&backgroundRows); err != nil {
		t.Fatalf("count background tasks: %v", err)
	}
	if backgroundRows != 1 {
		t.Fatalf("background task rows = %d; want 1 recoverable owner after stale commit race", backgroundRows)
	}
	var resultRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COUNT(*)
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_stale_commit'
		    AND tool_use_event_id = 'evt_tool_stale_commit'`).Scan(&resultRows); err != nil {
		t.Fatalf("count runtime tool results: %v", err)
	}
	if resultRows != 1 {
		t.Fatalf("runtime tool result rows = %d; want 1 running result after stale commit race", resultRows)
	}

	fresh := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	fresh.Clock = store.Clock
	fresh.SandboxToolExecutor = executor
	poll, err := fresh.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          bridgeAPIScope("sesn_bridge_tool_stale_commit", "thr_bridge_tool_stale_commit", "bind_bridge_tool_stale_commit", 1, "pod_uid_tool_stale_commit"),
		TaskId:         "task_bridge_stale_commit",
		ToolUseEventId: "evt_tool_stale_commit_poll",
	})
	if err != nil || !strings.Contains(poll.GetResultJson(), `"status":"running"`) {
		t.Fatalf("cold poll = %s err=%v; want original running helper task", poll.GetResultJson(), err)
	}
	stdinScope := bridgeAPIScope("sesn_bridge_tool_stale_commit", "thr_bridge_tool_stale_commit", "bind_bridge_tool_stale_commit", 1, "pod_uid_tool_stale_commit")
	stdinScope.RequestId = "req_tool_stale_commit_stdin"
	stdin, err := fresh.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope: stdinScope, TaskId: "task_bridge_stale_commit", InputJson: `{"chars":"still here\n"}`,
	})
	if err != nil || !strings.Contains(stdin.GetResultJson(), `"status":"accepted"`) {
		t.Fatalf("cold stdin = %s err=%v; want original running helper task", stdin.GetResultJson(), err)
	}
	cancelScope := bridgeAPIScope("sesn_bridge_tool_stale_commit", "thr_bridge_tool_stale_commit", "bind_bridge_tool_stale_commit", 1, "pod_uid_tool_stale_commit")
	cancelScope.RequestId = "req_tool_stale_commit_cancel"
	cancel, err := fresh.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope:          cancelScope,
		TaskId:         "task_bridge_stale_commit",
		ToolUseEventId: "evt_tool_stale_commit_cancel",
		Reason:         "test_complete",
	})
	if err != nil || !strings.Contains(cancel.GetResultJson(), `"status":"cancelled"`) {
		t.Fatalf("cold cancel = %s err=%v; want original helper task cancelled", cancel.GetResultJson(), err)
	}
	if len(executor.reads) != 1 || executor.reads[0].Target.ProviderSandboxID != "provider_session_stale_commit" || executor.reads[0].Task.ProviderCommandID != "provider_command_stale_commit" {
		t.Fatalf("cold poll recovery = %+v; want original launch target/provider identity", executor.reads)
	}
	if len(executor.inputs) != 1 || executor.inputs[0].Target.ProviderSandboxID != "provider_session_stale_commit" || executor.inputs[0].Task.ProviderCommandID != "provider_command_stale_commit" {
		t.Fatalf("cold stdin recovery = %+v; want original launch target/provider identity", executor.inputs)
	}
	if len(executor.cancels) != 1 || executor.cancels[0].Target.ProviderSandboxID != "provider_session_stale_commit" || executor.cancels[0].Task.ProviderCommandID != "provider_command_stale_commit" {
		t.Fatalf("cold cancel recovery = %+v; want original launch target/provider identity", executor.cancels)
	}
	var terminalStatus string
	var terminalEventID sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id FROM session_background_tasks
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_stale_commit' AND task_id = 'task_bridge_stale_commit'`,
	).Scan(&terminalStatus, &terminalEventID); err != nil {
		t.Fatalf("read released-sandbox terminal CAS: %v", err)
	}
	if terminalStatus != "cancelled" || !terminalEventID.Valid {
		t.Fatalf("released-sandbox terminal CAS = %q/%v; want cancelled durable winner", terminalStatus, terminalEventID)
	}
}

func TestPostgreSQLBridgeAPIStoreCommandCleanupWinnerDefeatsLateHelperPayload(t *testing.T) {
	for _, operation := range []string{"poll", "stdin", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_cleanup_winner_" + operation
			threadID := "thr_bridge_cleanup_winner_" + operation
			bindingID := "bind_bridge_cleanup_winner_" + operation
			taskID := "task_bridge_cleanup_winner_" + operation
			sourceID := "evt_bridge_cleanup_winner_" + operation
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_"+operation)
			seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, taskID, sourceID)

			executor := &recordingSandboxToolExecutor{
				commandResult: SandboxCommandResult{ResultJSON: `{"status":"success","result":{"stdout":{"text":"loser"}}}`, TerminalStatus: "completed"},
				inputResult:   SandboxCommandResult{ResultJSON: `{"status":"accepted","loser":true}`},
				cancelResult:  SandboxCommandResult{ResultJSON: `{"status":"cancelled","loser":true}`, TerminalStatus: "cancelled"},
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 31, 0, time.UTC) }
			store.SandboxToolExecutor = executor
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+operation)
			scope.RequestId = "req_cleanup_winner_" + operation
			winnerJSON := `{"status":"cancelled","result":{"stdout":{"text":"winner","truncated":false},"stderr":{"text":"","truncated":false}}}`
			cleanupWin := func() {
				if err := store.withScopeTx(context.Background(), scope, "test.cleanup_winner", func(tx *dbconnect.Tx) error {
					settled, _, err := settleBackgroundTaskTx(context.Background(), tx, scope, taskID, sourceID, "cancelled_by_cleanup", winnerJSON, store.now())
					if err == nil && !settled {
						return errors.New("cleanup did not win terminal CAS")
					}
					return err
				}); err != nil {
					t.Fatalf("settle cleanup winner: %v", err)
				}
			}
			switch operation {
			case "poll":
				executor.onRead = func(SandboxCommandReference) { cleanupWin() }
			case "stdin":
				executor.onInput = func(SandboxCommandInput) { cleanupWin() }
			case "cancel":
				executor.onCancel = func(SandboxCommandCancel) { cleanupWin() }
			}

			call := func() (string, error) {
				switch operation {
				case "poll":
					response, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{Scope: scope, TaskId: taskID, ToolUseEventId: "evt_poll_cleanup_winner"})
					return response.GetResultJson(), err
				case "stdin":
					response, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{Scope: scope, TaskId: taskID, InputJson: `{"chars":"late\n"}`})
					return response.GetResultJson(), err
				default:
					response, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{Scope: scope, TaskId: taskID, ToolUseEventId: "evt_cancel_cleanup_winner", Reason: "late"})
					return response.GetResultJson(), err
				}
			}
			first, err := call()
			if err != nil || !strings.Contains(first, `"text":"winner"`) || strings.Contains(first, "loser") {
				t.Fatalf("%s cleanup race result = %s err=%v; want durable winner without losing helper payload", operation, first, err)
			}
			replay, err := call()
			if err != nil || replay != first {
				t.Fatalf("%s cleanup replay = %s err=%v; want durable winner", operation, replay, err)
			}
			helperCalls := len(executor.reads) + len(executor.inputs) + len(executor.cancels)
			if helperCalls != 1 {
				t.Fatalf("%s helper calls = %d; want one losing attempt and zero on replay", operation, helperCalls)
			}
			var statusValue string
			var eventID sql.NullString
			var eventCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = $1 AND task_id = $2`, sessionID, taskID,
			).Scan(&statusValue, &eventID); err != nil {
				t.Fatalf("read cleanup winner task: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = $1 AND type = 'runtime_notification'`, sessionID,
			).Scan(&eventCount); err != nil {
				t.Fatalf("count cleanup winner events: %v", err)
			}
			if statusValue != "cancelled_by_cleanup" || !eventID.Valid || eventCount != 1 {
				t.Fatalf("%s durable winner = %q/%v events=%d; want one cleanup terminal", operation, statusValue, eventID, eventCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreReadCommandReplayRequiresBackgroundTaskRow(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_read_fence", "thr_bridge_read_fence")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_read_fence", "bind_bridge_read_fence", 1, "pod_uid_read_fence")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_read_fence", "thr_bridge_read_fence", "bind_bridge_read_fence", 1, "pod_uid_read_fence")
	scope.RequestId = "req_bridge_read_fence"
	taskID := "task_bridge_read_fence"
	sourceToolUseEventID := "evt_bridge_read_fence"
	_, key, payloadHashPart := readCommandResultOwnerIdentity(sourceToolUseEventID, taskID, false, 0)
	requestHash := bridgeRequestHash(bridgeOpReadCommandResult, key, payloadHashPart)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, idempotency_key,
			request_hash, ack_status, result_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'committed', $7, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		"default",
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		bridgeOpReadCommandResult,
		key,
		requestHash,
		`{"status":"success","result":{"task_id":"task_bridge_read_fence"}}`,
	); err != nil {
		t.Fatalf("seed read command operation: %v", err)
	}

	_, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          scope,
		TaskId:         taskID,
		ToolUseEventId: sourceToolUseEventID,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("ReadCommandResult replay without task row error = %v; want NotFound", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommandFollowUpsUseStoredRecoveryIdentity(t *testing.T) {
	tests := []struct {
		name      string
		requestID string
		call      func(context.Context, *PostgreSQLBridgeAPIStore, *bridgev1.RuntimeScope) (string, error)
		calls     func(*recordingSandboxToolExecutor) int
	}{
		{
			name:      "read",
			requestID: "req_cmd_gate_read",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.ReadCommandResult(ctx, &bridgev1.ReadCommandResultRequest{
					Scope:          scope,
					TaskId:         "task_bridge_cmd_gate",
					ToolUseEventId: "evt_bridge_cmd_gate_read",
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.reads) },
		},
		{
			name:      "stdin",
			requestID: "req_cmd_gate_stdin",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.SendCommandInput(ctx, &bridgev1.SendCommandInputRequest{
					Scope:     scope,
					TaskId:    "task_bridge_cmd_gate",
					InputJson: `{"chars":"hello\n"}`,
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.inputs) },
		},
		{
			name:      "cancel",
			requestID: "req_cmd_gate_cancel",
			call: func(ctx context.Context, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope) (string, error) {
				response, err := store.CancelCommand(ctx, &bridgev1.CancelCommandRequest{
					Scope:  scope,
					TaskId: "task_bridge_cmd_gate",
					Reason: "user_cancel",
				})
				if err != nil {
					return "", err
				}
				return response.GetResultJson(), nil
			},
			calls: func(executor *recordingSandboxToolExecutor) int { return len(executor.cancels) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_cmd_gate_" + tc.name
			threadID := "thr_bridge_cmd_gate_" + tc.name
			bindingID := "bind_bridge_cmd_gate_" + tc.name
			preparationID := "prep_bridge_cmd_gate_" + tc.name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_cmd_gate_"+tc.name)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationID)
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:44:30Z")
			seedBridgeAPIResourceRootsJSON(t, admin, "default", sessionID, preparationID, `[{"path":"/workspace/data/report.csv","mode":"read"}]`)
			seedBridgeAPIResourceCredentialExpiresAt(t, admin, "default", sessionID, preparationID, "2026-01-01T01:00:00Z")
			seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, "task_bridge_cmd_gate", "evt_tool_cmd_gate")

			executor := &recordingSandboxToolExecutor{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC) }
			store.SandboxToolExecutor = executor

			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_cmd_gate_"+tc.name)
			scope.RequestId = tc.requestID
			resultJSON, err := tc.call(context.Background(), store, scope)
			if err != nil {
				t.Fatalf("%s follow-up: %v", tc.name, err)
			}
			if got := tc.calls(executor); got != 1 {
				t.Fatalf("helper %s calls = %d; want stored running-task recovery despite preparation rotation", tc.name, got)
			}

			replay, err := tc.call(context.Background(), store, scope)
			if err != nil {
				t.Fatalf("%s replay: %v", tc.name, err)
			}
			if replay != resultJSON || tc.calls(executor) != 1 {
				t.Fatalf("%s replay result=%s calls=%d; want durable operation replay without another helper call", tc.name, replay, tc.calls(executor))
			}
			if tc.name == "stdin" {
				var metadata string
				if err := admin.QueryRowContext(context.Background(),
					`SELECT provider_command_metadata_json
					   FROM session_background_tasks
					  WHERE workspace_id = 'default'
					    AND session_id = $1
					    AND task_id = 'task_bridge_cmd_gate'`,
					sessionID,
				).Scan(&metadata); err != nil {
					t.Fatalf("read stdin metadata: %v", err)
				}
				if strings.Contains(metadata, "stdin_write_seq") {
					t.Fatalf("provider metadata = %s; want no write_seq allocated while readiness is gated", metadata)
				}
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreSendCommandInputTerminalSettlesPendingOperation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_stdin_terminal", "thr_bridge_stdin_terminal")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_stdin_terminal", "bind_bridge_stdin_terminal", 1, "pod_uid_stdin_terminal")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_stdin_terminal", "prep_bridge_stdin_terminal")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_stdin_terminal", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_stdin_terminal"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_stdin_terminal",
				ProviderSessionID:           "provider_session_stdin_terminal",
				ProviderCommandID:           "provider_command_stdin_terminal",
				ProviderCommandMetadataJSON: `{}`,
			},
		},
		inputResult: SandboxCommandResult{
			ResultJSON:     `{"status":"error","error_kind":"task_exited","result":{"task_id":"task_bridge_stdin_terminal","exit_code":0,"stdout":{"text":"done","total_bytes":4,"total_lines":1,"returned_bytes":4,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false}}}`,
			TerminalStatus: "completed",
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	if _, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_stdin_terminal", "thr_bridge_stdin_terminal", "bind_bridge_stdin_terminal", 1, "pod_uid_stdin_terminal"),
		ToolUseEventId:      "evt_tool_stdin_terminal",
		NormalizedInputHash: sha256Hex(`{"cmd":"cat"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"cat"}`,
	}); err != nil {
		t.Fatalf("RunTool stdin terminal seed: %v", err)
	}

	inputScope := bridgeAPIScope("sesn_bridge_stdin_terminal", "thr_bridge_stdin_terminal", "bind_bridge_stdin_terminal", 1, "pod_uid_stdin_terminal")
	inputScope.RequestId = "req_bridge_stdin_terminal_input"
	response, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:     inputScope,
		TaskId:    "task_bridge_stdin_terminal",
		InputJson: `{"chars":"exit\n"}`,
	})
	if err != nil {
		t.Fatalf("SendCommandInput terminal: %v", err)
	}
	if testJSONPathString(t, response.GetResultJson(), "status") != "error" ||
		testJSONPathString(t, response.GetResultJson(), "error_kind") != "task_exited" ||
		testJSONPathString(t, response.GetResultJson(), "result.task_id") != "task_bridge_stdin_terminal" ||
		testJSONPathString(t, response.GetResultJson(), "result.stdout.text") != "done" {
		t.Fatalf("terminal stdin result = %s; want helper task_exited terminal facts", response.GetResultJson())
	}
	if response.GetWriteSeq() != 1 {
		t.Fatalf("terminal stdin write_seq = %d; want 1", response.GetWriteSeq())
	}
	if len(executor.inputs) != 1 {
		t.Fatalf("stdin executor calls = %d; want 1", len(executor.inputs))
	}
	var taskStatus string
	var notificationEvents int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_stdin_terminal'
		    AND task_id = 'task_bridge_stdin_terminal'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read stdin terminal task: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_stdin_terminal'
		    AND type = 'runtime_notification'`).Scan(&notificationEvents); err != nil {
		t.Fatalf("read stdin terminal notification count: %v", err)
	}
	if taskStatus != "completed" || notificationEvents != 1 {
		t.Fatalf("stdin terminal status/events = %q/%d; want completed/1", taskStatus, notificationEvents)
	}
	var notificationDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_stdin_terminal'
		    AND type = 'runtime_notification'`).Scan(&notificationDataJSON); err != nil {
		t.Fatalf("read stdin terminal notification data: %v", err)
	}
	assertNoTaskOutputPaths(t, notificationDataJSON)
	var storedResult string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT result_json
		   FROM session_bridge_operations
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_stdin_terminal'
		    AND operation = $1
		    AND idempotency_key = $2`,
		bridgeOpSendCommandInput,
		"task_bridge_stdin_terminal:req_bridge_stdin_terminal_input",
	).Scan(&storedResult); err != nil {
		t.Fatalf("read stdin terminal operation: %v", err)
	}
	if storedResult != response.GetResultJson() || strings.Contains(storedResult, "_tetral_pending_command_input") {
		t.Fatalf("stored stdin terminal result = %s; want final helper result", storedResult)
	}

	replay, err := store.SendCommandInput(context.Background(), &bridgev1.SendCommandInputRequest{
		Scope:     inputScope,
		TaskId:    "task_bridge_stdin_terminal",
		InputJson: `{"chars":"exit\n"}`,
	})
	if err != nil {
		t.Fatalf("SendCommandInput terminal replay: %v", err)
	}
	if replay.GetResultJson() != response.GetResultJson() || replay.GetWriteSeq() != 1 || len(executor.inputs) != 1 {
		t.Fatalf("terminal stdin replay = %s write_seq=%d calls=%d; want stored result without helper", replay.GetResultJson(), replay.GetWriteSeq(), len(executor.inputs))
	}
}

func TestPostgreSQLBridgeAPIStoreCancelCommandHelperErrorDoesNotSettleTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cancel_lost", "thr_bridge_cancel_lost")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cancel_lost", "bind_bridge_cancel_lost", 1, "pod_uid_cancel_lost")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cancel_lost", "prep_bridge_cancel_lost")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cancel_lost", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_cancel_lost"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_cancel_lost",
				ProviderSessionID:           "provider_session_cancel_lost",
				ProviderCommandID:           "provider_command_cancel_lost",
				ProviderCommandMetadataJSON: `{}`,
			},
		},
		cancelResult: SandboxCommandResult{ResultJSON: `{"status":"error","error_kind":"task_lost","result":{}}`},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	if _, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_cancel_lost", "thr_bridge_cancel_lost", "bind_bridge_cancel_lost", 1, "pod_uid_cancel_lost"),
		ToolUseEventId:      "evt_tool_cancel_lost",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 10"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 10"}`,
	}); err != nil {
		t.Fatalf("RunTool cancel lost seed: %v", err)
	}

	cancelScope := bridgeAPIScope("sesn_bridge_cancel_lost", "thr_bridge_cancel_lost", "bind_bridge_cancel_lost", 1, "pod_uid_cancel_lost")
	cancelScope.RequestId = "req_bridge_cancel_lost"
	response, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope:          cancelScope,
		TaskId:         "task_bridge_cancel_lost",
		Reason:         "user_cancel",
		ToolUseEventId: "evt_bridge_cancel_followup",
	})
	if err != nil {
		t.Fatalf("CancelCommand helper task_lost: %v", err)
	}
	if testJSONPathString(t, response.GetResultJson(), "status") != "error" ||
		testJSONPathString(t, response.GetResultJson(), "error_kind") != "task_lost" {
		t.Fatalf("cancel helper error result = %s; want task_lost error result", response.GetResultJson())
	}
	if len(executor.cancels) != 1 {
		t.Fatalf("cancel executor calls = %d; want 1", len(executor.cancels))
	}
	if executor.cancels[0].ToolUseEventID != "evt_bridge_cancel_followup" {
		t.Fatalf("cancel tool use event id = %q; want current follow-up event", executor.cancels[0].ToolUseEventID)
	}
	var taskStatus string
	var terminalEventID sql.NullString
	var notificationEvents int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cancel_lost'
		    AND task_id = 'task_bridge_cancel_lost'`).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read cancel helper error task: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cancel_lost'
		    AND type = 'runtime_notification'`).Scan(&notificationEvents); err != nil {
		t.Fatalf("read cancel helper error notifications: %v", err)
	}
	if taskStatus != "running" || terminalEventID.Valid || notificationEvents != 0 {
		t.Fatalf("cancel helper error task status=%q terminal=%v notifications=%d; want running/no terminal/no notification",
			taskStatus, terminalEventID.Valid, notificationEvents)
	}
}

func TestPostgreSQLBridgeAPIStoreCancelCommandTerminalProjectsTaskOutputPaths(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_cancel_terminal", "thr_bridge_cancel_terminal")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_cancel_terminal", "bind_bridge_cancel_terminal", 1, "pod_uid_cancel_terminal")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_cancel_terminal", "prep_bridge_cancel_terminal")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_cancel_terminal", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_cancel_terminal"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_cancel_terminal",
				ProviderSessionID:           "provider_session_cancel_terminal",
				ProviderCommandID:           "provider_command_cancel_terminal",
				ProviderCommandMetadataJSON: `{}`,
			},
		},
		cancelResult: SandboxCommandResult{
			ResultJSON:     `{"status":"success","result":{"task_id":"task_bridge_cancel_terminal","signal":"killed","cancelled":true,"stdout":{"text":"partial","total_bytes":7,"total_lines":1,"returned_bytes":7,"truncated":false},"stderr":{"text":"","total_bytes":0,"total_lines":0,"returned_bytes":0,"truncated":false}}}`,
			TerminalStatus: "cancelled",
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	if _, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_cancel_terminal", "thr_bridge_cancel_terminal", "bind_bridge_cancel_terminal", 1, "pod_uid_cancel_terminal"),
		ToolUseEventId:      "evt_tool_cancel_terminal",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 10"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 10"}`,
	}); err != nil {
		t.Fatalf("RunTool cancel terminal seed: %v", err)
	}

	cancelScope := bridgeAPIScope("sesn_bridge_cancel_terminal", "thr_bridge_cancel_terminal", "bind_bridge_cancel_terminal", 1, "pod_uid_cancel_terminal")
	cancelScope.RequestId = "req_bridge_cancel_terminal"
	if _, err := store.CancelCommand(context.Background(), &bridgev1.CancelCommandRequest{
		Scope:  cancelScope,
		TaskId: "task_bridge_cancel_terminal",
		Reason: "user_cancel",
	}); err != nil {
		t.Fatalf("CancelCommand terminal: %v", err)
	}
	var taskStatus string
	var notificationDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cancel_terminal'
		    AND task_id = 'task_bridge_cancel_terminal'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read cancel terminal task: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_cancel_terminal'
		    AND type = 'runtime_notification'`).Scan(&notificationDataJSON); err != nil {
		t.Fatalf("read cancel terminal notification data: %v", err)
	}
	if taskStatus != "cancelled" {
		t.Fatalf("cancel terminal status=%q notification=%s; want cancelled", taskStatus, notificationDataJSON)
	}
	assertNoTaskOutputPaths(t, notificationDataJSON)
}

func TestPostgreSQLBridgeAPIStoreDeferredReadDoesNotSettleBackgroundTask(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_deferred_read", "thr_bridge_deferred_read")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_deferred_read", "bind_bridge_deferred_read", 1, "pod_uid_deferred_read")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_deferred_read", "prep_bridge_deferred_read")
	seedBridgeAPIResourceRootsJSON(t, admin, "default", "sesn_bridge_deferred_read", "prep_bridge_deferred_read", `[{"path":"/mnt/session/uploads/input.txt","mode":"read"}]`)
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_deferred_read", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_deferred_read"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_deferred_read",
				ProviderSessionID:           "provider_session_deferred_read",
				ProviderCommandID:           "provider_command_deferred_read",
				ProviderCommandMetadataJSON: `{}`,
			},
		},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	if _, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_deferred_read", "thr_bridge_deferred_read", "bind_bridge_deferred_read", 1, "pod_uid_deferred_read"),
		ToolUseEventId:      "evt_tool_deferred_read",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 1"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 1"}`,
	}); err != nil {
		t.Fatalf("RunTool deferred read seed: %v", err)
	}

	executor.commandResult = SandboxCommandResult{ResultJSON: `{"status":"completed","task_id":"task_bridge_deferred_read"}`, TerminalStatus: "completed"}
	readResponse, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:                   bridgeAPIScope("sesn_bridge_deferred_read", "thr_bridge_deferred_read", "bind_bridge_deferred_read", 1, "pod_uid_deferred_read"),
		TaskId:                  "task_bridge_deferred_read",
		DeferTerminalSettlement: true,
		ToolUseEventId:          "evt_bridge_deferred_read_poll",
	})
	if err != nil {
		t.Fatalf("deferred ReadCommandResult: %v", err)
	}
	if readResponse.GetResultJson() != `{"status":"completed","task_id":"task_bridge_deferred_read"}` {
		t.Fatalf("deferred read result = %s", readResponse.GetResultJson())
	}
	if len(executor.reads) != 1 || executor.reads[0].Target.ResourceRootsJSON != `[{"path":"/mnt/session/uploads/input.txt","mode":"read"}]` {
		t.Fatalf("deferred read target roots = %+v; want latest preparation roots", executor.reads)
	}
	var taskStatus string
	var notificationEvents int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_deferred_read' AND task_id = 'task_bridge_deferred_read'`).Scan(&taskStatus); err != nil {
		t.Fatalf("read deferred task status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_deferred_read' AND type = 'runtime_notification'`).Scan(&notificationEvents); err != nil {
		t.Fatalf("read deferred notification events: %v", err)
	}
	if taskStatus != "running" || notificationEvents != 0 {
		t.Fatalf("deferred read status=%q notifications=%d; want running/0 before Runtime ACK", taskStatus, notificationEvents)
	}
}

func TestCanonicalTaskNotificationPayloadRejectsNullRequiredStreamFields(t *testing.T) {
	_, err := canonicalTaskNotificationPayloadJSON(
		"task_bridge_null",
		"sevt_tool_null",
		"completed",
		`{"status":"completed","stdout":{"text":null,"truncated":false}}`,
	)
	if err == nil {
		t.Fatal("canonical task notification payload accepted null stdout.text; want schema error")
	}
}

func TestCanonicalTaskNotificationPayloadRequiresBothStreams(t *testing.T) {
	for _, resultJSON := range []string{
		`{"status":"completed","stderr":{"text":"","truncated":false}}`,
		`{"status":"completed","stdout":{"text":"","truncated":false}}`,
	} {
		if _, err := canonicalTaskNotificationPayloadJSON("task_bridge_stream", "sevt_tool_stream", "completed", resultJSON); err == nil {
			t.Fatalf("canonical task notification payload accepted missing required stream: %s", resultJSON)
		}
	}
}

func TestCanonicalTaskNotificationPayloadFitsRuntimeRail(t *testing.T) {
	resultJSON := fmt.Sprintf(
		`{"status":"completed","stdout":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":5000},"stderr":{"text":%q,"truncated":false,"total_bytes":51200,"total_lines":6000}}`,
		strings.Repeat("out-\u754c", 10240),
		strings.Repeat("err-\u754c", 10240),
	)

	payloadJSON, err := canonicalTaskNotificationPayloadJSON("task_bridge_large", "sevt_tool_large", "completed", resultJSON)
	if err != nil {
		t.Fatalf("canonicalTaskNotificationPayloadJSON: %v", err)
	}
	if len([]byte(payloadJSON)) > runtimeTaskNotificationPayloadMaxBytes {
		t.Fatalf("canonical payload bytes = %d; want <= %d", len([]byte(payloadJSON)), runtimeTaskNotificationPayloadMaxBytes)
	}
	var payload struct {
		Stdout struct {
			Text          string `json:"text"`
			Truncated     bool   `json:"truncated"`
			OriginalBytes int64  `json:"original_bytes"`
			OriginalLines int64  `json:"original_lines"`
		} `json:"stdout"`
		Stderr struct {
			Text          string `json:"text"`
			Truncated     bool   `json:"truncated"`
			OriginalBytes int64  `json:"original_bytes"`
			OriginalLines int64  `json:"original_lines"`
		} `json:"stderr"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode canonical payload: %v", err)
	}
	if !utf8.ValidString(payload.Stdout.Text) || !utf8.ValidString(payload.Stderr.Text) {
		t.Fatal("canonical payload split a UTF-8 sequence")
	}
	if !payload.Stdout.Truncated || !payload.Stderr.Truncated {
		t.Fatalf("truncated flags = stdout:%t stderr:%t; want both true", payload.Stdout.Truncated, payload.Stderr.Truncated)
	}
	if payload.Stdout.OriginalBytes != 51200 || payload.Stdout.OriginalLines != 5000 || payload.Stderr.OriginalBytes != 51200 || payload.Stderr.OriginalLines != 6000 {
		t.Fatalf("original totals changed during fit: stdout=%+v stderr=%+v", payload.Stdout, payload.Stderr)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitTaskNotificationProjectsRuntimeNotification(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_notify", "2026-01-01T00:00:00Z")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", "task_bridge_notify", "sevt_tool_notify")
	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "rin_bridge_task_notify", "bind_bridge_task_notify", "pod_uid_task_notify")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	request := &bridgev1.CommitTaskNotificationResultRequest{
		Scope:          bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		RuntimeInputId: "rin_bridge_task_notify",
		TaskId:         "task_bridge_notify",
		ResultJson:     `{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"","truncated":false},"stderr":{"text":"","truncated":false},"exit_code":null}`,
	}
	response, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	replay, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}

	var taskStatus string
	var terminalEventID sql.NullString
	var inboxStatus string
	var notificationMessages int
	var notificationEvents int
	var notificationDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, terminal_event_id
		   FROM session_background_tasks
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND task_id = 'task_bridge_notify'`).Scan(&taskStatus, &terminalEventID); err != nil {
		t.Fatalf("read task settlement: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_runtime_inbox
		  WHERE workspace_id = 'default'
		    AND runtime_input_id = 'rin_bridge_task_notify'`).Scan(&inboxStatus); err != nil {
		t.Fatalf("read task inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationMessages); err != nil {
		t.Fatalf("read runtime notification messages: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationDataJSON); err != nil {
		t.Fatalf("read runtime notification data: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND type = 'runtime_notification'
		    AND visibility = 'internal'`).Scan(&notificationEvents); err != nil {
		t.Fatalf("read runtime notification events: %v", err)
	}
	if taskStatus != "expired" || !terminalEventID.Valid || inboxStatus != "committed" || notificationMessages != 1 || notificationEvents != 1 {
		t.Fatalf("settlement status=%q terminal=%v inbox=%q messages=%d events=%d; want expired/event/committed/1/1",
			taskStatus, terminalEventID.Valid, inboxStatus, notificationMessages, notificationEvents)
	}
	if response.GetRuntimeMessageJson() != notificationDataJSON {
		t.Fatalf("runtime_message_json = %s; want projected session message %s", response.GetRuntimeMessageJson(), notificationDataJSON)
	}
	if replay.GetRuntimeMessageJson() != notificationDataJSON {
		t.Fatalf("replay runtime_message_json = %s; want projected session message %s", replay.GetRuntimeMessageJson(), notificationDataJSON)
	}
	_, err = admin.ExecContext(context.Background(),
		`UPDATE session_bridge_operations
		    SET result_json = $1
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND operation = 'commit_task_notification_result'
		    AND idempotency_key = 'task_bridge_notify:rin_bridge_task_notify'`,
		request.GetResultJson(),
	)
	if err != nil {
		t.Fatalf("downgrade task notification operation result to legacy shape: %v", err)
	}
	legacyReplay, err := store.CommitTaskNotificationResult(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitTaskNotificationResult legacy replay: %v", err)
	}
	if legacyReplay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		legacyReplay.GetRuntimeMessageJson() != notificationDataJSON {
		t.Fatalf("legacy replay = %#v projection=%s; want duplicate with projected session message", legacyReplay.GetAck(), legacyReplay.GetRuntimeMessageJson())
	}
	var notificationMessage struct {
		Role   string `json:"role"`
		Origin string `json:"origin"`
		Parts  []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(notificationDataJSON), &notificationMessage); err != nil {
		t.Fatalf("decode runtime notification message: %v", err)
	}
	if notificationMessage.Role != "user" || notificationMessage.Origin != "runtime" || len(notificationMessage.Parts) != 1 ||
		notificationMessage.Parts[0].Type != "text" ||
		!strings.Contains(notificationMessage.Parts[0].Text, `"task_id":"task_bridge_notify"`) ||
		strings.Contains(notificationMessage.Parts[0].Text, `"output_paths"`) ||
		strings.Contains(notificationMessage.Parts[0].Text, `/tmp/tetral-runtime/tasks/`) ||
		strings.Contains(notificationDataJSON, "provider_") {
		t.Fatalf("runtime notification data = %s; want runtime-origin RuntimeMessage without task output paths or provider metadata", notificationDataJSON)
	}

	seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_notify", "thr_bridge_task_notify", "rin_bridge_task_notify_late", "bind_bridge_task_notify", "pod_uid_task_notify")
	lateRequest := &bridgev1.CommitTaskNotificationResultRequest{
		Scope:          bridgeAPIScope("sesn_bridge_task_notify", "thr_bridge_task_notify", "bind_bridge_task_notify", 1, "pod_uid_task_notify"),
		RuntimeInputId: "rin_bridge_task_notify_late",
		TaskId:         "task_bridge_notify",
		ResultJson:     `{"task_id":"task_bridge_notify","source_tool_use_event_id":"sevt_tool_notify","status":"expired","stdout":{"text":"late","truncated":false},"stderr":{"text":"","truncated":false}}`,
	}
	late, err := store.CommitTaskNotificationResult(context.Background(), lateRequest)
	if err != nil {
		t.Fatalf("late CommitTaskNotificationResult: %v", err)
	}
	if late.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || late.GetAck().GetErrorCode() != "task_notification_stale" {
		t.Fatalf("late task notification ack = %#v; want stale rejected", late.GetAck())
	}
	if late.GetRuntimeMessageJson() != "" {
		t.Fatalf("late task notification runtime_message_json = %s; want empty stale ACK projection", late.GetRuntimeMessageJson())
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_task_notify'
		    AND kind = 'runtime_notification'`).Scan(&notificationMessages); err != nil {
		t.Fatalf("read runtime notification messages after late notification: %v", err)
	}
	if notificationMessages != 1 {
		t.Fatalf("late notification wrote %d runtime notification messages; want still 1", notificationMessages)
	}
	replayedLate, err := store.CommitTaskNotificationResult(context.Background(), lateRequest)
	if err != nil {
		t.Fatalf("late CommitTaskNotificationResult replay: %v", err)
	}
	if replayedLate.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED || replayedLate.GetAck().GetErrorCode() != "task_notification_stale" {
		t.Fatalf("late task notification replay ack = %#v; want stale rejected", replayedLate.GetAck())
	}
	if replayedLate.GetRuntimeMessageJson() != "" {
		t.Fatalf("late task notification replay runtime_message_json = %s; want empty stale ACK projection", replayedLate.GetRuntimeMessageJson())
	}
}

func TestPostgreSQLBridgeAPIStoreCommitTaskNotificationRequiresSettlementFences(t *testing.T) {
	t.Run("source tool use mismatch is stale without settlement", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_source_fence", "bind_bridge_task_source_fence", 1, "pod_uid_task_source_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_source_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "bind_bridge_task_source_fence", "task_source_fence", "sevt_tool_original")
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "rin_task_source_fence", "bind_bridge_task_source_fence", "pod_uid_task_source_fence")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{
			Scope:          bridgeAPIScope("sesn_bridge_task_source_fence", "thr_bridge_task_source_fence", "bind_bridge_task_source_fence", 1, "pod_uid_task_source_fence"),
			RuntimeInputId: "rin_task_source_fence",
			TaskId:         "task_source_fence",
			ResultJson:     `{"task_id":"task_source_fence","source_tool_use_event_id":"sevt_tool_other","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		})
		if err != nil {
			t.Fatalf("CommitTaskNotificationResult source mismatch: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_REJECTED ||
			response.GetAck().GetErrorCode() != "task_notification_stale" {
			t.Fatalf("source mismatch ack = %#v; want stale rejected", response.GetAck())
		}
		assertBackgroundTaskStillRunningWithoutNotification(t, admin, "sesn_bridge_task_source_fence", "task_source_fence")
	})

	t.Run("released launch sandbox still permits terminal settlement", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", 1, "pod_uid_task_sandbox_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_sandbox_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", "task_sandbox_fence", "sevt_tool_sandbox")
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "rin_task_sandbox_fence", "bind_bridge_task_sandbox_fence", "pod_uid_task_sandbox_fence")
		if _, err := admin.ExecContext(context.Background(), `UPDATE sandboxes SET status = 'released' WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_sandbox_fence'`); err != nil {
			t.Fatalf("release sandbox: %v", err)
		}

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		response, err := store.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{
			Scope:          bridgeAPIScope("sesn_bridge_task_sandbox_fence", "thr_bridge_task_sandbox_fence", "bind_bridge_task_sandbox_fence", 1, "pod_uid_task_sandbox_fence"),
			RuntimeInputId: "rin_task_sandbox_fence",
			TaskId:         "task_sandbox_fence",
			ResultJson:     `{"task_id":"task_sandbox_fence","source_tool_use_event_id":"sevt_tool_sandbox","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		})
		if err != nil {
			t.Fatalf("CommitTaskNotificationResult sandbox mismatch: %v", err)
		}
		if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED || response.GetRuntimeMessageJson() == "" {
			t.Fatalf("released sandbox ack = %#v message=%s; want committed terminal winner", response.GetAck(), response.GetRuntimeMessageJson())
		}
		var taskStatus string
		var terminalEventID sql.NullString
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, terminal_event_id FROM session_background_tasks WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_task_sandbox_fence' AND task_id = 'task_sandbox_fence'`,
		).Scan(&taskStatus, &terminalEventID); err != nil {
			t.Fatalf("read released sandbox terminal task: %v", err)
		}
		if taskStatus != "completed" || !terminalEventID.Valid {
			t.Fatalf("released sandbox terminal task = %q/%v; want completed durable event", taskStatus, terminalEventID)
		}
	})

	t.Run("runtime inbox target pod mismatch is not deliverable", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence")
		seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_task_inbox_fence", "2026-01-01T00:00:00Z")
		seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", "task_inbox_fence", "sevt_tool_inbox")
		seedBridgeAPITaskNotificationInbox(t, admin, "default", "sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "rin_task_inbox_fence", "bind_bridge_task_inbox_fence", "pod_uid_other")

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		_, err := store.CommitTaskNotificationResult(context.Background(), &bridgev1.CommitTaskNotificationResultRequest{
			Scope:          bridgeAPIScope("sesn_bridge_task_inbox_fence", "thr_bridge_task_inbox_fence", "bind_bridge_task_inbox_fence", 1, "pod_uid_task_inbox_fence"),
			RuntimeInputId: "rin_task_inbox_fence",
			TaskId:         "task_inbox_fence",
			ResultJson:     `{"task_id":"task_inbox_fence","source_tool_use_event_id":"sevt_tool_inbox","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("inbox pod mismatch err = %v; want FailedPrecondition", err)
		}
		assertBackgroundTaskStillRunningWithoutNotification(t, admin, "sesn_bridge_task_inbox_fence", "task_inbox_fence")
	})
}

func TestPostgreSQLBridgeAPIStoreReadCommandSynthesizesHelperFailure(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_read_helper_failure", "thr_bridge_read_helper_failure")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_read_helper_failure", "bind_bridge_read_helper_failure", 1, "pod_uid_read_helper_failure")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_read_helper_failure", "prep_bridge_read_helper_failure")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_read_helper_failure", "2026-01-01T00:00:00Z")

	executor := &recordingSandboxToolExecutor{
		execution: SandboxToolExecution{
			ResultJSON: `{"status":"running","task_id":"task_bridge_read_helper_failure"}`,
			BackgroundTask: &SandboxBackgroundTask{
				TaskID:                      "task_bridge_read_helper_failure",
				SourceToolUseEventID:        "evt_tool_read_helper_failure",
				ProviderSessionID:           "provider_session_read_helper_failure",
				ProviderCommandID:           "provider_command_read_helper_failure",
				ProviderCommandMetadataJSON: `{}`,
			},
		},
		commandErr: &sandboxdriver.HelperFailureError{Message: "poll envelope invalid"},
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.SandboxToolExecutor = executor

	if _, err := store.RunTool(context.Background(), &bridgev1.RunToolRequest{
		Scope:               bridgeAPIScope("sesn_bridge_read_helper_failure", "thr_bridge_read_helper_failure", "bind_bridge_read_helper_failure", 1, "pod_uid_read_helper_failure"),
		ToolUseEventId:      "evt_tool_read_helper_failure",
		NormalizedInputHash: sha256Hex(`{"cmd":"sleep 1"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"sleep 1"}`,
	}); err != nil {
		t.Fatalf("RunTool read helper failure seed: %v", err)
	}

	response, err := store.ReadCommandResult(context.Background(), &bridgev1.ReadCommandResultRequest{
		Scope:          bridgeAPIScope("sesn_bridge_read_helper_failure", "thr_bridge_read_helper_failure", "bind_bridge_read_helper_failure", 1, "pod_uid_read_helper_failure"),
		TaskId:         "task_bridge_read_helper_failure",
		ToolUseEventId: "evt_bridge_read_helper_failure_poll",
	})
	if err != nil {
		t.Fatalf("ReadCommandResult helper failure: %v", err)
	}
	assertRuntimeToolErrorCode(t, response.GetResultJson(), "helper_failure")
}
