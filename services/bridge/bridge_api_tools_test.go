package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/memory"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tools protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionCommitsBeforeIndependentWait(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID  = "default"
		sessionID    = "sesn_bridge_durable_tool"
		threadID     = "thr_bridge_durable_tool"
		toolUseID    = "evt_bridge_durable_tool"
		modelCallID  = "call_bridge_durable_tool"
		modelRequest = "mreq_bridge_durable_tool"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_bridge_durable_tool", 1, "pod_uid_bridge_durable_tool")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"printf ok"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = $2 WHERE workspace_id = $1 AND event_id = $3`,
		workspaceID, modelRequest, toolUseID,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, modelRequest, toolUseID, modelCallID, "exec_command")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	request := &bridgev1.AcceptSandboxExecutionRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_durable_tool", 1, "pod_uid_bridge_durable_tool"),
		ToolUseEventId:      toolUseID,
		ModelToolCallId:     modelCallID,
		NormalizedInputHash: sha256Hex(`{"cmd":"printf ok"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"printf ok"}`,
	}
	accepted, err := store.AcceptSandboxExecution(context.Background(), request)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution: %v", err)
	}
	if accepted.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("AcceptSandboxExecution ack = %s; want committed", accepted.GetAck().GetStatus())
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND tool_use_event_id = $4 AND tool_kind = 'sandbox_tool'
			    AND model_tool_call_id = $5 AND execution_state = 'pending'`,
		workspaceID, sessionID, threadID, toolUseID, modelCallID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("read accepted sandbox execution: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs
			  WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'
			    AND partition_key = $2 AND dedupe_key = $3 AND status = 'pending'`,
		workspaceID,
		queue.FormatSandboxExecutionPartitionKey(workspace.ID(workspaceID), sessionID, threadID, toolUseID),
		queue.FormatSandboxToolExecuteDedupeKey(workspace.ID(workspaceID), sessionID, threadID, toolUseID, 1),
	).Scan(&queueCount); err != nil {
		t.Fatalf("read accepted sandbox queue job: %v", err)
	}
	if executionCount != 1 || queueCount != 1 {
		t.Fatalf("accepted execution/queue counts = %d/%d; want 1/1", executionCount, queueCount)
	}

	type result struct {
		response *bridgev1.AwaitSandboxExecutionResponse
		err      error
	}
	waitResult := make(chan result, 1)
	go func() {
		response, err := store.AwaitSandboxExecution(context.Background(), request)
		waitResult <- result{response: response, err: err}
	}()
	select {
	case completed := <-waitResult:
		t.Fatalf("AwaitSandboxExecution returned before durable settlement: response=%+v err=%v", completed.response, completed.err)
	case <-time.After(50 * time.Millisecond):
	}

	const terminalResult = `{"status":"success","result":{"exit_code":0,"stdout":"ok"}}`
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'terminal_unconsumed', result_json = $5,
		        result_digest = $6, updated_at = '2026-01-01T00:00:31Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND tool_use_event_id = $4`,
		workspaceID, sessionID, threadID, toolUseID, terminalResult, sha256Hex(terminalResult),
	); err != nil {
		t.Fatalf("settle durable sandbox execution: %v", err)
	}
	select {
	case completed := <-waitResult:
		if completed.err != nil {
			t.Fatalf("AwaitSandboxExecution after durable settlement: %v", completed.err)
		}
		if completed.response.GetResultJson() != terminalResult {
			t.Fatalf("AwaitSandboxExecution response = %+v; want durable result", completed.response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AwaitSandboxExecution did not observe durable sandbox settlement")
	}

	replayRequest := proto.Clone(request).(*bridgev1.AcceptSandboxExecutionRequest)
	replayRequest.Scope.RequestId = "req_bridge_durable_tool_replay"
	replayStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	replay, err := replayStore.AcceptSandboxExecution(context.Background(), replayRequest)
	if err != nil {
		t.Fatalf("AcceptSandboxExecution replay from another Bridge store: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("AcceptSandboxExecution replay = %+v; want duplicate", replay)
	}
	lockTx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin unrelated Session lock: %v", err)
	}
	if _, err := lockTx.ExecContext(context.Background(),
		`SELECT id FROM sessions WHERE workspace_id = $1 AND id = $2 FOR UPDATE`, workspaceID, sessionID,
	); err != nil {
		_ = lockTx.Rollback()
		t.Fatalf("hold unrelated Session lock: %v", err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 500*time.Millisecond)
	replayedResult, err := replayStore.AwaitSandboxExecution(waitCtx, replayRequest)
	cancelWait()
	if rollbackErr := lockTx.Rollback(); rollbackErr != nil {
		t.Fatalf("rollback unrelated Session lock: %v", rollbackErr)
	}
	if err != nil || replayedResult.GetResultJson() != terminalResult {
		t.Fatalf("AwaitSandboxExecution behind unrelated Session lock = %+v, %v; want durable result", replayedResult, err)
	}
	conflict := proto.Clone(replayRequest).(*bridgev1.AcceptSandboxExecutionRequest)
	conflict.Scope.RequestId = "req_bridge_durable_tool_conflict"
	conflict.ModelToolCallId = "call_bridge_durable_tool_changed"
	if _, err := store.AcceptSandboxExecution(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("AcceptSandboxExecution model call conflict error = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionRejectsSettledToolUse(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		workspaceID = "default"
		sessionID   = "sesn_bridge_settled_tool"
		threadID    = "thr_bridge_settled_tool"
		toolUseID   = "evt_bridge_settled_tool"
	)
	seedBridgeAPISession(t, admin, workspaceID, sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, workspaceID, sessionID, "bind_bridge_settled_tool", 1, "pod_uid_bridge_settled_tool")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, toolUseID, 1, "agent.tool_use", `{"name":"exec_command","input":{"cmd":"true"},"evaluated_permission":"allow"}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_events SET model_request_id = 'mreq_bridge_settled_tool' WHERE workspace_id = $1 AND event_id = $2`,
		workspaceID, toolUseID,
	); err != nil {
		t.Fatalf("stamp durable tool-use model request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(t, admin, workspaceID, sessionID, threadID, "mreq_bridge_settled_tool", toolUseID, "call_bridge_settled_tool", "exec_command")
	seedBridgeAPIEvent(t, admin, workspaceID, sessionID, threadID, "evt_bridge_settled_result", 2, "agent.tool_result", `{"tool_use_event_id":"evt_bridge_settled_tool","content":[{"type":"text","text":"cancelled"}]}`)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.AcceptSandboxExecution(context.Background(), &bridgev1.AcceptSandboxExecutionRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_settled_tool", 1, "pod_uid_bridge_settled_tool"),
		ToolUseEventId:      toolUseID,
		ModelToolCallId:     "call_bridge_settled_tool",
		NormalizedInputHash: sha256Hex(`{"cmd":"true"}`),
		ToolName:            "exec_command",
		InputJson:           `{"cmd":"true"}`,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AcceptSandboxExecution settled Tool Use error = %v; want FailedPrecondition", err)
	}
	var executionCount, queueCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id = $1 AND session_id = $2 AND tool_use_event_id = $3`,
		workspaceID, sessionID, toolUseID,
	).Scan(&executionCount); err != nil {
		t.Fatalf("count fenced executions: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = 'sandbox_tool_execute'`, workspaceID,
	).Scan(&queueCount); err != nil {
		t.Fatalf("count fenced queue jobs: %v", err)
	}
	if executionCount != 0 || queueCount != 0 {
		t.Fatalf("fenced execution/queue counts = %d/%d; want 0/0", executionCount, queueCount)
	}
}

func TestInternalToolRepairKeyIsBoundedTupleSafeAndCrossLanguageStable(t *testing.T) {
	key := internalToolRepairKey("request", "call:a", "b")
	const expected = "internal_invalid_tool_6b53f75d29a34b47f5fdadebf740525a170464690d545d7deb4c9b859818b6fd"
	if key != expected {
		t.Fatalf("internalToolRepairKey() = %q; want %q", key, expected)
	}
	if key == internalToolRepairKey("request", "call", "a:b") {
		t.Fatal("internalToolRepairKey aliases distinct tuples")
	}
	if unicodeKey := internalToolRepairKey("请求", "调用", "工具"); len(unicodeKey) != len("internal_invalid_tool_")+sha256.Size*2 {
		t.Fatalf("unicode internalToolRepairKey length = %d; want fixed length", len(unicodeKey))
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInternalToolRepairPersistsReplaysAndLoads(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_repair", "thr_bridge_repair")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair")
	seedBridgeAPIInternalToolCallMessage(t, admin, "default", "sesn_bridge_repair", "thr_bridge_repair", "msg_invalid_call", "part_invalid_call", "call_repair", "unknown_tool", 1)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	repairKey := internalToolRepairKey("mreq_repair", "call_repair", "unknown_tool")
	request := &bridgev1.CommitInternalToolRepairRequest{
		Scope:           bridgeAPIScope("sesn_bridge_repair", "thr_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair"),
		ModelRequestId:  "mreq_repair",
		ModelToolCallId: "call_repair",
		ToolName:        "unknown_tool",
		Drafts: []*bridgev1.RuntimeMessageDraft{
			bridgeInternalToolRepairDraftForTest("default", "sesn_bridge_repair", "thr_bridge_repair", repairKey, "call_repair", "unknown_tool", "invalid tool"),
		},
	}
	response, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 {
		t.Fatalf("declaration receipts = %d; want 1", len(response.GetDeclaration().GetReceipts()))
	}
	receipt := response.GetDeclaration().GetReceipts()[0]
	if receipt.GetOperationKind() != bridgeOpCommitInternalToolRepair ||
		receipt.GetSourceKind() != "internal_tool_repair" ||
		receipt.GetSourceId() != repairKey ||
		len(receipt.GetEvents()) != 1 ||
		len(receipt.GetMessages()) != 1 ||
		len(receipt.GetMessages()[0].GetParts()) != 1 {
		t.Fatalf("repair receipt = %+v; want one complete repair declaration", receipt)
	}
	replay, err := store.CommitInternalToolRepair(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitInternalToolRepair replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if !proto.Equal(replay.GetDeclaration(), response.GetDeclaration()) {
		t.Fatal("replayed repair declaration differs from the committed receipt")
	}

	var messageID string
	var kind string
	var sourceEventID sql.NullString
	var storedRepairKey string
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT message_id, kind, source_event_id, repair_key, data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_repair'
		    AND session_thread_id = 'thr_bridge_repair'
		    AND repair_key IS NOT NULL`).Scan(&messageID, &kind, &sourceEventID, &storedRepairKey, &dataJSON); err != nil {
		t.Fatalf("read repair row: %v", err)
	}
	if messageID != receipt.GetMessages()[0].GetMessageId() || kind != "assistant" || !sourceEventID.Valid || storedRepairKey != repairKey {
		t.Fatalf("repair row message=%q kind=%q source=%v key=%q; want event-owned assistant repair", messageID, kind, sourceEventID.Valid, storedRepairKey)
	}
	var durable map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &durable); err != nil {
		t.Fatalf("unmarshal repair data: %v", err)
	}
	part := durable["parts"].([]any)[0].(map[string]any)
	if _, ok := part["toolUseEventId"]; ok {
		t.Fatalf("repair part persisted public toolUseEventId in %s", dataJSON)
	}

	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	contextResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_repair", "thr_bridge_repair", "bind_bridge_repair", 1, "pod_uid_repair"),
		RuntimeInputId: "rin_bridge_repair_reload",
	})
	if err != nil {
		t.Fatalf("LoadContext after repair: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(contextResponse.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse LoadContext after repair: %v", err)
	}
	if len(payload.Messages) != 2 ||
		!strings.Contains(string(payload.Messages[0]), `"id":"msg_invalid_call"`) ||
		!strings.Contains(string(payload.Messages[1]), `"id":"`+messageID+`"`) ||
		!strings.Contains(string(payload.Messages[1]), `"toolCallId":"call_repair"`) {
		t.Fatalf("LoadContext repair messages = %s; want invalid call followed by durable repair row", contextResponse.GetContextJson())
	}

	conflict := proto.Clone(request).(*bridgev1.CommitInternalToolRepairRequest)
	conflict.Drafts[0].Parts[0].PartJson = strings.ReplaceAll(
		conflict.Drafts[0].Parts[0].GetPartJson(),
		"invalid tool",
		"different invalid tool",
	)
	if _, err := store.CommitInternalToolRepair(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CommitInternalToolRepair err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitInternalToolRepairRejectsStaleBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_repair_stale", "thr_bridge_repair_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_repair_stale", "bind_bridge_repair_stale", 1, "pod_uid_repair_stale")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.CommitInternalToolRepair(context.Background(), &bridgev1.CommitInternalToolRepairRequest{
		Scope:           bridgeAPIScope("sesn_bridge_repair_stale", "thr_bridge_repair_stale", "bind_bridge_repair_stale", 2, "pod_uid_repair_stale"),
		ModelRequestId:  "mreq_repair_stale",
		ModelToolCallId: "call_repair_stale",
		ToolName:        "unknown_tool",
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeInternalToolRepairDraftForTest(
			"default",
			"sesn_bridge_repair_stale",
			"thr_bridge_repair_stale",
			internalToolRepairKey("mreq_repair_stale", "call_repair_stale", "unknown_tool"),
			"call_repair_stale",
			"unknown_tool",
			"invalid tool",
		)},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale CommitInternalToolRepair err = %v; want FailedPrecondition", err)
	}
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_repair_stale'`).Scan(&messageCount); err != nil {
		t.Fatalf("read stale repair message count: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("stale repair message count = %d; want 0", messageCount)
	}
}

func TestMemoryProjectionHeadsChunksBeyondPostgreSQLParameterLimit(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const storeID = "memstore_bridge_memory_many_paths"
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_many_paths", "thr_bridge_memory_many_paths")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_many_paths", storeID)
	seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_many_paths_first", "/paths/00000", "first")
	seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_many_paths_last", "/paths/65534", "last")
	paths := make([]string, 65_535)
	for index := range paths {
		paths[index] = fmt.Sprintf("/paths/%05d", index)
	}
	var heads map[string]memoryProjectionHead
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "bridge_api.memory_projection_many_paths", func(tx *dbconnect.Tx) error {
		var err error
		heads, err = memoryProjectionHeadsTx(context.Background(), tx, "default", storeID, paths)
		return err
	}); err != nil {
		t.Fatalf("memoryProjectionHeadsTx beyond parameter limit: %v", err)
	}
	if len(heads) != 2 || heads["/paths/00000"].Content != "first" || heads["/paths/65534"].Content != "last" {
		t.Fatalf("chunked heads = %+v; want first and last paths from separate chunks", heads)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryMutatesDurableMemoryAndReplays(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory", "thr_bridge_memory")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory", "memstore_bridge_memory")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
	create := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory", "thr_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory"),
		ToolUseEventId:      "evt_tool_memory_create",
		NormalizedInputHash: "hash_create",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/todo.md","content":"one"}`,
	}
	response, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory create: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	replay, err := store.RunMemory(context.Background(), create)
	if err != nil {
		t.Fatalf("RunMemory replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetResultJson() != response.GetResultJson() {
		t.Fatalf("RunMemory replay = %+v; want duplicate same result", replay)
	}
	reorderedReplay := proto.Clone(create).(*bridgev1.RunMemoryRequest)
	reorderedReplay.InputJson = `{"content":"one","path":"notes/todo.md","action":"create"}`
	reordered, err := store.RunMemory(context.Background(), reorderedReplay)
	if err != nil {
		t.Fatalf("RunMemory reordered JSON replay: %v", err)
	}
	if reordered.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || reordered.GetResultJson() != response.GetResultJson() {
		t.Fatalf("RunMemory reordered replay = %+v; want duplicate same result", reordered)
	}

	var currentContent string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = 'memstore_bridge_memory'
		    AND m.path = '/notes/todo.md'
		    AND m.deleted_at IS NULL`).Scan(&currentContent); err != nil {
		t.Fatalf("read created memory: %v", err)
	}
	if currentContent != "one" {
		t.Fatalf("created memory content = %q; want one", currentContent)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory"); count != 1 {
		t.Fatalf("memory versions after replay = %d; want 1", count)
	}

	conflict := proto.Clone(create).(*bridgev1.RunMemoryRequest)
	conflict.NormalizedInputHash = "different_hash"
	if _, err := store.RunMemory(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting RunMemory err = %v; want AlreadyExists", err)
	}

	replace := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory", "thr_bridge_memory", "bind_bridge_memory", 1, "pod_uid_memory"),
		ToolUseEventId:      "evt_tool_memory_replace",
		NormalizedInputHash: "hash_replace",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/todo.md","old_text":"one","new_text":"two"}`,
	}
	replaced, err := store.RunMemory(context.Background(), replace)
	if err != nil {
		t.Fatalf("RunMemory replace: %v", err)
	}
	assertMemoryResultStatus(t, replaced.GetResultJson(), "completed")
	if err := admin.QueryRowContext(context.Background(),
		`SELECT v.content
		   FROM memories m
		   JOIN memory_versions v
		     ON v.workspace_id = m.workspace_id
		    AND v.memory_store_id = m.memory_store_id
		    AND v.memory_id = m.memory_id
		    AND v.memory_version_id = m.current_version_id
		  WHERE m.workspace_id = 'default'
		    AND m.memory_store_id = 'memstore_bridge_memory'
		    AND m.path = '/notes/todo.md'
		    AND m.deleted_at IS NULL`).Scan(&currentContent); err != nil {
		t.Fatalf("read replaced memory: %v", err)
	}
	if currentContent != "two" {
		t.Fatalf("replaced memory content = %q; want two", currentContent)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryEnforcesDurableMemoryQuotas(t *testing.T) {
	t.Run("memory identities", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_identity_quota", "thr_bridge_memory_identity_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_identity_quota", "bind_bridge_memory_identity_quota", 1, "pod_uid_memory_identity_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_identity_quota", "memstore_bridge_memory_identity_quota")
		seedBridgeAPIMemoryIdentities(t, admin, "memstore_bridge_memory_identity_quota", memory.MaxMemoriesPerStore)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
		_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
			Scope:               bridgeAPIScope("sesn_bridge_memory_identity_quota", "thr_bridge_memory_identity_quota", "bind_bridge_memory_identity_quota", 1, "pod_uid_memory_identity_quota"),
			ToolUseEventId:      "evt_tool_memory_identity_quota",
			NormalizedInputHash: "hash_memory_identity_quota",
			Operation:           "create",
			InputJson:           `{"action":"create","path":"over-limit.md","content":"x"}`,
		})
		var quota *memory.QuotaError
		if !errors.As(err, &quota) {
			t.Fatalf("RunMemory create err = %T %v; want memory quota", err, err)
		}
		if count := countBridgeAPIMemories(t, admin, "memstore_bridge_memory_identity_quota"); count != memory.MaxMemoriesPerStore {
			t.Fatalf("memory count after quota rejection = %d; want %d", count, memory.MaxMemoriesPerStore)
		}
		assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_identity_quota", "evt_tool_memory_identity_quota")
	})

	t.Run("versions", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_version_quota", "thr_bridge_memory_version_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_version_quota", "bind_bridge_memory_version_quota", 1, "pod_uid_memory_version_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_version_quota", "memstore_bridge_memory_version_quota")
		seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_version_quota", "mem_bridge_memory_version_quota", "/quota.md", "x")
		seedBridgeAPIAdditionalMemoryVersions(t, admin, "memstore_bridge_memory_version_quota", "mem_bridge_memory_version_quota", memory.MaxMemoryVersionsPerStore-1)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
		requests := []struct {
			name      string
			operation string
			inputJSON string
		}{
			{name: "replace", operation: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "delete", operation: "delete", inputJSON: `{"action":"delete","path":"quota.md","expected_text":"x"}`},
			{name: "rename", operation: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
					Scope:               bridgeAPIScope("sesn_bridge_memory_version_quota", "thr_bridge_memory_version_quota", "bind_bridge_memory_version_quota", 1, "pod_uid_memory_version_quota"),
					ToolUseEventId:      "evt_tool_memory_version_quota_" + test.name,
					NormalizedInputHash: "hash_memory_version_quota_" + test.name,
					Operation:           test.operation,
					InputJson:           test.inputJSON,
				})
				var quota *memory.QuotaError
				if !errors.As(err, &quota) {
					t.Fatalf("RunMemory %s err = %T %v; want memory version quota", test.name, err, err)
				}
				assertBridgeAPIMemoryHead(t, admin, "memstore_bridge_memory_version_quota", "/quota.md", "x")
				if count := countMemoryVersions(t, admin, "memstore_bridge_memory_version_quota"); count != memory.MaxMemoryVersionsPerStore {
					t.Fatalf("version count after %s quota rejection = %d; want %d", test.name, count, memory.MaxMemoryVersionsPerStore)
				}
				assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_version_quota", "evt_tool_memory_version_quota_"+test.name)
			})
		}
	})

	t.Run("retained payload bytes", func(t *testing.T) {
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_retained_quota", "thr_bridge_memory_retained_quota")
		seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_retained_quota", "bind_bridge_memory_retained_quota", 1, "pod_uid_memory_retained_quota")
		seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_retained_quota", "memstore_bridge_memory_retained_quota")
		seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_retained_quota", "mem_bridge_memory_retained_quota", "/quota.md", "x")
		seedBridgeAPIRetainedMemoryPayload(t, admin, "memstore_bridge_memory_retained_quota", "mem_bridge_memory_retained_quota", memory.MaxRetainedMemoryPayloadBytesPerStore-1)

		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
		requests := []struct {
			name      string
			operation string
			inputJSON string
		}{
			{name: "create", operation: "create", inputJSON: `{"action":"create","path":"over-limit.md","content":"x"}`},
			{name: "replace", operation: "replace", inputJSON: `{"action":"replace","path":"quota.md","old_text":"x","new_text":"y"}`},
			{name: "rename", operation: "rename", inputJSON: `{"action":"rename","path":"quota.md","new_path":"renamed.md","expected_text":"x"}`},
		}
		for _, test := range requests {
			t.Run(test.name, func(t *testing.T) {
				_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
					Scope:               bridgeAPIScope("sesn_bridge_memory_retained_quota", "thr_bridge_memory_retained_quota", "bind_bridge_memory_retained_quota", 1, "pod_uid_memory_retained_quota"),
					ToolUseEventId:      "evt_tool_memory_retained_quota_" + test.name,
					NormalizedInputHash: "hash_memory_retained_quota_" + test.name,
					Operation:           test.operation,
					InputJson:           test.inputJSON,
				})
				var quota *memory.RequestTooLargeError
				if !errors.As(err, &quota) {
					t.Fatalf("RunMemory %s err = %T %v; want retained payload quota", test.name, err, err)
				}
				assertBridgeAPIMemoryHead(t, admin, "memstore_bridge_memory_retained_quota", "/quota.md", "x")
				if count := countMemoryVersions(t, admin, "memstore_bridge_memory_retained_quota"); count != 2 {
					t.Fatalf("version count after %s retained quota rejection = %d; want 2", test.name, count)
				}
				assertNoBridgeAPIRuntimeToolResult(t, admin, "sesn_bridge_memory_retained_quota", "evt_tool_memory_retained_quota_"+test.name)
			})
		}
	})
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRequiresWritableSessionBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_missing", "thr_bridge_memory_missing")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_missing", "bind_bridge_memory_missing", 1, "pod_uid_memory_missing")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = refresher
	missing := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_missing", "thr_bridge_memory_missing", "bind_bridge_memory_missing", 1, "pod_uid_memory_missing"),
		ToolUseEventId:      "evt_tool_memory_missing",
		NormalizedInputHash: "hash_memory_missing",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/missing.md","content":"one"}`,
	}
	missingResponse, err := store.RunMemory(context.Background(), missing)
	if err != nil {
		t.Fatalf("RunMemory missing binding: %v", err)
	}
	assertMemoryToolErrorCode(t, missingResponse.GetResultJson(), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_missing", "evt_tool_memory_missing")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_detached", "thr_bridge_memory_detached")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached")
	seedBridgeAPIDetachedMemoryStoreBinding(t, admin, "default", "sesn_bridge_memory_detached", "memstore_bridge_memory_detached", "read_write", "/mnt/memory/detached")
	detached := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_detached", "thr_bridge_memory_detached", "bind_bridge_memory_detached", 1, "pod_uid_memory_detached"),
		ToolUseEventId:      "evt_tool_memory_detached",
		NormalizedInputHash: "hash_memory_detached",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/detached.md","content":"one"}`,
	}
	detachedResponse, err := store.RunMemory(context.Background(), detached)
	if err != nil {
		t.Fatalf("RunMemory detached binding: %v", err)
	}
	assertMemoryToolErrorCode(t, detachedResponse.GetResultJson(), "memory_store_not_configured")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_detached", "evt_tool_memory_detached")

	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_a")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_ambiguous", "memstore_bridge_memory_b")
	ambiguous := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_ambiguous", "thr_bridge_memory_ambiguous", "bind_bridge_memory_ambiguous", 1, "pod_uid_memory_ambiguous"),
		ToolUseEventId:      "evt_tool_memory_ambiguous",
		NormalizedInputHash: "hash_memory_ambiguous",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/ambiguous.md","content":"one"}`,
	}
	ambiguousResponse, err := store.RunMemory(context.Background(), ambiguous)
	if err != nil {
		t.Fatalf("RunMemory ambiguous binding: %v", err)
	}
	assertMemoryToolErrorCode(t, ambiguousResponse.GetResultJson(), "memory_store_ambiguous")
	assertMemoryProjectionStateNull(t, admin, "sesn_bridge_memory_ambiguous", "evt_tool_memory_ambiguous")
	if len(refresher.calls) != 0 {
		t.Fatalf("projection calls = %d; want none for memory binding errors", len(refresher.calls))
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRefreshesProjectionAfterCommit(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		inputJSON string
		seed      func(t *testing.T, db *sql.DB, storeID string)
		wantOps   []MemoryProjectionOp
	}{
		{
			name:      "create",
			operation: "create",
			inputJSON: `{"action":"create","path":"notes/create.md","content":"created"}`,
			wantOps: []MemoryProjectionOp{{
				Kind:          "upsert",
				RelativePath:  "/notes/create.md",
				Content:       "created",
				ContentSHA256: sha256Hex("created"),
			}},
		},
		{
			name:      "replace",
			operation: "replace",
			inputJSON: `{"action":"replace","path":"notes/replace.md","old_text":"one","new_text":"two"}`,
			seed: func(t *testing.T, db *sql.DB, storeID string) {
				seedBridgeAPIMemory(t, db, "default", storeID, "mem_bridge_memory_refresh_replace", "/notes/replace.md", "one")
			},
			wantOps: []MemoryProjectionOp{{
				Kind:          "upsert",
				RelativePath:  "/notes/replace.md",
				Content:       "two",
				ContentSHA256: sha256Hex("two"),
			}},
		},
		{
			name:      "delete",
			operation: "delete",
			inputJSON: `{"action":"delete","path":"notes/delete.md","expected_text":"delete me"}`,
			seed: func(t *testing.T, db *sql.DB, storeID string) {
				seedBridgeAPIMemory(t, db, "default", storeID, "mem_bridge_memory_refresh_delete", "/notes/delete.md", "delete me")
			},
			wantOps: []MemoryProjectionOp{{
				Kind:         "remove",
				RelativePath: "/notes/delete.md",
			}},
		},
		{
			name:      "rename",
			operation: "rename",
			inputJSON: `{"action":"rename","path":"notes/old.md","new_path":"notes/new.md","expected_text":"rename me"}`,
			seed: func(t *testing.T, db *sql.DB, storeID string) {
				seedBridgeAPIMemory(t, db, "default", storeID, "mem_bridge_memory_refresh_rename", "/notes/old.md", "rename me")
			},
			wantOps: []MemoryProjectionOp{
				{Kind: "remove", RelativePath: "/notes/old.md"},
				{Kind: "upsert", RelativePath: "/notes/new.md", Content: "rename me", ContentSHA256: sha256Hex("rename me")},
			},
		},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_refresh_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_refresh_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_refresh_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_refresh_" + strconv.Itoa(index)
			toolUseID := "evt_tool_memory_refresh_" + tc.name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_refresh")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIMemoryStoreBinding(t, admin, "default", sessionID, storeID, "read_only", "/mnt/memory/readonly")
			seedBridgeAPIDetachedMemoryStoreBinding(t, admin, "default", sessionID, storeID, "read_only", "/mnt/memory/detached")
			if tc.seed != nil {
				tc.seed(t, admin, storeID)
			}
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_refresh_"+strconv.Itoa(index))
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")

			refresher := &recordingMemoryProjectionRefresher{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
			store.MemoryProjectionRefresher = refresher
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_refresh"),
				ToolUseEventId:      toolUseID,
				NormalizedInputHash: "hash_memory_refresh_" + tc.name,
				Operation:           tc.operation,
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory refresh: %v", err)
			}
			assertMemoryResultStatus(t, response.GetResultJson(), "completed")
			if len(refresher.calls) != 1 {
				t.Fatalf("projection refresh calls = %d; want 1", len(refresher.calls))
			}
			call := refresher.calls[0]
			wantMounts := []string{"/mnt/memory/bridge_memory_refresh_" + strconv.Itoa(index), "/mnt/memory/readonly"}
			if call.Target.ProviderSandboxID != "provider_"+sessionID ||
				!equalStringSlices(call.MountPaths, wantMounts) {
				t.Fatalf("refresh target/mounts = %+v %v; want active sandbox and every attached store mount %v", call.Target, call.MountPaths, wantMounts)
			}
			assertMemoryProjectionOps(t, call.Ops, tc.wantOps)
			assertMemoryProjectionState(t, admin, sessionID, toolUseID, memoryProjectionStateRefreshed)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryBoundsProjectionRefreshWithPushTimeout(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_bridge_memory_push_timeout"
	threadID := "thr_bridge_memory_push_timeout"
	bindingID := "bind_bridge_memory_push_timeout"
	storeID := "memstore_bridge_memory_push_timeout"
	toolUseID := "evt_tool_memory_push_timeout"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_push_timeout")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
	seedBridgeAPIMemoryStoreBinding(t, admin, "default", sessionID, storeID, "read_only", "/mnt/memory/push-timeout")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_push_timeout")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionPushTimeout = 10 * time.Second
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_push_timeout"),
		ToolUseEventId:      toolUseID,
		NormalizedInputHash: "hash_memory_push_timeout",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/timeout.md","content":"bounded"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory push timeout: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	if len(refresher.deadlines) != 1 {
		t.Fatalf("projection refresh deadlines = %d; want 1", len(refresher.deadlines))
	}
	deadline := refresher.deadlines[0]
	if deadline.IsZero() {
		t.Fatal("projection refresh context has no deadline")
	}
	if len(refresher.observedAt) != 1 {
		t.Fatalf("projection refresh observedAt = %d; want 1", len(refresher.observedAt))
	}
	remaining := deadline.Sub(refresher.observedAt[0])
	if remaining < 9*time.Second || remaining > 10*time.Second {
		t.Fatalf("projection refresh deadline is %s after callback start; want bounded by 10s push timeout", remaining)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryProjectionPushLockContentionReturnsRetryable(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_bridge_memory_push_lock_contention"
	threadID := "thr_bridge_memory_push_lock_contention"
	bindingID := "bind_bridge_memory_push_lock_contention"
	storeID := "memstore_bridge_memory_push_lock_contention"
	toolUseID := "evt_tool_memory_push_lock_contention"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_push_lock_contention")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
	seedBridgeAPIMemoryStoreBinding(t, admin, "default", sessionID, storeID, "read_only", "/mnt/memory/contention")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_push_lock_contention")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
	seedPendingMemoryProjectionResult(t, admin, "default", sessionID, threadID, toolUseID,
		"hash_memory_push_lock_contention",
		`{"action":"delete","path":"notes/contention.md","expected_text":"old"}`,
		`{"status":"completed","action":"delete","path":"notes/contention.md","projection_refreshed":false}`)

	lockTx, err := admin.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	defer func() { _ = lockTx.Rollback() }()
	if err := storage.AcquireMemoryStoreMutationLock(context.Background(), lockTx, "default", storeID); err != nil {
		t.Fatalf("acquire blocking memory lock: %v", err)
	}

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_push_lock_contention"),
		ToolUseEventId:      toolUseID,
		NormalizedInputHash: "hash_memory_push_lock_contention",
		Operation:           "delete",
		InputJson:           `{"action":"delete","path":"notes/contention.md","expected_text":"old"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory contended refresh: %v", err)
	}
	assertRuntimeToolErrorCode(t, response.GetResultJson(), "projection_refresh_failed")
	if len(refresher.calls) != 0 {
		t.Fatalf("projection refresh calls = %d; want none while push lock is contended", len(refresher.calls))
	}
	assertMemoryProjectionState(t, admin, sessionID, toolUseID, memoryProjectionStatePending)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryProjectionPushLockCoversRefreshAndReleasesOnFailure(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	sessionID := "sesn_bridge_memory_push_lock_release"
	threadID := "thr_bridge_memory_push_lock_release"
	bindingID := "bind_bridge_memory_push_lock_release"
	storeID := "memstore_bridge_memory_push_lock_release"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_push_lock_release")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
	seedBridgeAPIMemoryStoreBinding(t, admin, "default", sessionID, storeID, "read_only", "/mnt/memory/release")
	seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_push_lock_release")
	seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")

	refresher := newBlockingMemoryProjectionRefresher(errors.New("refresh crashed"))
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionRefresher = refresher
	runDone := make(chan struct {
		response *bridgev1.RunMemoryResponse
		err      error
	}, 1)
	go func() {
		response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
			Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_push_lock_release"),
			ToolUseEventId:      "evt_tool_memory_push_lock_release",
			NormalizedInputHash: "hash_memory_push_lock_release",
			Operation:           "create",
			InputJson:           `{"action":"create","path":"notes/release.md","content":"one"}`,
		})
		runDone <- struct {
			response *bridgev1.RunMemoryResponse
			err      error
		}{response: response, err: err}
	}()

	select {
	case <-refresher.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for projection refresh to start")
	}
	if err := tryMemoryStoreMutationLockWithTimeout(admin, storeID, 50*time.Millisecond); err == nil {
		close(refresher.release)
		t.Fatal("prepare-time memory lock acquired while projection push lock was held")
	}
	close(refresher.release)
	var completed struct {
		response *bridgev1.RunMemoryResponse
		err      error
	}
	select {
	case completed = <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RunMemory to finish")
	}
	if completed.err != nil {
		t.Fatalf("RunMemory refresh failure: %v", completed.err)
	}
	assertRuntimeToolErrorCode(t, completed.response.GetResultJson(), "projection_refresh_failed")
	if err := tryMemoryStoreMutationLockWithTimeout(admin, storeID, time.Second); err != nil {
		t.Fatalf("memory store lock after failed refresh = %v; want released", err)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReturnsRetryableWhenRefreshFails(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_refresh_fail", "thr_bridge_memory_refresh_fail")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_refresh_fail", "bind_bridge_memory_refresh_fail", 1, "pod_uid_memory_refresh_fail")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_refresh_fail", "memstore_bridge_memory_refresh_fail")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_refresh_fail", "prep_bridge_memory_refresh_fail")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_refresh_fail", "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{err: errors.New("projection unavailable")}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionRefresher = refresher
	request := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_refresh_fail", "thr_bridge_memory_refresh_fail", "bind_bridge_memory_refresh_fail", 1, "pod_uid_memory_refresh_fail"),
		ToolUseEventId:      "evt_tool_memory_refresh_fail",
		NormalizedInputHash: "hash_memory_refresh_fail",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/todo.md","content":"one"}`,
	}
	failed, err := store.RunMemory(context.Background(), request)
	if err != nil {
		t.Fatalf("RunMemory refresh failure: %v", err)
	}
	assertRuntimeToolErrorCode(t, failed.GetResultJson(), "projection_refresh_failed")
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_refresh_fail", "evt_tool_memory_refresh_fail", memoryProjectionStatePending)
	assertStoredMemoryResultStatus(t, admin, "sesn_bridge_memory_refresh_fail", "evt_tool_memory_refresh_fail", "completed")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_refresh_fail"); count != 1 {
		t.Fatalf("memory versions after failed refresh = %d; want committed mutation only once", count)
	}
	storedSemanticResult := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_refresh_fail", "evt_tool_memory_refresh_fail")
	if strings.Contains(storedSemanticResult, "projection_refresh_failed") {
		t.Fatalf("stored result contains synthesized projection failure: %s", storedSemanticResult)
	}

	refresher.err = nil
	reopenedStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	reopenedStore.Clock = store.Clock
	reopenedStore.MemoryProjectionRefresher = refresher
	replayed, err := reopenedStore.RunMemory(context.Background(), request)
	if err != nil {
		t.Fatalf("RunMemory refresh replay after store reopen: %v", err)
	}
	assertMemoryResultStatus(t, replayed.GetResultJson(), "completed")
	if replayed.GetResultJson() != storedSemanticResult {
		t.Fatalf("replayed result = %s; want byte-identical stored result %s", replayed.GetResultJson(), storedSemanticResult)
	}
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_refresh_fail", "evt_tool_memory_refresh_fail", memoryProjectionStateRefreshed)
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_refresh_fail"); count != 1 {
		t.Fatalf("memory versions after healed replay = %d; want no duplicate mutation", count)
	}
	if len(refresher.calls) != 2 {
		t.Fatalf("projection calls after replay = %d; want failed attempt plus healed attempt", len(refresher.calls))
	}

	conflict := proto.Clone(request).(*bridgev1.RunMemoryRequest)
	conflict.NormalizedInputHash = "different_hash"
	if _, err := reopenedStore.RunMemory(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting RunMemory replay err = %v; want AlreadyExists", err)
	}
	if storedAfterConflict := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_refresh_fail", "evt_tool_memory_refresh_fail"); storedAfterConflict != storedSemanticResult {
		t.Fatalf("stored result after mismatch = %s; want preserved %s", storedAfterConflict, storedSemanticResult)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_refresh_fail"); count != 1 {
		t.Fatalf("memory versions after mismatched replay = %d; want first mutation preserved", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReplayRefreshUsesStoredInputOperation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_replay_input", "thr_bridge_memory_replay_input")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_replay_input", "bind_bridge_memory_replay_input", 1, "pod_uid_memory_replay_input")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_replay_input", "memstore_bridge_memory_replay_input")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_replay_input", "mem_bridge_memory_replay_old", "/notes/old.md", "one")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_replay_input", "prep_bridge_memory_replay_input")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_replay_input", "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{err: errors.New("projection unavailable")}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionRefresher = refresher
	request := &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replay_input", "thr_bridge_memory_replay_input", "bind_bridge_memory_replay_input", 1, "pod_uid_memory_replay_input"),
		ToolUseEventId:      "evt_tool_memory_replay_input",
		NormalizedInputHash: "hash_memory_replay_input",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/new.md","expected_text":"wrong"}`,
	}
	first, err := store.RunMemory(context.Background(), request)
	if err != nil {
		t.Fatalf("RunMemory first stale rename refresh failure: %v", err)
	}
	assertRuntimeToolErrorCode(t, first.GetResultJson(), "projection_refresh_failed")
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_replay_input", "evt_tool_memory_replay_input", memoryProjectionStatePending)
	storedSemanticResult := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_replay_input", "evt_tool_memory_replay_input")
	if strings.Contains(storedSemanticResult, "projection_refresh_failed") {
		t.Fatalf("stored result contains synthesized projection failure: %s", storedSemanticResult)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_replay_input"); count != 1 {
		t.Fatalf("memory versions after first stale result = %d; want original durable version only", count)
	}

	refresher.err = nil
	replay := proto.Clone(request).(*bridgev1.RunMemoryRequest)
	replay.Operation = "create"
	replay.InputJson = `{"action":"create","path":"notes/replay-only.md","content":"must-not-drive-replay-planning"}`
	reopenedStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	reopenedStore.Clock = store.Clock
	reopenedStore.MemoryProjectionRefresher = refresher
	replayed, err := reopenedStore.RunMemory(context.Background(), replay)
	if err != nil {
		t.Fatalf("RunMemory pending replay with conflicting request input: %v", err)
	}
	assertMemoryToolError(t, replayed.GetResultJson(), "expected_text_mismatch", true)
	assertMemoryProjectionRefreshed(t, replayed.GetResultJson(), true)
	if len(refresher.calls) != 2 {
		t.Fatalf("projection calls = %d; want initial failed attempt plus replay", len(refresher.calls))
	}
	ops := refresher.calls[1].Ops
	if len(ops) != 2 ||
		ops[0].Kind != "upsert" || ops[0].RelativePath != "/notes/old.md" || ops[0].Content != "one" ||
		ops[1].Kind != "remove" || ops[1].RelativePath != "/notes/new.md" {
		t.Fatalf("replay projection ops = %+v; want durable rename input old+new paths, never replay-only create input", ops)
	}
	storedAfterReplay := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_replay_input", "evt_tool_memory_replay_input")
	if replayed.GetResultJson() != storedAfterReplay {
		t.Fatalf("replayed result = %s; want byte-identical stored result %s", replayed.GetResultJson(), storedAfterReplay)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_replay_input"); count != 1 {
		t.Fatalf("memory versions after same-ID replay = %d; want no phase-1 replay", count)
	}

	conflict := proto.Clone(replay).(*bridgev1.RunMemoryRequest)
	conflict.NormalizedInputHash = "different_hash"
	if _, err := reopenedStore.RunMemory(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting RunMemory replay err = %v; want AlreadyExists", err)
	}
	if storedAfterConflict := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_replay_input", "evt_tool_memory_replay_input"); storedAfterConflict != storedAfterReplay {
		t.Fatalf("stored result after mismatch = %s; want preserved %s", storedAfterConflict, storedAfterReplay)
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_replay_input"); count != 1 {
		t.Fatalf("memory versions after mismatched replay = %d; want first durable version preserved", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsOperationActionMismatchBeforeDurableWrite(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_action_mismatch", "thr_bridge_memory_action_mismatch")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_action_mismatch", "bind_bridge_memory_action_mismatch", 1, "pod_uid_memory_action_mismatch")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_action_mismatch", "memstore_bridge_memory_action_mismatch")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = refresher
	_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_action_mismatch", "thr_bridge_memory_action_mismatch", "bind_bridge_memory_action_mismatch", 1, "pod_uid_memory_action_mismatch"),
		ToolUseEventId:      "evt_tool_memory_action_mismatch",
		NormalizedInputHash: "hash_memory_action_mismatch",
		Operation:           "delete",
		InputJson:           `{"action":"create","path":"notes/mismatch.md","content":"one"}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("RunMemory mismatch error = %v; want InvalidArgument", err)
	}
	assertNoRuntimeToolResult(t, admin, "sesn_bridge_memory_action_mismatch", "evt_tool_memory_action_mismatch")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_action_mismatch"); count != 0 {
		t.Fatalf("memory versions after action mismatch = %d; want 0", count)
	}
	if len(refresher.calls) != 0 {
		t.Fatalf("projection calls after action mismatch = %d; want none", len(refresher.calls))
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshWhenSandboxCold(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_cold", "thr_bridge_memory_cold")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_cold", "bind_bridge_memory_cold", 1, "pod_uid_memory_cold")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_cold", "memstore_bridge_memory_cold")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_cold", "thr_bridge_memory_cold", "bind_bridge_memory_cold", 1, "pod_uid_memory_cold"),
		ToolUseEventId:      "evt_tool_memory_cold",
		NormalizedInputHash: "hash_memory_cold",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/cold.md","content":"one"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory cold refresh: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_cold", "evt_tool_memory_cold", memoryProjectionStateSkippedCold)
	if len(refresher.calls) != 0 {
		t.Fatalf("projection refresher calls = %d; want none for cold sandbox", len(refresher.calls))
	}

	reopenedStore := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	reopenedStore.MemoryProjectionRefresher = refresher
	replayed, err := reopenedStore.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_cold", "thr_bridge_memory_cold", "bind_bridge_memory_cold", 1, "pod_uid_memory_cold"),
		ToolUseEventId:      "evt_tool_memory_cold",
		NormalizedInputHash: "hash_memory_cold",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/cold.md","content":"one"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory cold same-ID replay after reopen: %v", err)
	}
	if replayed.GetResultJson() != response.GetResultJson() {
		t.Fatalf("cold replay result = %s; want byte-identical %s", replayed.GetResultJson(), response.GetResultJson())
	}
	if len(refresher.calls) != 0 {
		t.Fatalf("projection refresher calls after cold replay = %d; want zero", len(refresher.calls))
	}
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_cold"); count != 1 {
		t.Fatalf("memory versions after cold replay = %d; want one mutation", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsStaleErrorRefreshWhenSandboxCold(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_cold_stale", "thr_bridge_memory_cold_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_cold_stale", "bind_bridge_memory_cold_stale", 1, "pod_uid_memory_cold_stale")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_cold_stale", "memstore_bridge_memory_cold_stale")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_cold_stale", "mem_bridge_memory_cold_stale", "/notes/todo.md", "one")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_cold_stale", "thr_bridge_memory_cold_stale", "bind_bridge_memory_cold_stale", 1, "pod_uid_memory_cold_stale"),
		ToolUseEventId:      "evt_tool_memory_cold_stale",
		NormalizedInputHash: "hash_memory_cold_stale",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/todo.md","old_text":"absent","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory cold stale refresh: %v", err)
	}
	assertMemoryToolError(t, response.GetResultJson(), "old_text_not_found", true)
	assertMemoryProjectionRefreshed(t, response.GetResultJson(), false)
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_cold_stale", "evt_tool_memory_cold_stale", memoryProjectionStateSkippedCold)
	if len(refresher.calls) != 0 {
		t.Fatalf("projection refresher calls = %d; want none for cold stale error", len(refresher.calls))
	}
	stored := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_cold_stale", "evt_tool_memory_cold_stale")
	assertMemoryProjectionRefreshed(t, stored, false)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRequeuesPreparationForInactiveSandbox(t *testing.T) {
	for _, sandboxStatus := range []string{"stopped", "archived", "released"} {
		t.Run(sandboxStatus, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_inactive_" + sandboxStatus
			threadID := "thr_bridge_memory_inactive_" + sandboxStatus
			preparationAttemptID := "prep_bridge_memory_inactive_" + sandboxStatus
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_memory_inactive_"+sandboxStatus, 1, "pod_uid_memory_inactive_"+sandboxStatus)
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, "memstore_bridge_memory_inactive_"+sandboxStatus)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, preparationAttemptID)
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
			setBridgeAPISandboxStatus(t, admin, "default", sessionID, sandboxStatus)

			refresher := &recordingMemoryProjectionRefresher{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
			store.MemoryProjectionRefresher = refresher
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_memory_inactive_"+sandboxStatus, 1, "pod_uid_memory_inactive_"+sandboxStatus),
				ToolUseEventId:      "evt_tool_memory_inactive_" + sandboxStatus,
				NormalizedInputHash: "hash_memory_inactive_" + sandboxStatus,
				Operation:           "create",
				InputJson:           `{"action":"create","path":"notes/inactive.md","content":"one"}`,
			})
			if err != nil {
				t.Fatalf("RunMemory inactive sandbox: %v", err)
			}
			assertMemoryResultStatus(t, response.GetResultJson(), "completed")
			assertMemoryProjectionState(t, admin, sessionID, "evt_tool_memory_inactive_"+sandboxStatus, memoryProjectionStateSkippedCold)
			if len(refresher.calls) != 0 {
				t.Fatalf("projection calls = %d; want none while sandbox status is %s", len(refresher.calls), sandboxStatus)
			}
			assertSessionPrepareRequeued(t, admin, "default", sessionID, preparationAttemptID)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshWhenSandboxReadinessStale(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "thr_bridge_memory_stale_sandbox")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "bind_bridge_memory_stale_sandbox", 1, "pod_uid_memory_stale_sandbox")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "memstore_bridge_memory_stale_sandbox")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "prep_bridge_memory_stale_sandbox")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_stale_sandbox", "thr_bridge_memory_stale_sandbox", "bind_bridge_memory_stale_sandbox", 1, "pod_uid_memory_stale_sandbox"),
		ToolUseEventId:      "evt_tool_memory_stale_sandbox",
		NormalizedInputHash: "hash_memory_stale_sandbox",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/stale-sandbox.md","content":"one"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory stale sandbox refresh: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_stale_sandbox", "evt_tool_memory_stale_sandbox", memoryProjectionStateSkippedCold)
	if len(refresher.calls) != 0 {
		t.Fatalf("projection calls = %d; want none while sandbox status is stale", len(refresher.calls))
	}
	assertSessionPrepareRequeued(t, admin, "default", "sesn_bridge_memory_stale_sandbox", "prep_bridge_memory_stale_sandbox")
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshWithExpiringResourceCredential(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_cred_expiring", "thr_bridge_memory_cred_expiring")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_cred_expiring", "bind_bridge_memory_cred_expiring", 1, "pod_uid_memory_cred_expiring")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_cred_expiring", "memstore_bridge_memory_cred_expiring")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_cred_expiring", "prep_bridge_memory_cred_expiring")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_cred_expiring", "2026-01-01T00:40:00Z")
	seedBridgeAPIResourceCredentialExpiresAt(t, admin, "default", "sesn_bridge_memory_cred_expiring", "prep_bridge_memory_cred_expiring", "2026-01-01T01:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 40, 1, 0, time.UTC) }
	store.SandboxStatusFreshnessWindow = time.Minute
	store.ResourceCredentialRefreshMargin = 30 * time.Minute
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_cred_expiring", "thr_bridge_memory_cred_expiring", "bind_bridge_memory_cred_expiring", 1, "pod_uid_memory_cred_expiring"),
		ToolUseEventId:      "evt_tool_memory_cred_expiring",
		NormalizedInputHash: "hash_memory_cred_expiring",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/cred-expiring.md","content":"one"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory expiring resource credential: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_cred_expiring", "evt_tool_memory_cred_expiring", memoryProjectionStateSkippedCold)
	if len(refresher.calls) != 0 {
		t.Fatalf("projection calls = %d; want none while file-resource credential is expiring", len(refresher.calls))
	}
	assertSessionPrepareRequeuedForCredentialRotation(t, admin, "default", "sesn_bridge_memory_cred_expiring", "prep_bridge_memory_cred_expiring")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryFailsClosedWithoutProjectionRefresher(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_no_refresher", "thr_bridge_memory_no_refresher")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_no_refresher", "bind_bridge_memory_no_refresher", 1, "pod_uid_memory_no_refresher")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_no_refresher", "memstore_bridge_memory_no_refresher")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_no_refresher", "thr_bridge_memory_no_refresher", "bind_bridge_memory_no_refresher", 1, "pod_uid_memory_no_refresher"),
		ToolUseEventId:      "evt_tool_memory_no_refresher",
		NormalizedInputHash: "hash_memory_no_refresher",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"notes/no-refresher.md","content":"one"}`,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RunMemory without projection refresher err = %v; want FailedPrecondition", err)
	}
	assertMemoryProjectionState(t, admin, "sesn_bridge_memory_no_refresher", "evt_tool_memory_no_refresher", memoryProjectionStatePending)
	assertStoredMemoryResultStatus(t, admin, "sesn_bridge_memory_no_refresher", "evt_tool_memory_no_refresher", "completed")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_no_refresher"); count != 1 {
		t.Fatalf("memory versions after fail-closed projection = %d; want committed mutation once", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRefreshesStaleErrorPaths(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_stale", "thr_bridge_memory_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_stale", "bind_bridge_memory_stale", 1, "pod_uid_memory_stale")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_stale", "memstore_bridge_memory_stale")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_stale", "mem_bridge_memory_stale", "/notes/todo.md", "one")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_stale", "prep_bridge_memory_stale")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_stale", "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_stale", "thr_bridge_memory_stale", "bind_bridge_memory_stale", 1, "pod_uid_memory_stale"),
		ToolUseEventId:      "evt_tool_memory_stale",
		NormalizedInputHash: "hash_memory_stale",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/todo.md","old_text":"absent","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory stale refresh: %v", err)
	}
	assertMemoryToolError(t, response.GetResultJson(), "old_text_not_found", true)
	assertMemoryProjectionRefreshed(t, response.GetResultJson(), true)
	if len(refresher.calls) != 1 || len(refresher.calls[0].Ops) != 1 ||
		refresher.calls[0].Ops[0].Kind != "upsert" ||
		refresher.calls[0].Ops[0].RelativePath != "/notes/todo.md" ||
		refresher.calls[0].Ops[0].Content != "one" {
		t.Fatalf("stale refresh calls = %+v; want current durable truth for referenced path", refresher.calls)
	}
	stored := storedMemoryResultJSON(t, admin, "sesn_bridge_memory_stale", "evt_tool_memory_stale")
	assertMemoryProjectionRefreshed(t, stored, true)
}

func TestPostgreSQLBridgeAPIStoreRunMemorySkipsRefreshForValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		inputJSON string
		wantCode  string
		seed      func(t *testing.T, db *sql.DB, storeID string)
	}{
		{name: "invalid input non object", operation: "create", inputJSON: `[]`, wantCode: "invalid_input"},
		{name: "invalid input null", operation: "create", inputJSON: `null`, wantCode: "invalid_input"},
		{name: "invalid action", operation: "unknown", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", operation: "create", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", operation: "create", inputJSON: `{"action":"create","path":"../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", operation: "create", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", operation: "create", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", operation: "create", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", operation: "replace", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", operation: "delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", operation: "rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_validation_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_validation_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_validation_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_validation_" + strconv.Itoa(index)
			toolUseID := "evt_tool_memory_validation_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_validation")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_validation_"+strconv.Itoa(index))
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")
			if tc.seed != nil {
				tc.seed(t, admin, storeID)
			}

			refresher := &recordingMemoryProjectionRefresher{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.MemoryProjectionRefresher = refresher
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_validation"),
				ToolUseEventId:      toolUseID,
				NormalizedInputHash: "hash_memory_validation_" + strconv.Itoa(index),
				Operation:           tc.operation,
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory validation error: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), tc.wantCode, false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
			if len(refresher.calls) != 0 {
				t.Fatalf("projection calls = %d; want none for validation error", len(refresher.calls))
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsInvalidInputs(t *testing.T) {
	tests := []struct {
		name       string
		operation  string
		inputJSON  string
		wantCode   string
		wantReread bool
	}{
		{name: "invalid input non object", operation: "create", inputJSON: `[]`, wantCode: "invalid_input"},
		{name: "invalid action", operation: "unknown", inputJSON: `{"action":"unknown","path":"notes/todo.md"}`, wantCode: "invalid_action"},
		{name: "invalid path absolute", operation: "create", inputJSON: `{"action":"create","path":"/absolute","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path dotdot", operation: "create", inputJSON: `{"action":"create","path":"notes/../bad","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection path", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path runtime projection prefix", operation: "create", inputJSON: `{"action":"create","path":"mnt/memory-old/notes.md","content":"one"}`, wantCode: "invalid_path"},
		{name: "invalid path too long", operation: "create", inputJSON: memoryCreateInputJSON(t, strings.Repeat("a", 1024), "one"), wantCode: "invalid_path"},
		{name: "missing content", operation: "create", inputJSON: `{"action":"create","path":"notes/todo.md"}`, wantCode: "missing_content"},
		{name: "content too large", operation: "create", inputJSON: memoryCreateInputJSON(t, "notes/large.md", strings.Repeat("x", 102401)), wantCode: "content_too_large"},
		{name: "missing replace text", operation: "replace", inputJSON: `{"action":"replace","path":"notes/todo.md","old_text":"one"}`, wantCode: "missing_replace_text"},
		{name: "missing expected text delete", operation: "delete", inputJSON: `{"action":"delete","path":"notes/todo.md"}`, wantCode: "missing_expected_text"},
		{name: "missing expected text rename", operation: "rename", inputJSON: `{"action":"rename","path":"notes/todo.md","new_path":"notes/new.md"}`, wantCode: "missing_expected_text"},
		{name: "not found", operation: "delete", inputJSON: `{"action":"delete","path":"notes/missing.md","expected_text":"gone"}`, wantCode: "not_found", wantReread: true},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_invalid_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_invalid_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_invalid_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_invalid_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_invalid")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_invalid_"+strconv.Itoa(index), "/notes/todo.md", "one")

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_invalid"),
				ToolUseEventId:      "evt_tool_memory_invalid_" + strconv.Itoa(index),
				NormalizedInputHash: "hash_memory_invalid_" + strconv.Itoa(index),
				Operation:           tc.operation,
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory invalid input: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), tc.wantCode, tc.wantReread)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsOversizedReplaceBeforeDurableWrite(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		inputJSON string
	}{
		{
			name:      "single replacement final content",
			content:   "one",
			inputJSON: memoryReplaceInputJSON(t, "notes/todo.md", "one", strings.Repeat("x", memoryToolContentMaxBytes+1), false),
		},
		{
			name:      "replace all final content",
			content:   "one one",
			inputJSON: memoryReplaceInputJSON(t, "notes/todo.md", "o", strings.Repeat("x", memoryToolContentMaxBytes/2), true),
		},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_memory_replace_cap_" + strconv.Itoa(index)
			threadID := "thr_bridge_memory_replace_cap_" + strconv.Itoa(index)
			bindingID := "bind_bridge_memory_replace_cap_" + strconv.Itoa(index)
			storeID := "memstore_bridge_memory_replace_cap_" + strconv.Itoa(index)
			toolUseID := "evt_tool_memory_replace_cap_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_memory_replace_cap")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_bridge_memory_replace_cap_"+strconv.Itoa(index), "/notes/todo.md", tc.content)

			refresher := &recordingMemoryProjectionRefresher{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.MemoryProjectionRefresher = refresher
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_memory_replace_cap"),
				ToolUseEventId:      toolUseID,
				NormalizedInputHash: "hash_memory_replace_cap_" + strconv.Itoa(index),
				Operation:           "replace",
				InputJson:           tc.inputJSON,
			})
			if err != nil {
				t.Fatalf("RunMemory oversized replace: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), "content_too_large", false)
			assertMemoryProjectionStateNull(t, admin, sessionID, toolUseID)
			if len(refresher.calls) != 0 {
				t.Fatalf("projection calls = %d; want none for oversized replace validation", len(refresher.calls))
			}
			if count := countMemoryVersions(t, admin, storeID); count != 1 {
				t.Fatalf("memory versions after oversized replace = %d; want seed version only", count)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReadsLegacyOversizedRows(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_legacy_large", "thr_bridge_memory_legacy_large")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_legacy_large", "bind_bridge_memory_legacy_large", 1, "pod_uid_memory_legacy_large")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_legacy_large", "memstore_bridge_memory_legacy_large")
	legacyContent := strings.Repeat("x", memoryToolContentMaxBytes+1)
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_legacy_large", "mem_bridge_memory_legacy_large", "/notes/legacy-large.md", legacyContent)
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_legacy_large", "prep_bridge_memory_legacy_large")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_legacy_large", "2026-01-01T00:00:00Z")

	refresher := &recordingMemoryProjectionRefresher{}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = refresher
	response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_legacy_large", "thr_bridge_memory_legacy_large", "bind_bridge_memory_legacy_large", 1, "pod_uid_memory_legacy_large"),
		ToolUseEventId:      "evt_tool_memory_legacy_large",
		NormalizedInputHash: "hash_memory_legacy_large",
		Operation:           "delete",
		InputJson:           memoryDeleteInputJSON(t, "notes/legacy-large.md", legacyContent),
	})
	if err != nil {
		t.Fatalf("RunMemory legacy oversized delete: %v", err)
	}
	assertMemoryResultStatus(t, response.GetResultJson(), "completed")
	if count := countMemoryVersions(t, admin, "memstore_bridge_memory_legacy_large"); count != 2 {
		t.Fatalf("memory versions after legacy oversized delete = %d; want seed plus delete", count)
	}
}

func TestPostgreSQLBridgeAPIStoreRunMemoryDeletesWithExpectedText(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_delete", "thr_bridge_memory_delete")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_delete", "memstore_bridge_memory_delete")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_delete", "mem_bridge_memory_delete", "/notes/delete.md", "delete me")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_delete", "mem_bridge_memory_delete_other", "/notes/other.md", "keep me")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
	deleted, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_delete", "thr_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete"),
		ToolUseEventId:      "evt_tool_memory_delete",
		NormalizedInputHash: "hash_memory_delete",
		Operation:           "delete",
		InputJson:           `{"action":"delete","path":"notes/delete.md","expected_text":"delete me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory delete: %v", err)
	}
	assertMemoryResultStatus(t, deleted.GetResultJson(), "completed")
	assertMemoryDeleted(t, admin, "memstore_bridge_memory_delete", "/notes/delete.md")

	wrongDelete, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_delete", "thr_bridge_memory_delete", "bind_bridge_memory_delete", 1, "pod_uid_memory_delete"),
		ToolUseEventId:      "evt_tool_memory_delete_wrong",
		NormalizedInputHash: "hash_memory_delete_wrong",
		Operation:           "delete",
		InputJson:           `{"action":"delete","path":"notes/other.md","expected_text":"wrong"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory wrong delete: %v", err)
	}
	assertMemoryToolError(t, wrongDelete.GetResultJson(), "expected_text_mismatch", true)
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRenamesWithExpectedText(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_rename", "thr_bridge_memory_rename")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_rename", "memstore_bridge_memory_rename")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename", "/notes/old.md", "rename me")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_rename", "mem_bridge_memory_rename_collision", "/notes/existing.md", "existing")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}

	wrongRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename_wrong",
		NormalizedInputHash: "hash_memory_rename_wrong",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/wrong.md","expected_text":"wrong"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory wrong rename: %v", err)
	}
	assertMemoryToolError(t, wrongRename.GetResultJson(), "expected_text_mismatch", true)

	collision, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename_collision",
		NormalizedInputHash: "hash_memory_rename_collision",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/existing.md","expected_text":"rename me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory rename collision: %v", err)
	}
	assertMemoryToolError(t, collision.GetResultJson(), "path_exists", true)

	renamed, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_rename", "thr_bridge_memory_rename", "bind_bridge_memory_rename", 1, "pod_uid_memory_rename"),
		ToolUseEventId:      "evt_tool_memory_rename",
		NormalizedInputHash: "hash_memory_rename",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"notes/old.md","new_path":"notes/new.md","expected_text":"rename me"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory rename: %v", err)
	}
	assertMemoryResultStatus(t, renamed.GetResultJson(), "completed")
	if testJSONPathString(t, renamed.GetResultJson(), "path") != "notes/old.md" {
		t.Fatalf("rename result = %s; want path notes/old.md", renamed.GetResultJson())
	}
	if testJSONPathString(t, renamed.GetResultJson(), "new_path") != "notes/new.md" {
		t.Fatalf("rename result = %s; want new_path notes/new.md", renamed.GetResultJson())
	}
	assertMemoryCurrentPathContentAndOperation(t, admin, "memstore_bridge_memory_rename", "mem_bridge_memory_rename", "/notes/new.md", "rename me", "modified")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryReportsStaleReplace(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_replace_stale", "memstore_bridge_memory_replace_stale")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_replace_stale", "mem_bridge_memory_replace_stale", "/notes/repeat.md", "one one")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.MemoryProjectionRefresher = &recordingMemoryProjectionRefresher{}
	missing, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_missing",
		NormalizedInputHash: "hash_memory_replace_missing",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"absent","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace missing: %v", err)
	}
	assertMemoryToolError(t, missing.GetResultJson(), "old_text_not_found", true)

	nonUnique, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_nonunique",
		NormalizedInputHash: "hash_memory_replace_nonunique",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace nonunique: %v", err)
	}
	assertMemoryToolError(t, nonUnique.GetResultJson(), "old_text_not_unique", true)

	replaced, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_replace_stale", "thr_bridge_memory_replace_stale", "bind_bridge_memory_replace_stale", 1, "pod_uid_memory_replace_stale"),
		ToolUseEventId:      "evt_tool_memory_replace_all",
		NormalizedInputHash: "hash_memory_replace_all",
		Operation:           "replace",
		InputJson:           `{"action":"replace","path":"notes/repeat.md","old_text":"one","new_text":"two","replace_all":true}`,
	})
	if err != nil {
		t.Fatalf("RunMemory replace all: %v", err)
	}
	assertMemoryResultStatus(t, replaced.GetResultJson(), "completed")
	assertMemoryCurrentPathAndContent(t, admin, "memstore_bridge_memory_replace_stale", "mem_bridge_memory_replace_stale", "/notes/repeat.md", "two two")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryRejectsPrefixConflictingPaths(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_memory_prefix", "thr_bridge_memory_prefix")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix")
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_memory_prefix", "memstore_bridge_memory_prefix")
	seedBridgeAPIPreparationReady(t, admin, "default", "sesn_bridge_memory_prefix", "prep_bridge_memory_prefix")
	seedBridgeAPIActiveSandbox(t, admin, "default", "sesn_bridge_memory_prefix", "2026-01-01T00:00:00Z")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_a", "/a", "a")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_parent_child", "/parent/child", "child")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_parent_other", "/parent/other", "other")
	seedBridgeAPIMemory(t, admin, "default", "memstore_bridge_memory_prefix", "mem_bridge_memory_prefix_percent", "/literal_%", "percent")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	refresher := &recordingMemoryProjectionRefresher{}
	store.MemoryProjectionRefresher = refresher
	descendant, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_descendant",
		NormalizedInputHash: "hash_memory_prefix_descendant",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"a/b","content":"child"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory descendant conflict: %v", err)
	}
	assertMemoryToolError(t, descendant.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, descendant.GetResultJson(), "message"); got != "memory path is inside an existing memory" {
		t.Fatalf("descendant conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendant.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)
	if len(refresher.calls) != 1 {
		t.Fatalf("memory projection refresh calls = %+v; want one descendant cleanup refresh", refresher.calls)
	}
	assertMemoryProjectionOps(t, refresher.calls[0].Ops, []MemoryProjectionOp{
		{Kind: "remove", RelativePath: "/a/b"},
		{Kind: "upsert", RelativePath: "/a", Content: "a", ContentSHA256: sha256Hex("a")},
	})

	ancestorCreate, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_create_ancestor",
		NormalizedInputHash: "hash_memory_prefix_create_ancestor",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"parent","content":"root"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory create ancestor conflict: %v", err)
	}
	assertMemoryToolError(t, ancestorCreate.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, ancestorCreate.GetResultJson(), "message"); got != "memory path would contain an existing memory" {
		t.Fatalf("ancestor create conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorCreate.GetResultJson(), []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)
	if len(refresher.calls) != 2 {
		t.Fatalf("memory projection refresh calls = %+v; want ancestor refresh", refresher.calls)
	}
	assertMemoryProjectionOps(t, refresher.calls[1].Ops, []MemoryProjectionOp{
		{Kind: "remove", RelativePath: "/parent"},
		{Kind: "upsert", RelativePath: "/parent/child", Content: "child", ContentSHA256: sha256Hex("child")},
		{Kind: "upsert", RelativePath: "/parent/other", Content: "other", ContentSHA256: sha256Hex("other")},
	})

	exactRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_exact",
		NormalizedInputHash: "hash_memory_prefix_rename_exact",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"a","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory exact rename conflict: %v", err)
	}
	assertMemoryToolError(t, exactRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, exactRename.GetResultJson(), "message"); got != "memory target path already exists" {
		t.Fatalf("exact rename conflict message = %q; want exact collision message", got)
	}
	assertMemoryPathConflictResult(t, exactRename.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	descendantRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_descendant",
		NormalizedInputHash: "hash_memory_prefix_rename_descendant",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"a/b","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory descendant rename conflict: %v", err)
	}
	assertMemoryToolError(t, descendantRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, descendantRename.GetResultJson(), "message"); got != "memory target path is inside an existing memory" {
		t.Fatalf("descendant rename conflict message = %q; want distinguishing descendant message", got)
	}
	assertMemoryPathConflictResult(t, descendantRename.GetResultJson(), []memoryPathConflictWireHead{{MemoryID: "mem_bridge_memory_prefix_a", Path: "/a"}}, 1, false)

	ancestorRename, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_rename_ancestor",
		NormalizedInputHash: "hash_memory_prefix_rename_ancestor",
		Operation:           "rename",
		InputJson:           `{"action":"rename","path":"literal_%","new_path":"parent","expected_text":"percent"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory ancestor rename conflict: %v", err)
	}
	assertMemoryToolError(t, ancestorRename.GetResultJson(), "path_exists", true)
	if got := testJSONPathString(t, ancestorRename.GetResultJson(), "message"); got != "memory target path would contain an existing memory" {
		t.Fatalf("ancestor rename conflict message = %q; want distinguishing ancestor message", got)
	}
	assertMemoryPathConflictResult(t, ancestorRename.GetResultJson(), []memoryPathConflictWireHead{
		{MemoryID: "mem_bridge_memory_prefix_parent_child", Path: "/parent/child"},
		{MemoryID: "mem_bridge_memory_prefix_parent_other", Path: "/parent/other"},
	}, 2, false)

	underscore, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
		Scope:               bridgeAPIScope("sesn_bridge_memory_prefix", "thr_bridge_memory_prefix", "bind_bridge_memory_prefix", 1, "pod_uid_memory_prefix"),
		ToolUseEventId:      "evt_tool_memory_prefix_underscore",
		NormalizedInputHash: "hash_memory_prefix_underscore",
		Operation:           "create",
		InputJson:           `{"action":"create","path":"literal_X","content":"underscore is literal"}`,
	})
	if err != nil {
		t.Fatalf("RunMemory literal underscore: %v", err)
	}
	assertMemoryResultStatus(t, underscore.GetResultJson(), "completed")
}

func TestPostgreSQLBridgeAPIStoreRunMemoryBoundsPathConflictWire(t *testing.T) {
	for _, conflictTotal := range []int{MaxMemoryPathConflicts, MaxMemoryPathConflicts + 1} {
		t.Run(strconv.Itoa(conflictTotal), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strconv.Itoa(conflictTotal)
			sessionID := "sesn_bridge_memory_conflict_bound_" + suffix
			threadID := "thr_bridge_memory_conflict_bound_" + suffix
			storeID := "memstore_bridge_memory_conflict_bound_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_memory_conflict_bound", 1, "pod_uid_memory_conflict_bound")
			seedBridgeAPIWritableMemoryStore(t, admin, "default", sessionID, storeID)
			seedBridgeAPIPreparationReady(t, admin, "default", sessionID, "prep_bridge_memory_conflict_bound")
			seedBridgeAPIActiveSandbox(t, admin, "default", sessionID, "2026-01-01T00:00:00Z")

			targetPath := "/root/target"
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_00_exact", targetPath, "exact")
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_01_ancestor", "/root", "ancestor")
			seedBridgeAPIMemory(t, admin, "default", storeID, "mem_99_deep", targetPath+"/deep/descendant", "deep")
			for index := 0; index < conflictTotal-3; index++ {
				memoryID := fmt.Sprintf("mem_%02d_descendant", index+2)
				pathValue := fmt.Sprintf("%s/child-%02d", targetPath, index)
				seedBridgeAPIMemory(t, admin, "default", storeID, memoryID, pathValue, memoryID)
			}

			refresher := &recordingMemoryProjectionRefresher{}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
			store.MemoryProjectionRefresher = refresher
			response, err := store.RunMemory(context.Background(), &bridgev1.RunMemoryRequest{
				Scope:               bridgeAPIScope(sessionID, threadID, "bind_bridge_memory_conflict_bound", 1, "pod_uid_memory_conflict_bound"),
				ToolUseEventId:      "evt_tool_memory_conflict_bound_" + suffix,
				NormalizedInputHash: "hash_memory_conflict_bound_" + suffix,
				Operation:           "create",
				InputJson:           `{"action":"create","path":"root/target","content":"rejected"}`,
			})
			if err != nil {
				t.Fatalf("RunMemory: %v", err)
			}
			assertMemoryToolError(t, response.GetResultJson(), "path_exists", true)

			wantHeads := []memoryPathConflictWireHead{
				{MemoryID: "mem_00_exact", Path: targetPath},
				{MemoryID: "mem_01_ancestor", Path: "/root"},
				{MemoryID: "mem_99_deep", Path: targetPath + "/deep/descendant"},
			}
			for index := 0; len(wantHeads) < min(conflictTotal, MaxMemoryPathConflicts); index++ {
				wantHeads = append(wantHeads, memoryPathConflictWireHead{
					MemoryID: fmt.Sprintf("mem_%02d_descendant", index+2),
					Path:     fmt.Sprintf("%s/child-%02d", targetPath, index),
				})
			}
			assertMemoryPathConflictResult(t, response.GetResultJson(), wantHeads, conflictTotal, conflictTotal > MaxMemoryPathConflicts)

			if len(refresher.calls) != 1 {
				t.Fatalf("memory projection refresh calls = %d; want one", len(refresher.calls))
			}
			if len(refresher.calls[0].Ops) != len(wantHeads) {
				t.Fatalf("refresh ops = %+v; want exactly %d returned conflict heads", refresher.calls[0].Ops, len(wantHeads))
			}
			for index, head := range wantHeads {
				op := refresher.calls[0].Ops[index]
				if op.Kind != "upsert" || op.RelativePath != head.Path {
					t.Fatalf("refresh op[%d] = %+v; want returned conflict head %+v", index, op, head)
				}
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreAcceptSandboxExecutionReplayIsIdentityFenced(t *testing.T) {
	t.Run("durable identity and cross-kind conflicts", testPostgreSQLAcceptSandboxExecutionIdentityFencing)
}

func TestCanonicalRunToolInputMatchesJavaScriptStringifyEscaping(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "shell operators",
			raw:  `{"command":"printf '<>&' && cat <in >out"}`,
			want: `{"command":"printf '<>&' && cat <in >out"}`,
		},
		{
			name: "unicode line separators",
			raw:  "{\"command\":\"before\u2028middle\u2029after\"}",
			want: "{\"command\":\"before\u2028middle\u2029after\"}",
		},
		{
			name: "literal backslash u2028",
			raw:  `{"command":"\\u2028"}`,
			want: `{"command":"\\u2028"}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			canonical, hash, err := canonicalRunToolInput(test.raw)
			if err != nil {
				t.Fatalf("canonicalRunToolInput: %v", err)
			}
			if canonical != test.want || hash != sha256Hex(test.want) {
				t.Fatalf("canonical/hash = %q/%q; want JavaScript bytes %q/%q", canonical, hash, test.want, sha256Hex(test.want))
			}
		})
	}

	first, firstHash, err := canonicalRunToolInput(`{"workdir":"/workspace","cmd":"printf <ok>"}`)
	if err != nil {
		t.Fatalf("canonical first: %v", err)
	}
	second, secondHash, err := canonicalRunToolInput("{ \"cmd\" : \"printf <ok>\", \"workdir\" : \"/workspace\" }")
	if err != nil {
		t.Fatalf("canonical reordered: %v", err)
	}
	if first != second || firstHash != secondHash {
		t.Fatalf("reordered canonical/hash = %q/%q vs %q/%q; want equivalent", first, firstHash, second, secondHash)
	}
}

func TestCanonicalRunToolInputSharedCrossLanguageVectors(t *testing.T) {
	encoded, err := os.ReadFile(filepath.Join(repoRootFromBridgeTest(t), "testdata", "run-tool-canonical-vectors.json"))
	if err != nil {
		t.Fatalf("read shared canonical vectors: %v", err)
	}
	var vectors []struct {
		Name      string   `json:"name"`
		Inputs    []string `json:"inputs"`
		Canonical string   `json:"canonical"`
	}
	if err := json.Unmarshal(encoded, &vectors); err != nil {
		t.Fatalf("decode shared canonical vectors: %v", err)
	}
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			for _, input := range vector.Inputs {
				canonical, hash, err := canonicalRunToolInput(input)
				if err != nil {
					t.Fatalf("canonicalRunToolInput(%q): %v", input, err)
				}
				if canonical != vector.Canonical || hash != sha256Hex(vector.Canonical) {
					t.Fatalf("canonical/hash = %q/%q; want shared vector %q/%q", canonical, hash, vector.Canonical, sha256Hex(vector.Canonical))
				}
			}
		})
	}
	if _, _, err := canonicalRunToolInput(strings.Repeat("[", 257) + "0" + strings.Repeat("]", 257)); err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("over-depth canonical error = %v; want shared closed nesting bound", err)
	}
	if _, _, err := canonicalRunToolInput(`{"unterminated":`); err == nil {
		t.Fatal("malformed canonical input accepted")
	}
}

func TestBridgePreparationResetLocksSessionBeforePreparationForUpdate(t *testing.T) {
	source, err := os.ReadFile("bridge_api_tools.go")
	if err != nil {
		t.Fatalf("read bridge_api_tools.go: %v", err)
	}
	text := string(source)
	resetStart := strings.Index(text, "func resetSessionPreparationAndEnqueuePrepareTx")
	if resetStart < 0 {
		t.Fatal("resetSessionPreparationAndEnqueuePrepareTx not found")
	}
	resetBody := text[resetStart:]
	lockIndex := strings.Index(resetBody, "lockSessionPreparationResetFenceTx")
	forUpdateIndex := strings.Index(resetBody, "loadLatestSessionPreparationReadinessForUpdateTx")
	if lockIndex < 0 || forUpdateIndex < 0 || lockIndex > forUpdateIndex {
		t.Fatalf("preparation reset must lock the sessions row before loading preparation FOR UPDATE; lock=%d load=%d", lockIndex, forUpdateIndex)
	}
	lockStart := strings.Index(text, "func lockSessionPreparationResetFenceTx")
	if lockStart < 0 {
		t.Fatal("lockSessionPreparationResetFenceTx not found")
	}
	lockBody := text[lockStart:]
	lockEnd := strings.Index(lockBody, "\n}\n")
	if lockEnd < 0 {
		t.Fatal("lockSessionPreparationResetFenceTx body end not found")
	}
	lockBody = lockBody[:lockEnd]
	for _, fragment := range []string{"FROM sessions", "FOR UPDATE"} {
		if !strings.Contains(lockBody, fragment) {
			t.Fatalf("session reset fence lock missing %q in:\n%s", fragment, lockBody)
		}
	}
}

func TestRuntimePreparationReadinessLocksSessionBeforePreparationForUpdate(t *testing.T) {
	source, err := os.ReadFile("runtime_delivery.go")
	if err != nil {
		t.Fatalf("read runtime_delivery.go: %v", err)
	}
	body := string(source)
	start := strings.Index(body, "func requireRuntimePreparationReadyTx")
	if start < 0 {
		t.Fatal("requireRuntimePreparationReadyTx not found")
	}
	body = body[start:]
	lockIndex := strings.Index(body, "lockSessionPreparationResetFenceTx")
	forUpdateIndex := strings.Index(body, "loadLatestSessionPreparationReadinessForUpdateTx")
	plainReadIndex := strings.Index(body, "loadLatestSessionPreparationReadinessTx")
	if lockIndex < 0 || forUpdateIndex < 0 || lockIndex > forUpdateIndex {
		t.Fatalf("runtime preparation readiness must lock session before loading preparation FOR UPDATE; lock=%d load=%d", lockIndex, forUpdateIndex)
	}
	if plainReadIndex >= 0 && plainReadIndex < strings.Index(body, "\n}\n") {
		t.Fatalf("runtime preparation readiness uses non-locking readiness load at index %d", plainReadIndex)
	}
}

func TestBridgeAPIReadinessGatesLockSessionBeforePreparationForUpdate(t *testing.T) {
	source, err := os.ReadFile("bridge_api_tools.go")
	if err != nil {
		t.Fatalf("read bridge_api_tools.go: %v", err)
	}
	text := string(source)
	for _, name := range []string{"runToolTargetTx", "outputCaptureTargetTx"} {
		t.Run(name, func(t *testing.T) {
			start := strings.Index(text, "func "+name)
			if start < 0 {
				t.Fatalf("%s not found", name)
			}
			body := text[start:]
			if next := strings.Index(body[len("func "+name):], "\nfunc "); next >= 0 {
				body = body[:len("func "+name)+next]
			}
			lockIndex := strings.Index(body, "lockSessionPreparationResetFenceTx")
			forUpdateIndex := strings.Index(body, "loadLatestSessionPreparationReadinessForUpdateTx")
			plainReadIndex := strings.Index(body, "loadLatestSessionPreparationReadinessTx")
			if lockIndex < 0 || forUpdateIndex < 0 || lockIndex > forUpdateIndex {
				t.Fatalf("%s must lock session before preparation FOR UPDATE; lock=%d load=%d", name, lockIndex, forUpdateIndex)
			}
			if plainReadIndex >= 0 {
				t.Fatalf("%s uses non-locking readiness load at index %d", name, plainReadIndex)
			}
		})
	}
}

func TestBridgeAPIMemoryProjectionRefreshLoadCanResetPreparation(t *testing.T) {
	source, err := os.ReadFile("bridge_api_tools.go")
	if err != nil {
		t.Fatalf("read bridge_api_tools.go: %v", err)
	}
	text := string(source)
	start := strings.Index(text, "func (s *PostgreSQLBridgeAPIStore) completePendingMemoryProjection")
	if start < 0 {
		t.Fatal("completePendingMemoryProjection not found")
	}
	body := text[start:]
	if next := strings.Index(body[len("func (s *PostgreSQLBridgeAPIStore) completePendingMemoryProjection"):], "\nfunc "); next >= 0 {
		body = body[:len("func (s *PostgreSQLBridgeAPIStore) completePendingMemoryProjection")+next]
	}
	for _, fragment := range []string{
		"withScopeTx",
		"verifyRuntimeScopeReadOnlyTx",
		"readRuntimeToolResultReadOnlyTx",
		"memoryProjectionTargetTx",
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("memory projection refresh load missing fragment %q in:\n%s", fragment, body)
		}
	}
	targetStart := strings.Index(text, "func memoryProjectionTargetTx")
	if targetStart < 0 {
		t.Fatal("memoryProjectionTargetTx not found")
	}
	targetBody := text[targetStart:]
	if next := strings.Index(targetBody[len("func memoryProjectionTargetTx"):], "\nfunc "); next >= 0 {
		targetBody = targetBody[:len("func memoryProjectionTargetTx")+next]
	}
	for _, fragment := range []string{
		"lockSessionPreparationResetFenceTx",
		"loadLatestSessionPreparationReadinessForUpdateTx",
		"resetSessionPreparationAndEnqueuePrepareTx",
	} {
		if !strings.Contains(targetBody, fragment) {
			t.Fatalf("memory projection target missing reset-gate fragment %q in:\n%s", fragment, targetBody)
		}
	}
}

func TestVerifyRuntimeScopeRejectsRuntimePodUIDMismatchFromIdentity(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_scope_identity", "thr_bridge_scope_identity")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_scope_identity", "bind_bridge_scope_identity", 1, "pod_uid_scope_identity")

	client := dbconnect.NewClientForTesting(runtime)
	scope := bridgeAPIScope("sesn_bridge_scope_identity", "thr_bridge_scope_identity", "bind_bridge_scope_identity", 1, "pod_uid_scope_identity")
	for _, test := range []struct {
		name     string
		ctx      context.Context
		wantCode codes.Code
	}{
		{
			name:     "no grpc identity keeps direct store tests usable",
			ctx:      context.Background(),
			wantCode: codes.OK,
		},
		{
			name: "bridge service account without pod uid is not a runtime caller",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-system", Name: "bridge"},
			}),
			wantCode: codes.OK,
		},
		{
			name: "runtime pod with matching tokenreview pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
				KubernetesPodUID: "pod_uid_scope_identity",
			}),
			wantCode: codes.OK,
		},
		{
			name: "runtime pod without tokenreview pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount: internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
			}),
			wantCode: codes.PermissionDenied,
		},
		{
			name: "runtime pod cannot claim another target pod uid",
			ctx: internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
				ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
				KubernetesPodUID: "pod_uid_other",
			}),
			wantCode: codes.PermissionDenied,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := client.WithWorkspaceTx(test.ctx, "default", "bridge_api.verify_runtime_scope_identity", func(tx *dbconnect.Tx) error {
				return verifyRuntimeScopeTx(test.ctx, tx, scope)
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("verifyRuntimeScopeTx error = %v; want %s", err, test.wantCode)
			}
			err = client.WithWorkspaceTx(test.ctx, "default", "bridge_api.verify_runtime_scope_identity_readonly", func(tx *dbconnect.Tx) error {
				return verifyRuntimeScopeReadOnlyTx(test.ctx, tx, scope)
			})
			if status.Code(err) != test.wantCode {
				t.Fatalf("verifyRuntimeScopeReadOnlyTx error = %v; want %s", err, test.wantCode)
			}
		})
	}
}

func TestVerifyRuntimeScopeRejectsDeletedSessionWithLiveBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_deleted_scope", "thr_bridge_deleted_scope")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_deleted_scope", "bind_bridge_deleted_scope", 1, "pod_uid_deleted_scope")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET lifecycle_state = 'deleted' WHERE workspace_id = 'default' AND id = 'sesn_bridge_deleted_scope'`); err != nil {
		t.Fatalf("delete session: %v", err)
	}

	client := dbconnect.NewClientForTesting(runtime)
	scope := bridgeAPIScope("sesn_bridge_deleted_scope", "thr_bridge_deleted_scope", "bind_bridge_deleted_scope", 1, "pod_uid_deleted_scope")
	err := client.WithWorkspaceTx(context.Background(), "default", "bridge_api.verify_deleted_runtime_scope", func(tx *dbconnect.Tx) error {
		return verifyRuntimeScopeTx(context.Background(), tx, scope)
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("verifyRuntimeScopeTx error = %v; want FailedPrecondition", err)
	}
	var bindingRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_runtime_bindings WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_deleted_scope'`).Scan(&bindingRows); err != nil {
		t.Fatalf("count retained cleanup binding: %v", err)
	}
	if bindingRows != 1 {
		t.Fatalf("binding rows = %d; want retained for cleanup finalization", bindingRows)
	}
}

func TestResourceCredentialExpiresFresh(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	tests := []struct {
		name      string
		expiresAt sql.NullString
		want      bool
	}{
		{name: "null means no file resource projection credential", expiresAt: sql.NullString{}, want: true},
		{name: "outside margin", expiresAt: sql.NullString{String: "2026-01-01T01:01:00Z", Valid: true}, want: true},
		{name: "exact margin boundary", expiresAt: sql.NullString{String: "2026-01-01T01:00:00Z", Valid: true}, want: true},
		{name: "inside margin", expiresAt: sql.NullString{String: "2026-01-01T00:59:59Z", Valid: true}, want: false},
		{name: "malformed fails closed", expiresAt: sql.NullString{String: "not-rfc3339", Valid: true}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resourceCredentialExpiresFresh(tc.expiresAt, now, 30*time.Minute); got != tc.want {
				t.Fatalf("resourceCredentialExpiresFresh() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestResourceCredentialMaterializationRecorded(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt sql.NullString
		want      bool
	}{
		{name: "null means no file resource projection credential", expiresAt: sql.NullString{}, want: false},
		{name: "blank means no materialized file resource credential", expiresAt: sql.NullString{String: "   ", Valid: true}, want: false},
		{name: "future expiry records materialization", expiresAt: sql.NullString{String: "2026-01-01T00:30:01Z", Valid: true}, want: true},
		{name: "past expiry still records materialization", expiresAt: sql.NullString{String: "2026-01-01T00:29:59Z", Valid: true}, want: true},
		{name: "malformed expiry still fails toward rotation", expiresAt: sql.NullString{String: "not-rfc3339", Valid: true}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resourceCredentialMaterializationRecorded(tc.expiresAt); got != tc.want {
				t.Fatalf("resourceCredentialMaterializationRecorded() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestRunToolReadsResourceRoot(t *testing.T) {
	rootsJSON := `[{"path":"/workspace/data/report.csv","mode":"read"},{"path":"/mnt/session/uploads","mode":"read"},{"path":"/workspace/writable","mode":"read_write"}]`
	tests := []struct {
		name      string
		toolName  string
		inputJSON string
		want      bool
	}{
		{name: "absolute resource path", toolName: "Read", inputJSON: `{"path":"/workspace/data/report.csv"}`, want: true},
		{name: "relative resource path resolves under workspace", toolName: "read", inputJSON: `{"path":"data/report.csv"}`, want: true},
		{name: "read under resource directory", toolName: "Read", inputJSON: `{"path":"/mnt/session/uploads/nested/file.txt"}`, want: true},
		{name: "non read tool ignored", toolName: "Write", inputJSON: `{"path":"/workspace/data/report.csv"}`, want: false},
		{name: "writable root ignored", toolName: "Read", inputJSON: `{"path":"/workspace/writable/file.txt"}`, want: false},
		{name: "non resource path ignored", toolName: "Read", inputJSON: `{"path":"/workspace/other.txt"}`, want: false},
		{name: "runtime file path field", toolName: "Read", inputJSON: `{"file_path":"/workspace/data/report.csv"}`, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := runToolReadsResourceRoot(tc.toolName, tc.inputJSON, rootsJSON); got != tc.want {
				t.Fatalf("runToolReadsResourceRoot() = %v; want %v", got, tc.want)
			}
		})
	}
}
