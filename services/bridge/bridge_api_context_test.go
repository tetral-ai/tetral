package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge context protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreLoadContextReturnsFreshSignedSnapshot(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load", "thr_bridge_load")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load", "bind_bridge_load", 7, "pod_uid_load")

	key := []byte("bridge-runtime-binding-token-test-key-32")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = key
	store.Clock = func() time.Time { return now }
	request := &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_load", "thr_bridge_load", "bind_bridge_load", 7, "pod_uid_load"),
		RuntimeInputId: "rin_bridge_load",
	}
	response, err := store.LoadContext(context.Background(), request)
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	claims := verifyRuntimeBindingTokenForTest(t, response.GetRuntimeBindingToken(), key)
	if claims["workspace_id"] != "default" ||
		claims["session_id"] != "sesn_bridge_load" ||
		claims["session_thread_id"] != "thr_bridge_load" ||
		claims["binding_id"] != "bind_bridge_load" ||
		int64(claims["binding_generation"].(float64)) != 7 ||
		claims["runtime_pod_uid"] != "pod_uid_load" ||
		int64(claims["exp"].(float64)) != now.Add(5*time.Minute).Unix() {
		t.Fatalf("runtime binding token claims = %#v; want scoped binding token", claims)
	}
	second, err := store.LoadContext(context.Background(), request)
	if err != nil {
		t.Fatalf("second LoadContext: %v", err)
	}
	if second.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("second LoadContext status = %s; want committed fresh snapshot", second.GetAck().GetStatus())
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(second.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse second LoadContext payload: %v", err)
	}
	if payload.Messages == nil || len(payload.Messages) != 0 {
		t.Fatalf("second LoadContext messages = %#v; want explicit empty array", payload.Messages)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCarriesRawTurnFactsAndMessageLineage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_turn_facts"
		threadID  = "thr_turn_facts"
		bindingID = "bind_turn_facts"
		podUID    = "pod_turn_facts"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIRuntimeInput(t, admin, "default", sessionID, threadID, "rin_turn_input", bindingID, podUID, "evt_turn_input")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-turn-facts-key-32-bytes!!")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	committed, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_turn_input", InputKind: "messages",
		EventIds: []string{"evt_turn_input"}, SequenceFrom: 1, SequenceTo: 1,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeInputDraftForTest(
			"default", sessionID, threadID, "messages", "rin_turn_input", "evt_turn_input",
			bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT, "user", "hello",
		)},
	})
	if err != nil {
		t.Fatalf("commit input: %v", err)
	}
	messageID := committed.GetDeclaration().GetReceipts()[0].GetMessages()[0].GetMessageId()
	messageBoundary := int64(1)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_turn_start", ModelRequestId: "mreq_turn_facts",
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_turn_facts"}`,
		ContextThroughMessageSequence: &messageBoundary, RequestKind: "agent_provider_request",
	})
	if err != nil {
		t.Fatalf("write request start: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_turn_end", ModelRequestId: "mreq_turn_facts",
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "tool_use", UsageJson: `{}`,
		RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_turn_facts",
	})
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode LoadContext: %v", err)
	}
	if len(payload.TurnFacts.Events) != 2 || len(payload.TurnFacts.MessageLineage) != 1 {
		t.Fatalf("turn facts = %+v; want start/end and one message lineage", payload.TurnFacts)
	}
	if got := payload.TurnFacts.Events[0]; got.EventID != start.GetEventId() || got.RequestStart == nil ||
		got.RequestStart.RequestKind != "agent_provider_request" || got.RequestStart.ContextThroughMessageSequence != 1 {
		t.Fatalf("request-start fact = %+v", got)
	}
	lineage := payload.TurnFacts.MessageLineage[0]
	if lineage.MessageID != messageID || len(lineage.Entries) != 1 ||
		lineage.Entries[0].LineageKind != "declaration_receipt" || lineage.Entries[0].SourceKind != "messages" {
		t.Fatalf("message lineage = %+v", lineage)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCutsTurnFactsAtLatestCompaction(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_context_compaction_cut"
		threadID  = "thr_context_compaction_cut"
		bindingID = "bind_context_compaction_cut"
		podUID    = "pod_context_compaction_cut"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("context-compaction-cut-key-32byte")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "evt_rwrite_context_compaction_running")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, model_request_id, runtime_write_id, projection_json, created_at, updated_at
		) VALUES
		('default', $1, $2, 'evt_context_old_start', 2, 'span.model_request_start',
		 '{"type":"span.model_request_start","model_request_id":"mreq_context_old"}',
		 'internal', false, 'mreq_context_old', NULL,
		 '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', NOW(), NOW()),
		('default', $1, $2, 'evt_context_old_tool', 3, 'agent.tool_use',
		 '{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}',
		 'public', true, 'mreq_context_old', NULL, '{}', NOW(), NOW()),
		('default', $1, $2, 'evt_context_old_end', 4, 'span.model_request_end',
		 '{"type":"span.model_request_end","model_request_id":"mreq_context_old","model_request_start_id":"evt_context_old_start","is_error":false}',
		 'internal', false, 'mreq_context_old', NULL, '{}', NOW(), NOW()),
		('default', $1, $2, 'evt_context_old_repair', 5, 'agent.tool_result',
		 '{"type":"agent.tool_result","tool_use_id":"evt_context_old_tool","is_error":true,"reason":"runtime_pod_lost"}',
		 'public', true, 'mreq_context_old', 'rwrite_runtime_pod_lost_tool_old', '{}', NOW(), NOW())`,
		sessionID, threadID,
	); err != nil {
		t.Fatalf("seed pre-compaction request: %v", err)
	}
	seedBridgeAPIDurableToolMessage(
		t, admin, "default", sessionID, threadID, "mreq_context_old",
		"evt_context_old_tool", "call_context_old", "Read",
	)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_messages
		    SET last_event_id = 'evt_context_old_repair',
		        data_json = jsonb_set(
		          data_json::jsonb, '{parts,0,state}',
		          '{"status":"error","error":{"type":"runtime_pod_lost","message":"lost"}}'::jsonb
		        )::text
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2`,
		sessionID, threadID,
	); err != nil {
		t.Fatalf("project pre-compaction pod-loss repair: %v", err)
	}

	start := seedBridgeAPIRequestStart(
		t, store, scope, "rwrite_context_compaction_start", "mreq_context_compaction",
		"compaction_summary", 1,
	)
	const checkpointText = "<conversation-checkpoint><summary>Old request compacted.</summary><recent-context></recent-context></conversation-checkpoint>"
	checkpointID := stableRuntimeID(
		"runtime_message_draft", "default", sessionID, threadID,
		"agent.thread_context_compacted", "mreq_context_compaction",
		runtimeDraftKindToken(bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT), "0",
	)
	compactedThrough := int64(1)
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_compaction_end", ModelRequestId: "mreq_context_compaction",
		ModelRequestStartEventId: start.GetEventId(), RequestKind: "compaction_summary", FinishReason: "stop", UsageJson: `{}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId: checkpointID, SourceKind: "agent.thread_context_compacted", SourceId: "mreq_context_compaction",
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT,
			MessageInfoJson: `{"role":"user","origin":"runtime","status":"completed"}`,
			Parts: []*bridgev1.RuntimePartDraft{{
				RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", checkpointID, "text", "0"),
				PartKind:           "text", PartJson: `{"type":"text","text":"` + checkpointText + `","truncated":false,"status":"completed"}`,
			}},
		}},
		CompactedThroughMessageSequence: &compactedThrough,
		CompactionEventPayloadJson:      `{"type":"agent.thread_context_compacted","summary":"Old request compacted.","recent_context":[]}`,
	}); err != nil {
		t.Fatalf("write compaction checkpoint: %v", err)
	}

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_context_compaction_cut",
	})
	if err != nil {
		t.Fatalf("LoadContext after compaction: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode compacted context: %v", err)
	}
	if payload.DurableTurnID == nil || *payload.DurableTurnID != "evt_rwrite_context_compaction_running" {
		t.Fatalf("durable turn id = %v; want open compaction run", payload.DurableTurnID)
	}
	if len(payload.Messages) != 1 || len(payload.TurnFacts.Events) != 3 ||
		payload.TurnFacts.Events[0].EventID != *payload.DurableTurnID {
		t.Fatalf("compacted context = messages %d events %+v; want running plus compaction Start/End", len(payload.Messages), payload.TurnFacts.Events)
	}
	for _, event := range payload.TurnFacts.Events[1:] {
		if event.ModelRequestID == nil || *event.ModelRequestID != "mreq_context_compaction" {
			t.Fatalf("historical turn fact leaked past compaction: %+v", event)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCarriesMCPPodLossRepairLineage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_context_mcp_pod_loss"
		threadID       = "thr_context_mcp_pod_loss"
		modelRequestID = "mreq_context_mcp_pod_loss"
		bindingID      = "bind_context_mcp_pod_loss"
		podUID         = "pod_context_mcp_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("context-mcp-pod-loss-key-32bytes")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_context_mcp_start", modelRequestID, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_mcp_tool", ModelRequestId: modelRequestID,
		EventType:   "agent.mcp_tool_use",
		PayloadJson: `{"type":"agent.mcp_tool_use","name":"search_code","mcp_server_name":"github","input":{"q":"x"},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_context_mcp_tool", "agent.mcp_tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_context_mcp","toolName":"search_code","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write MCP Tool Use: %v", err)
	}
	binding := runtimeBindingForDelivery{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("repair MCP pod loss: %v", err)
	}

	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_context_mcp_replacement', binding_generation = 2,
		        agent_runtime_pod_uid = 'pod_context_mcp_replacement'
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
		t.Fatalf("replace MCP pod-loss binding: %v", err)
	}
	replacementScope := bridgeAPIScope(sessionID, threadID, "bind_context_mcp_replacement", 2, "pod_context_mcp_replacement")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: replacementScope, RuntimeInputId: "rin_context_mcp_replacement",
	})
	if err != nil {
		t.Fatalf("LoadContext after MCP pod loss: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode MCP pod-loss context: %v", err)
	}
	if len(payload.TurnFacts.MessageLineage) != 1 {
		t.Fatalf("MCP pod-loss lineage = %+v; want one repaired message", payload.TurnFacts.MessageLineage)
	}
	lineage := payload.TurnFacts.MessageLineage[0]
	if len(lineage.Entries) != 2 || lineage.Entries[0].LineageKind != "declaration_receipt" ||
		lineage.Entries[0].SourceKind != "agent.mcp_tool_use" ||
		lineage.Entries[1].LineageKind != "bridge_pod_loss_repair" ||
		lineage.Entries[1].ToolUseEventID != toolUse.GetEventId() || lineage.Entries[1].Outcome != "error" {
		t.Fatalf("MCP pod-loss lineage = %+v; want declaration followed by terminal repair", lineage)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCarriesEveryPodLossRepairOnSharedMessage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_context_multi_pod_loss"
		threadID       = "thr_context_multi_pod_loss"
		modelRequestID = "mreq_context_multi_pod_loss"
		bindingID      = "bind_context_multi_pod_loss"
		podUID         = "pod_context_multi_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("context-multi-pod-loss-key-32b")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_context_multi_start", modelRequestID, "agent_provider_request", 0)
	first, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_multi_tool_1", ModelRequestId: modelRequestID,
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_context_multi_tool_1", "agent.tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_context_multi_1","toolName":"Read","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write first shared Tool Use: %v", err)
	}
	second, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_multi_tool_2", ModelRequestId: modelRequestID,
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Bash","input":{},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_context_multi_tool_2", "agent.tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_context_multi_1","toolName":"Read","toolUseEventId":"` + first.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_context_multi_2","toolName":"Bash","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write second shared Tool Use: %v", err)
	}
	binding := runtimeBindingForDelivery{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID}
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, sessionID, binding, time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("repair shared-message pod loss: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_context_multi_replacement', binding_generation = 2,
		        agent_runtime_pod_uid = 'pod_context_multi_replacement'
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID); err != nil {
		t.Fatalf("replace shared-message binding: %v", err)
	}
	replacementScope := bridgeAPIScope(sessionID, threadID, "bind_context_multi_replacement", 2, "pod_context_multi_replacement")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: replacementScope, RuntimeInputId: "rin_context_multi_replacement",
	})
	if err != nil {
		t.Fatalf("LoadContext after shared-message pod loss: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode shared-message pod-loss context: %v", err)
	}
	if len(payload.TurnFacts.MessageLineage) != 1 {
		t.Fatalf("shared-message lineage = %+v; want one message", payload.TurnFacts.MessageLineage)
	}
	repaired := make(map[string]bool)
	for _, entry := range payload.TurnFacts.MessageLineage[0].Entries {
		if entry.LineageKind == "bridge_pod_loss_repair" {
			repaired[entry.ToolUseEventID] = true
		}
	}
	if !repaired[first.GetEventId()] || !repaired[second.GetEventId()] || len(repaired) != 2 {
		t.Fatalf("shared-message repair lineage = %+v; want both Tool Uses", payload.TurnFacts.MessageLineage[0])
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCarriesPodLossDeliveryRepairLineage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	fixture := seedRuntimePodLostDeliveryFixture(
		t, admin, 80, "spawn_agent", "idle", true, true, false, true, false,
	)
	if _, err := runRuntimePodLostRepairTransaction(
		context.Background(), runtime, fixture.sessionID, fixture.binding, fixture.now,
	); err != nil {
		t.Fatalf("repair delivered sub-agent Tool Use: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_id = 'bind_context_delivery_replacement', binding_generation = 81,
		        agent_runtime_pod_uid = 'pod_context_delivery_replacement'
		  WHERE workspace_id = 'default' AND session_id = $1`, fixture.sessionID); err != nil {
		t.Fatalf("replace delivery-repair binding: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("context-delivery-repair-key-32by")
	scope := bridgeAPIScope(
		fixture.sessionID, fixture.parentThreadID,
		"bind_context_delivery_replacement", 81, "pod_context_delivery_replacement",
	)
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_context_delivery_replacement",
	})
	if err != nil {
		t.Fatalf("LoadContext after delivery repair: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode delivery-repair context: %v", err)
	}
	found := false
	for _, lineage := range payload.TurnFacts.MessageLineage {
		for _, entry := range lineage.Entries {
			if entry.LineageKind == "bridge_pod_loss_repair" && entry.ToolUseEventID == fixture.toolUseEventID {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("delivery repair lineage = %+v; want pod-loss repair for %s", payload.TurnFacts.MessageLineage, fixture.toolUseEventID)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextCarriesIdleCleanupRepairLineage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_context_idle_cleanup"
		threadID       = "thr_context_idle_cleanup"
		modelRequestID = "mreq_context_idle_cleanup"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_context_idle_cleanup", 1, "pod_context_idle_cleanup")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("context-idle-cleanup-key-32bytes!!")
	scope := bridgeAPIScope(sessionID, threadID, "bind_context_idle_cleanup", 1, "pod_context_idle_cleanup")
	const durableTurnID = "evt_context_idle_cleanup_running"
	seedBridgeAPIOpenDurableTurn(t, admin, scope, durableTurnID)
	start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_context_cleanup_start", modelRequestID, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_cleanup_tool", ModelRequestId: modelRequestID,
		EventType:   "agent.tool_use",
		PayloadJson: `{"type":"agent.tool_use","name":"Write","input":{"path":"a.txt"},"evaluated_permission":"ask"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_context_cleanup_tool", "agent.tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_context_cleanup","toolName":"Write","state":{"status":"running","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write cleanup Tool Use: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_context_cleanup_end", ModelRequestId: modelRequestID,
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "tool_calls", UsageJson: `{}`,
		RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("seal cleanup request: %v", err)
	}
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, &bridgev1.FinishIdleRequest{
		Scope: scope, DurableTurnId: durableTurnID,
		StopReasonJson: `{"type":"requires_action","event_ids":["` + toolUse.GetEventId() + `"]}`,
	}); err != nil {
		t.Fatalf("finish requires-action turn: %v", err)
	}
	var resultEventID string
	client := dbconnect.NewClientForTesting(runtime)
	if err := client.WithWorkspaceTx(context.Background(), "default", "test.context_idle_cleanup_writer", func(tx *dbconnect.Tx) error {
		var err error
		resultEventID, err = insertPendingToolTerminalResultTx(
			context.Background(), tx, scope,
			cleanupPendingWait{
				ThreadID: threadID, ToolUseEventID: toolUse.GetEventId(), ModelToolCallID: "call_context_cleanup",
				ToolName: "Write", InputJSON: `{"path":"a.txt"}`, EventType: "agent.tool_use",
			},
			pendingToolTerminal{PartStatus: "error", ErrorType: "cleanup_expired", Message: "expired"},
			time.Date(2026, 1, 1, 0, 10, 0, 0, time.UTC),
		)
		return err
	}); err != nil {
		t.Fatalf("write idle-cleanup Tool Result: %v", err)
	}

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_context_idle_cleanup",
	})
	if err != nil {
		t.Fatalf("LoadContext after idle cleanup: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("decode idle-cleanup context: %v", err)
	}
	var repair *bridgeLoadContextMessageLineageEntry
	for _, lineage := range payload.TurnFacts.MessageLineage {
		for index := range lineage.Entries {
			entry := &lineage.Entries[index]
			if entry.LineageKind == "bridge_idle_cleanup_repair" {
				repair = entry
			}
		}
	}
	if repair == nil || repair.RepairEventID != resultEventID ||
		repair.ToolUseEventID != toolUse.GetEventId() || repair.Outcome != "error" {
		t.Fatalf("idle-cleanup lineage = %+v; want terminal cleanup repair", payload.TurnFacts.MessageLineage)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextSeparatesApprovalAndSandboxExecutionRecovery(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_sandbox_recovery"
		threadID  = "thr_bridge_sandbox_recovery"
		bindingID = "bind_bridge_sandbox_recovery"
		podUID    = "pod_bridge_sandbox_recovery"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-sandbox-recovery-key-32b")
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_sandbox_recovery_start", "mrq_pending_approval", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_sandbox_recovery_tool", ModelRequestId: "mrq_pending_approval",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"dangerous_tool","input":{},"evaluated_permission":"ask"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_bridge_sandbox_recovery_tool", "agent.tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"toolu_cleanup_wait","toolName":"dangerous_tool","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write pending sandbox tool: %v", err)
	}
	toolUseEventID := toolUse.GetEventId()
	_, inputHash, err := canonicalRunToolInput(`{}`)
	if err != nil {
		t.Fatalf("canonical sandbox input: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving', decision = 'allow'
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND tool_use_event_id = $3`,
		sessionID, threadID, toolUseEventID,
	); err != nil {
		t.Fatalf("allow pending sandbox tool: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			background_task_started, created_at, updated_at
		) VALUES ('default', $1, $2, $3, 'sandbox_tool', $4, 'dangerous_tool', '{}',
			'committed', NULL, 'toolu_cleanup_wait', 'running', 1, FALSE, NOW(), NOW())`,
		sessionID, threadID, toolUseEventID, inputHash,
	); err != nil {
		t.Fatalf("seed sandbox execution: %v", err)
	}
	seedBridgeAPIBackgroundTask(t, admin, "default", sessionID, threadID, bindingID, "task_bridge_sandbox_recovery", toolUseEventID)

	load := func(runtimeInputID string) bridgeLoadContextPayload {
		t.Helper()
		response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope:          scope,
			RuntimeInputId: runtimeInputID,
		})
		if err != nil {
			t.Fatalf("LoadContext: %v", err)
		}
		var payload bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
			t.Fatalf("decode context: %v", err)
		}
		return payload
	}

	payload := load("rin_bridge_sandbox_recovery")
	if len(payload.PendingToolUses) != 0 {
		t.Fatalf("pending approvals = %#v; want execution-owned approval excluded", payload.PendingToolUses)
	}
	if len(payload.PendingSandboxExecutions) != 1 {
		t.Fatalf("pending sandbox executions = %#v; want one", payload.PendingSandboxExecutions)
	}
	execution := payload.PendingSandboxExecutions[0]
	if execution.ToolUseEventID != toolUseEventID || execution.ModelRequestID != "mrq_pending_approval" ||
		execution.ModelToolCallID != "toolu_cleanup_wait" || execution.ToolName != "dangerous_tool" ||
		execution.ExecutionState != "running" || string(execution.Input) != "{}" {
		t.Fatalf("pending sandbox execution = %#v; want durable execution identity", execution)
	}
	if !reflect.DeepEqual(payload.ColdCoverage.PendingSandboxExecutionIDs, []string{toolUseEventID}) ||
		len(payload.ColdCoverage.PendingToolIDs) != 0 {
		t.Fatalf("cold coverage = %#v; want disjoint sandbox execution identity", payload.ColdCoverage)
	}
	if !reflect.DeepEqual(payload.BackgroundTools, []bridgeLoadContextBackgroundTool{{
		TaskID: "task_bridge_sandbox_recovery", SourceToolUseEventID: toolUseEventID,
	}}) {
		t.Fatalf("background tools = %#v; want durable task and Tool Use identity", payload.BackgroundTools)
	}

	const resultJSON = `{"status":"success","result":{"summary":"already settled","attachments":[]}}`
	resultDigest := sha256Hex(resultJSON)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'terminal_unconsumed', result_json = $4, result_digest = $5
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND tool_use_event_id = $3`,
		sessionID, threadID, toolUseEventID, resultJSON, resultDigest,
	); err != nil {
		t.Fatalf("make sandbox result ready: %v", err)
	}
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_sandbox_recovery_result", ModelRequestId: "mrq_pending_approval",
		EventType: "agent.tool_result", SandboxResultDigest: &resultDigest,
		PayloadJson: `{"type":"agent.tool_result","tool_use_id":"` + toolUseEventID + `","content":[{"type":"text","text":"already settled"}],"is_error":false}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_bridge_sandbox_recovery_result", "agent.tool_result", "completed",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"toolu_cleanup_wait","toolName":"dangerous_tool","toolUseEventId":"` + toolUseEventID + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{},"preview":"{}","truncated":false},"output":{"text":"already settled","truncated":false}}}`},
		)},
	}); err != nil {
		t.Fatalf("write sandbox terminal result: %v", err)
	}
	payload = load("rin_bridge_sandbox_recovery_terminal")
	if len(payload.PendingSandboxExecutions) != 0 || len(payload.ColdCoverage.PendingSandboxExecutionIDs) != 0 {
		t.Fatalf("terminal Tool Result left sandbox recovery visible: %#v", payload)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextReturnsOnlyUncommittedCompletionMail(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_baseline"
		mainID    = "thr_bridge_completion_baseline_main"
		childID   = "thr_bridge_completion_baseline_child"
		bindingID = "bind_bridge_completion_baseline"
		podUID    = "pod_bridge_completion_baseline"
		delivery  = "delivery_bridge_completion_baseline"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	messageJSON := bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_bridge_completion_baseline", completionMailEnvelope("main", "task_"+childID, "done"))
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_baseline_sent", 1, "agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, delivery, childID, mainID, "", "sevt_bridge_completion_baseline_spawn", messageJSON))
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-completion-baseline-key-32b")

	before, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		RuntimeInputId: "rin_bridge_completion_baseline_before",
	})
	if err != nil {
		t.Fatalf("LoadContext before completion receipt: %v", err)
	}
	var beforePayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(before.GetContextJson()), &beforePayload); err != nil {
		t.Fatalf("decode context before completion receipt: %v", err)
	}
	if len(beforePayload.PendingAgentMail) != 1 || beforePayload.PendingAgentMail[0].DeliveryID != delivery {
		t.Fatalf("pending completion mail before receipt = %#v; want delivery", beforePayload.PendingAgentMail)
	}
	var rawBeforePayload struct {
		PendingAgentMail []map[string]json.RawMessage `json:"pendingAgentMail"`
	}
	if err := json.Unmarshal([]byte(before.GetContextJson()), &rawBeforePayload); err != nil {
		t.Fatalf("decode pending completion descriptor shape: %v", err)
	}
	if len(rawBeforePayload.PendingAgentMail) != 1 ||
		len(rawBeforePayload.PendingAgentMail[0]) != 3 ||
		rawBeforePayload.PendingAgentMail[0]["deliveryId"] == nil ||
		rawBeforePayload.PendingAgentMail[0]["sourceThreadId"] == nil ||
		rawBeforePayload.PendingAgentMail[0]["sourceToolUseEventId"] == nil {
		t.Fatalf("pending completion descriptor = %#v; want identity fields only", rawBeforePayload.PendingAgentMail)
	}

	if _, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		"agent_mail:"+delivery,
		delivery,
		childID,
		"sevt_bridge_completion_baseline_spawn",
		messageJSON,
	)); err != nil {
		t.Fatalf("CommitInputs completion receipt: %v", err)
	}
	after, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID),
		RuntimeInputId: "rin_bridge_completion_baseline_after",
	})
	if err != nil {
		t.Fatalf("LoadContext after completion receipt: %v", err)
	}
	var afterPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(after.GetContextJson()), &afterPayload); err != nil {
		t.Fatalf("decode context after completion receipt: %v", err)
	}
	if len(afterPayload.PendingAgentMail) != 0 {
		t.Fatalf("pending completion mail after receipt = %#v; want none", afterPayload.PendingAgentMail)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextReturnsOnlyUncommittedCompletionDescriptors(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_currency"
		mainID    = "thr_bridge_completion_currency_main"
		childID   = "thr_bridge_completion_currency_child"
		bindingID = "bind_bridge_completion_currency"
		podUID    = "pod_bridge_completion_currency"
		delivery  = "delivery_bridge_completion_currency"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_currency_opener", 1,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`)
	messageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_currency",
		completionMailEnvelope("main", "task_"+childID, "current body"),
	)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_currency_sent", 2,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, delivery, childID, mainID, "", "sevt_bridge_completion_currency_spawn", messageJSON))
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-completion-currency-key")
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)
	if _, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
		t,
		admin,
		scope,
		"agent_mail:"+delivery,
		delivery,
		childID,
		"sevt_bridge_completion_currency_spawn",
		messageJSON,
	)); err != nil {
		t.Fatalf("commit completion receipt: %v", err)
	}

	current, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_currency_current",
	})
	if err != nil {
		t.Fatalf("load context after committed completion: %v", err)
	}
	var currentPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(current.GetContextJson()), &currentPayload); err != nil {
		t.Fatalf("decode context after committed completion: %v", err)
	}
	if len(currentPayload.PendingAgentMail) != 0 {
		t.Fatalf("pending mail after committed completion = %#v; want none", currentPayload.PendingAgentMail)
	}

	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_currency_newer_opener", 3,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`)
	superseded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_currency_superseded",
	})
	if err != nil {
		t.Fatalf("load context after a newer child event: %v", err)
	}
	var supersededPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(superseded.GetContextJson()), &supersededPayload); err != nil {
		t.Fatalf("decode context after a newer child event: %v", err)
	}
	if len(supersededPayload.PendingAgentMail) != 0 {
		t.Fatalf("pending committed completion = %#v; want none", supersededPayload.PendingAgentMail)
	}
	var receiptCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2
		    AND type='agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id'=$3`,
		sessionID,
		mainID,
		delivery,
	).Scan(&receiptCount); err != nil {
		t.Fatalf("count received completion events: %v", err)
	}
	if receiptCount != 1 {
		t.Fatalf("received completion events = %d; want original receipt only", receiptCount)
	}

	const owedDelivery = "delivery_bridge_completion_currency_owed"
	owedMessageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_currency_owed",
		completionMailEnvelope("main", "task_"+childID, "owed body"),
	)
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_currency_owed", 4,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, owedDelivery, childID, mainID, "", "sevt_bridge_completion_currency_owed", owedMessageJSON))
	seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_currency_after_owed", 5,
		"agent.thread_message_received", `{"type":"agent.thread_message_received"}`)
	owed, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_currency_owed",
	})
	if err != nil {
		t.Fatalf("load owed completion after newer opener: %v", err)
	}
	var owedPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(owed.GetContextJson()), &owedPayload); err != nil {
		t.Fatalf("decode owed completion: %v", err)
	}
	if len(owedPayload.PendingAgentMail) != 1 || owedPayload.PendingAgentMail[0].DeliveryID != owedDelivery {
		t.Fatalf("owed completion = %#v; want %s", owedPayload.PendingAgentMail, owedDelivery)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextBoundsCompletionMailAcrossColdPasses(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_completion_window"
		mainID    = "thr_bridge_completion_window_main"
		bindingID = "bind_bridge_completion_window"
		podUID    = "pod_bridge_completion_window"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	for index := 1; index <= 6; index++ {
		childID := "thr_bridge_completion_window_child_" + strconv.Itoa(index)
		seedBridgeAPIChildThread(t, admin, "default", sessionID, mainID, childID)
		messageJSON := bridgeRuntimeNotificationMessageJSON(
			t,
			sessionID,
			"msg_bridge_completion_window_"+strconv.Itoa(index),
			completionMailEnvelope("main", "child_"+strconv.Itoa(index), "done "+strconv.Itoa(index)),
		)
		seedBridgeAPIEvent(t, admin, "default", sessionID, childID, "evt_bridge_completion_window_"+strconv.Itoa(index), int64(index),
			"agent.thread_message_sent",
			bridgeInterAgentSentEventJSON(t, "delivery_bridge_completion_window_"+strconv.Itoa(index), childID, mainID, "", "sevt_bridge_completion_window_"+strconv.Itoa(index), messageJSON))
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-completion-window-key-32b")
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, mainID, bindingID, 1, podUID)

	unfiltered, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_window_unfiltered",
	})
	if err != nil {
		t.Fatalf("LoadContext unfiltered completion window: %v", err)
	}
	var unfilteredPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(unfiltered.GetContextJson()), &unfilteredPayload); err != nil {
		t.Fatalf("decode unfiltered completion window: %v", err)
	}
	if len(unfilteredPayload.PendingAgentMail) != MailFetchMaxEnvelopes {
		t.Fatalf("unfiltered pending mail = %d; want %d", len(unfilteredPayload.PendingAgentMail), MailFetchMaxEnvelopes)
	}
	for _, mail := range unfilteredPayload.PendingAgentMail {
		resolved, err := store.ResolveInterAgentDelivery(context.Background(), &bridgev1.ResolveInterAgentDeliveryRequest{
			Scope:         scope,
			ChildThreadId: mail.SourceThreadID,
			DeliveryId:    mail.DeliveryID,
		})
		if err != nil {
			t.Fatalf("resolve bounded completion %s: %v", mail.DeliveryID, err)
		}
		if _, err := store.CommitInputs(context.Background(), bridgeAgentMailCommitRequestForTest(
			t,
			admin,
			scope,
			"agent_mail:"+mail.DeliveryID,
			mail.DeliveryID,
			mail.SourceThreadID,
			mail.SourceToolUseEventID,
			resolved.GetMessageJson(),
		)); err != nil {
			t.Fatalf("commit bounded completion receipt %s: %v", mail.DeliveryID, err)
		}
	}
	nextPass, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_window_next_pass",
	})
	if err != nil {
		t.Fatalf("LoadContext next completion window: %v", err)
	}
	var nextPassPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(nextPass.GetContextJson()), &nextPassPayload); err != nil {
		t.Fatalf("decode next completion window: %v", err)
	}
	if len(nextPassPayload.PendingAgentMail) != 2 {
		t.Fatalf("next-pass pending mail = %d; want remaining 2", len(nextPassPayload.PendingAgentMail))
	}

	targetChildID := "thr_bridge_completion_window_child_6"
	secondTargetMessageJSON := bridgeRuntimeNotificationMessageJSON(
		t,
		sessionID,
		"msg_bridge_completion_window_7",
		completionMailEnvelope("main", "child_6", "done again"),
	)
	seedBridgeAPIEvent(t, admin, "default", sessionID, targetChildID, "evt_bridge_completion_window_7", 7,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(t, "delivery_bridge_completion_window_7", targetChildID, mainID, "", "sevt_bridge_completion_window_7", secondTargetMessageJSON))
	filtered, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_window_after_append",
	})
	if err != nil {
		t.Fatalf("LoadContext completion window after append: %v", err)
	}
	var filteredPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(filtered.GetContextJson()), &filteredPayload); err != nil {
		t.Fatalf("decode completion window after append: %v", err)
	}
	if len(filteredPayload.PendingAgentMail) != 3 ||
		filteredPayload.PendingAgentMail[0].DeliveryID != "delivery_bridge_completion_window_5" ||
		filteredPayload.PendingAgentMail[1].DeliveryID != "delivery_bridge_completion_window_6" ||
		filteredPayload.PendingAgentMail[2].DeliveryID != "delivery_bridge_completion_window_7" {
		t.Fatalf("pending mail after append = %#v; want remaining envelopes in sent order", filteredPayload.PendingAgentMail)
	}

	refiltered, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_completion_window_replay",
	})
	if err != nil {
		t.Fatalf("LoadContext replayed completion window: %v", err)
	}
	var refilteredPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(refiltered.GetContextJson()), &refilteredPayload); err != nil {
		t.Fatalf("decode replayed completion window: %v", err)
	}
	if !reflect.DeepEqual(refilteredPayload.PendingAgentMail, filteredPayload.PendingAgentMail) {
		t.Fatalf("replayed pending mail = %#v; want stable %#v", refilteredPayload.PendingAgentMail, filteredPayload.PendingAgentMail)
	}
}

func TestPostgreSQLBridgeAPIStoreRefreshRuntimeBindingTokenUsesLiveThreadBinding(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_refresh_token", "thr_bridge_refresh_token")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_refresh_token", "bind_bridge_refresh_token", 7, "pod_uid_refresh_token")

	key := []byte("bridge-runtime-binding-token-test-key-32")
	now := time.Date(2026, 1, 1, 0, 4, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = key
	store.Clock = func() time.Time { return now }
	response, err := store.RefreshRuntimeBindingToken(context.Background(), &bridgev1.RefreshRuntimeBindingTokenRequest{
		Scope: bridgeAPIScope("sesn_bridge_refresh_token", "thr_bridge_refresh_token", "bind_bridge_refresh_token", 7, "pod_uid_refresh_token"),
	})
	if err != nil {
		t.Fatalf("RefreshRuntimeBindingToken: %v", err)
	}
	claims := verifyRuntimeBindingTokenForTest(t, response.GetRuntimeBindingToken(), key)
	if claims["session_thread_id"] != "thr_bridge_refresh_token" ||
		int64(claims["binding_generation"].(float64)) != 7 ||
		int64(claims["exp"].(float64)) != now.Add(5*time.Minute).Unix() {
		t.Fatalf("refreshed runtime binding token claims = %#v; want live thread binding", claims)
	}

	_, err = admin.ExecContext(context.Background(),
		`UPDATE session_runtime_bindings
		    SET binding_generation = 8
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_refresh_token'`)
	if err != nil {
		t.Fatalf("supersede runtime binding: %v", err)
	}
	_, err = store.RefreshRuntimeBindingToken(context.Background(), &bridgev1.RefreshRuntimeBindingTokenRequest{
		Scope: bridgeAPIScope("sesn_bridge_refresh_token", "thr_bridge_refresh_token", "bind_bridge_refresh_token", 7, "pod_uid_refresh_token"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RefreshRuntimeBindingToken stale generation err = %v; want FailedPrecondition", err)
	}

	_, err = store.RefreshRuntimeBindingToken(context.Background(), &bridgev1.RefreshRuntimeBindingTokenRequest{
		Scope: bridgeAPIScope("sesn_bridge_refresh_token", "thr_missing_refresh_token", "bind_bridge_refresh_token", 8, "pod_uid_refresh_token"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RefreshRuntimeBindingToken missing thread err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextRejectsStaleThreadScope(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load_stale_thread", "thr_bridge_load_stale_thread")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load_stale_thread", "bind_bridge_load_stale_thread", 1, "pod_uid_load_stale_thread")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_load_stale_thread", "thr_missing_load_stale_thread", "bind_bridge_load_stale_thread", 1, "pod_uid_load_stale_thread"),
		RuntimeInputId: "rin_bridge_load_stale_thread",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("LoadContext stale thread err = %v; want FAILED_PRECONDITION", err)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextRejectsMalformedDurableMessage(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load_malformed", "thr_bridge_load_malformed")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load_malformed", "bind_bridge_load_malformed", 1, "pod_uid_load_malformed")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind, data_json, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_load_malformed', 'thr_bridge_load_malformed',
			'msg_bridge_load_malformed', 1, 'assistant', '{not-json',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed malformed durable message: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_load_malformed", "thr_bridge_load_malformed", "bind_bridge_load_malformed", 1, "pod_uid_load_malformed"),
		RuntimeInputId: "rin_bridge_load_malformed",
	})
	if status.Code(err) != codes.FailedPrecondition || strings.Contains(err.Error(), "{not-json") {
		t.Fatalf("LoadContext malformed message err = %v; want sanitized FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextReturnsRuntimeSurface(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load_surface", "thr_bridge_load_surface")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load_surface", "bind_bridge_load_surface", 3, "pod_uid_load_surface")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_load_surface", `{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"system":"Operate as the session specialist.",
		"tools":[{"type":"tetral_agent_toolset","family":"claude"}],
		"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],
		"skills":[{"skill_id":"sk_docs","version":"latest"}],
		"metadata":{}
	}`)
	seedBridgeAPIWritableMemoryStore(t, admin, "default", "sesn_bridge_load_surface", "memstore_bridge_load_surface")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO skills (workspace_id, skill_id, display_title, latest_version, created_at, updated_at)
		 VALUES ('default', 'sk_docs', 'Docs', '3.0.0', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("seed skill: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO skill_versions (
			workspace_id, skill_id, skill_version_id, version, name, description,
			directory, blob_key, size_bytes, sha256, created_at
		 ) VALUES (
			'default', 'sk_docs', 'skv_docs_3', '3.0.0', 'Docs',
			'Read the project documentation.', 'docs', 'skills/sk_docs/3.0.0', 1,
			'sha256_docs', '2026-01-01T00:00:00Z'
		 )`); err != nil {
		t.Fatalf("seed skill version: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET approval_mode = 'approve_for_me',
		        config_generation = 9,
		        installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}',
		        provider_access_json = '{"anthropic":"session"}',
		        vault_ids_json = '["vlt_1"]'
		  WHERE workspace_id = 'default'
		    AND id = 'sesn_bridge_load_surface'`,
	); err != nil {
		t.Fatalf("seed runtime surface session fields: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	scope := bridgeAPIScope("sesn_bridge_load_surface", "thr_bridge_load_surface", "bind_bridge_load_surface", 3, "pod_uid_load_surface")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_load_surface_start", "mrq_pending_approval", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_load_surface_tool", ModelRequestId: "mrq_pending_approval",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"dangerous_tool","input":{},"evaluated_permission":"ask"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t, scope, "rwrite_bridge_load_surface_tool", "agent.tool_use", "streaming",
			bridgeRuntimePartDraftForTest{kind: "tool", json: `{"type":"tool","toolCallId":"toolu_cleanup_wait","toolName":"dangerous_tool","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("write pending approval: %v", err)
	}
	toolUseEventID := toolUse.GetEventId()
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving',
		        decision = 'deny',
		        deny_message = 'not safe'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_load_surface'
		    AND tool_use_event_id = $1`, toolUseEventID); err != nil {
		t.Fatalf("seed pending approval decision: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_inbox (
			workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
			event_ids_json, sequence_from, sequence_to, status, binding_id, binding_generation,
			target_pod_uid, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_load_surface', 'thr_bridge_load_surface', 'rin_should_not_load',
			'messages', $1, 2, 2, 'accepted',
			'bind_bridge_load_surface', 3, 'pod_uid_load_surface',
			'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`, `["`+toolUseEventID+`"]`,
	); err != nil {
		t.Fatalf("seed runtime inbox: %v", err)
	}
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_load_surface", "thr_bridge_load_surface", "", "task_bridge_load_surface", "evt_bridge_load_background")
	seedBridgeAPIBackgroundTask(t, admin, "default", "sesn_bridge_load_surface", "thr_bridge_load_surface", "bind_bridge_load_stale", "task_bridge_load_stale", "evt_bridge_load_stale")

	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_load_surface",
	})
	if err != nil {
		t.Fatalf("LoadContext runtime surface: %v", err)
	}
	var payload struct {
		Messages      []json.RawMessage `json:"messages"`
		RuntimeConfig struct {
			ConfigGeneration           int64                      `json:"configGeneration"`
			ApprovalMode               string                     `json:"approvalMode"`
			ProviderRescheduleBudget   int64                      `json:"providerRescheduleBudget"`
			CompactionRescheduleBudget int64                      `json:"compactionRescheduleBudget"`
			System                     *string                    `json:"system"`
			MemoryStores               []bridgeRuntimeMemoryStore `json:"memoryStores"`
			Agent                      struct {
				ID      string          `json:"id"`
				Version int64           `json:"version"`
				Config  json.RawMessage `json:"config"`
			} `json:"agent"`
			ToolPolicy struct {
				ApprovalMode string `json:"approvalMode"`
				Tools        any    `json:"tools"`
			} `json:"toolPolicy"`
			Skills         json.RawMessage `json:"skills"`
			SkillsIndex    json.RawMessage `json:"skillsIndex"`
			InstalledTools []struct {
				Type   string `json:"type"`
				Family string `json:"family"`
			} `json:"installedTools"`
		} `json:"runtimeConfig"`
		PendingToolUses []struct {
			ToolUseEventID  string          `json:"toolUseEventId"`
			ModelRequestID  string          `json:"modelRequestId"`
			ModelToolCallID string          `json:"modelToolCallId"`
			ToolName        string          `json:"toolName"`
			Status          string          `json:"status"`
			Decision        string          `json:"decision"`
			DenyMessage     string          `json:"denyMessage"`
			Input           json.RawMessage `json:"input"`
		} `json:"pendingToolUses"`
		BackgroundTools []struct {
			TaskID               string `json:"taskId"`
			SourceToolUseEventID string `json:"sourceToolUseEventId"`
		} `json:"backgroundTools"`
	}
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse LoadContext runtime surface: %v", err)
	}
	if len(payload.Messages) != 1 {
		t.Fatalf("LoadContext messages = %d; want pending Tool Use projection", len(payload.Messages))
	}
	if payload.RuntimeConfig.ConfigGeneration != 9 ||
		payload.RuntimeConfig.ApprovalMode != "approve_for_me" ||
		payload.RuntimeConfig.ProviderRescheduleBudget != 3 ||
		payload.RuntimeConfig.CompactionRescheduleBudget != 2 ||
		payload.RuntimeConfig.System == nil ||
		*payload.RuntimeConfig.System != "Operate as the session specialist." ||
		len(payload.RuntimeConfig.MemoryStores) != 1 ||
		payload.RuntimeConfig.MemoryStores[0].MemoryStoreID != "memstore_bridge_load_surface" ||
		payload.RuntimeConfig.MemoryStores[0].Name != "memory" ||
		payload.RuntimeConfig.MemoryStores[0].Access != "read_write" ||
		payload.RuntimeConfig.MemoryStores[0].Instructions != nil ||
		payload.RuntimeConfig.Agent.ID != "agent_sesn_bridge_load_surface" ||
		payload.RuntimeConfig.Agent.Version != 1 ||
		payload.RuntimeConfig.ToolPolicy.ApprovalMode != "approve_for_me" ||
		len(payload.RuntimeConfig.InstalledTools) != 1 ||
		payload.RuntimeConfig.InstalledTools[0].Type != "tetral_agent_toolset" ||
		payload.RuntimeConfig.InstalledTools[0].Family != "claude" ||
		!strings.Contains(string(payload.RuntimeConfig.Skills), `"version":"latest"`) ||
		!strings.Contains(string(payload.RuntimeConfig.SkillsIndex), `"skill_version_id":"skv_docs_3"`) ||
		!strings.Contains(string(payload.RuntimeConfig.SkillsIndex), `"version":"3.0.0"`) {
		t.Fatalf("runtime surface = %s; missing expected runtime config", response.GetContextJson())
	}
	if strings.Contains(response.GetContextJson(), "providerAccess") ||
		strings.Contains(response.GetContextJson(), "vaultIds") ||
		strings.Contains(response.GetContextJson(), "vlt_1") {
		t.Fatalf("LoadContext leaked provider/vault fields: %s", response.GetContextJson())
	}
	if len(payload.PendingToolUses) != 1 ||
		payload.PendingToolUses[0].ToolUseEventID != toolUseEventID ||
		payload.PendingToolUses[0].ModelRequestID != "mrq_pending_approval" ||
		payload.PendingToolUses[0].ModelToolCallID != "toolu_cleanup_wait" ||
		payload.PendingToolUses[0].ToolName != "dangerous_tool" ||
		payload.PendingToolUses[0].Status != "resolving" ||
		payload.PendingToolUses[0].Decision != "deny" ||
		payload.PendingToolUses[0].DenyMessage != "not safe" ||
		string(payload.PendingToolUses[0].Input) != "{}" {
		t.Fatalf("pending tool uses = %+v in %s; want decided unresolved approval row", payload.PendingToolUses, response.GetContextJson())
	}
	if len(payload.BackgroundTools) != 1 ||
		payload.BackgroundTools[0].TaskID != "task_bridge_load_surface" ||
		payload.BackgroundTools[0].SourceToolUseEventID != "evt_bridge_load_background" {
		t.Fatalf("background tools = %+v in %s; want running task handle", payload.BackgroundTools, response.GetContextJson())
	}
	if strings.Contains(response.GetContextJson(), "rin_should_not_load") {
		t.Fatalf("LoadContext leaked session_runtime_inbox row: %s", response.GetContextJson())
	}
}

func TestPostgreSQLBridgeAPIStoreColdPatchAlwaysCarriesToolPolicy(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load_runtime_agent", "thr_bridge_load_runtime_agent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load_runtime_agent", "bind_bridge_load_runtime_agent", 4, "pod_uid_load_runtime_agent")
	seedBridgeAPIAgentConfig(t, admin, "default", "sesn_bridge_load_runtime_agent", `{
		"name":"agent",
		"model":"anthropic/claude-opus-4-8",
		"tools":[],
		"mcp_servers":[],
		"skills":[],
		"metadata":{}
	}`)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET installed_tools_json = '{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}'
		  WHERE workspace_id = 'default'
		    AND id = 'sesn_bridge_load_runtime_agent'`,
	); err != nil {
		t.Fatalf("seed session runtime agent config: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_load_runtime_agent", "thr_bridge_load_runtime_agent", "bind_bridge_load_runtime_agent", 4, "pod_uid_load_runtime_agent"),
		RuntimeInputId: "rin_bridge_load_runtime_agent",
	})
	if err != nil {
		t.Fatalf("LoadContext runtime agent config: %v", err)
	}
	var payload struct {
		RuntimeConfig struct {
			ToolPolicy struct {
				MCPServers []struct {
					Name string `json:"name"`
				} `json:"mcpServers"`
				MCPToolsets []struct {
					MCPServerName string `json:"mcpServerName"`
					Configs       []struct {
						Name string `json:"name"`
					} `json:"configs"`
				} `json:"mcpToolsets"`
			} `json:"toolPolicy"`
		} `json:"runtimeConfig"`
	}
	if err := json.Unmarshal([]byte(response.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse LoadContext runtime agent config: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(response.GetContextJson()), &raw); err != nil {
		t.Fatalf("parse LoadContext raw context: %v", err)
	}
	var runtimeConfig map[string]json.RawMessage
	if err := json.Unmarshal(raw["runtimeConfig"], &runtimeConfig); err != nil {
		t.Fatalf("parse LoadContext raw runtime config: %v", err)
	}
	if _, exists := runtimeConfig["toolPolicy"]; !exists {
		t.Fatalf("LoadContext runtimeConfig omits mandatory toolPolicy: %s", response.GetContextJson())
	}
	if len(payload.RuntimeConfig.ToolPolicy.MCPServers) != 1 ||
		payload.RuntimeConfig.ToolPolicy.MCPServers[0].Name != "github" ||
		len(payload.RuntimeConfig.ToolPolicy.MCPToolsets) != 1 ||
		payload.RuntimeConfig.ToolPolicy.MCPToolsets[0].MCPServerName != "github" ||
		len(payload.RuntimeConfig.ToolPolicy.MCPToolsets[0].Configs) != 1 ||
		payload.RuntimeConfig.ToolPolicy.MCPToolsets[0].Configs[0].Name != "github_search" {
		t.Fatalf("runtime tool policy = %+v in %s; want session runtime agent config", payload.RuntimeConfig.ToolPolicy, response.GetContextJson())
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextRejectsMalformedDurableInstalledToolFamilyAsAPIError(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_load_bad_family", "thr_bridge_load_bad_family")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_load_bad_family", "bind_bridge_load_bad_family", 1, "pod_uid_load_bad_family")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions
		    SET installed_tools_json = '{"tools":[],"mcp_servers":[]}'
		  WHERE workspace_id = 'default' AND id = 'sesn_bridge_load_bad_family'`); err != nil {
		t.Fatalf("seed malformed durable tools: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_load_bad_family", "thr_bridge_load_bad_family", "bind_bridge_load_bad_family", 1, "pod_uid_load_bad_family"),
		RuntimeInputId: "rin_bridge_load_bad_family",
	})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "api_error") {
		t.Fatalf("LoadContext error = %v; want internal api_error", err)
	}
}

func TestPostgreSQLBridgeAPIStoreLoadContextRejectsEveryInvalidInstalledToolsSnapshotAsAPIError(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{name: "blank", raw: ``},
		{name: "empty_array", raw: `[]`},
		{name: "non_object", raw: `"tools"`},
		{name: "malformed", raw: `{`},
		{name: "missing_family", raw: `{"tools":[{"type":"mcp_toolset","mcp_server_name":"github"}]}`},
		{name: "duplicate_family", raw: `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"tetral_agent_toolset","family":"claude"}]}`},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_invalid_installed_" + strconv.Itoa(index)
			threadID := "thr_bridge_invalid_installed_" + strconv.Itoa(index)
			bindingID := "bind_bridge_invalid_installed_" + strconv.Itoa(index)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_invalid_installed")
			// A valid Agent config proves the durable snapshot can never fall back.
			seedBridgeAPIAgentConfig(t, admin, "default", sessionID, `{"tools":[{"type":"tetral_agent_toolset","family":"claude"}]}`)
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE sessions SET installed_tools_json = $1 WHERE workspace_id = 'default' AND id = $2`, test.raw, sessionID); err != nil {
				t.Fatalf("seed invalid installed tools: %v", err)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
			_, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
				Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_invalid_installed"), RuntimeInputId: "rin_invalid_installed",
			})
			if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "api_error") {
				t.Fatalf("LoadContext error = %v; want Internal api_error", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventProjectsToolResultIntoLoadContext(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_result", "thr_bridge_tool_result")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_result", "bind_bridge_tool_result", 1, "pod_uid_tool_result")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return now }
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	scope := bridgeAPIScope("sesn_bridge_tool_result", "thr_bridge_tool_result", "bind_bridge_tool_result", 1, "pod_uid_tool_result")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_tool_result_start", "mrq_pending_approval", "agent_provider_request", 0)
	seedBridgeAPIPendingApproval(t, admin, "default", "sesn_bridge_tool_result", "thr_bridge_tool_result", "evt_public_tool_use", 2)
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_pending_tool_uses
		    SET status = 'resolving',
		        decision = 'allow'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_result'
		    AND tool_use_event_id = 'evt_public_tool_use'`); err != nil {
		t.Fatalf("seed resolving pending approval: %v", err)
	}

	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_tool_result",
		ModelRequestId: "mrq_pending_approval",
		EventType:      "agent.tool_result",
		PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"evt_public_tool_use","content":[{"type":"text","text":"done"}]}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_tool_result",
			"agent.tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"tool-call-result","toolName":"search","toolUseEventId":"evt_public_tool_use","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false},"output":{"text":"done","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent tool_result: %v", err)
	}

	var messageDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND source_event_id = $1 AND kind = 'assistant'`, response.GetEventId()).Scan(&messageDataJSON); err != nil {
		t.Fatalf("read projected tool result message: %v", err)
	}
	assertToolResultRuntimeMessage(t, messageDataJSON, "tool-call-result", "search", "evt_public_tool_use", "completed", "done")
	var pendingStatus string
	var pendingResultEventID sql.NullString
	var pendingResolvedAt sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, result_event_id, resolved_at
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_result'
		    AND tool_use_event_id = 'evt_public_tool_use'`).Scan(&pendingStatus, &pendingResultEventID, &pendingResolvedAt); err != nil {
		t.Fatalf("read pending approval after tool result: %v", err)
	}
	if pendingStatus != "resolved" || !pendingResultEventID.Valid || pendingResultEventID.String != response.GetEventId() || !pendingResolvedAt.Valid {
		t.Fatalf("pending approval after tool result status=%q result=%v resolved=%v; want resolved with terminal event",
			pendingStatus, pendingResultEventID, pendingResolvedAt.Valid)
	}

	contextResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_tool_result", "thr_bridge_tool_result", "bind_bridge_tool_result", 1, "pod_uid_tool_result"),
		RuntimeInputId: "rin_bridge_tool_result_load",
	})
	if err != nil {
		t.Fatalf("LoadContext after tool result: %v", err)
	}
	var contextPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(contextResponse.GetContextJson()), &contextPayload); err != nil {
		t.Fatalf("parse context JSON: %v", err)
	}
	if len(contextPayload.Messages) != 1 {
		t.Fatalf("LoadContext messages = %d; want 1 in %s", len(contextPayload.Messages), contextResponse.GetContextJson())
	}
	assertToolResultRuntimeMessage(t, string(contextPayload.Messages[0]), "tool-call-result", "search", "evt_public_tool_use", "completed", "done")
	var resultFact *bridgeLoadContextToolResult
	for _, event := range contextPayload.TurnFacts.Events {
		if event.Type == "agent.tool_result" {
			resultFact = event.ToolResult
		}
	}
	if resultFact == nil || resultFact.ToolUseEventID != "evt_public_tool_use" || resultFact.Outcome != "success" {
		t.Fatalf("LoadContext Tool Result fact = %+v; want successful projected outcome", resultFact)
	}
}

func TestBridgeTurnToolResultFactUsesToolUseProjectionIdentity(t *testing.T) {
	result, err := bridgeTurnToolResultFact(
		"agent.tool_result",
		`{"type":"agent.tool_result","tool_use_id":"evt_cancelled_tool"}`,
		map[string][]bridgeContextToolPart{
			"evt_cancelled_tool": {{Status: "cancelled"}},
		},
	)
	if err != nil {
		t.Fatalf("build cancelled Tool Result fact: %v", err)
	}
	if result.ToolUseEventID != "evt_cancelled_tool" || result.Outcome != "cancelled" {
		t.Fatalf("cancelled Tool Result fact = %+v; want Tool Use keyed cancelled outcome", result)
	}
}

func TestPostgreSQLBridgeAPIStoreProjectsMCPApprovalAndResultIntoLoadContext(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_mcp_projection", "thr_bridge_mcp_projection")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_mcp_projection", "bind_bridge_mcp_projection", 1, "pod_uid_mcp_projection")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	scope := bridgeAPIScope("sesn_bridge_mcp_projection", "thr_bridge_mcp_projection", "bind_bridge_mcp_projection", 1, "pod_uid_mcp_projection")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_mcp_start", "mreq_bridge_mcp", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_mcp_use",
		ModelRequestId: "mreq_bridge_mcp",
		EventType:      "agent.mcp_tool_use",
		PayloadJson:    `{"type":"agent.mcp_tool_use","name":"search_code","input":{"q":"x"},"mcp_server_name":"github","evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_mcp_use",
			"agent.mcp_tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_mcp","toolName":"search_code","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent MCP tool use: %v", err)
	}
	setBridgeAPIPendingApprovalStatus(t, admin, "default", "sesn_bridge_mcp_projection", "thr_bridge_mcp_projection", toolUse.GetEventId(), "resolving")
	claim := &bridgev1.ClaimMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      toolUse.GetEventId(),
		NormalizedInputHash: "hash_bridge_mcp_projection",
		McpServerName:       "github",
		ToolName:            "search_code",
		InputJson:           `{"q":"x"}`,
	}
	if _, err := store.ClaimMcpToolResult(context.Background(), claim); err != nil {
		t.Fatalf("ClaimMcpToolResult: %v", err)
	}
	materialized, err := store.CommitMcpToolResult(context.Background(), &bridgev1.CommitMcpToolResultRequest{
		Scope:               scope,
		ToolUseEventId:      claim.GetToolUseEventId(),
		NormalizedInputHash: claim.GetNormalizedInputHash(),
		McpServerName:       claim.GetMcpServerName(),
		ToolName:            claim.GetToolName(),
		InputJson:           claim.GetInputJson(),
		ResultJson:          `{"response":{"status":1,"result_text":"done","attachments":[]},"content_items":1,"refresh_triggered":false}`,
	})
	if err != nil {
		t.Fatalf("CommitMcpToolResult: %v", err)
	}

	_, err = store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_mcp_result_conflicting_identity",
		ModelRequestId:           "mreq_bridge_mcp",
		EventType:                "agent.mcp_tool_result",
		PayloadJson:              `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","tool_use_event_id":"evt_conflicting_mcp_alias","content":[{"type":"text","text":"done"}]}`,
		SessionVisible:           true,
		McpMaterializationHandle: materialized.MaterializationHandle,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_mcp_result_conflicting_identity",
			"agent.mcp_tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_mcp","toolName":"search_code","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"completed","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false},"output":{"text":"done","truncated":false}}}`,
			},
		)},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent conflicting MCP identity err = %v; want FailedPrecondition", err)
	}

	result, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_mcp_result",
		ModelRequestId:           "mreq_bridge_mcp",
		EventType:                "agent.mcp_tool_result",
		PayloadJson:              `{"type":"agent.mcp_tool_result","mcp_tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"done"}]}`,
		SessionVisible:           true,
		McpMaterializationHandle: materialized.MaterializationHandle,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_mcp_result",
			"agent.mcp_tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_mcp","toolName":"search_code","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"completed","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false},"output":{"text":"done","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent MCP tool result: %v", err)
	}

	var pendingStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_pending_tool_uses WHERE workspace_id = 'default' AND tool_use_event_id = $1`,
		toolUse.GetEventId()).Scan(&pendingStatus); err != nil {
		t.Fatalf("read MCP pending status: %v", err)
	}
	if pendingStatus != "resolved" {
		t.Fatalf("MCP pending status = %q; want resolved", pendingStatus)
	}
	var messageDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND last_event_id = $1`,
		result.GetEventId()).Scan(&messageDataJSON); err != nil {
		t.Fatalf("read MCP result projection: %v", err)
	}
	assertToolResultRuntimeMessage(t, messageDataJSON, "call_bridge_mcp", "search_code", toolUse.GetEventId(), "completed", "done")
	if !strings.Contains(messageDataJSON, `"toolEvent":{"kind":"mcp","mcpServerName":"github"}`) {
		t.Fatalf("MCP result lost tool event identity: %s", messageDataJSON)
	}

	contextResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          scope,
		RuntimeInputId: "rin_bridge_mcp_load",
	})
	if err != nil {
		t.Fatalf("LoadContext MCP result: %v", err)
	}
	if !strings.Contains(contextResponse.GetContextJson(), `"toolEvent":{"kind":"mcp","mcpServerName":"github"}`) {
		t.Fatalf("LoadContext lost MCP result identity: %s", contextResponse.GetContextJson())
	}
	var contextPayload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(contextResponse.GetContextJson()), &contextPayload); err != nil {
		t.Fatalf("parse MCP context JSON: %v", err)
	}
	var resultFact *bridgeLoadContextToolResult
	for _, event := range contextPayload.TurnFacts.Events {
		if event.Type == "agent.mcp_tool_result" {
			resultFact = event.ToolResult
		}
	}
	if resultFact == nil || resultFact.ToolUseEventID != toolUse.GetEventId() || resultFact.Outcome != "success" {
		t.Fatalf("LoadContext MCP Tool Result fact = %+v; want successful projected outcome", resultFact)
	}
}
