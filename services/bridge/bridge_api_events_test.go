package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge events protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreRejectsClosedDeclarationCarrierMatrixBeforeMutation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_declaration_carrier_matrix"
	const threadID = "thr_declaration_carrier_matrix"
	const modelRequestID = "mreq_declaration_carrier_matrix"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_declaration_carrier_matrix", 1, "pod_declaration_carrier_matrix")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, "bind_declaration_carrier_matrix", 1, "pod_declaration_carrier_matrix")
	start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_declaration_carrier_start", modelRequestID, "agent_provider_request", 0)
	appendDeclaration := bridgeRuntimeOutputAppendForTest(t, scope, "carrier_matrix", "agent.message", "streaming",
		bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"x","truncated":false,"status":"completed"}`})
	settlement := bridgeCompletedToolSettlementForTest("tool_carrier_matrix", "done")

	tests := []struct {
		name string
		call func() error
	}{
		{name: "Assistant event missing append", call: func() error {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "carrier_missing_append", ModelRequestId: modelRequestID, EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[]}`})
			return err
		}},
		{name: "Assistant event carries Tool settlement", call: func() error {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "carrier_wrong_assistant", ModelRequestId: modelRequestID, EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[]}`, Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: settlement}})
			return err
		}},
		{name: "Tool Result missing settlement", call: func() error {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "carrier_missing_settlement", ModelRequestId: modelRequestID, EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_id":"tool_carrier_matrix","content":[]}`})
			return err
		}},
		{name: "Tool Result carries Assistant append", call: func() error {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "carrier_wrong_result", ModelRequestId: modelRequestID, EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_id":"tool_carrier_matrix","content":[]}`, Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: appendDeclaration}})
			return err
		}},
		{name: "unrelated event carries declaration", call: func() error {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{Scope: scope, RuntimeWriteId: "carrier_unrelated", ModelRequestId: modelRequestID, EventType: "agent.thinking", PayloadJson: `{"type":"agent.thinking"}`, Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: appendDeclaration}})
			return err
		}},
		{name: "ordinary Request End carries compaction create", call: func() error {
			_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "carrier_request_end_compaction", ModelRequestId: modelRequestID, ModelRequestStartEventId: start.GetEventId(), RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`, CompactionCheckpointCreate: &bridgev1.RuntimeMessageCreate{}})
			return err
		}},
		{name: "failed Request End carries trailing append", call: func() error {
			_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{Scope: scope, RuntimeWriteId: "carrier_request_end_append", ModelRequestId: modelRequestID, ModelRequestStartEventId: start.GetEventId(), RequestKind: "agent_provider_request", FinishReason: "error", UsageJson: `{}`, IsError: true, ErrorKind: "provider_error", TrailingPartAppend: appendDeclaration})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var beforeEvents, beforeMessages, beforeOperations int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&beforeEvents, &beforeMessages, &beforeOperations); err != nil {
				t.Fatalf("count declaration state before rejection: %v", err)
			}
			if err := test.call(); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("carrier error = %v; want InvalidArgument", err)
			}
			var afterEvents, afterMessages, afterOperations int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&afterEvents, &afterMessages, &afterOperations); err != nil {
				t.Fatalf("count declaration state after rejection: %v", err)
			}
			if afterEvents != beforeEvents || afterMessages != beforeMessages || afterOperations != beforeOperations {
				t.Fatalf("rejected carrier mutated events/messages/operations: before %d/%d/%d after %d/%d/%d", beforeEvents, beforeMessages, beforeOperations, afterEvents, afterMessages, afterOperations)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventPersistsStreamProjectionAndIdempotency(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_event", "thr_bridge_event")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_event", "bind_bridge_event", 1, "pod_uid_event")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_event", "thr_bridge_event", "bind_bridge_event", 1, "pod_uid_event")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_event_start", "mreq_bridge_event", "agent_provider_request", 0)
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_event",
		ModelRequestId: "mreq_bridge_event",
		EventType:      "agent.message",
		PayloadJson:    `{"type":"agent.message","provider_session_id":"sess_provider","content":[{"type":"text","text":"hello","provider_metadata":{"raw":"secret"}}],"byte_identity":{"raw":"&<>   \u0026"}}`,
		SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_event",
			"agent.message",
			"completed",
			bridgeRuntimePartCreateForTest{
				kind: "text",
				json: `{"type":"text","text":"hello","truncated":false,"status":"completed"}`,
			},
		)},
	}
	expectedPayloadJSON := `{"byte_identity":{"raw":"&<>   \u0026"},"content":[{"text":"hello","type":"text"}],"type":"agent.message"}`
	response, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}
	if response.GetEventId() == "" || response.GetSequence() != 2 {
		t.Fatalf("WriteEvent response event=%q sequence=%d; want generated/2 after Request Start", response.GetEventId(), response.GetSequence())
	}
	if response.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("WriteEvent committed ack write id = %q; want %q", response.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}
	replay, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetEventId() != response.GetEventId() {
		t.Fatalf("replay = %+v; want duplicate same event", replay)
	}
	if replay.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("WriteEvent duplicate ack write id = %q; want %q", replay.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}

	var streamPosition int64
	var latestPosition int64
	var assistantProjectionCount int
	var eventPayloadJSON string
	var eventProjectionJSON string
	var messageID string
	var messageDataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT stream_position FROM session_event_stream_changes WHERE workspace_id = 'default' AND event_id = $1`, response.GetEventId()).Scan(&streamPosition); err != nil {
		t.Fatalf("read stream change: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT latest_stream_position, payload_json, projection_json FROM session_events WHERE workspace_id = 'default' AND event_id = $1`, response.GetEventId()).Scan(&latestPosition, &eventPayloadJSON, &eventProjectionJSON); err != nil {
		t.Fatalf("read latest stream position: %v", err)
	}
	if eventPayloadJSON != expectedPayloadJSON {
		t.Fatalf("stored event payload = %q; want byte-identical %q", eventPayloadJSON, expectedPayloadJSON)
	}
	serverToolUse, err := normalizeServerToolUseUsage(request)
	if err != nil {
		t.Fatalf("normalize server tool use: %v", err)
	}
	storedPayloadDigest, err := writeEventDeclarationDigest(
		request,
		eventPayloadJSON,
		serverToolUse.CanonicalJSON,
	)
	if err != nil {
		t.Fatalf("digest stored event payload: %v", err)
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 || response.GetDeclaration().GetReceipts()[0].GetDeclarationDigest() != storedPayloadDigest {
		t.Fatalf("receipt digest does not cover the stored payload bytes")
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_event' AND kind = 'assistant'`).Scan(&assistantProjectionCount); err != nil {
		t.Fatalf("read assistant projection count: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT message_id, data_json FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_event' AND kind = 'assistant'`).Scan(&messageID, &messageDataJSON); err != nil {
		t.Fatalf("read assistant projection: %v", err)
	}
	if messageID == "" || messageID == "msg_bridge_event" {
		t.Fatalf("assistant projection message id = %q; want Bridge-minted identity", messageID)
	}
	if streamPosition == 0 || latestPosition != streamPosition || assistantProjectionCount != 1 {
		t.Fatalf("stream/projection = stream %d latest %d assistant %d; want durable stream and assistant projection", streamPosition, latestPosition, assistantProjectionCount)
	}
	for name, data := range map[string]string{
		"event payload":      eventPayloadJSON,
		"event projection":   eventProjectionJSON,
		"message projection": messageDataJSON,
	} {
		if strings.Contains(data, "provider_") || strings.Contains(data, "engine_sandbox_id") {
			t.Fatalf("%s leaked provider metadata: %s", name, data)
		}
	}
	for name, data := range map[string]string{
		"event payload":      eventPayloadJSON,
		"message projection": messageDataJSON,
	} {
		if !strings.Contains(data, `"text":"hello"`) {
			t.Fatalf("%s lost safe semantic content: %s", name, data)
		}
	}
	if eventProjectionJSON != "{}" {
		t.Fatalf("event projection = %s; want no clerk-authored projection metadata", eventProjectionJSON)
	}
	var assistantMessage struct {
		Role   string `json:"role"`
		Origin string `json:"origin"`
		Status string `json:"status"`
		Parts  []struct {
			Type   string `json:"type"`
			Text   string `json:"text"`
			Status string `json:"status"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(messageDataJSON), &assistantMessage); err != nil {
		t.Fatalf("parse assistant RuntimeMessage projection: %v", err)
	}
	if assistantMessage.Role != "assistant" || assistantMessage.Origin != "agent" || assistantMessage.Status != "streaming" ||
		len(assistantMessage.Parts) != 1 || assistantMessage.Parts[0].Type != "text" || assistantMessage.Parts[0].Text != "hello" || assistantMessage.Parts[0].Status != "completed" {
		t.Fatalf("assistant RuntimeMessage projection = %+v; want streaming assistant with completed text member", assistantMessage)
	}

	conflict := proto.Clone(request).(*bridgev1.WriteEventRequest)
	conflict.PayloadJson = `{"type":"agent.message","content":[{"type":"text","text":"different"}]}`
	if _, err := store.WriteEvent(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting WriteEvent err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningTracksDurableMembersAndTargetedSettlement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const sessionID = "sesn_stable_reasoning"
	const threadID = "thr_stable_reasoning"
	const requestID = "mreq_stable_reasoning"
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_stable_reasoning", 1, "pod_stable_reasoning")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, "bind_stable_reasoning", 1, "pod_stable_reasoning")
	start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_stable_start", requestID, "agent_provider_request", 0)
	writeRequests := make(map[string]*bridgev1.WriteEventRequest)

	writeTool := func(writeID, callID, reasoningID, reasoningText string) *bridgev1.WriteEventResponse {
		t.Helper()
		request := &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: requestID,
			EventType: "agent.tool_use", PayloadJson: fmt.Sprintf(`{"type":"agent.tool_use","name":"Read","input":{"path":%q},"evaluated_permission":"allow"}`, callID),
			Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(t, scope, writeID, "agent.tool_use", "streaming",
				bridgeRuntimePartCreateForTest{kind: "reasoning", json: fmt.Sprintf(`{"type":"reasoning","providerPartId":%q,"text":%q,"providerMetadata":{},"truncated":false}`, reasoningID, reasoningText)},
				bridgeRuntimePartCreateForTest{kind: "tool", json: fmt.Sprintf(`{"type":"tool","toolCallId":%q,"toolName":"Read","state":{"status":"running","input":{"value":{"path":%q}}}}`, callID, callID)},
			)},
		}
		response, err := store.WriteEvent(context.Background(), request)
		if err != nil {
			t.Fatalf("WriteEvent %s: %v", callID, err)
		}
		writeRequests[callID] = request
		return response
	}
	toolA := writeTool("rwrite_stable_tool_a", "call_stable_a", "provider_reasoning_a", "R1")
	toolB := writeTool("rwrite_stable_tool_b", "call_stable_b", "provider_reasoning_b", "R2")

	readLedger := func(eventID string) []normalizedStableReasoningPart {
		t.Helper()
		var raw sql.NullString
		if err := admin.QueryRow(`SELECT stable_reasoning_json FROM session_events WHERE workspace_id='default' AND event_id=$1`, eventID).Scan(&raw); err != nil {
			t.Fatalf("read stable reasoning for %s: %v", eventID, err)
		}
		if !raw.Valid {
			return nil
		}
		var parts []normalizedStableReasoningPart
		if err := json.Unmarshal([]byte(raw.String), &parts); err != nil {
			t.Fatalf("decode stable reasoning for %s: %v", eventID, err)
		}
		return parts
	}
	assertLedger := func(eventID string, texts []string, sequences []int32) []normalizedStableReasoningPart {
		t.Helper()
		parts := readLedger(eventID)
		if len(parts) != len(texts) {
			t.Fatalf("stable reasoning for %s = %+v; want texts %v", eventID, parts, texts)
		}
		for i, text := range texts {
			expectedID := stableRuntimeID(
				"reasoning_ledger_part", "default", sessionID, threadID, requestID,
				strconv.FormatInt(int64(sequences[i]), 10),
			)
			if parts[i].Text != text || parts[i].PartSequence != sequences[i] || parts[i].ReasoningPartID != expectedID {
				t.Fatalf("stable reasoning part %d for %s = %+v; want identity %q at deterministic sequence/text", i, eventID, parts[i], expectedID)
			}
		}
		return parts
	}
	toolALedger := assertLedger(toolA.GetEventId(), []string{"R1"}, []int32{0})
	toolBLedger := assertLedger(toolB.GetEventId(), []string{"R1", "R2"}, []int32{0, 2})
	for callID, written := range map[string]*bridgev1.WriteEventResponse{"call_stable_a": toolA, "call_stable_b": toolB} {
		replay, err := store.WriteEvent(context.Background(), writeRequests[callID])
		if err != nil || replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetEventId() != written.GetEventId() {
			t.Fatalf("WriteEvent replay %s = %+v, %v; want duplicate original event", callID, replay, err)
		}
		beforeReplay := toolALedger
		if callID == "call_stable_b" {
			beforeReplay = toolBLedger
		}
		if afterReplay := readLedger(written.GetEventId()); !reflect.DeepEqual(afterReplay, beforeReplay) {
			t.Fatalf("stable reasoning changed across replay for %s: before %+v after %+v", callID, beforeReplay, afterReplay)
		}
	}

	_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_stable_end", ModelRequestId: requestID,
		ModelRequestStartEventId: start.GetEventId(), RequestKind: "agent_provider_request",
		FinishReason: "tool-calls", UsageJson: `{}`,
	})
	if err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	var endEventID string
	if err := admin.QueryRow(`SELECT event_id FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3 AND type='span.model_request_end'`, sessionID, threadID, requestID).Scan(&endEventID); err != nil {
		t.Fatalf("read Request End event: %v", err)
	}
	assertLedger(endEventID, []string{"R1", "R2"}, []int32{0, 2})

	type storedMessage struct {
		Parts []json.RawMessage `json:"parts"`
	}
	readParts := func() []json.RawMessage {
		t.Helper()
		var raw string
		if err := admin.QueryRow(`SELECT data_json FROM session_messages WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3 AND kind='assistant'`, sessionID, threadID, requestID).Scan(&raw); err != nil {
			t.Fatalf("read Assistant message: %v", err)
		}
		var message storedMessage
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatalf("decode Assistant message: %v", err)
		}
		return message.Parts
	}
	before := readParts()
	if len(before) != 4 {
		t.Fatalf("Assistant parts before settlement = %d; want R1/A/R2/B", len(before))
	}
	settle := func(writeID string, target *bridgev1.WriteEventResponse, textValue string) *bridgev1.WriteEventResponse {
		t.Helper()
		response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: requestID,
			EventType: "agent.tool_result", PayloadJson: fmt.Sprintf(`{"type":"agent.tool_result","tool_use_id":%q,"content":[{"type":"text","text":%q}]}`, target.GetEventId(), textValue),
			Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(target.GetEventId(), textValue)},
		})
		if err != nil {
			t.Fatalf("settle %s: %v", target.GetEventId(), err)
		}
		if got := readLedger(response.GetEventId()); got != nil {
			t.Fatalf("Tool Result stable ledger = %+v; want absent", got)
		}
		return response
	}
	settle("rwrite_stable_result_b", toolB, "done B")
	afterB := readParts()
	for _, index := range []int{0, 1, 2} {
		if string(afterB[index]) != string(before[index]) {
			t.Fatalf("settling B changed sibling part %d: before %s after %s", index, before[index], afterB[index])
		}
	}
	if string(afterB[3]) == string(before[3]) {
		t.Fatal("settling B did not change target Tool part")
	}
	settle("rwrite_stable_result_a", toolA, "done A")
	afterA := readParts()
	for _, index := range []int{0, 2, 3} {
		if string(afterA[index]) != string(afterB[index]) {
			t.Fatalf("settling A changed sibling part %d: before %s after %s", index, afterB[index], afterA[index])
		}
	}
	if string(afterA[1]) == string(afterB[1]) {
		t.Fatal("settling A did not change target Tool part")
	}
	for _, target := range []*bridgev1.WriteEventResponse{toolA, toolB} {
		var results int
		if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND type='agent.tool_result' AND payload_json::jsonb->>'tool_use_id'=$3`, sessionID, threadID, target.GetEventId()).Scan(&results); err != nil {
			t.Fatalf("count Tool Results: %v", err)
		}
		if results != 1 {
			t.Fatalf("Tool Result count for %s = %d; want one", target.GetEventId(), results)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningEnforcesCumulativeBoundsAtomically(t *testing.T) {
	tests := []struct {
		name      string
		partCount int
		textBytes int
		wantCode  codes.Code
	}{
		{name: "part count at limit", partCount: MaxStableReasoningPartsPerRequest, wantCode: codes.OK},
		{name: "part count over limit", partCount: MaxStableReasoningPartsPerRequest + 1, wantCode: codes.InvalidArgument},
		{name: "aggregate bytes at limit", partCount: 1, textBytes: MaxStableReasoningBytesPerRequest - len(`{}`), wantCode: codes.OK},
		{name: "aggregate bytes over limit", partCount: 1, textBytes: MaxStableReasoningBytesPerRequest - len(`{}`) + 1, wantCode: codes.InvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID, threadID := "sesn_reasoning_bound_"+suffix, "thr_reasoning_bound_"+suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_reasoning_bound_"+suffix, 1, "pod_reasoning_bound_"+suffix)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_reasoning_bound_"+suffix, 1, "pod_reasoning_bound_"+suffix)
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_reasoning_bound_start_"+suffix, "mreq_reasoning_bound_"+suffix, "agent_provider_request", 0)
			parts := make([]bridgeRuntimePartCreateForTest, 0, test.partCount+1)
			for i := 0; i < test.partCount; i++ {
				textValue := fmt.Sprintf("reasoning-%d", i)
				if test.textBytes > 0 {
					textValue = strings.Repeat("x", test.textBytes)
				}
				encodedText, err := json.Marshal(textValue)
				if err != nil {
					t.Fatal(err)
				}
				parts = append(parts, bridgeRuntimePartCreateForTest{kind: "reasoning", json: fmt.Sprintf(`{"type":"reasoning","providerPartId":"provider-%d","text":%s,"providerMetadata":{},"truncated":false}`, i, encodedText)})
			}
			parts = append(parts, bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"anchor","truncated":false,"status":"completed"}`})
			request := &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reasoning_bound_member_" + suffix,
				ModelRequestId: "mreq_reasoning_bound_" + suffix, EventType: "agent.message",
				PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"anchor"}]}`,
				Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(t, scope, "", "", "", parts...)},
			}
			var beforeEvents, beforeMessages, beforeOperations int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&beforeEvents, &beforeMessages, &beforeOperations); err != nil {
				t.Fatalf("count state before append: %v", err)
			}
			response, err := store.WriteEvent(context.Background(), request)
			if status.Code(err) != test.wantCode {
				t.Fatalf("WriteEvent error = %v; want %v", err, test.wantCode)
			}
			if test.wantCode == codes.OK {
				var ledger string
				if err := admin.QueryRow(`SELECT stable_reasoning_json FROM session_events WHERE workspace_id='default' AND event_id=$1`, response.GetEventId()).Scan(&ledger); err != nil {
					t.Fatalf("read accepted stable ledger: %v", err)
				}
				var persisted []normalizedStableReasoningPart
				if err := json.Unmarshal([]byte(ledger), &persisted); err != nil || len(persisted) != test.partCount {
					t.Fatalf("accepted stable ledger = %d parts, %v; want %d", len(persisted), err, test.partCount)
				}
				return
			}
			var afterEvents, afterMessages, afterOperations int
			if err := admin.QueryRow(`SELECT (SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).Scan(&afterEvents, &afterMessages, &afterOperations); err != nil {
				t.Fatalf("count state after rejected append: %v", err)
			}
			if afterEvents != beforeEvents || afterMessages != beforeMessages || afterOperations != beforeOperations {
				t.Fatalf("over-limit append mutated events/messages/operations: before %d/%d/%d after %d/%d/%d", beforeEvents, beforeMessages, beforeOperations, afterEvents, afterMessages, afterOperations)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreFailedAndRescheduledEndsDoNotPublishStableReasoning(t *testing.T) {
	for _, rescheduled := range []bool{false, true} {
		name := map[bool]string{false: "failed", true: "rescheduled"}[rescheduled]
		t.Run(name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID, threadID, requestID := "sesn_reasoning_"+name, "thr_reasoning_"+name, "mreq_reasoning_"+name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_reasoning_"+name, 1, "pod_reasoning_"+name)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			now := time.Date(2026, time.August, 9, 0, 0, 0, 0, time.UTC)
			store.Clock = func() time.Time { return now }
			store.ProviderRescheduleBudget = 1
			scope := bridgeAPIScope(sessionID, threadID, "bind_reasoning_"+name, 1, "pod_reasoning_"+name)
			start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_reasoning_start_"+name, requestID, "agent_provider_request", 0)
			member, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reasoning_member_" + name, ModelRequestId: requestID,
				EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
				Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(t, scope, "", "", "",
					bridgeRuntimePartCreateForTest{kind: "reasoning", json: `{"type":"reasoning","providerPartId":"provider-reasoning","text":"anchored","providerMetadata":{},"truncated":false}`},
					bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call-reasoning-end","toolName":"Read","state":{"status":"running","input":{"value":{}}}}`},
				)},
			})
			if err != nil {
				t.Fatalf("WriteEvent anchored prefix: %v", err)
			}
			var anchoredLedger sql.NullString
			if err := admin.QueryRow(`SELECT stable_reasoning_json FROM session_events WHERE workspace_id='default' AND event_id=$1`, member.GetEventId()).Scan(&anchoredLedger); err != nil || !anchoredLedger.Valid {
				t.Fatalf("anchored member ledger = %v, %v; want present", anchoredLedger, err)
			}
			endRequest := &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reasoning_end_" + name, ModelRequestId: requestID,
				ModelRequestStartEventId: start.GetEventId(), RequestKind: "agent_provider_request",
				FinishReason: "error", IsError: true, ErrorKind: "provider_error", UsageJson: `{}`,
			}
			if rescheduled {
				endRequest.Reschedule = &bridgev1.RequestEndReschedule{Attempt: 1, Deadline: now.Add(time.Minute).Format(time.RFC3339Nano)}
			}
			if _, err := store.WriteRequestEnd(context.Background(), endRequest); err != nil {
				t.Fatalf("WriteRequestEnd: %v", err)
			}
			var endLedger sql.NullString
			if err := admin.QueryRow(`SELECT stable_reasoning_json FROM session_events WHERE workspace_id='default' AND session_id=$1 AND session_thread_id=$2 AND model_request_id=$3 AND type='span.model_request_end'`, sessionID, threadID, requestID).Scan(&endLedger); err != nil {
				t.Fatalf("read Request End stable ledger: %v", err)
			}
			if endLedger.Valid {
				t.Fatalf("%s Request End stable ledger = %q; want absent", name, endLedger.String)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsAssistantMembersOutsideOpenRequestWithoutMutation(t *testing.T) {
	for _, membership := range []string{"missing_start", "sealed"} {
		for _, eventType := range []string{"agent.message", "agent.tool_use", "agent.mcp_tool_use"} {
			t.Run(membership+"/"+eventType, func(t *testing.T) {
				runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
				suffix := strings.NewReplacer(".", "_", "/", "_").Replace(membership + "_" + eventType)
				sessionID := "sesn_assistant_membership_" + suffix
				threadID := "thr_assistant_membership_" + suffix
				modelRequestID := "mreq_assistant_membership_" + suffix
				bindingID := "bind_assistant_membership_" + suffix
				podUID := "pod_assistant_membership_" + suffix
				seedBridgeAPISession(t, admin, "default", sessionID, threadID)
				seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
				store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
				scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
				if membership == "sealed" {
					start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_membership_start_"+suffix, modelRequestID, "agent_provider_request", 0)
					if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
						Scope: scope, RuntimeWriteId: "rwrite_membership_end_" + suffix,
						ModelRequestId: modelRequestID, ModelRequestStartEventId: start.GetEventId(),
						RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
					}); err != nil {
						t.Fatalf("seal model request: %v", err)
					}
				}

				request := &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: "rwrite_membership_member_" + suffix,
					ModelRequestId: modelRequestID, EventType: eventType,
				}
				switch eventType {
				case "agent.message":
					request.PayloadJson = `{"type":"agent.message","content":[{"type":"text","text":"late"}]}`
					request.Declaration = &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
						t, scope, request.GetRuntimeWriteId(), eventType, "completed",
						bridgeRuntimePartCreateForTest{kind: "text", json: `{"type":"text","text":"late","truncated":false,"status":"completed"}`},
					)}
				case "agent.tool_use":
					request.PayloadJson = `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`
					request.Declaration = &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
						t, scope, request.GetRuntimeWriteId(), eventType, "streaming",
						bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_membership","toolName":"Read","state":{"status":"running","input":{"value":{}}}}`},
					)}
				case "agent.mcp_tool_use":
					request.PayloadJson = `{"type":"agent.mcp_tool_use","name":"search","input":{},"mcp_server_name":"github","evaluated_permission":"allow"}`
					request.Declaration = &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
						t, scope, request.GetRuntimeWriteId(), eventType, "streaming",
						bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_mcp_membership","toolName":"search","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{}}}}`},
					)}
				}

				var beforeEvents, beforeMessages, beforeOperations int
				if err := admin.QueryRow(`SELECT
					(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
					(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
					(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).
					Scan(&beforeEvents, &beforeMessages, &beforeOperations); err != nil {
					t.Fatalf("count state before rejected append: %v", err)
				}
				if _, err := store.WriteEvent(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("WriteEvent outside open request err = %v; want FailedPrecondition", err)
				}
				var afterEvents, afterMessages, afterOperations int
				if err := admin.QueryRow(`SELECT
					(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1),
					(SELECT count(*) FROM session_messages WHERE workspace_id='default' AND session_id=$1),
					(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1)`, sessionID).
					Scan(&afterEvents, &afterMessages, &afterOperations); err != nil {
					t.Fatalf("count state after rejected append: %v", err)
				}
				if afterEvents != beforeEvents || afterMessages != beforeMessages || afterOperations != beforeOperations {
					t.Fatalf("rejected append mutated events/messages/operations: before %d/%d/%d after %d/%d/%d",
						beforeEvents, beforeMessages, beforeOperations, afterEvents, afterMessages, afterOperations)
				}
			})
		}
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventStampsPrivateRequestStartBoundary(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_request_start_stamp", "thr_request_start_stamp")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_request_start_stamp", "bind_request_start_stamp", 1, "pod_request_start_stamp")
	seedBridgeAPIRuntimeInput(t, admin, "default", "sesn_request_start_stamp", "thr_request_start_stamp", "rin_request_input", "bind_request_start_stamp", "pod_request_start_stamp", "evt_request_input")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_request_start_stamp", "thr_request_start_stamp", "bind_request_start_stamp", 1, "pod_request_start_stamp")
	if _, err := store.CommitInputs(context.Background(), &bridgev1.CommitInputsRequest{
		Scope: scope, RuntimeInputId: "rin_request_input", InputKind: "messages",
		EventIds: []string{"evt_request_input"}, SequenceFrom: 1, SequenceTo: 1,
		MessageCreates: []*bridgev1.RuntimeMessageCreate{bridgeUserInputCreateForTest(
			"default", "sesn_request_start_stamp", "thr_request_start_stamp",
			"rin_request_input", "evt_request_input", "hello",
		)},
	}); err != nil {
		t.Fatalf("CommitInputs: %v", err)
	}
	request := &bridgev1.WriteEventRequest{
		Scope:                         scope,
		RuntimeWriteId:                "rwrite_request_start_stamp",
		ModelRequestId:                "mreq_request_start_stamp",
		EventType:                     "span.model_request_start",
		PayloadJson:                   `{"type":"span.model_request_start","model_request_id":"mreq_request_start_stamp"}`,
		ContextThroughMessageSequence: func() *int64 { value := int64(1); return &value }(),
		RequestKind:                   "agent_provider_request",
	}
	response, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent request start: %v", err)
	}
	stamp := response.GetDeclaration().GetReceipts()[0].GetRequestStart()
	if stamp.GetRequestKind() != "agent_provider_request" || stamp.GetContextThroughMessageSequence() != 1 {
		t.Fatalf("request start stamp = %+v; want agent request through message 1", stamp)
	}
	var payloadJSON, projectionJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json, projection_json FROM session_events WHERE workspace_id = 'default' AND event_id = $1`,
		response.GetEventId(),
	).Scan(&payloadJSON, &projectionJSON); err != nil {
		t.Fatalf("read request start projection: %v", err)
	}
	if payloadJSON != `{"model_request_id":"mreq_request_start_stamp","type":"span.model_request_start"}` {
		t.Fatalf("public request-start payload changed: %s", payloadJSON)
	}
	if projectionJSON != `{"context_through_message_sequence":1,"request_kind":"agent_provider_request"}` {
		t.Fatalf("private request-start projection = %s", projectionJSON)
	}
	duplicateStart := proto.Clone(request).(*bridgev1.WriteEventRequest)
	duplicateStart.RuntimeWriteId = "rwrite_request_start_duplicate_identity"
	if _, err := store.WriteEvent(context.Background(), duplicateStart); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second Request Start for one model request err = %v; want AlreadyExists", err)
	}

	missing := proto.Clone(request).(*bridgev1.WriteEventRequest)
	missing.RuntimeWriteId = "rwrite_request_start_missing"
	missing.ContextThroughMessageSequence = nil
	if _, err := store.WriteEvent(context.Background(), missing); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing request-start boundary err = %v; want InvalidArgument", err)
	}

	for name, mutate := range map[string]func(*bridgev1.WriteEventRequest){
		"stale boundary": func(candidate *bridgev1.WriteEventRequest) {
			candidate.ContextThroughMessageSequence = bridgeAPIInt64(0)
		},
		"future boundary": func(candidate *bridgev1.WriteEventRequest) {
			candidate.ContextThroughMessageSequence = bridgeAPIInt64(2)
		},
		"unknown request kind": func(candidate *bridgev1.WriteEventRequest) {
			candidate.RequestKind = "invented_request"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(request).(*bridgev1.WriteEventRequest)
			candidate.RuntimeWriteId = "rwrite_request_start_invalid_" + strings.ReplaceAll(name, " ", "_")
			candidate.ModelRequestId = "mreq_request_start_invalid_" + strings.ReplaceAll(name, " ", "_")
			candidate.PayloadJson = `{"type":"span.model_request_start","model_request_id":"` + candidate.ModelRequestId + `"}`
			mutate(candidate)
			if _, err := store.WriteEvent(context.Background(), candidate); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid request-start metadata err = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsOrphanAndDuplicateToolResults(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_tool_result_membership", "thr_tool_result_membership")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_tool_result_membership", "bind_tool_result_membership", 1, "pod_tool_result_membership")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_tool_result_membership", "thr_tool_result_membership", "bind_tool_result_membership", 1, "pod_tool_result_membership")

	seedBridgeAPIRequestStart(t, store, scope, "rwrite_tool_result_membership_start", "mreq_tool_result_membership", "agent_provider_request", 0)
	orphan := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_orphan_tool_result", ModelRequestId: "mreq_tool_result_membership",
		EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_id":"sevt_missing_tool_use","content":[{"type":"text","text":"done"}],"is_error":false}`,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest("sevt_missing_tool_use", "done")},
	}
	if _, err := store.WriteEvent(context.Background(), orphan); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("orphan Tool Result err = %v; want FailedPrecondition", err)
	}

	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_tool_result_membership_use", ModelRequestId: "mreq_tool_result_membership",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_tool_result_membership_use", "agent.tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_membership","toolName":"Read","state":{"status":"running","input":{"value":{}}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent Tool Use: %v", err)
	}
	resultRequest := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_tool_result_membership_first", ModelRequestId: "mreq_tool_result_membership",
		EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"done"}],"is_error":false}`,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "done")},
	}
	if _, err := store.WriteEvent(context.Background(), resultRequest); err != nil {
		t.Fatalf("WriteEvent first Tool Result: %v", err)
	}
	duplicateResult := proto.Clone(resultRequest).(*bridgev1.WriteEventRequest)
	duplicateResult.RuntimeWriteId = "rwrite_tool_result_membership_second"
	if _, err := store.WriteEvent(context.Background(), duplicateResult); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second Tool Result err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsToolUseAfterRequestEnd(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_tool_use_after_end"
		threadID       = "thr_tool_use_after_end"
		modelRequestID = "mreq_tool_use_after_end"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_tool_use_after_end", 1, "pod_tool_use_after_end")
	scope := bridgeAPIScope(sessionID, threadID, "bind_tool_use_after_end", 1, "pod_tool_use_after_end")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	boundary := int64(0)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_tool_use_after_end_start", ModelRequestId: modelRequestID,
		EventType: "span.model_request_start", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
		ContextThroughMessageSequence: &boundary, RequestKind: "agent_provider_request",
	})
	if err != nil {
		t.Fatalf("write request start: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_tool_use_after_end_close", ModelRequestId: modelRequestID,
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "tool_calls", UsageJson: `{}`,
		RequestKind: "agent_provider_request",
	}); err != nil {
		t.Fatalf("write request end: %v", err)
	}
	request := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_tool_use_after_end", ModelRequestId: modelRequestID,
		EventType:   "agent.tool_use",
		PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_tool_use_after_end", "agent.tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_after_end","toolName":"Read","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`},
		)},
	}
	if _, err := store.WriteEvent(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("post-seal tool use err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventSettlesWebUsageExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_web_usage", "thr_bridge_web_usage")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_web_usage", "bind_bridge_web_usage", 1, "pod_uid_web_usage")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE sessions SET usage_json = '{"request_count":4}' WHERE workspace_id = 'default' AND id = 'sesn_bridge_web_usage'`); err != nil {
		t.Fatalf("seed request count: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_web_usage", "thr_bridge_web_usage", "bind_bridge_web_usage", 1, "pod_uid_web_usage")
	const modelRequestID = "mreq_bridge_web_usage"
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_web_start", modelRequestID, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_use",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"web","input":{"search_query":[{"q":"tetral"}]},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_web","toolName":"web","state":{"status":"running","input":{"value":{"search_query":[{"q":"tetral"}]},"preview":"{\"search_query\":[{\"q\":\"tetral\"}]}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent web tool use: %v", err)
	}
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_result",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_result",
		PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"web result"}]}`,
		Declaration:    &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "web result")},
		ServerToolUse: &bridgev1.ServerToolUseUsage{
			WebSearchRequests: 32,
			WebFetchRequests:  8,
		},
	}
	response, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent web result: %v", err)
	}
	replay, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent web result replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE || replay.GetEventId() != response.GetEventId() {
		t.Fatalf("replay = %+v; want duplicate same event", replay)
	}

	var usageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_web_usage'`).Scan(&usageJSON); err != nil {
		t.Fatalf("read session usage: %v", err)
	}
	for path, want := range map[string]int64{
		"web_search_requests":                 32,
		"web_fetch_requests":                  8,
		"server_tool_use.web_search_requests": 32,
		"server_tool_use.web_fetch_requests":  8,
	} {
		if got := testJSONPathInt(t, usageJSON, path); got != want {
			t.Fatalf("session usage %s = %d; want %d", path, got, want)
		}
	}
	if got := testJSONPathInt(t, usageJSON, "request_count"); got != 4 {
		t.Fatalf("request_count = %d; want unchanged 4", got)
	}

	conflict := proto.Clone(request).(*bridgev1.WriteEventRequest)
	conflict.ServerToolUse.WebFetchRequests = 2
	if _, err := store.WriteEvent(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteEvent divergent web usage err = %v; want AlreadyExists", err)
	}
	var unchanged string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_web_usage'`).Scan(&unchanged); err != nil {
		t.Fatalf("read unchanged session usage: %v", err)
	}
	if unchanged != usageJSON {
		t.Fatalf("usage changed after divergent replay: before=%s after=%s", usageJSON, unchanged)
	}
}

func TestPostgreSQLBridgeAPIStoreConcurrentWebUsageReplayReturnsOneCommitAndOneIncrement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_web_usage_concurrent", "thr_bridge_web_usage_concurrent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_web_usage_concurrent", "bind_bridge_web_usage_concurrent", 1, "pod_uid_web_usage_concurrent")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_web_usage_concurrent", "thr_bridge_web_usage_concurrent", "bind_bridge_web_usage_concurrent", 1, "pod_uid_web_usage_concurrent")
	const modelRequestID = "mreq_bridge_web_usage_concurrent"
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_web_start_concurrent", modelRequestID, "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_use_concurrent",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"web","input":{"search_query":[{"q":"tetral"}]},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_use_concurrent",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_web_concurrent","toolName":"web","state":{"status":"running","input":{"value":{"search_query":[{"q":"tetral"}]},"preview":"{\"search_query\":[{\"q\":\"tetral\"}]}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent web tool use: %v", err)
	}
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_result_concurrent",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_result",
		PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"web result"}]}`,
		Declaration:    &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "web result")},
		ServerToolUse:  &bridgev1.ServerToolUseUsage{WebSearchRequests: 1, WebFetchRequests: 1},
	}

	const writers = 6
	responses := make(chan *bridgev1.WriteEventResponse, writers)
	errors := make(chan error, writers)
	var group sync.WaitGroup
	group.Add(writers)
	for range writers {
		go func() {
			defer group.Done()
			response, err := store.WriteEvent(context.Background(), proto.Clone(request).(*bridgev1.WriteEventRequest))
			if err != nil {
				errors <- err
				return
			}
			responses <- response
		}()
	}
	group.Wait()
	close(responses)
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent WriteEvent: %v", err)
	}
	committed := 0
	duplicates := 0
	for response := range responses {
		switch response.GetAck().GetStatus() {
		case bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED:
			committed++
		case bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE:
			duplicates++
		default:
			t.Fatalf("unexpected concurrent ack: %+v", response)
		}
	}
	if committed != 1 || duplicates != writers-1 {
		t.Fatalf("concurrent acks committed/duplicate = %d/%d; want 1/%d", committed, duplicates, writers-1)
	}
	var usageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_web_usage_concurrent'`).Scan(&usageJSON); err != nil {
		t.Fatalf("read session usage: %v", err)
	}
	if testJSONPathInt(t, usageJSON, "server_tool_use.web_search_requests") != 1 || testJSONPathInt(t, usageJSON, "server_tool_use.web_fetch_requests") != 1 {
		t.Fatalf("concurrent usage = %s; want one increment", usageJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsUnauthorizedWebUsageBeforeWrite(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_web_usage_reject", "thr_bridge_web_usage_reject")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_web_usage_reject", "bind_bridge_web_usage_reject", 1, "pod_uid_web_usage_reject")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_web_usage_reject", "thr_bridge_web_usage_reject", "bind_bridge_web_usage_reject", 1, "pod_uid_web_usage_reject")
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_usage_wrong_event",
		EventType:      "agent.message",
		PayloadJson:    `{"type":"agent.message","content":[{"type":"text","text":"hello"}]}`,
		ServerToolUse:  &bridgev1.ServerToolUseUsage{WebSearchRequests: 1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent usage on message err = %v; want InvalidArgument", err)
	}
	_, err = store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_usage_negative",
		EventType:      "agent.tool_result",
		PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"evt_missing"}`,
		ServerToolUse:  &bridgev1.ServerToolUseUsage{WebFetchRequests: -1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent negative usage err = %v; want InvalidArgument", err)
	}
	for _, usage := range []*bridgev1.ServerToolUseUsage{
		{WebSearchRequests: 33},
		{WebFetchRequests: 9},
	} {
		_, err = store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope:          scope,
			RuntimeWriteId: fmt.Sprintf("rwrite_bridge_web_usage_over_%d_%d", usage.GetWebSearchRequests(), usage.GetWebFetchRequests()),
			EventType:      "agent.tool_result",
			PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"evt_missing"}`,
			ServerToolUse:  usage,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("WriteEvent over-limit usage %+v err = %v; want InvalidArgument", usage, err)
		}
	}
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_read_start", "mreq_bridge_read_tool", "agent_provider_request", 0)
	readUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_read_tool_use",
		ModelRequestId: "mreq_bridge_read_tool",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_read_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_read","toolName":"Read","state":{"status":"running","input":{"value":{"file_path":"README.md"},"preview":"{\"file_path\":\"README.md\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent Read tool use: %v", err)
	}
	_, err = store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_read_tool_result_with_web_usage",
		ModelRequestId: "mreq_bridge_read_tool",
		EventType:      "agent.tool_result",
		PayloadJson:    `{"type":"agent.tool_result","tool_use_id":"` + readUse.GetEventId() + `","content":[{"type":"text","text":"done"}]}`,
		Declaration:    &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(readUse.GetEventId(), "done")},
		ServerToolUse:  &bridgev1.ServerToolUseUsage{WebSearchRequests: 1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent usage on non-Web result err = %v; want InvalidArgument", err)
	}
	var count int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_web_usage_reject'
		    AND type = 'agent.tool_use'`).Scan(&count); err != nil {
		t.Fatalf("count rejected events: %v", err)
	}
	if count != 1 {
		t.Fatalf("event count = %d; want only the accepted Read tool-use event", count)
	}
	var rejectedOperations int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_web_usage_reject' AND operation = 'write_event' AND idempotency_key LIKE 'rwrite_bridge_web_usage_over_%'`).Scan(&rejectedOperations); err != nil {
		t.Fatalf("count rejected bridge operations: %v", err)
	}
	if rejectedOperations != 0 {
		t.Fatalf("rejected bridge operations = %d; want 0", rejectedOperations)
	}
	var usageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_web_usage_reject'`).Scan(&usageJSON); err != nil {
		t.Fatalf("read rejected session usage: %v", err)
	}
	if usageJSON != "{}" {
		t.Fatalf("rejected usage changed session usage: %s", usageJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventPublishesThinkingWithoutMessageProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_thinking", "thr_bridge_thinking")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_thinking", "bind_bridge_thinking", 1, "pod_uid_thinking")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_bridge_thinking", "thr_bridge_thinking", "bind_bridge_thinking", 1, "pod_uid_thinking"),
		RuntimeWriteId: "rwrite_bridge_thinking",
		ModelRequestId: "mreq_bridge_thinking",
		EventType:      "agent.thinking",
		PayloadJson:    `{"type":"agent.thinking"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent thinking: %v", err)
	}
	var visibility string
	var sessionVisible bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT visibility, session_visible
		   FROM session_events
		  WHERE workspace_id = 'default' AND event_id = $1`, response.GetEventId()).Scan(&visibility, &sessionVisible); err != nil {
		t.Fatalf("read thinking event: %v", err)
	}
	if visibility != "public" || !sessionVisible {
		t.Fatalf("thinking visibility = %q/%v; want public/true", visibility, sessionVisible)
	}
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_thinking'`).Scan(&messageCount); err != nil {
		t.Fatalf("count thinking message projections: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("thinking message projections = %d; want 0", messageCount)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsCustomToolUseWithoutSideEffects(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_custom_tool", "thr_bridge_custom_tool")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_custom_tool", "bind_bridge_custom_tool", 1, "pod_uid_custom_tool")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_bridge_custom_tool", "thr_bridge_custom_tool", "bind_bridge_custom_tool", 1, "pod_uid_custom_tool"),
		RuntimeWriteId: "rwrite_bridge_custom_tool",
		EventType:      "agent.custom_tool_use",
		PayloadJson:    `{"type":"agent.custom_tool_use","name":"legacy_custom","input":{}}`,
		SessionVisible: true,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent custom tool err = %v; want InvalidArgument", err)
	}
	for name, query := range map[string]string{
		"events":            `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_custom_tool'`,
		"messages":          `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_custom_tool'`,
		"pending approvals": `SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_custom_tool'`,
		"stream changes":    `SELECT count(*) FROM session_event_stream_changes WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_custom_tool'`,
		"bridge operations": `SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_custom_tool'`,
		"queue jobs":        `SELECT count(*) FROM queue_jobs WHERE workspace_id = 'default' AND payload_json LIKE '%sesn_bridge_custom_tool%'`,
	} {
		var count int
		if err := admin.QueryRowContext(context.Background(), query).Scan(&count); err != nil {
			t.Fatalf("read %s count: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d; want 0 after rejected custom tool write", name, count)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsUnknownAndDedicatedBoundaryEvents(t *testing.T) {
	store := &PostgreSQLBridgeAPIStore{}
	for _, eventType := range []string{
		"unknown.runtime_event",
		"span.model_request_end",
		"session.status_idle",
		"session.status_terminated",
		"session.thread_status_terminated",
	} {
		t.Run(eventType, func(t *testing.T) {
			_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				RuntimeWriteId: "rwrite_closed_event_boundary",
				EventType:      eventType,
				PayloadJson:    `{"type":"` + eventType + `"}`,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteEvent(%q) err = %v; want InvalidArgument", eventType, err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventOpensPendingApprovalFromToolUseAsk(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_tool_ask", "thr_bridge_tool_ask")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_tool_ask", "bind_bridge_tool_ask", 1, "pod_uid_tool_ask")

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope("sesn_bridge_tool_ask", "thr_bridge_tool_ask", "bind_bridge_tool_ask", 1, "pod_uid_tool_ask")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_tool_ask_start", "mreq_bridge_tool_ask", "agent_provider_request", 0)
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_tool_ask",
		ModelRequestId: "mreq_bridge_tool_ask",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"dangerous_tool","input":{"path":"README.md"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t,
			scope,
			"rwrite_bridge_tool_ask",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartCreateForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"tool-call-ask","toolName":"dangerous_tool","state":{"status":"running","input":{"value":{"path":"README.md"},"preview":"{\"path\":\"README.md\"}","truncated":false}}}`,
			},
		)},
	}
	response, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent tool_use ask: %v", err)
	}
	replay, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent tool_use ask replay: %v", err)
	}

	var modelToolCallID string
	var toolName string
	var inputJSON string
	var pendingStatus string
	var pendingCount int
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT model_tool_call_id, tool_name, input_json, status
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_ask'
		    AND session_thread_id = 'thr_bridge_tool_ask'
		    AND tool_use_event_id = $1`,
		response.GetEventId()).Scan(&modelToolCallID, &toolName, &inputJSON, &pendingStatus); err != nil {
		t.Fatalf("read generated pending approval: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_ask'`).Scan(&pendingCount); err != nil {
		t.Fatalf("count pending approvals: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_tool_ask'`).Scan(&messageCount); err != nil {
		t.Fatalf("count projected messages: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		replay.GetEventId() != response.GetEventId() ||
		modelToolCallID != "tool-call-ask" || toolName != "dangerous_tool" ||
		inputJSON != `{"path":"README.md"}` || pendingStatus != "pending" ||
		pendingCount != 1 || messageCount != 1 {
		t.Fatalf("pending declaration ack=%s replay=%s event=%s/%s model=%q tool=%q input=%s status=%q pending=%d messages=%d; want ask pending idempotent with one loop-authored context row",
			response.GetAck().GetStatus(), replay.GetAck().GetStatus(), response.GetEventId(), replay.GetEventId(),
			modelToolCallID, toolName, inputJSON, pendingStatus, pendingCount, messageCount)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventToolUseAllowDenyDoNotOpenPendingApproval(t *testing.T) {
	for _, permission := range []string{"allow", "deny"} {
		t.Run(permission, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_tool_" + permission
			threadID := "thr_bridge_tool_" + permission
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_tool_"+permission, 1, "pod_uid_tool_"+permission)

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_tool_"+permission, 1, "pod_uid_tool_"+permission)
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_tool_start_"+permission, "mreq_bridge_tool_"+permission, "agent_provider_request", 0)
			runtimeWriteID := "rwrite_bridge_tool_" + permission
			if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:          scope,
				RuntimeWriteId: runtimeWriteID,
				ModelRequestId: "mreq_bridge_tool_" + permission,
				EventType:      "agent.tool_use",
				PayloadJson:    `{"type":"agent.tool_use","name":"safe_tool","input":{"ok":true},"evaluated_permission":"` + permission + `"}`,
				SessionVisible: true,
				Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
					t,
					scope,
					runtimeWriteID,
					"agent.tool_use",
					"streaming",
					bridgeRuntimePartCreateForTest{
						kind: "tool",
						json: `{"type":"tool","toolCallId":"tool-call-` + permission + `","toolName":"safe_tool","state":{"status":"running","input":{"value":{"ok":true},"preview":"{\"ok\":true}","truncated":false}}}`,
					},
				)},
			}); err != nil {
				t.Fatalf("WriteEvent tool_use %s: %v", permission, err)
			}

			var pendingCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_pending_tool_uses WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&pendingCount); err != nil {
				t.Fatalf("count pending approvals: %v", err)
			}
			if pendingCount != 0 {
				t.Fatalf("pending approvals for %s = %d; want 0", permission, pendingCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventConsumesDurableSandboxExecution(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_sandbox_consume", "thr_bridge_sandbox_consume")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_sandbox_consume", "bind_bridge_sandbox_consume", 1, "pod_uid_bridge_sandbox_consume")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope("sesn_bridge_sandbox_consume", "thr_bridge_sandbox_consume", "bind_bridge_sandbox_consume", 1, "pod_uid_bridge_sandbox_consume")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_sandbox_consume_start", "mreq_bridge_sandbox_consume", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_sandbox_consume_use", ModelRequestId: "mreq_bridge_sandbox_consume",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Write","input":{"file_path":"a.txt","content":"ok"},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_bridge_sandbox_consume_use", "agent.tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_bridge_sandbox_consume","toolName":"Write","state":{"status":"running","input":{"value":{"file_path":"a.txt","content":"ok"},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent sandbox tool use: %v", err)
	}
	const attachmentRef = "att_bridge_sandbox_consume"
	const secondAttachmentRef = "att_bridge_sandbox_consume_second"
	const resultJSON = `{"status":"success","result":{"summary":"created a.txt","attachments":[{"attachment_ref":"` + attachmentRef + `"},{"attachment_ref":"` + secondAttachmentRef + `"}]}}`
	resultDigest := sha256Hex(resultJSON)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			result_digest, created_at, updated_at
		) VALUES ('default', 'sesn_bridge_sandbox_consume', 'thr_bridge_sandbox_consume', $1, 'sandbox_tool',
			$2, 'Write', $3, 'committed', $4, 'call_bridge_sandbox_consume',
			'terminal_unconsumed', 1, $5, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')`,
		toolUse.GetEventId(), sha256Hex(`{"content":"ok","file_path":"a.txt"}`), `{"content":"ok","file_path":"a.txt"}`, resultJSON, resultDigest,
	); err != nil {
		t.Fatalf("seed terminal sandbox execution: %v", err)
	}

	resultRequest := func(runtimeWriteID string, digest *string) *bridgev1.WriteEventRequest {
		return &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: runtimeWriteID, ModelRequestId: "mreq_bridge_sandbox_consume",
			EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_event_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"created a.txt"}]}`,
			SandboxResultDigest: digest,
			Declaration:         &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "created a.txt")},
		}
	}
	assertRejectedResultHasNoSideEffects := func(runtimeWriteID string) {
		t.Helper()
		var resultEvents, bridgeOperations, assistantMessages int
		var messageDataJSON, executionState string
		var storedResult sql.NullString
		if err := admin.QueryRowContext(context.Background(),
			`SELECT
				(SELECT count(*) FROM session_events
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume' AND type='agent.tool_result'),
				(SELECT count(*) FROM session_bridge_operations
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume'
				    AND operation='write_event' AND idempotency_key=$1),
				(SELECT count(*) FROM session_messages
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume' AND kind='assistant'),
				(SELECT data_json FROM session_messages
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume' AND kind='assistant'),
				(SELECT execution_state FROM session_runtime_tool_results
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume' AND tool_use_event_id=$2),
				(SELECT result_json FROM session_runtime_tool_results
				  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
				    AND session_thread_id='thr_bridge_sandbox_consume' AND tool_use_event_id=$2)`,
			runtimeWriteID, toolUse.GetEventId(),
		).Scan(&resultEvents, &bridgeOperations, &assistantMessages, &messageDataJSON, &executionState, &storedResult); err != nil {
			t.Fatalf("read rejected sandbox result side effects for %s: %v", runtimeWriteID, err)
		}
		if resultEvents != 0 || bridgeOperations != 0 || assistantMessages != 1 || executionState != "terminal_unconsumed" ||
			!storedResult.Valid || storedResult.String != resultJSON || !strings.Contains(messageDataJSON, `"status":"streaming"`) ||
			strings.Contains(messageDataJSON, "created a.txt") {
			t.Fatalf("rejected sandbox result %s leaked events=%d operations=%d messages=%d state=%q result=%v projection=%s",
				runtimeWriteID, resultEvents, bridgeOperations, assistantMessages, executionState, storedResult, messageDataJSON)
		}
	}
	missingDigest := resultRequest("rwrite_bridge_sandbox_consume_missing_digest", nil)
	if _, err := store.WriteEvent(context.Background(), missingDigest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent sandbox tool result without digest err = %v; want FailedPrecondition", err)
	}
	assertRejectedResultHasNoSideEffects(missingDigest.GetRuntimeWriteId())
	wrongDigest := strings.Repeat("f", 64)
	mismatchedDigest := resultRequest("rwrite_bridge_sandbox_consume_wrong_digest", &wrongDigest)
	if _, err := store.WriteEvent(context.Background(), mismatchedDigest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent sandbox tool result with wrong digest err = %v; want FailedPrecondition", err)
	}
	assertRejectedResultHasNoSideEffects(mismatchedDigest.GetRuntimeWriteId())
	missingAttachment := resultRequest("rwrite_bridge_sandbox_consume_missing_attachment", &resultDigest)
	if _, err := store.WriteEvent(context.Background(), missingAttachment); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent sandbox tool result with missing staged attachment err = %v; want FailedPrecondition", err)
	}
	assertRejectedResultHasNoSideEffects(missingAttachment.GetRuntimeWriteId())
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id, source_tool_use_event_id,
			blob_pointer, mime, metadata_json, status, expires_at, created_at, updated_at
		) VALUES
			('default', $1, 'sesn_bridge_sandbox_consume', 'thr_bridge_sandbox_consume', $3,
			 'blob/bridge-sandbox-consume', 'image/png', '{}', 'staged', $4, $5, $5),
			('default', $2, 'sesn_bridge_sandbox_consume', 'thr_bridge_sandbox_consume', $3,
			 'blob/bridge-sandbox-consume-second', 'image/png', '{}', 'active', $4, $5, $5)`,
		attachmentRef, secondAttachmentRef, toolUse.GetEventId(), now.Add(time.Minute), now,
	); err != nil {
		t.Fatalf("seed staged-first and non-staged-second sandbox attachments: %v", err)
	}
	activeAttachment := resultRequest("rwrite_bridge_sandbox_consume_active_attachment", &resultDigest)
	if _, err := store.WriteEvent(context.Background(), activeAttachment); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent sandbox tool result with staged-first and active-second attachments err = %v; want FailedPrecondition", err)
	}
	assertRejectedResultHasNoSideEffects(activeAttachment.GetRuntimeWriteId())
	var firstAttachmentStatus string
	var firstAttachmentExpiry time.Time
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, expires_at FROM session_transient_attachments
		  WHERE workspace_id='default' AND attachment_ref=$1`, attachmentRef,
	).Scan(&firstAttachmentStatus, &firstAttachmentExpiry); err != nil {
		t.Fatalf("read staged-first sandbox attachment after rejected multi-attachment result: %v", err)
	}
	if firstAttachmentStatus != "staged" || !firstAttachmentExpiry.Equal(now.Add(time.Minute)) {
		t.Fatalf("staged-first sandbox attachment after rollback = %q/%s; want staged/%s",
			firstAttachmentStatus, firstAttachmentExpiry, now.Add(time.Minute))
	}
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_transient_attachments
		    SET status='staged', expires_at=$2
		  WHERE workspace_id='default' AND attachment_ref IN ($1,$3)`,
		attachmentRef, now.Add(time.Minute), secondAttachmentRef,
	); err != nil {
		t.Fatalf("stage sandbox attachments: %v", err)
	}
	settled, err := store.WriteEvent(context.Background(), resultRequest("rwrite_bridge_sandbox_consume_result", &resultDigest))
	if err != nil {
		t.Fatalf("WriteEvent sandbox tool result: %v", err)
	}
	replayed, err := store.WriteEvent(context.Background(), resultRequest("rwrite_bridge_sandbox_consume_result", &resultDigest))
	if err != nil {
		t.Fatalf("replay WriteEvent sandbox tool result: %v", err)
	}
	if replayed.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay status = %s; want duplicate", replayed.GetAck().GetStatus())
	}
	changedDigest := strings.Repeat("e", 64)
	if _, err := store.WriteEvent(context.Background(), resultRequest("rwrite_bridge_sandbox_consume_result", &changedDigest)); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("replay WriteEvent sandbox tool result with changed digest err = %v; want AlreadyExists", err)
	}
	var state string
	var storedResult sql.NullString
	var storedDigest, terminalEventID, reason string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT execution_state, result_json, result_digest, consumed_by_terminal_event_id, consumption_reason
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_sandbox_consume'
		    AND session_thread_id = 'thr_bridge_sandbox_consume' AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
	).Scan(&state, &storedResult, &storedDigest, &terminalEventID, &reason); err != nil {
		t.Fatalf("read consumed sandbox execution: %v", err)
	}
	if state != "consumed" || storedResult.Valid || storedDigest != resultDigest || terminalEventID != settled.GetEventId() || reason != "conversation_tool_result" {
		t.Fatalf("consumed execution = state %q result %+v digest %q event %q reason %q; want thin conversation receipt", state, storedResult, storedDigest, terminalEventID, reason)
	}
	var activatedAttachmentCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_transient_attachments
		  WHERE workspace_id='default' AND attachment_ref IN ($1,$2)
		    AND status='active' AND expires_at=$3`,
		attachmentRef, secondAttachmentRef, now.Add(defaultTransientAttachmentTTL),
	).Scan(&activatedAttachmentCount); err != nil {
		t.Fatalf("count activated sandbox attachments: %v", err)
	}
	if activatedAttachmentCount != 2 {
		t.Fatalf("activated sandbox attachments = %d; want both active with refreshed TTL", activatedAttachmentCount)
	}
	var committedResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id='default' AND session_id='sesn_bridge_sandbox_consume'
		    AND session_thread_id='thr_bridge_sandbox_consume' AND type='agent.tool_result'`,
	).Scan(&committedResultCount); err != nil {
		t.Fatalf("count committed sandbox result events: %v", err)
	}
	if committedResultCount != 1 {
		t.Fatalf("committed sandbox result events = %d; want exactly one", committedResultCount)
	}
}

func TestPostgreSQLBridgeAPIStoreRejectsMCPHandleOnSandboxToolResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_non_mcp_handle", "thr_bridge_non_mcp_handle")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_non_mcp_handle", "bind_bridge_non_mcp_handle", 1, "pod_bridge_non_mcp_handle")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_non_mcp_handle", "thr_bridge_non_mcp_handle", "bind_bridge_non_mcp_handle", 1, "pod_bridge_non_mcp_handle")
	materializationHandle := "evt_bridge_non_mcp_handle"

	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_non_mcp_handle", ModelRequestId: "mreq_bridge_non_mcp_handle",
		EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_event_id":"evt_bridge_non_mcp_handle","content":[{"type":"text","text":"done"}]}`,
		McpMaterializationHandle: &materializationHandle,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent non-MCP result with MCP handle err = %v; want InvalidArgument", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventConsumesDurableBackgroundResult(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_background_consume", "thr_bridge_background_consume")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_background_consume", "bind_bridge_background_consume", 1, "pod_uid_bridge_background_consume")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_background_consume", "thr_bridge_background_consume", "bind_bridge_background_consume", 1, "pod_uid_bridge_background_consume")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_background_consume_start", "mreq_bridge_background_consume", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_background_consume_use", ModelRequestId: "mreq_bridge_background_consume",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"write_stdin","input":{"task_id":"task_background","chars":"ok"},"evaluated_permission":"allow"}`,
		Declaration: &bridgev1.WriteEventRequest_AssistantPartAppend{AssistantPartAppend: bridgeRuntimeOutputAppendForTest(
			t, scope, "rwrite_bridge_background_consume_use", "agent.tool_use", "streaming",
			bridgeRuntimePartCreateForTest{kind: "tool", json: `{"type":"tool","toolCallId":"call_bridge_background_consume","toolName":"write_stdin","state":{"status":"running","input":{"value":{"task_id":"task_background","chars":"ok"},"preview":"{}","truncated":false}}}`},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent background tool use: %v", err)
	}
	const resultJSON = `{"status":"accepted"}`
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json, result_digest,
		background_operation_kind, background_operation_state, background_request_id,
		background_task_id, background_max_output_tokens, background_write_sequence,
		created_at, updated_at
	) VALUES ('default','sesn_bridge_background_consume','thr_bridge_background_consume',$1,'sandbox_background',
		'hash_background_consume','write_stdin','{}','committed',$2,$3,
		'stdin','terminal','request_background_consume','task_background',0,1,
		'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`,
		toolUse.GetEventId(), resultJSON, sha256Hex(resultJSON)); err != nil {
		t.Fatalf("seed terminal background result: %v", err)
	}

	resultRequest := &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_background_consume_result", ModelRequestId: "mreq_bridge_background_consume",
		EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_event_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"accepted"}]}`,
		Declaration: &bridgev1.WriteEventRequest_ToolSettlement{ToolSettlement: bridgeCompletedToolSettlementForTest(toolUse.GetEventId(), "accepted")},
	}
	backgroundDigest := sha256Hex(resultJSON)
	withDigest := proto.Clone(resultRequest).(*bridgev1.WriteEventRequest)
	withDigest.SandboxResultDigest = &backgroundDigest
	if _, err := store.WriteEvent(context.Background(), withDigest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent background result with sandbox digest err = %v; want FailedPrecondition", err)
	}
	settled, err := store.WriteEvent(context.Background(), resultRequest)
	if err != nil {
		t.Fatalf("WriteEvent background tool result: %v", err)
	}
	var storedResult sql.NullString
	var digest, terminalEventID, reason string
	if err := admin.QueryRowContext(context.Background(), `SELECT result_json, result_digest, consumed_by_terminal_event_id, consumption_reason
		FROM session_runtime_tool_results
		WHERE workspace_id='default' AND session_id='sesn_bridge_background_consume'
		  AND session_thread_id='thr_bridge_background_consume' AND tool_use_event_id=$1`, toolUse.GetEventId()).Scan(
		&storedResult, &digest, &terminalEventID, &reason,
	); err != nil {
		t.Fatalf("read consumed background result: %v", err)
	}
	if storedResult.Valid || digest != sha256Hex(resultJSON) || terminalEventID != settled.GetEventId() || reason != "conversation_tool_result" {
		t.Fatalf("consumed background result = result %+v digest %q event %q reason %q", storedResult, digest, terminalEventID, reason)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventForInternalReviewerStaysInternal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reviewer_event", "thr_bridge_reviewer_parent")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reviewer_event", "bind_bridge_reviewer_event", 1, "pod_uid_reviewer_event")
	seedBridgeAPIInternalReviewerThread(t, admin, "default", "sesn_bridge_reviewer_event", "thr_bridge_reviewer_parent", "thr_bridge_reviewer")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	for _, test := range []struct {
		name           string
		runtimeWriteID string
		eventType      string
		payloadJSON    string
	}{
		{
			name:           "decision",
			runtimeWriteID: "rwrite_bridge_reviewer_decision",
			eventType:      "approval_review.decision",
			payloadJSON:    `{"type":"approval_review.decision","review_id":"arvw_bridge","parent_thread_id":"thr_bridge_reviewer_parent","target_model_tool_call_id":"tool_call_bridge","target_tool_name":"Write","risk_level":"low","user_authorization":"high","outcome":"allow","rationale":"safe"}`,
		},
		{
			name:           "failure",
			runtimeWriteID: "rwrite_bridge_reviewer_failure",
			eventType:      "approval_review.failure",
			payloadJSON:    `{"type":"approval_review.failure","review_id":"arvw_bridge","parent_thread_id":"thr_bridge_reviewer_parent","target_model_tool_call_id":"tool_call_bridge","target_tool_name":"Write","failure_kind":"parse_failure","message":"approval reviewer decision is not JSON"}`,
		},
	} {
		response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope:          bridgeAPIScope("sesn_bridge_reviewer_event", "thr_bridge_reviewer", "bind_bridge_reviewer_event", 1, "pod_uid_reviewer_event"),
			RuntimeWriteId: test.runtimeWriteID,
			EventType:      test.eventType,
			PayloadJson:    test.payloadJSON,
			SessionVisible: true,
		})
		if err != nil {
			t.Fatalf("WriteEvent reviewer %s: %v", test.name, err)
		}

		var eventVisibility string
		var eventSessionVisible bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT visibility, session_visible
			   FROM session_events
			  WHERE workspace_id = 'default'
			    AND event_id = $1`,
			response.GetEventId(),
		).Scan(&eventVisibility, &eventSessionVisible); err != nil {
			t.Fatalf("read reviewer event projection %s: %v", test.name, err)
		}
		var changeVisibility string
		var changeSessionVisible bool
		if err := admin.QueryRowContext(context.Background(),
			`SELECT visibility, session_visible
			   FROM session_event_stream_changes
			  WHERE workspace_id = 'default'
			    AND event_id = $1`,
			response.GetEventId(),
		).Scan(&changeVisibility, &changeSessionVisible); err != nil {
			t.Fatalf("read reviewer stream projection %s: %v", test.name, err)
		}
		if eventVisibility != "internal" || eventSessionVisible || changeVisibility != "internal" || changeSessionVisible {
			t.Fatalf("reviewer projection %s event=%s/%v change=%s/%v; want internal/false", test.name, eventVisibility, eventSessionVisible, changeVisibility, changeSessionVisible)
		}
	}

	contextResponse, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope:          bridgeAPIScope("sesn_bridge_reviewer_event", "thr_bridge_reviewer", "bind_bridge_reviewer_event", 1, "pod_uid_reviewer_event"),
		RuntimeInputId: "rin_bridge_reviewer_load",
	})
	if err != nil {
		t.Fatalf("LoadContext reviewer failure record: %v", err)
	}
	var payload bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(contextResponse.GetContextJson()), &payload); err != nil {
		t.Fatalf("parse LoadContext reviewer failure record: %v", err)
	}
	if len(payload.Messages) != 0 {
		t.Fatalf("LoadContext reviewer messages = %s; want failure record not rehydrated", contextResponse.GetContextJson())
	}
}

func TestUserMessageProjectionKeepsFileMediaOutOfTextParts(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	scope := &bridgev1.RuntimeScope{SessionId: "sesn_media_projection"}
	for _, test := range []struct {
		name      string
		payload   string
		wantParts int
	}{
		{
			name:      "media only",
			payload:   `{"content":[{"type":"image","source":{"type":"file","file_id":"file_image"}},{"type":"document","source":{"type":"file","file_id":"file_document"}}]}`,
			wantParts: 0,
		},
		{
			name:      "text and media",
			payload:   `{"content":[{"type":"text","text":"inspect these"},{"type":"image","source":{"type":"file","file_id":"file_image"}}]}`,
			wantParts: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataJSON, err := userMessageDataJSON(scope, "msg_media_projection", 1, test.payload, now)
			if err != nil {
				t.Fatalf("userMessageDataJSON: %v", err)
			}
			var message struct {
				Parts []json.RawMessage `json:"parts"`
			}
			if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
				t.Fatalf("parse projected message: %v", err)
			}
			if len(message.Parts) != test.wantParts {
				t.Fatalf("projected parts = %s; want %d text parts", dataJSON, test.wantParts)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventMainStatusRunningPersistsRuntimeStatus(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_running_status", "thr_bridge_running_status")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_running_status", "bind_bridge_running_status", 2, "pod_uid_running_status")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, status_event_id, idle_since, cleanup_after,
			cleanup_enqueued_at, cleanup_job_id, binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_running_status', 'idle', 'evt_old_idle',
			'2026-01-01T00:00:00Z', '2026-01-01T00:10:00Z',
			'2026-01-01T00:11:00Z', 'qjob_old_cleanup',
			'bind_old_running_status', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
		)`); err != nil {
		t.Fatalf("seed idle runtime status: %v", err)
	}
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_running_status", "thr_bridge_running_status", "bind_bridge_running_status", 2, "pod_uid_running_status")

	running, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_running_status",
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent running: %v", err)
	}
	replay, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_running_status",
		EventType:      "session.status_running",
		PayloadJson:    `{"type":"session.status_running"}`,
		SessionVisible: true,
	})
	if err != nil {
		t.Fatalf("WriteEvent running replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		replay.GetEventId() != running.GetEventId() ||
		replay.GetSequence() != running.GetSequence() {
		t.Fatalf("running replay = %+v; want duplicate for first event", replay)
	}
	var sessionStatus, threadStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT s.status, t.status
		   FROM sessions s
		   JOIN session_threads t
		     ON t.workspace_id = s.workspace_id
		    AND t.session_id = s.id
		  WHERE s.workspace_id = 'default'
		    AND s.id = 'sesn_bridge_running_status'
		    AND t.id = 'thr_bridge_running_status'`).Scan(&sessionStatus, &threadStatus); err != nil {
		t.Fatalf("read public running status: %v", err)
	}
	if sessionStatus != "idle" || threadStatus != "running" {
		t.Fatalf("raw status = session %q thread %q; want idle/running with session_runtime_status as session source of truth", sessionStatus, threadStatus)
	}
	var runtimeStatus string
	var statusEventID string
	var idleSince, cleanupAfter, cleanupEnqueuedAt, cleanupClaimedAt, cleanupJobID sql.NullString
	var bindingID string
	var bindingGeneration int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, status_event_id, idle_since::text, cleanup_after::text,
		        cleanup_enqueued_at::text, cleanup_claimed_at::text, cleanup_job_id,
		        binding_id, binding_generation
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_running_status'`).Scan(
		&runtimeStatus,
		&statusEventID,
		&idleSince,
		&cleanupAfter,
		&cleanupEnqueuedAt,
		&cleanupClaimedAt,
		&cleanupJobID,
		&bindingID,
		&bindingGeneration,
	); err != nil {
		t.Fatalf("read runtime status: %v", err)
	}
	if runtimeStatus != "running" || statusEventID != running.GetEventId() {
		t.Fatalf("runtime status/event = %q/%q; want running/%q", runtimeStatus, statusEventID, running.GetEventId())
	}
	if idleSince.Valid || cleanupAfter.Valid || cleanupEnqueuedAt.Valid || cleanupClaimedAt.Valid || cleanupJobID.Valid {
		t.Fatalf("running status left idle/cleanup markers: idle=%v cleanupAfter=%v enqueued=%v claimed=%v job=%v", idleSince, cleanupAfter, cleanupEnqueuedAt, cleanupClaimedAt, cleanupJobID)
	}
	if bindingID != "bind_bridge_running_status" || bindingGeneration != 2 {
		t.Fatalf("runtime binding = %q/%d; want current binding", bindingID, bindingGeneration)
	}
	var runningEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_running_status'
		    AND type = 'session.status_running'`).Scan(&runningEventCount); err != nil {
		t.Fatalf("count running events: %v", err)
	}
	if runningEventCount != 1 {
		t.Fatalf("running event count = %d; want idempotent single event", runningEventCount)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventRejectsDirectRescheduledStatus(t *testing.T) {
	store := NewPostgreSQLBridgeAPIStore(nil)
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		RuntimeWriteId: "rwrite_bridge_rescheduled_status",
		EventType:      "session.status_rescheduled",
		PayloadJson:    `{"type":"session.status_rescheduled"}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent direct rescheduled err = %v; want InvalidArgument", err)
	}
}
