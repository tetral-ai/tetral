package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

func TestRuntimeToolProjectionSelectsTheToolNamedByTheCurrentEvent(t *testing.T) {
	drafts := []*bridgev1.RuntimeMessageDraft{{
		Parts: []*bridgev1.RuntimePartDraft{
			{
				RuntimeLocalPartId: "local_tool_1",
				PartKind:           "tool",
				Ordinal:            0,
				PartJson:           `{"type":"tool","toolCallId":"call_1","toolName":"Read","toolUseEventId":"sevt_tool_1","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"path":"one"}}}}`,
			},
			{
				RuntimeLocalPartId: "local_tool_2",
				PartKind:           "tool",
				Ordinal:            1,
				PartJson:           `{"type":"tool","toolCallId":"call_2","toolName":"Write","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"path":"two"}}}}`,
			},
		},
	}}
	stamps := []*bridgev1.DurableMessageStamp{{
		MessageId: "msg_1",
		Parts: []*bridgev1.DurablePartStamp{
			{
				RuntimeLocalPartId: "local_tool_1",
				PartId:             "part_1",
				MessageId:          "msg_1",
				PartSequence:       0,
				Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_UPDATED,
			},
			{
				RuntimeLocalPartId: "local_tool_2",
				PartId:             "part_2",
				MessageId:          "msg_1",
				PartSequence:       1,
				Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
			},
		},
	}}

	projection, err := runtimeToolProjectionFromDeclaration(
		"sevt_tool_2",
		"agent.tool_use",
		`{"type":"agent.tool_use","name":"Write","input":{"path":"two"},"evaluated_permission":"allow"}`,
		drafts,
		stamps,
	)
	if err != nil {
		t.Fatalf("select current tool projection: %v", err)
	}
	if projection.ModelToolCallID != "call_2" || projection.ToolName != "Write" || projection.PartID != "part_2" {
		t.Fatalf("current tool projection = %#v; want call_2/Write/part_2", projection)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteEventPersistsStreamProjectionAndIdempotency(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_event", "thr_bridge_event")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_event", "bind_bridge_event", 1, "pod_uid_event")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_event", "thr_bridge_event", "bind_bridge_event", 1, "pod_uid_event")
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_event",
		ModelRequestId: "mreq_bridge_event",
		EventType:      "agent.message",
		PayloadJson:    `{"type":"agent.message","provider_session_id":"sess_provider","content":[{"type":"text","text":"hello","provider_metadata":{"raw":"secret"}}]}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_event",
			"agent.message",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "text",
				json: `{"type":"text","text":"hello","truncated":false,"status":"completed"}`,
			},
		)},
	}
	response, err := store.WriteEvent(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteEvent: %v", err)
	}
	if response.GetEventId() == "" || response.GetSequence() != 1 {
		t.Fatalf("WriteEvent response event=%q sequence=%d; want generated/1", response.GetEventId(), response.GetSequence())
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
	if assistantMessage.Role != "assistant" || assistantMessage.Origin != "agent" || assistantMessage.Status != "completed" ||
		len(assistantMessage.Parts) != 1 || assistantMessage.Parts[0].Type != "text" || assistantMessage.Parts[0].Text != "hello" || assistantMessage.Parts[0].Status != "completed" {
		t.Fatalf("assistant RuntimeMessage projection = %+v; want completed assistant text message", assistantMessage)
	}

	conflict := proto.Clone(request).(*bridgev1.WriteEventRequest)
	conflict.PayloadJson = `{"type":"agent.message","content":[{"type":"text","text":"different"}]}`
	if _, err := store.WriteEvent(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting WriteEvent err = %v; want AlreadyExists", err)
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
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_use",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"web","input":{"search_query":[{"q":"tetral"}]},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
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
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_result",
			"agent.tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_web","toolName":"web","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"search_query":[{"q":"tetral"}]},"preview":"{\"search_query\":[{\"q\":\"tetral\"}]}","truncated":false},"output":{"text":"web result","truncated":false}}}`,
			},
		)},
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
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_web_tool_use_concurrent",
		ModelRequestId: modelRequestID,
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"web","input":{"search_query":[{"q":"tetral"}]},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_use_concurrent",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
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
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_web_tool_result_concurrent",
			"agent.tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_bridge_web_concurrent","toolName":"web","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"search_query":[{"q":"tetral"}]},"preview":"{\"search_query\":[{\"q\":\"tetral\"}]}","truncated":false},"output":{"text":"web result","truncated":false}}}`,
			},
		)},
		ServerToolUse: &bridgev1.ServerToolUseUsage{WebSearchRequests: 1, WebFetchRequests: 1},
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
	readUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_read_tool_use",
		ModelRequestId: "mreq_bridge_read_tool",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Read","input":{"file_path":"README.md"},"evaluated_permission":"allow"}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_read_tool_use",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
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
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_read_tool_result_with_web_usage",
			"agent.tool_result",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_read","toolName":"Read","toolUseEventId":"` + readUse.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"file_path":"README.md"},"preview":"{\"file_path\":\"README.md\"}","truncated":false},"output":{"text":"done","truncated":false}}}`,
			},
		)},
		ServerToolUse: &bridgev1.ServerToolUseUsage{WebSearchRequests: 1},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent usage on non-Web result err = %v; want InvalidArgument", err)
	}
	var count int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_web_usage_reject'`).Scan(&count); err != nil {
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
	request := &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_tool_ask",
		ModelRequestId: "mreq_bridge_tool_ask",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"dangerous_tool","input":{"path":"README.md"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_bridge_tool_ask",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
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
	var expiresAt string
	var pendingCount int
	var messageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT model_tool_call_id, tool_name, input_json, status, expires_at
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_tool_ask'
		    AND session_thread_id = 'thr_bridge_tool_ask'
		    AND tool_use_event_id = $1`,
		response.GetEventId()).Scan(&modelToolCallID, &toolName, &inputJSON, &pendingStatus, &expiresAt); err != nil {
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
		expiresAt != "2026-01-01T00:30:00Z" || pendingCount != 1 || messageCount != 1 {
		t.Fatalf("pending declaration ack=%s replay=%s event=%s/%s model=%q tool=%q input=%s status=%q expires=%q pending=%d messages=%d; want ask pending idempotent with one loop-authored context row",
			response.GetAck().GetStatus(), replay.GetAck().GetStatus(), response.GetEventId(), replay.GetEventId(),
			modelToolCallID, toolName, inputJSON, pendingStatus, expiresAt, pendingCount, messageCount)
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
			runtimeWriteID := "rwrite_bridge_tool_" + permission
			if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:          scope,
				RuntimeWriteId: runtimeWriteID,
				ModelRequestId: "mreq_bridge_tool_" + permission,
				EventType:      "agent.tool_use",
				PayloadJson:    `{"type":"agent.tool_use","name":"safe_tool","input":{"ok":true},"evaluated_permission":"` + permission + `"}`,
				SessionVisible: true,
				Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
					t,
					scope,
					runtimeWriteID,
					"agent.tool_use",
					"streaming",
					bridgeRuntimePartDraftForTest{
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
