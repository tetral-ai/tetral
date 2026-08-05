package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/blob"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	internalgrpcauth "github.com/tetral-ai/tetral/internal/internalgrpc/auth"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

// This file owns the Bridge settlement protocol-family boundary.

func TestPostgreSQLBridgeAPIStoreWriteRequestEndPersistsSpanUsageAndCumulativeProjection(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_end", "thr_bridge_request_end")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_end", "bind_bridge_request_end", 1, "pod_uid_request_end")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_bridge_request_end", "thr_bridge_request_end", "bind_bridge_request_end", 1, "pod_uid_request_end"),
		RuntimeWriteId: "rwrite_bridge_request_start",
		ModelRequestId: "mreq_bridge_request",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson:    `{"type":"span.model_request_start","model_request_id":"mreq_bridge_request"}`,
		SessionVisible: false,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    bridgeAPIScope("sesn_bridge_request_end", "thr_bridge_request_end", "bind_bridge_request_end", 1, "pod_uid_request_end"),
		RuntimeWriteId:           "rwrite_bridge_request_end",
		ModelRequestId:           "mreq_bridge_request",
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":11,"cache_read_input_tokens":3,"cache_creation_input_tokens":2,"output_tokens":7,"reasoning_output_tokens":1,"provider_usage_json":"{\"provider\":\"openai\",\"raw_tokens\":18}"}`,
	}
	response, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	if response.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("request-end committed ack write id = %q; want %q", response.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if replay.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("request-end duplicate ack write id = %q; want %q", replay.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}

	var eventPayload string
	var sessionVisible bool
	var modelRequestID string
	var latestStreamPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json, session_visible, model_request_id, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_request_end'
		    AND type = 'span.model_request_end'`).Scan(&eventPayload, &sessionVisible, &modelRequestID, &latestStreamPosition); err != nil {
		t.Fatalf("read request end event: %v", err)
	}
	if !sessionVisible || modelRequestID != "mreq_bridge_request" || latestStreamPosition == 0 {
		t.Fatalf("request end event sessionVisible/model/stream = %v/%q/%d; want true/model/stream", sessionVisible, modelRequestID, latestStreamPosition)
	}
	var startSessionVisible bool
	if err := admin.QueryRowContext(context.Background(),
		`SELECT session_visible FROM session_events WHERE workspace_id = 'default' AND event_id = $1`,
		start.GetEventId(),
	).Scan(&startSessionVisible); err != nil {
		t.Fatalf("read request start visibility: %v", err)
	}
	if !startSessionVisible {
		t.Fatal("primary request start event must be session-visible")
	}
	if got := testJSONPathString(t, eventPayload, "model_request_start_id"); got != start.GetEventId() {
		t.Fatalf("model_request_start_id = %q; want %q", got, start.GetEventId())
	}
	if got := testJSONPathInt(t, eventPayload, "model_usage.cache_creation_input_tokens"); got != 2 {
		t.Fatalf("cache creation in event = %d; want 2", got)
	}
	if !strings.Contains(eventPayload, `"speed":null`) {
		t.Fatalf("request end event = %s; want nullable model_usage.speed", eventPayload)
	}

	var inputTotal int64
	var requestKind string
	var inputUncached int64
	var cacheRead sql.NullInt64
	var cacheWrite sql.NullInt64
	var outputTotal int64
	var outputReasoning sql.NullInt64
	var total sql.NullInt64
	var providerUsageJSON sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT request_kind, input_total_tokens, input_uncached_tokens, input_cache_read_tokens, input_cache_write_tokens,
		        output_total_tokens, output_reasoning_tokens, total_tokens, provider_usage_json
		   FROM request_usage_details
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_request_end'
		    AND model_request_id = 'mreq_bridge_request'`).Scan(
		&requestKind,
		&inputTotal,
		&inputUncached,
		&cacheRead,
		&cacheWrite,
		&outputTotal,
		&outputReasoning,
		&total,
		&providerUsageJSON,
	); err != nil {
		t.Fatalf("read request usage detail: %v", err)
	}
	if requestKind != "agent_provider_request" || inputTotal != 11 || inputUncached != 6 || !cacheRead.Valid || cacheRead.Int64 != 3 ||
		!cacheWrite.Valid || cacheWrite.Int64 != 2 || outputTotal != 7 ||
		!outputReasoning.Valid || outputReasoning.Int64 != 1 || !total.Valid || total.Int64 != 18 {
		t.Fatalf("usage detail = kind %q input %d uncached %d read %v write %v output %d reasoning %v total %v; want normalized usage",
			requestKind, inputTotal, inputUncached, cacheRead, cacheWrite, outputTotal, outputReasoning, total)
	}
	if !providerUsageJSON.Valid || providerUsageJSON.String != `{"provider":"openai","raw_tokens":18}` {
		t.Fatalf("provider usage json = %q valid=%v; want raw provider usage", providerUsageJSON.String, providerUsageJSON.Valid)
	}

	var sessionUsage string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_request_end'`).Scan(&sessionUsage); err != nil {
		t.Fatalf("read session usage: %v", err)
	}
	if got := testJSONPathInt(t, sessionUsage, "input_tokens"); got != 11 {
		t.Fatalf("session input usage = %d; want 11", got)
	}
	if got := testJSONPathInt(t, sessionUsage, "output_tokens"); got != 7 {
		t.Fatalf("session output usage = %d; want 7", got)
	}
	if got := testJSONPathInt(t, sessionUsage, "cache_creation.ephemeral_1h_input_tokens"); got != 2 {
		t.Fatalf("session cache write usage = %d; want 2", got)
	}
	if got := testJSONPathInt(t, sessionUsage, "cache_read_input_tokens"); got != 3 {
		t.Fatalf("session cache read usage = %d; want 3", got)
	}
	if got := testJSONPathInt(t, sessionUsage, "request_count"); got != 1 {
		t.Fatalf("session request_count = %d; want 1", got)
	}
	for path, want := range map[string]int64{
		"input_total_tokens":       11,
		"input_uncached_tokens":    6,
		"input_cache_read_tokens":  3,
		"input_cache_write_tokens": 2,
		"output_total_tokens":      7,
		"output_reasoning_tokens":  1,
		"total_tokens":             18,
	} {
		if got := testJSONPathInt(t, sessionUsage, path); got != want {
			t.Fatalf("session internal usage %s = %d; want %d", path, got, want)
		}
	}
	var endEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_request_end' AND type = 'span.model_request_end'`).Scan(&endEventCount); err != nil {
		t.Fatalf("read request end event count: %v", err)
	}
	if endEventCount != 1 {
		t.Fatalf("request end event count after replay = %d; want 1", endEventCount)
	}

	conflict := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
	conflict.UsageJson = `{"input_tokens":12,"output_tokens":7}`
	if _, err := store.WriteRequestEnd(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting WriteRequestEnd err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndSettlesOpenInterruptAtomically(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID       = "sesn_bridge_request_interrupt"
		threadID        = "thr_bridge_request_interrupt"
		bindingID       = "bind_bridge_request_interrupt"
		podUID          = "pod_uid_request_interrupt"
		modelRequestID  = "mreq_bridge_request_interrupt"
		runtimeInputID  = "rin_bridge_request_interrupt"
		interruptEvent  = "evt_bridge_request_interrupt"
		toolUseEventID  = "evt_bridge_request_interrupt_tool"
		requestEndWrite = "rwrite_bridge_request_interrupt_end"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	seedBridgeAPIPendingApproval(t, admin, "default", sessionID, threadID, toolUseEventID, 1)
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, interruptEvent, 3, "user.interrupt", `{"type":"user.interrupt"}`)
	seedBridgeAPIRuntimeInbox(t, admin, "default", sessionID, threadID, runtimeInputID, "interrupt_control", `["`+interruptEvent+`"]`, "accepted", bindingID, podUID, 3, 3)

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_request_interrupt_start",
		ModelRequestId: modelRequestID,
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	cancellationLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		sessionID,
		threadID,
		"interrupt_control",
		runtimeInputID,
		"cancellation",
		"0",
	)
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           requestEndWrite,
		ModelRequestId:           modelRequestID,
		ModelRequestStartEventId: start.GetEventId(),
		IsError:                  true,
		ErrorKind:                "runtime_interrupted",
		FinishReason:             "cancelled",
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  cancellationLocalID,
			SourceKind:      "interrupt_control",
			SourceId:        runtimeInputID,
			SourceEventId:   interruptEvent,
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION,
			MessageInfoJson: `{"role":"assistant","origin":"agent","status":"completed"}`,
			Parts: []*bridgev1.RuntimePartDraft{{
				RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", cancellationLocalID, "tool", "0"),
				PartKind:           "tool",
				PartJson:           `{"type":"tool","toolCallId":"tool-1","toolName":"Write","toolUseEventId":"` + toolUseEventID + `","state":{"status":"cancelled","error":{"type":"runtime","message":"aborted","retryable":false}},"completedAt":"2026-07-30T00:00:00Z"}`,
			}},
		}},
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{
			RuntimeInputId: runtimeInputID,
			EventIds:       []string{interruptEvent},
			SequenceFrom:   3,
			SequenceTo:     3,
			PendingToolCancellations: []*bridgev1.PendingToolCancellationDraft{{
				ToolUseEventId: toolUseEventID,
				RuntimeLocalId: cancellationLocalID,
			}},
		},
	}
	response, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd replay: %v", err)
	}

	var inboxStatus string
	var processedAt sql.NullString
	var pendingStatus string
	var requestEndCount int
	var cancellationMessageCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_runtime_inbox WHERE workspace_id = 'default' AND runtime_input_id = $1`,
		runtimeInputID,
	).Scan(&inboxStatus); err != nil {
		t.Fatalf("read interrupt inbox: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT processed_at FROM session_events WHERE workspace_id = 'default' AND event_id = $1`,
		interruptEvent,
	).Scan(&processedAt); err != nil {
		t.Fatalf("read interrupt event: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_pending_tool_uses WHERE workspace_id = 'default' AND tool_use_event_id = $1`,
		toolUseEventID,
	).Scan(&pendingStatus); err != nil {
		t.Fatalf("read pending tool: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'span.model_request_end'`,
		sessionID,
	).Scan(&requestEndCount); err != nil {
		t.Fatalf("read request end count: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_messages
		  WHERE workspace_id = 'default' AND session_id = $1 AND source_event_id = $2`,
		sessionID,
		interruptEvent,
	).Scan(&cancellationMessageCount); err != nil {
		t.Fatalf("read cancellation message count: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		len(response.GetDeclaration().GetReceipts()) != 2 ||
		len(replay.GetDeclaration().GetReceipts()) != 2 ||
		inboxStatus != "committed" || !processedAt.Valid || pendingStatus != "cancelled" ||
		requestEndCount != 1 || cancellationMessageCount != 1 {
		t.Fatalf("request-end interrupt settlement ack=%s replay=%s receipts=%d/%d inbox=%q processed=%v pending=%q ends=%d cancellations=%d; want one atomic settlement and exact replay",
			response.GetAck().GetStatus(), replay.GetAck().GetStatus(),
			len(response.GetDeclaration().GetReceipts()), len(replay.GetDeclaration().GetReceipts()),
			inboxStatus, processedAt.Valid, pendingStatus, requestEndCount, cancellationMessageCount)
	}
}

func TestRequestEndInterruptCommitCarriesAcceptedSandboxExecutions(t *testing.T) {
	request, _, err := requestEndInterruptCommitRequest(&bridgev1.WriteRequestEndRequest{
		IsError:      true,
		ErrorKind:    "runtime_interrupted",
		FinishReason: "cancelled",
		InterruptSettlement: &bridgev1.RequestEndInterruptSettlement{
			RuntimeInputId:                  "rin_interrupt_request_end",
			EventIds:                        []string{"sevt_interrupt_request_end"},
			SequenceFrom:                    4,
			SequenceTo:                      4,
			SandboxExecutionToolUseEventIds: []string{"sevt_sandbox_execution"},
		},
	})
	if err != nil {
		t.Fatalf("requestEndInterruptCommitRequest: %v", err)
	}
	if got := request.GetSandboxExecutionToolUseEventIds(); !reflect.DeepEqual(got, []string{"sevt_sandbox_execution"}) {
		t.Fatalf("sandbox execution ids = %v; want accepted execution identity", got)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsWebToolCounters(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_end_web_usage", "thr_bridge_request_end_web_usage")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_end_web_usage", "bind_bridge_request_end_web_usage", 1, "pod_uid_request_end_web_usage")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_bridge_request_end_web_usage", "thr_bridge_request_end_web_usage", "bind_bridge_request_end_web_usage", 1, "pod_uid_request_end_web_usage"),
		RuntimeWriteId: "rwrite_bridge_request_start_web_usage",
		ModelRequestId: "mreq_bridge_request_web_usage",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_request_web_usage"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope:                    bridgeAPIScope("sesn_bridge_request_end_web_usage", "thr_bridge_request_end_web_usage", "bind_bridge_request_end_web_usage", 1, "pod_uid_request_end_web_usage"),
		RuntimeWriteId:           "rwrite_bridge_request_end_web_usage",
		ModelRequestId:           "mreq_bridge_request_web_usage",
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":1,"output_tokens":1,"web_search_requests":1}`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRequestEnd web usage err = %v; want InvalidArgument", err)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningSettlementIsAtomic(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reasoning_settle", "thr_bridge_reasoning_settle")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reasoning_settle", "bind_bridge_reasoning_settle", 1, "pod_uid_reasoning_settle")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC) }
	store.RuntimeBindingTokenHMACKey = []byte("bridge-runtime-binding-token-test-key-32")
	scope := bridgeAPIScope("sesn_bridge_reasoning_settle", "thr_bridge_reasoning_settle", "bind_bridge_reasoning_settle", 1, "pod_uid_reasoning_settle")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_reasoning_settle_start",
		ModelRequestId: "mreq_bridge_reasoning_settle",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_reasoning_settle"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	terminalMessageLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		"sesn_bridge_reasoning_settle",
		"thr_bridge_reasoning_settle",
		"model_request",
		"mreq_bridge_reasoning_settle",
		"assistant_text",
		"0",
	)
	firstReasoningLocalID := stableRuntimeID(
		"runtime_message_part_draft",
		terminalMessageLocalID,
		"reasoning",
		"0",
	)
	secondReasoningLocalID := stableRuntimeID(
		"runtime_message_part_draft",
		terminalMessageLocalID,
		"reasoning",
		"1",
	)
	firstReasoningText := "REASONING_HEAD" + strings.Repeat("r", 9_000) + "REASONING_TAIL"
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_reasoning_settle_end",
		ModelRequestId:           "mreq_bridge_reasoning_settle",
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":4,"output_tokens":3}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{
			{ReasoningPartId: firstReasoningLocalID, ProviderPartId: "provider_reason_1", PartSequence: 0, Text: firstReasoningText, MetadataJson: `{"anthropic":{"signature":"sig_signed"}}`},
			{ReasoningPartId: secondReasoningLocalID, ProviderPartId: "provider_reason_2", PartSequence: 1, Text: "second private thought", MetadataJson: `{"openai":{"encrypted_content":"ciphertext"}}`, Truncated: true},
		},
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  terminalMessageLocalID,
			SourceKind:      "model_request",
			SourceId:        "mreq_bridge_reasoning_settle",
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_ASSISTANT_TEXT,
			MessageInfoJson: `{"role":"assistant","origin":"agent","status":"completed","finishReason":"stop"}`,
			Parts: []*bridgev1.RuntimePartDraft{
				{
					RuntimeLocalPartId: firstReasoningLocalID,
					PartKind:           "reasoning",
					PartJson:           `{"type":"reasoning","providerPartId":"provider_reason_1","text":"` + firstReasoningText + `","providerMetadata":{"anthropic":{"signature":"sig_signed"}},"truncated":false,"status":"completed"}`,
				},
				{
					RuntimeLocalPartId: secondReasoningLocalID,
					PartKind:           "reasoning",
					Ordinal:            1,
					PartJson:           `{"type":"reasoning","providerPartId":"provider_reason_2","text":"second private thought","providerMetadata":{"openai":{"encrypted_content":"ciphertext"}},"truncated":true,"status":"completed"}`,
				},
			},
		}},
	}
	response, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd stable reasoning: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("stable reasoning ack = %s; want committed", response.GetAck().GetStatus())
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 ||
		len(response.GetDeclaration().GetReceipts()[0].GetMessages()) != 1 ||
		len(response.GetDeclaration().GetReceipts()[0].GetMessages()[0].GetParts()) != 2 {
		t.Fatalf("stable reasoning receipt = %+v; want one sealed message with two part stamps", response.GetDeclaration())
	}
	sealedMessageStamp := response.GetDeclaration().GetReceipts()[0].GetMessages()[0]
	if sealedMessageStamp.GetDisposition() != bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED {
		t.Fatalf("reasoning-only sealed message disposition = %s; want created", sealedMessageStamp.GetDisposition())
	}

	var messageID, modelRequestID, dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT message_id, model_request_id, data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_reasoning_settle'
		    AND session_thread_id = 'thr_bridge_reasoning_settle'`,
	).Scan(&messageID, &modelRequestID, &dataJSON); err != nil {
		t.Fatalf("read settled assistant row: %v", err)
	}
	if messageID == "" || modelRequestID != "mreq_bridge_reasoning_settle" {
		t.Fatalf("settled assistant identity = %q/%q; want Bridge message id/model request association", messageID, modelRequestID)
	}
	var message struct {
		ID    string `json:"id"`
		Parts []struct {
			ID               string         `json:"id"`
			MessageID        string         `json:"messageId"`
			Sequence         int            `json:"sequence"`
			ProviderMetadata map[string]any `json:"providerMetadata"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
		t.Fatalf("decode settled assistant row: %v", err)
	}
	if message.ID != messageID || len(message.Parts) != 2 ||
		message.Parts[0].ID != sealedMessageStamp.GetParts()[0].GetPartId() || message.Parts[0].ID == firstReasoningLocalID ||
		message.Parts[0].Sequence != 0 || message.Parts[0].MessageID != messageID ||
		message.Parts[1].ID != sealedMessageStamp.GetParts()[1].GetPartId() || message.Parts[1].ID == secondReasoningLocalID ||
		message.Parts[1].Sequence != 1 || message.Parts[1].MessageID != messageID {
		t.Fatalf("settled reasoning message = %+v; want two ordered Bridge-owned parts", message)
	}
	if anthropic, ok := message.Parts[0].ProviderMetadata["anthropic"].(map[string]any); !ok || anthropic["signature"] != "sig_signed" {
		t.Fatalf("first provider metadata = %+v; want signed metadata retained", message.Parts[0].ProviderMetadata)
	}
	if openai, ok := message.Parts[1].ProviderMetadata["openai"].(map[string]any); !ok || openai["encrypted_content"] != "ciphertext" {
		t.Fatalf("second provider metadata = %+v; want encrypted metadata retained", message.Parts[1].ProviderMetadata)
	}
	normalized, err := normalizeStableReasoningParts(request)
	if err != nil {
		t.Fatalf("normalize settled stable reasoning: %v", err)
	}
	var stableReasoningJSON sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT stable_reasoning_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_reasoning_settle'
		    AND model_request_id = 'mreq_bridge_reasoning_settle'
		    AND type = 'span.model_request_end'`,
	).Scan(&stableReasoningJSON); err != nil {
		t.Fatalf("read request-end stable reasoning ledger: %v", err)
	}
	if !stableReasoningJSON.Valid || stableReasoningJSON.String != normalized.CanonicalJSON {
		t.Fatalf("request-end stable reasoning ledger = %q valid=%v; want %s", stableReasoningJSON.String, stableReasoningJSON.Valid, normalized.CanonicalJSON)
	}
	var publicEventJSON string
	var reasoningEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT COALESCE(string_agg(payload_json || COALESCE(projection_json, ''), ''), ''),
		        count(*) FILTER (WHERE type LIKE '%reasoning%')
		   FROM session_events
		  WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_reasoning_settle'`,
	).Scan(&publicEventJSON, &reasoningEventCount); err != nil {
		t.Fatalf("read reasoning public event surfaces: %v", err)
	}
	if reasoningEventCount != 0 || strings.Contains(publicEventJSON, "sig_signed") || strings.Contains(publicEventJSON, "ciphertext") || strings.Contains(publicEventJSON, "providerMetadata") {
		t.Fatalf("reasoning public event surface count/body = %d/%s; want no reasoning or provider metadata exposure", reasoningEventCount, publicEventJSON)
	}
	loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope, RuntimeInputId: "rin_bridge_reasoning_settle_cold"})
	if err != nil {
		t.Fatalf("LoadContext stable reasoning cold round trip: %v", err)
	}
	contextJSON := loaded.GetContextJson()
	firstIndex := strings.Index(contextJSON, `"id":"`+message.Parts[0].ID+`"`)
	secondIndex := strings.Index(contextJSON, `"id":"`+message.Parts[1].ID+`"`)
	if firstIndex < 0 || secondIndex <= firstIndex ||
		strings.Count(contextJSON, `"id":"`+message.Parts[0].ID+`"`) != 1 ||
		strings.Count(contextJSON, `"id":"`+message.Parts[1].ID+`"`) != 1 ||
		strings.Count(contextJSON, `"signature":"sig_signed"`) != 1 ||
		strings.Count(contextJSON, `"encrypted_content":"ciphertext"`) != 1 ||
		strings.Count(contextJSON, "REASONING_HEAD") != 1 || strings.Count(contextJSON, "REASONING_TAIL") != 1 {
		t.Fatalf("cold context = %s; want ordered signed/encrypted stable reasoning", contextJSON)
	}

	replay, err := store.WriteRequestEnd(context.Background(), proto.Clone(request).(*bridgev1.WriteRequestEndRequest))
	if err != nil || replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("stable reasoning replay = %+v err=%v; want duplicate", replay, err)
	}
	replayMessages := replay.GetDeclaration().GetReceipts()[0].GetMessages()
	if len(replayMessages) != 1 ||
		replayMessages[0].GetMessageId() != sealedMessageStamp.GetMessageId() ||
		len(replayMessages[0].GetParts()) != 2 ||
		replayMessages[0].GetParts()[0].GetPartId() != sealedMessageStamp.GetParts()[0].GetPartId() ||
		replayMessages[0].GetParts()[1].GetPartId() != sealedMessageStamp.GetParts()[1].GetPartId() {
		t.Fatalf("stable reasoning replay stamps = %+v; want original terminal seal stamps", replayMessages)
	}
	divergent := map[string]func(*bridgev1.WriteRequestEndRequest){
		"changed": func(candidate *bridgev1.WriteRequestEndRequest) {
			candidate.StableReasoningParts[0].Text = "changed"
		},
		"missing": func(candidate *bridgev1.WriteRequestEndRequest) {
			candidate.StableReasoningParts = candidate.StableReasoningParts[:1]
		},
		"added": func(candidate *bridgev1.WriteRequestEndRequest) {
			candidate.StableReasoningParts = append(candidate.StableReasoningParts, &bridgev1.StableReasoningPart{ReasoningPartId: "reason_bridge_settle_3", ProviderPartId: "provider_reason_3", PartSequence: 2, Text: "added", MetadataJson: `{}`})
		},
		"reordered": func(candidate *bridgev1.WriteRequestEndRequest) {
			candidate.StableReasoningParts[0], candidate.StableReasoningParts[1] = candidate.StableReasoningParts[1], candidate.StableReasoningParts[0]
		},
	}
	for name, mutate := range divergent {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
			mutate(candidate)
			if _, err := store.WriteRequestEnd(context.Background(), candidate); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("divergent stable reasoning replay err = %v; want AlreadyExists", err)
			}
		})
	}

	var endCount, usageCount, messageCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_reasoning_settle' AND type = 'span.model_request_end'`).Scan(&endCount); err != nil {
		t.Fatalf("count request ends: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM request_usage_details WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_reasoning_settle'`).Scan(&usageCount); err != nil {
		t.Fatalf("count usage rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_bridge_reasoning_settle'`).Scan(&messageCount); err != nil {
		t.Fatalf("count assistant rows: %v", err)
	}
	if endCount != 1 || usageCount != 1 || messageCount != 1 {
		t.Fatalf("settlement counts end/usage/message = %d/%d/%d; want 1/1/1", endCount, usageCount, messageCount)
	}
}

func TestNormalizeStableReasoningPartsEnforcesExactBoundsAndCanonicalMetadata(t *testing.T) {
	part := func(id string, sequence int32, text string, metadata string) *bridgev1.StableReasoningPart {
		return &bridgev1.StableReasoningPart{
			ReasoningPartId: id,
			ProviderPartId:  "provider_" + id,
			PartSequence:    sequence,
			Text:            text,
			MetadataJson:    metadata,
		}
	}
	exactAggregate := &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{
		part("exact_1", 0, strings.Repeat("a", 1024*1024-2), `{}`),
		part("exact_2", 1, strings.Repeat("b", 1024*1024-2), `{}`),
	}}
	if normalized, err := normalizeStableReasoningParts(exactAggregate); err != nil || len(normalized.Parts) != 2 || !normalized.StrictlyOrdered {
		t.Fatalf("exact 2 MiB aggregate = %+v err=%v; want accepted ordered pair", normalized, err)
	}

	exactCount := &bridgev1.WriteRequestEndRequest{}
	for sequence := range MaxStableReasoningPartsPerRequest {
		exactCount.StableReasoningParts = append(exactCount.StableReasoningParts, part(fmt.Sprintf("count_%d", sequence), int32(sequence), "x", `{ "z": 1, "a": 2 }`))
	}
	normalized, err := normalizeStableReasoningParts(exactCount)
	if err != nil || len(normalized.Parts) != MaxStableReasoningPartsPerRequest || !strings.Contains(normalized.CanonicalJSON, `"metadata":{"a":2,"z":1}`) {
		t.Fatalf("exact count/canonical metadata = count %d canonical=%q err=%v", len(normalized.Parts), normalized.CanonicalJSON, err)
	}

	exactMetadata := `{"x":"` + strings.Repeat("m", 64*1024-8) + `"}`
	if _, err := normalizeStableReasoningParts(&bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("metadata_exact", 0, "", exactMetadata)}}); err != nil {
		t.Fatalf("exact 64 KiB metadata: %v", err)
	}

	tests := []struct {
		name    string
		request *bridgev1.WriteRequestEndRequest
	}{
		{name: "seventeen parts", request: func() *bridgev1.WriteRequestEndRequest {
			request := proto.Clone(exactCount).(*bridgev1.WriteRequestEndRequest)
			request.StableReasoningParts = append(request.StableReasoningParts, part("count_16", 16, "x", `{}`))
			return request
		}()},
		{name: "aggregate over", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{
			part("over_1", 0, strings.Repeat("a", 1024*1024-2), `{}`),
			part("over_2", 1, strings.Repeat("b", 1024*1024-1), `{}`),
		}}},
		{name: "text over", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("text_over", 0, strings.Repeat("x", 1024*1024+1), `{}`)}}},
		{name: "metadata over", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("metadata_over", 0, "", `{"x":"`+strings.Repeat("m", 64*1024-7)+`"}`)}}},
		{name: "metadata array", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("metadata_array", 0, "", `[]`)}}},
		{name: "invalid utf8", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("utf8", 0, string([]byte{0xff}), `{}`)}}},
		{name: "missing id", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("", 0, "x", `{}`)}}},
		{name: "duplicate id", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("duplicate", 0, "x", `{}`), part("duplicate", 1, "y", `{}`)}}},
		{name: "duplicate sequence", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("sequence_1", 0, "x", `{}`), part("sequence_2", 0, "y", `{}`)}}},
		{name: "negative sequence", request: &bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{part("negative", -1, "x", `{}`)}}},
		{name: "error end", request: &bridgev1.WriteRequestEndRequest{IsError: true, ErrorKind: "provider_error", StableReasoningParts: []*bridgev1.StableReasoningPart{part("error", 0, "x", `{}`)}}},
		{name: "reschedule end", request: &bridgev1.WriteRequestEndRequest{IsError: true, ErrorKind: "provider_error", Reschedule: &bridgev1.RequestEndReschedule{Attempt: 1}, StableReasoningParts: []*bridgev1.StableReasoningPart{part("reschedule", 0, "x", `{}`)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := normalizeStableReasoningParts(test.request); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("normalizeStableReasoningParts err = %v; want InvalidArgument", err)
			}
		})
	}

	outOfOrder, err := normalizeStableReasoningParts(&bridgev1.WriteRequestEndRequest{StableReasoningParts: []*bridgev1.StableReasoningPart{
		part("order_2", 2, "x", `{}`),
		part("order_1", 1, "y", `{}`),
	}})
	if err != nil || outOfOrder.StrictlyOrdered {
		t.Fatalf("out-of-order set = %+v err=%v; want deferred order rejection for replay hash comparison", outOfOrder, err)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningTextAndToolConvergeOnModelRequestAssistant(t *testing.T) {
	for _, test := range []struct {
		name      string
		textFirst bool
	}{
		{name: "text-first", textFirst: true},
		{name: "reasoning-tool-first", textFirst: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, "-", "_")
			sessionID := "sesn_bridge_reasoning_" + suffix
			threadID := "thr_bridge_reasoning_" + suffix
			bindingID := "bind_bridge_reasoning_" + suffix
			podUID := "pod_uid_reasoning_" + suffix
			modelRequestID := "mreq_reasoning_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
			store.Clock = func() time.Time { return now }
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)

			start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_start_" + suffix, ModelRequestId: modelRequestID,
				EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
			})
			if err != nil {
				t.Fatalf("WriteEvent start: %v", err)
			}
			reasoningPart := bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: `{"type":"reasoning","providerPartId":"provider_converge","providerMetadata":{},"text":"private","truncated":false,"status":"completed"}`,
			}
			textPart := bridgeRuntimePartDraftForTest{
				kind: "text",
				json: `{"type":"text","text":"answer","truncated":false,"status":"completed"}`,
			}
			writeText := func() {
				parts := []bridgeRuntimePartDraftForTest{textPart}
				if !test.textFirst {
					parts = []bridgeRuntimePartDraftForTest{
						reasoningPart,
						textPart,
						{
							kind: "tool",
							json: `{"type":"tool","toolCallId":"call_converge","toolName":"Read","toolEvent":{"kind":"tool"},"state":{"status":"running","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false}}}`,
						},
					}
				}
				if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: "rwrite_text_" + suffix, ModelRequestId: modelRequestID,
					EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"answer"}]}`,
					Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
						t,
						scope,
						"rwrite_text_"+suffix,
						"agent.message",
						"streaming",
						parts...,
					)},
				}); err != nil {
					t.Fatalf("WriteEvent text: %v", err)
				}
			}
			writeToolUse := func() *bridgev1.WriteEventResponse {
				parts := []bridgeRuntimePartDraftForTest{
					reasoningPart,
					{
						kind: "tool",
						json: `{"type":"tool","toolCallId":"call_converge","toolName":"Read","state":{"status":"running","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false}}}`,
					},
				}
				if test.textFirst {
					parts = []bridgeRuntimePartDraftForTest{reasoningPart, textPart, parts[1]}
				}
				response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: "rwrite_tool_use_" + suffix, ModelRequestId: modelRequestID,
					EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{"path":"a.txt"},"evaluated_permission":"allow"}`,
					StableReasoningParts: []*bridgev1.StableReasoningPart{{ReasoningPartId: "part_converge_reasoning", ProviderPartId: "provider_converge", PartSequence: 0, Text: "private", MetadataJson: `{}`}},
					Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
						t,
						scope,
						"rwrite_tool_use_"+suffix,
						"agent.tool_use",
						"streaming",
						parts...,
					)},
				})
				if err != nil {
					t.Fatalf("WriteEvent tool use: %v", err)
				}
				return response
			}
			var toolUse *bridgev1.WriteEventResponse
			if test.textFirst {
				writeText()
				toolUse = writeToolUse()
			} else {
				toolUse = writeToolUse()
				writeText()
			}
			if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_tool_result_" + suffix, ModelRequestId: modelRequestID,
				EventType: "agent.tool_result", PayloadJson: `{"type":"agent.tool_result","tool_use_id":"` + toolUse.GetEventId() + `","content":[{"type":"text","text":"file body"}],"is_error":false}`,
				Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
					t,
					scope,
					"rwrite_tool_result_"+suffix,
					"agent.tool_result",
					"streaming",
					reasoningPart,
					textPart,
					bridgeRuntimePartDraftForTest{
						kind: "tool",
						json: `{"type":"tool","toolCallId":"call_converge","toolName":"Read","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false},"output":{"text":"file body","truncated":false}}}`,
					},
				)},
			}); err != nil {
				t.Fatalf("WriteEvent tool result: %v", err)
			}
			var preSealMessageID, preSealSourceEventID, preSealStatus string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT message_id, source_event_id, data_json::jsonb->>'status'
				   FROM session_messages
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND session_thread_id = $3
				    AND model_request_id = $4`,
				"default", sessionID, threadID, modelRequestID,
			).Scan(&preSealMessageID, &preSealSourceEventID, &preSealStatus); err != nil {
				t.Fatalf("read pre-seal assistant row: %v", err)
			}
			if preSealMessageID == "" || preSealSourceEventID == "" || preSealStatus != "streaming" {
				t.Fatalf("pre-seal assistant identity/status = %q/%q/%q; want one durable streaming row", preSealMessageID, preSealSourceEventID, preSealStatus)
			}
			now = now.Add(time.Minute)
			if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_end_" + suffix, ModelRequestId: modelRequestID, ModelRequestStartEventId: start.GetEventId(),
				FinishReason: "stop", UsageJson: `{"input_tokens":2,"output_tokens":2}`,
				StableReasoningParts: []*bridgev1.StableReasoningPart{{ReasoningPartId: "part_converge_reasoning", ProviderPartId: "provider_converge", PartSequence: 0, Text: "private", MetadataJson: `{}`}},
				Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
					t,
					scope,
					modelRequestID,
					"completed",
					reasoningPart,
					textPart,
					bridgeRuntimePartDraftForTest{
						kind: "tool",
						json: `{"type":"tool","toolCallId":"call_converge","toolName":"Read","toolUseEventId":"` + toolUse.GetEventId() + `","toolEvent":{"kind":"tool"},"state":{"status":"completed","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false},"output":{"text":"file body","truncated":false}}}`,
					},
				)},
			}); err != nil {
				t.Fatalf("WriteRequestEnd reasoning: %v", err)
			}

			var rowCount, associatedRowCount int
			var messageID, sourceEventID, messageStatus, dataJSON string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*), count(*) FILTER (WHERE model_request_id = $4),
				        max(message_id), max(source_event_id), max(data_json::jsonb->>'status'), max(data_json)
				   FROM session_messages
				  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND kind = 'assistant'`,
				"default", sessionID, threadID, modelRequestID,
			).Scan(&rowCount, &associatedRowCount, &messageID, &sourceEventID, &messageStatus, &dataJSON); err != nil {
				t.Fatalf("read converged assistant row: %v", err)
			}
			if rowCount != 1 || associatedRowCount != 1 ||
				messageID != preSealMessageID || sourceEventID != preSealSourceEventID ||
				messageStatus != "completed" ||
				messageID == "msg_client_text" || messageID == "msg_client_tool_use" || messageID == "msg_client_tool_result" {
				t.Fatalf("sealed row count/associated/id/source/status = %d/%d/%q/%q/%q; want the same Bridge-owned row completed",
					rowCount, associatedRowCount, messageID, sourceEventID, messageStatus)
			}
			var message struct {
				Parts []struct {
					ID        string `json:"id"`
					MessageID string `json:"messageId"`
					Type      string `json:"type"`
				} `json:"parts"`
			}
			if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
				t.Fatalf("decode converged assistant row: %v", err)
			}
			wantParts := []string{"reasoning", "text", "tool"}
			if len(message.Parts) != len(wantParts) {
				t.Fatalf("converged parts = %+v; want %+v", message.Parts, wantParts)
			}
			for index, wantType := range wantParts {
				if message.Parts[index].ID == "" || message.Parts[index].Type != wantType || message.Parts[index].MessageID != messageID {
					t.Fatalf("converged part %d = %+v; want Bridge id/type/message %q/%q", index, message.Parts[index], wantType, messageID)
				}
			}
			assertStableReasoningProjectionCoveredByEventLedger(t, admin, sessionID, modelRequestID)
		})
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningSharedAnchorVector(t *testing.T) {
	encoded, err := os.ReadFile(filepath.Join(repoRootFromBridgeTest(t), "testdata", "stable-reasoning-anchor-vector.json"))
	if err != nil {
		t.Fatalf("read shared stable reasoning vector: %v", err)
	}
	var fixture struct {
		ModelRequestID string          `json:"model_request_id"`
		Event          json.RawMessage `json:"event"`
		Parts          []struct {
			ReasoningPartID string `json:"reasoning_part_id"`
			ProviderPartID  string `json:"provider_part_id"`
			PartSequence    int32  `json:"part_sequence"`
			Text            string `json:"text"`
			MetadataJSON    string `json:"metadata_json"`
			Truncated       bool   `json:"truncated"`
		} `json:"stable_reasoning_parts"`
	}
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatalf("decode shared stable reasoning vector: %v", err)
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_shared_anchor", "thr_shared_anchor")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_shared_anchor", "bind_shared_anchor", 1, "pod_shared_anchor")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	requestParts := make([]*bridgev1.StableReasoningPart, 0, len(fixture.Parts))
	draftParts := make([]bridgeRuntimePartDraftForTest, 0, len(fixture.Parts)+1)
	for _, part := range fixture.Parts {
		requestParts = append(requestParts, &bridgev1.StableReasoningPart{
			ReasoningPartId: part.ReasoningPartID,
			ProviderPartId:  part.ProviderPartID,
			PartSequence:    part.PartSequence,
			Text:            part.Text,
			MetadataJson:    part.MetadataJSON,
			Truncated:       part.Truncated,
		})
		draftParts = append(draftParts, bridgeRuntimePartDraftForTest{
			kind: "reasoning",
			json: fmt.Sprintf(
				`{"type":"reasoning","providerPartId":%q,"providerMetadata":%s,"text":%q,"truncated":%t,"status":"completed"}`,
				part.ProviderPartID,
				part.MetadataJSON,
				part.Text,
				part.Truncated,
			),
		})
	}
	draftParts = append(draftParts, bridgeRuntimePartDraftForTest{
		kind: "tool",
		json: `{"type":"tool","toolCallId":"call_shared_anchor","toolName":"Read","state":{"status":"running","input":{"value":{"path":"a.txt"},"preview":"{\"path\":\"a.txt\"}","truncated":false}}}`,
	})
	scope := bridgeAPIScope("sesn_shared_anchor", "thr_shared_anchor", "bind_shared_anchor", 1, "pod_shared_anchor")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_shared_anchor_start", fixture.ModelRequestID, "agent_provider_request", 0)
	draft := bridgeRuntimeOutputDraftForTest(
		t,
		scope,
		"rwrite_shared_anchor",
		"agent.tool_use",
		"streaming",
		draftParts...,
	)
	response, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:                scope,
		RuntimeWriteId:       "rwrite_shared_anchor",
		ModelRequestId:       fixture.ModelRequestID,
		EventType:            "agent.tool_use",
		PayloadJson:          string(fixture.Event),
		StableReasoningParts: requestParts,
		Drafts:               []*bridgev1.RuntimeMessageDraft{draft},
	})
	if err != nil {
		t.Fatalf("WriteEvent shared stable reasoning vector: %v", err)
	}
	normalized, err := normalizeStableReasoningParts(&bridgev1.WriteEventRequest{StableReasoningParts: requestParts})
	if err != nil {
		t.Fatalf("normalize shared stable reasoning vector: %v", err)
	}
	var stableReasoningJSON sql.NullString
	if err := admin.QueryRowContext(context.Background(),
		`SELECT stable_reasoning_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_shared_anchor'
		    AND model_request_id = $1
		    AND type = 'agent.tool_use'`,
		fixture.ModelRequestID,
	).Scan(&stableReasoningJSON); err != nil {
		t.Fatalf("read anchored stable reasoning ledger: %v", err)
	}
	if !stableReasoningJSON.Valid || stableReasoningJSON.String != normalized.CanonicalJSON {
		t.Fatalf("anchored stable reasoning ledger = %q valid=%v; want %s", stableReasoningJSON.String, stableReasoningJSON.Valid, normalized.CanonicalJSON)
	}
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_shared_anchor' AND model_request_id = $1`,
		fixture.ModelRequestID,
	).Scan(&dataJSON); err != nil {
		t.Fatalf("read shared stable reasoning assistant row: %v", err)
	}
	var message struct {
		Parts []struct {
			ID               string         `json:"id"`
			Type             string         `json:"type"`
			ProviderPartID   string         `json:"providerPartId"`
			Sequence         int32          `json:"sequence"`
			Text             string         `json:"text"`
			ProviderMetadata map[string]any `json:"providerMetadata"`
			Truncated        bool           `json:"truncated"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
		t.Fatalf("decode shared stable reasoning assistant row: %v", err)
	}
	receiptParts := response.GetDeclaration().GetReceipts()[0].GetMessages()[0].GetParts()
	if len(message.Parts) != len(draft.GetParts()) || len(receiptParts) != len(draft.GetParts()) {
		t.Fatalf("shared stable reasoning part counts message/receipt/draft = %d/%d/%d", len(message.Parts), len(receiptParts), len(draft.GetParts()))
	}
	for index, part := range message.Parts[:len(fixture.Parts)] {
		if receiptParts[index].GetRuntimeLocalPartId() != draft.GetParts()[index].GetRuntimeLocalPartId() ||
			receiptParts[index].GetPartId() != part.ID {
			t.Fatalf("shared stable reasoning receipt part %d = %+v; draft/message ids %q/%q", index, receiptParts[index], draft.GetParts()[index].GetRuntimeLocalPartId(), part.ID)
		}
		expected := fixture.Parts[index]
		var expectedMetadata map[string]any
		if err := json.Unmarshal([]byte(expected.MetadataJSON), &expectedMetadata); err != nil {
			t.Fatalf("decode shared stable reasoning metadata %d: %v", index, err)
		}
		if part.Type != "reasoning" || part.ProviderPartID != expected.ProviderPartID ||
			part.Sequence != expected.PartSequence || part.Text != expected.Text ||
			part.Truncated != expected.Truncated || !reflect.DeepEqual(part.ProviderMetadata, expectedMetadata) {
			t.Fatalf("shared stable reasoning assistant part %d = %+v; want fixture %+v metadata=%v", index, part, expected, expectedMetadata)
		}
	}
	if message.Parts[len(message.Parts)-1].Type != "tool" {
		t.Fatalf("shared stable reasoning final part = %+v; want tool", message.Parts[len(message.Parts)-1])
	}
	assertStableReasoningProjectionCoveredByEventLedger(t, admin, "sesn_shared_anchor", fixture.ModelRequestID)
}

func assertStableReasoningProjectionCoveredByEventLedger(t *testing.T, db *sql.DB, sessionID string, modelRequestID string) {
	t.Helper()
	var dataJSON string
	if err := db.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND model_request_id = $2`,
		sessionID,
		modelRequestID,
	).Scan(&dataJSON); err != nil {
		t.Fatalf("read stable reasoning projection: %v", err)
	}
	var message struct {
		Parts []struct {
			ID               string         `json:"id"`
			Type             string         `json:"type"`
			ProviderPartID   string         `json:"providerPartId"`
			Sequence         int32          `json:"sequence"`
			Text             string         `json:"text"`
			ProviderMetadata map[string]any `json:"providerMetadata"`
			Truncated        bool           `json:"truncated"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil {
		t.Fatalf("decode stable reasoning projection: %v", err)
	}
	rows, err := db.QueryContext(context.Background(),
		`SELECT stable_reasoning_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = $1
		    AND model_request_id = $2
		    AND stable_reasoning_json IS NOT NULL
		  ORDER BY sequence`,
		sessionID,
		modelRequestID,
	)
	if err != nil {
		t.Fatalf("read stable reasoning event ledger: %v", err)
	}
	defer func() { _ = rows.Close() }()
	ledger := make(map[string]normalizedStableReasoningPart)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			t.Fatalf("scan stable reasoning event ledger: %v", err)
		}
		var parts []normalizedStableReasoningPart
		if err := json.Unmarshal([]byte(encoded), &parts); err != nil {
			t.Fatalf("decode stable reasoning event ledger: %v", err)
		}
		for _, part := range parts {
			key := part.ProviderPartID
			if key == "" {
				key = strconv.FormatInt(int64(part.PartSequence), 10)
			}
			ledger[key] = part
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate stable reasoning event ledger: %v", err)
	}
	reasoningCount := 0
	for _, part := range message.Parts {
		if part.Type != "reasoning" {
			continue
		}
		reasoningCount++
		key := part.ProviderPartID
		if key == "" {
			key = strconv.FormatInt(int64(part.Sequence), 10)
		}
		got, ok := ledger[key]
		if !ok || got.ProviderPartID != part.ProviderPartID || got.PartSequence != part.Sequence ||
			got.Text != part.Text || got.Truncated != part.Truncated || !reflect.DeepEqual(got.Metadata, part.ProviderMetadata) {
			t.Fatalf("projected stable reasoning part %+v has ledger match %+v present=%v", part, got, ok)
		}
	}
	if reasoningCount == 0 {
		t.Fatal("stable reasoning projection contains no reasoning parts")
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningRejectsToolEventWithoutModelRequestID(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_missing_model", "thr_reasoning_missing_model")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_missing_model", "bind_reasoning_missing_model", 1, "pod_reasoning_missing_model")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_reasoning_missing_model", "thr_reasoning_missing_model", "bind_reasoning_missing_model", 1, "pod_reasoning_missing_model"),
		RuntimeWriteId: "rwrite_reasoning_missing_model",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{{
			ReasoningPartId: "part_missing_model", PartSequence: 0, Text: "private", MetadataJson: `{}`,
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent stable reasoning without model request id err = %v; want InvalidArgument", err)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningSettlementFirstRejectsPostSealToolAnchor(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_settlement_first", "thr_reasoning_settlement_first")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_settlement_first", "bind_reasoning_settlement_first", 1, "pod_reasoning_settlement_first")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	now := time.Date(2026, time.July, 16, 11, 0, 0, 0, time.UTC)
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope("sesn_reasoning_settlement_first", "thr_reasoning_settlement_first", "bind_reasoning_settlement_first", 1, "pod_reasoning_settlement_first")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_settlement_first_start", ModelRequestId: "mreq_settlement_first",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_settlement_first"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	part := &bridgev1.StableReasoningPart{ReasoningPartId: "part_settlement_first", ProviderPartId: "provider_settlement_first", PartSequence: 0, Text: "private", MetadataJson: `{"signature":"sig"}`}
	endResponse, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_settlement_first_end", ModelRequestId: "mreq_settlement_first", ModelRequestStartEventId: start.GetEventId(),
		FinishReason: "stop", UsageJson: `{}`, StableReasoningParts: []*bridgev1.StableReasoningPart{part},
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
			t,
			scope,
			"mreq_settlement_first",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: `{"type":"reasoning","providerPartId":"provider_settlement_first","providerMetadata":{"signature":"sig"},"text":"private","truncated":false,"status":"completed"}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	reasoningPartID := endResponse.GetDeclaration().GetReceipts()[0].GetMessages()[0].GetParts()[0].GetPartId()
	now = now.Add(time.Minute)
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_settlement_first_tool", ModelRequestId: "mreq_settlement_first",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{part},
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_settlement_first_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: `{"type":"reasoning","providerPartId":"provider_settlement_first","providerMetadata":{"signature":"sig"},"text":"private","truncated":false,"status":"completed"}`,
			},
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_settlement","toolName":"Read","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`,
			},
		)},
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteEvent anchor after settlement err = %v; want FailedPrecondition", err)
	}
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_settlement_first'`).Scan(&dataJSON); err != nil {
		t.Fatalf("read assistant row: %v", err)
	}
	if strings.Count(dataJSON, `"id":"`+reasoningPartID+`"`) != 1 || !strings.Contains(dataJSON, `"createdAt":"2026-07-16T11:00:00Z"`) {
		t.Fatalf("settlement-first row = %s; want one reasoning part with first-writer timestamp", dataJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningRejectsNonToolEventBeforeMutation(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_invalid_event", "thr_reasoning_invalid_event")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_invalid_event", "bind_reasoning_invalid_event", 1, "pod_reasoning_invalid_event")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_reasoning_invalid_event", "thr_reasoning_invalid_event", "bind_reasoning_invalid_event", 1, "pod_reasoning_invalid_event"),
		RuntimeWriteId: "rwrite_reasoning_invalid_event", ModelRequestId: "mreq_reasoning_invalid_event",
		EventType: "agent.message", PayloadJson: `{"type":"agent.message","content":[{"type":"text","text":"answer"}]}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{{ReasoningPartId: "part_invalid", PartSequence: 0, Text: "private", MetadataJson: `{}`}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteEvent non-tool stable reasoning err = %v; want InvalidArgument", err)
	}
	var events, messages, operations int
	for _, query := range []struct {
		target *int
		sql    string
	}{
		{&events, `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_reasoning_invalid_event'`},
		{&messages, `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND session_id = 'sesn_reasoning_invalid_event'`},
		{&operations, `SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = 'sesn_reasoning_invalid_event'`},
	} {
		if err := admin.QueryRowContext(context.Background(), query.sql).Scan(query.target); err != nil {
			t.Fatalf("count invalid-event writes: %v", err)
		}
	}
	if events != 0 || messages != 0 || operations != 0 {
		t.Fatalf("invalid-event writes events/messages/operations = %d/%d/%d; want zero", events, messages, operations)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningSettlementCannotOmitAnchor(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_missing_anchor", "thr_reasoning_missing_anchor")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_missing_anchor", "bind_reasoning_missing_anchor", 1, "pod_reasoning_missing_anchor")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_reasoning_missing_anchor", "thr_reasoning_missing_anchor", "bind_reasoning_missing_anchor", 1, "pod_reasoning_missing_anchor")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_missing_anchor_start", ModelRequestId: "mreq_missing_anchor",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_missing_anchor"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	toolMessageLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		"sesn_reasoning_missing_anchor",
		"thr_reasoning_missing_anchor",
		"agent.tool_use",
		"rwrite_missing_anchor_tool",
		runtimeDraftKindToken(bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_USE),
		"0",
	)
	reasoningPartLocalID := stableRuntimeID("runtime_message_part_draft", toolMessageLocalID, "reasoning", "0")
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_missing_anchor_tool", ModelRequestId: "mreq_missing_anchor",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{{ReasoningPartId: reasoningPartLocalID, PartSequence: 0, Text: "private", MetadataJson: `{}`}},
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  toolMessageLocalID,
			SourceKind:      "agent.tool_use",
			SourceId:        "rwrite_missing_anchor_tool",
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_USE,
			MessageInfoJson: `{"role":"assistant","origin":"agent","status":"streaming"}`,
			Parts: []*bridgev1.RuntimePartDraft{
				{
					RuntimeLocalPartId: reasoningPartLocalID,
					PartKind:           "reasoning",
					PartJson:           `{"type":"reasoning","text":"private","providerMetadata":{},"truncated":false,"status":"completed"}`,
				},
				{
					RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", toolMessageLocalID, "tool", "0"),
					PartKind:           "tool",
					PartJson:           `{"type":"tool","toolCallId":"call_missing_anchor","toolName":"Read","state":{"status":"running","input":{}}}`,
				},
			},
		}},
	}); err != nil {
		t.Fatalf("WriteEvent anchor: %v", err)
	}
	_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_missing_anchor_end", ModelRequestId: "mreq_missing_anchor", ModelRequestStartEventId: start.GetEventId(),
		FinishReason: "stop", UsageJson: `{}`,
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteRequestEnd missing anchor err = %v; want AlreadyExists", err)
	}
	var ends, usage int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = 'mreq_missing_anchor' AND type = 'span.model_request_end'`).Scan(&ends); err != nil {
		t.Fatalf("count request ends: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM request_usage_details WHERE workspace_id = 'default' AND model_request_id = 'mreq_missing_anchor'`).Scan(&usage); err != nil {
		t.Fatalf("count request usage: %v", err)
	}
	if ends != 0 || usage != 0 {
		t.Fatalf("missing-anchor writes end/usage = %d/%d; want zero", ends, usage)
	}
}

func TestPostgreSQLBridgeAPIStoreFailedRequestEndPreservesToolAnchoredReasoning(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_failed_end", "thr_reasoning_failed_end")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_failed_end", "bind_reasoning_failed_end", 1, "pod_reasoning_failed_end")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_reasoning_failed_end", "thr_reasoning_failed_end", "bind_reasoning_failed_end", 1, "pod_reasoning_failed_end")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_failed_start", ModelRequestId: "mreq_reasoning_failed",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_reasoning_failed"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	toolMessageLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		"sesn_reasoning_failed_end",
		"thr_reasoning_failed_end",
		"agent.tool_use",
		"rwrite_reasoning_failed_tool",
		runtimeDraftKindToken(bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_USE),
		"0",
	)
	reasoningPartLocalID := stableRuntimeID("runtime_message_part_draft", toolMessageLocalID, "reasoning", "0")
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_failed_tool", ModelRequestId: "mreq_reasoning_failed",
		EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{{
			ReasoningPartId: reasoningPartLocalID, PartSequence: 0, Text: "private", MetadataJson: `{}`,
		}},
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  toolMessageLocalID,
			SourceKind:      "agent.tool_use",
			SourceId:        "rwrite_reasoning_failed_tool",
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TOOL_USE,
			MessageInfoJson: `{"role":"assistant","origin":"agent","status":"streaming"}`,
			Parts: []*bridgev1.RuntimePartDraft{
				{
					RuntimeLocalPartId: reasoningPartLocalID,
					PartKind:           "reasoning",
					PartJson:           `{"type":"reasoning","text":"private","providerMetadata":{},"truncated":false,"status":"completed"}`,
				},
				{
					RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", toolMessageLocalID, "tool", "0"),
					PartKind:           "tool",
					PartJson:           `{"type":"tool","toolCallId":"call_reasoning_failed","toolName":"Read","state":{"status":"running","input":{}}}`,
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("WriteEvent anchor: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_failed_end", ModelRequestId: "mreq_reasoning_failed", ModelRequestStartEventId: start.GetEventId(),
		IsError: true, ErrorKind: "provider_error", FinishReason: "error", UsageJson: `{}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
			t,
			scope,
			"mreq_reasoning_failed",
			"failed",
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: `{"type":"reasoning","text":"private","providerMetadata":{},"truncated":false,"status":"completed"}`,
			},
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: fmt.Sprintf(
					`{"type":"tool","toolCallId":"call_reasoning_failed","toolName":"Read","toolUseEventId":%q,"toolEvent":{"kind":"tool"},"state":{"status":"running","input":{}}}`,
					toolUse.GetEventId(),
				),
			},
		)},
	}); err != nil {
		t.Fatalf("WriteRequestEnd failed settlement: %v", err)
	}
	var endCount int
	if err := admin.QueryRowContext(context.Background(), `
		SELECT count(*) FROM session_events
		WHERE workspace_id = 'default' AND session_id = 'sesn_reasoning_failed_end' AND type = 'span.model_request_end'
	`).Scan(&endCount); err != nil {
		t.Fatalf("count request ends: %v", err)
	}
	var dataJSON string
	if err := admin.QueryRowContext(context.Background(), `
		SELECT data_json FROM session_messages
		WHERE workspace_id = 'default' AND session_id = 'sesn_reasoning_failed_end'
		  AND model_request_id = 'mreq_reasoning_failed'
	`).Scan(&dataJSON); err != nil {
		t.Fatalf("read anchored reasoning message: %v", err)
	}
	if endCount != 1 || !strings.Contains(dataJSON, `"status":"failed"`) || !strings.Contains(dataJSON, `"type":"reasoning"`) || !strings.Contains(dataJSON, `"text":"private"`) {
		t.Fatalf("failed settlement end/message = %d/%s; want one end retaining anchored reasoning", endCount, dataJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningUnifiesBudgetAcrossAnchors(t *testing.T) {
	part := func(id string, sequence int32, text string) *bridgev1.StableReasoningPart {
		return &bridgev1.StableReasoningPart{ReasoningPartId: id, PartSequence: sequence, Text: text, MetadataJson: `{}`}
	}
	countFirst := make([]*bridgev1.StableReasoningPart, 0, MaxStableReasoningPartsPerRequest)
	for sequence := range MaxStableReasoningPartsPerRequest {
		countFirst = append(countFirst, part(fmt.Sprintf("part_count_%d", sequence), int32(sequence), "x"))
	}
	for _, test := range []struct {
		name          string
		first         []*bridgev1.StableReasoningPart
		second        []*bridgev1.StableReasoningPart
		firstToolSeq  int
		secondToolSeq int
	}{
		{name: "count", first: countFirst, second: []*bridgev1.StableReasoningPart{part("part_count_16", 16, "x")}, firstToolSeq: 100, secondToolSeq: 101},
		{name: "bytes", first: []*bridgev1.StableReasoningPart{part("part_bytes_1", 0, strings.Repeat("a", 1024*1024))}, second: []*bridgev1.StableReasoningPart{part("part_bytes_2", 1, strings.Repeat("b", 1024*1024))}, firstToolSeq: 2, secondToolSeq: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_reasoning_unified_" + test.name
			threadID := "thr_reasoning_unified_" + test.name
			bindingID := "bind_reasoning_unified_" + test.name
			modelRequestID := "mreq_reasoning_unified_" + test.name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_reasoning_unified")
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_reasoning_unified")
			seedBridgeAPIRequestStart(t, store, scope, "rwrite_unified_start", modelRequestID, "agent_provider_request", 0)
			anchoredParts := make([]*bridgev1.StableReasoningPart, 0, len(test.first)+len(test.second))
			writeTool := func(writeID, callID string, parts []*bridgev1.StableReasoningPart) error {
				allParts := append(append([]*bridgev1.StableReasoningPart{}, anchoredParts...), parts...)
				draftParts := make([]bridgeRuntimePartDraftForTest, 0, len(allParts)+1)
				for _, reasoning := range allParts {
					draftParts = append(draftParts, bridgeRuntimePartDraftForTest{
						kind: "reasoning",
						json: fmt.Sprintf(
							`{"type":"reasoning","text":%q,"truncated":%t,"status":"completed"}`,
							reasoning.GetText(),
							reasoning.GetTruncated(),
						),
					})
				}
				draftParts = append(draftParts, bridgeRuntimePartDraftForTest{
					kind: "tool",
					json: fmt.Sprintf(
						`{"type":"tool","toolCallId":%q,"toolName":"Read","state":{"status":"running","input":{"value":{},"preview":"{}","truncated":false}}}`,
						callID,
					),
				})
				_, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
					Scope: scope, RuntimeWriteId: writeID, ModelRequestId: modelRequestID,
					EventType: "agent.tool_use", PayloadJson: `{"type":"agent.tool_use","name":"Read","input":{},"evaluated_permission":"allow"}`,
					StableReasoningParts: parts,
					Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
						t,
						scope,
						writeID,
						"agent.tool_use",
						"streaming",
						draftParts...,
					)},
				})
				if err == nil {
					anchoredParts = allParts
				}
				return err
			}
			if err := writeTool("rwrite_unified_first", "call_first", test.first); err != nil {
				t.Fatalf("first anchor: %v", err)
			}
			if err := writeTool("rwrite_unified_second", "call_second", test.second); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("second anchor err = %v; want InvalidArgument", err)
			}
			var toolEvents int
			if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = $1 AND type = 'agent.tool_use'`, sessionID).Scan(&toolEvents); err != nil {
				t.Fatalf("count tool events: %v", err)
			}
			if toolEvents != 1 {
				t.Fatalf("tool events after unified %s overflow = %d; want first anchor only", test.name, toolEvents)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningLaterMergeFailureRollsBackWholeSettlement(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_reasoning_rollback", "thr_bridge_reasoning_rollback")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_reasoning_rollback", "bind_bridge_reasoning_rollback", 1, "pod_uid_reasoning_rollback")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_reasoning_rollback", "thr_bridge_reasoning_rollback", "bind_bridge_reasoning_rollback", 1, "pod_uid_reasoning_rollback")
	attachment := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_reasoning_rollback", "sevt_reasoning_rollback", []byte("image"))
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_rollback_start", ModelRequestId: "mreq_reasoning_rollback",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_reasoning_rollback"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	var usageBefore string
	if err := admin.QueryRowContext(context.Background(), `SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_reasoning_rollback'`).Scan(&usageBefore); err != nil {
		t.Fatalf("read cumulative usage before settlement: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_second_reasoning_part() RETURNS trigger AS $$
		BEGIN
			IF NEW.data_json LIKE '%provider_rollback_2%' THEN
				RAISE EXCEPTION 'forced later stable reasoning merge failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("create later merge failure function: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(), `CREATE TRIGGER fail_second_reasoning_part
		BEFORE INSERT OR UPDATE ON session_messages
		FOR EACH ROW EXECUTE FUNCTION fail_second_reasoning_part()`); err != nil {
		t.Fatalf("create later merge failure trigger: %v", err)
	}

	reasoningOne := "first"
	reasoningTwo := "second"
	_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_rollback_end", ModelRequestId: "mreq_reasoning_rollback", ModelRequestStartEventId: start.GetEventId(),
		FinishReason: "stop", UsageJson: `{"input_tokens":9,"output_tokens":5}`, ConsumedAttachmentRefs: []string{attachment.GetAttachmentRef()},
		StableReasoningParts: []*bridgev1.StableReasoningPart{
			{ReasoningPartId: "reason_rollback_1", ProviderPartId: "provider_rollback_1", PartSequence: 0, Text: reasoningOne, MetadataJson: `{}`},
			{ReasoningPartId: "reason_rollback_2", ProviderPartId: "provider_rollback_2", PartSequence: 1, Text: reasoningTwo, MetadataJson: `{}`},
		},
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
			t,
			scope,
			"mreq_reasoning_rollback",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: fmt.Sprintf(`{"type":"reasoning","providerPartId":"provider_rollback_1","providerMetadata":{},"text":%q,"truncated":false,"status":"completed"}`, reasoningOne),
			},
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: fmt.Sprintf(`{"type":"reasoning","providerPartId":"provider_rollback_2","providerMetadata":{},"text":%q,"truncated":false,"status":"completed"}`, reasoningTwo),
			},
		)},
	})
	if err == nil {
		t.Fatal("WriteRequestEnd forced later merge failure succeeded")
	}

	for label, query := range map[string]string{
		"request end":  `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = 'mreq_reasoning_rollback' AND type = 'span.model_request_end'`,
		"usage detail": `SELECT count(*) FROM request_usage_details WHERE workspace_id = 'default' AND model_request_id = 'mreq_reasoning_rollback'`,
		"message":      `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_reasoning_rollback'`,
	} {
		var count int
		if err := admin.QueryRowContext(context.Background(), query).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", label, err)
		}
		if count != 0 {
			t.Fatalf("%s rows after rollback = %d; want 0", label, count)
		}
	}
	var usageAfter string
	if err := admin.QueryRowContext(context.Background(), `SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_reasoning_rollback'`).Scan(&usageAfter); err != nil {
		t.Fatalf("read cumulative usage after rollback: %v", err)
	}
	if usageAfter != usageBefore {
		t.Fatalf("cumulative usage after rollback = %s; want unchanged %s", usageAfter, usageBefore)
	}
	if status := bridgeTransientAttachmentStatus(t, admin, attachment.GetAttachmentRef()); status != "active" {
		t.Fatalf("attachment status after rollback = %q; want active", status)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningInvalidSetsWriteNothing(t *testing.T) {
	part := func(id string, sequence int32, text string) *bridgev1.StableReasoningPart {
		return &bridgev1.StableReasoningPart{ReasoningPartId: id, ProviderPartId: "provider_" + id, PartSequence: sequence, Text: text, MetadataJson: `{}`}
	}
	tests := []struct {
		name   string
		mutate func(*bridgev1.WriteRequestEndRequest)
	}{
		{name: "seventeen_parts", mutate: func(request *bridgev1.WriteRequestEndRequest) {
			for sequence := range MaxStableReasoningPartsPerRequest + 1 {
				request.StableReasoningParts = append(request.StableReasoningParts, part(fmt.Sprintf("reason_%d", sequence), int32(sequence), "x"))
			}
		}},
		{name: "aggregate_over", mutate: func(request *bridgev1.WriteRequestEndRequest) {
			request.StableReasoningParts = []*bridgev1.StableReasoningPart{
				part("reason_1", 0, strings.Repeat("a", 1024*1024-2)),
				part("reason_2", 1, strings.Repeat("b", 1024*1024-1)),
			}
		}},
		{name: "out_of_order", mutate: func(request *bridgev1.WriteRequestEndRequest) {
			request.StableReasoningParts = []*bridgev1.StableReasoningPart{part("reason_2", 2, "x"), part("reason_1", 1, "y")}
		}},
		{name: "error_end", mutate: func(request *bridgev1.WriteRequestEndRequest) {
			request.IsError = true
			request.ErrorKind = "provider_error"
			request.StableReasoningParts = []*bridgev1.StableReasoningPart{part("reason_error", 0, "x")}
		}},
		{name: "reschedule_end", mutate: func(request *bridgev1.WriteRequestEndRequest) {
			request.IsError = true
			request.ErrorKind = "provider_error"
			request.Reschedule = &bridgev1.RequestEndReschedule{Attempt: 1, BackoffMs: 1}
			request.StableReasoningParts = []*bridgev1.StableReasoningPart{part("reason_reschedule", 0, "x")}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_reasoning_invalid_" + test.name
			threadID := "thr_reasoning_invalid_" + test.name
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_reasoning_invalid_"+test.name, 1, "pod_reasoning_invalid_"+test.name)
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_reasoning_invalid_"+test.name, 1, "pod_reasoning_invalid_"+test.name)
			modelRequestID := "mreq_reasoning_invalid_" + test.name
			start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reasoning_invalid_start_" + test.name, ModelRequestId: modelRequestID,
				EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
			})
			if err != nil {
				t.Fatalf("WriteEvent start: %v", err)
			}
			request := &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reasoning_invalid_end_" + test.name, ModelRequestId: modelRequestID,
				ModelRequestStartEventId: start.GetEventId(), FinishReason: "stop", UsageJson: `{"input_tokens":3,"output_tokens":2}`,
			}
			test.mutate(request)
			if _, err := store.WriteRequestEnd(context.Background(), request); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteRequestEnd invalid set err = %v; want InvalidArgument", err)
			}
			for label, query := range map[string]string{
				"request end": `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = $1 AND type = 'span.model_request_end'`,
				"usage":       `SELECT count(*) FROM request_usage_details WHERE workspace_id = 'default' AND model_request_id = $1`,
				"message":     `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = $1`,
			} {
				var count int
				if err := admin.QueryRowContext(context.Background(), query, modelRequestID).Scan(&count); err != nil {
					t.Fatalf("count %s rows: %v", label, err)
				}
				if count != 0 {
					t.Fatalf("%s rows after invalid set = %d; want 0", label, count)
				}
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningExactAggregateBoundCommits(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_reasoning_exact_bytes", "thr_reasoning_exact_bytes")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_reasoning_exact_bytes", "bind_reasoning_exact_bytes", 1, "pod_reasoning_exact_bytes")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_reasoning_exact_bytes", "thr_reasoning_exact_bytes", "bind_reasoning_exact_bytes", 1, "pod_reasoning_exact_bytes")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_exact_start", ModelRequestId: "mreq_reasoning_exact",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_reasoning_exact"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	firstText := strings.Repeat("a", 1024*1024-2)
	secondText := strings.Repeat("b", 1024*1024-2)
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_reasoning_exact_end", ModelRequestId: "mreq_reasoning_exact", ModelRequestStartEventId: start.GetEventId(),
		FinishReason: "stop", UsageJson: `{}`,
		StableReasoningParts: []*bridgev1.StableReasoningPart{
			{ReasoningPartId: "reason_exact_1", ProviderPartId: "provider_exact_1", PartSequence: 0, Text: firstText, MetadataJson: `{}`},
			{ReasoningPartId: "reason_exact_2", ProviderPartId: "provider_exact_2", PartSequence: 1, Text: secondText, MetadataJson: `{}`},
		},
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
			t,
			scope,
			"mreq_reasoning_exact",
			"completed",
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: fmt.Sprintf(`{"type":"reasoning","providerPartId":"provider_exact_1","providerMetadata":{},"text":%q,"truncated":false,"status":"completed"}`, firstText),
			},
			bridgeRuntimePartDraftForTest{
				kind: "reasoning",
				json: fmt.Sprintf(`{"type":"reasoning","providerPartId":"provider_exact_2","providerMetadata":{},"text":%q,"truncated":false,"status":"completed"}`, secondText),
			},
		)},
	}); err != nil {
		t.Fatalf("WriteRequestEnd exact aggregate: %v", err)
	}
	var messageCount int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = 'mreq_reasoning_exact'`).Scan(&messageCount); err != nil {
		t.Fatalf("count exact-bound assistant rows: %v", err)
	}
	if messageCount != 1 {
		t.Fatalf("exact-bound assistant rows = %d; want 1", messageCount)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndPersistsRescheduleDispositionAndCeiling(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_retry", "thr_bridge_request_retry")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_retry", "bind_bridge_request_retry", 1, "pod_uid_request_retry")
	now := time.Date(2026, time.July, 13, 12, 0, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.RuntimeBindingTokenHMACKey = []byte("request-reschedule-load-key-32b")
	store.Clock = func() time.Time { return now }
	store.ProviderRescheduleBudget = 1
	store.CompactionRescheduleBudget = 2
	scope := bridgeAPIScope("sesn_bridge_request_retry", "thr_bridge_request_retry", "bind_bridge_request_retry", 1, "pod_uid_request_retry")

	writeStart := func(writeID string, requestID string) string {
		t.Helper()
		start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope:          scope,
			RuntimeWriteId: writeID,
			ModelRequestId: requestID,
			EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
			PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + requestID + `"}`,
		})
		if err != nil {
			t.Fatalf("WriteEvent start %s: %v", requestID, err)
		}
		return start.GetEventId()
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_request_retry_end_1",
		ModelRequestId:           "mreq_bridge_request_retry_1",
		ModelRequestStartEventId: writeStart("rwrite_bridge_request_retry_start_1", "mreq_bridge_request_retry_1"),
		RequestKind:              "agent_provider_request",
		FinishReason:             "error",
		IsError:                  true,
		ErrorKind:                "provider_error",
		UsageJson:                `{}`,
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt:   1,
			Deadline:  now.Add(5 * time.Minute).Format(time.RFC3339Nano),
			BackoffMs: int64((5 * time.Minute) / time.Millisecond),
		},
	}
	response, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd accepted reschedule: %v", err)
	}
	disposition := response.GetDeclaration().GetReceipts()[0].GetRequestReschedule()
	if disposition.GetDisposition() != bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED ||
		disposition.GetAttempt() != 1 ||
		disposition.GetEffectiveDeadline() != now.Add(120*time.Second).Format(time.RFC3339Nano) {
		t.Fatalf("accepted disposition = %+v; want attempt 1 clamped to 120 seconds", disposition)
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd accepted replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		replay.GetDeclaration().GetReceipts()[0].GetRequestReschedule().GetEffectiveDeadline() != disposition.GetEffectiveDeadline() {
		t.Fatalf("replay = %+v; want duplicate with stored disposition", replay)
	}
	if _, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
		Scope: scope, RuntimeInputId: "rin_bridge_request_retry_load",
	}); err != nil {
		t.Fatalf("LoadContext after accepted reschedule: %v", err)
	}

	var providerAttempts, compactionAttempts int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_attempts, compaction_attempts
		   FROM session_turn_retries
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_request_retry'
		    AND session_thread_id = 'thr_bridge_request_retry'`).Scan(&providerAttempts, &compactionAttempts); err != nil {
		t.Fatalf("read retry counters: %v", err)
	}
	if providerAttempts != 1 || compactionAttempts != 0 {
		t.Fatalf("retry counters = %d/%d; want 1/0", providerAttempts, compactionAttempts)
	}
	var sessionStatus, threadStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT s.status, t.status
		   FROM sessions s
		   JOIN session_threads t ON t.workspace_id = s.workspace_id AND t.session_id = s.id
		  WHERE s.workspace_id = 'default' AND s.id = 'sesn_bridge_request_retry' AND t.id = 'thr_bridge_request_retry'`).Scan(&sessionStatus, &threadStatus); err != nil {
		t.Fatalf("read rescheduled status: %v", err)
	}
	if sessionStatus != "rescheduling" || threadStatus != "rescheduling" {
		t.Fatalf("rescheduled status = %q/%q; want rescheduling/rescheduling", sessionStatus, threadStatus)
	}

	mismatch := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_request_retry_end_2",
		ModelRequestId:           "mreq_bridge_request_retry_2",
		ModelRequestStartEventId: writeStart("rwrite_bridge_request_retry_start_2", "mreq_bridge_request_retry_2"),
		RequestKind:              "agent_provider_request",
		FinishReason:             "error",
		IsError:                  true,
		ErrorKind:                "provider_error",
		UsageJson:                `{}`,
		Reschedule:               &bridgev1.RequestEndReschedule{Attempt: 3, Deadline: now.Format(time.RFC3339Nano)},
	}
	mismatchResponse, err := store.WriteRequestEnd(context.Background(), mismatch)
	if err != nil {
		t.Fatalf("WriteRequestEnd attempt mismatch: %v", err)
	}
	if got := mismatchResponse.GetDeclaration().GetReceipts()[0].GetRequestReschedule(); got.GetDisposition() != bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_DENIED_ATTEMPT_MISMATCH {
		t.Fatalf("attempt mismatch disposition = %+v", got)
	}

	exhausted := proto.Clone(mismatch).(*bridgev1.WriteRequestEndRequest)
	exhausted.RuntimeWriteId = "rwrite_bridge_request_retry_end_3"
	exhausted.ModelRequestId = "mreq_bridge_request_retry_3"
	exhausted.ModelRequestStartEventId = writeStart("rwrite_bridge_request_retry_start_3", "mreq_bridge_request_retry_3")
	exhausted.Reschedule.Attempt = 2
	exhaustedResponse, err := store.WriteRequestEnd(context.Background(), exhausted)
	if err != nil {
		t.Fatalf("WriteRequestEnd exhausted: %v", err)
	}
	if got := exhaustedResponse.GetDeclaration().GetReceipts()[0].GetRequestReschedule(); got.GetDisposition() != bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_DENIED_BUDGET_EXHAUSTED {
		t.Fatalf("budget exhausted disposition = %+v", got)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_attempts FROM session_turn_retries
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_request_retry' AND session_thread_id = 'thr_bridge_request_retry'`).Scan(&providerAttempts); err != nil {
		t.Fatalf("read retry counter after denials: %v", err)
	}
	if providerAttempts != 1 {
		t.Fatalf("provider attempts after denials = %d; want 1", providerAttempts)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsDivergentCloseAfterExistingTerminal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_divergent_close", "thr_bridge_request_divergent_close")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_divergent_close", "bind_bridge_request_divergent_close", 1, "pod_uid_request_divergent_close")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_request_divergent_close", "thr_bridge_request_divergent_close", "bind_bridge_request_divergent_close", 1, "pod_uid_request_divergent_close")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_stale_start", ModelRequestId: "mreq_bridge_stale",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_stale"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	base := &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_stale_winner", ModelRequestId: "mreq_bridge_stale",
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "error", IsError: true,
		ErrorKind: "runtime_pod_lost", UsageJson: `{}`,
	}
	if _, err := store.WriteRequestEnd(context.Background(), base); err != nil {
		t.Fatalf("WriteRequestEnd winner: %v", err)
	}
	loser := proto.Clone(base).(*bridgev1.WriteRequestEndRequest)
	loser.RuntimeWriteId = "rwrite_bridge_stale_loser"
	loser.ErrorKind = "provider_error"
	loser.Reschedule = &bridgev1.RequestEndReschedule{Attempt: 1, Deadline: time.Now().UTC().Format(time.RFC3339Nano)}
	if _, err := store.WriteRequestEnd(context.Background(), loser); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("WriteRequestEnd divergent loser err = %v; want AlreadyExists", err)
	}
	var endCount, retryRows, operationRows int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_request_divergent_close' AND type = 'span.model_request_end'`).Scan(&endCount); err != nil {
		t.Fatalf("count terminal events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_turn_retries WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_request_divergent_close'`).Scan(&retryRows); err != nil {
		t.Fatalf("count retry rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_request_divergent_close' AND operation = 'write_request_end'`).Scan(&operationRows); err != nil {
		t.Fatalf("count request-end operations: %v", err)
	}
	if endCount != 1 || retryRows != 0 || operationRows != 1 {
		t.Fatalf("divergent terminal effects = ends %d retry rows %d operations %d; want 1/0/1", endCount, retryRows, operationRows)
	}
}

func TestPostgreSQLBridgeAPIStoreStableReasoningTerminalRaceIsSymmetric(t *testing.T) {
	type fixture struct {
		runtime        *sql.DB
		admin          *sql.DB
		store          *PostgreSQLBridgeAPIStore
		scope          *bridgev1.RuntimeScope
		startEventID   string
		sessionID      string
		modelRequestID string
		binding        runtimeBindingForDelivery
	}
	newFixture := func(t *testing.T, suffix string) fixture {
		t.Helper()
		runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
		sessionID := "sesn_reasoning_race_" + suffix
		threadID := "thr_reasoning_race_" + suffix
		bindingID := "bind_reasoning_race_" + suffix
		podUID := "pod_reasoning_race_" + suffix
		modelRequestID := "mreq_reasoning_race_" + suffix
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
		scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID)
		start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope: scope, RuntimeWriteId: "rwrite_reasoning_race_start_" + suffix, ModelRequestId: modelRequestID,
			EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `","request_kind":"agent_provider_request"}`,
		})
		if err != nil {
			t.Fatalf("WriteEvent start: %v", err)
		}
		return fixture{
			runtime: runtime, admin: admin, store: store, scope: scope, startEventID: start.GetEventId(), sessionID: sessionID, modelRequestID: modelRequestID,
			binding: runtimeBindingForDelivery{BindingID: bindingID, BindingGeneration: 1, PodUID: podUID},
		}
	}
	podClose := func(f fixture, writeID string) *bridgev1.WriteRequestEndRequest {
		return &bridgev1.WriteRequestEndRequest{
			Scope: f.scope, RuntimeWriteId: writeID, ModelRequestId: f.modelRequestID, ModelRequestStartEventId: f.startEventID,
			FinishReason: "stop", UsageJson: `{"input_tokens":4,"output_tokens":2}`,
			StableReasoningParts: []*bridgev1.StableReasoningPart{
				{ReasoningPartId: "reason_race_1", ProviderPartId: "provider_race_1", PartSequence: 0, Text: "first", MetadataJson: `{}`},
				{ReasoningPartId: "reason_race_2", ProviderPartId: "provider_race_2", PartSequence: 1, Text: "second", MetadataJson: `{}`},
			},
			Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRequestEndDraftForTest(
				t,
				f.scope,
				f.modelRequestID,
				"completed",
				bridgeRuntimePartDraftForTest{
					kind: "reasoning",
					json: `{"type":"reasoning","providerPartId":"provider_race_1","providerMetadata":{},"text":"first","truncated":false,"status":"completed"}`,
				},
				bridgeRuntimePartDraftForTest{
					kind: "reasoning",
					json: `{"type":"reasoning","providerPartId":"provider_race_2","providerMetadata":{},"text":"second","truncated":false,"status":"completed"}`,
				},
			)},
		}
	}
	repairClose := func(t *testing.T, f fixture) bool {
		t.Helper()
		inserted := false
		client := dbconnect.NewClientForTesting(f.runtime)
		if err := client.WithWorkspaceTx(context.Background(), "default", "test.reasoning_terminal_repair", func(tx *dbconnect.Tx) error {
			starts, err := runtimePodLostOpenRequestStartsTx(context.Background(), tx, "default", f.sessionID, []string{f.scope.GetSessionThreadId()})
			if err != nil {
				return err
			}
			for _, start := range starts {
				won, err := insertRuntimePodLostRequestEndTx(context.Background(), tx, "default", f.sessionID, f.binding, start, time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC))
				if err != nil {
					return err
				}
				inserted = inserted || won
			}
			return nil
		}); err != nil {
			t.Fatalf("runtime pod-loss request repair: %v", err)
		}
		return inserted
	}
	assertCounts := func(t *testing.T, f fixture, wantMessages int, wantOperations int) {
		t.Helper()
		var endCount, usageCount, messageCount, operationCount, retryCount, rescheduledCount int
		queries := []struct {
			target *int
			query  string
		}{
			{&endCount, `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = $1 AND type = 'span.model_request_end'`},
			{&usageCount, `SELECT count(*) FROM request_usage_details WHERE workspace_id = 'default' AND model_request_id = $1`},
			{&messageCount, `SELECT count(*) FROM session_messages WHERE workspace_id = 'default' AND model_request_id = $1`},
			{&operationCount, `SELECT count(*) FROM session_bridge_operations WHERE workspace_id = 'default' AND operation = 'write_request_end' AND idempotency_key = $1`},
			{&retryCount, `SELECT count(*) FROM session_turn_retries WHERE workspace_id = 'default' AND session_id = $1`},
			{&rescheduledCount, `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND model_request_id = $1 AND type = 'session.status_rescheduled'`},
		}
		for _, query := range queries {
			argument := f.modelRequestID
			if query.target == &retryCount {
				argument = f.sessionID
			}
			if err := f.admin.QueryRowContext(context.Background(), query.query, argument).Scan(query.target); err != nil {
				t.Fatalf("read terminal race count: %v", err)
			}
		}
		if endCount != 1 || usageCount != 1 || messageCount != wantMessages || operationCount != wantOperations || retryCount != 0 || rescheduledCount != 0 {
			t.Fatalf("terminal race counts end/usage/message/operation/retry/rescheduled = %d/%d/%d/%d/%d/%d; want 1/1/%d/%d/0/0", endCount, usageCount, messageCount, operationCount, retryCount, rescheduledCount, wantMessages, wantOperations)
		}
	}

	t.Run("repair first", func(t *testing.T) {
		f := newFixture(t, "repair_first")
		if !repairClose(t, f) {
			t.Fatal("repair-first close did not win")
		}
		if _, err := f.store.WriteRequestEnd(context.Background(), podClose(f, "rwrite_reasoning_race_pod_loser")); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("late pod close err = %v; want AlreadyExists", err)
		}
		assertCounts(t, f, 0, 0)
	})

	t.Run("pod first", func(t *testing.T) {
		f := newFixture(t, "pod_first")
		request := podClose(f, "rwrite_reasoning_race_pod_winner")
		if _, err := f.store.WriteRequestEnd(context.Background(), request); err != nil {
			t.Fatalf("pod-first close: %v", err)
		}
		if repairClose(t, f) {
			t.Fatal("late repair inserted a second request end")
		}
		assertCounts(t, f, 1, 1)
		var dataJSON string
		if err := f.admin.QueryRowContext(context.Background(), `SELECT data_json FROM session_messages WHERE workspace_id = 'default' AND model_request_id = $1`, f.modelRequestID).Scan(&dataJSON); err != nil {
			t.Fatalf("read pod-first reasoning row: %v", err)
		}
		if strings.Count(dataJSON, `"type":"reasoning"`) != 2 {
			t.Fatalf("pod-first reasoning row = %s; want two parts", dataJSON)
		}
		divergent := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
		divergent.StableReasoningParts[1].Text = "divergent"
		if _, err := f.store.WriteRequestEnd(context.Background(), divergent); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("pod-first divergent replay err = %v; want AlreadyExists", err)
		}
		assertCounts(t, f, 1, 1)
	})
}

func TestParseBridgeUsageRejectsMalformedCountersAndArithmetic(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
	}{
		{name: "null object", raw: `null`},
		{name: "unknown field", raw: `{"input_tokens":1,"unknown":1}`},
		{name: "negative", raw: `{"input_tokens":-1}`},
		{name: "fractional", raw: `{"input_tokens":1.5}`},
		{name: "string", raw: `{"input_tokens":"1"}`},
		{name: "cache exceeds input", raw: `{"input_tokens":1,"cache_read_input_tokens":2}`},
		{name: "explicit uncached mismatch", raw: `{"input_tokens":4,"input_uncached_tokens":1,"cache_read_input_tokens":1}`},
		{name: "reasoning exceeds output", raw: `{"output_tokens":1,"reasoning_output_tokens":2}`},
		{name: "total mismatch", raw: `{"input_tokens":2,"output_tokens":3,"total_tokens":4}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseBridgeUsage(test.raw); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("parseBridgeUsage(%s) err = %v; want InvalidArgument", test.raw, err)
			}
		})
	}

	usage, err := parseBridgeUsage(`{"input_tokens":11,"cache_read_input_tokens":3,"cache_creation_input_tokens":2,"output_tokens":7,"reasoning_output_tokens":1,"total_tokens":18}`)
	if err != nil {
		t.Fatalf("parseBridgeUsage valid: %v", err)
	}
	if usage.InputUncached != 6 || optionalInt64Value(usage.Total) != 18 {
		t.Fatalf("usage = %+v; want derived uncached=6 and total=18", usage)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRecordsCompactionRequestKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_compaction_usage", "thr_bridge_compaction_usage")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_compaction_usage", "bind_bridge_compaction_usage", 1, "pod_uid_compaction_usage")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_compaction_usage", "thr_bridge_compaction_usage", "bind_bridge_compaction_usage", 1, "pod_uid_compaction_usage")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_compaction_usage_start",
		ModelRequestId: "mreq_bridge_compaction_usage",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "compaction_summary",
		PayloadJson:    `{"type":"span.model_request_start","model_request_id":"mreq_bridge_compaction_usage"}`,
		SessionVisible: false,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	const (
		modelRequestID = "mreq_bridge_compaction_usage"
		checkpointText = "<conversation-checkpoint><summary>Older turns were compacted.</summary><recent-context></recent-context></conversation-checkpoint>"
	)
	checkpointLocalID := stableRuntimeID(
		"runtime_message_draft",
		"default",
		"sesn_bridge_compaction_usage",
		"thr_bridge_compaction_usage",
		"agent.thread_context_compacted",
		modelRequestID,
		runtimeDraftKindToken(bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT),
		"0",
	)
	compactedThrough := int64(0)
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_compaction_usage_end",
		ModelRequestId:           modelRequestID,
		ModelRequestStartEventId: start.GetEventId(),
		RequestKind:              "compaction_summary",
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":3,"output_tokens":2}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{{
			RuntimeLocalId:  checkpointLocalID,
			SourceKind:      "agent.thread_context_compacted",
			SourceId:        modelRequestID,
			DraftKind:       bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPACTION_CHECKPOINT,
			MessageInfoJson: `{"role":"user","origin":"runtime","status":"completed"}`,
			Parts: []*bridgev1.RuntimePartDraft{{
				RuntimeLocalPartId: stableRuntimeID("runtime_message_part_draft", checkpointLocalID, "text", "0"),
				PartKind:           "text",
				PartJson:           `{"type":"text","text":"` + checkpointText + `","truncated":false,"status":"completed"}`,
			}},
		}},
		CompactedThroughMessageSequence: &compactedThrough,
		CompactionEventPayloadJson:      `{"type":"agent.thread_context_compacted","summary":"Older turns were compacted.","recent_context":[]}`,
	}
	response, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd compaction: %v", err)
	}
	receipts := response.GetDeclaration().GetReceipts()
	if len(receipts) != 1 || len(receipts[0].GetEvents()) != 2 || len(receipts[0].GetMessages()) != 1 {
		t.Fatalf("compaction receipt = %+v; want request-end event, compaction event, and checkpoint message", response.GetDeclaration())
	}
	receipt := receipts[0]
	checkpoint := receipt.GetMessages()[0]
	if receipt.GetCompactedThroughMessageSequence() != 0 ||
		checkpoint.GetRuntimeLocalId() != checkpointLocalID ||
		checkpoint.GetMessageSequence() != 1 ||
		checkpoint.GetOwningEventId() != receipt.GetEvents()[1].GetEventId() {
		t.Fatalf("compaction receipt = %+v; want boundary 0 and DB-stamped checkpoint sequence 1 owned by the compaction event", receipt)
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil ||
		replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE ||
		!proto.Equal(replay.GetDeclaration(), response.GetDeclaration()) {
		t.Fatalf("WriteRequestEnd compaction replay = %+v err=%v; want duplicate with the original receipt", replay, err)
	}
	var requestKind string
	var payloadJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT u.request_kind, e.payload_json
		   FROM request_usage_details u
		   JOIN session_events e
		     ON e.workspace_id = u.workspace_id
		    AND e.session_id = u.session_id
		    AND e.model_request_id = u.model_request_id
		  WHERE u.workspace_id = 'default'
		    AND u.session_id = 'sesn_bridge_compaction_usage'
		    AND u.model_request_id = 'mreq_bridge_compaction_usage'
		    AND e.type = 'span.model_request_end'`).Scan(&requestKind, &payloadJSON); err != nil {
		t.Fatalf("read compaction usage: %v", err)
	}
	if requestKind != "compaction_summary" || testJSONPathString(t, payloadJSON, "request_kind") != "compaction_summary" {
		t.Fatalf("request kind detail/event = %q/%s; want compaction_summary", requestKind, payloadJSON)
	}
	var checkpointCount, compactionEventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_compaction_usage'
		    AND kind = 'compaction'
		    AND sequence = 1`).Scan(&checkpointCount); err != nil {
		t.Fatalf("count compaction checkpoint: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_compaction_usage'
		    AND type = 'agent.thread_context_compacted'`).Scan(&compactionEventCount); err != nil {
		t.Fatalf("count compaction event: %v", err)
	}
	if checkpointCount != 1 || compactionEventCount != 1 {
		t.Fatalf("atomic compaction rows = checkpoint %d event %d; want one each after replay", checkpointCount, compactionEventCount)
	}
	invalid := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
	invalid.RuntimeWriteId = "rwrite_bridge_compaction_usage_invalid"
	invalid.RequestKind = "model"
	if _, err := store.WriteRequestEnd(context.Background(), invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("WriteRequestEnd invalid request_kind err = %v; want InvalidArgument", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsRequestKindMismatch(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_kind_mismatch", "thr_bridge_request_kind_mismatch")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_kind_mismatch", "bind_bridge_request_kind_mismatch", 1, "pod_bridge_request_kind_mismatch")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_request_kind_mismatch", "thr_bridge_request_kind_mismatch", "bind_bridge_request_kind_mismatch", 1, "pod_bridge_request_kind_mismatch")
	start := seedBridgeAPIRequestStart(t, store, scope, "rwrite_bridge_request_kind_mismatch_start", "mreq_bridge_request_kind_mismatch", "compaction_summary", 0)

	_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_request_kind_mismatch_end",
		ModelRequestId: "mreq_bridge_request_kind_mismatch", ModelRequestStartEventId: start.GetEventId(),
		RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("WriteRequestEnd mismatched request kind err = %v; want FailedPrecondition", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndScopesDuplicateCheckToThread(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID      = "sesn_bridge_request_end_thread_scope"
		mainThreadID   = "thr_bridge_request_end_thread_scope_main"
		childThreadID  = "thr_bridge_request_end_thread_scope_child"
		modelRequestID = "mreq_bridge_request_end_shared"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, mainThreadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, mainThreadID, childThreadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_request_end_thread_scope", 1, "pod_bridge_request_end_thread_scope")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	mainScope := bridgeAPIScope(sessionID, mainThreadID, "bind_bridge_request_end_thread_scope", 1, "pod_bridge_request_end_thread_scope")
	childScope := bridgeAPIScope(sessionID, childThreadID, "bind_bridge_request_end_thread_scope", 1, "pod_bridge_request_end_thread_scope")
	mainStart := seedBridgeAPIRequestStart(t, store, mainScope, "rwrite_request_end_thread_scope_main_start", modelRequestID, "agent_provider_request", 0)
	childStart := seedBridgeAPIRequestStart(t, store, childScope, "rwrite_request_end_thread_scope_child_start", modelRequestID, "agent_provider_request", 0)

	for _, request := range []*bridgev1.WriteRequestEndRequest{
		{
			Scope: mainScope, RuntimeWriteId: "rwrite_request_end_thread_scope_main_end", ModelRequestId: modelRequestID,
			ModelRequestStartEventId: mainStart.GetEventId(), RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
		},
		{
			Scope: childScope, RuntimeWriteId: "rwrite_request_end_thread_scope_child_end", ModelRequestId: modelRequestID,
			ModelRequestStartEventId: childStart.GetEventId(), RequestKind: "agent_provider_request", FinishReason: "stop", UsageJson: `{}`,
		},
	} {
		if _, err := store.WriteRequestEnd(context.Background(), request); err != nil {
			t.Fatalf("WriteRequestEnd thread %s: %v", request.GetScope().GetSessionThreadId(), err)
		}
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsCompactionAttachmentConsumption(t *testing.T) {
	for _, test := range []struct {
		name      string
		transient []string
		files     []*bridgev1.FileAttachmentPair
	}{
		{
			name:      "transient attachment",
			transient: []string{"att_bridge_compaction_forbidden"},
		},
		{
			name: "file attachment",
			files: []*bridgev1.FileAttachmentPair{{
				SourceEventId: "sevt_bridge_compaction_forbidden",
				FileId:        "file_bridge_compaction_forbidden",
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_bridge_compaction_attachment_" + suffix
			threadID := "thr_bridge_compaction_attachment_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_compaction_attachment", 1, "pod_uid_compaction_attachment")

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_compaction_attachment", 1, "pod_uid_compaction_attachment")
			start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:          scope,
				RuntimeWriteId: "rwrite_bridge_compaction_attachment_start_" + suffix,
				ModelRequestId: "mreq_bridge_compaction_attachment_" + suffix,
				EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "compaction_summary",
				PayloadJson: `{"type":"span.model_request_start"}`,
			})
			if err != nil {
				t.Fatalf("WriteEvent start: %v", err)
			}
			_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
				Scope:                    scope,
				RuntimeWriteId:           "rwrite_bridge_compaction_attachment_end_" + suffix,
				ModelRequestId:           "mreq_bridge_compaction_attachment_" + suffix,
				ModelRequestStartEventId: start.GetEventId(),
				RequestKind:              "compaction_summary",
				FinishReason:             "stop",
				UsageJson:                `{"input_tokens":1,"output_tokens":1}`,
				ConsumedAttachmentRefs:   test.transient,
				ConsumedFileAttachments:  test.files,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteRequestEnd compaction attachment err = %v; want InvalidArgument", err)
			}
			var terminalCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*)
				   FROM session_events
				  WHERE workspace_id = 'default'
				    AND session_id = $1
				    AND type = 'span.model_request_end'`,
				sessionID,
			).Scan(&terminalCount); err != nil {
				t.Fatalf("count request-end events: %v", err)
			}
			if terminalCount != 0 {
				t.Fatalf("request-end events after rejected compaction attachment = %d; want zero", terminalCount)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndValidatesErrorKind(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_request_error_kind", "thr_bridge_request_error_kind")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_request_error_kind", "bind_bridge_request_error_kind", 1, "pod_uid_request_error_kind")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          bridgeAPIScope("sesn_bridge_request_error_kind", "thr_bridge_request_error_kind", "bind_bridge_request_error_kind", 1, "pod_uid_request_error_kind"),
		RuntimeWriteId: "rwrite_bridge_request_error_start",
		ModelRequestId: "mreq_bridge_request_error",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson:    `{"type":"span.model_request_start","model_request_id":"mreq_bridge_request_error"}`,
		SessionVisible: false,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	base := &bridgev1.WriteRequestEndRequest{
		Scope:                    bridgeAPIScope("sesn_bridge_request_error_kind", "thr_bridge_request_error_kind", "bind_bridge_request_error_kind", 1, "pod_uid_request_error_kind"),
		RuntimeWriteId:           "rwrite_bridge_request_error_end",
		ModelRequestId:           "mreq_bridge_request_error",
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "error",
		IsError:                  true,
		ErrorKind:                "provider_error",
		UsageJson:                `{}`,
	}
	if _, err := store.WriteRequestEnd(context.Background(), base); err != nil {
		t.Fatalf("WriteRequestEnd with provider_error: %v", err)
	}
	var payload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_request_error_kind'
		    AND type = 'span.model_request_end'`).Scan(&payload); err != nil {
		t.Fatalf("read request end payload: %v", err)
	}
	if got := testJSONPathString(t, payload, "error_kind"); got != "provider_error" {
		t.Fatalf("error_kind = %q; want provider_error", got)
	}

	tests := []struct {
		name   string
		mutate func(*bridgev1.WriteRequestEndRequest)
	}{
		{
			name: "missing error kind on error",
			mutate: func(request *bridgev1.WriteRequestEndRequest) {
				request.RuntimeWriteId = "rwrite_bridge_request_error_missing_kind"
				request.ErrorKind = ""
			},
		},
		{
			name: "invalid error kind",
			mutate: func(request *bridgev1.WriteRequestEndRequest) {
				request.RuntimeWriteId = "rwrite_bridge_request_error_bad_kind"
				request.ErrorKind = "request_too_large"
			},
		},
		{
			name: "error kind on successful request",
			mutate: func(request *bridgev1.WriteRequestEndRequest) {
				request.RuntimeWriteId = "rwrite_bridge_request_error_success_kind"
				request.IsError = false
				request.ErrorKind = "provider_error"
				request.FinishReason = "stop"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := proto.Clone(base).(*bridgev1.WriteRequestEndRequest)
			test.mutate(request)
			if _, err := store.WriteRequestEnd(context.Background(), request); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteRequestEnd err = %v; want InvalidArgument", err)
			}
		})
	}
}

func TestValidateRequestEndErrorKindAcceptsContractKinds(t *testing.T) {
	for _, errorKind := range []string{
		"provider_error",
		"gateway_stream_error",
		"gateway_protocol_error",
		"runtime_interrupted",
		"runtime_persistence_error",
		"runtime_semantic_error",
		"runtime_pod_lost",
	} {
		t.Run(errorKind, func(t *testing.T) {
			request := &bridgev1.WriteRequestEndRequest{IsError: true, ErrorKind: errorKind}
			if err := validateRequestEndErrorKind(request); err != nil {
				t.Fatalf("validateRequestEndErrorKind(%q): %v", errorKind, err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreFinishIdlePersistsStatusEventAndRuntimeStatus(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_finish_idle", "thr_bridge_finish_idle")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_finish_idle", "thr_bridge_finish_idle", "thr_bridge_finish_idle_sender")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_finish_idle", "bind_bridge_finish_idle", 1, "pod_uid_finish_idle")
	seedBridgeAPIEvent(
		t,
		admin,
		"default",
		"sesn_bridge_finish_idle",
		"thr_bridge_finish_idle_sender",
		"evt_bridge_finish_idle_pending_mail",
		1,
		"agent.thread_message_sent",
		bridgeInterAgentSentEventJSON(
			t,
			"delivery_bridge_finish_idle_pending",
			"thr_bridge_finish_idle_sender",
			"thr_bridge_finish_idle",
			"",
			"sevt_bridge_finish_idle_pending",
			bridgeRuntimeNotificationMessageJSON(
				t,
				"sesn_bridge_finish_idle",
				"msg_bridge_finish_idle_pending",
				completionMailEnvelope("main", "sender", "pending"),
			),
		),
	)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, running_since, active_seconds_total,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_finish_idle', 'running', '2026-01-01T00:00:15Z', 5,
			'bind_bridge_finish_idle', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:15Z'
		)`); err != nil {
		t.Fatalf("seed running runtime status: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_turn_retries (
			workspace_id, session_id, session_thread_id, provider_attempts, compaction_attempts, updated_at
		) VALUES ('default', 'sesn_bridge_finish_idle', 'thr_bridge_finish_idle', 2, 1, '2026-01-01T00:00:15Z')`); err != nil {
		t.Fatalf("seed turn retry counters: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	request := bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_finish_idle", "thr_bridge_finish_idle", "bind_bridge_finish_idle", 1, "pod_uid_finish_idle"),
		"evt_bridge_finish_idle_running",
		`{"type":"end_turn"}`,
	)
	response, err := finishIdleWithStagedCaptureForTest(t, admin, store, request)
	if err != nil {
		t.Fatalf("FinishIdle: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("ack = %s; want committed", response.GetAck().GetStatus())
	}
	if response.GetAck().GetRuntimeWriteId() != request.GetDurableTurnId() {
		t.Fatalf("finish-idle committed ack write id = %q; want %q", response.GetAck().GetRuntimeWriteId(), request.GetDurableTurnId())
	}
	replay, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if replay.GetAck().GetRuntimeWriteId() != request.GetDurableTurnId() {
		t.Fatalf("finish-idle duplicate ack write id = %q; want %q", replay.GetAck().GetRuntimeWriteId(), request.GetDurableTurnId())
	}

	var eventID string
	var payloadJSON string
	var streamPosition int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json, latest_stream_position
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_finish_idle'
		    AND type = 'session.status_idle'`).Scan(&eventID, &payloadJSON, &streamPosition); err != nil {
		t.Fatalf("read idle event: %v", err)
	}
	if eventID == "" || streamPosition == 0 {
		t.Fatalf("idle event eventID/stream = %q/%d; want durable stream event", eventID, streamPosition)
	}
	if got := testJSONPathString(t, payloadJSON, "stop_reason.type"); got != "end_turn" {
		t.Fatalf("idle stop reason = %q; want end_turn", got)
	}
	var statusValue string
	var statusEventID string
	var idleSince string
	var cleanupAfter string
	var runningSince sql.NullString
	var activeSeconds float64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status, status_event_id, idle_since, cleanup_after, running_since, active_seconds_total
		   FROM session_runtime_status
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_finish_idle'`).Scan(&statusValue, &statusEventID, &idleSince, &cleanupAfter, &runningSince, &activeSeconds); err != nil {
		t.Fatalf("read runtime status: %v", err)
	}
	if statusValue != "idle" || statusEventID != eventID || idleSince != "2026-01-01T00:00:45Z" || cleanupAfter != "2026-01-01T00:30:45Z" {
		t.Fatalf("runtime status = %q/%q/%q/%q; want idle/%q/idleSince/cleanup_after", statusValue, statusEventID, idleSince, cleanupAfter, eventID)
	}
	if runningSince.Valid || activeSeconds != 35 {
		t.Fatalf("idle running/active stats = %v/%v; want null/35", runningSince, activeSeconds)
	}
	var eventCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_finish_idle' AND type = 'session.status_idle'`).Scan(&eventCount); err != nil {
		t.Fatalf("read idle event count: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("idle event count after replay = %d; want 1", eventCount)
	}
	var completionWakeCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND dedupe_key = 'runtime_input:default:sesn_bridge_finish_idle:agent_mail:delivery_bridge_finish_idle_pending'
		    AND status IN ('pending', 'leased')`).Scan(&completionWakeCount); err != nil {
		t.Fatalf("read main-thread completion wake: %v", err)
	}
	if completionWakeCount != 1 {
		t.Fatalf("main-thread completion wake count = %d; want 1", completionWakeCount)
	}
	var providerAttempts, compactionAttempts int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_attempts, compaction_attempts
		   FROM session_turn_retries
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_finish_idle'
		    AND session_thread_id = 'thr_bridge_finish_idle'`).Scan(&providerAttempts, &compactionAttempts); err != nil {
		t.Fatalf("read reset turn retry counters: %v", err)
	}
	if providerAttempts != 0 || compactionAttempts != 0 {
		t.Fatalf("turn retry counters = %d/%d; want 0/0 after end_turn", providerAttempts, compactionAttempts)
	}

	conflict := proto.Clone(request).(*bridgev1.FinishIdleRequest)
	conflict.StopReasonJson = `{"type":"retries_exhausted"}`
	if _, err := store.FinishIdle(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting FinishIdle err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreFinishIdleAdoptsCaptureAndMailInOneTransaction(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_finish_idle_adoption_atomic"
		threadID  = "thr_finish_idle_adoption_atomic"
		senderID  = "thr_finish_idle_adoption_sender"
		bindingID = "bind_finish_idle_adoption_atomic"
		writeID   = "evt_finish_idle_adoption_atomic"
		source    = "outputs/report.txt"
		blobKey   = "capture/finish-idle-adoption/report.txt"
		oldFileID = "file_finish_idle_adoption_old"
		oldObject = "fobj_finish_idle_adoption_old"
		oldBlob   = "capture/finish-idle-adoption/old-report.txt"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, senderID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_finish_idle_adoption_atomic")
	seedBridgeAPIEvent(t, admin, "default", sessionID, senderID, "evt_finish_idle_adoption_mail", 1,
		"agent.thread_message_sent", bridgeInterAgentSentEventJSON(
			t, "delivery_finish_idle_adoption", senderID, threadID, "", "sevt_finish_idle_adoption_spawn",
			bridgeRuntimeNotificationMessageJSON(t, sessionID, "msg_finish_idle_adoption", completionMailEnvelope("main", "sender", "done")),
		))
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return now }
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_finish_idle_adoption_atomic")
	request := bridgeAPIFinishIdleRequest(t, admin, scope, writeID, `{"type":"end_turn"}`)
	oldCapturedAt := now.Add(-time.Hour)
	if _, err := admin.Exec(`INSERT INTO file_objects (
		object_id, workspace_id, blob_key, size_bytes, sha256, created_at
	) VALUES ($1,'default',$2,7,$3,$4)`, oldObject, oldBlob, strings.Repeat("d", 64), oldCapturedAt); err != nil {
		t.Fatalf("seed previous capture object: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO files (
		file_id, workspace_id, object_id, filename, mime_type, downloadable, scope_type, scope_id, created_at
	) VALUES ($1,'default',$2,'old-report.txt','text/plain',true,'session',$3,$4)`, oldFileID, oldObject, sessionID, oldCapturedAt); err != nil {
		t.Fatalf("seed previous captured file: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO session_output_captures (
		workspace_id, session_id, source_path, last_file_id, last_size_bytes, last_sha256,
		last_captured_at, created_at, updated_at
	) VALUES ('default',$1,$2,$3,7,$4,$5,$5,$5)`, sessionID, source, oldFileID, strings.Repeat("d", 64), oldCapturedAt); err != nil {
		t.Fatalf("seed previous capture index: %v", err)
	}

	quotaManifest := []stagedOutputCaptureEntry{{
		SourcePath: source, Filename: "report.txt", MIMEType: "text/plain",
		SizeBytes: files.MaxRetainedBytesPerWorkspace + 1, SHA256: strings.Repeat("a", 64), BlobPointer: blobKey,
	}}
	staged := make(chan error, 1)
	go func() { staged <- stageOutputCaptureManifestForTest(admin, sessionID, writeID, 1, quotaManifest, now) }()
	if _, err := store.FinishIdle(context.Background(), request); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("FinishIdle over quota err = %v; want ResourceExhausted", err)
	}
	if err := <-staged; err != nil {
		t.Fatalf("stage over-quota capture: %v", err)
	}

	validEntry := stagedOutputCaptureEntry{
		SourcePath: source, Filename: "report.txt", MIMEType: "text/plain",
		SizeBytes: 3, SHA256: strings.Repeat("b", 64), BlobPointer: blobKey,
	}
	invalidExisting := stagedOutputCaptureEntry{
		SourcePath: "outputs/missing.txt", Filename: "missing.txt", MIMEType: "text/plain",
		SizeBytes: 1, SHA256: strings.Repeat("c", 64), ExistingFileID: "file_missing_adoption",
	}
	manifestJSON, err := json.Marshal([]stagedOutputCaptureEntry{validEntry, invalidExisting})
	if err != nil {
		t.Fatalf("encode rollback manifest: %v", err)
	}
	if _, err := admin.Exec(`UPDATE sandbox_output_capture_operations
		SET manifest_json=$4, updated_at=$5
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
		sessionID, writeID, 1, string(manifestJSON), now); err != nil {
		t.Fatalf("install rollback manifest: %v", err)
	}
	if _, err := admin.Exec(`INSERT INTO sandbox_output_capture_blobs (
		workspace_id, session_id, finish_idle_write_id, capture_generation, source_path,
		blob_pointer, size_bytes, sha256, state, created_at, updated_at, uploaded_at
	) VALUES ('default',$1,$2,1,$3,$4,3,$5,'uploaded',$6,$6,$6)`,
		sessionID, writeID, source, blobKey, validEntry.SHA256, now); err != nil {
		t.Fatalf("seed uploaded capture blob: %v", err)
	}
	if _, err := store.FinishIdle(context.Background(), request); err == nil {
		t.Fatal("FinishIdle with missing existing file succeeded")
	}
	var fileRows, objectRows, captureRows, idleEvents, wakeJobs int
	var blobState string
	var blobFileID sql.NullString
	var indexedFileID, indexedDigest string
	var indexedSize int64
	var indexedAt time.Time
	if err := admin.QueryRow(`SELECT count(*) FROM files WHERE workspace_id='default' AND scope_type='session' AND scope_id=$1`, sessionID).Scan(&fileRows); err != nil {
		t.Fatalf("count rolled-back files: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM file_objects WHERE workspace_id='default'`).Scan(&objectRows); err != nil {
		t.Fatalf("count rolled-back file objects: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*), min(last_file_id), min(last_size_bytes), min(last_sha256), min(last_captured_at)
		FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, source).
		Scan(&captureRows, &indexedFileID, &indexedSize, &indexedDigest, &indexedAt); err != nil {
		t.Fatalf("read rolled-back capture index: %v", err)
	}
	if err := admin.QueryRow(`SELECT state, file_id FROM sandbox_output_capture_blobs
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND source_path=$3`, sessionID, writeID, source).Scan(&blobState, &blobFileID); err != nil {
		t.Fatalf("read rolled-back blob custody: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_idle'`, sessionID).Scan(&idleEvents); err != nil {
		t.Fatalf("count rolled-back idle events: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1 AND status IN ('pending','leased')`,
		"runtime_input:default:"+sessionID+":agent_mail:delivery_finish_idle_adoption").Scan(&wakeJobs); err != nil {
		t.Fatalf("count rolled-back mail wakes: %v", err)
	}
	if fileRows != 1 || objectRows != 1 || captureRows != 1 || indexedFileID != oldFileID || indexedSize != 7 ||
		indexedDigest != strings.Repeat("d", 64) || !indexedAt.Equal(oldCapturedAt) ||
		blobState != "uploaded" || blobFileID.Valid || idleEvents != 0 || wakeJobs != 0 {
		t.Fatalf("failed adoption leaked files=%d objects=%d captures=%d index=%q/%d/%q/%s blob=%s/%v idle=%d wakes=%d",
			fileRows, objectRows, captureRows, indexedFileID, indexedSize, indexedDigest, indexedAt, blobState, blobFileID, idleEvents, wakeJobs)
	}

	manifestJSON, err = json.Marshal([]stagedOutputCaptureEntry{validEntry})
	if err != nil {
		t.Fatalf("encode successful manifest: %v", err)
	}
	if _, err := admin.Exec(`UPDATE sandbox_output_capture_operations
		SET manifest_json=$4, updated_at=$5
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
		sessionID, writeID, 1, string(manifestJSON), now); err != nil {
		t.Fatalf("install successful manifest: %v", err)
	}
	if _, err := admin.Exec(`CREATE FUNCTION reject_finish_idle_operation_for_test() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject finish idle operation for rollback proof'; END $$`); err != nil {
		t.Fatalf("create FinishIdle rollback trigger function: %v", err)
	}
	if _, err := admin.Exec(`CREATE TRIGGER reject_finish_idle_operation_for_test
		BEFORE INSERT ON session_bridge_operations FOR EACH ROW
		WHEN (NEW.operation = 'finish_idle') EXECUTE FUNCTION reject_finish_idle_operation_for_test()`); err != nil {
		t.Fatalf("create FinishIdle rollback trigger: %v", err)
	}
	if _, err := store.FinishIdle(context.Background(), request); err == nil {
		t.Fatal("FinishIdle succeeded despite final operation rejection")
	}
	var captureState string
	if err := admin.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=1`, sessionID, writeID).Scan(&captureState); err != nil {
		t.Fatalf("read capture operation after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM files WHERE workspace_id='default' AND scope_type='session' AND scope_id=$1`, sessionID).Scan(&fileRows); err != nil {
		t.Fatalf("count files after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM file_objects WHERE workspace_id='default'`).Scan(&objectRows); err != nil {
		t.Fatalf("count file objects after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*), min(last_file_id), min(last_size_bytes), min(last_sha256), min(last_captured_at)
		FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, source).
		Scan(&captureRows, &indexedFileID, &indexedSize, &indexedDigest, &indexedAt); err != nil {
		t.Fatalf("read capture index after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT state, file_id FROM sandbox_output_capture_blobs
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND source_path=$3`, sessionID, writeID, source).Scan(&blobState, &blobFileID); err != nil {
		t.Fatalf("read blob custody after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_idle'`, sessionID).Scan(&idleEvents); err != nil {
		t.Fatalf("count idle events after final-operation rollback: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1 AND status IN ('pending','leased')`,
		"runtime_input:default:"+sessionID+":agent_mail:delivery_finish_idle_adoption").Scan(&wakeJobs); err != nil {
		t.Fatalf("count mail wakes after final-operation rollback: %v", err)
	}
	if captureState != "staged" || fileRows != 1 || objectRows != 1 || captureRows != 1 || indexedFileID != oldFileID ||
		indexedSize != 7 || indexedDigest != strings.Repeat("d", 64) || !indexedAt.Equal(oldCapturedAt) ||
		blobState != "uploaded" || blobFileID.Valid || idleEvents != 0 || wakeJobs != 0 {
		t.Fatalf("final-operation rollback leaked capture=%s files=%d objects=%d index=%d/%q/%d/%q/%s blob=%s/%v idle=%d wakes=%d",
			captureState, fileRows, objectRows, captureRows, indexedFileID, indexedSize, indexedDigest, indexedAt,
			blobState, blobFileID, idleEvents, wakeJobs)
	}
	if _, err := admin.Exec(`DROP TRIGGER reject_finish_idle_operation_for_test ON session_bridge_operations`); err != nil {
		t.Fatalf("drop FinishIdle rollback trigger: %v", err)
	}
	if _, err := admin.Exec(`DROP FUNCTION reject_finish_idle_operation_for_test()`); err != nil {
		t.Fatalf("drop FinishIdle rollback trigger function: %v", err)
	}
	response, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle successful adoption: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("FinishIdle ack = %s; want committed", response.GetAck().GetStatus())
	}
	var capturedFileID string
	var capturedBlobKey, capturedDigest string
	var capturedSize int64
	if err := admin.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=1`, sessionID, writeID).Scan(&captureState); err != nil {
		t.Fatalf("read adopted operation: %v", err)
	}
	if err := admin.QueryRow(`SELECT state, file_id FROM sandbox_output_capture_blobs
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND source_path=$3`, sessionID, writeID, source).Scan(&blobState, &capturedFileID); err != nil {
		t.Fatalf("read adopted blob custody: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM files WHERE workspace_id='default' AND scope_type='session' AND scope_id=$1`, sessionID).Scan(&fileRows); err != nil {
		t.Fatalf("count adopted file: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM file_objects WHERE workspace_id='default'`).Scan(&objectRows); err != nil {
		t.Fatalf("count adopted file objects: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*), min(last_file_id), min(last_size_bytes), min(last_sha256), min(last_captured_at)
		FROM session_output_captures WHERE workspace_id='default' AND session_id=$1 AND source_path=$2`, sessionID, source).
		Scan(&captureRows, &indexedFileID, &indexedSize, &indexedDigest, &indexedAt); err != nil {
		t.Fatalf("read adopted capture index: %v", err)
	}
	if err := admin.QueryRow(`SELECT o.blob_key, o.size_bytes, o.sha256
		FROM files f JOIN file_objects o ON o.workspace_id=f.workspace_id AND o.object_id=f.object_id
		WHERE f.workspace_id='default' AND f.file_id=$1`, capturedFileID).
		Scan(&capturedBlobKey, &capturedSize, &capturedDigest); err != nil {
		t.Fatalf("read adopted file object identity: %v", err)
	}
	if err := admin.QueryRow(`SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND dedupe_key=$1 AND status IN ('pending','leased')`,
		"runtime_input:default:"+sessionID+":agent_mail:delivery_finish_idle_adoption").Scan(&wakeJobs); err != nil {
		t.Fatalf("count committed mail wake: %v", err)
	}
	if captureState != "adopted" || blobState != "adopted" || capturedFileID == "" || fileRows != 2 || objectRows != 2 ||
		captureRows != 1 || indexedFileID != capturedFileID || indexedSize != validEntry.SizeBytes ||
		indexedDigest != validEntry.SHA256 || !indexedAt.Equal(now) || capturedBlobKey != blobKey ||
		capturedSize != validEntry.SizeBytes || capturedDigest != validEntry.SHA256 || wakeJobs != 1 {
		t.Fatalf("adoption result operation=%s blob=%s file=%q files=%d objects=%d captures=%d index=%q/%d/%q/%s object=%q/%d/%q wakes=%d",
			captureState, blobState, capturedFileID, fileRows, objectRows, captureRows, indexedFileID, indexedSize, indexedDigest, indexedAt,
			capturedBlobKey, capturedSize, capturedDigest, wakeJobs)
	}
}

func TestPostgreSQLBridgeAPIStoreFinishIdleReplaysFailedCaptureWithNewGeneration(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_finish_idle_capture_generation"
		threadID  = "thr_finish_idle_capture_generation"
		bindingID = "bind_finish_idle_capture_generation"
		writeID   = "evt_finish_idle_capture_generation"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_finish_idle_capture_generation")
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_roots_json, provider_metadata_json, created_at, updated_at
	) VALUES ('default',$1,'sbox_finish_idle_capture_generation',$2,1,'daytona',
		'provider_finish_idle_capture_generation',1,1,'[]','{}',$3,$3)`,
		sessionID, "env_"+sessionID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	request := bridgeAPIFinishIdleRequest(t, admin,
		bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_finish_idle_capture_generation"),
		writeID, `{"type":"end_turn"}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }

	failDone := make(chan error, 1)
	go func() {
		failDone <- settleOutputCaptureGenerationForTest(admin, sessionID, writeID, 1, "failed")
	}()
	if _, err := store.FinishIdle(context.Background(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("FinishIdle failed capture error = %v; want Unavailable", err)
	}
	if err := <-failDone; err != nil {
		t.Fatalf("settle failed capture: %v", err)
	}

	stageDone := make(chan error, 1)
	go func() {
		stageDone <- settleOutputCaptureGenerationForTest(admin, sessionID, writeID, 2, "staged")
	}()
	response, err := store.FinishIdle(context.Background(), request)
	if err != nil {
		t.Fatalf("FinishIdle replay: %v", err)
	}
	if err := <-stageDone; err != nil {
		t.Fatalf("stage replay capture: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("FinishIdle replay status = %s; want committed", response.GetAck().GetStatus())
	}
	rows, err := admin.Query(`SELECT capture_generation, state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 ORDER BY capture_generation`, sessionID, writeID)
	if err != nil {
		t.Fatalf("read capture generations: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var generations []string
	for rows.Next() {
		var generation int
		var state string
		if err := rows.Scan(&generation, &state); err != nil {
			t.Fatalf("scan capture generation: %v", err)
		}
		generations = append(generations, fmt.Sprintf("%d:%s", generation, state))
	}
	if !reflect.DeepEqual(generations, []string{"1:failed", "2:adopted"}) {
		t.Fatalf("capture generations = %v; want immutable failure then adopted successor", generations)
	}
	var idleEvents int
	if err := admin.QueryRow(`SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='session.status_idle'`, sessionID).Scan(&idleEvents); err != nil {
		t.Fatalf("count idle events: %v", err)
	}
	if idleEvents != 1 {
		t.Fatalf("idle event count = %d; want one", idleEvents)
	}
}

func TestPostgreSQLBridgeAPIStoreFinishIdleCannotAdoptAfterPodLossSettlesTurn(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_finish_idle_pod_loss"
		threadID  = "thr_finish_idle_pod_loss"
		bindingID = "bind_finish_idle_pod_loss"
		writeID   = "evt_finish_idle_pod_loss"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, "pod_uid_"+sessionID)
	seedRuntimePodLostStatusFence(t, admin, sessionID, bindingID, 1)
	now := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	if _, err := admin.Exec(`INSERT INTO session_sandbox_bindings (
		workspace_id, session_id, logical_sandbox_id, environment_id, environment_generation,
		provider, provider_resource_id, binding_revision, materialized_resource_revision,
		resource_roots_json, provider_metadata_json, created_at, updated_at
	) VALUES ('default',$1,'sbox_finish_idle_pod_loss',$2,1,'daytona',
		'provider_finish_idle_pod_loss',1,1,'[]','{}',$3,$3)`, sessionID, "env_"+sessionID, now); err != nil {
		t.Fatalf("seed sandbox binding: %v", err)
	}
	scope := bridgeAPIScope(sessionID, threadID, bindingID, 1, "pod_uid_"+sessionID)
	request := bridgeAPIFinishIdleRequest(t, admin, scope, writeID, `{"type":"end_turn"}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return now }
	finishDone := make(chan error, 1)
	go func() {
		_, err := store.FinishIdle(context.Background(), request)
		finishDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var exists bool
		if err := admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM sandbox_output_capture_operations
			WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2)`, sessionID, writeID).Scan(&exists); err != nil {
			t.Fatalf("inspect output capture: %v", err)
		}
		if exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FinishIdle did not create output capture")
		}
		time.Sleep(10 * time.Millisecond)
	}
	binding := runtimePodLostBinding(sessionID, bindingID, 1)
	if _, err := runRuntimePodLostRepairTransaction(context.Background(), runtime, sessionID, binding, now.Add(time.Second)); err != nil {
		t.Fatalf("settle runtime pod loss: %v", err)
	}
	if err := settleOutputCaptureGenerationForTest(admin, sessionID, writeID, 1, "staged"); err != nil {
		t.Fatalf("stage output capture: %v", err)
	}
	if err := <-finishDone; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("FinishIdle after pod-loss settlement error = %v; want FailedPrecondition", err)
	}
	var idleEvents int
	var captureState string
	if err := admin.QueryRow(`SELECT count(*) FROM session_events
		WHERE workspace_id='default' AND session_id=$1 AND type='session.status_idle'`, sessionID).Scan(&idleEvents); err != nil {
		t.Fatalf("count idle events: %v", err)
	}
	if err := admin.QueryRow(`SELECT state FROM sandbox_output_capture_operations
		WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2`, sessionID, writeID).Scan(&captureState); err != nil {
		t.Fatalf("read capture state: %v", err)
	}
	if idleEvents != 1 || captureState != "staged" {
		t.Fatalf("pod-loss/FinishIdle result = %d idle events and %s capture; want one/staged", idleEvents, captureState)
	}
}

func settleOutputCaptureGenerationForTest(db *sql.DB, sessionID string, writeID string, generation int, state string) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var current string
		err := db.QueryRow(`SELECT state FROM sandbox_output_capture_operations
			WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
			sessionID, writeID, generation).Scan(&current)
		if err == nil && (current == "pending" || current == "running") {
			digest := strings.Repeat("a", 64)
			if state == "failed" {
				_, err = db.Exec(`UPDATE sandbox_output_capture_operations SET state='failed',
					failure_kind='capture_test_failure', failure_detail='capture test failure',
					outcome_state='failed', outcome_digest=$4, updated_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
					sessionID, writeID, generation, digest)
			} else {
				_, err = db.Exec(`UPDATE sandbox_output_capture_operations SET state='staged',
					outcome_state='staged', outcome_digest=$4, staged_at=clock_timestamp(), updated_at=clock_timestamp()
					WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2 AND capture_generation=$3`,
					sessionID, writeID, generation, digest)
			}
			return err
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("capture generation was not created")
}

func stageOutputCaptureManifestForTest(db *sql.DB, sessionID string, writeID string, generation int, manifest []stagedOutputCaptureEntry, now time.Time) error {
	body, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		result, err := db.Exec(`UPDATE sandbox_output_capture_operations
			SET state='staged', manifest_json=$4, skipped_json='[]', scan_records_json='[]',
			    outcome_state='staged', outcome_digest=$5, staged_at=$6, updated_at=$6
			WHERE workspace_id='default' AND session_id=$1 AND finish_idle_write_id=$2
			  AND capture_generation=$3 AND state IN ('pending','running')`,
			sessionID, writeID, generation, string(body), strings.Repeat("d", 64), now)
		if err != nil {
			return err
		}
		if count, err := result.RowsAffected(); err != nil {
			return err
		} else if count == 1 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("capture generation was not created")
}

func finishIdleWithStagedCaptureForTest(t *testing.T, db *sql.DB, store *PostgreSQLBridgeAPIStore, request *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error) {
	t.Helper()
	settled := make(chan error, 1)
	go func() {
		settled <- settleOutputCaptureGenerationForTest(db, request.GetScope().GetSessionId(), request.GetDurableTurnId(), 1, "staged")
	}()
	response, err := store.FinishIdle(context.Background(), request)
	if settleErr := <-settled; settleErr != nil {
		t.Fatalf("stage output capture: %v", settleErr)
	}
	return response, err
}

func TestPostgreSQLBridgeAPIStoreFinishIdleRequiresActionPreservesTurnRetryCounters(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_requires_action", "thr_bridge_requires_action")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_requires_action", "bind_bridge_requires_action", 1, "pod_uid_requires_action")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, running_since, active_seconds_total,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_requires_action', 'running', '2026-01-01T00:00:15Z', 0,
			'bind_bridge_requires_action', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:15Z'
		)`); err != nil {
		t.Fatalf("seed runtime status: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_turn_retries (
			workspace_id, session_id, session_thread_id, provider_attempts, compaction_attempts, updated_at
		) VALUES ('default', 'sesn_bridge_requires_action', 'thr_bridge_requires_action', 2, 1, '2026-01-01T00:00:15Z')`); err != nil {
		t.Fatalf("seed turn retry counters: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 45, 0, time.UTC) }
	request := bridgeAPIFinishIdleRequest(
		t,
		admin,
		bridgeAPIScope("sesn_bridge_requires_action", "thr_bridge_requires_action", "bind_bridge_requires_action", 1, "pod_uid_requires_action"),
		"evt_bridge_requires_action_running",
		`{"type":"requires_action"}`,
	)
	if _, err := finishIdleWithStagedCaptureForTest(t, admin, store, request); err != nil {
		t.Fatalf("FinishIdle requires_action: %v", err)
	}

	var providerAttempts, compactionAttempts int64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT provider_attempts, compaction_attempts
		   FROM session_turn_retries
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_requires_action'
		    AND session_thread_id = 'thr_bridge_requires_action'`).Scan(&providerAttempts, &compactionAttempts); err != nil {
		t.Fatalf("read preserved turn retry counters: %v", err)
	}
	if providerAttempts != 2 || compactionAttempts != 1 {
		t.Fatalf("turn retry counters = %d/%d; want 2/1 after requires_action", providerAttempts, compactionAttempts)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitRuntimeTerminationSettlesSessionAtomically(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_terminate", "thr_bridge_terminate")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_terminate", "thr_bridge_terminate", "thr_bridge_terminate_sibling")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_terminate", "thr_bridge_terminate", "thr_bridge_terminate_idle_sibling")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_terminate", "thr_bridge_terminate", "thr_bridge_terminate_failed_sibling")
	if _, err := admin.ExecContext(context.Background(),
		`UPDATE session_threads
		    SET status = 'failed'
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND id = 'thr_bridge_terminate_failed_sibling'`,
	); err != nil {
		t.Fatalf("seed failed sibling: %v", err)
	}
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_terminate", "bind_bridge_terminate", 1, "pod_uid_terminate")
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, running_since, active_seconds_total,
			binding_id, binding_generation, created_at, updated_at
		) VALUES (
			'default', 'sesn_bridge_terminate', 'running', '2026-01-01T00:00:20Z', 4,
			'bind_bridge_terminate', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:20Z'
		)`); err != nil {
		t.Fatalf("seed running runtime status: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_terminate", "thr_bridge_terminate", "bind_bridge_terminate", 1, "pod_uid_terminate")
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_terminate")
	siblingScope := bridgeAPIScope("sesn_bridge_terminate", "thr_bridge_terminate_sibling", "bind_bridge_terminate", 1, "pod_uid_terminate")
	seedBridgeAPIOpenDurableTurn(t, admin, siblingScope, "rwrite_terminate_sibling")
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: siblingScope, RuntimeWriteId: "rwrite_terminate_sibling_start", ModelRequestId: "mreq_terminate_sibling",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_terminate_sibling"}`, SessionVisible: true,
	}); err != nil {
		t.Fatalf("WriteEvent sibling request start: %v", err)
	}
	siblingToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: siblingScope, RuntimeWriteId: "rwrite_terminate_sibling_tool", ModelRequestId: "mreq_terminate_sibling",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Write","input":{"file_path":"sibling.txt"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			siblingScope,
			"rwrite_terminate_sibling_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_terminate_sibling","toolName":"Write","state":{"status":"running","input":{"value":{"file_path":"sibling.txt"},"preview":"{\"file_path\":\"sibling.txt\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent sibling tool use: %v", err)
	}
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate_start", ModelRequestId: "mreq_terminate",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_terminate"}`, SessionVisible: true,
	}); err != nil {
		t.Fatalf("WriteEvent request start: %v", err)
	}
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate_tool", ModelRequestId: "mreq_terminate",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Bash","input":{"command":"sleep 10"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_terminate_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_terminate","toolName":"Bash","state":{"status":"running","input":{"value":{"command":"sleep 10"},"preview":"{\"command\":\"sleep 10\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent tool use: %v", err)
	}

	failureJSON := `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`
	terminationDraft := bridgeRuntimeTerminationDraftForTest(
		scope,
		"rwrite_terminate",
		0,
		"assistant",
		"agent",
		bridgeRuntimePartDraftForTest{
			kind: "tool",
			json: `{"type":"tool","toolCallId":"call_terminate","toolName":"Bash","toolUseEventId":"` + toolUse.GetEventId() + `","state":{"status":"cancelled","error":{"type":"runtime","code":"runtime_terminated","message":"Loop-authored terminal cancellation.","retryable":false,"fatal":true}}}`,
		},
	)
	request := &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate", FailureJson: failureJSON,
		Drafts: []*bridgev1.RuntimeMessageDraft{terminationDraft},
		PendingToolCancellations: []*bridgev1.PendingToolCancellationDraft{{
			ToolUseEventId: toolUse.GetEventId(),
			RuntimeLocalId: terminationDraft.GetRuntimeLocalId(),
		}},
	}
	response, err := store.CommitRuntimeTermination(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitRuntimeTermination: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED {
		t.Fatalf("termination ack = %s; want committed", response.GetAck().GetStatus())
	}
	if response.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("termination committed ack write id = %q; want %q", response.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}
	if len(response.GetDeclaration().GetReceipts()) != 1 {
		t.Fatalf("termination declaration receipts = %d; want 1", len(response.GetDeclaration().GetReceipts()))
	}
	receipt := response.GetDeclaration().GetReceipts()[0]
	if receipt.GetOperationKind() != bridgeOpCommitRuntimeTermination ||
		receipt.GetSourceKind() != "runtime_termination" ||
		receipt.GetSourceId() != request.GetRuntimeWriteId() ||
		len(receipt.GetMessages()) != 1 {
		t.Fatalf("termination declaration receipt = %+v; want loop-authored cancellation receipt", receipt)
	}
	replay, err := store.CommitRuntimeTermination(context.Background(), request)
	if err != nil {
		t.Fatalf("CommitRuntimeTermination replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("termination replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if replay.GetAck().GetRuntimeWriteId() != request.GetRuntimeWriteId() {
		t.Fatalf("termination duplicate ack write id = %q; want %q", replay.GetAck().GetRuntimeWriteId(), request.GetRuntimeWriteId())
	}
	if !proto.Equal(response.GetDeclaration(), replay.GetDeclaration()) {
		t.Fatalf("termination replay declaration differs: first=%+v replay=%+v", response.GetDeclaration(), replay.GetDeclaration())
	}
	wrongCaller := internalgrpcauth.ContextWithIdentity(context.Background(), internalgrpcauth.Identity{
		ServiceAccount:   internalgrpcauth.ServiceAccount{Namespace: "tetral-agent-runtime", Name: "agent-runtime"},
		KubernetesPodUID: "pod_uid_other",
	})
	if _, err := store.CommitRuntimeTermination(wrongCaller, request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("termination replay from wrong pod err = %v; want PermissionDenied", err)
	}

	var sessionStatus, threadStatus, waitStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_terminate'`).Scan(&sessionStatus); err != nil {
		t.Fatalf("read terminated session: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate' AND id = 'thr_bridge_terminate'`).Scan(&threadStatus); err != nil {
		t.Fatalf("read terminated thread: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_pending_tool_uses WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate' AND tool_use_event_id = $1`, toolUse.GetEventId()).Scan(&waitStatus); err != nil {
		t.Fatalf("read terminated wait: %v", err)
	}
	if sessionStatus != "terminated" || threadStatus != "failed" || waitStatus != "cancelled" {
		t.Fatalf("terminal states = session:%q thread:%q wait:%q; want terminated/failed/cancelled", sessionStatus, threadStatus, waitStatus)
	}
	var siblingStatus, idleSiblingStatus, failedSiblingStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND id = 'thr_bridge_terminate_sibling'`,
	).Scan(&siblingStatus); err != nil {
		t.Fatalf("read live sibling status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND id = 'thr_bridge_terminate_idle_sibling'`,
	).Scan(&idleSiblingStatus); err != nil {
		t.Fatalf("read idle sibling status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND id = 'thr_bridge_terminate_failed_sibling'`,
	).Scan(&failedSiblingStatus); err != nil {
		t.Fatalf("read failed sibling status: %v", err)
	}
	if siblingStatus != "terminated" || idleSiblingStatus != "terminated" || failedSiblingStatus != "failed" {
		t.Fatalf("sibling statuses = live:%q idle:%q failed:%q; want terminated/terminated/failed", siblingStatus, idleSiblingStatus, failedSiblingStatus)
	}
	var siblingRequestEnds int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND session_thread_id = 'thr_bridge_terminate_sibling'
		    AND model_request_id = 'mreq_terminate_sibling'
		    AND type = 'span.model_request_end'`,
	).Scan(&siblingRequestEnds); err != nil {
		t.Fatalf("count live sibling request ends: %v", err)
	}
	if siblingRequestEnds != 1 {
		t.Fatalf("live sibling request ends = %d; want one terminal closeout", siblingRequestEnds)
	}
	var siblingPendingStatus string
	var siblingToolResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status,
		        (SELECT count(*)
		           FROM session_events result
		          WHERE result.workspace_id = pending.workspace_id
		            AND result.session_id = pending.session_id
		            AND result.session_thread_id = pending.session_thread_id
		            AND result.type = 'agent.tool_result'
		            AND result.payload_json::jsonb ->> 'tool_use_event_id' = pending.tool_use_event_id)
		   FROM session_pending_tool_uses pending
		  WHERE pending.workspace_id = 'default'
		    AND pending.session_id = 'sesn_bridge_terminate'
		    AND pending.session_thread_id = 'thr_bridge_terminate_sibling'
		    AND pending.tool_use_event_id = $1`,
		siblingToolUse.GetEventId(),
	).Scan(&siblingPendingStatus, &siblingToolResultCount); err != nil {
		t.Fatalf("read sibling terminal tool settlement: %v", err)
	}
	if siblingPendingStatus != "cancelled" || siblingToolResultCount != 1 {
		t.Fatalf("sibling terminal tool settlement = %q/results %d; want cancelled/1", siblingPendingStatus, siblingToolResultCount)
	}

	for eventType, want := range map[string]int{
		"agent.thread_message_sent":        0,
		"span.model_request_end":           2,
		"agent.tool_result":                2,
		"session.error":                    1,
		"session.status_terminated":        1,
		"session.thread_status_terminated": 2,
		"session.thread_status_idle":       0,
		"session.status_idle":              0,
		"session.status_rescheduled":       0,
	} {
		var count int
		if err := admin.QueryRowContext(context.Background(),
			`SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate' AND type = $1`, eventType).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", eventType, err)
		}
		if count != want {
			t.Fatalf("%s count = %d; want %d", eventType, count, want)
		}
	}
	var terminatedPayload string
	if err := admin.QueryRowContext(context.Background(), `SELECT payload_json FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate' AND type = 'session.status_terminated'`).Scan(&terminatedPayload); err != nil {
		t.Fatalf("read terminated payload: %v", err)
	}
	if terminatedPayload != `{"type":"session.status_terminated"}` {
		t.Fatalf("terminated payload = %s; want reasonless SDK shape", terminatedPayload)
	}
	var errorPayload string
	if err := admin.QueryRowContext(context.Background(), `SELECT payload_json FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate' AND type = 'session.error'`).Scan(&errorPayload); err != nil {
		t.Fatalf("read terminal error payload: %v", err)
	}
	if got := testJSONPathString(t, errorPayload, "error.type"); got != "unknown_error" {
		t.Fatalf("terminal error type = %q; want unknown_error", got)
	}
	if got := testJSONPathString(t, errorPayload, "error.retry_status.type"); got != "terminal" {
		t.Fatalf("terminal retry status = %q; want terminal", got)
	}
	if strings.Contains(errorPayload, `"code"`) || strings.Contains(errorPayload, `"retryable"`) || strings.Contains(errorPayload, `"fatal"`) {
		t.Fatalf("terminal public error leaked internal fields: %s", errorPayload)
	}
	var toolResultMessage string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_terminate'
		    AND session_thread_id = 'thr_bridge_terminate'
		    AND source_event_id IN (
		        SELECT event_id
		          FROM session_events
		         WHERE workspace_id = 'default'
		           AND session_id = 'sesn_bridge_terminate'
		           AND type = 'agent.tool_result'
		    )`,
	).Scan(&toolResultMessage); err != nil {
		t.Fatalf("read loop-authored terminal tool result: %v", err)
	}
	var toolResultProjection struct {
		Parts []struct {
			State struct {
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			} `json:"state"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(toolResultMessage), &toolResultProjection); err != nil {
		t.Fatalf("decode loop-authored terminal tool result: %v", err)
	}
	if len(toolResultProjection.Parts) != 1 ||
		toolResultProjection.Parts[0].State.Error.Message != "Loop-authored terminal cancellation." {
		t.Fatalf("terminal tool result projection = %+v; want loop-authored content", toolResultProjection)
	}
	var completionWakeJobs int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM queue_jobs
		  WHERE workspace_id='default' AND payload_json::jsonb ->> 'input_kind'='agent_mail'
		    AND payload_json::jsonb ->> 'session_id'='sesn_bridge_terminate'`,
	).Scan(&completionWakeJobs); err != nil {
		t.Fatalf("count session-wide termination completion wake jobs: %v", err)
	}
	if completionWakeJobs != 0 {
		t.Fatalf("session-wide termination completion wake jobs = %d; want zero", completionWakeJobs)
	}
	var runningSince sql.NullString
	var activeSeconds float64
	if err := admin.QueryRowContext(context.Background(),
		`SELECT running_since, active_seconds_total
		   FROM session_runtime_status
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate'`).Scan(&runningSince, &activeSeconds); err != nil {
		t.Fatalf("read terminal runtime stats: %v", err)
	}
	if runningSince.Valid || activeSeconds != 44 {
		t.Fatalf("terminal running/active stats = %v/%v; want null/44", runningSince, activeSeconds)
	}

	conflict := proto.Clone(request).(*bridgev1.CommitRuntimeTerminationRequest)
	conflict.FailureJson = `{"type":"provider","code":"provider_invalid_request","message":"Provider request failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"}}`
	if _, err := store.CommitRuntimeTermination(context.Background(), conflict); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("termination conflict err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitRuntimeTerminationConsumesDispatchedSandboxExecution(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_terminate_direct_sandbox", "thr_bridge_terminate_direct_sandbox")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_terminate_direct_sandbox", "bind_bridge_terminate_direct_sandbox", 1, "pod_uid_terminate_direct_sandbox")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_terminate_direct_sandbox", "thr_bridge_terminate_direct_sandbox", "bind_bridge_terminate_direct_sandbox", 1, "pod_uid_terminate_direct_sandbox")
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_terminate_direct_sandbox")
	seedBridgeAPIRequestStart(t, store, scope, "rwrite_terminate_direct_sandbox_start", "mreq_terminate_direct_sandbox", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate_direct_sandbox_tool", ModelRequestId: "mreq_terminate_direct_sandbox",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Bash","input":{"command":"sleep 10"},"evaluated_permission":"allow"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_terminate_direct_sandbox_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_terminate_direct_sandbox","toolName":"Bash","state":{"status":"running","input":{"value":{"command":"sleep 10"},"preview":"{\"command\":\"sleep 10\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent sandbox tool use: %v", err)
	}
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			created_at, updated_at
		) VALUES ('default', 'sesn_bridge_terminate_direct_sandbox', 'thr_bridge_terminate_direct_sandbox',
			$1, 'sandbox_tool', $2, 'Bash', $3, 'committed', NULL,
			'call_terminate_direct_sandbox', 'running', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		toolUse.GetEventId(), sha256Hex(`{"command":"sleep 10"}`), `{"command":"sleep 10"}`,
	); err != nil {
		t.Fatalf("seed dispatched sandbox execution: %v", err)
	}
	failureJSON := `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`
	response, err := store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate_direct_sandbox", FailureJson: failureJSON,
		SandboxExecutionToolUseEventIds: []string{toolUse.GetEventId()},
	})
	if err != nil {
		t.Fatalf("CommitRuntimeTermination dispatched sandbox execution: %v", err)
	}
	var executionState string
	var resultJSON sql.NullString
	var terminalEventID, consumptionReason string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT execution_state, result_json, consumed_by_terminal_event_id, consumption_reason
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_direct_sandbox'
		    AND session_thread_id = 'thr_bridge_terminate_direct_sandbox' AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
	).Scan(&executionState, &resultJSON, &terminalEventID, &consumptionReason); err != nil {
		t.Fatalf("read terminated sandbox execution: %v", err)
	}
	var toolResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_direct_sandbox'
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $1`,
		toolUse.GetEventId(),
	).Scan(&toolResultCount); err != nil {
		t.Fatalf("count terminated sandbox Tool Results: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		executionState != "consumed" || resultJSON.Valid || terminalEventID == "" ||
		consumptionReason != "runtime_terminated" || toolResultCount != 1 {
		t.Fatalf("termination ack=%s execution=%q result=%v terminal=%q reason=%q events=%d; want committed consumed thin receipt and one Tool Result",
			response.GetAck().GetStatus(), executionState, resultJSON, terminalEventID, consumptionReason, toolResultCount)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitRuntimeTerminationSettlesSiblingMCPAndStagedSandboxResults(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox_sibling")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox", "thr_bridge_terminate_mcp_sibling")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_terminate_sandbox", "bind_bridge_terminate_sandbox", 1, "pod_uid_terminate_sandbox")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope("sesn_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox", "bind_bridge_terminate_sandbox", 1, "pod_uid_terminate_sandbox")
	siblingScope := bridgeAPIScope("sesn_bridge_terminate_sandbox", "thr_bridge_terminate_sandbox_sibling", "bind_bridge_terminate_sandbox", 1, "pod_uid_terminate_sandbox")
	mcpSiblingScope := bridgeAPIScope("sesn_bridge_terminate_sandbox", "thr_bridge_terminate_mcp_sibling", "bind_bridge_terminate_sandbox", 1, "pod_uid_terminate_sandbox")
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_terminate_sandbox")
	seedBridgeAPIRequestStart(t, store, siblingScope, "rwrite_terminate_sandbox_start", "mreq_terminate_sandbox", "agent_provider_request", 0)
	seedBridgeAPIRequestStart(t, store, mcpSiblingScope, "rwrite_terminate_mcp_start", "mreq_terminate_mcp", "agent_provider_request", 0)
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: siblingScope, RuntimeWriteId: "rwrite_terminate_sandbox_tool", ModelRequestId: "mreq_terminate_sandbox",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Bash","input":{"command":"sleep 10"},"evaluated_permission":"allow"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			siblingScope,
			"rwrite_terminate_sandbox_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_terminate_sandbox","toolName":"Bash","state":{"status":"running","input":{"value":{"command":"sleep 10"},"preview":"{\"command\":\"sleep 10\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent sandbox tool use: %v", err)
	}
	mcpToolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: mcpSiblingScope, RuntimeWriteId: "rwrite_terminate_sandbox_mcp", ModelRequestId: "mreq_terminate_mcp",
		EventType:      "agent.mcp_tool_use",
		PayloadJson:    `{"type":"agent.mcp_tool_use","name":"search_code","mcp_server_name":"github","input":{"q":"x"},"evaluated_permission":"allow"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			mcpSiblingScope,
			"rwrite_terminate_sandbox_mcp",
			"agent.mcp_tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_terminate_sandbox_mcp","toolName":"search_code","toolEvent":{"kind":"mcp","mcpServerName":"github"},"state":{"status":"running","input":{"value":{"q":"x"},"preview":"{\"q\":\"x\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent sibling MCP tool use: %v", err)
	}
	const stagedResultJSON = `{"status":"success","result":{"summary":"command completed"}}`
	stagedResultDigest := sha256Hex(stagedResultJSON)
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json,
			model_tool_call_id, execution_state, execution_attempt_generation,
			result_digest, created_at, updated_at
		) VALUES ('default', 'sesn_bridge_terminate_sandbox', 'thr_bridge_terminate_sandbox_sibling',
			$1, 'sandbox_tool', $2, 'Bash', $3, 'committed', $4,
			'call_terminate_sandbox', 'terminal_unconsumed', 1, $5,
			CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		toolUse.GetEventId(), sha256Hex(`{"command":"sleep 10"}`), `{"command":"sleep 10"}`, stagedResultJSON, stagedResultDigest,
	); err != nil {
		t.Fatalf("seed staged sandbox execution: %v", err)
	}
	failureJSON := `{"type":"runtime","code":"runtime_invalid_sequence","message":"Runtime operation failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"},"reason":"runtime_contract_validation"}`
	response, err := store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope: scope, RuntimeWriteId: "rwrite_terminate_sandbox", FailureJson: failureJSON,
	})
	if err != nil {
		t.Fatalf("CommitRuntimeTermination sibling tool executions: %v", err)
	}
	var executionState string
	var resultJSON sql.NullString
	var resultDigest, terminalEventID, consumptionReason string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT execution_state, result_json, result_digest, consumed_by_terminal_event_id, consumption_reason
		   FROM session_runtime_tool_results
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_sandbox'
		    AND session_thread_id = 'thr_bridge_terminate_sandbox_sibling' AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
	).Scan(&executionState, &resultJSON, &resultDigest, &terminalEventID, &consumptionReason); err != nil {
		t.Fatalf("read terminated sandbox execution: %v", err)
	}
	var toolResultEventID, toolResultPayload string
	var toolResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT event_id, payload_json, count(*) OVER () FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_sandbox'
		    AND session_thread_id = 'thr_bridge_terminate_sandbox_sibling'
		    AND type = 'agent.tool_result'
		    AND payload_json::jsonb ->> 'tool_use_event_id' = $1`,
		toolUse.GetEventId(),
	).Scan(&toolResultEventID, &toolResultPayload, &toolResultCount); err != nil {
		t.Fatalf("read terminated sibling sandbox Tool Result: %v", err)
	}
	var mcpResultPayload string
	var mcpResultCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json, count(*) OVER () FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_sandbox'
		    AND session_thread_id = 'thr_bridge_terminate_mcp_sibling'
		    AND type = 'agent.mcp_tool_result'
		    AND payload_json::jsonb ->> 'mcp_tool_use_id' = $1`,
		mcpToolUse.GetEventId(),
	).Scan(&mcpResultPayload, &mcpResultCount); err != nil {
		t.Fatalf("read terminated sibling MCP Tool Result: %v", err)
	}
	var siblingStatus, mcpSiblingStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_sandbox'
		    AND id = 'thr_bridge_terminate_sandbox_sibling'`,
	).Scan(&siblingStatus); err != nil {
		t.Fatalf("read terminated sibling status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status FROM session_threads
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_terminate_sandbox'
		    AND id = 'thr_bridge_terminate_mcp_sibling'`,
	).Scan(&mcpSiblingStatus); err != nil {
		t.Fatalf("read terminated MCP sibling status: %v", err)
	}
	if response.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_COMMITTED ||
		siblingStatus != "terminated" || mcpSiblingStatus != "terminated" || executionState != "consumed" || resultJSON.Valid ||
		resultDigest != stagedResultDigest || terminalEventID != toolResultEventID ||
		consumptionReason != "runtime_terminated" ||
		toolResultCount != 1 || mcpResultCount != 1 ||
		testJSONPathString(t, toolResultPayload, "reason") != "runtime_terminated" ||
		testJSONPathString(t, mcpResultPayload, "reason") != "runtime_terminated" {
		t.Fatalf("termination ack=%s siblings=%q/%q execution=%q result=%v digest=%q terminal=%q event=%q reason=%q counts=%d/%d sandbox=%s mcp=%s; want sibling terminal settlement and consumed thin receipt",
			response.GetAck().GetStatus(), siblingStatus, mcpSiblingStatus, executionState, resultJSON, resultDigest,
			terminalEventID, toolResultEventID, consumptionReason, toolResultCount, mcpResultCount, toolResultPayload, mcpResultPayload)
	}
}

func TestPostgreSQLBridgeAPIStoreCommitRuntimeTerminationKeepsChildBlastRadiusLocal(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_child_terminate", "thr_bridge_child_terminate_main")
	seedBridgeAPIChildThread(t, admin, "default", "sesn_bridge_child_terminate", "thr_bridge_child_terminate_main", "thr_bridge_child_terminate")
	seedBridgeAPIEvent(t, admin, "default", "sesn_bridge_child_terminate", "thr_bridge_child_terminate", "evt_bridge_child_terminate_created", 1, "session.thread_created",
		`{"type":"session.thread_created","parent_thread_id":"thr_bridge_child_terminate_main","source_tool_use_event_id":"sevt_bridge_child_terminate_spawn"}`)
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_child_terminate", "bind_bridge_child_terminate", 1, "pod_uid_child_terminate")
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 0, 2, 0, 0, time.UTC) }

	scope := bridgeAPIScope("sesn_bridge_child_terminate", "thr_bridge_child_terminate", "bind_bridge_child_terminate", 1, "pod_uid_child_terminate")
	seedBridgeAPIOpenDurableTurn(t, admin, scope, "rwrite_child_terminate")
	if _, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_child_terminate_start", ModelRequestId: "mreq_child_terminate",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_child_terminate"}`, SessionVisible: true,
	}); err != nil {
		t.Fatalf("WriteEvent child request start: %v", err)
	}
	toolUse, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_child_terminate_tool", ModelRequestId: "mreq_child_terminate",
		EventType:      "agent.tool_use",
		PayloadJson:    `{"type":"agent.tool_use","name":"Bash","input":{"command":"sleep 10"},"evaluated_permission":"ask"}`,
		SessionVisible: true,
		Drafts: []*bridgev1.RuntimeMessageDraft{bridgeRuntimeOutputDraftForTest(
			t,
			scope,
			"rwrite_child_terminate_tool",
			"agent.tool_use",
			"streaming",
			bridgeRuntimePartDraftForTest{
				kind: "tool",
				json: `{"type":"tool","toolCallId":"call_child_terminate","toolName":"Bash","state":{"status":"running","input":{"value":{"command":"sleep 10"},"preview":"{\"command\":\"sleep 10\"}","truncated":false}}}`,
			},
		)},
	})
	if err != nil {
		t.Fatalf("WriteEvent child tool use: %v", err)
	}
	terminationDraft := bridgeRuntimeTerminationDraftForTest(
		scope,
		"rwrite_child_terminate",
		0,
		"assistant",
		"agent",
		bridgeRuntimePartDraftForTest{
			kind: "tool",
			json: `{"type":"tool","toolCallId":"call_child_terminate","toolName":"Bash","toolUseEventId":"` + toolUse.GetEventId() + `","state":{"status":"cancelled","error":{"type":"runtime","code":"runtime_terminated","message":"Loop-authored child cancellation.","retryable":false,"fatal":true}}}`,
		},
	)
	envelope := completionMailEnvelope(
		"main",
		"task_thr_bridge_child_terminate",
		"Agent errored: Loop-authored child failure.\n\nThis agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task.",
	)
	_, err = store.CommitRuntimeTermination(context.Background(), &bridgev1.CommitRuntimeTerminationRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_child_terminate",
		FailureJson:    `{"type":"provider","code":"provider_invalid_request","message":"Provider request failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"}}`,
		Drafts: []*bridgev1.RuntimeMessageDraft{
			terminationDraft,
			bridgeRuntimeTerminationCompletionMailDraftForTest(scope, "rwrite_child_terminate", 1, envelope),
		},
		PendingToolCancellations: []*bridgev1.PendingToolCancellationDraft{{
			ToolUseEventId: toolUse.GetEventId(),
			RuntimeLocalId: terminationDraft.GetRuntimeLocalId(),
		}},
	})
	if err != nil {
		t.Fatalf("CommitRuntimeTermination child: %v", err)
	}
	var sessionStatus, mainStatus, childStatus string
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_child_terminate'`).Scan(&sessionStatus); err != nil {
		t.Fatalf("read child session status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_terminate' AND id = 'thr_bridge_child_terminate_main'`).Scan(&mainStatus); err != nil {
		t.Fatalf("read child main status: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT status FROM session_threads WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_terminate' AND id = 'thr_bridge_child_terminate'`).Scan(&childStatus); err != nil {
		t.Fatalf("read child terminal status: %v", err)
	}
	if sessionStatus != "running" || mainStatus != "idle" || childStatus != "failed" {
		t.Fatalf("child blast radius = session:%q main:%q child:%q; want running/idle/failed", sessionStatus, mainStatus, childStatus)
	}
	var pendingToolStatus string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT status
		   FROM session_pending_tool_uses
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_terminate'
		    AND tool_use_event_id = $1`,
		toolUse.GetEventId(),
	).Scan(&pendingToolStatus); err != nil {
		t.Fatalf("read child terminal pending tool: %v", err)
	}
	if pendingToolStatus != "cancelled" {
		t.Fatalf("child terminal pending tool status = %q; want cancelled", pendingToolStatus)
	}
	var childEvents, sessionEvents int
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_terminate' AND type = 'session.thread_status_terminated'`).Scan(&childEvents); err != nil {
		t.Fatalf("count child terminated event: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(), `SELECT count(*) FROM session_events WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_child_terminate' AND type = 'session.status_terminated'`).Scan(&sessionEvents); err != nil {
		t.Fatalf("count session terminated event: %v", err)
	}
	if childEvents != 1 || sessionEvents != 0 {
		t.Fatalf("termination events child/session = %d/%d; want 1/0", childEvents, sessionEvents)
	}
	var completionEvents, completionJobs int
	var completionPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_terminate'
		    AND type = 'agent.thread_message_sent'`).Scan(&completionEvents); err != nil {
		t.Fatalf("count child termination completion mail: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_terminate'
		    AND type = 'agent.thread_message_sent'`).Scan(&completionPayload); err != nil {
		t.Fatalf("read child termination completion payload: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = 'default'
		    AND payload_json::jsonb ->> 'input_kind' = 'agent_mail'`).Scan(&completionJobs); err != nil {
		t.Fatalf("count child termination wake jobs: %v", err)
	}
	if completionEvents != 1 || completionJobs != 1 {
		t.Fatalf("child termination completion event/job = %d/%d; want 1/1", completionEvents, completionJobs)
	}
	var completion struct {
		Message struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(completionPayload), &completion); err != nil {
		t.Fatalf("decode child termination completion payload: %v", err)
	}
	if len(completion.Message.Parts) != 1 {
		t.Fatalf("child termination completion parts = %d; want 1", len(completion.Message.Parts))
	}
	if message := completion.Message.Parts[0].Text; message != envelope {
		t.Fatalf("child termination completion envelope = %q", message)
	}
	var childTerminatedPayload string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = 'default'
		    AND session_id = 'sesn_bridge_child_terminate'
		    AND type = 'session.thread_status_terminated'`).Scan(&childTerminatedPayload); err != nil {
		t.Fatalf("read child terminated payload: %v", err)
	}
	if testJSONPathString(t, childTerminatedPayload, "session_thread_id") != "thr_bridge_child_terminate" ||
		testJSONPathString(t, childTerminatedPayload, "task_name") != "task_thr_bridge_child_terminate" {
		t.Fatalf("child terminated payload = %s; want child ID and callable task_name", childTerminatedPayload)
	}
}

func TestParseRuntimeTerminationFailureRejectsNonTerminalClasses(t *testing.T) {
	for name, failureJSON := range map[string]string{
		"retryable provider": `{"type":"provider","code":"provider_unavailable","retryable":true,"retryStatus":{"type":"terminal"}}`,
		"exhausted provider": `{"type":"provider","code":"provider_invalid_request","retryable":false,"retryStatus":{"type":"exhausted"}}`,
		"runtime shutdown":   `{"type":"runtime","code":"runtime_invalid_sequence","reason":"runtime_shutdown","retryable":false,"retryStatus":{"type":"terminal"}}`,
		"gateway transient":  `{"type":"runtime","code":"gateway_stream_error","retryable":false,"retryStatus":{"type":"terminal"}}`,
		"persistence error":  `{"type":"session-event-writer","code":"unavailable","retryable":false,"retryStatus":{"type":"terminal"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseRuntimeTerminationFailure(failureJSON); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("parseRuntimeTerminationFailure err = %v; want InvalidArgument", err)
			}
		})
	}
	for name, failureJSON := range map[string]string{
		"semantic invariant": `{"type":"runtime","code":"runtime_invalid_sequence","reason":"runtime_contract_validation","retryable":false,"retryStatus":{"type":"terminal"}}`,
		"provider terminal":  `{"type":"provider","code":"provider_invalid_request","retryable":false,"retryStatus":{"type":"terminal"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := parseRuntimeTerminationFailure(failureJSON); err != nil {
				t.Fatalf("parseRuntimeTerminationFailure: %v", err)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndConsumesOnlyActiveTransientAttachments(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_consumed", "thr_bridge_attachment_consumed")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_consumed", "bind_bridge_attachment_consumed", 1, "pod_uid_attachment_consumed")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_consumed", "thr_bridge_attachment_consumed", "bind_bridge_attachment_consumed", 1, "pod_uid_attachment_consumed")
	states := []string{"active", "uploading", "consumed", "expired", "deleting", "deleted", "failed"}
	refs := make([]string, 0, len(states))
	for _, state := range states {
		created := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_consumed_"+state, "sevt_attachment_consumed_"+state, []byte(state))
		refs = append(refs, created.GetAttachmentRef())
		if state != "active" {
			if _, err := admin.ExecContext(context.Background(),
				`UPDATE session_transient_attachments
				    SET status = $2, updated_at = '2026-01-01T11:00:00Z'
				  WHERE workspace_id = 'default' AND attachment_ref = $1`,
				created.GetAttachmentRef(), state); err != nil {
				t.Fatalf("set %s attachment state: %v", state, err)
			}
		}
	}
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope:          scope,
		RuntimeWriteId: "rwrite_bridge_attachment_consumed_start",
		ModelRequestId: "mreq_bridge_attachment_consumed",
		EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
		PayloadJson:    `{"type":"span.model_request_start","model_request_id":"mreq_bridge_attachment_consumed"}`,
		SessionVisible: false,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           "rwrite_bridge_attachment_consumed_end",
		ModelRequestId:           "mreq_bridge_attachment_consumed",
		ModelRequestStartEventId: start.GetEventId(),
		FinishReason:             "stop",
		UsageJson:                `{"input_tokens":1,"output_tokens":1}`,
		ConsumedAttachmentRefs:   refs,
	}
	if _, err := store.WriteRequestEnd(context.Background(), request); err != nil {
		t.Fatalf("WriteRequestEnd: %v", err)
	}
	for index, initialState := range states {
		wantState := initialState
		if initialState == "active" {
			wantState = "consumed"
		}
		var statusValue, updatedAt string
		if err := admin.QueryRowContext(context.Background(),
			`SELECT status, updated_at
			   FROM session_transient_attachments
			  WHERE workspace_id = 'default' AND attachment_ref = $1`, refs[index]).Scan(&statusValue, &updatedAt); err != nil {
			t.Fatalf("read %s attachment: %v", initialState, err)
		}
		if statusValue != wantState {
			t.Fatalf("%s attachment status = %q; want %q", initialState, statusValue, wantState)
		}
		if initialState != "active" && updatedAt != "2026-01-01T11:00:00Z" {
			t.Fatalf("%s attachment updated_at = %q; want unchanged", initialState, updatedAt)
		}
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	var eventCount, usageCount int
	var sessionUsageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_attachment_consumed'
		    AND model_request_id = 'mreq_bridge_attachment_consumed' AND type = 'span.model_request_end'`).Scan(&eventCount); err != nil {
		t.Fatalf("count request-end events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM request_usage_details
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_attachment_consumed'
		    AND model_request_id = 'mreq_bridge_attachment_consumed'`).Scan(&usageCount); err != nil {
		t.Fatalf("count request usage details: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_attachment_consumed'`).Scan(&sessionUsageJSON); err != nil {
		t.Fatalf("read cumulative usage: %v", err)
	}
	if eventCount != 1 || usageCount != 1 || testJSONPathInt(t, sessionUsageJSON, "input_tokens") != 1 || testJSONPathInt(t, sessionUsageJSON, "output_tokens") != 1 {
		t.Fatalf("replayed settlement event=%d usage=%d cumulative=%s; want exactly-once 1/1", eventCount, usageCount, sessionUsageJSON)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndErrorKeepsBothAttachmentChannelsPending(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_attachment_error"
		threadID  = "thr_bridge_attachment_error"
		sourceID  = "sevt_bridge_attachment_error"
		fileID    = "file_bridge_attachment_error"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_attachment_error", 1, "pod_bridge_attachment_error")
	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_attachment_error", 1, "pod_bridge_attachment_error")
	transient := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_error", "sevt_attachment_error_tool", []byte("transient"))
	seedBridgeAPIFileAttachment(t, admin, blobStore, fileID, "error.png", "image/png", "file")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_attachment_error"}}]}`)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_attachment_error_start", ModelRequestId: "mreq_bridge_attachment_error",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_attachment_error"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent error start: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_attachment_error_end", ModelRequestId: "mreq_bridge_attachment_error",
		ModelRequestStartEventId: start.GetEventId(), IsError: true, ErrorKind: "provider_error",
		FinishReason: "error", UsageJson: `{"input_tokens":1,"output_tokens":0}`,
		ConsumedAttachmentRefs:  []string{transient.GetAttachmentRef()},
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceID, FileId: fileID}},
	}); err != nil {
		t.Fatalf("WriteRequestEnd error: %v", err)
	}

	var eventCount, usageCount, fileConsumptionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND model_request_id = 'mreq_bridge_attachment_error' AND type = 'span.model_request_end'`,
		sessionID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count error request ends: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM request_usage_details
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND model_request_id = 'mreq_bridge_attachment_error'`,
		sessionID,
	).Scan(&usageCount); err != nil {
		t.Fatalf("count error usage rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_file_attachment_consumptions
		  WHERE workspace_id = 'default' AND session_id = $1`,
		sessionID,
	).Scan(&fileConsumptionCount); err != nil {
		t.Fatalf("count error file consumptions: %v", err)
	}
	if eventCount != 1 || usageCount != 1 {
		t.Fatalf("error settlement event/usage = %d/%d; want 1/1", eventCount, usageCount)
	}
	if fileConsumptionCount != 0 || bridgeTransientAttachmentStatus(t, admin, transient.GetAttachmentRef()) != "active" {
		t.Fatalf("error consumption file=%d transient=%s; want 0/active",
			fileConsumptionCount, bridgeTransientAttachmentStatus(t, admin, transient.GetAttachmentRef()))
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRecordsServedFileAttachmentConsumptionExactlyOnce(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_file_consumed"
		threadID  = "thr_bridge_file_consumed"
		sourceID  = "sevt_bridge_file_consumed"
		fileID    = "file_bridge_file_consumed"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_file_consumed", 1, "pod_bridge_file_consumed")
	blobStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, blobStore, fileID, "consumed.png", "image/png", "consumed")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_file_consumed"}}]}`)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_file_consumed", 1, "pod_bridge_file_consumed")
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_file_consumed_start", ModelRequestId: "mreq_bridge_file_consumed",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_file_consumed"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	request := &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_file_consumed_end", ModelRequestId: "mreq_bridge_file_consumed",
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "stop", UsageJson: `{"input_tokens":1,"output_tokens":1}`,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceID, FileId: fileID}},
	}
	if _, err := store.WriteRequestEnd(context.Background(), request); err != nil {
		t.Fatalf("WriteRequestEnd served file attachment: %v", err)
	}
	var consumptionCount int
	var requestEndEventID string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*), min(request_end_event_id)
		   FROM session_file_attachment_consumptions
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2
		    AND source_event_id = $3 AND file_id = $4`,
		sessionID, threadID, sourceID, fileID).Scan(&consumptionCount, &requestEndEventID); err != nil {
		t.Fatalf("read file attachment consumption: %v", err)
	}
	var requestEndType string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT type FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1 AND session_thread_id = $2 AND event_id = $3`,
		sessionID, threadID, requestEndEventID).Scan(&requestEndType); err != nil {
		t.Fatalf("read consumption request-end event: %v", err)
	}
	if consumptionCount != 1 || requestEndType != "span.model_request_end" {
		t.Fatalf("consumption count/event type = %d/%q; want 1/span.model_request_end", consumptionCount, requestEndType)
	}
	replay, err := store.WriteRequestEnd(context.Background(), request)
	if err != nil {
		t.Fatalf("WriteRequestEnd file attachment replay: %v", err)
	}
	if replay.GetAck().GetStatus() != bridgev1.BridgeWriteStatus_BRIDGE_WRITE_STATUS_DUPLICATE {
		t.Fatalf("file attachment replay ack = %s; want duplicate", replay.GetAck().GetStatus())
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_file_attachment_consumptions
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&consumptionCount); err != nil {
		t.Fatalf("count replayed file consumptions: %v", err)
	}
	if consumptionCount != 1 {
		t.Fatalf("replayed file consumption count = %d; want 1", consumptionCount)
	}
	divergent := proto.Clone(request).(*bridgev1.WriteRequestEndRequest)
	divergent.ConsumedFileAttachments = []*bridgev1.FileAttachmentPair{{
		SourceEventId: sourceID,
		FileId:        "file_bridge_file_consumed_other",
	}}
	if _, err := store.WriteRequestEnd(context.Background(), divergent); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("divergent file consumption replay err = %v; want AlreadyExists", err)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndConsumesDeletedFileAttachmentRide(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_deleted_file_consumed"
		threadID  = "thr_bridge_deleted_file_consumed"
		sourceID  = "sevt_bridge_deleted_file_consumed"
		fileID    = "file_bridge_deleted_file_consumed"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_deleted_file_consumed", 1, "pod_bridge_deleted_file_consumed")
	blobStore := blob.NewFakeBlobStore()
	seedBridgeAPIFileAttachment(t, admin, blobStore, fileID, "deleted.png", "image/png", "deleted")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_deleted_file_consumed"}}]}`)
	if _, err := admin.ExecContext(context.Background(),
		`DELETE FROM files WHERE workspace_id = 'default' AND file_id = $1`,
		fileID,
	); err != nil {
		t.Fatalf("delete file after admission: %v", err)
	}

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_deleted_file_consumed", 1, "pod_bridge_deleted_file_consumed")
	metadata, err := store.ResolveFileAttachmentMetadata(context.Background(), &bridgev1.ResolveFileAttachmentMetadataRequest{
		Scope:       scope,
		Attachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceID, FileId: fileID}},
	})
	if err != nil {
		t.Fatalf("ResolveFileAttachmentMetadata deleted file: %v", err)
	}
	if len(metadata.GetAttachments()) != 1 ||
		metadata.GetAttachments()[0].GetRejected().GetReason() != bridgev1.FileAttachmentRejectionReason_FILE_ATTACHMENT_REJECTION_REASON_DELETED {
		t.Fatalf("deleted file metadata = %+v; want one deleted item", metadata.GetAttachments())
	}
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_deleted_file_consumed_start", ModelRequestId: "mreq_bridge_deleted_file_consumed",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_deleted_file_consumed"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent start: %v", err)
	}
	if _, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_deleted_file_consumed_end", ModelRequestId: "mreq_bridge_deleted_file_consumed",
		ModelRequestStartEventId: start.GetEventId(), FinishReason: "stop", UsageJson: `{"input_tokens":2,"output_tokens":1}`,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceID, FileId: fileID}},
	}); err != nil {
		t.Fatalf("WriteRequestEnd served deleted file attachment: %v", err)
	}

	var eventCount, usageCount, consumptionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND model_request_id = 'mreq_bridge_deleted_file_consumed'
		    AND type = 'span.model_request_end'`,
		sessionID,
	).Scan(&eventCount); err != nil {
		t.Fatalf("count deleted-file request-end events: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM request_usage_details
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND model_request_id = 'mreq_bridge_deleted_file_consumed'`,
		sessionID,
	).Scan(&usageCount); err != nil {
		t.Fatalf("count deleted-file usage rows: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_file_attachment_consumptions
		  WHERE workspace_id = 'default' AND session_id = $1
		    AND source_event_id = $2 AND file_id = $3`,
		sessionID,
		sourceID,
		fileID,
	).Scan(&consumptionCount); err != nil {
		t.Fatalf("count deleted-file consumptions: %v", err)
	}
	if eventCount != 1 || usageCount != 1 || consumptionCount != 1 {
		t.Fatalf("deleted-file settlement event/usage/consumption = %d/%d/%d; want 1/1/1", eventCount, usageCount, consumptionCount)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndDoesNotConsumeAttachmentsOnReschedule(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_bridge_attachment_reschedule"
		threadID  = "thr_bridge_attachment_reschedule"
		sourceID  = "sevt_bridge_attachment_reschedule"
		fileID    = "file_bridge_attachment_reschedule"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, threadID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_attachment_reschedule", 1, "pod_bridge_attachment_reschedule")
	blobStore := blob.NewFakeBlobStore()
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blobStore
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_attachment_reschedule", 1, "pod_bridge_attachment_reschedule")
	transient := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_reschedule", "sevt_attachment_reschedule_tool", []byte("transient"))
	seedBridgeAPIFileAttachment(t, admin, blobStore, fileID, "reschedule.png", "image/png", "file")
	seedBridgeAPIEvent(t, admin, "default", sessionID, threadID, sourceID, 1, "user.message",
		`{"content":[{"type":"image","source":{"type":"file","file_id":"file_bridge_attachment_reschedule"}}]}`)
	start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_attachment_reschedule_start", ModelRequestId: "mreq_bridge_attachment_reschedule",
		EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request", PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_bridge_attachment_reschedule"}`,
	})
	if err != nil {
		t.Fatalf("WriteEvent reschedule start: %v", err)
	}
	response, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		Scope: scope, RuntimeWriteId: "rwrite_bridge_attachment_reschedule_end", ModelRequestId: "mreq_bridge_attachment_reschedule",
		ModelRequestStartEventId: start.GetEventId(), IsError: true, ErrorKind: "provider_error",
		FinishReason: "error", UsageJson: `{"input_tokens":1,"output_tokens":0}`,
		ConsumedAttachmentRefs:  []string{transient.GetAttachmentRef()},
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{SourceEventId: sourceID, FileId: fileID}},
		Reschedule: &bridgev1.RequestEndReschedule{
			Attempt: 1, Deadline: "2026-01-01T12:00:30Z", BackoffMs: 1000,
		},
	})
	if err != nil {
		t.Fatalf("WriteRequestEnd reschedule: %v", err)
	}
	if got := response.GetDeclaration().GetReceipts()[0].GetRequestReschedule(); got.GetDisposition() != bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED {
		t.Fatalf("reschedule disposition = %+v; want accepted", got)
	}
	var fileConsumptionCount int
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_file_attachment_consumptions
		  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&fileConsumptionCount); err != nil {
		t.Fatalf("count rescheduled file consumptions: %v", err)
	}
	if fileConsumptionCount != 0 || bridgeTransientAttachmentStatus(t, admin, transient.GetAttachmentRef()) != "active" {
		t.Fatalf("rescheduled consumption file=%d transient=%s; want 0/active",
			fileConsumptionCount, bridgeTransientAttachmentStatus(t, admin, transient.GetAttachmentRef()))
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRescheduleStillValidatesAttachmentScope(t *testing.T) {
	for _, test := range []struct {
		name      string
		transient []string
		files     []*bridgev1.FileAttachmentPair
	}{
		{
			name:      "missing transient ref",
			transient: []string{"att_reschedule_missing"},
		},
		{
			name: "file source outside scoped thread",
			files: []*bridgev1.FileAttachmentPair{{
				SourceEventId: "sevt_reschedule_other_thread",
				FileId:        "file_reschedule_other_thread",
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			suffix := strings.ReplaceAll(test.name, " ", "_")
			sessionID := "sesn_reschedule_scope_" + suffix
			threadID := "thr_reschedule_scope_" + suffix
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_reschedule_scope", 1, "pod_reschedule_scope")
			if len(test.files) > 0 {
				otherThreadID := threadID + "_other"
				seedBridgeAPIChildThread(t, admin, "default", sessionID, threadID, otherThreadID)
				seedBridgeAPIEvent(t, admin, "default", sessionID, otherThreadID, test.files[0].GetSourceEventId(), 1, "user.message",
					`{"content":[{"type":"image","source":{"type":"file","file_id":"file_reschedule_other_thread"}}]}`)
			}
			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
			scope := bridgeAPIScope(sessionID, threadID, "bind_reschedule_scope", 1, "pod_reschedule_scope")
			start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reschedule_scope_start_" + suffix,
				ModelRequestId: "mreq_reschedule_scope_" + suffix, EventType: "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
				PayloadJson: `{"type":"span.model_request_start","model_request_id":"mreq_reschedule_scope_` + suffix + `"}`,
			})
			if err != nil {
				t.Fatalf("WriteEvent start: %v", err)
			}
			_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
				Scope: scope, RuntimeWriteId: "rwrite_reschedule_scope_end_" + suffix,
				ModelRequestId: "mreq_reschedule_scope_" + suffix, ModelRequestStartEventId: start.GetEventId(),
				IsError: true, ErrorKind: "provider_error", UsageJson: `{"input_tokens":1,"output_tokens":0}`,
				ConsumedAttachmentRefs: test.transient, ConsumedFileAttachments: test.files,
				Reschedule: &bridgev1.RequestEndReschedule{
					Attempt: 1, Deadline: "2026-01-01T12:00:30Z", BackoffMs: 1000,
				},
			})
			if status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("reschedule invalid attachment err = %v; want scope rejection", err)
			}
			var requestEndCount int
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'span.model_request_end'`,
				sessionID,
			).Scan(&requestEndCount); err != nil {
				t.Fatalf("count rejected request ends: %v", err)
			}
			if requestEndCount != 0 {
				t.Fatalf("rejected reschedule request ends = %d; want 0", requestEndCount)
			}
		})
	}
}

func TestConsumedTransientAttachmentHashEncodingIsUnambiguous(t *testing.T) {
	one, err := normalizeConsumedAttachmentRefs([]string{"att_a\x00att_b"})
	if err != nil {
		t.Fatalf("normalize one ref: %v", err)
	}
	two, err := normalizeConsumedAttachmentRefs([]string{"att_a", "att_b"})
	if err != nil {
		t.Fatalf("normalize two refs: %v", err)
	}
	reordered, err := normalizeConsumedAttachmentRefs([]string{"att_b", "att_a"})
	if err != nil {
		t.Fatalf("normalize reordered refs: %v", err)
	}
	oneHash := bridgeRequestHash("write_request_end", one.CanonicalJSON)
	twoHash := bridgeRequestHash("write_request_end", two.CanonicalJSON)
	reorderedHash := bridgeRequestHash("write_request_end", reordered.CanonicalJSON)
	if oneHash == twoHash || twoHash == reorderedHash {
		t.Fatalf("canonical ref hashes collide one=%s two=%s reordered=%s", oneHash, twoHash, reorderedHash)
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsUnknownAttachmentRefsAtomically(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_unknown", "thr_bridge_attachment_unknown")
	seedBridgeAPIRuntimeBinding(t, admin, "default", "sesn_bridge_attachment_unknown", "bind_bridge_attachment_unknown", 1, "pod_uid_attachment_unknown")
	seedBridgeAPISession(t, admin, "default", "sesn_bridge_attachment_foreign", "thr_bridge_attachment_foreign")

	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
	store.AttachmentBlobStore = blob.NewFakeBlobStore()
	store.Clock = func() time.Time { return time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC) }
	scope := bridgeAPIScope("sesn_bridge_attachment_unknown", "thr_bridge_attachment_unknown", "bind_bridge_attachment_unknown", 1, "pod_uid_attachment_unknown")
	owned := createBridgeTransientAttachmentForTest(t, store, scope, "attachment_unknown_owned", "sevt_attachment_unknown_owned", []byte("owned"))

	foreignRef := "att_bridge_attachment_foreign"
	if _, err := admin.ExecContext(context.Background(),
		`INSERT INTO session_transient_attachments (
			workspace_id, attachment_ref, session_id, session_thread_id, source_tool_use_event_id,
			blob_pointer, mime, metadata_json, status, expires_at, created_at, updated_at
		) VALUES ('default', $1, 'sesn_bridge_attachment_foreign', 'thr_bridge_attachment_foreign',
			'sevt_bridge_attachment_foreign', 'foreign/blob', 'image/png', '{}', 'active',
			'2026-01-01T13:00:00Z', '2026-01-01T12:00:00Z', '2026-01-01T12:00:00Z')`, foreignRef); err != nil {
		t.Fatalf("seed foreign attachment: %v", err)
	}

	for index, unknownRef := range []string{foreignRef, "att_bridge_attachment_missing"} {
		modelRequestID := fmt.Sprintf("mreq_bridge_attachment_unknown_%d", index)
		start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
			Scope:          scope,
			RuntimeWriteId: "rwrite_bridge_attachment_unknown_start_" + strconv.Itoa(index),
			ModelRequestId: modelRequestID,
			EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
			PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
		})
		if err != nil {
			t.Fatalf("WriteEvent start %d: %v", index, err)
		}
		_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
			Scope:                    scope,
			RuntimeWriteId:           "rwrite_bridge_attachment_unknown_end_" + strconv.Itoa(index),
			ModelRequestId:           modelRequestID,
			ModelRequestStartEventId: start.GetEventId(),
			UsageJson:                `{"input_tokens":3,"output_tokens":2}`,
			ConsumedAttachmentRefs:   []string{owned.GetAttachmentRef(), unknownRef},
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("WriteRequestEnd unknown ref %q err = %v; want FailedPrecondition", unknownRef, err)
		}
	}

	var endCount, usageCount int
	var usageJSON string
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM session_events
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_attachment_unknown'
		    AND type = 'span.model_request_end'`).Scan(&endCount); err != nil {
		t.Fatalf("count rejected request ends: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT count(*) FROM request_usage_details
		  WHERE workspace_id = 'default' AND session_id = 'sesn_bridge_attachment_unknown'`).Scan(&usageCount); err != nil {
		t.Fatalf("count rejected usage: %v", err)
	}
	if err := admin.QueryRowContext(context.Background(),
		`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = 'sesn_bridge_attachment_unknown'`).Scan(&usageJSON); err != nil {
		t.Fatalf("read rejected cumulative usage: %v", err)
	}
	if endCount != 0 || usageCount != 0 || usageJSON != "{}" || bridgeTransientAttachmentStatus(t, admin, owned.GetAttachmentRef()) != "active" || bridgeTransientAttachmentStatus(t, admin, foreignRef) != "active" {
		t.Fatalf("rejected attachment settlement mutated end=%d usage=%d cumulative=%s owned=%s foreign=%s",
			endCount, usageCount, usageJSON, bridgeTransientAttachmentStatus(t, admin, owned.GetAttachmentRef()), bridgeTransientAttachmentStatus(t, admin, foreignRef))
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndBoundsConsumedAttachmentsAtomically(t *testing.T) {
	for _, count := range []int{MaxProviderRequestAttachments, MaxProviderRequestAttachments + 1} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
			sessionID := "sesn_bridge_attachment_bound_" + strconv.Itoa(count)
			threadID := "thr_bridge_attachment_bound_" + strconv.Itoa(count)
			seedBridgeAPISession(t, admin, "default", sessionID, threadID)
			seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, "bind_bridge_attachment_bound", 1, "pod_uid_attachment_bound")

			store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtime))
			scope := bridgeAPIScope(sessionID, threadID, "bind_bridge_attachment_bound", 1, "pod_uid_attachment_bound")
			refs := make([]string, 0, count)
			for index := 0; index < count; index++ {
				ref := fmt.Sprintf("att_bridge_attachment_bound_%d_%02d", count, index)
				refs = append(refs, ref)
				if _, err := admin.ExecContext(context.Background(),
					`INSERT INTO session_transient_attachments (
						workspace_id, attachment_ref, session_id, session_thread_id, source_tool_use_event_id,
						blob_pointer, mime, metadata_json, status, expires_at, created_at, updated_at
					) VALUES ('default', $1, $2, $3, $4, $5, 'image/png', '{}', 'active',
						'2026-01-01T13:00:00Z', '2026-01-01T12:00:00Z', '2026-01-01T12:00:00Z')`,
					ref, sessionID, threadID, "sevt_"+ref, "blob/"+ref); err != nil {
					t.Fatalf("seed attachment %d: %v", index, err)
				}
			}
			modelRequestID := "mreq_bridge_attachment_bound_" + strconv.Itoa(count)
			start, err := store.WriteEvent(context.Background(), &bridgev1.WriteEventRequest{
				Scope:          scope,
				RuntimeWriteId: "rwrite_bridge_attachment_bound_start_" + strconv.Itoa(count),
				ModelRequestId: modelRequestID,
				EventType:      "span.model_request_start", ContextThroughMessageSequence: bridgeAPIInt64(0), RequestKind: "agent_provider_request",
				PayloadJson: `{"type":"span.model_request_start","model_request_id":"` + modelRequestID + `"}`,
			})
			if err != nil {
				t.Fatalf("WriteEvent start: %v", err)
			}
			_, err = store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
				Scope:                    scope,
				RuntimeWriteId:           "rwrite_bridge_attachment_bound_end_" + strconv.Itoa(count),
				ModelRequestId:           modelRequestID,
				ModelRequestStartEventId: start.GetEventId(),
				UsageJson:                `{"input_tokens":5,"output_tokens":4}`,
				ConsumedAttachmentRefs:   refs,
			})

			wantCommitted := count == MaxProviderRequestAttachments
			if wantCommitted && err != nil {
				t.Fatalf("WriteRequestEnd with %d attachments: %v", count, err)
			}
			if !wantCommitted && status.Code(err) != codes.InvalidArgument {
				t.Fatalf("WriteRequestEnd with %d attachments err = %v; want InvalidArgument", count, err)
			}
			var endCount, usageCount, consumedCount int
			var usageJSON string
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_events
				  WHERE workspace_id = 'default' AND session_id = $1 AND type = 'span.model_request_end'`, sessionID).Scan(&endCount); err != nil {
				t.Fatalf("count request ends: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM request_usage_details
				  WHERE workspace_id = 'default' AND session_id = $1`, sessionID).Scan(&usageCount); err != nil {
				t.Fatalf("count usage details: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT count(*) FROM session_transient_attachments
				  WHERE workspace_id = 'default' AND session_id = $1 AND status = 'consumed'`, sessionID).Scan(&consumedCount); err != nil {
				t.Fatalf("count consumed attachments: %v", err)
			}
			if err := admin.QueryRowContext(context.Background(),
				`SELECT usage_json FROM sessions WHERE workspace_id = 'default' AND id = $1`, sessionID).Scan(&usageJSON); err != nil {
				t.Fatalf("read cumulative usage: %v", err)
			}
			if wantCommitted {
				if endCount != 1 || usageCount != 1 || consumedCount != count || testJSONPathInt(t, usageJSON, "input_tokens") != 5 || testJSONPathInt(t, usageJSON, "output_tokens") != 4 {
					t.Fatalf("accepted bound effects end=%d usage=%d consumed=%d cumulative=%s", endCount, usageCount, consumedCount, usageJSON)
				}
			} else if endCount != 0 || usageCount != 0 || consumedCount != 0 || usageJSON != "{}" {
				t.Fatalf("rejected overflow effects end=%d usage=%d consumed=%d cumulative=%s", endCount, usageCount, consumedCount, usageJSON)
			}
		})
	}
}

func TestPostgreSQLBridgeAPIStoreWriteRequestEndRejectsCombinedAttachmentOverflow(t *testing.T) {
	refs := make([]string, MaxProviderRequestAttachments)
	for index := range refs {
		refs[index] = fmt.Sprintf("att_combined_bound_%02d", index)
	}
	store := NewPostgreSQLBridgeAPIStore(nil)
	_, err := store.WriteRequestEnd(context.Background(), &bridgev1.WriteRequestEndRequest{
		RuntimeWriteId:           "rwrite_combined_attachment_bound",
		ModelRequestId:           "mreq_combined_attachment_bound",
		ModelRequestStartEventId: "sevt_combined_attachment_bound_start",
		ConsumedAttachmentRefs:   refs,
		ConsumedFileAttachments: []*bridgev1.FileAttachmentPair{{
			SourceEventId: "sevt_combined_attachment_bound_source",
			FileId:        "file_combined_attachment_bound",
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("combined attachment overflow err = %v; want InvalidArgument", err)
	}
}
