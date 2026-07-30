package agentruntimebridge

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestStableRuntimeIDMatchesSharedVectors(t *testing.T) {
	messageID := stableRuntimeID(
		"runtime_message_draft",
		"ws_1",
		"ses_1",
		"thr_1",
		"messages",
		"rin_1",
		"user_message",
		"0",
	)
	if want := "stid_edcc6aa88ddf349e058ade77c0f169d73c6f3c0343d99cef1adc640fd486d82e"; messageID != want {
		t.Fatalf("message stable id = %q; want %q", messageID, want)
	}
	if got, want := stableRuntimeID("runtime_message_part_draft", messageID, "text", "0"),
		"stid_080e946f2be6e403aaaeb2afab207746171d95850660f612af67de33f1ed5828"; got != want {
		t.Fatalf("part stable id = %q; want %q", got, want)
	}
}

type runtimeDeclarationVector struct {
	SessionThreadID          string                          `json:"session_thread_id"`
	RuntimeInputID           string                          `json:"runtime_input_id"`
	EventIDs                 []string                        `json:"event_ids"`
	SequenceFrom             int64                           `json:"sequence_from"`
	SequenceTo               int64                           `json:"sequence_to"`
	InputKind                string                          `json:"input_kind"`
	RuntimeWriteID           string                          `json:"runtime_write_id"`
	ModelRequestID           string                          `json:"model_request_id"`
	ModelRequestStartEventID string                          `json:"model_request_start_event_id"`
	ModelToolCallID          string                          `json:"model_tool_call_id"`
	EventType                string                          `json:"event_type"`
	PayloadJSON              string                          `json:"payload_json"`
	FinishReason             string                          `json:"finish_reason"`
	UsageJSON                string                          `json:"usage_json"`
	RequestKind              string                          `json:"request_kind"`
	DurableTurnID            string                          `json:"durable_turn_id"`
	StopReasonJSON           string                          `json:"stop_reason_json"`
	FailureJSON              string                          `json:"failure_json"`
	ChildThreadID            string                          `json:"child_thread_id"`
	OperationKind            string                          `json:"operation_kind"`
	Action                   string                          `json:"action"`
	SourceKind               string                          `json:"source_kind"`
	SourceCommandID          string                          `json:"source_command_id"`
	RequestedAt              string                          `json:"requested_at"`
	ToolName                 string                          `json:"tool_name"`
	RepairKey                string                          `json:"repair_key"`
	TaskID                   string                          `json:"task_id"`
	ResultJSON               string                          `json:"result_json"`
	ToolUseEventID           string                          `json:"tool_use_event_id"`
	NormalizedInputHash      string                          `json:"normalized_input_hash"`
	MCPServerName            string                          `json:"mcp_server_name"`
	InputJSON                string                          `json:"input_json"`
	InlineMedia              []runtimeDeclarationInlineMedia `json:"inline_media"`
	Draft                    *runtimeDeclarationDraft        `json:"draft"`
	CanonicalJSON            string                          `json:"canonical_json"`
	Digest                   string                          `json:"digest"`
}

type runtimeDeclarationDraft struct {
	RuntimeLocalID  string                 `json:"runtime_local_id"`
	SourceKind      string                 `json:"source_kind"`
	SourceID        string                 `json:"source_id"`
	SourceEventID   string                 `json:"source_event_id"`
	Ordinal         int32                  `json:"ordinal"`
	MessageInfoJSON string                 `json:"message_info_json"`
	Part            runtimeDeclarationPart `json:"part"`
}

type runtimeDeclarationPart struct {
	RuntimeLocalPartID string `json:"runtime_local_part_id"`
	PartKind           string `json:"part_kind"`
	Ordinal            int32  `json:"ordinal"`
	PartJSON           string `json:"part_json"`
}

type runtimeDeclarationInlineMedia struct {
	Data              string `json:"data"`
	MIME              string `json:"mime"`
	SuggestedFilename string `json:"suggested_filename"`
}

func TestRuntimeDeclarationDigestsMatchSharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/runtime_declaration_vectors.json") //nolint:gosec // Repository-owned fixture.
	if err != nil {
		t.Fatalf("read declaration vectors: %v", err)
	}
	var corpus map[string]runtimeDeclarationVector
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("decode declaration vectors: %v", err)
	}

	families := []string{
		"commit_inputs",
		"write_event",
		"write_request_end",
		"finish_idle",
		"runtime_termination",
		"child_lifecycle",
		"internal_tool_repair",
		"task_notification",
		"mcp_materialization",
	}
	if len(corpus) != len(families) {
		t.Fatalf("declaration corpus has %d families; want %d", len(corpus), len(families))
	}
	for _, family := range families {
		vector, ok := corpus[family]
		if !ok {
			t.Fatalf("declaration corpus is missing %q", family)
		}
		t.Run(family, func(t *testing.T) {
			if got := sha256Hex(vector.CanonicalJSON); got != vector.Digest {
				t.Fatalf("canonical declaration digest = %q; want %q", got, vector.Digest)
			}
			got := runtimeDeclarationDigestForVector(t, family, vector)
			if got != vector.Digest {
				t.Fatalf("production declaration digest = %q; want %q", got, vector.Digest)
			}
		})
	}
}

func runtimeDeclarationDigestForVector(t *testing.T, family string, vector runtimeDeclarationVector) string {
	t.Helper()
	scope := &bridgev1.RuntimeScope{SessionThreadId: vector.SessionThreadID}

	var (
		digest string
		err    error
	)
	switch family {
	case "commit_inputs":
		request := &bridgev1.CommitInputsRequest{
			Scope:          scope,
			RuntimeInputId: vector.RuntimeInputID,
			EventIds:       vector.EventIDs,
			SequenceFrom:   vector.SequenceFrom,
			SequenceTo:     vector.SequenceTo,
			InputKind:      vector.InputKind,
			Drafts:         []*bridgev1.RuntimeMessageDraft{runtimeDeclarationDraftForVector(t, vector.Draft, bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT)},
		}
		digest, err = commitInputsDeclarationDigest(request, request.GetInputKind())
	case "write_event":
		request := &bridgev1.WriteEventRequest{
			Scope:          scope,
			RuntimeWriteId: vector.RuntimeWriteID,
			ModelRequestId: vector.ModelRequestID,
			EventType:      vector.EventType,
			PayloadJson:    vector.PayloadJSON,
		}
		stableReasoning := mustNormalizeStableReasoning(t, request)
		serverToolUse, normalizeErr := normalizeServerToolUseUsage(request)
		if normalizeErr != nil {
			t.Fatalf("normalize server tool use: %v", normalizeErr)
		}
		digest, err = writeEventDeclarationDigest(request, request.GetPayloadJson(), stableReasoning.CanonicalJSON, serverToolUse.CanonicalJSON)
	case "write_request_end":
		request := &bridgev1.WriteRequestEndRequest{
			Scope:                    scope,
			RuntimeWriteId:           vector.RuntimeWriteID,
			ModelRequestId:           vector.ModelRequestID,
			ModelRequestStartEventId: vector.ModelRequestStartEventID,
			FinishReason:             vector.FinishReason,
			UsageJson:                vector.UsageJSON,
			RequestKind:              vector.RequestKind,
		}
		stableReasoning := mustNormalizeStableReasoning(t, request)
		consumedTransient, normalizeErr := normalizeConsumedAttachmentRefs(request.GetConsumedAttachmentRefs())
		if normalizeErr != nil {
			t.Fatalf("normalize consumed attachment refs: %v", normalizeErr)
		}
		consumedFiles, normalizeErr := normalizeConsumedFileAttachments(request.GetConsumedFileAttachments())
		if normalizeErr != nil {
			t.Fatalf("normalize consumed file attachments: %v", normalizeErr)
		}
		requestKind, normalizeErr := normalizeRequestKind(request.GetRequestKind())
		if normalizeErr != nil {
			t.Fatalf("normalize request kind: %v", normalizeErr)
		}
		digest, err = writeRequestEndDeclarationDigest(
			request,
			requestKind,
			defaultString(request.GetFinishReason(), "unknown"),
			defaultString(request.GetUsageJson(), "{}"),
			stableReasoning.CanonicalJSON,
			consumedTransient.CanonicalJSON,
			consumedFiles.CanonicalJSON,
		)
	case "finish_idle":
		request := &bridgev1.FinishIdleRequest{
			Scope:          scope,
			DurableTurnId:  vector.DurableTurnID,
			StopReasonJson: vector.StopReasonJSON,
		}
		digest, err = finishIdleDeclarationDigest(request, request.GetStopReasonJson())
	case "runtime_termination":
		request := &bridgev1.CommitRuntimeTerminationRequest{
			Scope:          scope,
			RuntimeWriteId: vector.RuntimeWriteID,
			FailureJson:    vector.FailureJSON,
		}
		digest, err = runtimeTerminationDeclarationDigest(request, request.GetFailureJson())
	case "child_lifecycle":
		digest, err = childLifecycleDeclarationDigest(
			vector.OperationKind,
			vector.Action,
			vector.SessionThreadID,
			vector.ChildThreadID,
			vector.SourceKind,
			vector.SourceCommandID,
			vector.RequestedAt,
		)
	case "internal_tool_repair":
		request := &bridgev1.CommitInternalToolRepairRequest{
			Scope:           scope,
			ModelRequestId:  vector.ModelRequestID,
			ModelToolCallId: vector.ModelToolCallID,
			ToolName:        vector.ToolName,
			Drafts:          []*bridgev1.RuntimeMessageDraft{runtimeDeclarationDraftForVector(t, vector.Draft, bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_INTERNAL_TOOL_REPAIR)},
		}
		digest, err = internalToolRepairDeclarationDigest(request, vector.RepairKey)
	case "task_notification":
		request := &bridgev1.CommitTaskNotificationResultRequest{
			Scope:          scope,
			RuntimeInputId: vector.RuntimeInputID,
			TaskId:         vector.TaskID,
			ResultJson:     vector.ResultJSON,
			Draft:          runtimeDeclarationDraftForVector(t, vector.Draft, bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TASK_NOTIFICATION),
		}
		digest, err = taskNotificationDeclarationDigest(request, request.GetResultJson())
	case "mcp_materialization":
		request := &bridgev1.CommitMcpToolResultRequest{
			Scope:               scope,
			ToolUseEventId:      vector.ToolUseEventID,
			NormalizedInputHash: vector.NormalizedInputHash,
			McpServerName:       vector.MCPServerName,
			ToolName:            vector.ToolName,
			InputJson:           vector.InputJSON,
			ResultJson:          vector.ResultJSON,
			InlineMedia:         runtimeDeclarationInlineMediaForVector(t, vector.InlineMedia),
		}
		digest, err = mcpMaterializationDeclarationDigest(request)
	default:
		t.Fatalf("unsupported declaration family %q", family)
	}
	if err != nil {
		t.Fatalf("digest %s declaration: %v", family, err)
	}
	return digest
}

func mustNormalizeStableReasoning(t *testing.T, request stableReasoningCarrier) normalizedStableReasoningSet {
	t.Helper()
	normalized, err := normalizeStableReasoningParts(request)
	if err != nil {
		t.Fatalf("normalize stable reasoning: %v", err)
	}
	return normalized
}

func runtimeDeclarationDraftForVector(
	t *testing.T,
	vector *runtimeDeclarationDraft,
	kind bridgev1.RuntimeDraftKind,
) *bridgev1.RuntimeMessageDraft {
	t.Helper()
	if vector == nil {
		t.Fatal("declaration vector draft is missing")
	}
	return &bridgev1.RuntimeMessageDraft{
		RuntimeLocalId:  vector.RuntimeLocalID,
		SourceKind:      vector.SourceKind,
		SourceId:        vector.SourceID,
		SourceEventId:   vector.SourceEventID,
		DraftKind:       kind,
		Ordinal:         vector.Ordinal,
		MessageInfoJson: vector.MessageInfoJSON,
		Parts: []*bridgev1.RuntimePartDraft{{
			RuntimeLocalPartId: vector.Part.RuntimeLocalPartID,
			PartKind:           vector.Part.PartKind,
			Ordinal:            vector.Part.Ordinal,
			PartJson:           vector.Part.PartJSON,
		}},
	}
}

func runtimeDeclarationInlineMediaForVector(
	t *testing.T,
	vectors []runtimeDeclarationInlineMedia,
) []*bridgev1.McpInlineMedia {
	t.Helper()
	media := make([]*bridgev1.McpInlineMedia, 0, len(vectors))
	for _, vector := range vectors {
		data, err := base64.StdEncoding.DecodeString(vector.Data)
		if err != nil {
			t.Fatalf("decode inline media: %v", err)
		}
		media = append(media, &bridgev1.McpInlineMedia{
			Data:              data,
			Mime:              vector.MIME,
			SuggestedFilename: vector.SuggestedFilename,
		})
	}
	return media
}
