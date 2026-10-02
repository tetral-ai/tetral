package agentruntimebridge

import (
	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// Runtime declares only provider-visible context. Bridge validates storage
// bounds and the closed wire union, then persists the exact normalized facts.
func canonicalRuntimeContextParts(delta *bridgev1.RuntimeContextDelta) ([]map[string]any, error) {
	if delta == nil || len(delta.GetParts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "runtime context delta is empty")
	}
	parts := make([]map[string]any, 0, len(delta.GetParts()))
	for _, declared := range delta.GetParts() {
		if declared == nil {
			return nil, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		var part map[string]any
		switch content := declared.GetContent().(type) {
		case *bridgev1.RuntimeContextPart_Text:
			if content.Text == nil {
				return nil, status.Error(codes.InvalidArgument, "runtime text context is invalid")
			}
			if runtimecontrol.RuntimeJSONBytes(content.Text.GetText()) > runtimecontrol.RuntimeContextTextJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime text context exceeds its provider bound")
			}
			part = map[string]any{"type": "text", "text": content.Text.GetText()}
		case *bridgev1.RuntimeContextPart_Reasoning:
			if content.Reasoning == nil {
				return nil, status.Error(codes.InvalidArgument, "runtime reasoning context is invalid")
			}
			if runtimecontrol.RuntimeJSONBytes(content.Reasoning.GetText()) > runtimecontrol.RuntimeContextTextJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime reasoning context exceeds its provider bound")
			}
			part = map[string]any{"type": "reasoning", "text": content.Reasoning.GetText()}
			if content.Reasoning.ProviderMetadataJson != nil {
				metadata, err := runtimecontrol.DecodeRuntimeDeclarationObject(content.Reasoning.GetProviderMetadataJson())
				if err != nil || runtimecontrol.RuntimeJSONBytes(metadata) > runtimecontrol.RuntimeProviderMetadataMaxBytes {
					return nil, status.Error(codes.InvalidArgument, "runtime reasoning metadata is invalid")
				}
				part["providerMetadata"] = metadata
			}
		case *bridgev1.RuntimeContextPart_ToolCall:
			call := content.ToolCall
			if call == nil || !runtimecontrol.RuntimeAlreadyCanonicalIdentifier(call.GetModelToolCallId()) || !runtimecontrol.RuntimeAlreadyCanonicalIdentifier(call.GetToolName()) {
				return nil, status.Error(codes.InvalidArgument, "runtime tool call context is invalid")
			}
			input, err := runtimecontrol.DecodeRuntimeDeclarationValue(call.GetProviderInputJson())
			if err != nil || runtimecontrol.RuntimeJSONBytes(input) > runtimecontrol.RuntimeToolInputJSONMaxBytes {
				return nil, status.Error(codes.InvalidArgument, "runtime tool call input is invalid")
			}
			part = map[string]any{
				"type": "tool_call", "modelToolCallId": call.GetModelToolCallId(),
				"toolName": call.GetToolName(), "canonicalInput": input,
			}
		case *bridgev1.RuntimeContextPart_ToolResult:
			result := content.ToolResult
			if result == nil || !runtimecontrol.RuntimeAlreadyCanonicalIdentifier(result.GetModelToolCallId()) {
				return nil, status.Error(codes.InvalidArgument, "runtime tool result context is invalid")
			}
			outcome, err := runtimecontrol.CanonicalRuntimeToolResultOutcome(result)
			if err != nil {
				return nil, err
			}
			part = map[string]any{
				"type": "tool_result", "modelToolCallId": result.GetModelToolCallId(), "result": outcome,
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		parts = append(parts, part)
	}
	return parts, nil
}

func runtimeContextPartKind(part *bridgev1.RuntimeContextPart) string {
	if part == nil {
		return ""
	}
	switch part.GetContent().(type) {
	case *bridgev1.RuntimeContextPart_Text:
		return "text"
	case *bridgev1.RuntimeContextPart_Reasoning:
		return "reasoning"
	case *bridgev1.RuntimeContextPart_ToolCall:
		return "tool_call"
	case *bridgev1.RuntimeContextPart_ToolResult:
		return "tool_result"
	default:
		return ""
	}
}

func canonicalRuntimeContextDelta(delta *bridgev1.RuntimeContextDelta) (any, error) {
	if delta == nil {
		return nil, nil
	}
	parts, err := canonicalRuntimeContextParts(delta)
	if err != nil {
		return nil, err
	}
	return map[string]any{"parts": parts}, nil
}

func canonicalRuntimeToolError(value *bridgev1.RuntimeToolError) (map[string]any, error) {
	if value == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
	}
	failure, err := runtimecontrol.DecodeRuntimeToolErrorJSON(value.GetErrorJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
	}
	return failure, nil
}
