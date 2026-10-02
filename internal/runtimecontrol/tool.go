package runtimecontrol

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type DurableToolExecution struct {
	ModelRequestID      string
	ModelToolCallID     string
	ToolName            string
	MCPServerName       string
	ProviderInputJSON   string
	InputJSON           string
	NormalizedInputHash string
	EvaluatedPermission string
	RouteCapability     string
}

func LoadDurableToolExecutionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	eventType string,
	lock bool,
) (DurableToolExecution, error) {
	query := `SELECT projection_json, model_request_id
		  FROM session_events
		 WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		   AND event_id = $4 AND type = $5`
	if lock {
		query += ` FOR UPDATE`
	}
	var projectionJSON string
	var modelRequestID sql.NullString
	if err := tx.QueryRow(ctx, query,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID, eventType,
	).Scan(&projectionJSON, &modelRequestID); dbconnect.IsNoRows(err) {
		return DurableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool use is missing")
	} else if err != nil {
		return DurableToolExecution{}, err
	}
	var projection struct {
		ModelToolCallID         string          `json:"model_tool_call_id"`
		ToolName                string          `json:"tool_name"`
		ProviderInput           json.RawMessage `json:"provider_input"`
		CanonicalExecutionInput json.RawMessage `json:"canonical_execution_input"`
		RouteCapability         string          `json:"route_capability"`
		EvaluatedPermission     string          `json:"evaluated_permission"`
		EventType               string          `json:"event_type"`
		MCPServerName           string          `json:"mcp_server_name"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil {
		return DurableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool use projection is invalid")
	}
	inputJSON, inputHash, err := CanonicalRunToolInput(string(projection.CanonicalExecutionInput))
	if err != nil || !modelRequestID.Valid || modelRequestID.String == "" || projection.ModelToolCallID == "" ||
		projection.ToolName == "" || len(projection.ProviderInput) == 0 ||
		!RuntimeToolRouteCapabilityAllowed(projection.RouteCapability) ||
		projection.EventType != eventType ||
		(projection.EvaluatedPermission != "allow" && projection.EvaluatedPermission != "ask" && projection.EvaluatedPermission != "deny") {
		return DurableToolExecution{}, status.Error(codes.FailedPrecondition, "durable tool execution facts are incomplete")
	}
	return DurableToolExecution{
		ModelRequestID:      modelRequestID.String,
		ModelToolCallID:     projection.ModelToolCallID,
		ToolName:            projection.ToolName,
		MCPServerName:       projection.MCPServerName,
		ProviderInputJSON:   string(projection.ProviderInput),
		InputJSON:           inputJSON,
		NormalizedInputHash: inputHash,
		EvaluatedPermission: projection.EvaluatedPermission,
		RouteCapability:     projection.RouteCapability,
	}, nil
}

// stableRuntimeID derives deterministic identities for durable replay keys.
func StableRuntimeID(parts ...string) string {
	hasher := sha256.New()
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len([]byte(part)))) // #nosec G115 -- identifiers are bounded below uint32 at protocol validation.
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return "stid_" + hex.EncodeToString(hasher.Sum(nil))
}

func SettleRuntimeToolPartTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	settlement *bridgev1.RuntimeToolSettlement,
	now time.Time,
) (ToolProjection, error) {
	if modelRequestID == "" || settlement == nil || settlement.GetToolUseEventId() == "" {
		return ToolProjection{}, status.Error(codes.InvalidArgument, "runtime Tool settlement is incomplete")
	}
	var eventType string
	if err := tx.QueryRow(ctx,
		`SELECT type FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND event_id=$4 AND type IN ('agent.tool_use','agent.mcp_tool_use')
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), settlement.GetToolUseEventId(),
	).Scan(&eventType); dbconnect.IsNoRows(err) {
		return ToolProjection{}, status.Error(codes.FailedPrecondition, "Tool settlement target is missing")
	} else if err != nil {
		return ToolProjection{}, err
	}
	tool, err := LoadDurableToolExecutionTx(ctx, tx, scope, settlement.GetToolUseEventId(), eventType, false)
	if err != nil {
		return ToolProjection{}, err
	}
	if tool.ModelRequestID != modelRequestID {
		return ToolProjection{}, status.Error(codes.FailedPrecondition, "Tool settlement request identity is inconsistent")
	}
	result := &bridgev1.RuntimeContextToolResult{ModelToolCallId: tool.ModelToolCallID}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		output, err := DecodeRuntimeDeclarationValue(outcome.Completed.GetOutputJson())
		if err != nil || ValidateRuntimeBoundedText(output) != nil {
			return ToolProjection{}, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		outputObject := output.(map[string]any)
		contextOutputJSON, err := MarshalJSON(map[string]any{"text": outputObject["text"]})
		if err != nil {
			return ToolProjection{}, err
		}
		result.Outcome = &bridgev1.RuntimeContextToolResult_Completed{Completed: &bridgev1.RuntimeContextToolCompleted{OutputJson: contextOutputJSON}}
	case *bridgev1.RuntimeToolSettlement_Error:
		result.Outcome = &bridgev1.RuntimeContextToolResult_Error{Error: &bridgev1.RuntimeContextToolError{ErrorJson: outcome.Error.GetErrorJson()}}
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		result.Outcome = &bridgev1.RuntimeContextToolResult_Cancelled{Cancelled: &bridgev1.RuntimeContextToolCancelled{}}
	default:
		return ToolProjection{}, status.Error(codes.InvalidArgument, "Tool settlement outcome is missing")
	}
	resultValue, err := CanonicalRuntimeToolResultOutcome(result)
	if err != nil {
		return ToolProjection{}, err
	}
	resultPartsJSON, err := json.Marshal([]map[string]any{{
		"type": "tool_result", "modelToolCallId": tool.ModelToolCallID, "result": resultValue,
	}})
	if err != nil {
		return ToolProjection{}, err
	}
	updateResult, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = jsonb_set(
		          data_json::jsonb,
		          '{parts}',
		          (data_json::jsonb -> 'parts') || $5::jsonb
		        )::text,
		        updated_at = $6
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND model_request_id = $4 AND kind = 'assistant'
		    AND jsonb_typeof(data_json::jsonb -> 'parts') = 'array'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		modelRequestID, string(resultPartsJSON), now,
	)
	if err != nil {
		return ToolProjection{}, err
	}
	if !RowsAffected(updateResult) {
		return ToolProjection{}, status.Error(codes.FailedPrecondition, "Tool settlement lost its durable message")
	}
	return runtimeToolProjectionFromSettlement(tool, settlement)
}

func runtimeToolProjectionFromSettlement(tool DurableToolExecution, settlement *bridgev1.RuntimeToolSettlement) (ToolProjection, error) {
	var result map[string]any
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		output, err := DecodeRuntimeDeclarationValue(outcome.Completed.GetOutputJson())
		if err != nil || ValidateRuntimeBoundedText(output) != nil {
			return ToolProjection{}, status.Error(codes.InvalidArgument, "runtime tool completion is invalid")
		}
		result = map[string]any{"type": "completed", "output": output}
	case *bridgev1.RuntimeToolSettlement_Error:
		toolError, err := DecodeRuntimeToolErrorJSON(outcome.Error.GetErrorJson())
		if err != nil {
			return ToolProjection{}, status.Error(codes.InvalidArgument, "runtime tool error is invalid")
		}
		result = map[string]any{"type": "error", "error": toolError}
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		result = map[string]any{"type": "cancelled"}
		if outcome.Cancelled.ErrorJson != nil {
			toolError, err := DecodeRuntimeToolErrorJSON(outcome.Cancelled.GetErrorJson())
			if err != nil {
				return ToolProjection{}, status.Error(codes.InvalidArgument, "runtime tool cancellation is invalid")
			}
			result["error"] = toolError
		}
	default:
		return ToolProjection{}, status.Error(codes.InvalidArgument, "Tool settlement outcome is missing")
	}
	return RuntimeToolProjectionFromDurableTool(tool, result), nil
}

// Runtime owns failure projection; Bridge accepts and stores only the strict
// durable Tool error contract used by Tool settlement and repair declarations.
func DecodeRuntimeToolErrorJSON(raw string) (map[string]any, error) {
	declared, err := DecodeRuntimeDeclarationObject(raw)
	if err != nil || validateRuntimeToolError(declared) != nil {
		return nil, fmt.Errorf("invalid durable Tool error")
	}
	return declared, nil
}

func RuntimeToolProjectionFromDurableTool(tool DurableToolExecution, result map[string]any) ToolProjection {
	projection := ToolProjection{
		ModelToolCallID:         tool.ModelToolCallID,
		ToolName:                tool.ToolName,
		ProviderInput:           json.RawMessage(tool.ProviderInputJSON),
		CanonicalExecutionInput: json.RawMessage(tool.InputJSON),
	}
	if result != nil {
		projection.State, _ = result["type"].(string)
		if output, ok := result["output"].(map[string]any); ok {
			encoded, _ := json.Marshal(output)
			var bounded struct {
				Text      string `json:"text"`
				Truncated bool   `json:"truncated"`
			}
			if json.Unmarshal(encoded, &bounded) == nil {
				projection.Output = &bounded
			}
		}
		if normalizedError, ok := result["error"].(map[string]any); ok {
			encoded, _ := json.Marshal(normalizedError)
			var failure struct {
				Type      string `json:"type"`
				Message   string `json:"message"`
				Retryable bool   `json:"retryable"`
			}
			if json.Unmarshal(encoded, &failure) == nil {
				projection.Error = &failure
			}
		}
	}
	return projection
}
