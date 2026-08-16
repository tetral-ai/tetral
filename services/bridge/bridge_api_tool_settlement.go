package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// SettleToolResult is the sole ordinary Runtime Tool-result mutation. Runtime
// selects the durable Tool Use and supplies its terminal outcome; Bridge derives
// the immutable Request/Tool family, writes the public Event, updates the
// Assistant projection, and settles executor bookkeeping in one transaction.
func (s *PostgreSQLBridgeAPIStore) SettleToolResult(
	ctx context.Context,
	request *bridgev1.SettleToolResultRequest,
) (response *bridgev1.SettleToolResultResponse, resultErr error) {
	settlement := request.GetSettlement()
	evidence := runtimeDeclarationRejectionEvidence{
		Kind:          "identity",
		Operation:     bridgeOpSettleToolResult,
		OperationID:   settlement.GetToolUseEventId(),
		MessageOrPart: "tool",
	}
	defer func() { logRuntimeDeclarationRejected(s.Logger, request.GetScope(), evidence, resultErr) }()
	if settlement == nil || settlement.GetToolUseEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Tool settlement target is required")
	}
	canonicalSettlement, err := canonicalRuntimeToolSettlement(settlement)
	if err != nil {
		return nil, err
	}
	serverToolUse, err := runtimeToolSettlementServerUsage(settlement)
	if err != nil {
		return nil, err
	}
	requestJSON, err := json.Marshal(map[string]any{
		"operation_kind": bridgeOpSettleToolResult,
		"settlement":     canonicalSettlement,
	})
	if err != nil {
		return nil, err
	}
	requestHash := bridgeRequestHash(string(requestJSON))
	toolUseEventID := settlement.GetToolUseEventId()
	now := s.now()
	var outcome string
	var stagedMCPResult stagedMCPResultIdentity
	var stagedMCPResultUsed bool
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.settle_tool_result", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		evidence.Kind = "authorization"
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		evidence.Kind = "transaction"
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpSettleToolResult, toolUseEventID); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "Tool settlement idempotency conflict")
			}
			outcome = "duplicate"
			return nil
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		evidence.ThreadRole = threadScope.role

		toolEventType, tool, err := loadDurableToolSettlementTargetTx(ctx, tx, request.GetScope(), toolUseEventID)
		if err != nil {
			return err
		}
		alreadySettled, err := durableToolResultExistsTx(ctx, tx, request.GetScope(), toolUseEventID)
		if err != nil {
			return err
		}
		if alreadySettled {
			outcome = "stale"
			return nil
		}
		resultEventType := "agent.tool_result"
		if toolEventType == "agent.mcp_tool_use" {
			resultEventType = "agent.mcp_tool_result"
		}
		if serverToolUse.Present && (resultEventType != "agent.tool_result" || tool.ToolName != "web" || tool.MCPServerName != "") {
			return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
		}

		projection, err := settleRuntimeToolPartTx(ctx, tx, request.GetScope(), tool.ModelRequestID, settlement, now)
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				outcome = "stale"
				return nil
			}
			return err
		}
		payloadJSON, err := durableToolResultPayloadJSON(resultEventType, toolUseEventID, settlement)
		if err != nil {
			return err
		}
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		visibility, sessionVisible := threadScope.publicProjection(resultEventType)
		projectionJSON, err := marshalBridgeJSON(projection)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id,
				projection_json, created_at, updated_at, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12, $12, $12)`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(),
			eventID, sequence, resultEventType, payloadJSON, visibility, sessionVisible, tool.ModelRequestID, projectionJSON, now,
		); err != nil {
			return err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
			return err
		}
		if err := applyToolEventBookkeepingTx(ctx, tx, request.GetScope(), eventID, resultEventType, payloadJSON, projection, now); err != nil {
			return err
		}
		if resultEventType == "agent.mcp_tool_result" && settlement.GetCompleted() != nil {
			stagedMCPResult, err = consumeStagedMCPResultTx(ctx, tx, request.GetScope(), resultEventType, payloadJSON, now)
			if err != nil {
				return err
			}
			stagedMCPResultUsed = stagedMCPResult.ToolUseEventID != ""
		}
		if serverToolUse.Present {
			if err := incrementSessionServerToolUsageTx(ctx, tx, request.GetScope(), serverToolUse, now); err != nil {
				return err
			}
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation: bridgeOpSettleToolResult, SourceKind: bridgeOpSettleToolResult,
			IdempotencyKey: toolUseEventID, RequestHash: requestHash, AckStatus: bridgeAckCommitted,
			RuntimeWriteID: sql.NullString{}, ResultJSON: "{}", Now: now,
		}); err != nil {
			return err
		}
		outcome = "committed"
		return nil
	}); err != nil {
		if isScopeSupersededError(err) {
			return toolSettlementStaleResponse(), nil
		}
		return nil, err
	}
	if stagedMCPResultUsed {
		logStagedMCPResultConsumed(s.Logger, request.GetScope(), stagedMCPResult)
	}
	switch outcome {
	case "committed":
		return &bridgev1.SettleToolResultResponse{Outcome: &bridgev1.SettleToolResultResponse_Committed{Committed: &bridgev1.ToolResultCommitted{}}}, nil
	case "duplicate":
		return &bridgev1.SettleToolResultResponse{Outcome: &bridgev1.SettleToolResultResponse_Duplicate{Duplicate: &bridgev1.ToolResultDuplicate{}}}, nil
	case "stale":
		return toolSettlementStaleResponse(), nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "Tool settlement outcome is missing")
	}
}

func durableToolResultExistsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM session_events
			 WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			   AND type IN ('agent.tool_result','agent.mcp_tool_result')
			   AND COALESCE(
			         payload_json::jsonb ->> 'tool_use_event_id',
			         payload_json::jsonb ->> 'tool_use_id',
			         payload_json::jsonb ->> 'mcp_tool_use_id'
			       ) = $4
		)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&exists)
	return exists, err
}

func toolSettlementStaleResponse() *bridgev1.SettleToolResultResponse {
	return &bridgev1.SettleToolResultResponse{Outcome: &bridgev1.SettleToolResultResponse_Stale{Stale: &bridgev1.ToolResultStale{}}}
}

func runtimeToolSettlementServerUsage(settlement *bridgev1.RuntimeToolSettlement) (normalizedServerToolUseUsage, error) {
	if settlement == nil {
		return normalizedServerToolUseUsage{CanonicalJSON: "null"}, nil
	}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		return normalizeServerToolUseUsage(outcome.Completed.GetServerToolUse())
	case *bridgev1.RuntimeToolSettlement_Error:
		return normalizeServerToolUseUsage(outcome.Error.GetServerToolUse())
	default:
		return normalizedServerToolUseUsage{CanonicalJSON: "null"}, nil
	}
}

func loadDurableToolSettlementTargetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
) (string, durableToolExecution, error) {
	var eventType string
	if err := tx.QueryRow(ctx,
		`SELECT type FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND event_id = $4 AND type IN ('agent.tool_use', 'agent.mcp_tool_use')
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&eventType); dbconnect.IsNoRows(err) {
		return "", durableToolExecution{}, status.Error(codes.FailedPrecondition, "durable Tool settlement target is missing")
	} else if err != nil {
		return "", durableToolExecution{}, err
	}
	tool, err := loadDurableToolExecutionTx(ctx, tx, scope, toolUseEventID, eventType, false)
	return eventType, tool, err
}

func durableToolResultPayloadJSON(eventType string, toolUseEventID string, settlement *bridgev1.RuntimeToolSettlement) (string, error) {
	payload := map[string]any{}
	if eventType == "agent.mcp_tool_result" {
		payload["mcp_tool_use_id"] = toolUseEventID
	} else {
		payload["tool_use_id"] = toolUseEventID
	}
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Completed:
		decoded, err := decodeRuntimeDeclarationValue(outcome.Completed.GetOutputJson())
		output, ok := decoded.(map[string]any)
		if err != nil || !ok || validateRuntimeBoundedText(output) != nil {
			return "", status.Error(codes.InvalidArgument, "Tool completion output is invalid")
		}
		text, _ := output["text"].(string)
		payload["content"] = []map[string]string{{"type": "text", "text": text}}
	case *bridgev1.RuntimeToolSettlement_Error:
		declaredError, err := decodeRuntimeToolErrorJSON(outcome.Error.GetErrorJson())
		if err != nil {
			return "", status.Error(codes.InvalidArgument, "Tool error is invalid")
		}
		message, _ := declaredError["message"].(string)
		payload["content"] = []map[string]string{{"type": "text", "text": message}}
		payload["is_error"] = true
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		payload["is_error"] = true
	default:
		return "", status.Error(codes.InvalidArgument, "Tool settlement outcome is missing")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal Tool Result payload: %w", err)
	}
	return string(encoded), nil
}
