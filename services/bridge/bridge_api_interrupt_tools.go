package agentruntimebridge

import (
	"context"
	"database/sql"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type unfinishedRuntimeTool struct {
	eventID        string
	eventType      string
	eventSequence  int64
	modelRequestID string
}

type interruptedSandboxExecution struct {
	toolUseEventID                    string
	toolKind                          string
	executionState                    sql.NullString
	backgroundOperationKind           sql.NullString
	backgroundOperationState          sql.NullString
	executionAttemptGeneration        sql.NullInt64
	waitingActivationOperationID      sql.NullString
	waitingMaterializationOperationID sql.NullString
	providerCommandReference          sql.NullString
}

// settleInterruptedThreadToolsTx is the conversation-side interrupt terminal
// writer. It locks the complete durable unfinished-Tool census, writes one
// honest terminal result per target, and consumes execution ownership in the
// same transaction. Provider cancellation remains independent cleanup.
func settleInterruptedThreadToolsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	interruptEventID string,
	now time.Time,
) ([]*bridgev1.InterruptToolProjection, error) {
	tools, err := lockUnfinishedRuntimeToolsTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	projections := make([]*bridgev1.InterruptToolProjection, 0, len(tools))
	var cancellationJobs []queue.EnqueueRequest
	for _, tool := range tools {
		settlement, safeMessage, execution, err := interruptedToolOutcomeTx(ctx, tx, scope, tool.eventID)
		if err != nil {
			return nil, err
		}
		if execution != nil && execution.toolKind == "sandbox_tool" && execution.executionState.String == "running" &&
			execution.providerCommandReference.Valid && execution.providerCommandReference.String != "" {
			job, err := requestSandboxExecutionCancellationTx(ctx, tx, scope, *execution, now)
			if err != nil {
				return nil, err
			}
			cancellationJobs = append(cancellationJobs, job)
		}
		resultEventType := "agent.tool_result"
		identityField := "tool_use_id"
		if tool.eventType == "agent.mcp_tool_use" {
			resultEventType = "agent.mcp_tool_result"
			identityField = "mcp_tool_use_id"
		}
		payloadJSON, err := marshalBridgeJSON(map[string]any{
			"type": resultEventType, identityField: tool.eventID,
			"content":  []map[string]string{{"type": "text", "text": safeMessage}},
			"is_error": true, "reason": "runtime_interrupted",
		})
		if err != nil {
			return nil, err
		}
		visibility, sessionVisible := threadScope.publicProjection(resultEventType)
		resultEventID := id.New("evt_")
		resultSequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id, projection_json,
				created_at, updated_at, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, '{}', $12, $12, $12)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), resultEventID,
			resultSequence, resultEventType, payloadJSON, visibility, sessionVisible,
			stableRuntimeID("interrupt_tool_result", interruptEventID, tool.eventID), tool.modelRequestID, now,
		); err != nil {
			return nil, err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, resultEventID, visibility, sessionVisible, now); err != nil {
			return nil, err
		}
		if _, err := settleRuntimeToolPartTx(ctx, tx, scope, tool.modelRequestID, settlement, now); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_pending_tool_uses
			    SET status='cancelled', result_event_id=$5, resolved_at=$6, updated_at=$6
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			    AND tool_use_event_id=$4 AND status IN ('pending','resolving')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), tool.eventID, resultEventID, now,
		); err != nil {
			return nil, err
		}
		if execution != nil {
			if err := consumeSandboxExecutionForTerminalWriterTx(
				ctx, tx, scope, tool.eventID, resultEventID, "conversation_tool_result", now,
			); err != nil {
				return nil, err
			}
		}
		projection := &bridgev1.InterruptToolProjection{
			ToolUseEventId: tool.eventID,
			ResultEvent: &bridgev1.DurableEventStamp{
				SessionThreadId: scope.GetSessionThreadId(), EventId: resultEventID,
				EventSequence: resultSequence,
				Disposition:   bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
			},
		}
		switch outcome := settlement.GetOutcome().(type) {
		case *bridgev1.RuntimeToolSettlement_Error:
			projection.TerminalState = &bridgev1.InterruptToolProjection_Error{Error: outcome.Error}
		case *bridgev1.RuntimeToolSettlement_Cancelled:
			projection.TerminalState = &bridgev1.InterruptToolProjection_Cancelled{Cancelled: outcome.Cancelled}
		default:
			return nil, status.Error(codes.Internal, "interrupt Tool outcome is not terminal")
		}
		projections = append(projections, projection)
	}
	releaseJobs, err := sandboxrelease.ReadyRequestsTx(
		ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), now, nil,
	)
	if err != nil {
		return nil, err
	}
	jobs := append(cancellationJobs, releaseJobs...)
	if _, err := queue.EnqueueBatchTx(ctx, tx, jobs); err != nil {
		return nil, err
	}
	return projections, nil
}

func lockUnfinishedRuntimeToolsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) ([]unfinishedRuntimeTool, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.event_id, e.type, e.sequence, e.model_request_id
		   FROM session_events e
		  WHERE e.workspace_id=$1 AND e.session_id=$2 AND e.session_thread_id=$3
		    AND e.type IN ('agent.tool_use','agent.mcp_tool_use')
		    AND NOT EXISTS (
		      SELECT 1 FROM session_events result
		       WHERE result.workspace_id=e.workspace_id AND result.session_id=e.session_id
		         AND result.session_thread_id=e.session_thread_id
		         AND ((result.type='agent.tool_result' AND COALESCE(result.payload_json::jsonb->>'tool_use_event_id', result.payload_json::jsonb->>'tool_use_id')=e.event_id)
		           OR (result.type='agent.mcp_tool_result' AND result.payload_json::jsonb->>'mcp_tool_use_id'=e.event_id))
		    )
		  ORDER BY e.sequence, e.event_id
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var tools []unfinishedRuntimeTool
	for rows.Next() {
		var tool unfinishedRuntimeTool
		if err := rows.Scan(&tool.eventID, &tool.eventType, &tool.eventSequence, &tool.modelRequestID); err != nil {
			return nil, err
		}
		tools = append(tools, tool)
	}
	return tools, rows.Err()
}

func interruptedToolOutcomeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
) (*bridgev1.RuntimeToolSettlement, string, *interruptedSandboxExecution, error) {
	var pending bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM session_pending_tool_uses
		    WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		      AND tool_use_event_id=$4 AND status IN ('pending','resolving'))`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&pending); err != nil {
		return nil, "", nil, err
	}
	var execution interruptedSandboxExecution
	err := tx.QueryRow(ctx,
		`SELECT tool_use_event_id, tool_kind, execution_state, background_operation_kind,
		        background_operation_state, execution_attempt_generation,
		        waiting_activation_operation_id, waiting_materialization_operation_id,
		        provider_command_reference_json
		   FROM session_runtime_tool_results
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND tool_use_event_id=$4 AND tool_kind IN ('sandbox_tool','sandbox_background')
		    AND (tool_kind='sandbox_tool' AND execution_state <> 'consumed'
		      OR tool_kind='sandbox_background' AND consumed_by_terminal_event_id IS NULL)
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&execution.toolUseEventID, &execution.toolKind, &execution.executionState,
		&execution.backgroundOperationKind, &execution.backgroundOperationState,
		&execution.executionAttemptGeneration,
		&execution.waitingActivationOperationID, &execution.waitingMaterializationOperationID,
		&execution.providerCommandReference)
	var executionRef *interruptedSandboxExecution
	if err == nil {
		executionRef = &execution
	} else if !dbconnect.IsNoRows(err) {
		return nil, "", nil, err
	}
	cancelledBeforeStart := executionRef == nil && pending
	resultNotCommitted := false
	if executionRef != nil {
		switch execution.toolKind {
		case "sandbox_tool":
			cancelledBeforeStart = execution.executionState.String != "running" && execution.executionState.String != "terminal_unconsumed"
			resultNotCommitted = execution.executionState.String == "terminal_unconsumed"
		case "sandbox_background":
			cancelledBeforeStart = execution.backgroundOperationState.String == "pending" && execution.backgroundOperationKind.String == "stdin"
			resultNotCommitted = execution.backgroundOperationState.String == "terminal"
		}
	}
	if cancelledBeforeStart {
		errorJSON := `{"type":"runtime","code":"runtime_interrupted","message":"Tool execution was cancelled before it started.","retryable":false,"fatal":false}`
		return &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUseEventID,
			Outcome:        &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{ErrorJson: &errorJSON}},
		}, "Tool execution was cancelled before it started.", executionRef, nil
	}
	message := "Tool execution outcome is unknown because the Thread was interrupted."
	code := "runtime_interrupted_outcome_unknown"
	if resultNotCommitted {
		message = "Tool execution may have completed, but its result did not commit before interruption."
		code = "runtime_interrupted_result_not_committed"
	}
	errorJSON, err := marshalBridgeJSON(map[string]any{
		"type": "runtime", "code": code, "message": message, "retryable": false, "fatal": false,
	})
	if err != nil {
		return nil, "", nil, err
	}
	return &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUseEventID,
		Outcome:        &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{ErrorJson: errorJSON}},
	}, message, executionRef, nil
}

func requestSandboxExecutionCancellationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, execution interruptedSandboxExecution, now time.Time) (queue.EnqueueRequest, error) {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET cancel_requested_at = COALESCE(cancel_requested_at, $6),
		        cancel_state = COALESCE(cancel_state, 'pending'), updated_at = $6
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND execution_attempt_generation = $5
		    AND execution_state = 'running'
		    AND provider_command_reference_json IS NOT NULL`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID,
		execution.executionAttemptGeneration.Int64, now.UTC(),
	)
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	if !rowsAffected(result) {
		return queue.EnqueueRequest{}, status.Error(codes.Aborted, "sandbox execution changed during interrupt settlement")
	}
	payload, err := marshalBridgeJSON(map[string]string{
		"workspace_id": scope.GetWorkspaceId(), "session_id": scope.GetSessionId(),
		"session_thread_id": scope.GetSessionThreadId(), "tool_use_event_id": execution.toolUseEventID,
	})
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	workspaceID := workspace.ID(scope.GetWorkspaceId())
	return queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxToolCancel,
		PartitionKey:   queue.FormatSandboxCancelPartitionKey(workspaceID, scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID),
		DedupeKey:      queue.FormatSandboxToolCancelDedupeKey(workspaceID, scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID),
		PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: sandboxToolCancelMaxAttempts, Now: now,
	}, nil
}
