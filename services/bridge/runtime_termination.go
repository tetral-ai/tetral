package agentruntimebridge

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const runtimeTerminationFailureMaxBytes = 64 * 1024

type runtimeTerminationFailure struct {
	Type        string `json:"type"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	Reason      string `json:"reason,omitempty"`
	Retryable   bool   `json:"retryable"`
	RetryStatus struct {
		Type string `json:"type"`
	} `json:"retryStatus"`
}

func parseRuntimeTerminationFailure(raw string) (runtimeTerminationFailure, string, error) {
	if raw == "" || len(raw) > runtimeTerminationFailureMaxBytes {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure must be a JSON object")
	}
	var failure runtimeTerminationFailure
	if err := json.Unmarshal([]byte(raw), &failure); err != nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	if failure.Retryable || failure.RetryStatus.Type != "terminal" || !isWhitelistedRuntimeTerminationFailure(failure) {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime failure is not terminal")
	}
	canonical, err := marshalBridgeJSON(object)
	if err != nil {
		return runtimeTerminationFailure{}, "", status.Error(codes.InvalidArgument, "runtime termination failure is invalid")
	}
	return failure, canonical, nil
}

func isWhitelistedRuntimeTerminationFailure(failure runtimeTerminationFailure) bool {
	if failure.Type == "runtime" {
		return (failure.Code == "runtime_invalid_sequence" && failure.Reason == "runtime_contract_validation") ||
			(failure.Code == "runtime_persistence_exhausted" && failure.Reason == "runtime_input_commit_exhausted")
	}
	if failure.Type != "provider" {
		return false
	}
	switch failure.Code {
	case "credential_required", "platform_keys_exhausted", "provider_key_unavailable",
		"provider_rate_limited", "provider_timeout", "provider_stream_error", "provider_unavailable",
		"provider_cancelled", "attachment_unavailable":
		return false
	default:
		return strings.HasPrefix(failure.Code, "provider_") || failure.Code == "context_overflow"
	}
}

func runtimeTerminationErrorKind(failure runtimeTerminationFailure) string {
	if failure.Type == "provider" {
		return "provider_error"
	}
	return "runtime_semantic_error"
}

func closeRuntimeTerminationSpansTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, failure runtimeTerminationFailure, now time.Time) error {
	starts, err := runtimeTerminationOpenRequestStartsTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	for _, start := range starts {
		if _, err := insertRuntimeTerminalRequestEndTx(
			ctx, tx, scopeForThread(scope, start.SessionThreadID), start,
			runtimeTerminationErrorKind(failure), "rwrite_runtime_termination_", now,
		); err != nil {
			return err
		}
	}
	return nil
}

func runtimeTerminationOpenRequestStartsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]runtimeOpenRequestStart, error) {
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.model_request_id, e.projection_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id = $3
		    AND e.type = 'span.model_request_start'
		    AND COALESCE(e.model_request_id, '') <> ''
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events ended
		         WHERE ended.workspace_id = e.workspace_id
		           AND ended.session_id = e.session_id
		           AND ended.session_thread_id = e.session_thread_id
		           AND ended.model_request_id = e.model_request_id
		           AND ended.type = 'span.model_request_end'
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	starts := make([]runtimeOpenRequestStart, 0)
	for rows.Next() {
		var start runtimeOpenRequestStart
		var projectionJSON string
		if err := rows.Scan(&start.SessionThreadID, &start.EventID, &start.ModelRequestID, &projectionJSON); err != nil {
			return nil, err
		}
		requestKind, err := requestKindFromModelRequestStartProjection(projectionJSON)
		if err != nil {
			return nil, err
		}
		start.RequestKind = requestKind
		starts = append(starts, start)
	}
	return starts, rows.Err()
}

func runtimeTerminationOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool) ([]runtimeOrphanToolUse, error) {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.type, COALESCE(e.model_request_id, ''), e.payload_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND ($3 = '' OR e.session_thread_id = $3)
		    AND e.type IN ('agent.tool_use','agent.mcp_tool_use')
		    AND e.visibility = 'public'
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events result
			         WHERE result.workspace_id = e.workspace_id
			           AND result.session_id = e.session_id
			           AND result.session_thread_id = e.session_thread_id
		           AND ((result.type = 'agent.tool_result'
		             AND (result.payload_json::jsonb ->> 'tool_use_event_id' = e.event_id
		               OR result.payload_json::jsonb ->> 'tool_use_id' = e.event_id))
		             OR (result.type = 'agent.mcp_tool_result'
		               AND result.payload_json::jsonb ->> 'mcp_tool_use_id' = e.event_id))
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), threadID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	toolUses := make([]runtimeOrphanToolUse, 0)
	for rows.Next() {
		var toolUse runtimeOrphanToolUse
		if err := rows.Scan(&toolUse.SessionThreadID, &toolUse.EventID, &toolUse.EventType, &toolUse.ModelRequestID, &toolUse.PayloadJSON); err != nil {
			return nil, err
		}
		toolUses = append(toolUses, toolUse)
	}
	return toolUses, rows.Err()
}

func commitRuntimeTerminationDeclarationsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	runtimeWriteID string,
	settlements []*bridgev1.RuntimeToolSettlement,
	completionMail *bridgev1.RuntimeMessageCreate,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	toolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, scope, false)
	if err != nil {
		return nil, err
	}
	if len(toolUses) != len(settlements) {
		return nil, status.Error(codes.FailedPrecondition, "runtime termination Tool settlement census is incomplete")
	}
	receipt := &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(), OperationKind: bridgeOpCommitRuntimeTermination,
		SourceKind: "runtime_termination", OperationId: runtimeWriteID,
	}
	seen := make(map[string]struct{}, len(settlements))
	for index, toolUse := range toolUses {
		settlement := settlements[index]
		if settlement == nil || settlement.GetToolUseEventId() != toolUse.EventID {
			return nil, status.Error(codes.InvalidArgument, "runtime termination Tool settlement order is invalid")
		}
		if _, duplicate := seen[toolUse.EventID]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "runtime termination Tool settlement is duplicated")
		}
		seen[toolUse.EventID] = struct{}{}
		if err := validateRuntimeTerminationSettlement(settlement); err != nil {
			return nil, err
		}
		resultEventType := "agent.tool_result"
		identityField := "tool_use_event_id"
		if toolUse.EventType == "agent.mcp_tool_use" {
			resultEventType = "agent.mcp_tool_result"
			identityField = "mcp_tool_use_id"
		}
		payloadJSON, err := marshalBridgeJSON(map[string]any{
			"type": resultEventType, identityField: toolUse.EventID,
			"content": []map[string]string{{
				"type": "text", "text": "Tool result unavailable because the runtime terminated.",
			}},
			"is_error": true, "reason": "runtime_terminated",
		})
		if err != nil {
			return nil, err
		}
		visibility, sessionVisible := threadScope.publicProjection(resultEventType)
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id, projection_json,
				created_at, updated_at, processed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'{}',$12,$12,$12)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
			resultEventType, payloadJSON, visibility, sessionVisible,
			stableRuntimeID("runtime_termination_tool_result", runtimeWriteID, toolUse.EventID),
			toolUse.ModelRequestID, now,
		); err != nil {
			return nil, err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
			return nil, err
		}
		if _, err := settleRuntimeToolPartTx(ctx, tx, scope, toolUse.ModelRequestID, settlement, now); err != nil {
			return nil, err
		}
		if err := consumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, toolUse.EventID, eventID, "runtime_terminated", now); err != nil {
			return nil, err
		}
		if err := cancelPendingToolUseForTerminalResultTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return nil, err
		}
		receipt.Events = append(receipt.Events, &bridgev1.DurableEventStamp{
			SessionThreadId: scope.GetSessionThreadId(), EventId: eventID, EventSequence: sequence,
			Disposition: bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
		})
	}
	if completionMail != nil {
		eventStamp, messageStamp, err := appendDeclaredCompletionMailForSourceTx(
			ctx, tx, scope, threadScope, "runtime_termination", runtimeWriteID, completionMail, now,
		)
		if err != nil {
			return nil, err
		}
		receipt.Events = append(receipt.Events, eventStamp)
		receipt.Messages = append(receipt.Messages, messageStamp)
	}
	return receipt, nil
}

func validateRuntimeTerminationSettlement(settlement *bridgev1.RuntimeToolSettlement) error {
	var raw string
	switch outcome := settlement.GetOutcome().(type) {
	case *bridgev1.RuntimeToolSettlement_Error:
		raw = outcome.Error.GetErrorJson()
	case *bridgev1.RuntimeToolSettlement_Cancelled:
		if outcome.Cancelled.ErrorJson == nil {
			return status.Error(codes.InvalidArgument, "runtime termination cancellation requires its fixed failure")
		}
		raw = outcome.Cancelled.GetErrorJson()
	default:
		return status.Error(codes.InvalidArgument, "runtime termination Tool settlement must be terminal failure")
	}
	var toolError struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(raw), &toolError); err != nil || toolError.Type != "runtime_terminated" {
		return status.Error(codes.InvalidArgument, "runtime termination Tool settlement failure is invalid")
	}
	return nil
}

type runtimeTerminationSibling struct {
	threadID    string
	threadScope threadMutationScope
}

func closeRuntimeTerminatedSessionSiblingsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	now time.Time,
) error {
	rows, err := tx.Query(ctx,
		`SELECT id, visibility, role, status, task_name
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id <> $3
		    AND status NOT IN ('terminated', 'failed')
		  ORDER BY id
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	siblings := make([]runtimeTerminationSibling, 0)
	threadIDs := make([]string, 0)
	for rows.Next() {
		var sibling runtimeTerminationSibling
		if err := rows.Scan(
			&sibling.threadID,
			&sibling.threadScope.visibility,
			&sibling.threadScope.role,
			&sibling.threadScope.status,
			&sibling.threadScope.taskName,
		); err != nil {
			return err
		}
		siblings = append(siblings, sibling)
		threadIDs = append(threadIDs, sibling.threadID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(siblings) == 0 {
		return nil
	}

	starts, err := runtimePodLostOpenRequestStartsTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadIDs,
	)
	if err != nil {
		return err
	}
	for _, start := range starts {
		if _, err := insertRuntimeTerminalRequestEndTx(
			ctx,
			tx,
			scopeForThread(scope, start.SessionThreadID),
			start,
			"runtime_terminated",
			"rwrite_session_termination_"+runtimeWriteID+"_",
			now,
		); err != nil {
			return err
		}
	}

	toolUses, err := runtimeTerminalOrphanToolUsesTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadIDs,
		true,
	)
	if err != nil {
		return err
	}
	for _, toolUse := range toolUses {
		inserted, err := insertRuntimeTerminalToolResultForScopeTx(
			ctx,
			tx,
			scopeForThread(scope, toolUse.SessionThreadID),
			toolUse,
			runtimeTerminalToolResult{
				WriteIDPrefix:     "rwrite_session_termination_tool_" + runtimeWriteID + "_",
				Reason:            "runtime_terminated",
				ErrorType:         "runtime_terminated",
				Message:           "Tool result unavailable because the session terminated.",
				Retryable:         false,
				ConsumptionReason: "runtime_terminated",
			},
			now,
		)
		if err != nil {
			return err
		}
		if !inserted {
			return status.Error(codes.FailedPrecondition, "session termination tool result already exists")
		}
	}

	for _, sibling := range siblings {
		result, err := tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'terminated',
			        closed_at = COALESCE(closed_at, $4),
			        last_active_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND id = $3
			    AND status NOT IN ('terminated', 'failed')`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			sibling.threadID,
			now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "session termination child status is stale")
		}
		childScope := scopeForThread(scope, sibling.threadID)
		payloadJSON, err := threadStatusPayloadJSON("session.thread_status_terminated", childScope, sibling.threadScope, "")
		if err != nil {
			return err
		}
		if _, err := insertRuntimeTerminationEventTx(
			ctx,
			tx,
			childScope,
			sibling.threadScope,
			runtimeWriteID+":terminate:"+sibling.threadID,
			runtimeWriteID,
			"session.thread_status_terminated",
			payloadJSON,
			now,
		); err != nil {
			return err
		}
	}
	return nil
}

// cancelRuntimeTerminationInputsTx is the durable release boundary for Inbox
// custody owned by a terminalized Thread. Main-thread termination covers the
// whole Session tree; child termination remains thread-local. Reactivated task
// jobs are cancelled together with queued/parked Inbox rows, invalidating any
// runner lease before the Runtime receipt can release hot state.
type runtimeTerminationCustodyTransitions struct {
	accepted int
	parked   int
}

func cancelRuntimeTerminationInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sessionWide bool,
	now time.Time,
) (runtimeTerminationCustodyTransitions, error) {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	var transitions runtimeTerminationCustodyTransitions
	err := tx.QueryRow(ctx,
		`WITH target_inputs AS MATERIALIZED (
		    SELECT inbox.runtime_input_id, inbox.status
		      FROM session_runtime_inbox inbox
		     WHERE inbox.workspace_id = $1
		       AND inbox.session_id = $2
		       AND ($3 = '' OR inbox.session_thread_id = $3)
		       AND inbox.input_kind <> 'approval_review'
		       AND (
		           inbox.status IN ('delivering', 'accepted')
		           OR (inbox.input_kind = 'task_notification' AND inbox.status IN ('queued', 'parked'))
		       )
		     FOR UPDATE
		), cancelled_jobs AS (
		    UPDATE queue_jobs job
		       SET status = 'cancelled', cancelled_at = $4,
		           lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
		           updated_at = $4
		     WHERE job.workspace_id = $1
		       AND job.status IN ('pending', 'leased')
		       AND EXISTS (
		           SELECT 1 FROM target_inputs input
		            WHERE job.dedupe_key = 'runtime_input:' || $1 || ':' || $2 || ':' || input.runtime_input_id
		       )
		), updated_inputs AS (
		  UPDATE session_runtime_inbox inbox
		     SET status = 'cancelled', updated_at = $4
		    FROM target_inputs input
		   WHERE inbox.workspace_id = $1
		     AND inbox.runtime_input_id = input.runtime_input_id
		  RETURNING input.status
		)
		SELECT count(*) FILTER (WHERE status IN ('delivering', 'accepted')),
		       count(*) FILTER (WHERE status = 'parked')
		  FROM updated_inputs`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadID,
		now,
	).Scan(&transitions.accepted, &transitions.parked)
	return transitions, err
}

func appendRuntimeTerminationErrorTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, runtimeWriteID string, failureJSON string, now time.Time) (*bridgev1.DurableEventStamp, error) {
	var failure runtimeTerminationFailure
	if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil {
		return nil, err
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":  "session.error",
		"error": publicRuntimeTerminationError(failure),
	})
	if err != nil {
		return nil, err
	}
	return insertRuntimeTerminationEventTx(
		ctx,
		tx,
		scope,
		threadScope,
		runtimeWriteID+":error",
		runtimeWriteID,
		"session.error",
		payloadJSON,
		now,
	)
}

func publicRuntimeTerminationError(failure runtimeTerminationFailure) map[string]any {
	errorType := "unknown_error"
	if failure.Type == "provider" {
		errorType = "model_request_failed_error"
	}
	message := strings.TrimSpace(failure.Message)
	if message == "" {
		message = "The runtime terminated because the request could not be completed."
	}
	return map[string]any{
		"type":         errorType,
		"message":      message,
		"retry_status": map[string]any{"type": "terminal"},
	}
}

func appendRuntimeTerminatedStatusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, runtimeWriteID string, now time.Time) (*bridgev1.DurableEventStamp, error) {
	eventType := "session.thread_status_terminated"
	payloadJSON, err := threadStatusPayloadJSON(eventType, scope, threadScope, "")
	if err != nil {
		return nil, err
	}
	if threadScope.role == "main" {
		eventType = "session.status_terminated"
		payloadJSON, err = marshalBridgeJSON(map[string]any{"type": eventType})
		if err != nil {
			return nil, err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sessions SET status = 'terminated', updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
			scope.GetWorkspaceId(), scope.GetSessionId(), now)
		if err != nil {
			return nil, err
		}
		if !rowsAffected(result) {
			return nil, status.Error(codes.FailedPrecondition, "runtime session is stale")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET active_seconds_total = active_seconds_total + CASE
			          WHEN running_since IS NULL THEN 0
			          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($3 - running_since)))
			        END,
			        running_since = NULL,
			        updated_at = $3
			  WHERE workspace_id = $1 AND session_id = $2`,
			scope.GetWorkspaceId(), scope.GetSessionId(), now); err != nil {
			return nil, err
		}
		result, err = tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'failed',
			        closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
			    AND status NOT IN ('terminated', 'failed')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
		if err != nil {
			return nil, err
		}
		if !rowsAffected(result) {
			return nil, status.Error(codes.FailedPrecondition, "runtime main thread status update failed")
		}
	} else {
		result, err := tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'failed', closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3 AND role <> 'main'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
		if err != nil {
			return nil, err
		}
		if !rowsAffected(result) {
			return nil, status.Error(codes.FailedPrecondition, "child thread status update failed")
		}
	}
	return insertRuntimeTerminationEventTx(ctx, tx, scope, threadScope, runtimeWriteID, runtimeWriteID, eventType, payloadJSON, now)
}

func insertRuntimeTerminationEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	runtimeWriteID string,
	sourceEventID string,
	eventType string,
	payloadJSON string,
	now time.Time,
) (*bridgev1.DurableEventStamp, error) {
	visibility, sessionVisible := threadScope.publicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $7, $11, $11, $11)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
		eventType, payloadJSON, visibility, sessionVisible, runtimeWriteID, now); err != nil {
		return nil, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return nil, err
	}
	return &bridgev1.DurableEventStamp{
		SessionThreadId: scope.GetSessionThreadId(),
		EventId:         eventID,
		EventSequence:   sequence,
		Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
	}, nil
}
