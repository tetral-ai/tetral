package agentruntimebridge

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
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
		return failure.Code == "runtime_invalid_sequence" && failure.Reason == "runtime_contract_validation"
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
		`SELECT e.session_thread_id, e.event_id, COALESCE(e.model_request_id, ''), e.payload_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND ($3 = '' OR e.session_thread_id = $3)
		    AND e.type = 'agent.tool_use'
		    AND e.visibility = 'public'
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events result
			         WHERE result.workspace_id = e.workspace_id
			           AND result.session_id = e.session_id
			           AND result.session_thread_id = e.session_thread_id
		           AND result.type = 'agent.tool_result'
		           AND (result.payload_json::jsonb ->> 'tool_use_event_id' = e.event_id
		             OR result.payload_json::jsonb ->> 'tool_use_id' = e.event_id)
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
		if err := rows.Scan(&toolUse.SessionThreadID, &toolUse.EventID, &toolUse.ModelRequestID, &toolUse.PayloadJSON); err != nil {
			return nil, err
		}
		toolUse.EventType = "agent.tool_use"
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
	drafts []*bridgev1.RuntimeMessageDraft,
	cancellations []*bridgev1.PendingToolCancellationDraft,
	sandboxExecutionToolUseEventIDs []string,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	cancellationByLocalID := make(map[string]*bridgev1.PendingToolCancellationDraft, len(cancellations))
	cancellationByToolUseID := make(map[string]struct{}, len(cancellations))
	for _, cancellation := range cancellations {
		if cancellation == nil || cancellation.GetRuntimeLocalId() == "" || cancellation.GetToolUseEventId() == "" {
			return nil, status.Error(codes.InvalidArgument, "runtime termination pending tool cancellation is invalid")
		}
		if _, duplicate := cancellationByLocalID[cancellation.GetRuntimeLocalId()]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "runtime termination pending tool cancellation is duplicated")
		}
		if _, duplicate := cancellationByToolUseID[cancellation.GetToolUseEventId()]; duplicate {
			return nil, status.Error(codes.InvalidArgument, "runtime termination pending tool use is duplicated")
		}
		cancellationByLocalID[cancellation.GetRuntimeLocalId()] = cancellation
		cancellationByToolUseID[cancellation.GetToolUseEventId()] = struct{}{}
	}
	pendingToolUseIDs, err := runtimeTerminationPendingToolUseIDsTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if len(pendingToolUseIDs) != len(cancellationByToolUseID) {
		return nil, status.Error(codes.FailedPrecondition, "runtime termination pending tool declaration is incomplete")
	}
	for _, toolUseID := range pendingToolUseIDs {
		if _, ok := cancellationByToolUseID[toolUseID]; !ok {
			return nil, status.Error(codes.FailedPrecondition, "runtime termination pending tool declaration is incomplete")
		}
	}
	sandboxExecutionIDs, err := runtimeTerminationSandboxExecutionIDsTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	providedSandboxExecutionIDs := append([]string(nil), sandboxExecutionToolUseEventIDs...)
	sort.Strings(providedSandboxExecutionIDs)
	if !sameBridgeStringSlice(sandboxExecutionIDs, providedSandboxExecutionIDs) {
		return nil, status.Error(codes.FailedPrecondition, "runtime termination sandbox execution declaration is incomplete")
	}
	for _, toolUseID := range sandboxExecutionIDs {
		if _, overlap := cancellationByToolUseID[toolUseID]; overlap {
			return nil, status.Error(codes.InvalidArgument, "runtime termination tool ownership sets overlap")
		}
	}

	receipt := &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpCommitRuntimeTermination,
		SourceKind:      "runtime_termination",
		SourceId:        runtimeWriteID,
	}
	seenCompletionMail := false
	seenCancellations := make(map[string]struct{}, len(cancellations))
	for index, draft := range drafts {
		if draft == nil || draft.GetOrdinal() != int32(index) ||
			draft.GetSourceKind() != "runtime_termination" ||
			draft.GetSourceId() != runtimeWriteID ||
			draft.GetSourceEventId() != "" {
			return nil, status.Error(codes.InvalidArgument, "runtime termination draft identity is invalid")
		}
		expectedLocalID := stableRuntimeID(
			"runtime_message_draft",
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			"runtime_termination",
			runtimeWriteID,
			runtimeDraftKindToken(draft.GetDraftKind()),
			strconv.FormatInt(int64(index), 10),
		)
		if draft.GetRuntimeLocalId() != expectedLocalID {
			return nil, status.Error(codes.InvalidArgument, "runtime termination draft id is invalid")
		}
		switch draft.GetDraftKind() {
		case bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_TERMINATION:
			cancellation, ok := cancellationByLocalID[draft.GetRuntimeLocalId()]
			if !ok {
				return nil, status.Error(codes.InvalidArgument, "runtime termination tool draft has no pending cancellation")
			}
			eventStamp, messageStamp, delta, err := appendDeclaredRuntimeTerminationToolResultTx(
				ctx,
				tx,
				scope,
				threadScope,
				runtimeWriteID,
				cancellation.GetToolUseEventId(),
				draft,
				now,
			)
			if err != nil {
				return nil, err
			}
			receipt.Events = append(receipt.Events, eventStamp)
			receipt.Messages = append(receipt.Messages, messageStamp)
			receipt.PendingToolDeltaJson = append(receipt.PendingToolDeltaJson, delta)
			seenCancellations[draft.GetRuntimeLocalId()] = struct{}{}
		case bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_COMPLETION_MAIL:
			if seenCompletionMail {
				return nil, status.Error(codes.InvalidArgument, "runtime termination completion mail is duplicated")
			}
			eventStamp, messageStamp, err := appendDeclaredCompletionMailForSourceTx(
				ctx,
				tx,
				scope,
				threadScope,
				"runtime_termination",
				runtimeWriteID,
				draft,
				now,
			)
			if err != nil {
				return nil, err
			}
			receipt.Events = append(receipt.Events, eventStamp)
			receipt.Messages = append(receipt.Messages, messageStamp)
			seenCompletionMail = true
		default:
			return nil, status.Error(codes.InvalidArgument, "runtime termination draft class is invalid")
		}
	}
	if len(seenCancellations) != len(cancellationByLocalID) {
		return nil, status.Error(codes.InvalidArgument, "runtime termination pending cancellation draft is missing")
	}
	if threadScope.role == "subagent" && threadScope.status != "closed_for_runtime" && !seenCompletionMail {
		return nil, status.Error(codes.InvalidArgument, "runtime termination sub-agent completion mail is required")
	}
	if threadScope.role != "subagent" && seenCompletionMail {
		return nil, status.Error(codes.InvalidArgument, "runtime termination completion mail requires a sub-agent")
	}
	if err := settleRuntimeTerminationSandboxExecutionsTx(
		ctx,
		tx,
		scope,
		runtimeWriteID,
		sandboxExecutionIDs,
		now,
	); err != nil {
		return nil, err
	}
	return receipt, nil
}

func runtimeTerminationPendingToolUseIDsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT tool_use_event_id
		   FROM session_pending_tool_uses p
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.session_thread_id = $3
		    AND p.status IN ('pending', 'resolving')
		    AND NOT EXISTS (
		      SELECT 1 FROM session_runtime_tool_results r
		       WHERE r.workspace_id = p.workspace_id
		         AND r.session_id = p.session_id
		         AND r.session_thread_id = p.session_thread_id
		         AND r.tool_use_event_id = p.tool_use_event_id
		         AND r.tool_kind = 'sandbox_tool'
		         AND r.execution_state <> 'consumed'
		    )
		  ORDER BY p.tool_use_event_id
		  FOR UPDATE OF p`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var toolUseID string
		if err := rows.Scan(&toolUseID); err != nil {
			return nil, err
		}
		ids = append(ids, toolUseID)
	}
	return ids, rows.Err()
}

func runtimeTerminationSandboxExecutionIDsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT tool_use_event_id
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_kind = 'sandbox_tool' AND execution_state <> 'consumed'
		  ORDER BY tool_use_event_id
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var toolUseEventID string
		if err := rows.Scan(&toolUseEventID); err != nil {
			return nil, err
		}
		ids = append(ids, toolUseEventID)
	}
	return ids, rows.Err()
}

func settleRuntimeTerminationSandboxExecutionsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeWriteID string,
	toolUseEventIDs []string,
	now time.Time,
) error {
	if len(toolUseEventIDs) == 0 {
		return nil
	}
	orphanToolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, scope, false)
	if err != nil {
		return err
	}
	orphansByID := make(map[string]runtimeOrphanToolUse, len(orphanToolUses))
	for _, toolUse := range orphanToolUses {
		orphansByID[toolUse.EventID] = toolUse
	}
	for _, toolUseEventID := range toolUseEventIDs {
		toolUse, ok := orphansByID[toolUseEventID]
		if !ok {
			return status.Error(codes.FailedPrecondition, "runtime termination sandbox execution has no live Tool Use")
		}
		inserted, err := insertRuntimeTerminalToolResultForScopeTx(ctx, tx, scope, toolUse, runtimeTerminalToolResult{
			WriteIDPrefix:     "rwrite_runtime_termination_tool_" + runtimeWriteID + "_",
			Reason:            "runtime_terminated",
			ErrorType:         "runtime_terminated",
			Message:           "Tool result unavailable because the runtime terminated.",
			Retryable:         false,
			ConsumptionReason: "runtime_terminated",
		}, now)
		if err != nil {
			return err
		}
		if !inserted {
			return status.Error(codes.FailedPrecondition, "runtime termination sandbox execution result already exists")
		}
	}
	return nil
}

func appendDeclaredRuntimeTerminationToolResultTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	runtimeWriteID string,
	toolUseEventID string,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableEventStamp, *bridgev1.DurableMessageStamp, string, error) {
	if len(draft.GetParts()) != 1 || draft.GetParts()[0].GetPartKind() != "tool" {
		return nil, nil, "", status.Error(codes.InvalidArgument, "runtime termination tool draft is invalid")
	}
	var partInfo struct {
		Type           string `json:"type"`
		ToolUseEventID string `json:"toolUseEventId"`
		State          struct {
			Status string `json:"status"`
			Error  struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"state"`
	}
	if err := json.Unmarshal([]byte(draft.GetParts()[0].GetPartJson()), &partInfo); err != nil ||
		partInfo.Type != "tool" ||
		partInfo.ToolUseEventID != toolUseEventID ||
		(partInfo.State.Status != "cancelled" && partInfo.State.Status != "error") ||
		strings.TrimSpace(partInfo.State.Error.Message) == "" {
		return nil, nil, "", status.Error(codes.InvalidArgument, "runtime termination tool part is invalid")
	}
	var sourceEventType string
	if err := tx.QueryRow(ctx,
		`SELECT type
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
	).Scan(&sourceEventType); err != nil {
		return nil, nil, "", err
	}
	eventType := "agent.tool_result"
	toolUseField := "tool_use_id"
	if sourceEventType == "agent.mcp_tool_use" {
		eventType = "agent.mcp_tool_result"
		toolUseField = "mcp_tool_use_id"
	} else if sourceEventType != "agent.tool_use" {
		return nil, nil, "", status.Error(codes.FailedPrecondition, "runtime termination pending tool source is invalid")
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":       eventType,
		toolUseField: toolUseEventID,
		"is_error":   true,
		"content": []map[string]any{{
			"type": "text",
			"text": partInfo.State.Error.Message,
		}},
	})
	if err != nil {
		return nil, nil, "", err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return nil, nil, "", err
	}
	visibility, sessionVisible := threadScope.publicProjection(eventType)
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $7, $11, $11, $11)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventType,
		payloadJSON,
		visibility,
		sessionVisible,
		runtimeWriteID,
		now,
	); err != nil {
		return nil, nil, "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return nil, nil, "", err
	}
	messageStamp, err := insertGeneratedRuntimeMessageDraftTx(
		ctx,
		tx,
		scope,
		eventID,
		"assistant",
		"agent",
		draft,
		now,
	)
	if err != nil {
		return nil, nil, "", err
	}
	if eventType == "agent.tool_result" {
		if err := consumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, toolUseEventID, eventID, "runtime_terminated", now); err != nil {
			return nil, nil, "", err
		}
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'cancelled',
		        result_event_id = $5,
		        resolved_at = $6,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND status IN ('pending', 'resolving')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		eventID,
		now,
	)
	if err != nil {
		return nil, nil, "", err
	}
	if !rowsAffected(result) {
		return nil, nil, "", status.Error(codes.FailedPrecondition, "runtime termination pending tool use is stale")
	}
	delta, err := marshalBridgeJSON(map[string]any{
		"result_event_id":   eventID,
		"runtime_local_id":  draft.GetRuntimeLocalId(),
		"status":            "cancelled",
		"tool_use_event_id": toolUseEventID,
	})
	if err != nil {
		return nil, nil, "", err
	}
	return &bridgev1.DurableEventStamp{
		SessionThreadId: scope.GetSessionThreadId(),
		SourceEventId:   toolUseEventID,
		EventId:         eventID,
		EventSequence:   sequence,
		Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
	}, messageStamp, delta, nil
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
		SourceEventId:   sourceEventID,
		EventId:         eventID,
		EventSequence:   sequence,
		Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
	}, nil
}

func runtimeTerminationBinding(scope *bridgev1.RuntimeScope) runtimeBindingForDelivery {
	return runtimeBindingForDelivery{
		BindingID:         scope.GetBinding().GetBindingId(),
		BindingGeneration: scope.GetBinding().GetBindingGeneration(),
		PodUID:            scope.GetBinding().GetTargetPodUid(),
	}
}
