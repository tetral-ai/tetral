package runtimecontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type RuntimeTerminationFailure struct {
	Type        string `json:"type"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	Reason      string `json:"reason,omitempty"`
	Retryable   bool   `json:"retryable"`
	RetryStatus struct {
		Type string `json:"type"`
	} `json:"retryStatus"`
}

func runtimeTerminationErrorKind(failure RuntimeTerminationFailure) string {
	if failure.Type == "provider" {
		return "provider_error"
	}
	return "runtime_semantic_error"
}

func closeRuntimeTerminationSpansTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, failure RuntimeTerminationFailure, now time.Time) error {
	starts, err := RuntimeTerminationOpenRequestStartsTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	for _, start := range starts {
		if _, err := InsertRuntimeTerminalRequestEndTx(
			ctx, tx, ScopeForThread(scope, start.SessionThreadID), start,
			runtimeTerminationErrorKind(failure), "rwrite_runtime_termination_", now,
		); err != nil {
			return err
		}
	}
	return nil
}

func RuntimeTerminationOpenRequestStartsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]OpenRequestStart, error) {
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
	starts := make([]OpenRequestStart, 0)
	for rows.Next() {
		var start OpenRequestStart
		var projectionJSON string
		if err := rows.Scan(&start.SessionThreadID, &start.EventID, &start.ModelRequestID, &projectionJSON); err != nil {
			return nil, err
		}
		requestKind, err := RequestKindFromModelRequestStartProjection(projectionJSON)
		if err != nil {
			return nil, err
		}
		start.RequestKind = requestKind
		starts = append(starts, start)
	}
	return starts, rows.Err()
}

func runtimeTerminationOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sessionWide bool) ([]OrphanToolUse, error) {
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
		    AND NOT EXISTS (
		        SELECT 1 FROM session_events result
			         WHERE result.workspace_id = e.workspace_id
			           AND result.session_id = e.session_id
			           AND result.session_thread_id = e.session_thread_id
		           AND result.type IN ('agent.tool_result','agent.mcp_tool_result')
		           AND result.tool_use_event_id = e.event_id
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		scope.GetWorkspaceId(), scope.GetSessionId(), threadID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	toolUses := make([]OrphanToolUse, 0)
	for rows.Next() {
		var toolUse OrphanToolUse
		if err := rows.Scan(&toolUse.SessionThreadID, &toolUse.EventID, &toolUse.EventType, &toolUse.ModelRequestID, &toolUse.PayloadJSON); err != nil {
			return nil, err
		}
		toolUses = append(toolUses, toolUse)
	}
	return toolUses, rows.Err()
}

func settleRuntimeTerminationDurableFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope ThreadMutationScope,
	runtimeWriteID string,
	failure RuntimeTerminationFailure,
	now time.Time,
) error {
	toolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, scope, false)
	if err != nil {
		return err
	}
	toolErrorJSON, err := MarshalJSON(map[string]any{
		"type": failure.Code, "message": failure.Message, "retryable": false,
	})
	if err != nil {
		return err
	}
	for _, toolUse := range toolUses {
		settlement := &bridgev1.RuntimeToolSettlement{
			ToolUseEventId: toolUse.EventID,
			Outcome: &bridgev1.RuntimeToolSettlement_Cancelled{
				Cancelled: &bridgev1.RuntimeToolCancelled{ErrorJson: &toolErrorJSON},
			},
		}
		resultEventType := "agent.tool_result"
		identityField := "tool_use_id"
		if toolUse.EventType == "agent.mcp_tool_use" {
			resultEventType = "agent.mcp_tool_result"
			identityField = "mcp_tool_use_id"
		}
		payloadJSON, err := MarshalJSON(map[string]any{
			"type": resultEventType, identityField: toolUse.EventID,
			"content": []map[string]string{{
				"type": "text", "text": "Tool result unavailable because the runtime terminated.",
			}},
			"is_error": true, "reason": "runtime_terminated",
		})
		if err != nil {
			return err
		}
		visibility, sessionVisible := threadScope.PublicProjection(resultEventType)
		eventID := id.New("evt_")
		sequence, err := NextSessionEventSequenceTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		projection, err := SettleRuntimeToolPartTx(ctx, tx, scope, toolUse.ModelRequestID, settlement, now)
		if err != nil {
			return err
		}
		projectionJSON, err := MarshalJSON(projection)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id, projection_json,
				tool_use_event_id, created_at, updated_at, processed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$14,$14)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
			resultEventType, payloadJSON, visibility, sessionVisible,
			StableRuntimeID("runtime_termination_tool_result", runtimeWriteID, toolUse.EventID),
			toolUse.ModelRequestID, projectionJSON, toolUse.EventID, now,
		); err != nil {
			return ToolRelationInsertError(err)
		}
		if _, err := AppendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
			return err
		}
		if err := ConsumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, toolUse.EventID, eventID, "runtime_terminated", now); err != nil {
			return err
		}
		if err := cancelPendingToolUseForTerminalResultTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return err
		}
	}
	completionMail, err := runtimeTerminationCompletionMailTextTx(ctx, tx, scope, threadScope, failure)
	if err != nil {
		return err
	}
	if completionMail != nil {
		if _, err := AppendDeclaredCompletionMailForSourceTx(
			ctx, tx, scope, threadScope, runtimeWriteID, *completionMail, now,
		); err != nil {
			return err
		}
	}
	return nil
}

const runtimeTerminationCompletionReasonMaxBytes = 3600

const runtimeTerminationCompletionGuidance = "This agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task."

func runtimeTerminationCompletionMailTextTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope ThreadMutationScope,
	failure RuntimeTerminationFailure,
) (*string, error) {
	if threadScope.Role != "subagent" {
		return nil, nil
	}
	if !threadScope.TaskName.Valid || threadScope.TaskName.String == "" {
		return nil, status.Error(codes.FailedPrecondition, "runtime termination sub-agent task name is missing")
	}
	_, _, targetTaskName, err := completionLineageTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	targetTask := "main"
	if targetTaskName.Valid && targetTaskName.String != "" {
		targetTask = targetTaskName.String
	}
	payload := fmt.Sprintf(
		"Agent errored: %s\n\n%s",
		truncateRuntimeTerminationCompletionReason(failure.Message),
		runtimeTerminationCompletionGuidance,
	)
	envelope := strings.Join([]string{
		"Message Type: FINAL_ANSWER",
		"Task name: " + targetTask,
		"Sender: " + threadScope.TaskName.String,
		"Payload:",
		payload,
	}, "\n")
	return &envelope, nil
}

func truncateRuntimeTerminationCompletionReason(reason string) string {
	bytes := []byte(reason)
	if len(bytes) <= runtimeTerminationCompletionReasonMaxBytes {
		return reason
	}
	halfBudget := runtimeTerminationCompletionReasonMaxBytes / 2
	headEnd := halfBudget
	for headEnd > 0 && bytes[headEnd]&0xc0 == 0x80 {
		headEnd--
	}
	tailStart := len(bytes) - halfBudget
	for tailStart < len(bytes) && bytes[tailStart]&0xc0 == 0x80 {
		tailStart++
	}
	removedTokens := (len(bytes) - runtimeTerminationCompletionReasonMaxBytes + 3) / 4
	return string(bytes[:headEnd]) + fmt.Sprintf("…%d tokens truncated…", removedTokens) + string(bytes[tailStart:])
}

type runtimeTerminationSibling struct {
	threadID    string
	threadScope ThreadMutationScope
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
			&sibling.threadScope.Visibility,
			&sibling.threadScope.Role,
			&sibling.threadScope.Status,
			&sibling.threadScope.TaskName,
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

	starts, err := RuntimePodLostOpenRequestStartsTx(
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
		if _, err := InsertRuntimeTerminalRequestEndTx(
			ctx,
			tx,
			ScopeForThread(scope, start.SessionThreadID),
			start,
			"runtime_terminated",
			"rwrite_session_termination_"+runtimeWriteID+"_",
			now,
		); err != nil {
			return err
		}
	}

	toolUses, err := selectRuntimeTerminationOrphanToolUsesTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadIDs,
	)
	if err != nil {
		return err
	}
	for _, toolUse := range toolUses {
		inserted, err := InsertRuntimeTerminalToolResultForScopeTx(
			ctx,
			tx,
			ScopeForThread(scope, toolUse.SessionThreadID),
			toolUse,
			TerminalToolResult{
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
		if !RowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "session termination child status is stale")
		}
		childScope := ScopeForThread(scope, sibling.threadID)
		payloadJSON, err := ThreadStatusPayloadJSON("session.thread_status_terminated", childScope, sibling.threadScope, "")
		if err != nil {
			return err
		}
		if _, err := InsertRuntimeTerminationEventTx(
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
// runner lease before the Runtime's typed result can release hot state.
type RuntimeTerminationCustodyTransitions struct {
	Accepted int
	Parked   int
}

func cancelRuntimeTerminationInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sessionWide bool,
	includeQueued bool,
	now time.Time,
) (RuntimeTerminationCustodyTransitions, error) {
	threadID := scope.GetSessionThreadId()
	if sessionWide {
		threadID = ""
	}
	var transitions RuntimeTerminationCustodyTransitions
	err := tx.QueryRow(ctx,
		`WITH target_inputs AS MATERIALIZED (
		    SELECT inbox.runtime_input_id, inbox.status
		      FROM session_runtime_inbox inbox
		     WHERE inbox.workspace_id = $1
		       AND inbox.session_id = $2
		       AND ($3 = '' OR inbox.session_thread_id = $3)
		       AND (inbox.input_kind <> 'approval_review' OR $3 = '')
		       AND (
		           inbox.status IN ('delivering', 'accepted')
		           OR ($5 AND inbox.status IN ('queued', 'parked'))
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
		includeQueued,
	).Scan(&transitions.Accepted, &transitions.Parked)
	if err != nil || !sessionWide {
		return transitions, err
	}
	_, err = tx.Exec(ctx,
		`UPDATE queue_jobs
			    SET status='cancelled', cancelled_at=$3,
		        lease_token=NULL, leased_by=NULL, leased_at=NULL, leased_until=NULL,
		        updated_at=$3
			  WHERE workspace_id=$1 AND kind IN ($4, $5, $6, $7) AND partition_key=$2
		    AND status IN ('pending','leased')`,
		scope.GetWorkspaceId(),
		queue.FormatSessionPartitionKey(workspace.ID(scope.GetWorkspaceId()), scope.GetSessionId()),
		now,
		queue.KindRuntimeRecovery,
		queue.KindRuntimeConfigUpdate,
		queue.KindCleanupSession,
		queue.KindRuntimeInput,
	)
	return transitions, err
}

func appendRuntimeTerminationErrorTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope ThreadMutationScope, runtimeWriteID string, failureJSON string, now time.Time) (runtimeTerminationEventFact, error) {
	var failure RuntimeTerminationFailure
	if err := json.Unmarshal([]byte(failureJSON), &failure); err != nil {
		return runtimeTerminationEventFact{}, err
	}
	payloadJSON, err := MarshalJSON(map[string]any{
		"type":  "session.error",
		"error": publicRuntimeTerminationError(failure),
	})
	if err != nil {
		return runtimeTerminationEventFact{}, err
	}
	return InsertRuntimeTerminationEventTx(
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

func publicRuntimeTerminationError(failure RuntimeTerminationFailure) map[string]any {
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

func appendRuntimeTerminatedStatusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, threadScope ThreadMutationScope, runtimeWriteID string, now time.Time) (runtimeTerminationEventFact, error) {
	eventType := "session.thread_status_terminated"
	payloadJSON, err := ThreadStatusPayloadJSON(eventType, scope, threadScope, "")
	if err != nil {
		return runtimeTerminationEventFact{}, err
	}
	if threadScope.Role == "main" {
		eventType = "session.status_terminated"
		payloadJSON, err = MarshalJSON(map[string]any{"type": eventType})
		if err != nil {
			return runtimeTerminationEventFact{}, err
		}
		result, err := tx.Exec(ctx,
			`UPDATE sessions SET status = 'terminated', updated_at = $3 WHERE workspace_id = $1 AND id = $2`,
			scope.GetWorkspaceId(), scope.GetSessionId(), now)
		if err != nil {
			return runtimeTerminationEventFact{}, err
		}
		if !RowsAffected(result) {
			return runtimeTerminationEventFact{}, status.Error(codes.FailedPrecondition, "runtime session is stale")
		}
		result, err = tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'failed',
			        closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
			    AND status NOT IN ('terminated', 'failed')`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
		if err != nil {
			return runtimeTerminationEventFact{}, err
		}
		if !RowsAffected(result) {
			return runtimeTerminationEventFact{}, status.Error(codes.FailedPrecondition, "runtime main thread status update failed")
		}
	} else {
		result, err := tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'failed', closed_at = COALESCE(closed_at, $4), last_active_at = $4, updated_at = $4
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3 AND role <> 'main'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
		if err != nil {
			return runtimeTerminationEventFact{}, err
		}
		if !RowsAffected(result) {
			return runtimeTerminationEventFact{}, status.Error(codes.FailedPrecondition, "child thread status update failed")
		}
	}
	statusStamp, err := InsertRuntimeTerminationEventTx(ctx, tx, scope, threadScope, runtimeWriteID, runtimeWriteID, eventType, payloadJSON, now)
	if err != nil {
		return runtimeTerminationEventFact{}, err
	}
	if threadScope.Role != "main" {
		return statusStamp, nil
	}
	// Terminal receipt replay is independent of residency. Close the live
	// binding and running markers in this transaction so cleanup/GC observes an
	// unbound terminal Session and late Runtime declarations cannot re-arm it.
	runtimeStatusResult, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET status = 'idle',
		        status_event_id = $3,
		        idle_since = $4,
		        binding_id = NULL,
		        binding_generation = NULL,
		        active_seconds_total = active_seconds_total + CASE
		          WHEN running_since IS NULL THEN 0
		          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($4 - running_since)))
		        END,
		        running_since = NULL,
		        cleanup_after = NULL,
		        cleanup_enqueued_at = NULL,
		        cleanup_claimed_at = NULL,
		        cleanup_job_id = NULL,
		        updated_at = $4
		  WHERE workspace_id = $1 AND session_id = $2`,
		scope.GetWorkspaceId(), scope.GetSessionId(), statusStamp.EventID, now)
	if err != nil {
		return runtimeTerminationEventFact{}, err
	}
	if !RowsAffected(runtimeStatusResult) {
		return runtimeTerminationEventFact{}, status.Error(codes.FailedPrecondition, "runtime residency closeout is stale")
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM session_runtime_bindings
		  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetBinding().GetBindingId(), scope.GetBinding().GetBindingGeneration()); err != nil {
		return runtimeTerminationEventFact{}, err
	}
	return statusStamp, nil
}

type runtimeTerminationEventFact struct {
	EventID       string
	EventSequence int64
}

func InsertRuntimeTerminationEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope ThreadMutationScope,
	runtimeWriteID string,
	sourceEventID string,
	eventType string,
	payloadJSON string,
	now time.Time,
) (runtimeTerminationEventFact, error) {
	visibility, sessionVisible := threadScope.PublicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return runtimeTerminationEventFact{}, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $7, $11, $11, $11)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID, sequence,
		eventType, payloadJSON, visibility, sessionVisible, runtimeWriteID, now); err != nil {
		return runtimeTerminationEventFact{}, err
	}
	if _, err := AppendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return runtimeTerminationEventFact{}, err
	}
	return runtimeTerminationEventFact{EventID: eventID, EventSequence: sequence}, nil
}
