package jobrunner

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/tetral-ai/tetral/internal/runtimecontrol"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/sessioneventwrite"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file owns durable repair after a Runtime Pod is proven lost. Runtime
// custody is disposable; losing it never releases the Session's Sandbox.

type runtimePodLostAffectedThreads struct {
	MainThreadID string
	ThreadIDs    []string
}

func repairLostRuntimeBindingDetailedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, now time.Time) (int, int, error) {
	barriers, err := runtimecontrol.ActiveInterruptBarriersTx(ctx, tx, workspaceID, sessionID, "")
	if err != nil {
		return 0, 0, err
	}
	interruptedThreads := make(map[string]string, len(barriers))
	for threadID, barrier := range barriers {
		interruptedThreads[threadID] = barrier.RuntimeInputID
	}
	handedOff, err := runtimecontrol.HandBackRuntimeInputsTx(ctx, tx, workspaceID, sessionID, binding, now)
	if err != nil {
		return 0, 0, err
	}
	affected, err := runtimePodLostAffectedThreadsTx(ctx, tx, workspaceID, sessionID, binding)
	if err != nil {
		return 0, 0, err
	}
	starts, err := runtimecontrol.RuntimePodLostOpenRequestStartsTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, 0, err
	}
	starts = filterRuntimePodLostRequestStarts(starts, interruptedThreads)
	toolUses, err := runtimePodLostOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, 0, err
	}
	if err := enqueueInterruptedRuntimePodLostRecoveriesTx(
		ctx,
		tx,
		workspaceID,
		sessionID,
		affected.ThreadIDs,
		interruptedThreads,
		binding,
		now,
	); err != nil {
		return 0, 0, err
	}
	toolUses = filterRuntimePodLostToolUses(toolUses, interruptedThreads)
	repaired := handedOff
	for _, start := range starts {
		inserted, err := insertRuntimePodLostRequestEndTx(ctx, tx, workspaceID, sessionID, binding, start, now)
		if err != nil {
			return 0, 0, err
		}
		if inserted {
			repaired++
		}
	}
	for _, toolUse := range toolUses {
		inserted, err := insertRuntimePodLostToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, now)
		if err != nil {
			return 0, 0, err
		}
		if inserted {
			repaired++
		}
	}
	// Tool closeout removes every Sandbox execution blocker before readiness is
	// evaluated once for the Session. The shared release boundary assigns fresh
	// Queue custody atomically; duplicate repair and late settlement therefore
	// observe the same pending operation/job rather than creating another wake.
	releaseJobs, err := sandboxrelease.ReadyRequestsTx(ctx, tx, workspaceID, sessionID, now, nil)
	if err != nil {
		return 0, 0, err
	}
	if _, err := queue.EnqueueBatchTx(ctx, tx, releaseJobs); err != nil {
		return 0, 0, err
	}
	uninterruptedThreadIDs := filterRuntimePodLostThreadIDs(affected.ThreadIDs, interruptedThreads)
	deliveryRepaired, err := settleRuntimePodLostSubAgentDeliveriesTx(ctx, tx, workspaceID, sessionID, uninterruptedThreadIDs, binding, now)
	if err != nil {
		return 0, 0, err
	}
	repaired += deliveryRepaired
	for _, start := range starts {
		if _, interrupted := interruptedThreads[start.SessionThreadID]; interrupted {
			continue
		}
		if err := retainRuntimePodLostToolPairsTx(ctx, tx, workspaceID, sessionID, start, now); err != nil {
			return 0, 0, err
		}
	}
	affected.ThreadIDs = uninterruptedThreadIDs
	liveScopesSettled, err := settleRuntimePodLostLiveScopesTx(ctx, tx, workspaceID, sessionID, affected, binding, now)
	if err != nil {
		return 0, 0, err
	}
	repaired += liveScopesSettled
	return repaired, handedOff, nil
}

func enqueueInterruptedRuntimePodLostRecoveriesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	threadIDs []string,
	interruptedThreads map[string]string,
	binding runtimecontrol.Binding,
	now time.Time,
) error {
	for _, threadID := range threadIDs {
		if _, interrupted := interruptedThreads[threadID]; !interrupted {
			continue
		}
		scope := runtimePodLostRepairScope(workspaceID, sessionID, threadID, binding)
		toolUseEventID, ownedToolRoute, err := runtimePodLostNonterminalToolOwnerTx(ctx, tx, scope)
		if err != nil {
			return err
		}
		if !ownedToolRoute {
			continue
		}
		if err := retireRuntimePodLostRecoverableBindingTx(ctx, tx, scope, binding, toolUseEventID, now); err != nil {
			return err
		}
	}
	return nil
}

func filterRuntimePodLostThreadIDs(threadIDs []string, interrupted map[string]string) []string {
	if len(interrupted) == 0 {
		return threadIDs
	}
	filtered := make([]string, 0, len(threadIDs))
	for _, threadID := range threadIDs {
		if _, blocked := interrupted[threadID]; !blocked {
			filtered = append(filtered, threadID)
		}
	}
	return filtered
}

func filterRuntimePodLostRequestStarts(starts []runtimecontrol.OpenRequestStart, interrupted map[string]string) []runtimecontrol.OpenRequestStart {
	if len(interrupted) == 0 {
		return starts
	}
	filtered := make([]runtimecontrol.OpenRequestStart, 0, len(starts))
	for _, start := range starts {
		if _, blocked := interrupted[start.SessionThreadID]; !blocked {
			filtered = append(filtered, start)
		}
	}
	return filtered
}

func filterRuntimePodLostToolUses(toolUses []runtimecontrol.OrphanToolUse, interrupted map[string]string) []runtimecontrol.OrphanToolUse {
	if len(interrupted) == 0 {
		return toolUses
	}
	filtered := make([]runtimecontrol.OrphanToolUse, 0, len(toolUses))
	for _, toolUse := range toolUses {
		if _, blocked := interrupted[toolUse.SessionThreadID]; !blocked {
			filtered = append(filtered, toolUse)
		}
	}
	return filtered
}

// runtimePodLostRetainPageSize bounds one page of missing Tool facts that Pod-loss
// repair projects and appends.
const runtimePodLostRetainPageSize = 128

// retainRuntimePodLostToolPairsTx appends to the request's Assistant message
// only the Tool facts its immutable Tool events prove and its parts lack.
// Candidates are scalar event identities in event order: a Tool Use yields its
// call, an ordinary or MCP result yields a result keyed by the call ID of the
// Tool Use it references, and a synthetic invalid-tool repair yields its call
// then its result. Each candidate is anti-joined on the message's part
// identity index, and only the selected missing facts are projected from
// their events. Existing parts are never read, validated or rewritten; the
// next context read validates them. Events authorize the additions; parts are
// never settlement authority.
func retainRuntimePodLostToolPairsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	start runtimecontrol.OpenRequestStart,
	now time.Time,
) error {
	scope := &bridgev1.RuntimeScope{WorkspaceId: workspaceID, SessionId: sessionID, SessionThreadId: start.SessionThreadID}
	header, found, err := runtimecontrol.LockAssistantMessageHeaderTx(ctx, tx, scope, start.ModelRequestID)
	if err != nil || !found {
		return err
	}
	var afterSequence, afterOrdinal int64
	for {
		missing, err := runtimePodLostMissingToolPartsTx(ctx, tx, scope, start.ModelRequestID, header.MessageID, afterSequence, afterOrdinal)
		if err != nil {
			return err
		}
		if len(missing) == 0 {
			return nil
		}
		parts := make([]runtimecontrol.AssistantPart, 0, len(missing))
		for _, candidate := range missing {
			var part map[string]any
			if candidate.partKind == "tool_call" {
				part, err = runtimeToolCallPartFromProjection(candidate.projectionJSON)
			} else {
				part, err = runtimeToolResultPartFromProjection(candidate.projectionJSON)
			}
			if err != nil {
				return err
			}
			if part["modelToolCallId"] != candidate.modelToolCallID {
				return status.Error(codes.FailedPrecondition, "durable Tool projection identity is inconsistent")
			}
			parts = append(parts, runtimecontrol.AssistantPart{Value: part})
		}
		header, err = runtimecontrol.AppendAssistantMessagePartsTx(ctx, tx, runtimecontrol.AssistantPartsAppend{
			Scope: scope, ModelRequestID: start.ModelRequestID, Header: &header, Parts: parts, Now: now,
		})
		if err != nil {
			return err
		}
		if len(missing) < runtimePodLostRetainPageSize {
			return nil
		}
		last := missing[len(missing)-1]
		afterSequence, afterOrdinal = last.sequence, last.ordinal
	}
}

type runtimePodLostMissingToolPart struct {
	sequence        int64
	ordinal         int64
	partKind        string
	modelToolCallID string
	projectionJSON  string
}

// runtimePodLostMissingToolPartsTx selects the next page of missing Tool facts
// after (afterSequence, afterOrdinal). The candidate and anti-join steps read
// only scalar event and part identities; a projection is read only for a
// selected missing candidate.
func runtimePodLostMissingToolPartsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	messageID string,
	afterSequence int64,
	afterOrdinal int64,
) ([]runtimePodLostMissingToolPart, error) {
	rows, err := tx.Query(ctx,
		`WITH candidate AS (
			SELECT use_event.sequence, 0 AS ordinal, 'tool_call' AS part_kind,
			       use_event.model_tool_call_id, use_event.event_id
			  FROM session_events use_event
			 WHERE use_event.workspace_id = $1 AND use_event.session_id = $2
			   AND use_event.session_thread_id = $3 AND use_event.model_request_id = $4
			   AND use_event.type IN ('agent.tool_use', 'agent.mcp_tool_use')
			UNION ALL
			SELECT repair.sequence, repair_candidate.ordinal, repair_candidate.part_kind,
			       repair.model_tool_call_id, repair.event_id
			  FROM session_events repair
			 CROSS JOIN (VALUES (0, 'tool_call'), (1, 'tool_result')) repair_candidate(ordinal, part_kind)
			 WHERE repair.workspace_id = $1 AND repair.session_id = $2
			   AND repair.session_thread_id = $3 AND repair.model_request_id = $4
			   AND repair.type = 'agent.tool_result'
			   AND repair.model_tool_call_id IS NOT NULL
			UNION ALL
			SELECT result.sequence, 0, 'tool_result', use_event.model_tool_call_id, result.event_id
			  FROM session_events result
			  JOIN session_events use_event
			    ON use_event.workspace_id = result.workspace_id
			   AND use_event.session_id = result.session_id
			   AND use_event.session_thread_id = result.session_thread_id
			   AND use_event.event_id = result.tool_use_event_id
			 WHERE result.workspace_id = $1 AND result.session_id = $2
			   AND result.session_thread_id = $3 AND result.model_request_id = $4
			   AND result.type IN ('agent.tool_result', 'agent.mcp_tool_result')
			   AND result.tool_use_event_id IS NOT NULL
		), missing AS (
			SELECT candidate.sequence, candidate.ordinal, candidate.part_kind,
			       candidate.model_tool_call_id, candidate.event_id
			  FROM candidate
			 WHERE (candidate.sequence, candidate.ordinal) > ($6, $7)
			   AND NOT EXISTS (
			     SELECT 1
			       FROM session_message_parts part
			      WHERE part.workspace_id = $1
			        AND part.message_id = $5
			        AND part.part_kind = candidate.part_kind
			        AND part.model_tool_call_id = candidate.model_tool_call_id
			   )
			 ORDER BY candidate.sequence, candidate.ordinal
			 LIMIT $8
		)
		SELECT missing.sequence, missing.ordinal, missing.part_kind, missing.model_tool_call_id, event.projection_json
		  FROM missing
		  JOIN session_events event
		    ON event.workspace_id = $1
		   AND event.session_id = $2
		   AND event.session_thread_id = $3
		   AND event.event_id = missing.event_id
		 ORDER BY missing.sequence, missing.ordinal`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
		messageID, afterSequence, afterOrdinal, runtimePodLostRetainPageSize,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	missing := make([]runtimePodLostMissingToolPart, 0)
	for rows.Next() {
		var candidate runtimePodLostMissingToolPart
		if err := rows.Scan(&candidate.sequence, &candidate.ordinal, &candidate.partKind, &candidate.modelToolCallID, &candidate.projectionJSON); err != nil {
			return nil, err
		}
		missing = append(missing, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return missing, nil
}

func runtimeToolCallPartFromProjection(projectionJSON string) (map[string]any, error) {
	var projection runtimecontrol.ToolProjection
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
		projection.ModelToolCallID == "" || projection.ToolName == "" || len(projection.ProviderInput) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool projection is malformed")
	}
	// Numbers keep their admitted tokens; a float64 round trip would round
	// large integers and re-spell signed zero and exponents.
	decoder := json.NewDecoder(bytes.NewReader(projection.ProviderInput))
	decoder.UseNumber()
	var input any
	if err := decoder.Decode(&input); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool input projection is malformed")
	}
	return map[string]any{
		"type": "tool_call", "modelToolCallId": projection.ModelToolCallID,
		"toolName": projection.ToolName, "canonicalInput": input,
	}, nil
}

func runtimeToolResultPartFromProjection(projectionJSON string) (map[string]any, error) {
	var projection runtimecontrol.ToolProjection
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil || projection.ModelToolCallID == "" {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool Result projection is malformed")
	}
	result := map[string]any{"type": projection.State}
	switch projection.State {
	case "completed":
		if projection.Output == nil {
			return nil, status.Error(codes.FailedPrecondition, "durable Tool completion projection is malformed")
		}
		result["output"] = map[string]any{"text": projection.Output.Text, "truncated": projection.Output.Truncated}
	case "error":
		if projection.Error == nil {
			return nil, status.Error(codes.FailedPrecondition, "durable Tool error projection is malformed")
		}
		result["error"] = map[string]any{
			"type": projection.Error.Type, "message": projection.Error.Message, "retryable": projection.Error.Retryable,
		}
	case "cancelled":
	default:
		return nil, status.Error(codes.FailedPrecondition, "durable Tool Result projection is malformed")
	}
	return map[string]any{
		"type": "tool_result", "modelToolCallId": projection.ModelToolCallID, "result": result,
	}, nil
}

func runtimePodLostAffectedThreadsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	binding runtimecontrol.Binding,
) (runtimePodLostAffectedThreads, error) {
	var mainThreadID, sessionStatus string
	if err := tx.QueryRow(ctx,
		`SELECT main_thread_id, status
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		workspaceID,
		sessionID,
	).Scan(&mainThreadID, &sessionStatus); dbconnect.IsNoRows(err) {
		return runtimePodLostAffectedThreads{}, runtimePodLostStaleFenceError("runtime pod-loss session is stale")
	} else if err != nil {
		return runtimePodLostAffectedThreads{}, err
	}
	var runtimeStatus string
	if err := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_runtime_status
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		binding.BindingID,
		binding.BindingGeneration,
	).Scan(&runtimeStatus); dbconnect.IsNoRows(err) {
		return runtimePodLostAffectedThreads{}, runtimePodLostStaleFenceError("runtime pod-loss status binding is stale")
	} else if err != nil {
		return runtimePodLostAffectedThreads{}, err
	}

	runtimeWasActive := runtimeStatus == "running" || sessionStatus == "rescheduling"
	threadIDs := make([]string, 0)
	rows, err := tx.Query(ctx,
		`SELECT thread.id
		   FROM session_threads thread
		  WHERE thread.workspace_id = $1
		    AND thread.session_id = $2
		    AND (
		      (thread.id = $3 AND $4)
		      OR (thread.id <> $3 AND thread.status IN ('running', 'rescheduling'))
		      OR EXISTS (
		        SELECT 1
		          FROM session_events started
		         WHERE started.workspace_id = thread.workspace_id
		           AND started.session_id = thread.session_id
		           AND started.session_thread_id = thread.id
		           AND started.type = 'span.model_request_start'
		           AND NOT EXISTS (
		             SELECT 1
		               FROM session_events ended
		              WHERE ended.workspace_id = started.workspace_id
		                AND ended.session_id = started.session_id
		                AND ended.session_thread_id = started.session_thread_id
		                AND ended.model_request_id = started.model_request_id
		                AND ended.type = 'span.model_request_end'
		           )
		      )
		      OR EXISTS (
		        SELECT 1
		          FROM session_pending_tool_uses pending
		         WHERE pending.workspace_id = thread.workspace_id
		           AND pending.session_id = thread.session_id
		           AND pending.session_thread_id = thread.id
		           AND pending.status IN ('pending', 'resolving')
		      )
		      OR EXISTS (
		        SELECT 1
		          FROM session_events tool_use
		         WHERE tool_use.workspace_id = thread.workspace_id
		           AND tool_use.session_id = thread.session_id
		           AND tool_use.session_thread_id = thread.id
		           AND tool_use.type IN ('agent.tool_use', 'agent.mcp_tool_use')
		           AND NOT EXISTS (
		             SELECT 1
		               FROM session_events result
		              WHERE result.workspace_id = tool_use.workspace_id
		                AND result.session_id = tool_use.session_id
		                AND result.session_thread_id = tool_use.session_thread_id
		                AND (
		                     (tool_use.type = 'agent.tool_use' AND result.type = 'agent.tool_result')
		                  OR (tool_use.type = 'agent.mcp_tool_use' AND result.type = 'agent.mcp_tool_result')
		                )
		                AND result.tool_use_event_id = tool_use.event_id
		           )
		      )
		    )
		  ORDER BY thread.id
		  FOR UPDATE OF thread`,
		workspaceID,
		sessionID,
		mainThreadID,
		runtimeWasActive,
	)
	if err != nil {
		return runtimePodLostAffectedThreads{}, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return runtimePodLostAffectedThreads{}, err
		}
		threadIDs = append(threadIDs, threadID)
	}
	if err := rows.Err(); err != nil {
		return runtimePodLostAffectedThreads{}, err
	}
	return runtimePodLostAffectedThreads{MainThreadID: mainThreadID, ThreadIDs: threadIDs}, nil
}

func settleRuntimePodLostLiveScopesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	affected runtimePodLostAffectedThreads,
	binding runtimecontrol.Binding,
	now time.Time,
) (int, error) {
	for _, threadID := range affected.ThreadIDs {
		if err := settleRuntimePodLostLiveScopeTx(ctx, tx, workspaceID, sessionID, affected.MainThreadID, threadID, binding, now); err != nil {
			return 0, err
		}
	}
	return len(affected.ThreadIDs), nil
}

func settleRuntimePodLostLiveScopeTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	mainThreadID string,
	threadID string,
	binding runtimecontrol.Binding,
	now time.Time,
) error {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, threadID, binding)
	threadScope, err := runtimecontrol.LockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	rescheduleEventID, rescheduleActive, err := activeRuntimePodLostRescheduleTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if rescheduleActive {
		return retireRuntimePodLostRescheduledBindingTx(ctx, tx, scope, binding, rescheduleEventID, now)
	}
	toolUseEventID, ownedToolRoute, err := runtimePodLostNonterminalToolOwnerTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if ownedToolRoute {
		return retireRuntimePodLostRecoverableBindingTx(ctx, tx, scope, binding, toolUseEventID, now)
	}
	interruptCloseoutEventID, interruptCloseoutPending, err := runtimePodLostPendingInterruptCloseoutSourceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if interruptCloseoutPending {
		return retireRuntimePodLostRecoverableBindingTx(ctx, tx, scope, binding, interruptCloseoutEventID, now)
	}
	interruptedThenLost, err := runtimePodLostInterruptedThenLostTx(ctx, tx, workspaceID, sessionID, threadID)
	if err != nil {
		return err
	}
	if !interruptedThenLost {
		errorPayload, err := runtimecontrol.MarshalJSON(map[string]any{
			"type": "session.error",
			"error": map[string]any{
				"type":         "unknown_error",
				"message":      "The session runtime was lost before the request turn settled.",
				"retry_status": map[string]any{"type": "exhausted"},
			},
		})
		if err != nil {
			return err
		}
		if _, err := insertRuntimePodLostSettlementEventTx(ctx, tx, scope, threadScope, "session.error", errorPayload, now); err != nil {
			return err
		}
	}
	idleEventType := "session.status_idle"
	stopReasonJSON := `{"type":"retries_exhausted"}`
	if interruptedThenLost {
		stopReasonJSON = `{"type":"end_turn"}`
	}
	idlePayload, err := runtimecontrol.IdleStatusPayloadJSON(stopReasonJSON)
	if threadID != mainThreadID {
		idleEventType = "session.thread_status_idle"
		idlePayload, err = runtimecontrol.ThreadStatusPayloadJSON(idleEventType, scope, threadScope, stopReasonJSON)
	}
	if err != nil {
		return err
	}
	idleEventID, err := insertRuntimePodLostSettlementEventTx(ctx, tx, scope, threadScope, idleEventType, idlePayload, now)
	if err != nil {
		return err
	}
	if err := runtimecontrol.ResetTurnRetryCountersTx(ctx, tx, scope, now); err != nil {
		return err
	}
	formattedNow := now
	if threadID != mainThreadID {
		_, err = tx.Exec(ctx,
			`UPDATE session_threads
			    SET status = 'idle',
			        last_active_at = $4,
			        updated_at = $4
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND id = $3
			    AND status IN ('running', 'rescheduling')`,
			workspaceID,
			sessionID,
			threadID,
			formattedNow,
		)
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = 'idle',
		        last_active_at = $4,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND status IN ('running', 'rescheduling', 'idle')`,
		workspaceID,
		sessionID,
		threadID,
		formattedNow,
	); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET status = CASE WHEN status = 'rescheduling' THEN 'idle' ELSE status END,
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND id = $2
		    AND status <> 'terminated'`,
		workspaceID,
		sessionID,
		formattedNow,
	); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET status = 'idle',
		        status_event_id = $5,
		        idle_since = $6,
		        active_seconds_total = active_seconds_total + CASE
		          WHEN running_since IS NULL THEN 0
		          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($6 - running_since)))
		        END,
		        running_since = NULL,
		        cleanup_after = $7,
		        cleanup_enqueued_at = NULL,
		        cleanup_claimed_at = NULL,
		        cleanup_job_id = NULL,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND binding_id = $3
		    AND binding_generation = $4`,
		workspaceID,
		sessionID,
		binding.BindingID,
		binding.BindingGeneration,
		idleEventID,
		formattedNow,
		now.Add(runtimecontrol.IdleCleanupDelay),
	)
	return err
}

func runtimePodLostNonterminalToolOwnerTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (string, bool, error) {
	var toolUseEventID string
	err := tx.QueryRow(ctx,
		`SELECT route.tool_use_event_id
		     FROM session_pending_tool_uses route
		     JOIN session_events tool_use
		       ON tool_use.workspace_id=route.workspace_id AND tool_use.session_id=route.session_id
		      AND tool_use.session_thread_id=route.session_thread_id AND tool_use.event_id=route.tool_use_event_id
		    WHERE route.workspace_id=$1 AND route.session_id=$2 AND route.session_thread_id=$3
		      AND route.status IN ('pending','resolving')
		      AND NOT EXISTS (
		        SELECT 1 FROM session_events result
		         WHERE result.workspace_id=route.workspace_id
		           AND result.session_id=route.session_id
		           AND result.session_thread_id=route.session_thread_id
		           AND result.type IN ('agent.tool_result','agent.mcp_tool_result')
		           AND result.tool_use_event_id=route.tool_use_event_id
		      )
		    ORDER BY tool_use.sequence, route.tool_use_event_id
		    LIMIT 1
		    FOR UPDATE OF route`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&toolUseEventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	return toolUseEventID, err == nil, err
}

// A lost binding does not own a Runtime-selected nonterminal Tool route. Retire
// only the process custody; the existing Thread lifecycle remains recoverable
// so a replacement Runtime can wait on the same durable owner.
func retireRuntimePodLostRecoverableBindingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	binding runtimecontrol.Binding,
	toolUseEventID string,
	now time.Time,
) error {
	if _, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET status='idle',
		        idle_since=$5,
		        active_seconds_total=active_seconds_total + CASE
		          WHEN running_since IS NULL THEN 0
		          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($5 - running_since)))
		        END,
		        running_since=NULL,
		        cleanup_after=$6,
		        cleanup_enqueued_at=NULL,
		        cleanup_claimed_at=NULL,
		        cleanup_job_id=NULL,
		        updated_at=$5
		  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), binding.BindingID, binding.BindingGeneration,
		now, now.Add(runtimecontrol.IdleCleanupDelay),
	); err != nil {
		return err
	}
	return enqueueRuntimeRecoveryTx(ctx, tx, scope, toolUseEventID, now)
}

func runtimePodLostPendingInterruptCloseoutSourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (string, bool, error) {
	var requestEndEventID string
	err := tx.QueryRow(ctx,
		`WITH latest_running AS MATERIALIZED (
		   SELECT event_id, sequence
		     FROM session_events
		    WHERE workspace_id = $1
		      AND session_id = $2
		      AND session_thread_id = $3
		      AND type IN ('session.status_running', 'session.thread_status_running')
		    ORDER BY sequence DESC
		    LIMIT 1
		 ), open_running AS MATERIALIZED (
		   SELECT event_id, sequence
		     FROM latest_running running
		    WHERE NOT EXISTS (
		      SELECT 1
		        FROM session_events closeout
		       WHERE closeout.workspace_id = $1
		         AND closeout.session_id = $2
		         AND closeout.session_thread_id = $3
		         AND closeout.type IN (
		           'session.status_idle',
		           'session.thread_status_idle',
		           'session.status_terminated',
		           'session.thread_status_terminated'
		         )
		         AND closeout.sequence > running.sequence
		    )
		 )
		 SELECT request_end.event_id
		   FROM open_running running
		   JOIN session_events request_end
		     ON request_end.workspace_id = $1
		    AND request_end.session_id = $2
		    AND request_end.session_thread_id = $3
		    AND request_end.type = 'span.model_request_end'
		    AND request_end.sequence > running.sequence
		  WHERE EXISTS (
		    SELECT 1
		      FROM session_runtime_inbox inbox
		      JOIN session_bridge_operations receipt
		        ON receipt.workspace_id = inbox.workspace_id
		       AND receipt.session_id = inbox.session_id
		       AND receipt.session_thread_id = inbox.session_thread_id
		       AND receipt.operation = $4
		       AND receipt.source_kind = 'interrupt_control'
		       AND receipt.idempotency_key = inbox.runtime_input_id
		       AND receipt.receipt_json <> ''
		      JOIN queue_jobs queue
		        ON queue.workspace_id = inbox.workspace_id
		       AND queue.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		       AND queue.kind = 'runtime_input'
		       AND queue.status IN ('pending', 'leased')
		     WHERE inbox.workspace_id = $1
		       AND inbox.session_id = $2
		       AND inbox.session_thread_id = $3
		       AND inbox.input_kind = 'interrupt_control'
		       AND inbox.status = 'committed'
		  )
		  ORDER BY request_end.sequence DESC
		  LIMIT 1
		  FOR UPDATE OF request_end`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		runtimecontrol.OperationCommitInputs,
	).Scan(&requestEndEventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	return requestEndEventID, err == nil, err
}

func activeRuntimePodLostRescheduleTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
) (string, bool, error) {
	var eventID, eventType string
	var consumed bool
	err := tx.QueryRow(ctx,
		`SELECT lifecycle.event_id, lifecycle.type, EXISTS (
		     SELECT 1 FROM session_events request_start
		      WHERE request_start.workspace_id=lifecycle.workspace_id
		        AND request_start.session_id=lifecycle.session_id
		        AND request_start.session_thread_id=lifecycle.session_thread_id
		        AND request_start.type='span.model_request_start'
		        AND request_start.sequence > lifecycle.sequence
		   )
		   FROM session_events lifecycle
		  WHERE lifecycle.workspace_id=$1 AND lifecycle.session_id=$2 AND lifecycle.session_thread_id=$3
		    AND lifecycle.type IN (
		      'session.status_running','session.thread_status_running',
		      'session.status_rescheduled','session.thread_status_rescheduled',
		      'session.status_idle','session.thread_status_idle',
		      'session.status_terminated','session.thread_status_terminated'
		    )
		  ORDER BY sequence DESC
		  LIMIT 1`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&eventID, &eventType, &consumed)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return eventID, !consumed && (eventType == "session.status_rescheduled" || eventType == "session.thread_status_rescheduled"), nil
}

// A committed reschedule remains the Turn owner after pod loss. Repair may
// finish its outstanding Tool effects, but the lost binding is retired without
// projecting a competing idle closeout or resetting the accepted retry facts.
func retireRuntimePodLostRescheduledBindingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	binding runtimecontrol.Binding,
	rescheduleEventID string,
	now time.Time,
) error {
	if _, err := tx.Exec(ctx,
		`UPDATE session_runtime_status
		    SET status='idle',
		        status_event_id=$5,
		        idle_since=$6,
		        active_seconds_total=active_seconds_total + CASE
		          WHEN running_since IS NULL THEN 0
		          ELSE GREATEST(0, EXTRACT(EPOCH FROM ($6 - running_since)))
		        END,
		        running_since=NULL,
		        cleanup_after=$7,
		        cleanup_enqueued_at=NULL,
		        cleanup_claimed_at=NULL,
		        cleanup_job_id=NULL,
		        updated_at=$6
		  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), binding.BindingID, binding.BindingGeneration,
		rescheduleEventID, now, now.Add(runtimecontrol.IdleCleanupDelay),
	); err != nil {
		return err
	}
	return enqueueRuntimeRecoveryTx(ctx, tx, scope, rescheduleEventID, now)
}

func enqueueRuntimeRecoveryTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceEventID string, now time.Time) error {
	request, err := queue.NewRuntimeRecoveryEnqueueRequest(
		workspace.ID(scope.GetWorkspaceId()), scope.GetSessionId(), scope.GetSessionThreadId(), sourceEventID, now,
	)
	if err != nil {
		return err
	}
	_, err = queue.EnqueueTx(ctx, tx, request)
	return err
}

// The interrupted-then-lost exception is intentionally decidable from one
// thread only. Processed user events and committed inter-agent delivery events
// are durable input truth; sequence values from sibling threads are unrelated.
func runtimePodLostInterruptedThenLostTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	threadID string,
) (bool, error) {
	var interruptSequence int64
	err := tx.QueryRow(ctx,
		`SELECT sequence
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'user.interrupt'
		    AND processed_at IS NOT NULL
		  ORDER BY sequence DESC
		  LIMIT 1`,
		workspaceID,
		sessionID,
		threadID,
	).Scan(&interruptSequence)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var superseded bool
	err = tx.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1
		     FROM session_events
		    WHERE workspace_id = $1
		      AND session_id = $2
		      AND session_thread_id = $3
		      AND sequence > $4
		      AND type IN ('user.message', 'user.interrupt', 'user.tool_confirmation')
		      AND processed_at IS NOT NULL
		)`,
		workspaceID,
		sessionID,
		threadID,
		interruptSequence,
	).Scan(&superseded)
	if err != nil || superseded {
		return !superseded, err
	}
	// A received projection is accepted input truth only while its exact Inbox
	// custody remains nonterminal. Locking that row keeps Pod-loss repair from
	// inferring progress from an orphaned source event.
	var receivedEventID string
	err = tx.QueryRow(ctx,
		`SELECT received.event_id
		   FROM session_events received
		   JOIN session_runtime_inbox inbox
		     ON inbox.workspace_id = received.workspace_id
		    AND inbox.session_id = received.session_id
		    AND inbox.session_thread_id = received.session_thread_id
		    AND inbox.runtime_input_id = 'agent_mail:' || (received.payload_json::jsonb ->> 'delivery_id')
		    AND inbox.input_kind = 'agent_mail'
		    AND inbox.status IN ('queued', 'delivering', 'accepted', 'committed')
		    AND inbox.event_ids_json::jsonb = jsonb_build_array(received.event_id)
		    AND inbox.sequence_from = received.sequence
		    AND inbox.sequence_to = received.sequence
		  WHERE received.workspace_id = $1
		    AND received.session_id = $2
		    AND received.session_thread_id = $3
		    AND received.sequence > $4
		    AND received.type = 'agent.thread_message_received'
		  ORDER BY received.sequence
		  LIMIT 1
		  FOR UPDATE OF inbox`,
		workspaceID,
		sessionID,
		threadID,
		interruptSequence,
	).Scan(&receivedEventID)
	if dbconnect.IsNoRows(err) {
		return true, nil
	}
	return false, err
}

func insertRuntimePodLostSettlementEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope runtimecontrol.ThreadMutationScope,
	eventType string,
	payloadJSON string,
	now time.Time,
) (string, error) {
	visibility, sessionVisible := threadScope.PublicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := runtimecontrol.NextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	if _, err := sessioneventwrite.InsertInitialTx(ctx, tx, sessioneventwrite.InitialEvent{
		WorkspaceID: scope.GetWorkspaceId(), SessionID: scope.GetSessionId(), SessionThreadID: scope.GetSessionThreadId(),
		EventID: eventID, Sequence: sequence, Type: eventType,
		PayloadJSON: payloadJSON, ProjectionJSON: payloadJSON, Visibility: visibility, SessionVisible: sessionVisible,
		CreatedAt: now, ProcessedAt: &now,
	}); err != nil {
		return "", err
	}
	return eventID, nil
}

type runtimePodLossMutationStatus string

const (
	runtimePodLossMutationRepaired runtimePodLossMutationStatus = "repaired"
	runtimePodLossMutationStale    runtimePodLossMutationStatus = "stale"
)

type runtimePodLossMutationResult struct {
	status      runtimePodLossMutationStatus
	staleReason string
}

func (s *PostgreSQLRuntimeDeliveryStore) repairLostRuntimeBinding(ctx context.Context, workspaceID string, sessionID string, binding runtimecontrol.Binding, now time.Time) error {
	result, err := s.mutateLostRuntimeBinding(ctx, workspaceID, sessionID, binding, now, false)
	if err != nil {
		return err
	}
	if result.status == runtimePodLossMutationStale {
		return runtimePodLostStaleFenceError("runtime pod-loss repair binding fence is stale")
	}
	return nil
}

// mutateLostRuntimeBinding is the sole pod-loss closeout transaction. The proactive
// caller adds an active-session admission fence; input-triggered recovery retains the
// existing idle-binding behavior while sharing every closeout and binding mutation.
func (s *PostgreSQLRuntimeDeliveryStore) mutateLostRuntimeBinding(
	ctx context.Context,
	workspaceID string,
	sessionID string,
	binding runtimecontrol.Binding,
	now time.Time,
	requireActive bool,
) (runtimePodLossMutationResult, error) {
	result := runtimePodLossMutationResult{status: runtimePodLossMutationRepaired}
	handedOff := 0
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "jobrunner.repair_lost_runtime_binding", func(tx *dbconnect.Tx) error {
		if err := runtimecontrol.LockRuntimeMutationSessionTx(ctx, tx, workspaceID, sessionID); err != nil {
			if code, ok := runtimecontrol.SentinelCode(err); requireActive && ok && code == runtimecontrol.ScopeSupersededCode {
				result = runtimePodLossMutationResult{status: runtimePodLossMutationStale, staleReason: "inactive"}
				return nil
			}
			return err
		}
		current, found, err := runtimecontrol.ReadOptionalRuntimeBindingForDeliveryTx(ctx, tx, workspaceID, sessionID)
		if err != nil {
			return err
		}
		if !found || current.BindingID != binding.BindingID || current.BindingGeneration != binding.BindingGeneration {
			if requireActive {
				result = runtimePodLossMutationResult{status: runtimePodLossMutationStale, staleReason: "binding_changed"}
				return nil
			}
			return runtimePodLostStaleFenceError("runtime pod-loss repair binding fence is stale")
		}
		// Discovery retains only the fence identity. Once that fence wins, the
		// current durable row supplies the full binding facts consumed by closeout.
		binding = current
		if requireActive {
			active, err := runtimePodLossSessionActiveTx(ctx, tx, workspaceID, sessionID, binding)
			if err != nil {
				return err
			}
			if !active {
				result = runtimePodLossMutationResult{status: runtimePodLossMutationStale, staleReason: "inactive"}
				return nil
			}
		}
		if resolver, ok := s.TargetResolver.(KubernetesRuntimeTargetResolver); ok {
			decision, err := resolver.runtimeProcessDecisionTx(ctx, tx, binding)
			if err != nil {
				return err
			}
			if decision != runtimeProcessLoss {
				result = runtimePodLossMutationResult{status: runtimePodLossMutationStale, staleReason: "process_not_lost"}
				return nil
			}
		} else {
			return runtimecontrol.PreparationError{Kind: "runtime_visibility_unavailable", Message: "process-aware Runtime loss classification is unavailable", Retryable: true}
		}
		_, count, err := repairLostRuntimeBindingDetailedTx(ctx, tx, workspaceID, sessionID, binding, now)
		if err != nil {
			return err
		}
		handedOff = count
		if _, err := tx.Exec(ctx,
			`DELETE FROM session_runtime_bindings
			  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
			workspaceID, sessionID, binding.BindingID, binding.BindingGeneration,
		); err != nil {
			return err
		}
		dbResult, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET cleanup_after=NULL, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL,
			        cleanup_job_id=NULL, binding_id=NULL, binding_generation=NULL, updated_at=$5
			  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
			workspaceID, sessionID, binding.BindingID, binding.BindingGeneration, now,
		)
		if err != nil {
			return err
		}
		if !runtimecontrol.RowsAffected(dbResult) {
			return runtimePodLostStaleFenceError("runtime pod-loss binding finalization fence is stale")
		}
		return nil
	})
	if err != nil {
		var confirmation runtimePodConfirmationRequired
		if errors.As(err, &confirmation) {
			resolver := s.TargetResolver.(KubernetesRuntimeTargetResolver)
			observation, confirmErr := resolver.confirmRuntimePod(ctx, confirmation.binding)
			if confirmErr != nil {
				return runtimePodLossMutationResult{}, confirmErr
			}
			return s.mutateLostRuntimeBinding(context.WithValue(ctx, runtimePodObservationKey{}, observation), workspaceID, sessionID, binding, now, requireActive)
		}
		return runtimePodLossMutationResult{}, err
	}
	runtimecontrol.LogRuntimeInputCustodyTransition(s.Logger, ServiceNameJobRunner, &bridgev1.RuntimeScope{
		WorkspaceId: workspaceID,
		SessionId:   sessionID,
	}, "accepted_to_queued", handedOff)
	return result, nil
}

func runtimePodLossSessionActiveTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	binding runtimecontrol.Binding,
) (bool, error) {
	var active bool
	err := tx.QueryRow(ctx,
		`SELECT (
		    runtime.status = 'running'
		    OR session.status = 'rescheduling'
		    OR EXISTS (
		      SELECT 1
		        FROM session_runtime_inbox inbox
		       WHERE inbox.workspace_id = runtime.workspace_id
		         AND inbox.session_id = runtime.session_id
		         AND inbox.status = 'accepted'
		         AND inbox.binding_id = $3
		         AND inbox.binding_generation = $4
		         AND inbox.target_pod_uid = $5
		    )
		  )
		   FROM session_runtime_status runtime
		   JOIN sessions session
		     ON session.workspace_id = runtime.workspace_id
		    AND session.id = runtime.session_id
		  WHERE runtime.workspace_id = $1
		    AND runtime.session_id = $2
		    AND runtime.binding_id = $3
		    AND runtime.binding_generation = $4
		  FOR UPDATE OF runtime`,
		workspaceID, sessionID, binding.BindingID, binding.BindingGeneration, binding.PodUID,
	).Scan(&active)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	return active, err
}

func runtimePodLostStaleFenceError(message string) runtimecontrol.PreparationError {
	return runtimecontrol.PreparationError{Kind: "runtime_pod_lost_claim_stale", Message: message, Retryable: true}
}

func runtimePodLostOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]runtimecontrol.OrphanToolUse, error) {
	return runtimecontrol.RuntimeOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affectedThreadIDs, runtimecontrol.RuntimeOrphanToolSelection{
		PreserveNonterminalRoutes: true,
	})
}

func insertRuntimePodLostRequestEndTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, start runtimecontrol.OpenRequestStart, now time.Time) (bool, error) {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, start.SessionThreadID, binding)
	return runtimecontrol.InsertRuntimeTerminalRequestEndTx(ctx, tx, scope, start, "runtime_pod_lost", "rwrite_runtime_pod_lost_", now)
}

func insertRuntimePodLostToolResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, toolUse runtimecontrol.OrphanToolUse, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimecontrol.TerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_tool_",
		Reason:        "runtime_pod_lost",
		ErrorType:     "runtime_pod_lost",
		Message:       "Tool result unavailable because the runtime pod was lost.",
		Retryable:     false,
	}, now)
}

type runtimePodLostSubAgentDelivery struct {
	ToolUse   runtimecontrol.OrphanToolUse
	ToolName  string
	SentEvent string
	Delivery  string
}

func settleRuntimePodLostSubAgentDeliveriesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string, binding runtimecontrol.Binding, now time.Time) (int, error) {
	deliveries, err := runtimePodLostSubAgentDeliveriesTx(ctx, tx, workspaceID, sessionID, affectedThreadIDs)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, delivery := range deliveries {
		if _, exists, err := runtimecontrol.ToolResultForToolUseExistsTx(ctx, tx, workspaceID, sessionID, delivery.ToolUse.SessionThreadID, "agent.tool_use", delivery.ToolUse.EventID); err != nil {
			return 0, err
		} else if exists {
			continue
		}
		if delivery.SentEvent == "" || delivery.Delivery == "" {
			inserted, err := insertRuntimePodLostSubAgentDeliveryErrorTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse,
				"Sub-agent delivery could not be recovered because its durable delivery anchor is missing.", now)
			if err != nil {
				return 0, err
			}
			if inserted {
				repaired++
			}
			continue
		}

		envelope, err := runtimecontrol.LoadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, workspaceID, sessionID, delivery.Delivery)
		if err != nil {
			switch status.Code(err) {
			case codes.AlreadyExists, codes.FailedPrecondition, codes.NotFound:
				inserted, insertErr := insertRuntimePodLostSubAgentDeliveryErrorTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse,
					"Sub-agent delivery could not be recovered because its durable delivery anchor is invalid.", now)
				if insertErr != nil {
					return 0, insertErr
				}
				if inserted {
					repaired++
				}
				continue
			default:
				return 0, err
			}
		}
		if envelope.SentEventID != delivery.SentEvent ||
			envelope.SourceThreadID != delivery.ToolUse.SessionThreadID ||
			envelope.SourceToolUseEventID != delivery.ToolUse.EventID {
			inserted, insertErr := insertRuntimePodLostSubAgentDeliveryErrorTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse,
				"Sub-agent delivery could not be recovered because its durable delivery anchor is invalid.", now)
			if insertErr != nil {
				return 0, insertErr
			}
			if inserted {
				repaired++
			}
			continue
		}
		currentInbox, err := runtimePodLostAgentMailInboxCurrentTx(ctx, tx, workspaceID, sessionID, envelope, binding)
		if err != nil {
			return 0, err
		}
		if !currentInbox {
			inserted, insertErr := insertRuntimePodLostSubAgentDeliveryErrorTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse,
				"Sub-agent delivery could not be recovered because its durable Inbox custody is invalid.", now)
			if insertErr != nil {
				return 0, insertErr
			}
			if inserted {
				repaired++
			}
			continue
		}

		parentScope := runtimePodLostRepairScope(workspaceID, sessionID, delivery.ToolUse.SessionThreadID, binding)
		if err := runtimecontrol.RequireAgentMailInputTargetTx(ctx, tx, runtimecontrol.ScopeForThread(parentScope, envelope.TargetThreadID)); err != nil {
			switch status.Code(err) {
			case codes.FailedPrecondition, codes.NotFound:
				inserted, insertErr := insertRuntimePodLostSubAgentDeliveryErrorTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse,
					"Sub-agent delivery could not be recovered because the target child is not receivable.", now)
				if insertErr != nil {
					return 0, insertErr
				}
				if inserted {
					repaired++
				}
				continue
			default:
				return 0, err
			}
		}
		var taskName string
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(task_name, '')
			   FROM session_threads
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3`,
			workspaceID, sessionID, envelope.TargetThreadID).Scan(&taskName); err != nil {
			return 0, err
		}
		resultText := "task_name: " + runtimecontrol.DefaultString(taskName, "subagent") + "\nsession_thread_id: " + envelope.TargetThreadID + "\nstatus: delivered"
		inserted, err := insertRuntimePodLostSubAgentDeliveredResultTx(ctx, tx, workspaceID, sessionID, binding, delivery.ToolUse, resultText, now)
		if err != nil {
			return 0, err
		}
		if inserted {
			repaired++
		}
	}
	return repaired, nil
}

// Pod-loss delivery repair may declare success only from the same Inbox fact
// that owns Runtime delivery. Source events and the stored mail envelope prove
// content identity, but never substitute for current or committed custody.
func runtimePodLostAgentMailInboxCurrentTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	envelope runtimecontrol.StoredAgentMailEnvelope,
	binding runtimecontrol.Binding,
) (bool, error) {
	var (
		threadID                             string
		inputKind, eventIDsJSON, inboxStatus string
		sequenceFrom, sequenceTo             sql.NullInt64
		bindingID, targetPodUID              sql.NullString
		bindingGeneration                    sql.NullInt64
	)
	err := tx.QueryRow(ctx,
		`SELECT session_thread_id, input_kind, event_ids_json, sequence_from, sequence_to,
		        status, binding_id, binding_generation, target_pod_uid
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		runtimecontrol.CompletionRuntimeInputID(envelope.DeliveryID),
	).Scan(
		&threadID,
		&inputKind,
		&eventIDsJSON,
		&sequenceFrom,
		&sequenceTo,
		&inboxStatus,
		&bindingID,
		&bindingGeneration,
		&targetPodUID,
	)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if threadID != envelope.TargetThreadID || inputKind != "agent_mail" {
		return false, nil
	}
	switch inboxStatus {
	case "queued":
		if bindingID.Valid || bindingGeneration.Valid || targetPodUID.Valid {
			return false, nil
		}
	case "delivering", "accepted", "committed":
		if !bindingID.Valid || bindingID.String != binding.BindingID ||
			!bindingGeneration.Valid || bindingGeneration.Int64 != binding.BindingGeneration ||
			!targetPodUID.Valid || targetPodUID.String != binding.PodUID {
			return false, nil
		}
	default:
		return false, nil
	}

	var eventIDs []string
	if err := json.Unmarshal([]byte(eventIDsJSON), &eventIDs); err != nil || eventIDs == nil {
		return false, nil
	}
	if len(eventIDs) == 0 {
		return inboxStatus == "queued" && !sequenceFrom.Valid && !sequenceTo.Valid, nil
	}
	receivedEventID := runtimecontrol.StableRuntimeID("agent_mail_received_event", workspaceID, sessionID, envelope.TargetThreadID, envelope.DeliveryID)
	if len(eventIDs) != 1 || eventIDs[0] != receivedEventID || !sequenceFrom.Valid || !sequenceTo.Valid || sequenceFrom.Int64 != sequenceTo.Int64 {
		return false, nil
	}
	var sequence int64
	var sourceThreadID, sourceToolUseEventID string
	err = tx.QueryRow(ctx,
		`SELECT sequence,
		        payload_json::jsonb ->> 'source_thread_id',
		        payload_json::jsonb ->> 'source_tool_use_event_id'
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $5
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		envelope.TargetThreadID,
		receivedEventID,
		envelope.DeliveryID,
	).Scan(&sequence, &sourceThreadID, &sourceToolUseEventID)
	if dbconnect.IsNoRows(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return sequence == sequenceFrom.Int64 &&
		sourceThreadID == envelope.SourceThreadID &&
		sourceToolUseEventID == envelope.SourceToolUseEventID, nil
}

func runtimePodLostSubAgentDeliveriesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]runtimePodLostSubAgentDelivery, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT tool_use.session_thread_id,
		        tool_use.event_id,
		        COALESCE(tool_use.model_request_id, ''),
		        tool_use.model_tool_call_id,
		        tool_use.payload_json,
		        COALESCE(tool_use.payload_json::jsonb ->> 'name', ''),
		        COALESCE(sent.event_id, ''),
		        COALESCE(sent.payload_json::jsonb ->> 'delivery_id', '')
		   FROM session_events tool_use
		   LEFT JOIN LATERAL (
		       SELECT candidate.event_id, candidate.payload_json
		         FROM session_events candidate
		        WHERE candidate.workspace_id = tool_use.workspace_id
		          AND candidate.session_id = tool_use.session_id
		          AND candidate.session_thread_id = tool_use.session_thread_id
		          AND candidate.type = 'agent.thread_message_sent'
		          AND candidate.payload_json::jsonb ->> 'source_tool_use_event_id' = tool_use.event_id
		        ORDER BY candidate.sequence ASC, candidate.event_id ASC
		        LIMIT 1
		   ) sent ON TRUE
		  WHERE tool_use.workspace_id = $1
		    AND tool_use.session_id = $2
		    AND tool_use.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND tool_use.type = 'agent.tool_use'
		    AND tool_use.visibility = 'public'
		    AND COALESCE(tool_use.payload_json::jsonb ->> 'name', '') IN ('spawn_agent', 'send_message')
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events result
			         WHERE result.workspace_id = tool_use.workspace_id
			           AND result.session_id = tool_use.session_id
			           AND result.session_thread_id = tool_use.session_thread_id
		           AND result.type = 'agent.tool_result'
		           AND result.tool_use_event_id = tool_use.event_id
		    )
		  ORDER BY tool_use.sequence ASC, tool_use.event_id ASC
		  FOR UPDATE OF tool_use`,
		workspaceID,
		sessionID,
		string(threadIDsJSON),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make([]runtimePodLostSubAgentDelivery, 0)
	for rows.Next() {
		var delivery runtimePodLostSubAgentDelivery
		if err := rows.Scan(
			&delivery.ToolUse.SessionThreadID,
			&delivery.ToolUse.EventID,
			&delivery.ToolUse.ModelRequestID,
			&delivery.ToolUse.ModelToolCallID,
			&delivery.ToolUse.PayloadJSON,
			&delivery.ToolName,
			&delivery.SentEvent,
			&delivery.Delivery,
		); err != nil {
			return nil, err
		}
		if delivery.ToolUse.ModelToolCallID == "" {
			return nil, status.Error(codes.FailedPrecondition, "durable subagent Tool Use identity is missing")
		}
		delivery.ToolUse.EventType = "agent.tool_use"
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func insertRuntimePodLostSubAgentDeliveredResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, toolUse runtimecontrol.OrphanToolUse, message string, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimecontrol.TerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_delivery_",
		Message:       message,
		Success:       true,
	}, now)
}

func insertRuntimePodLostSubAgentDeliveryErrorTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, toolUse runtimecontrol.OrphanToolUse, message string, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimecontrol.TerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_delivery_",
		Reason:        "runtime_pod_lost_delivery_failed",
		ErrorType:     "runtime_pod_lost_delivery_failed",
		Message:       message,
		Retryable:     false,
	}, now)
}

func insertRuntimeTerminalToolResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimecontrol.Binding, toolUse runtimecontrol.OrphanToolUse, terminal runtimecontrol.TerminalToolResult, now time.Time) (bool, error) {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, toolUse.SessionThreadID, binding)
	return runtimecontrol.InsertRuntimeTerminalToolResultForScopeTx(ctx, tx, scope, toolUse, terminal, now)
}

func runtimePodLostRepairScope(workspaceID string, sessionID string, sessionThreadID string, binding runtimecontrol.Binding) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		WorkspaceId:     workspaceID,
		SessionId:       sessionID,
		SessionThreadId: sessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         binding.BindingID,
			BindingGeneration: binding.BindingGeneration,
			TargetPodUid:      binding.PodUID,
			RuntimeProcessId:  binding.RuntimeProcessID,
		},
	}
}
