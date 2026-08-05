package agentruntimebridge

import (
	"context"
	"encoding/json"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This file owns durable repair after a Runtime Pod is proven lost. Runtime
// custody is disposable; losing it never releases the Session's Sandbox.

type runtimeOpenRequestStart struct {
	SessionThreadID string
	EventID         string
	ModelRequestID  string
	RequestKind     string
}

type runtimeOrphanToolUse struct {
	SessionThreadID string
	EventID         string
	EventType       string
	ModelRequestID  string
	PayloadJSON     string
}

type runtimePodLostAffectedThreads struct {
	MainThreadID string
	ThreadIDs    []string
}

func repairLostRuntimeBindingTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, now time.Time) (int, error) {
	affected, err := runtimePodLostAffectedThreadsTx(ctx, tx, workspaceID, sessionID, binding)
	if err != nil {
		return 0, err
	}
	starts, err := runtimePodLostOpenRequestStartsTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, err
	}
	toolUses, err := runtimePodLostOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, start := range starts {
		inserted, err := insertRuntimePodLostRequestEndTx(ctx, tx, workspaceID, sessionID, binding, start, now)
		if err != nil {
			return 0, err
		}
		if inserted {
			repaired++
		}
	}
	for _, toolUse := range toolUses {
		inserted, err := insertRuntimePodLostToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, now)
		if err != nil {
			return 0, err
		}
		if inserted {
			repaired++
		}
	}
	deliveryRepaired, err := settleRuntimePodLostSubAgentDeliveriesTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs, binding, now)
	if err != nil {
		return 0, err
	}
	repaired += deliveryRepaired
	liveScopesSettled, err := settleRuntimePodLostLiveScopesTx(ctx, tx, workspaceID, sessionID, affected, binding, now)
	if err != nil {
		return 0, err
	}
	repaired += liveScopesSettled
	return repaired, nil
}

func runtimePodLostAffectedThreadsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	binding runtimeBindingForDelivery,
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

	threadIDs := make([]string, 0)
	if runtimeStatus == "running" || sessionStatus == "rescheduling" {
		threadIDs = append(threadIDs, mainThreadID)
	}
	rows, err := tx.Query(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id <> $3
		    AND status IN ('running', 'rescheduling')
		  ORDER BY id
		  FOR UPDATE`,
		workspaceID,
		sessionID,
		mainThreadID,
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
	binding runtimeBindingForDelivery,
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
	binding runtimeBindingForDelivery,
	now time.Time,
) error {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, threadID, binding, "settlement")
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	interruptedThenLost, err := runtimePodLostInterruptedThenLostTx(ctx, tx, workspaceID, sessionID, threadID)
	if err != nil {
		return err
	}
	if !interruptedThenLost {
		errorPayload, err := marshalBridgeJSON(map[string]any{
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
	idlePayload, err := idleStatusPayloadJSON(stopReasonJSON)
	if threadID != mainThreadID {
		idleEventType = "session.thread_status_idle"
		idlePayload, err = threadStatusPayloadJSON(idleEventType, scope, threadScope, stopReasonJSON)
	}
	if err != nil {
		return err
	}
	idleEventID, err := insertRuntimePodLostSettlementEventTx(ctx, tx, scope, threadScope, idleEventType, idlePayload, now)
	if err != nil {
		return err
	}
	if err := resetTurnRetryCountersTx(ctx, tx, scope, now); err != nil {
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
		now.Add(defaultIdleCleanupDelay),
	)
	return err
}

// The interrupted-then-lost exception is intentionally decidable from one
// thread only. Processed user events and committed inter-agent receipt events
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
		      AND (
		           (type IN ('user.message', 'user.interrupt', 'user.tool_confirmation') AND processed_at IS NOT NULL)
		        OR type = 'agent.thread_message_received'
		      )
		)`,
		workspaceID,
		sessionID,
		threadID,
		interruptSequence,
	).Scan(&superseded)
	return !superseded, err
}

func insertRuntimePodLostSettlementEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	eventType string,
	payloadJSON string,
	now time.Time,
) (string, error) {
	visibility, sessionVisible := threadScope.publicProjection(eventType)
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $7, $10, $10, $10)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventType,
		payloadJSON,
		visibility,
		sessionVisible,
		now,
	); err != nil {
		return "", err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return "", err
	}
	return eventID, nil
}

func (s *PostgreSQLRuntimeDeliveryStore) repairLostRuntimeBinding(ctx context.Context, workspaceID string, sessionID string, binding runtimeBindingForDelivery, now time.Time) error {
	return s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.repair_lost_runtime_binding", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, workspaceID, sessionID); err != nil {
			return err
		}
		current, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, workspaceID, sessionID)
		if err != nil {
			return err
		}
		if !found || current.BindingID != binding.BindingID || current.BindingGeneration != binding.BindingGeneration {
			return runtimePodLostStaleFenceError("runtime pod-loss repair binding fence is stale")
		}
		if _, err := repairLostRuntimeBindingTx(ctx, tx, workspaceID, sessionID, binding, now); err != nil {
			return err
		}
		if _, err := rearmPendingCompletionMailForSessionTx(ctx, tx, workspaceID, sessionID, now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM session_runtime_bindings
			  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
			workspaceID, sessionID, binding.BindingID, binding.BindingGeneration,
		); err != nil {
			return err
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_status
			    SET cleanup_after=NULL, cleanup_enqueued_at=NULL, cleanup_claimed_at=NULL,
			        cleanup_job_id=NULL, binding_id=NULL, binding_generation=NULL, updated_at=$5
			  WHERE workspace_id=$1 AND session_id=$2 AND binding_id=$3 AND binding_generation=$4`,
			workspaceID, sessionID, binding.BindingID, binding.BindingGeneration, now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return runtimePodLostStaleFenceError("runtime pod-loss binding finalization fence is stale")
		}
		return nil
	})
}

func runtimePodLostStaleFenceError(message string) runtimeDeliveryPrepareError {
	return runtimeDeliveryPrepareError{kind: "runtime_pod_lost_claim_stale", message: message, retryable: true}
}

func runtimePodLostOpenRequestStartsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]runtimeOpenRequestStart, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.model_request_id, e.projection_json
		   FROM session_events e
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND e.type = 'span.model_request_start'
		    AND COALESCE(e.model_request_id, '') <> ''
			    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events ended
		         WHERE ended.workspace_id = e.workspace_id
		           AND ended.session_id = e.session_id
		           AND ended.session_thread_id = e.session_thread_id
		           AND ended.model_request_id = e.model_request_id
		           AND ended.type = 'span.model_request_end'
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		workspaceID,
		sessionID,
		string(threadIDsJSON),
	)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return starts, nil
}

func runtimePodLostOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]runtimeOrphanToolUse, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.type, e.model_request_id, e.payload_json
		   FROM session_events e
		   JOIN session_threads thread_scope
		     ON thread_scope.workspace_id = e.workspace_id
		    AND thread_scope.session_id = e.session_id
		    AND thread_scope.id = e.session_thread_id
		   JOIN session_messages m
		     ON m.workspace_id = e.workspace_id
		    AND m.session_id = e.session_id
		    AND m.session_thread_id = e.session_thread_id
		    AND m.model_request_id = e.model_request_id
		    AND m.kind = 'assistant'
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND e.type IN ('agent.tool_use', 'agent.mcp_tool_use')
		    AND (
		      e.visibility = 'public'
		      OR (e.visibility = 'internal' AND thread_scope.role = 'approval_reviewer')
		    )
			    AND (
			      e.type <> 'agent.tool_use'
			      OR COALESCE(e.payload_json::jsonb ->> 'name', '') NOT IN ('spawn_agent', 'send_message')
			    )
			    AND EXISTS (
		        SELECT 1
		          FROM jsonb_array_elements(
		               CASE
		                 WHEN jsonb_typeof(m.data_json::jsonb -> 'parts') = 'array'
		                   THEN m.data_json::jsonb -> 'parts'
		                 ELSE '[]'::jsonb
		               END
		          ) AS part
		         WHERE part ->> 'type' = 'tool'
		           AND part ->> 'toolUseEventId' = e.event_id
		           AND part #>> '{state,status}' IN ('pending', 'running')
		    )
		    AND NOT EXISTS (
		        SELECT 1
		          FROM session_events result
			         WHERE result.workspace_id = e.workspace_id
			           AND result.session_id = e.session_id
			           AND result.session_thread_id = e.session_thread_id
			           AND (
			                (e.type = 'agent.tool_use'
			                 AND result.type = 'agent.tool_result'
			                 AND (result.payload_json::jsonb ->> 'tool_use_event_id' = e.event_id
			                      OR result.payload_json::jsonb ->> 'tool_use_id' = e.event_id))
			             OR (e.type = 'agent.mcp_tool_use'
			                 AND result.type = 'agent.mcp_tool_result'
			                 AND result.payload_json::jsonb ->> 'mcp_tool_use_id' = e.event_id)
			           )
		    )
		  ORDER BY e.sequence ASC, e.event_id ASC
		  FOR UPDATE OF e`,
		workspaceID,
		sessionID,
		string(threadIDsJSON),
	)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return toolUses, nil
}

func insertRuntimePodLostRequestEndTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, start runtimeOpenRequestStart, now time.Time) (bool, error) {
	return insertRuntimeTerminalRequestEndTx(ctx, tx, workspaceID, sessionID, binding, start, "runtime_pod_lost", "rwrite_runtime_pod_lost_", now)
}

func insertRuntimeTerminalRequestEndTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, start runtimeOpenRequestStart, errorKind string, writeIDPrefix string, now time.Time) (bool, error) {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, start.SessionThreadID, binding, start.ModelRequestID)
	if _, ok, err := modelRequestEndExistsTx(ctx, tx, workspaceID, sessionID, start.SessionThreadID, start.ModelRequestID); err != nil || ok {
		return false, err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	visibility, sessionVisible := threadScope.publicProjection("span.model_request_end")
	request := &bridgev1.WriteRequestEndRequest{
		Scope:                    scope,
		RuntimeWriteId:           writeIDPrefix + start.ModelRequestID,
		ModelRequestId:           start.ModelRequestID,
		ModelRequestStartEventId: start.EventID,
		RequestKind:              start.RequestKind,
		IsError:                  true,
		ErrorKind:                errorKind,
		FinishReason:             "error",
		UsageJson:                "{}",
	}
	payloadJSON, err := modelRequestEndPayloadJSON(request, start.RequestKind, "error", bridgeUsage{})
	if err != nil {
		return false, err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'span.model_request_end', $6, $7, $8, $9, $10, $6, $11, $11, $11)`,
		workspaceID,
		sessionID,
		start.SessionThreadID,
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		request.GetRuntimeWriteId(),
		start.ModelRequestID,
		now,
	); err != nil {
		return false, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO request_usage_details (
			workspace_id, session_id, session_thread_id, model_request_id, runtime_write_id,
			request_kind, input_total_tokens, input_uncached_tokens, output_total_tokens,
			total_tokens, provider_usage_json, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 0, 0, 0, 0, '{}', $7)
		ON CONFLICT (workspace_id, session_id, model_request_id, runtime_write_id) DO NOTHING`,
		workspaceID,
		sessionID,
		start.SessionThreadID,
		start.ModelRequestID,
		request.GetRuntimeWriteId(),
		start.RequestKind,
		now,
	)
	if err != nil {
		return false, err
	}
	return true, nil
}

func insertRuntimePodLostToolResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, toolUse runtimeOrphanToolUse, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimeTerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_tool_",
		Reason:        "runtime_pod_lost",
		ErrorType:     "runtime_pod_lost",
		Message:       "Tool result unavailable because the runtime pod was lost.",
		Retryable:     false,
	}, now)
}

type runtimePodLostSubAgentDelivery struct {
	ToolUse   runtimeOrphanToolUse
	ToolName  string
	SentEvent string
	Delivery  string
}

func settleRuntimePodLostSubAgentDeliveriesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string, binding runtimeBindingForDelivery, now time.Time) (int, error) {
	deliveries, err := runtimePodLostSubAgentDeliveriesTx(ctx, tx, workspaceID, sessionID, affectedThreadIDs)
	if err != nil {
		return 0, err
	}
	repaired := 0
	for _, delivery := range deliveries {
		if _, exists, err := toolResultForToolUseExistsTx(ctx, tx, workspaceID, sessionID, delivery.ToolUse.SessionThreadID, "agent.tool_use", delivery.ToolUse.EventID); err != nil {
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

		envelope, err := loadStoredAgentMailEnvelopeByDeliveryTx(ctx, tx, workspaceID, sessionID, delivery.Delivery)
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

		parentScope := runtimePodLostRepairScope(workspaceID, sessionID, delivery.ToolUse.SessionThreadID, binding, envelope.DeliveryID)
		if err := requireAgentMailInputTargetTx(ctx, tx, scopeForThread(parentScope, envelope.TargetThreadID)); err != nil {
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
		if _, err := enqueueCompletionMailWakeTx(
			ctx,
			tx,
			workspaceID,
			sessionID,
			envelope.TargetThreadID,
			envelope.DeliveryID,
			now,
		); err != nil {
			return 0, err
		}

		var taskName string
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(task_name, '')
			   FROM session_threads
			  WHERE workspace_id = $1 AND session_id = $2 AND id = $3`,
			workspaceID, sessionID, envelope.TargetThreadID).Scan(&taskName); err != nil {
			return 0, err
		}
		resultText := "task_name: " + defaultString(taskName, "subagent") + "\nsession_thread_id: " + envelope.TargetThreadID + "\nstatus: delivered"
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

func runtimePodLostSubAgentDeliveriesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string) ([]runtimePodLostSubAgentDelivery, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT tool_use.session_thread_id,
		        tool_use.event_id,
		        COALESCE(tool_use.model_request_id, ''),
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
		           AND (
		                result.payload_json::jsonb ->> 'tool_use_event_id' = tool_use.event_id
		             OR result.payload_json::jsonb ->> 'tool_use_id' = tool_use.event_id
		           )
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
			&delivery.ToolUse.PayloadJSON,
			&delivery.ToolName,
			&delivery.SentEvent,
			&delivery.Delivery,
		); err != nil {
			return nil, err
		}
		delivery.ToolUse.EventType = "agent.tool_use"
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func insertRuntimePodLostSubAgentDeliveredResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, toolUse runtimeOrphanToolUse, message string, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimeTerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_delivery_",
		Message:       message,
		Success:       true,
	}, now)
}

func insertRuntimePodLostSubAgentDeliveryErrorTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, toolUse runtimeOrphanToolUse, message string, now time.Time) (bool, error) {
	return insertRuntimeTerminalToolResultTx(ctx, tx, workspaceID, sessionID, binding, toolUse, runtimeTerminalToolResult{
		WriteIDPrefix: "rwrite_runtime_pod_lost_delivery_",
		Reason:        "runtime_pod_lost_delivery_failed",
		ErrorType:     "runtime_pod_lost_delivery_failed",
		Message:       message,
		Retryable:     false,
	}, now)
}

type runtimeTerminalToolResult struct {
	WriteIDPrefix     string
	Reason            string
	ErrorType         string
	Message           string
	Retryable         bool
	Success           bool
	ConsumptionReason string
}

func insertRuntimeTerminalToolResultTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, toolUse runtimeOrphanToolUse, terminal runtimeTerminalToolResult, now time.Time) (bool, error) {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, toolUse.SessionThreadID, binding, toolUse.ModelRequestID)
	if _, ok, err := toolResultForToolUseExistsTx(ctx, tx, workspaceID, sessionID, toolUse.SessionThreadID, toolUse.EventType, toolUse.EventID); err != nil || ok {
		return false, err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	resultEventType := "agent.tool_result"
	toolUseField := "tool_use_event_id"
	if toolUse.EventType == "agent.mcp_tool_use" {
		resultEventType = "agent.mcp_tool_result"
		toolUseField = "mcp_tool_use_id"
	}
	visibility, sessionVisible := threadScope.publicProjection(resultEventType)
	eventPayload := map[string]any{
		"type":       resultEventType,
		toolUseField: toolUse.EventID,
		"content": []map[string]string{{
			"type": "text",
			"text": terminal.Message,
		}},
		"is_error": !terminal.Success,
	}
	if terminal.Reason != "" {
		eventPayload["reason"] = terminal.Reason
	}
	payloadJSON, err := marshalBridgeJSON(eventPayload)
	if err != nil {
		return false, err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, '{}', $12, $12, $12)`,
		workspaceID,
		sessionID,
		toolUse.SessionThreadID,
		eventID,
		sequence,
		resultEventType,
		payloadJSON,
		visibility,
		sessionVisible,
		terminal.WriteIDPrefix+toolUse.EventID,
		toolUse.ModelRequestID,
		now,
	); err != nil {
		return false, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return false, err
	}
	if err := settleRuntimeTerminalToolPartTx(ctx, tx, scope, toolUse, eventID, terminal, now); err != nil {
		return false, err
	}
	consumptionReason := terminal.ConsumptionReason
	if consumptionReason == "" {
		consumptionReason = "pod_lost"
	}
	if err := consumeSandboxExecutionForTerminalWriterTx(ctx, tx, scope, toolUse.EventID, eventID, consumptionReason, now); err != nil {
		return false, err
	}
	if terminal.Success {
		if err := markPendingToolResultResolvedTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return false, err
		}
	} else {
		if err := cancelPendingToolUseForRuntimePodLostTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
			return false, err
		}
	}
	return true, nil
}

func settleRuntimeTerminalToolPartTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUse runtimeOrphanToolUse,
	resultEventID string,
	terminal runtimeTerminalToolResult,
	now time.Time,
) error {
	var messageID, dataJSON string
	if err := tx.QueryRow(ctx,
		`SELECT message_id, data_json
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUse.ModelRequestID,
	).Scan(&messageID, &dataJSON); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "durable tool message is missing")
	} else if err != nil {
		return err
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(dataJSON), &message); err != nil || message == nil {
		return status.Error(codes.FailedPrecondition, "durable tool message is invalid")
	}
	parts, _ := message["parts"].([]any)
	found := false
	timestamp := now.UTC().Format(time.RFC3339Nano)
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok || part["type"] != "tool" || part["toolUseEventId"] != toolUse.EventID {
			continue
		}
		state, _ := part["state"].(map[string]any)
		nextState := map[string]any{}
		if input, ok := state["input"]; ok {
			nextState["input"] = input
		}
		if terminal.Success {
			nextState["status"] = "completed"
			nextState["output"] = map[string]any{
				"text":      terminal.Message,
				"truncated": false,
			}
		} else {
			nextState["status"] = "error"
			nextState["error"] = map[string]any{
				"type":      terminal.ErrorType,
				"message":   terminal.Message,
				"retryable": terminal.Retryable,
			}
		}
		part["state"] = nextState
		part["updatedAt"] = timestamp
		part["completedAt"] = timestamp
		found = true
		break
	}
	if !found {
		return status.Error(codes.FailedPrecondition, "durable tool part is missing")
	}
	message["parts"] = parts
	message["status"] = "completed"
	message["updatedAt"] = timestamp
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = $5,
		        last_event_id = $6,
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND message_id = $4
		    AND model_request_id = $8`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		string(encoded),
		resultEventID,
		now,
		toolUse.ModelRequestID,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "durable tool message lost its fence")
	}
	return nil
}

func modelRequestEndExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sessionThreadID string, modelRequestID string) (string, bool, error) {
	var eventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type = 'span.model_request_end'
		  ORDER BY sequence ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
		sessionThreadID,
		modelRequestID,
	).Scan(&eventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return eventID, true, nil
}

func toolResultForToolUseExistsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, sessionThreadID string, toolUseEventType string, toolUseEventID string) (string, bool, error) {
	resultEventType := ""
	switch toolUseEventType {
	case "agent.tool_use":
		resultEventType = "agent.tool_result"
	case "agent.mcp_tool_use":
		resultEventType = "agent.mcp_tool_result"
	default:
		return "", false, status.Error(codes.FailedPrecondition, "tool use family is invalid")
	}
	var eventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND type = $4
			    AND (
			         ($4 = 'agent.tool_result' AND (
			              payload_json::jsonb ->> 'tool_use_event_id' = $5
			           OR payload_json::jsonb ->> 'tool_use_id' = $5
			         ))
			      OR ($4 = 'agent.mcp_tool_result' AND payload_json::jsonb ->> 'mcp_tool_use_id' = $5)
			    )
		  ORDER BY sequence ASC
		  LIMIT 1`,
		workspaceID,
		sessionID,
		sessionThreadID,
		resultEventType,
		toolUseEventID,
	).Scan(&eventID)
	if dbconnect.IsNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return eventID, true, nil
}

func cancelPendingToolUseForRuntimePodLostTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'cancelled',
		        result_event_id = COALESCE(result_event_id, $5),
		        resolved_at = COALESCE(resolved_at, $6),
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
		resultEventID,
		now,
	)
	return err
}

func requestKindFromModelRequestStartProjection(projectionJSON string) (string, error) {
	var projection struct {
		RequestKind                   string `json:"request_kind"`
		ContextThroughMessageSequence *int64 `json:"context_through_message_sequence"`
	}
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
		projection.RequestKind == "" || projection.ContextThroughMessageSequence == nil ||
		*projection.ContextThroughMessageSequence < 0 {
		return "", status.Error(codes.FailedPrecondition, "request start projection is malformed")
	}
	requestKind, err := normalizeRequestKind(projection.RequestKind)
	if err != nil {
		return "", status.Error(codes.FailedPrecondition, "request start projection is malformed")
	}
	return requestKind, nil
}

func runtimePodLostRepairScope(workspaceID string, sessionID string, sessionThreadID string, binding runtimeBindingForDelivery, requestIDPart string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		RequestId:       "repair_runtime_pod_lost:" + requestIDPart,
		WorkspaceId:     workspaceID,
		SessionId:       sessionID,
		SessionThreadId: sessionThreadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         binding.BindingID,
			BindingGeneration: binding.BindingGeneration,
			TargetPodUid:      binding.PodUID,
		},
	}
}
