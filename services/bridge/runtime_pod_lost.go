package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxrelease "github.com/tetral-ai/tetral/internal/sandbox/release"
	"github.com/tetral-ai/tetral/internal/workspace"
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
	ModelToolCallID string
	PayloadJSON     string
}

type runtimePodLostAffectedThreads struct {
	MainThreadID string
	ThreadIDs    []string
}

func repairLostRuntimeBindingTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, now time.Time) (int, error) {
	repaired, _, err := repairLostRuntimeBindingDetailedTx(ctx, tx, workspaceID, sessionID, binding, now)
	return repaired, err
}

func repairLostRuntimeBindingDetailedTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, now time.Time) (int, int, error) {
	handedOff, err := handOffLostRuntimeAcceptedInputsTx(ctx, tx, workspaceID, sessionID, binding, now)
	if err != nil {
		return 0, 0, err
	}
	affected, err := runtimePodLostAffectedThreadsTx(ctx, tx, workspaceID, sessionID, binding)
	if err != nil {
		return 0, 0, err
	}
	starts, err := runtimePodLostOpenRequestStartsTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, 0, err
	}
	toolUses, err := runtimePodLostOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs)
	if err != nil {
		return 0, 0, err
	}
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
	deliveryRepaired, err := settleRuntimePodLostSubAgentDeliveriesTx(ctx, tx, workspaceID, sessionID, affected.ThreadIDs, binding, now)
	if err != nil {
		return 0, 0, err
	}
	repaired += deliveryRepaired
	for _, start := range starts {
		if err := retainRuntimePodLostToolPairsTx(ctx, tx, workspaceID, sessionID, start); err != nil {
			return 0, 0, err
		}
	}
	liveScopesSettled, err := settleRuntimePodLostLiveScopesTx(ctx, tx, workspaceID, sessionID, affected, binding, now)
	if err != nil {
		return 0, 0, err
	}
	repaired += liveScopesSettled
	return repaired, handedOff, nil
}

// retainRuntimePodLostToolPairsTx preserves the committed Assistant projection
// and appends only Tool facts missing from it. Immutable Tool events authorize
// additions; existing conversation parts are never settlement authority and
// are never discarded during repair.
func retainRuntimePodLostToolPairsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	start runtimeOpenRequestStart,
) error {
	var existingDataJSON string
	if err := tx.QueryRow(ctx,
		`SELECT data_json FROM session_messages
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND model_request_id=$4 AND kind='assistant'
		  FOR UPDATE`,
		workspaceID, sessionID, start.SessionThreadID, start.ModelRequestID,
	).Scan(&existingDataJSON); dbconnect.IsNoRows(err) {
		return nil
	} else if err != nil {
		return err
	}
	existingParts, err := decodeStoredRuntimeContextParts(existingDataJSON)
	if err != nil {
		return err
	}
	retained := make([]map[string]any, 0, len(existingParts))
	seenCalls := make(map[string]struct{})
	seenResults := make(map[string]struct{})
	for _, raw := range existingParts {
		var part map[string]any
		if err := json.Unmarshal(raw, &part); err != nil {
			return status.Error(codes.FailedPrecondition, "durable context part is malformed")
		}
		retained = append(retained, part)
		modelToolCallID, _ := part["modelToolCallId"].(string)
		switch part["type"] {
		case "tool_call":
			seenCalls[modelToolCallID] = struct{}{}
		case "tool_result":
			seenResults[modelToolCallID] = struct{}{}
		}
	}
	rows, err := tx.Query(ctx,
		`SELECT type, payload_json, projection_json
		   FROM session_events
		  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		    AND model_request_id=$4
		    AND type IN ('agent.tool_use','agent.mcp_tool_use','agent.tool_result','agent.mcp_tool_result')
		  ORDER BY sequence`,
		workspaceID, sessionID, start.SessionThreadID, start.ModelRequestID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var eventType, payloadJSON, projectionJSON string
		if err := rows.Scan(&eventType, &payloadJSON, &projectionJSON); err != nil {
			return err
		}
		switch eventType {
		case "agent.tool_use", "agent.mcp_tool_use":
			call, err := runtimeToolCallPartFromDirectFacts(payloadJSON, projectionJSON)
			if err != nil {
				return err
			}
			modelToolCallID, _ := call["modelToolCallId"].(string)
			if _, exists := seenCalls[modelToolCallID]; !exists {
				retained = append(retained, call)
				seenCalls[modelToolCallID] = struct{}{}
			}
		case "agent.tool_result", "agent.mcp_tool_result":
			var payload struct {
				RepairKind string `json:"repair_kind"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return status.Error(codes.FailedPrecondition, "durable Tool Result event is malformed")
			}
			if payload.RepairKind == "invalid_tool" {
				call, err := runtimeToolCallPartFromProjection(projectionJSON)
				if err != nil {
					return err
				}
				modelToolCallID, _ := call["modelToolCallId"].(string)
				if _, exists := seenCalls[modelToolCallID]; !exists {
					retained = append(retained, call)
					seenCalls[modelToolCallID] = struct{}{}
				}
			}
			result, err := runtimeToolResultPartFromProjection(projectionJSON)
			if err != nil {
				return err
			}
			modelToolCallID, _ := result["modelToolCallId"].(string)
			if _, exists := seenResults[modelToolCallID]; !exists {
				retained = append(retained, result)
				seenResults[modelToolCallID] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	encoded, err := runtimeContextDataJSON(retained)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = $5,
		        updated_at = clock_timestamp()
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'`,
		workspaceID,
		sessionID,
		start.SessionThreadID,
		start.ModelRequestID,
		encoded,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return nil
	}
	return nil
}

func runtimeToolCallPartFromDirectFacts(payloadJSON string, projectionJSON string) (map[string]any, error) {
	var payload runtimeToolUseEventPayload
	var projection struct {
		ModelToolCallID string `json:"model_tool_call_id"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || payload.Name == "" || len(payload.Input) == 0 ||
		json.Unmarshal([]byte(projectionJSON), &projection) != nil || projection.ModelToolCallID == "" {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool Use event is malformed")
	}
	var input any
	if err := json.Unmarshal(payload.Input, &input); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool input event is malformed")
	}
	return map[string]any{
		"type": "tool_call", "modelToolCallId": projection.ModelToolCallID,
		"toolName": payload.Name, "canonicalInput": input,
	}, nil
}

func runtimeToolCallPartFromProjection(projectionJSON string) (map[string]any, error) {
	var projection runtimeToolProjectionPayload
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil ||
		projection.ModelToolCallID == "" || projection.ToolName == "" || len(projection.Input) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool projection is malformed")
	}
	var input any
	if err := json.Unmarshal(projection.Input, &input); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "durable Tool input projection is malformed")
	}
	return map[string]any{
		"type": "tool_call", "modelToolCallId": projection.ModelToolCallID,
		"toolName": projection.ToolName, "canonicalInput": input,
	}, nil
}

func runtimeToolResultPartFromProjection(projectionJSON string) (map[string]any, error) {
	var projection runtimeToolProjectionPayload
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

type runtimePodLostAcceptedInput struct {
	SessionThreadID string
	RuntimeInputID  string
	InputKind       string
	InboxStatus     string
	EventIDsJSON    string
	SequenceFrom    sql.NullInt64
	SequenceTo      sql.NullInt64
	RejectionReason sql.NullString
	QueueJobID      sql.NullString
	QueueStatus     sql.NullString
}

// handOffLostRuntimeAcceptedInputsTx is the sole transition from proven-lost
// Runtime custody back to Queue custody. It preserves the original job and its
// attempt lineage. A delivering interrupt receives its final attempt back only
// after this proven-loss fence; only already-acknowledged delivery receives a
// new job, in durable Inbox creation order.
func handOffLostRuntimeAcceptedInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	binding runtimeBindingForDelivery,
	now time.Time,
) (int, error) {
	rows, err := tx.Query(ctx,
		`SELECT inbox.session_thread_id,
		        inbox.runtime_input_id,
		        inbox.input_kind,
		        inbox.status,
		        inbox.event_ids_json,
		        inbox.sequence_from,
		        inbox.sequence_to,
		        inbox.rejection_reason_code,
		        active.id,
		        active.status
		   FROM session_runtime_inbox inbox
		   LEFT JOIN LATERAL (
		       SELECT job.id, job.status
		         FROM queue_jobs job
		        WHERE job.workspace_id = inbox.workspace_id
		          AND job.kind = 'runtime_input'
		          AND job.dedupe_key = 'runtime_input:' || inbox.workspace_id || ':' || inbox.session_id || ':' || inbox.runtime_input_id
		          AND job.status IN ('pending', 'leased')
		        ORDER BY job.created_at, job.id
		        LIMIT 1
		        FOR UPDATE OF job
		   ) active ON true
		  WHERE inbox.workspace_id = $1
		    AND inbox.session_id = $2
		    AND inbox.status IN ('delivering', 'accepted')
		    AND inbox.input_kind <> 'approval_review'
		    AND inbox.binding_id = $3
		    AND inbox.binding_generation = $4
		    AND inbox.target_pod_uid = $5
		  ORDER BY inbox.created_at, inbox.runtime_input_id
		  FOR UPDATE OF inbox`,
		workspaceID,
		sessionID,
		binding.BindingID,
		binding.BindingGeneration,
		binding.PodUID,
	)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	inputs := make([]runtimePodLostAcceptedInput, 0)
	for rows.Next() {
		var input runtimePodLostAcceptedInput
		if err := rows.Scan(
			&input.SessionThreadID,
			&input.RuntimeInputID,
			&input.InputKind,
			&input.InboxStatus,
			&input.EventIDsJSON,
			&input.SequenceFrom,
			&input.SequenceTo,
			&input.RejectionReason,
			&input.QueueJobID,
			&input.QueueStatus,
		); err != nil {
			return 0, err
		}
		inputs = append(inputs, input)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	handedOff := 0
	for _, input := range inputs {
		if input.InputKind == "" {
			return 0, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "runtime inbox input kind is missing", retryable: false}
		}
		if input.InboxStatus == "delivering" {
			if !input.QueueJobID.Valid {
				return 0, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "delivering runtime input has no active queue custody", retryable: false}
			}
			if input.InputKind != "interrupt_control" {
				continue
			}
		}
		if input.InboxStatus != "accepted" && input.InboxStatus != "delivering" {
			continue
		}
		if input.QueueJobID.Valid {
			if input.InboxStatus == "delivering" {
				if _, err := tx.Exec(ctx,
					`UPDATE queue_jobs
					    SET status = 'pending', available_at = $3,
					        attempt_count = CASE
					            WHEN max_attempts > 0 AND attempt_count >= max_attempts
					            THEN GREATEST(attempt_count - 1, 0)
					            ELSE attempt_count
					        END,
					        lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
					        updated_at = $3
					  WHERE workspace_id = $1 AND id = $2 AND status IN ('pending', 'leased')`,
					workspaceID,
					input.QueueJobID.String,
					now,
				); err != nil {
					return 0, err
				}
			} else if input.QueueStatus.String == queue.StatusLeased {
				if _, err := tx.Exec(ctx,
					`UPDATE queue_jobs
					    SET status = 'pending', available_at = $3,
					        lease_token = NULL, leased_by = NULL, leased_at = NULL, leased_until = NULL,
					        updated_at = $3
					  WHERE workspace_id = $1 AND id = $2 AND status = 'leased'`,
					workspaceID,
					input.QueueJobID.String,
					now,
				); err != nil {
					return 0, err
				}
			}
		} else {
			request, err := lostRuntimeInputEnqueueRequest(workspaceID, sessionID, input, now)
			if err != nil {
				return 0, err
			}
			if _, err := queue.EnqueueTx(ctx, tx, request); err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'queued', binding_id = NULL, binding_generation = NULL,
			        target_pod_uid = NULL, updated_at = $3
			  WHERE workspace_id = $1 AND runtime_input_id = $2
			    AND status IN ('delivering', 'accepted')
			    AND binding_id = $4 AND binding_generation = $5 AND target_pod_uid = $6`,
			workspaceID,
			input.RuntimeInputID,
			now,
			binding.BindingID,
			binding.BindingGeneration,
			binding.PodUID,
		); err != nil {
			return 0, err
		}
		handedOff++
	}
	return handedOff, nil
}

func lostRuntimeInputEnqueueRequest(
	workspaceID string,
	sessionID string,
	input runtimePodLostAcceptedInput,
	now time.Time,
) (queue.EnqueueRequest, error) {
	ws := workspace.ID(workspaceID)
	if input.InputKind == "task_notification" {
		taskID := strings.TrimPrefix(input.RuntimeInputID, "task_notification:")
		if taskID == "" || taskID == input.RuntimeInputID {
			return queue.EnqueueRequest{}, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "task notification runtime input identity is invalid", retryable: false}
		}
		return queue.NewTaskNotificationRuntimeInputEnqueueRequest(ws, sessionID, input.SessionThreadID, taskID, now)
	}
	if input.InputKind == "agent_mail" {
		deliveryID := strings.TrimPrefix(input.RuntimeInputID, "agent_mail:")
		if deliveryID == "" || deliveryID == input.RuntimeInputID {
			return queue.EnqueueRequest{}, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "agent mail runtime input identity is invalid", retryable: false}
		}
		request, _, err := agentMailWakeEnqueueRequest(workspaceID, sessionID, input.SessionThreadID, deliveryID, now)
		return request, err
	}
	payloadJSON, err := runtimeInputQueuePayloadJSON(workspaceID, sessionID, input)
	if err != nil {
		return queue.EnqueueRequest{}, err
	}
	return queue.EnqueueRequest{
		WorkspaceID:    ws,
		Kind:           queue.KindRuntimeInput,
		PartitionKey:   queue.FormatSessionPartitionKey(ws, sessionID),
		DedupeKey:      queue.FormatRuntimeInputDedupeKey(ws, sessionID, input.RuntimeInputID),
		PayloadVersion: 1,
		PayloadJSON:    payloadJSON,
		Priority:       runtimeInputPriority(input.InputKind),
		Now:            now,
	}, nil
}

type runtimeInputQueuePayload struct {
	WorkspaceID     string   `json:"workspace_id"`
	SessionID       string   `json:"session_id"`
	SessionThreadID string   `json:"session_thread_id"`
	RuntimeInputID  string   `json:"runtime_input_id"`
	EventIDs        []string `json:"event_ids"`
	SequenceFrom    int64    `json:"sequence_from"`
	SequenceTo      int64    `json:"sequence_to"`
	InputKind       string   `json:"input_kind"`
}

func runtimeInputQueuePayloadJSON(
	workspaceID string,
	sessionID string,
	input runtimePodLostAcceptedInput,
) ([]byte, error) {
	if !input.SequenceFrom.Valid || !input.SequenceTo.Valid || input.SequenceFrom.Int64 <= 0 || input.SequenceTo.Int64 < input.SequenceFrom.Int64 {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "runtime input has an invalid sequence range", retryable: false}
	}
	var eventIDs []string
	if err := json.Unmarshal([]byte(input.EventIDsJSON), &eventIDs); err != nil || len(eventIDs) == 0 {
		return nil, runtimeDeliveryPrepareError{kind: "runtime_inbox_invariant", message: "runtime input has invalid event identities", retryable: false}
	}
	return json.Marshal(runtimeInputQueuePayload{
		WorkspaceID:     workspaceID,
		SessionID:       sessionID,
		SessionThreadID: input.SessionThreadID,
		RuntimeInputID:  input.RuntimeInputID,
		EventIDs:        eventIDs,
		SequenceFrom:    input.SequenceFrom.Int64,
		SequenceTo:      input.SequenceTo.Int64,
		InputKind:       input.InputKind,
	})
}

func runtimeInputPriority(inputKind string) int {
	if inputKind == "interrupt_control" {
		return 100
	}
	return 0
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
		                     (tool_use.type = 'agent.tool_use'
		                      AND result.type = 'agent.tool_result'
		                      AND (result.payload_json::jsonb ->> 'tool_use_event_id' = tool_use.event_id
		                           OR result.payload_json::jsonb ->> 'tool_use_id' = tool_use.event_id))
		                  OR (tool_use.type = 'agent.mcp_tool_use'
		                      AND result.type = 'agent.mcp_tool_result'
		                      AND result.payload_json::jsonb ->> 'mcp_tool_use_id' = tool_use.event_id)
		                )
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
	scope := runtimePodLostRepairScope(workspaceID, sessionID, threadID, binding)
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

type runtimePodLossMutationStatus string

const (
	runtimePodLossMutationRepaired runtimePodLossMutationStatus = "repaired"
	runtimePodLossMutationStale    runtimePodLossMutationStatus = "stale"
)

type runtimePodLossMutationResult struct {
	status      runtimePodLossMutationStatus
	staleReason string
}

func (s *PostgreSQLRuntimeDeliveryStore) repairLostRuntimeBinding(ctx context.Context, workspaceID string, sessionID string, binding runtimeBindingForDelivery, now time.Time) error {
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
	binding runtimeBindingForDelivery,
	now time.Time,
	requireActive bool,
) (runtimePodLossMutationResult, error) {
	result := runtimePodLossMutationResult{status: runtimePodLossMutationRepaired}
	handedOff := 0
	err := s.Client.WithWorkspaceTx(ctx, workspaceID, "agentruntimebridge.repair_lost_runtime_binding", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, workspaceID, sessionID); err != nil {
			if code, ok := closeoutSentinelCode(err); requireActive && ok && code == closeoutScopeSupersededCode {
				result = runtimePodLossMutationResult{status: runtimePodLossMutationStale, staleReason: "inactive"}
				return nil
			}
			return err
		}
		current, found, err := readOptionalRuntimeBindingForDeliveryTx(ctx, tx, workspaceID, sessionID)
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
		// The census retains only the fence identity. Once that fence wins, the
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
		if !rowsAffected(dbResult) {
			return runtimePodLostStaleFenceError("runtime pod-loss binding finalization fence is stale")
		}
		return nil
	})
	if err != nil {
		return runtimePodLossMutationResult{}, err
	}
	logRuntimeInputCustodyTransition(s.Logger, &bridgev1.RuntimeScope{
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
	binding runtimeBindingForDelivery,
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
	return runtimeTerminalOrphanToolUsesTx(ctx, tx, workspaceID, sessionID, affectedThreadIDs, false)
}

func runtimeTerminalOrphanToolUsesTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, affectedThreadIDs []string, includeSubAgentDeliveries bool) ([]runtimeOrphanToolUse, error) {
	threadIDsJSON, err := json.Marshal(affectedThreadIDs)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx,
		`SELECT e.session_thread_id, e.event_id, e.type, e.model_request_id,
		        COALESCE(e.projection_json::jsonb ->> 'model_tool_call_id', ''), e.payload_json
		   FROM session_events e
		   JOIN session_threads thread_scope
		     ON thread_scope.workspace_id = e.workspace_id
		    AND thread_scope.session_id = e.session_id
		    AND thread_scope.id = e.session_thread_id
		  WHERE e.workspace_id = $1
		    AND e.session_id = $2
		    AND e.session_thread_id IN (SELECT jsonb_array_elements_text($3::jsonb))
		    AND e.type IN ('agent.tool_use', 'agent.mcp_tool_use')
		    AND (
		      e.visibility = 'public'
		      OR (e.visibility = 'internal' AND thread_scope.role = 'approval_reviewer')
		    )
			    AND ($4 OR (
			      e.type <> 'agent.tool_use'
			      OR COALESCE(e.payload_json::jsonb ->> 'name', '') NOT IN ('spawn_agent', 'send_message')
			    ))
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
		includeSubAgentDeliveries,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	toolUses := make([]runtimeOrphanToolUse, 0)
	for rows.Next() {
		var toolUse runtimeOrphanToolUse
		if err := rows.Scan(
			&toolUse.SessionThreadID,
			&toolUse.EventID,
			&toolUse.EventType,
			&toolUse.ModelRequestID,
			&toolUse.ModelToolCallID,
			&toolUse.PayloadJSON,
		); err != nil {
			return nil, err
		}
		if toolUse.ModelToolCallID == "" {
			return nil, status.Error(codes.FailedPrecondition, "durable Tool Use identity is missing")
		}
		toolUses = append(toolUses, toolUse)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return toolUses, nil
}

func insertRuntimePodLostRequestEndTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, binding runtimeBindingForDelivery, start runtimeOpenRequestStart, now time.Time) (bool, error) {
	scope := runtimePodLostRepairScope(workspaceID, sessionID, start.SessionThreadID, binding)
	return insertRuntimeTerminalRequestEndTx(ctx, tx, scope, start, "runtime_pod_lost", "rwrite_runtime_pod_lost_", now)
}

func insertRuntimeTerminalRequestEndTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, start runtimeOpenRequestStart, errorKind string, writeIDPrefix string, now time.Time) (bool, error) {
	if _, ok, err := modelRequestEndExistsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), start.SessionThreadID, start.ModelRequestID); err != nil || ok {
		return false, err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return false, err
	}
	visibility, sessionVisible := threadScope.publicProjection("span.model_request_end")
	request := &bridgev1.WriteRequestEndRequest{
		Scope:          scope,
		RuntimeWriteId: writeIDPrefix + start.ModelRequestID,
		ModelRequestId: start.ModelRequestID,
		IsError:        true,
		ErrorKind:      errorKind,
		FinishReason:   "error",
		UsageJson:      "{}",
	}
	payloadJSON, err := modelRequestEndPayloadJSON(request, start.EventID, start.RequestKind, "error", bridgeUsage{})
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
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
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
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
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

// Pod-loss delivery repair may declare success only from the same Inbox fact
// that owns Runtime delivery. Source events and the stored mail envelope prove
// content identity, but never substitute for current or committed custody.
func runtimePodLostAgentMailInboxCurrentTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	workspaceID string,
	sessionID string,
	envelope storedAgentMailEnvelope,
	binding runtimeBindingForDelivery,
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
		completionRuntimeInputID(envelope.DeliveryID),
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
	receivedEventID := stableRuntimeID("agent_mail_received_event", workspaceID, sessionID, envelope.TargetThreadID, envelope.DeliveryID)
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
		        COALESCE(tool_use.projection_json::jsonb ->> 'model_tool_call_id', ''),
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
	scope := runtimePodLostRepairScope(workspaceID, sessionID, toolUse.SessionThreadID, binding)
	return insertRuntimeTerminalToolResultForScopeTx(ctx, tx, scope, toolUse, terminal, now)
}

func insertRuntimeTerminalToolResultForScopeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUse runtimeOrphanToolUse, terminal runtimeTerminalToolResult, now time.Time) (bool, error) {
	if _, ok, err := toolResultForToolUseExistsTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), toolUse.SessionThreadID, toolUse.EventType, toolUse.EventID); err != nil || ok {
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
	projection, err := settleRuntimeTerminalToolPartTx(ctx, tx, scope, toolUse, terminal, now)
	if err != nil {
		return false, err
	}
	projectionJSON, err := marshalBridgeJSON(projection)
	if err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, model_request_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $13)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		toolUse.SessionThreadID,
		eventID,
		sequence,
		resultEventType,
		payloadJSON,
		visibility,
		sessionVisible,
		terminal.WriteIDPrefix+toolUse.EventID,
		toolUse.ModelRequestID,
		projectionJSON,
		now,
	); err != nil {
		return false, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
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
		if err := cancelPendingToolUseForTerminalResultTx(ctx, tx, scope, toolUse.EventID, eventID, now); err != nil {
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
	terminal runtimeTerminalToolResult,
	now time.Time,
) (runtimeToolProjectionPayload, error) {
	tool, err := loadDurableToolExecutionTx(ctx, tx, scope, toolUse.EventID, toolUse.EventType, false)
	if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	if tool.ModelRequestID != toolUse.ModelRequestID || tool.ModelToolCallID != toolUse.ModelToolCallID {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "durable Tool repair identity is inconsistent")
	}
	resultValue := map[string]any{"type": "completed", "output": map[string]any{"text": terminal.Message}}
	if !terminal.Success {
		resultValue = map[string]any{"type": "error", "error": map[string]any{
			"type": terminal.ErrorType, "message": terminal.Message, "retryable": terminal.Retryable,
		}}
	}
	resultPartsJSON, err := json.Marshal([]map[string]any{{
		"type": "tool_result", "modelToolCallId": toolUse.ModelToolCallID, "result": resultValue,
	}})
	if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_messages
		    SET data_json = jsonb_set(
		          data_json::jsonb,
		          '{parts}',
		          (data_json::jsonb -> 'parts') || $5::jsonb
		        )::text,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'
		    AND jsonb_typeof(data_json::jsonb -> 'parts') = 'array'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUse.ModelRequestID,
		string(resultPartsJSON),
		now,
	)
	if err != nil {
		return runtimeToolProjectionPayload{}, err
	}
	if !rowsAffected(result) {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "durable tool message lost its fence")
	}
	return runtimeToolProjectionFromDurableTool(tool, resultValue), nil
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

func cancelPendingToolUseForTerminalResultTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
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

func runtimePodLostRepairScope(workspaceID string, sessionID string, sessionThreadID string, binding runtimeBindingForDelivery) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
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
