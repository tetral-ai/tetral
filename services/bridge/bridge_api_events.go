package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge events protocol-family boundary.

const (
	// maxWebSearchRequestsPerResult and maxWebFetchRequestsPerResult clamp the
	// per-web-tool-result server_tool_use counters. Their values are derived
	// from web-connector's own item and domain caps: a single call fans out at
	// most WEB_INPUT_MAX_ITEMS (8) x WEB_DOMAINS_MAX (4) = 32 search backend
	// calls, and at most 8 open/find items x one fetch each = 8 reader calls. A
	// counter reported above its clamp is rejected before any write.
	maxWebSearchRequestsPerResult           = 32
	maxWebFetchRequestsPerResult            = 8
	runtimeCompactionProjectionTextMaxBytes = 64 * 1024
)

func (s *PostgreSQLBridgeAPIStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (*bridgev1.WriteEventResponse, error) {
	if request.GetRuntimeWriteId() == "" || request.GetEventType() == "" || request.GetPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid write event request")
	}
	if !writeEventTypeAllowed(request.GetEventType()) {
		return nil, status.Error(codes.InvalidArgument, "event type is not writable through WriteEvent")
	}
	if request.GetEventType() == "span.model_request_start" && request.GetModelRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "model request id is required")
	}
	stableReasoning, err := normalizeStableReasoningParts(request)
	if err != nil {
		return nil, err
	}
	serverToolUse, err := normalizeServerToolUseUsage(request)
	if err != nil {
		return nil, err
	}
	if len(stableReasoning.Parts) > 0 {
		if request.GetEventType() != "agent.tool_use" && request.GetEventType() != "agent.mcp_tool_use" {
			return nil, status.Error(codes.InvalidArgument, "stable reasoning requires a public tool-use event")
		}
		if request.GetModelRequestId() == "" {
			return nil, status.Error(codes.InvalidArgument, "stable reasoning requires a model request id")
		}
		if !stableReasoning.StrictlyOrdered {
			return nil, status.Error(codes.InvalidArgument, "stable reasoning parts must be strictly ordered")
		}
	}
	if !json.Valid([]byte(request.GetPayloadJson())) {
		return nil, status.Error(codes.InvalidArgument, "event payload must be JSON")
	}
	payloadJSON := stripInternalProviderFields(request.GetPayloadJson())
	projectionJSON := defaultString(request.GetProjectionJson(), "{}")
	if !json.Valid([]byte(projectionJSON)) {
		return nil, status.Error(codes.InvalidArgument, "event projection must be JSON")
	}
	if request.GetEventType() != "agent.thread_context_compacted" {
		projectionJSON = stripInternalProviderFields(projectionJSON)
	}
	key := request.GetRuntimeWriteId()
	requestHash := bridgeRequestHash(bridgeOpWriteEvent, key, request.GetModelRequestId(), request.GetEventType(), payloadJSON, projectionJSON, boolHashPart(request.GetSessionVisible()), stableReasoning.CanonicalJSON, serverToolUse.CanonicalJSON)
	now := s.now()
	var response *bridgev1.WriteEventResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.write_event", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpWriteEvent, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "runtime write id conflicts with existing event")
			}
			var existingResult writeEventResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &existingResult); err != nil {
				return err
			}
			response = &bridgev1.WriteEventResponse{
				Ack:      duplicateAck("", key),
				EventId:  existingResult.EventID,
				Sequence: existingResult.Sequence,
			}
			return nil
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpWriteEvent, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "runtime write id conflicts with existing event")
			}
			var existingResult writeEventResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &existingResult); err != nil {
				return err
			}
			response = &bridgev1.WriteEventResponse{
				Ack:      duplicateAck("", key),
				EventId:  existingResult.EventID,
				Sequence: existingResult.Sequence,
			}
			return nil
		}
		eventType := request.GetEventType()
		eventPayloadJSON := payloadJSON
		if eventType == "agent.thread_message_sent" {
			eventPayloadJSON, err = publicSentInterAgentEventPayloadTx(ctx, tx, request.GetScope(), eventPayloadJSON)
			if err != nil {
				return err
			}
		}
		if threadScope.role != "main" && request.GetEventType() == "session.status_running" {
			eventType = "session.thread_status_running"
			eventPayloadJSON, err = threadStatusPayloadJSON(eventType, request.GetScope(), threadScope, "")
			if err != nil {
				return err
			}
			if err := updateChildThreadStatusTx(ctx, tx, request.GetScope(), "running", now); err != nil {
				return err
			}
		}
		if serverToolUse.Present {
			if err := verifyWebToolResultUsageTx(ctx, tx, request.GetScope(), eventType, eventPayloadJSON, projectionJSON); err != nil {
				return err
			}
		}
		visibility, sessionVisible := threadScope.publicProjection(eventType)
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id, stable_reasoning_json,
				projection_json, created_at, updated_at, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), $12, $13, $14, $14, $14)`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			eventID,
			sequence,
			eventType,
			eventPayloadJSON,
			visibility,
			sessionVisible,
			key,
			request.GetModelRequestId(),
			stableReasoning.ledgerJSON(),
			projectionJSON,
			now,
		); err != nil {
			return err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
			return err
		}
		if err := projectRuntimeEventTx(ctx, tx, request.GetScope(), runtimeEventProjection{
			EventID:        eventID,
			ModelRequestID: request.GetModelRequestId(),
			EventType:      eventType,
			PayloadJSON:    eventPayloadJSON,
			ProjectionJSON: projectionJSON,
		}, now); err != nil {
			return err
		}
		// This is the SECOND writer of sessions.usage: the web server_tool_use
		// counters settle here, inside the durable web agent.tool_result WriteEvent
		// transaction, because tool usage exists only after the provider stream
		// ends. WriteRequestEnd (bridge_api_settlement.go) is therefore NOT the
		// sole sessions.usage writer. Counter meaning (as produced upstream):
		// web_search_requests counts each fan-out search backend call (failed
		// excluded); web_fetch_requests counts reader backend calls including lazy
		// upgrades (failed excluded). The Gateway never writes sessions.usage.
		if serverToolUse.Present {
			if err := incrementSessionServerToolUsageTx(ctx, tx, request.GetScope(), serverToolUse, now); err != nil {
				return err
			}
		}
		for _, reasoningPart := range stableReasoning.Parts {
			partJSON, err := stableReasoningPartJSON(request.GetScope(), reasoningPart, now)
			if err != nil {
				return err
			}
			if _, err := mergeAssistantMessagePartTx(ctx, tx, request.GetScope(), request.GetModelRequestId(), "", partJSON, true, now); err != nil {
				return err
			}
		}
		if threadScope.role == "main" && eventType == "session.status_running" {
			if err := markPublicSessionRunningTx(ctx, tx, request.GetScope(), eventID, now); err != nil {
				return err
			}
		}
		resultJSON, err := marshalBridgeJSON(writeEventResult{EventID: eventID, Sequence: sequence})
		if err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpWriteEvent,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			RuntimeWriteID: sql.NullString{String: key, Valid: true},
			ResultJSON:     resultJSON,
			Now:            now,
		}); err != nil {
			return err
		}
		response = &bridgev1.WriteEventResponse{
			Ack:      committedAck("", key),
			EventId:  eventID,
			Sequence: sequence,
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

type normalizedServerToolUseUsage struct {
	Present           bool
	WebSearchRequests int64
	WebFetchRequests  int64
	CanonicalJSON     string
}

func normalizeServerToolUseUsage(request *bridgev1.WriteEventRequest) (normalizedServerToolUseUsage, error) {
	usage := request.GetServerToolUse()
	if usage == nil {
		return normalizedServerToolUseUsage{CanonicalJSON: "null"}, nil
	}
	if request.GetEventType() != "agent.tool_result" {
		return normalizedServerToolUseUsage{}, status.Error(codes.InvalidArgument, "server tool usage requires a tool-result event")
	}
	if usage.GetWebSearchRequests() < 0 || usage.GetWebSearchRequests() > maxWebSearchRequestsPerResult ||
		usage.GetWebFetchRequests() < 0 || usage.GetWebFetchRequests() > maxWebFetchRequestsPerResult {
		return normalizedServerToolUseUsage{}, status.Error(codes.InvalidArgument, "server tool usage counters are out of range")
	}
	canonical, err := json.Marshal(map[string]int64{
		"web_fetch_requests":  usage.GetWebFetchRequests(),
		"web_search_requests": usage.GetWebSearchRequests(),
	})
	if err != nil {
		return normalizedServerToolUseUsage{}, err
	}
	return normalizedServerToolUseUsage{
		Present:           true,
		WebSearchRequests: usage.GetWebSearchRequests(),
		WebFetchRequests:  usage.GetWebFetchRequests(),
		CanonicalJSON:     string(canonical),
	}, nil
}

func verifyWebToolResultUsageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	payloadJSON string,
	projectionJSON string,
) error {
	if eventType != "agent.tool_result" {
		return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
	}
	var result runtimeToolResultEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &result); err != nil {
		return status.Error(codes.InvalidArgument, "server tool usage result payload is invalid")
	}
	toolUseEventID := defaultString(result.ToolUseEventID, result.ToolUseID)
	if toolUseEventID == "" || result.MCPToolUseID != "" {
		return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
	}
	projection, err := parseRuntimeToolProjection(projectionJSON)
	if err != nil || projection.ToolName != "web" || projection.MCPServerName != "" {
		return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
	}
	var toolUsePayloadJSON string
	if err := tx.QueryRow(ctx,
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4
		    AND type = 'agent.tool_use'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&toolUsePayloadJSON); dbconnect.IsNoRows(err) {
		return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
	} else if err != nil {
		return err
	}
	var toolUse runtimeToolUseEventPayload
	if err := json.Unmarshal([]byte(toolUsePayloadJSON), &toolUse); err != nil || toolUse.Name != "web" || toolUse.MCPServerName != "" {
		return status.Error(codes.InvalidArgument, "server tool usage requires a web tool result")
	}
	return nil
}

func incrementSessionServerToolUsageTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	usage normalizedServerToolUseUsage,
	now time.Time,
) error {
	var current string
	if err := tx.QueryRow(ctx,
		`SELECT usage_json
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(),
	).Scan(&current); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	} else if err != nil {
		return err
	}
	next, err := incrementSessionServerToolUsageJSON(current, usage)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET usage_json = $3,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND id = $2`,
		scope.GetWorkspaceId(), scope.GetSessionId(), next, now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	}
	return nil
}

func writeEventTypeAllowed(eventType string) bool {
	switch eventType {
	case "agent.message",
		"agent.thinking",
		"agent.tool_use",
		"agent.tool_result",
		"agent.mcp_tool_use",
		"agent.mcp_tool_result",
		"agent.thread_context_compacted",
		"agent.thread_message_sent",
		"approval_review.decision",
		"approval_review.failure",
		"session.status_running",
		"session.error",
		"span.model_request_start":
		return true
	default:
		return false
	}
}

type threadMutationScope struct {
	visibility string
	role       string
	status     string
	taskName   sql.NullString
}

func (s threadMutationScope) publicProjection(eventType string) (string, bool) {
	if s.visibility != "public" || s.role == "approval_reviewer" {
		return "internal", false
	}
	if s.role == "main" {
		return "public", true
	}
	switch eventType {
	case "agent.thread_message_sent",
		"agent.thread_message_received",
		"session.thread_created",
		"session.thread_status_running",
		"session.thread_status_idle",
		"session.thread_status_rescheduled",
		"session.thread_status_terminated":
		return "public", true
	default:
		return "public", false
	}
}

func lockThreadMutationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (threadMutationScope, error) {
	row := tx.QueryRow(ctx,
		`SELECT visibility, role, status, task_name
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	)
	var result threadMutationScope
	if err := row.Scan(&result.visibility, &result.role, &result.status, &result.taskName); dbconnect.IsNoRows(err) {
		return threadMutationScope{}, closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "runtime thread is stale"))
	} else if err != nil {
		return threadMutationScope{}, err
	}
	return result, nil
}

func sessionThreadCallableTaskNameTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadID string,
) (sql.NullString, error) {
	var role string
	var taskName sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT role, task_name
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		threadID,
	).Scan(&role, &taskName)
	if dbconnect.IsNoRows(err) {
		return sql.NullString{}, status.Error(codes.FailedPrecondition, "inter-agent message endpoint thread is stale")
	}
	if err != nil {
		return sql.NullString{}, err
	}
	if role == "main" {
		return sql.NullString{}, nil
	}
	if !taskName.Valid || taskName.String == "" {
		return sql.NullString{}, status.Error(codes.FailedPrecondition, "inter-agent message endpoint has no callable task name")
	}
	return taskName, nil
}

func verifyModelRequestStartTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, startEventID string, modelRequestID string) error {
	row := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4
		    AND type = 'span.model_request_start'
		    AND model_request_id = $5
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		startEventID,
		modelRequestID,
	)
	var eventID string
	if err := row.Scan(&eventID); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "model request start span is missing")
	} else if err != nil {
		return err
	}
	return nil
}

func incrementSessionUsageTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, usage bridgeUsage, now time.Time) error {
	var current string
	if err := tx.QueryRow(ctx,
		`SELECT usage_json
		   FROM sessions
		  WHERE workspace_id = $1
		    AND id = $2
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
	).Scan(&current); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	} else if err != nil {
		return err
	}
	next, err := incrementSessionUsageJSON(current, usage)
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET usage_json = $3,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		next,
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	}
	return nil
}

func markPublicSessionRunningTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, statusEventID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET status = CASE WHEN status = 'rescheduling' THEN 'running' ELSE status END,
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND id = $2
		    AND status <> 'terminated'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	}
	result, err = tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = 'running',
		        last_active_at = $4,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND status NOT IN ('terminated', 'archived')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime thread is stale")
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_runtime_status (
			workspace_id, session_id, status, status_event_id, idle_since, running_since, active_seconds_total, cleanup_after,
			cleanup_enqueued_at, cleanup_claimed_at, cleanup_job_id,
			binding_id, binding_generation, created_at, updated_at
		) VALUES ($1, $2, 'running', $3, NULL, $6, 0, NULL, NULL, NULL, NULL, $4, $5, $6, $6)
		ON CONFLICT (workspace_id, session_id) DO UPDATE SET
			status = 'running',
			status_event_id = EXCLUDED.status_event_id,
			idle_since = NULL,
			running_since = CASE
				WHEN session_runtime_status.status = 'running' THEN COALESCE(session_runtime_status.running_since, EXCLUDED.running_since)
				ELSE EXCLUDED.running_since
			END,
			cleanup_after = NULL,
			cleanup_enqueued_at = NULL,
			cleanup_claimed_at = NULL,
			cleanup_job_id = NULL,
			binding_id = EXCLUDED.binding_id,
			binding_generation = EXCLUDED.binding_generation,
			updated_at = EXCLUDED.updated_at`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		statusEventID,
		scope.GetBinding().GetBindingId(),
		scope.GetBinding().GetBindingGeneration(),
		now,
	)
	return err
}

func markPublicSessionIdleTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, idleSince time.Time, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET status = CASE WHEN status = 'rescheduling' THEN 'idle' ELSE status END,
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND id = $2
		    AND status <> 'terminated'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	}
	result, err = tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = 'idle',
		        last_active_at = $4,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND status NOT IN ('terminated', 'archived')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		idleSince,
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime thread is stale")
	}
	return nil
}

func markPublicSessionReschedulingTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE sessions
		    SET status = 'rescheduling',
		        updated_at = $3
		  WHERE workspace_id = $1
		    AND id = $2`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime session is stale")
	}
	result, err = tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = 'rescheduling',
		        last_active_at = $4,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime thread is stale")
	}
	return nil
}

func nextSessionEventSequenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (int64, error) {
	var sequence int64
	err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id IS NOT DISTINCT FROM $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence)
	return sequence, err
}

func appendSessionEventStreamChangeTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, visibility string, sessionVisible bool, now time.Time) (int64, error) {
	return appendSessionEventStreamChangeForRevisionTx(ctx, tx, scope, eventID, 1, visibility, sessionVisible, now)
}

func appendSessionEventStreamChangeForRevisionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, revision int64, visibility string, sessionVisible bool, now time.Time) (int64, error) {
	var streamPosition int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO session_event_stream_changes (
			workspace_id, session_id, event_id, session_thread_id, revision, visibility, session_visible, changed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING stream_position`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		eventID,
		scope.GetSessionThreadId(),
		revision,
		visibility,
		sessionVisible,
		now,
	).Scan(&streamPosition); err != nil {
		return 0, err
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_events
		    SET latest_stream_position = $4,
		        insert_stream_position = CASE
		            WHEN $5 = 1 AND insert_stream_position = 0 THEN $4
		            ELSE insert_stream_position
		        END
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		eventID,
		streamPosition,
		revision,
	)
	return streamPosition, err
}

type runtimeEventProjection struct {
	EventID        string
	ModelRequestID string
	EventType      string
	PayloadJSON    string
	ProjectionJSON string
}

func projectRuntimeEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, event runtimeEventProjection, now time.Time) error {
	switch event.EventType {
	case "agent.message":
		if strings.TrimSpace(event.ProjectionJSON) != "" && strings.TrimSpace(event.ProjectionJSON) != "{}" {
			return projectAssistantTextEventTx(ctx, tx, scope, event, now)
		}
		return insertSessionMessageProjectionTx(ctx, tx, scope, event.EventID, "assistant", event.PayloadJSON, now)
	case "agent.tool_use":
		return projectToolUseEventTx(ctx, tx, scope, event, now)
	case "agent.mcp_tool_use":
		return projectToolUseEventTx(ctx, tx, scope, event, now)
	case "agent.tool_result":
		return projectToolResultEventTx(ctx, tx, scope, event, now)
	case "agent.mcp_tool_result":
		return projectToolResultEventTx(ctx, tx, scope, event, now)
	case "runtime_notification":
		return insertSessionMessageProjectionTx(ctx, tx, scope, event.EventID, "runtime_notification", firstNonEmptyJSON(event.ProjectionJSON, event.PayloadJSON), now)
	case "agent.thread_context_compacted":
		return insertRuntimeCompactionMessageProjectionTx(ctx, tx, scope, event.EventID, event.ProjectionJSON, now)
	default:
		return nil
	}
}

func mergeAssistantMessagePartTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	suggestedMessageID string,
	partJSON json.RawMessage,
	rejectExisting bool,
	now time.Time,
) (string, error) {
	messageID := ""
	var messageData string
	var sequence int64
	var err error
	if modelRequestID != "" {
		err = tx.QueryRow(ctx,
			`SELECT message_id, data_json, sequence
			   FROM session_messages
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND model_request_id = $4
			  FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
		).Scan(&messageID, &messageData, &sequence)
	} else if suggestedMessageID != "" {
		messageID = suggestedMessageID
		err = tx.QueryRow(ctx,
			`SELECT data_json, sequence
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND message_id = $4
		  FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), messageID,
		).Scan(&messageData, &sequence)
	} else {
		err = sql.ErrNoRows
	}
	if dbconnect.IsNoRows(err) {
		if modelRequestID != "" || messageID == "" {
			messageID = id.New("msg_")
		}
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1
			   FROM session_messages
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&sequence); err != nil {
			return "", err
		}
		messageData = "{}"
	} else if err != nil {
		return "", err
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(messageData), &message); err != nil {
		return "", status.Error(codes.FailedPrecondition, "assistant message projection is invalid")
	}
	if existingID, _ := message["id"].(string); existingID != "" && existingID != messageID {
		return "", status.Error(codes.AlreadyExists, "assistant message identity conflict")
	}
	var part map[string]any
	if err := json.Unmarshal(partJSON, &part); err != nil {
		return "", status.Error(codes.InvalidArgument, "assistant message part is invalid")
	}
	partID, _ := part["id"].(string)
	if partID == "" {
		return "", status.Error(codes.InvalidArgument, "assistant message part identity is missing")
	}
	part["messageId"] = messageID
	candidateSequence, sequenceOK := part["sequence"].(float64)
	if !sequenceOK || candidateSequence < 0 {
		return "", status.Error(codes.InvalidArgument, "assistant message part sequence is invalid")
	}
	parts, _ := message["parts"].([]any)
	replaced := false
	for index, existing := range parts {
		existingPart, ok := existing.(map[string]any)
		if !ok {
			continue
		}
		existingID, _ := existingPart["id"].(string)
		existingSequence, _ := existingPart["sequence"].(float64)
		if existingID != partID && existingSequence == candidateSequence {
			return "", status.Error(codes.AlreadyExists, "assistant message part sequence conflict")
		}
		if existingID != partID {
			continue
		}
		if rejectExisting {
			if !sameStableReasoningContent(existingPart, part) {
				return "", status.Error(codes.AlreadyExists, "assistant message part identity conflict")
			}
			replaced = true
			break
		}
		existingJSON, _ := json.Marshal(existingPart)
		candidateJSON, _ := json.Marshal(part)
		if !bytes.Equal(existingJSON, candidateJSON) {
			return "", status.Error(codes.AlreadyExists, "assistant message part identity conflict")
		}
		parts[index] = part
		replaced = true
		break
	}
	if !replaced {
		parts = append(parts, part)
	}
	if rejectExisting {
		if err := validateStableReasoningBudget(parts); err != nil {
			return "", err
		}
	}
	sort.SliceStable(parts, func(i, j int) bool {
		left, _ := parts[i].(map[string]any)
		right, _ := parts[j].(map[string]any)
		leftSequence, _ := left["sequence"].(float64)
		rightSequence, _ := right["sequence"].(float64)
		if leftSequence == rightSequence {
			leftID, _ := left["id"].(string)
			rightID, _ := right["id"].(string)
			return leftID < rightID
		}
		return leftSequence < rightSequence
	})
	timestamp := now
	if len(message) == 0 {
		message = map[string]any{
			"id":        messageID,
			"sessionId": scope.GetSessionId(),
			"role":      "assistant",
			"origin":    "agent",
			"sequence":  sequence - 1,
			"createdAt": timestamp,
		}
	}
	message["status"] = "completed"
	message["updatedAt"] = timestamp
	message["parts"] = parts
	encoded, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, model_request_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, NULLIF($7, ''), $8, $8)
		ON CONFLICT (workspace_id, message_id)
		DO UPDATE SET
			data_json = EXCLUDED.data_json,
			model_request_id = COALESCE(session_messages.model_request_id, EXCLUDED.model_request_id),
			updated_at = EXCLUDED.updated_at
		WHERE session_messages.kind = 'assistant'
		  AND (session_messages.model_request_id IS NULL OR session_messages.model_request_id = EXCLUDED.model_request_id)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), messageID, sequence, string(encoded), modelRequestID, timestamp,
	)
	return messageID, err
}

type runtimeToolUseEventPayload struct {
	Name                string          `json:"name"`
	Input               json.RawMessage `json:"input"`
	MCPServerName       string          `json:"mcp_server_name"`
	EvaluatedPermission string          `json:"evaluated_permission"`
}

type runtimeToolResultEventPayload struct {
	ToolUseID      string `json:"tool_use_id"`
	ToolUseEventID string `json:"tool_use_event_id"`
	MCPToolUseID   string `json:"mcp_tool_use_id"`
	Content        []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"is_error"`
}

type runtimeToolProjectionPayload struct {
	Type            string          `json:"type"`
	MessageID       string          `json:"message_id"`
	PartID          string          `json:"part_id"`
	PartSequence    int             `json:"part_sequence"`
	ModelToolCallID string          `json:"model_tool_call_id"`
	ToolName        string          `json:"tool_name"`
	MCPServerName   string          `json:"mcp_server_name"`
	Input           json.RawMessage `json:"input"`
	State           string          `json:"state"`
	Output          *struct {
		Text      string `json:"text"`
		Truncated bool   `json:"truncated"`
	} `json:"output"`
	Error *struct {
		Type      string `json:"type"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

type runtimeTextProjectionPayload struct {
	Type         string `json:"type"`
	MessageID    string `json:"message_id"`
	PartID       string `json:"part_id"`
	PartSequence int    `json:"part_sequence"`
	Truncated    bool   `json:"truncated"`
}

func projectAssistantTextEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, event runtimeEventProjection, now time.Time) error {
	var projection runtimeTextProjectionPayload
	if err := json.Unmarshal([]byte(event.ProjectionJSON), &projection); err != nil || projection.Type != "runtime_text_projection" || projection.MessageID == "" || projection.PartID == "" || projection.PartSequence < 0 {
		return status.Error(codes.FailedPrecondition, "assistant text projection is invalid")
	}
	var payload struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil || len(payload.Content) != 1 || payload.Content[0].Type != "text" || payload.Content[0].Text == "" {
		return status.Error(codes.FailedPrecondition, "assistant message event payload is not projectable")
	}
	timestamp := now
	part, err := json.Marshal(map[string]any{
		"id":          projection.PartID,
		"sessionId":   scope.GetSessionId(),
		"messageId":   projection.MessageID,
		"sequence":    projection.PartSequence,
		"createdAt":   timestamp,
		"updatedAt":   timestamp,
		"type":        "text",
		"text":        payload.Content[0].Text,
		"truncated":   projection.Truncated,
		"status":      "completed",
		"completedAt": timestamp,
	})
	if err != nil {
		return err
	}
	_, err = mergeAssistantMessagePartTx(ctx, tx, scope, event.ModelRequestID, projection.MessageID, part, false, now)
	return err
}

func projectToolUseEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, event runtimeEventProjection, now time.Time) error {
	var payload runtimeToolUseEventPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return status.Error(codes.FailedPrecondition, "tool use event payload is not projectable")
	}
	switch payload.EvaluatedPermission {
	case "ask":
		projection, err := parseRuntimeToolProjection(event.ProjectionJSON)
		if err != nil {
			return err
		}
		if event.EventType == "agent.mcp_tool_use" && (payload.MCPServerName == "" || projection.MCPServerName != payload.MCPServerName) {
			return status.Error(codes.FailedPrecondition, "MCP tool use projection server is invalid")
		}
		if event.EventType == "agent.tool_use" && projection.MCPServerName != "" {
			return status.Error(codes.FailedPrecondition, "tool use projection server is invalid")
		}
		inputJSON, err := runtimeToolProjectionInputJSON(projection.Input, payload.Input)
		if err != nil {
			return err
		}
		return upsertPendingToolApprovalTx(ctx, tx, scope, event.EventID, projection, inputJSON, now)
	case "allow", "deny":
		return nil
	default:
		return status.Error(codes.FailedPrecondition, "tool use permission is not projectable")
	}
}

func projectToolResultEventTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, event runtimeEventProjection, now time.Time) error {
	var payload runtimeToolResultEventPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		return status.Error(codes.FailedPrecondition, "tool result event payload is not projectable")
	}
	toolUseEventID := defaultString(payload.MCPToolUseID, defaultString(payload.ToolUseEventID, payload.ToolUseID))
	if toolUseEventID == "" {
		return status.Error(codes.FailedPrecondition, "tool result event is missing tool use id")
	}
	projection, err := parseRuntimeToolProjection(event.ProjectionJSON)
	if err != nil {
		return err
	}
	if projection.State != "completed" && projection.State != "error" && projection.State != "cancelled" {
		return status.Error(codes.FailedPrecondition, "tool result projection state is not terminal")
	}
	if event.EventType == "agent.mcp_tool_result" && projection.MCPServerName == "" {
		return status.Error(codes.FailedPrecondition, "MCP tool result projection server is invalid")
	}
	if event.EventType == "agent.tool_result" && projection.MCPServerName != "" {
		return status.Error(codes.FailedPrecondition, "tool result projection server is invalid")
	}
	inputJSON, err := runtimeToolProjectionInputJSON(projection.Input, nil)
	if err != nil {
		return err
	}
	if err := insertToolResultMessageProjectionTx(ctx, tx, scope, event.ModelRequestID, event.EventID, toolUseEventID, payload, projection, inputJSON, now); err != nil {
		return err
	}
	return markPendingToolResultResolvedTx(ctx, tx, scope, toolUseEventID, event.EventID, now)
}

func parseRuntimeToolProjection(projectionJSON string) (runtimeToolProjectionPayload, error) {
	if strings.TrimSpace(projectionJSON) == "" || strings.TrimSpace(projectionJSON) == "{}" {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool projection is required")
	}
	var projection runtimeToolProjectionPayload
	if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool projection is not JSON")
	}
	if projection.Type != "" && projection.Type != "runtime_tool_projection" {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool projection type is not supported")
	}
	if projection.ModelToolCallID == "" || projection.ToolName == "" {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool projection is incomplete")
	}
	switch projection.State {
	case "pending", "running", "completed", "error", "cancelled":
	default:
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool projection state is not supported")
	}
	return projection, nil
}

func runtimeToolProjectionInputJSON(primary json.RawMessage, fallback json.RawMessage) (string, error) {
	raw := primary
	if len(raw) == 0 {
		raw = fallback
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if !json.Valid(raw) {
		return "", status.Error(codes.FailedPrecondition, "tool projection input is not JSON")
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", status.Error(codes.FailedPrecondition, "tool projection input is not JSON")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func upsertPendingToolApprovalTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, projection runtimeToolProjectionPayload, inputJSON string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, kind, input_json, status, expires_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'approval', $7, 'pending', $8, $9, $9)
		ON CONFLICT (workspace_id, session_id, session_thread_id, tool_use_event_id)
		DO UPDATE SET
			model_tool_call_id = EXCLUDED.model_tool_call_id,
			tool_name = EXCLUDED.tool_name,
			input_json = EXCLUDED.input_json,
			expires_at = EXCLUDED.expires_at,
			updated_at = EXCLUDED.updated_at
		WHERE session_pending_tool_uses.kind = 'approval'
		  AND session_pending_tool_uses.status IN ('pending', 'resolving')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		projection.ModelToolCallID,
		projection.ToolName,
		inputJSON,
		now.Add(defaultPendingToolApprovalTTL),
		now,
	)
	return err
}

func insertToolResultMessageProjectionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, modelRequestID string, resultEventID string, toolUseEventID string, payload runtimeToolResultEventPayload, projection runtimeToolProjectionPayload, inputJSON string, now time.Time) error {
	var existing string
	err := tx.QueryRow(ctx,
		`SELECT message_id
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND source_event_id = $2
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		resultEventID,
	).Scan(&existing)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	if projection.MessageID != "" {
		if projection.PartID == "" || projection.PartSequence < 0 {
			return status.Error(codes.FailedPrecondition, "tool result message projection identity is incomplete")
		}
		part, err := toolResultRuntimePartJSON(scope, projection.MessageID, projection.PartID, projection.PartSequence, toolUseEventID, payload, projection, inputJSON, now)
		if err != nil {
			return err
		}
		_, err = mergeAssistantMessagePartTx(ctx, tx, scope, modelRequestID, projection.MessageID, part, false, now)
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence); err != nil {
		return err
	}
	messageID := id.New("msg_")
	timestamp := now
	dataJSON, err := toolResultRuntimeMessageJSON(scope, messageID, sequence, toolUseEventID, payload, projection, inputJSON, timestamp.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', $6, $7, $7, $8, $8)
		ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)
		WHERE source_event_id IS NOT NULL
		DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		sequence,
		dataJSON,
		resultEventID,
		timestamp,
	)
	return err
}

func toolResultRuntimePartJSON(scope *bridgev1.RuntimeScope, messageID string, partID string, partSequence int, toolUseEventID string, payload runtimeToolResultEventPayload, projection runtimeToolProjectionPayload, inputJSON string, now time.Time) (json.RawMessage, error) {
	state, err := toolResultRuntimeState(payload, projection, inputJSON)
	if err != nil {
		return nil, err
	}
	timestamp := now
	return json.Marshal(map[string]any{
		"id":             partID,
		"sessionId":      scope.GetSessionId(),
		"messageId":      messageID,
		"sequence":       partSequence,
		"createdAt":      timestamp,
		"updatedAt":      timestamp,
		"type":           "tool",
		"toolCallId":     projection.ModelToolCallID,
		"toolName":       projection.ToolName,
		"toolUseEventId": toolUseEventID,
		"toolEvent":      runtimeToolEventProjection(projection),
		"state":          state,
		"completedAt":    timestamp,
	})
}

func markPendingToolResultResolvedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'resolved',
		        result_event_id = COALESCE(result_event_id, $5),
		        resolved_at = COALESCE(resolved_at, $6),
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND kind = 'approval'
		    AND status = 'resolving'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		resultEventID,
		now,
	)
	if err != nil {
		return err
	}
	return nil
}

func toolResultRuntimeMessageJSON(scope *bridgev1.RuntimeScope, messageID string, sequence int64, toolUseEventID string, payload runtimeToolResultEventPayload, projection runtimeToolProjectionPayload, inputJSON string, timestamp string) (string, error) {
	state, err := toolResultRuntimeState(payload, projection, inputJSON)
	if err != nil {
		return "", err
	}
	return marshalBridgeJSON(map[string]any{
		"id":        messageID,
		"sessionId": scope.GetSessionId(),
		"role":      "assistant",
		"origin":    "agent",
		"sequence":  sequence - 1,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts": []map[string]any{
			{
				"id":             messageID + "_tool",
				"sessionId":      scope.GetSessionId(),
				"messageId":      messageID,
				"sequence":       0,
				"createdAt":      timestamp,
				"updatedAt":      timestamp,
				"type":           "tool",
				"toolCallId":     projection.ModelToolCallID,
				"toolName":       projection.ToolName,
				"toolUseEventId": toolUseEventID,
				"toolEvent":      runtimeToolEventProjection(projection),
				"state":          state,
				"completedAt":    timestamp,
			},
		},
	})
}

func runtimeToolEventProjection(projection runtimeToolProjectionPayload) map[string]any {
	if projection.MCPServerName != "" {
		return map[string]any{"kind": "mcp", "mcpServerName": projection.MCPServerName}
	}
	return map[string]any{"kind": "tool"}
}

func toolResultRuntimeState(payload runtimeToolResultEventPayload, projection runtimeToolProjectionPayload, inputJSON string) (map[string]any, error) {
	input := cleanupRuntimeBoundedJSON(inputJSON)
	state := projection.State
	switch state {
	case "completed":
		return map[string]any{
			"status": "completed",
			"input":  input,
			"output": map[string]any{
				"text":      toolResultText(payload),
				"truncated": false,
			},
		}, nil
	case "error":
		return map[string]any{
			"status": "error",
			"input":  input,
			"error":  runtimeToolProjectionError(payload, projection),
		}, nil
	case "cancelled":
		return map[string]any{
			"status": "cancelled",
			"input":  input,
			"error":  runtimeToolProjectionError(payload, projection),
		}, nil
	default:
		return nil, status.Error(codes.FailedPrecondition, "tool result state is not terminal")
	}
}

func runtimeToolProjectionError(payload runtimeToolResultEventPayload, projection runtimeToolProjectionPayload) map[string]any {
	if projection.Error != nil {
		errorType := defaultString(projection.Error.Type, "tool_error")
		message := defaultString(projection.Error.Message, toolResultText(payload))
		return map[string]any{
			"type":      errorType,
			"message":   message,
			"retryable": projection.Error.Retryable,
		}
	}
	return map[string]any{
		"type":      "tool_error",
		"message":   defaultString(toolResultText(payload), "tool execution failed"),
		"retryable": false,
	}
}

func toolResultText(payload runtimeToolResultEventPayload) string {
	if len(payload.Content) == 0 {
		return ""
	}
	parts := make([]string, 0, len(payload.Content))
	for _, item := range payload.Content {
		if item.Type == "text" && item.Text != "" {
			parts = append(parts, item.Text)
		}
	}
	return strings.Join(parts, "\n")
}

type runtimeCompactionProjection struct {
	ID        string `json:"id"`
	SessionID string `json:"sessionId"`
	Role      string `json:"role"`
	Origin    string `json:"origin"`
	Sequence  int64  `json:"sequence"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
	UpdatedAt string `json:"updatedAt"`
	Parts     []struct {
		ID          string `json:"id"`
		SessionID   string `json:"sessionId"`
		MessageID   string `json:"messageId"`
		Sequence    int64  `json:"sequence"`
		CreatedAt   string `json:"createdAt"`
		UpdatedAt   string `json:"updatedAt"`
		Type        string `json:"type"`
		Text        string `json:"text"`
		Status      string `json:"status"`
		CompletedAt string `json:"completedAt"`
	} `json:"parts"`
}

func validateRuntimeCompactionProjection(scope *bridgev1.RuntimeScope, raw string) (runtimeCompactionProjection, error) {
	invalid := func() (runtimeCompactionProjection, error) {
		return runtimeCompactionProjection{}, status.Error(codes.FailedPrecondition, "compaction projection is invalid")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
		return invalid()
	}
	var projection runtimeCompactionProjection
	if err := json.Unmarshal([]byte(raw), &projection); err != nil {
		return invalid()
	}
	if projection.ID == "" || projection.SessionID != scope.GetSessionId() || projection.Role != "user" ||
		projection.Origin != "runtime" || projection.Status != "completed" || projection.Sequence < 0 || len(projection.Parts) != 1 {
		return invalid()
	}
	if _, err := time.Parse(time.RFC3339Nano, projection.CreatedAt); err != nil {
		return invalid()
	}
	if _, err := time.Parse(time.RFC3339Nano, projection.UpdatedAt); err != nil {
		return invalid()
	}
	part := projection.Parts[0]
	if part.ID == "" || part.SessionID != scope.GetSessionId() || part.MessageID != projection.ID ||
		part.Sequence < 0 || part.Type != "text" || part.Text == "" ||
		len(part.Text) > runtimeCompactionProjectionTextMaxBytes || part.Status != "completed" {
		return invalid()
	}
	for _, timestamp := range []string{part.CreatedAt, part.UpdatedAt, part.CompletedAt} {
		if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
			return invalid()
		}
	}
	for _, marker := range []string{"<conversation-checkpoint>", "<summary>", "<recent-context>"} {
		if !strings.Contains(part.Text, marker) {
			return invalid()
		}
	}
	return projection, nil
}

// Compaction is the sole Runtime-canonical message projection. Bridge owns only
// the durable thread-local row sequence and persists the validated body verbatim.
// insertRuntimeCompactionMessageProjectionTx persists a Runtime-canonical row:
// the compaction checkpoint RuntimeMessage is constructed by Runtime (id,
// part id, timestamps, whole body) and stored VERBATIM after shape validation.
// Bridge does not re-mint the id/part id or restamp timestamps, so every field
// except the Bridge-assigned sequence is identical to what Runtime holds
// post-ACK. This verbatim ownership is scoped to compaction-boundary rows only;
// an ordinary assistant message_id stays Bridge-minted (the two id spaces never
// collide).
func insertRuntimeCompactionMessageProjectionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, dataJSON string, now time.Time) error {
	projection, err := validateRuntimeCompactionProjection(scope, dataJSON)
	if err != nil {
		return err
	}
	var existing string
	err = tx.QueryRow(ctx,
		`SELECT message_id
		   FROM session_messages
		  WHERE workspace_id = $1 AND source_event_id = $2
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		eventID,
	).Scan(&existing)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'compaction', $6, $7, $7, $8, $8)
		ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)
		WHERE source_event_id IS NOT NULL
		DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		projection.ID,
		sequence,
		dataJSON,
		eventID,
		now,
	)
	return err
}

func insertSessionMessageProjectionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string, kind string, dataJSON string, now time.Time) error {
	dataJSON = stripInternalProviderFields(dataJSON)
	storedKind := kind
	if kind == "accepted_user" {
		storedKind = "user"
	}
	var existing string
	err := tx.QueryRow(ctx,
		`SELECT message_id
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND source_event_id = $2
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		eventID,
	).Scan(&existing)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	var sequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&sequence); err != nil {
		return err
	}
	if !json.Valid([]byte(dataJSON)) {
		dataJSON = "{}"
	}
	messageID := id.New("msg_")
	switch kind {
	case "accepted_user":
		var err error
		dataJSON, err = userMessageDataJSON(scope, messageID, sequence, dataJSON, now)
		if err != nil {
			return err
		}
	case "assistant":
		var err error
		dataJSON, err = agentMessageDataJSON(scope, messageID, sequence, dataJSON, now)
		if err != nil {
			return err
		}
	case "runtime_notification":
		var err error
		dataJSON, err = runtimeNotificationMessageDataJSON(scope, messageID, sequence, dataJSON, now)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $9)
		ON CONFLICT (workspace_id, session_id, session_thread_id, source_event_id)
		WHERE source_event_id IS NOT NULL
		DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		sequence,
		storedKind,
		dataJSON,
		eventID,
		now,
	)
	return err
}

func userMessageDataJSON(scope *bridgev1.RuntimeScope, messageID string, sequence int64, payloadJSON string, now time.Time) (string, error) {
	var payload struct {
		Content []struct {
			Type   string `json:"type"`
			Text   string `json:"text"`
			Source *struct {
				Type   string `json:"type"`
				FileID string `json:"file_id"`
			} `json:"source"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
	}
	if len(payload.Content) == 0 {
		return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
	}
	timestamp := now
	parts := make([]map[string]any, 0, len(payload.Content))
	for _, item := range payload.Content {
		switch item.Type {
		case "text":
			if item.Text == "" || item.Source != nil {
				return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
			}
		case "image", "document":
			if item.Text != "" || item.Source == nil || item.Source.Type != "file" || item.Source.FileID == "" {
				return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
			}
			continue
		default:
			return "", status.Error(codes.FailedPrecondition, "user message event payload is not projectable")
		}
		partID := fmt.Sprintf("%s_text_%d", messageID, len(parts))
		parts = append(parts, map[string]any{
			"id":          partID,
			"sessionId":   scope.GetSessionId(),
			"messageId":   messageID,
			"sequence":    len(parts),
			"createdAt":   timestamp,
			"updatedAt":   timestamp,
			"type":        "text",
			"text":        item.Text,
			"truncated":   false,
			"status":      "completed",
			"completedAt": timestamp,
		})
	}
	return marshalBridgeDataJSON(map[string]any{
		"id":        messageID,
		"sessionId": scope.GetSessionId(),
		"role":      "user",
		"origin":    "user",
		"sequence":  sequence - 1,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts":     parts,
	})
}

func agentMessageDataJSON(scope *bridgev1.RuntimeScope, messageID string, sequence int64, payloadJSON string, now time.Time) (string, error) {
	var payload struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", err
	}
	timestamp := now
	parts := make([]map[string]any, 0, len(payload.Content))
	for _, item := range payload.Content {
		if item.Type != "text" || item.Text == "" {
			continue
		}
		partID := fmt.Sprintf("%s_text_%d", messageID, len(parts))
		parts = append(parts, map[string]any{
			"id":          partID,
			"sessionId":   scope.GetSessionId(),
			"messageId":   messageID,
			"sequence":    len(parts),
			"createdAt":   timestamp,
			"updatedAt":   timestamp,
			"type":        "text",
			"text":        item.Text,
			"truncated":   false,
			"status":      "completed",
			"completedAt": timestamp,
		})
	}
	return marshalBridgeJSON(map[string]any{
		"id":        messageID,
		"sessionId": scope.GetSessionId(),
		"role":      "assistant",
		"origin":    "agent",
		"sequence":  sequence - 1,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts":     parts,
	})
}

func runtimeNotificationMessageDataJSON(scope *bridgev1.RuntimeScope, messageID string, sequence int64, notificationJSON string, now time.Time) (string, error) {
	var notification struct {
		TaskID               string          `json:"task_id"`
		SourceToolUseEventID string          `json:"source_tool_use_event_id"`
		Status               string          `json:"status"`
		Result               json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(notificationJSON), &notification); err != nil {
		return "", err
	}
	text := string(notification.Result)
	if text == "" || !json.Valid([]byte(text)) {
		fallback, err := marshalBridgeJSON(map[string]any{
			"task_id":                  notification.TaskID,
			"source_tool_use_event_id": notification.SourceToolUseEventID,
			"status":                   notification.Status,
		})
		if err != nil {
			return "", err
		}
		text = fallback
	}
	timestamp := now
	return marshalBridgeJSON(map[string]any{
		"id":        messageID,
		"sessionId": scope.GetSessionId(),
		"role":      "user",
		"origin":    "runtime",
		"sequence":  sequence - 1,
		"status":    "completed",
		"createdAt": timestamp,
		"updatedAt": timestamp,
		"parts": []map[string]any{
			{
				"id":          messageID + "_text",
				"sessionId":   scope.GetSessionId(),
				"messageId":   messageID,
				"sequence":    0,
				"createdAt":   timestamp,
				"updatedAt":   timestamp,
				"type":        "text",
				"text":        text,
				"truncated":   false,
				"status":      "completed",
				"completedAt": timestamp,
			},
		},
	})
}
