package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log/slog"
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
	maxWebSearchRequestsPerResult = 32
	maxWebFetchRequestsPerResult  = 8
)

func (s *PostgreSQLBridgeAPIStore) WriteEvent(ctx context.Context, request *bridgev1.WriteEventRequest) (response *bridgev1.WriteEventResponse, resultErr error) {
	evidence := runtimeDeclarationRejectionEvidence{
		Kind:        "identity",
		Operation:   bridgeOpWriteEvent,
		OperationID: request.GetRuntimeWriteId(),
	}
	if delta := request.GetAssistantContextDelta(); delta != nil && len(delta.GetParts()) > 0 && delta.GetParts()[0] != nil {
		evidence.MessageOrPart = runtimeContextPartKind(delta.GetParts()[0])
	}
	defer func() { logRuntimeDeclarationRejected(s.Logger, request.GetScope(), evidence, resultErr) }()
	if request.GetRuntimeWriteId() == "" || request.GetEventType() == "" || request.GetPayloadJson() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid write event request")
	}
	if !writeEventTypeAllowed(request.GetEventType()) {
		return nil, status.Error(codes.InvalidArgument, "event type is not writable through WriteEvent")
	}
	if request.GetEventType() == "span.model_request_start" && request.GetModelRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "model request id is required")
	}
	requestStart, err := normalizeRequestStartFact(request)
	if err != nil {
		return nil, err
	}
	consumedFileAttachments, err := normalizeConsumedFileAttachments(request.GetConsumedFileAttachments())
	if err != nil {
		return nil, err
	}
	if requestStart == nil && len(consumedFileAttachments.Pairs) > 0 {
		return nil, status.Error(codes.InvalidArgument, "file attachment consumption requires a model request start")
	}
	if !json.Valid([]byte(request.GetPayloadJson())) {
		evidence.Kind = "schema"
		return nil, status.Error(codes.InvalidArgument, "event payload must be JSON")
	}
	payloadJSON := stripInternalProviderFields(request.GetPayloadJson())
	switch request.GetEventType() {
	case "agent.message", "agent.tool_use", "agent.mcp_tool_use":
		if request.GetAssistantContextDelta() == nil {
			return nil, status.Error(codes.InvalidArgument, "Assistant member event requires one part append")
		}
	default:
		if request.GetAssistantContextDelta() != nil {
			return nil, status.Errorf(codes.InvalidArgument, "event type %q does not accept a Runtime declaration", request.GetEventType())
		}
	}
	evidence.Kind = "canonicality"
	declarationDigest, err := writeEventDeclarationDigest(
		request,
		payloadJSON,
		consumedFileAttachments.CanonicalJSON,
	)
	if err != nil {
		return nil, err
	}
	key := request.GetRuntimeWriteId()
	now := s.now()
	var (
		facts     writeEventDurableFacts
		duplicate bool
	)
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.write_event", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(
			ctx,
			tx,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
		); err != nil {
			return err
		}
		evidence.Kind = "authorization"
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		evidence.Kind = "transaction"
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpWriteEvent,
			bridgeOpWriteEvent,
			key,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "stored write event result is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "write event idempotency conflict")
			}
			if err := json.Unmarshal([]byte(existing.ReceiptJSON), &facts); err != nil || !validWriteEventDurableFacts(facts) {
				return status.Error(codes.FailedPrecondition, "stored write event result is invalid")
			}
			duplicate = true
			return nil
		}
		if requestStart != nil {
			if err := requireSessionInputDeliveryAllowedTx(ctx, tx, request.GetScope()); err != nil {
				return err
			}
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		evidence.ThreadRole = threadScope.role
		if request.GetAssistantContextDelta() != nil {
			if err := verifyModelRequestAcceptsMembersTx(ctx, tx, request.GetScope(), request.GetModelRequestId()); err != nil {
				return err
			}
		}
		if requestStart != nil {
			if err := verifyRequestStartUniqueTx(ctx, tx, request.GetScope(), request.GetModelRequestId()); err != nil {
				return err
			}
			if err := verifyRequestStartMessageBoundaryTx(ctx, tx, request.GetScope(), requestStart.ContextThroughMessageSequence); err != nil {
				return err
			}
			if err := validateFileAttachmentConsumptionsTx(ctx, tx, request.GetScope(), consumedFileAttachments.Pairs); err != nil {
				return err
			}
		}
		eventType := writeEventDurableEventType(request.GetEventType(), threadScope)
		eventPayloadJSON := payloadJSON
		if eventType != request.GetEventType() {
			eventPayloadJSON, err = threadStatusPayloadJSON(eventType, request.GetScope(), threadScope, "")
			if err != nil {
				return err
			}
			if err := updateChildThreadStatusTx(ctx, tx, request.GetScope(), "running", now); err != nil {
				return err
			}
		}
		toolProjection, err := runtimeToolProjectionFromContextDelta(
			eventType,
			request.GetAssistantContextDelta(),
			request.GetCanonicalExecutionInputJson(),
		)
		if err != nil {
			return err
		}
		visibility, sessionVisible := threadScope.publicProjection(eventType)
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		projectionJSON := `{}`
		if requestStart != nil {
			projectionJSON, err = marshalBridgeJSON(map[string]any{
				"context_through_message_sequence": requestStart.ContextThroughMessageSequence,
				"request_kind":                     requestStart.RequestKind,
			})
			if err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				visibility, session_visible, runtime_write_id, model_request_id,
				projection_json, created_at, updated_at, processed_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''), $12, $13, $13, $13)`,
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
			projectionJSON,
			now,
		); err != nil {
			return err
		}
		if requestStart != nil && len(consumedFileAttachments.Pairs) > 0 {
			if err := insertFileAttachmentConsumptionsTx(ctx, tx, request.GetScope(), eventID, consumedFileAttachments.Pairs); err != nil {
				return err
			}
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
			return err
		}
		facts, err = commitWriteEventContextTx(
			ctx,
			tx,
			request.GetScope(),
			eventType,
			eventID,
			request.GetModelRequestId(),
			request.GetAssistantContextDelta(),
			now,
		)
		if err != nil {
			return err
		}
		if eventType == "agent.tool_use" || eventType == "agent.mcp_tool_use" {
			if err := verifyModelToolCallIDUniqueTx(
				ctx,
				tx,
				request.GetScope(),
				toolProjection.ModelToolCallID,
			); err != nil {
				return err
			}
			projectionJSON, err := marshalBridgeJSON(map[string]any{
				"canonical_execution_input": toolProjection.CanonicalExecutionInput,
				"model_tool_call_id":        toolProjection.ModelToolCallID,
				"provider_input":            toolProjection.ProviderInput,
				"tool_name":                 toolProjection.ToolName,
			})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`UPDATE session_events
				    SET projection_json = $5
				  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND event_id = $4`,
				request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(),
				request.GetScope().GetSessionThreadId(), eventID, projectionJSON,
			); err != nil {
				return err
			}
		}
		if err := applyToolEventBookkeepingTx(
			ctx,
			tx,
			request.GetScope(),
			eventID,
			eventType,
			eventPayloadJSON,
			toolProjection,
			now,
		); err != nil {
			return err
		}
		if threadScope.role == "main" && eventType == "session.status_running" {
			if err := markPublicSessionRunningTx(ctx, tx, request.GetScope(), eventID, now); err != nil {
				return err
			}
		}
		resultJSON, err := marshalBridgeJSON(facts)
		if err != nil {
			return err
		}
		return insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpWriteEvent,
			bridgeOpWriteEvent,
			key,
			declarationDigest,
			resultJSON,
			now,
		)
	}); err != nil {
		if isSessionInterruptBarrierStaleError(err) {
			return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Stale{Stale: &bridgev1.WriteEventStale{}}}, nil
		}
		return nil, err
	}
	if !validWriteEventDurableFacts(facts) {
		return nil, status.Error(codes.FailedPrecondition, "write event result is invalid")
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpWriteEvent,
		request.GetEventType(),
		key,
		declarationDigest,
		duplicate,
		observation,
	)
	if !observation.Current {
		return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Stale{Stale: &bridgev1.WriteEventStale{}}}, nil
	}
	if duplicate {
		return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Duplicate{Duplicate: &bridgev1.WriteEventDuplicate{
			EventId: facts.EventID, AssignedMessageSequence: facts.MessageSequence,
			CreatedToolUseEventIds: facts.CreatedToolUseEventIDs,
		}}}, nil
	}
	return &bridgev1.WriteEventResponse{Outcome: &bridgev1.WriteEventResponse_Committed{Committed: &bridgev1.WriteEventCommitted{
		EventId: facts.EventID, AssignedMessageSequence: facts.MessageSequence,
		CreatedToolUseEventIds: facts.CreatedToolUseEventIDs,
	}}}, nil
}

func validWriteEventDurableFacts(facts writeEventDurableFacts) bool {
	return facts.EventID != "" && facts.CreatedToolUseEventIDs != nil
}

func verifyRequestStartUniqueTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
) error {
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND session_thread_id = $3
			   AND model_request_id = $4
			   AND type = 'span.model_request_start'
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		modelRequestID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return status.Error(codes.AlreadyExists, "model request already has a durable start")
	}
	var anotherOpen bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events started
			 WHERE started.workspace_id = $1
			   AND started.session_id = $2
			   AND started.session_thread_id = $3
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
		)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&anotherOpen); err != nil {
		return err
	}
	if anotherOpen {
		return status.Error(codes.FailedPrecondition, "another model request is already open")
	}
	return nil
}

func verifyModelRequestAcceptsMembersTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
) error {
	if modelRequestID == "" {
		return status.Error(codes.InvalidArgument, "model request id is required")
	}
	var starts, ends int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FILTER (WHERE type = 'span.model_request_start'),
		        count(*) FILTER (WHERE type = 'span.model_request_end')
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND type IN ('span.model_request_start', 'span.model_request_end')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		modelRequestID,
	).Scan(&starts, &ends); err != nil {
		return err
	}
	if starts != 1 {
		return status.Error(codes.FailedPrecondition, "model request start is not durable")
	}
	if ends != 0 {
		return status.Error(codes.FailedPrecondition, "model request is already sealed")
	}
	return nil
}

func verifyModelToolCallIDUniqueTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelToolCallID string,
) error {
	if modelToolCallID == "" {
		return status.Error(codes.FailedPrecondition, "model tool call id is missing")
	}
	var occurrences int
	if err := tx.QueryRow(ctx,
		`SELECT count(*)
		   FROM session_messages AS message
		  CROSS JOIN LATERAL jsonb_array_elements(
		    CASE
		      WHEN jsonb_typeof(message.data_json::jsonb -> 'parts') = 'array'
		      THEN message.data_json::jsonb -> 'parts'
		      ELSE '[]'::jsonb
		    END
		  ) AS part
		  WHERE message.workspace_id = $1
		    AND message.session_id = $2
		    AND message.session_thread_id = $3
		    AND part ->> 'type' = 'tool_call'
		    AND part ->> 'modelToolCallId' = $4`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		modelToolCallID,
	).Scan(&occurrences); err != nil {
		return err
	}
	if occurrences == 0 {
		return status.Error(codes.FailedPrecondition, "model tool call projection is missing")
	}
	if occurrences != 1 {
		return status.Error(codes.AlreadyExists, "model tool call id already has a durable declaration")
	}
	return nil
}

func verifyModelToolCallIDAvailableTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelToolCallID string,
) error {
	if modelToolCallID == "" {
		return status.Error(codes.FailedPrecondition, "model tool call id is missing")
	}
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_messages AS message
			 CROSS JOIN LATERAL jsonb_array_elements(
			   CASE
			     WHEN jsonb_typeof(message.data_json::jsonb -> 'parts') = 'array'
			     THEN message.data_json::jsonb -> 'parts'
			     ELSE '[]'::jsonb
			   END
			 ) AS part
			 WHERE message.workspace_id = $1
			   AND message.session_id = $2
			   AND message.session_thread_id = $3
			   AND part ->> 'type' = 'tool_call'
			   AND part ->> 'modelToolCallId' = $4
		)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelToolCallID,
	).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return status.Error(codes.AlreadyExists, "model tool call id already has a durable declaration")
	}
	return nil
}

func writeEventDurableEventType(eventType string, threadScope threadMutationScope) string {
	if eventType == "session.status_running" && threadScope.role != "main" {
		return "session.thread_status_running"
	}
	return eventType
}

type stagedMCPResultIdentity struct {
	ToolUseEventID string
	MCPServerName  string
	ToolName       string
}

func consumeStagedMCPResultTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	payloadJSON string,
	now time.Time,
) (stagedMCPResultIdentity, error) {
	if eventType != "agent.mcp_tool_result" {
		return stagedMCPResultIdentity{}, status.Error(codes.InvalidArgument, "stored MCP result requires an mcp tool result")
	}
	var resultPayload durableToolResultEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &resultPayload); err != nil || resultPayload.MCPToolUseID == "" {
		return stagedMCPResultIdentity{}, status.Error(codes.InvalidArgument, "mcp tool result target is invalid")
	}
	toolUseEventID := resultPayload.MCPToolUseID
	stored, ok, err := readRuntimeToolResultTx(ctx, tx, scope, toolUseEventID)
	if err != nil {
		return stagedMCPResultIdentity{}, err
	}
	if !ok {
		return stagedMCPResultIdentity{}, nil
	}
	if stored.ToolKind != bridgeToolKindMCP || stored.MCPClaimStatus.String != mcpClaimStatusStored {
		return stagedMCPResultIdentity{}, status.Error(codes.FailedPrecondition, "mcp tool result is not staged")
	}
	mcpServerName, toolName, ok := strings.Cut(stored.ToolName, "/")
	if !ok || mcpServerName == "" || toolName == "" {
		return stagedMCPResultIdentity{}, status.Error(codes.Internal, "stored mcp tool identity is invalid")
	}
	attachmentRefs, err := stagedMCPResultAttachmentRefs(stored.ResultJSON)
	if err != nil {
		return stagedMCPResultIdentity{}, err
	}
	for _, attachmentRef := range attachmentRefs {
		update, err := tx.Exec(ctx,
			`UPDATE session_transient_attachments
			    SET status = 'active',
			        updated_at = $6
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND source_tool_use_event_id = $4
			    AND attachment_ref = $5
			    AND status = 'staged'`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			toolUseEventID,
			attachmentRef,
			now,
		)
		if err != nil {
			return stagedMCPResultIdentity{}, err
		}
		if !rowsAffected(update) {
			return stagedMCPResultIdentity{}, status.Error(codes.FailedPrecondition, "mcp result attachment is not staged")
		}
	}
	update, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET mcp_claim_status = 'consumed',
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND tool_kind = 'mcp'
		    AND mcp_claim_status = 'stored'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		now,
	)
	if err != nil {
		return stagedMCPResultIdentity{}, err
	}
	if !rowsAffected(update) {
		return stagedMCPResultIdentity{}, status.Error(codes.FailedPrecondition, "staged mcp result consume failed")
	}
	return stagedMCPResultIdentity{
		ToolUseEventID: toolUseEventID,
		MCPServerName:  mcpServerName,
		ToolName:       toolName,
	}, nil
}

func stagedMCPResultAttachmentRefs(resultJSON string) ([]string, error) {
	var stored struct {
		Response struct {
			Attachments []struct {
				AttachmentRef string `json:"attachment_ref"`
			} `json:"attachments"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &stored); err != nil {
		return nil, status.Error(codes.Internal, "stored mcp tool result is invalid")
	}
	refs := make([]string, 0, len(stored.Response.Attachments))
	seen := make(map[string]struct{}, len(stored.Response.Attachments))
	for _, attachment := range stored.Response.Attachments {
		if attachment.AttachmentRef == "" {
			return nil, status.Error(codes.Internal, "stored mcp attachment reference is invalid")
		}
		if _, duplicate := seen[attachment.AttachmentRef]; duplicate {
			return nil, status.Error(codes.Internal, "stored mcp attachment reference is duplicated")
		}
		seen[attachment.AttachmentRef] = struct{}{}
		refs = append(refs, attachment.AttachmentRef)
	}
	return refs, nil
}

func logStagedMCPResultConsumed(
	logger *slog.Logger,
	scope *bridgev1.RuntimeScope,
	identity stagedMCPResultIdentity,
) {
	if logger == nil || scope == nil {
		return
	}
	defer func() { _ = recover() }()
	logger.Info("bridge.mcp_result_consumed",
		slog.String("operation", "mcp_result_consume"),
		slog.String("event.kind", "mcp_result_consumed"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("thread.id", scope.GetSessionThreadId()),
		slog.String("mcp.tool_use_event_id", identity.ToolUseEventID),
		slog.String("mcp.server.name", identity.MCPServerName),
		slog.String("mcp.tool.name", identity.ToolName),
		slog.String("outcome", "committed"),
	)
}

type normalizedServerToolUseUsage struct {
	Present           bool
	WebSearchRequests int64
	WebFetchRequests  int64
	CanonicalJSON     string
}

func normalizeServerToolUseUsage(usage *bridgev1.ServerToolUseUsage) (normalizedServerToolUseUsage, error) {
	if usage == nil {
		return normalizedServerToolUseUsage{CanonicalJSON: "null"}, nil
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
		"agent.mcp_tool_use",
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

type requestStartFact struct {
	RequestKind                   string
	ContextThroughMessageSequence int64
}

func normalizeRequestStartFact(request *bridgev1.WriteEventRequest) (*requestStartFact, error) {
	if request.GetEventType() != "span.model_request_start" {
		if request.ContextThroughMessageSequence != nil || request.GetRequestKind() != "" {
			return nil, status.Error(codes.InvalidArgument, "request-start metadata requires a model request start")
		}
		return nil, nil
	}
	if request.ContextThroughMessageSequence == nil || request.GetContextThroughMessageSequence() < 0 || request.GetRequestKind() == "" {
		return nil, status.Error(codes.InvalidArgument, "model request start metadata is required")
	}
	requestKind, err := normalizeRequestKind(request.GetRequestKind())
	if err != nil {
		return nil, err
	}
	return &requestStartFact{
		RequestKind:                   requestKind,
		ContextThroughMessageSequence: request.GetContextThroughMessageSequence(),
	}, nil
}

func verifyRequestStartMessageBoundaryTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	boundary int64,
) error {
	var current int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0)
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&current); err != nil {
		return err
	}
	if boundary != current {
		return status.Error(codes.InvalidArgument, "model request start message boundary is not current")
	}
	return nil
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
	if err := requireSessionMutationAllowedTx(ctx, tx, scope); err != nil {
		return threadMutationScope{}, err
	}
	return lockThreadMutationRowTx(ctx, tx, scope)
}

func lockThreadMutationRowTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (threadMutationScope, error) {
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

func verifyModelRequestStartTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, startEventID string, modelRequestID string, requestKind string) error {
	row := tx.QueryRow(ctx,
		`SELECT event_id, projection_json::jsonb ->> 'request_kind'
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
	var eventID, durableRequestKind string
	if err := row.Scan(&eventID, &durableRequestKind); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "model request start span is missing")
	} else if err != nil {
		return err
	}
	if durableRequestKind != requestKind {
		return status.Error(codes.FailedPrecondition, "model request kind does not match its start span")
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

type runtimeToolUseEventPayload struct {
	Name                string          `json:"name"`
	Input               json.RawMessage `json:"input"`
	MCPServerName       string          `json:"mcp_server_name"`
	EvaluatedPermission string          `json:"evaluated_permission"`
}

type durableToolResultEventPayload struct {
	ToolUseID      string `json:"tool_use_id"`
	ToolUseEventID string `json:"tool_use_event_id"`
	MCPToolUseID   string `json:"mcp_tool_use_id"`
	Content        []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError bool `json:"is_error"`
}

func durableToolResultUseEventID(eventType string, payload durableToolResultEventPayload) (string, error) {
	switch eventType {
	case "agent.tool_result":
		if payload.MCPToolUseID != "" ||
			(payload.ToolUseEventID != "" && payload.ToolUseID != "" && payload.ToolUseEventID != payload.ToolUseID) {
			return "", status.Error(codes.FailedPrecondition, "tool result event identity is invalid")
		}
		toolUseEventID := defaultString(payload.ToolUseEventID, payload.ToolUseID)
		if toolUseEventID == "" {
			return "", status.Error(codes.FailedPrecondition, "tool result event is missing its tool-use identity")
		}
		return toolUseEventID, nil
	case "agent.mcp_tool_result":
		if payload.MCPToolUseID == "" || payload.ToolUseEventID != "" || payload.ToolUseID != "" {
			return "", status.Error(codes.FailedPrecondition, "MCP tool result event identity is invalid")
		}
		return payload.MCPToolUseID, nil
	default:
		return "", status.Error(codes.FailedPrecondition, "tool result event type is invalid")
	}
}

type runtimeToolProjectionPayload struct {
	ModelToolCallID         string          `json:"model_tool_call_id"`
	ToolName                string          `json:"tool_name"`
	ProviderInput           json.RawMessage `json:"provider_input"`
	CanonicalExecutionInput json.RawMessage `json:"canonical_execution_input"`
	State                   string          `json:"state"`
	Output                  *struct {
		Text      string `json:"text"`
		Truncated bool   `json:"truncated"`
	} `json:"output"`
	Error *struct {
		Type      string `json:"type"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

func runtimeToolProjectionFromContextDelta(
	eventType string,
	delta *bridgev1.RuntimeContextDelta,
	canonicalExecutionInputJSON string,
) (runtimeToolProjectionPayload, error) {
	switch eventType {
	case "agent.message":
		if canonicalExecutionInputJSON != "" {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Assistant message cannot declare canonical execution input")
		}
		if delta == nil || len(delta.GetParts()) == 0 {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Assistant message context delta is empty")
		}
		return runtimeToolProjectionPayload{}, nil
	case "agent.tool_use", "agent.mcp_tool_use":
		canonicalExecutionInput, err := canonicalRunToolJSON(canonicalExecutionInputJSON)
		if err != nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool Use canonical execution input is invalid")
		}
		if len(canonicalExecutionInput) > runtimeToolInputJSONMaxBytes {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool Use canonical execution input exceeds its storage bound")
		}
		if delta == nil || len(delta.GetParts()) == 0 {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool Use context delta is empty")
		}
		var selected *runtimeToolProjectionPayload
		for _, part := range delta.GetParts() {
			call := part.GetToolCall()
			if call == nil {
				continue
			}
			if _, err := decodeRuntimeDeclarationValue(call.GetProviderInputJson()); err != nil {
				return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "Tool Use provider input is invalid")
			}
			projection := runtimeToolProjectionPayload{
				ModelToolCallID:         call.GetModelToolCallId(),
				ToolName:                call.GetToolName(),
				ProviderInput:           json.RawMessage(call.GetProviderInputJson()),
				CanonicalExecutionInput: json.RawMessage(canonicalExecutionInput),
				State:                   "running",
			}
			if selected != nil {
				return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool Use context delta is ambiguous")
			}
			selected = &projection
		}
		if selected == nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "Tool Use context delta is missing its Tool Call")
		}
		return *selected, nil
	default:
		if delta != nil || canonicalExecutionInputJSON != "" {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "event type does not accept a Runtime declaration")
		}
		return runtimeToolProjectionPayload{}, nil
	}
}

func applyToolEventBookkeepingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventID string,
	eventType string,
	payloadJSON string,
	projection runtimeToolProjectionPayload,
	now time.Time,
) error {
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		var payload runtimeToolUseEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return status.Error(codes.FailedPrecondition, "tool use event payload is invalid")
		}
		if projection.ModelToolCallID == "" || projection.ToolName == "" || len(projection.CanonicalExecutionInput) == 0 {
			return status.Error(codes.FailedPrecondition, "tool use declaration is missing its tool part")
		}
		if payload.Name != projection.ToolName {
			return status.Error(codes.FailedPrecondition, "tool use declaration identity is inconsistent")
		}
		if _, err := runtimeToolEventInputJSON(payload.Input); err != nil {
			return err
		}
		if eventType == "agent.mcp_tool_use" && payload.MCPServerName == "" {
			return status.Error(codes.FailedPrecondition, "MCP tool use declaration server is invalid")
		}
		switch payload.EvaluatedPermission {
		case "ask", "allow", "deny":
			return upsertPendingToolRouteTx(
				ctx,
				tx,
				scope,
				eventID,
				projection,
				string(projection.CanonicalExecutionInput),
				payload.EvaluatedPermission,
				now,
			)
		default:
			return status.Error(codes.FailedPrecondition, "tool use permission is invalid")
		}
	case "agent.tool_result", "agent.mcp_tool_result":
		var payload durableToolResultEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return status.Error(codes.FailedPrecondition, "tool result event payload is invalid")
		}
		toolUseEventID, err := durableToolResultUseEventID(eventType, payload)
		if err != nil {
			return err
		}
		if projection.ModelToolCallID == "" {
			return status.Error(codes.FailedPrecondition, "tool result declaration is incomplete")
		}
		if projection.State != "completed" && projection.State != "error" && projection.State != "cancelled" {
			return status.Error(codes.FailedPrecondition, "tool result declaration state is not terminal")
		}
		if err := resolveSettledToolRouteTx(ctx, tx, scope, toolUseEventID, eventID, now); err != nil {
			return err
		}
		if eventType == "agent.tool_result" {
			return consumeSandboxExecutionTx(ctx, tx, scope, toolUseEventID, eventID, projection, now)
		}
		return nil
	default:
		return nil
	}
}

func consumeSandboxExecutionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	terminalEventID string,
	projection runtimeToolProjectionPayload,
	now time.Time,
) error {
	var toolKind, toolName string
	var modelToolCallID sql.NullString
	var executionState, backgroundOperationState, resultJSON, resultDigest sql.NullString
	err := tx.QueryRow(ctx,
		`SELECT tool_kind, tool_name, model_tool_call_id, execution_state, background_operation_state, result_json, result_digest
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
	).Scan(&toolKind, &toolName, &modelToolCallID, &executionState, &backgroundOperationState, &resultJSON, &resultDigest)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if toolKind == bridgeToolKindSandboxBackground {
		if !backgroundOperationState.Valid || backgroundOperationState.String != "terminal" || !resultJSON.Valid || !json.Valid([]byte(resultJSON.String)) {
			return status.Error(codes.FailedPrecondition, "sandbox background result is not ready for conversation settlement")
		}
		updated, err := tx.Exec(ctx,
			`UPDATE session_runtime_tool_results
			    SET result_json = NULL, consumed_by_terminal_event_id = $5,
			        consumption_reason = 'conversation_tool_result', updated_at = $6
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND tool_use_event_id = $4 AND tool_kind = 'sandbox_background'
			    AND background_operation_state = 'terminal' AND result_json IS NOT NULL`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			toolUseEventID, terminalEventID, now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(updated) {
			return status.Error(codes.FailedPrecondition, "sandbox background result consume failed")
		}
		return nil
	}
	if toolKind != bridgeToolKindSandbox {
		return nil
	}
	if !modelToolCallID.Valid || modelToolCallID.String != projection.ModelToolCallID || toolName != projection.ToolName {
		return status.Error(codes.FailedPrecondition, "sandbox tool result identity does not match its execution")
	}
	if !executionState.Valid || executionState.String != "terminal_unconsumed" || !resultJSON.Valid || !json.Valid([]byte(resultJSON.String)) {
		return status.Error(codes.FailedPrecondition, "sandbox tool execution is not ready for conversation settlement")
	}
	if !resultDigest.Valid || !validSandboxResultDigest(resultDigest.String) {
		return status.Error(codes.FailedPrecondition, "sandbox tool result digest is invalid")
	}
	attachmentRefs, err := sandboxExecutionAttachmentRefs(resultJSON.String)
	if err != nil {
		return err
	}
	for _, attachmentRef := range attachmentRefs {
		updated, err := tx.Exec(ctx,
			`UPDATE session_transient_attachments
			    SET status = 'active', expires_at = $6, updated_at = $5
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND source_tool_use_event_id = $4 AND attachment_ref = $7
			    AND status = 'staged'`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			toolUseEventID, now, now.Add(defaultTransientAttachmentTTL), attachmentRef,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(updated) {
			return status.Error(codes.FailedPrecondition, "sandbox tool attachment is not staged")
		}
	}
	updated, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'consumed', result_json = NULL,
		        consumed_by_terminal_event_id = $5,
		        consumption_reason = 'conversation_tool_result', updated_at = $6
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND tool_kind = 'sandbox_tool'
		    AND execution_state = 'terminal_unconsumed'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		toolUseEventID, terminalEventID, now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(updated) {
		return status.Error(codes.FailedPrecondition, "sandbox tool execution consume failed")
	}
	return nil
}

func validSandboxResultDigest(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func consumeSandboxExecutionForTerminalWriterTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	toolUseEventID string,
	terminalEventID string,
	reason string,
	now time.Time,
) error {
	if reason != "pod_lost" && reason != "runtime_terminated" && reason != "cleanup_wait_expired" && reason != "conversation_tool_result" {
		return status.Error(codes.Internal, "sandbox execution terminal consumption reason is invalid")
	}
	var terminalPayloadJSON string
	if err := tx.QueryRow(ctx,
		`SELECT payload_json
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND event_id = $4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), terminalEventID,
	).Scan(&terminalPayloadJSON); err != nil {
		return err
	}
	// A staged provider result is not the conversation's terminal Tool Result.
	// The first terminal session event owns settlement; alternate terminal
	// writers clear staged output and retain only its digest and execution record so a
	// late provider result cannot rewrite terminal conversation history.
	fallbackDigest := sha256Hex(terminalPayloadJSON)
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = CASE WHEN tool_kind='sandbox_tool' THEN 'consumed' ELSE execution_state END,
		        background_operation_state = CASE WHEN tool_kind='sandbox_background' THEN 'terminal' ELSE background_operation_state END,
		        result_json = NULL,
		        result_digest = COALESCE(NULLIF(result_digest, ''), $8),
		        consumed_by_terminal_event_id = $5,
		        consumption_reason = $6, updated_at = $7
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND tool_kind IN ('sandbox_tool','sandbox_background')
		    AND (tool_kind='sandbox_tool' AND execution_state <> 'consumed'
		      OR tool_kind='sandbox_background' AND consumed_by_terminal_event_id IS NULL)`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		toolUseEventID, terminalEventID, reason, now, fallbackDigest,
	)
	if err != nil {
		return err
	}
	_ = result
	return nil
}

func sandboxExecutionAttachmentRefs(resultJSON string) ([]string, error) {
	var decoded any
	if err := json.Unmarshal([]byte(resultJSON), &decoded); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "sandbox tool execution result is invalid")
	}
	seen := make(map[string]struct{})
	var refs []string
	var visit func(any) error
	visit = func(value any) error {
		switch typed := value.(type) {
		case []any:
			for _, item := range typed {
				if err := visit(item); err != nil {
					return err
				}
			}
		case map[string]any:
			for key, item := range typed {
				if key == "attachment_ref" {
					ref, ok := item.(string)
					if !ok || ref == "" {
						return status.Error(codes.FailedPrecondition, "sandbox tool attachment reference is invalid")
					}
					if _, duplicate := seen[ref]; !duplicate {
						seen[ref] = struct{}{}
						refs = append(refs, ref)
					}
					continue
				}
				if err := visit(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(decoded); err != nil {
		return nil, err
	}
	return refs, nil
}

func runtimeToolEventInputJSON(raw json.RawMessage) (string, error) {
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

func upsertPendingToolRouteTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventID string,
	projection runtimeToolProjectionPayload,
	inputJSON string,
	evaluatedPermission string,
	now time.Time,
) error {
	statusValue := "pending"
	var decision any
	if evaluatedPermission != "ask" {
		statusValue = "resolving"
		decision = evaluatedPermission
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO session_pending_tool_uses (
			workspace_id, session_id, session_thread_id, tool_use_event_id, model_tool_call_id,
			tool_name, input_json, status, decision, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
		ON CONFLICT (workspace_id, session_id, session_thread_id, tool_use_event_id)
		DO UPDATE SET
			model_tool_call_id = EXCLUDED.model_tool_call_id,
			tool_name = EXCLUDED.tool_name,
			input_json = EXCLUDED.input_json,
			updated_at = EXCLUDED.updated_at
		WHERE session_pending_tool_uses.status IN ('pending', 'resolving')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		projection.ModelToolCallID,
		projection.ToolName,
		inputJSON,
		statusValue,
		decision,
		now,
	)
	return err
}

func resolveSettledToolRouteTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string, resultEventID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET status = 'resolved',
		        result_event_id = $5,
		        resolved_at = $6,
		        updated_at = $6
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND status = 'resolving'
		    AND decision IN ('allow','deny')
		    AND result_event_id IS NULL`,
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
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "durable Tool route settlement did not close its exact route")
	}
	return nil
}

func userMessageContextDraftJSON(payloadJSON string) (string, error) {
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
		parts = append(parts, map[string]any{
			"type":      "text",
			"text":      item.Text,
			"truncated": false,
		})
	}
	return marshalBridgeDataJSON(map[string]any{"parts": parts})
}
