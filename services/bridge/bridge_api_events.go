package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	requestStart, err := normalizeRequestStartStamp(request)
	if err != nil {
		return nil, err
	}
	if request.GetEventType() == "agent.mcp_tool_result" {
		if request.GetMcpMaterializationHandle() == "" {
			return nil, status.Error(codes.InvalidArgument, "mcp tool result requires a materialization handle")
		}
	} else if request.GetMcpMaterializationHandle() != "" {
		return nil, status.Error(codes.InvalidArgument, "materialization handle requires an mcp tool-result event")
	}
	if request.GetSandboxResultDigest() != "" {
		if request.GetEventType() != "agent.tool_result" || !validSandboxResultDigest(request.GetSandboxResultDigest()) {
			return nil, status.Error(codes.InvalidArgument, "sandbox result digest requires a valid tool-result event")
		}
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
	if _, _, projected := writeEventDraftClass(request.GetEventType()); projected {
		if len(request.GetDrafts()) != 1 {
			return nil, status.Error(codes.InvalidArgument, "projected write event requires exactly one runtime message draft")
		}
	} else if len(request.GetDrafts()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "event type does not accept runtime message drafts")
	}
	if len(stableReasoning.Parts) > 0 && len(request.GetDrafts()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "stable reasoning requires a runtime message draft")
	}
	declarationDigest, err := writeEventDeclarationDigest(
		request,
		payloadJSON,
		stableReasoning.CanonicalJSON,
		serverToolUse.CanonicalJSON,
	)
	if err != nil {
		return nil, err
	}
	key := request.GetRuntimeWriteId()
	now := s.now()
	var (
		ack                    *bridgev1.BridgeWriteAck
		receipt                *bridgev1.DeclarationReceipt
		mcpMaterialization     mcpMaterializationIdentity
		mcpMaterializationUsed bool
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
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		operationSourceKind, err := writeEventOperationSourceKindTx(ctx, tx, request.GetScope(), request.GetEventType())
		if err != nil {
			return err
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpWriteEvent,
			operationSourceKind,
			key,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "write event receipt is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "write event idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "write event receipt is invalid")
			}
			ack = duplicateAck("", key)
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if request.GetEventType() == "agent.tool_use" || request.GetEventType() == "agent.mcp_tool_use" {
			if err := verifyModelRequestAcceptsMembersTx(ctx, tx, request.GetScope(), request.GetModelRequestId()); err != nil {
				return err
			}
		}
		if request.GetEventType() == "agent.tool_result" || request.GetEventType() == "agent.mcp_tool_result" {
			if err := verifyToolResultMembershipTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetModelRequestId(),
				request.GetEventType(),
				payloadJSON,
			); err != nil {
				return err
			}
		}
		if requestStart != nil {
			if err := verifyRequestStartUniqueTx(ctx, tx, request.GetScope(), request.GetModelRequestId()); err != nil {
				return err
			}
			if err := verifyRequestStartMessageBoundaryTx(ctx, tx, request.GetScope(), requestStart.GetContextThroughMessageSequence()); err != nil {
				return err
			}
		}
		if len(stableReasoning.Parts) > 0 {
			if err := validateStableReasoningAnchorBudgetTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetModelRequestId(),
				stableReasoning,
			); err != nil {
				return err
			}
		}
		eventType := operationSourceKind
		eventPayloadJSON := payloadJSON
		if eventType == "agent.thread_message_sent" {
			eventPayloadJSON, err = publicSentInterAgentEventPayloadTx(ctx, tx, request.GetScope(), eventPayloadJSON)
			if err != nil {
				return err
			}
		}
		if operationSourceKind != request.GetEventType() {
			eventPayloadJSON, err = threadStatusPayloadJSON(eventType, request.GetScope(), threadScope, "")
			if err != nil {
				return err
			}
			if err := updateChildThreadStatusTx(ctx, tx, request.GetScope(), "running", now); err != nil {
				return err
			}
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
				"context_through_message_sequence": requestStart.GetContextThroughMessageSequence(),
				"request_kind":                     requestStart.GetRequestKind(),
			})
			if err != nil {
				return err
			}
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
		receipt, err = commitWriteEventDraftsTx(
			ctx,
			tx,
			request.GetScope(),
			key,
			eventType,
			eventID,
			sequence,
			request.GetModelRequestId(),
			request.GetDrafts(),
			stableReasoning,
			now,
		)
		if err != nil {
			return err
		}
		receipt.DeclarationDigest = declarationDigest
		receipt.RequestStart = requestStart
		toolProjection, err := runtimeToolProjectionFromDeclaration(
			eventID,
			eventType,
			eventPayloadJSON,
			request.GetDrafts(),
			receipt.GetMessages(),
		)
		if err != nil {
			return err
		}
		if eventType == "agent.tool_use" || eventType == "agent.mcp_tool_use" {
			if err := verifyModelToolCallIDUniqueTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetModelRequestId(),
				toolProjection.ModelToolCallID,
			); err != nil {
				return err
			}
		}
		if serverToolUse.Present {
			if err := verifyWebToolResultUsageTx(ctx, tx, request.GetScope(), eventType, eventPayloadJSON, toolProjection); err != nil {
				return err
			}
		}
		if err := applyWriteEventToolBookkeepingTx(
			ctx,
			tx,
			request.GetScope(),
			eventID,
			eventType,
			eventPayloadJSON,
			toolProjection,
			request.GetSandboxResultDigest(),
			now,
		); err != nil {
			return err
		}
		if request.GetMcpMaterializationHandle() != "" {
			mcpMaterialization, err = consumeMCPMaterializationTx(
				ctx,
				tx,
				request.GetScope(),
				eventType,
				eventPayloadJSON,
				request.GetMcpMaterializationHandle(),
				now,
			)
			if err != nil {
				return err
			}
			mcpMaterializationUsed = true
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
		if threadScope.role == "main" && eventType == "session.status_running" {
			if err := markPublicSessionRunningTx(ctx, tx, request.GetScope(), eventID, now); err != nil {
				return err
			}
		}
		receiptJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpWriteEvent,
			operationSourceKind,
			key,
			declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck("", key)
		return nil
	}); err != nil {
		return nil, err
	}
	if receipt == nil || len(receipt.GetEvents()) != 1 {
		return nil, status.Error(codes.FailedPrecondition, "write event receipt is invalid")
	}
	if mcpMaterializationUsed {
		logMCPMaterializationConsumed(
			s.Logger,
			request.GetScope(),
			request.GetMcpMaterializationHandle(),
			mcpMaterialization,
		)
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
		ack,
		observation,
	)
	eventStamp := receipt.GetEvents()[0]
	return &bridgev1.WriteEventResponse{
		Ack:      ack,
		EventId:  eventStamp.GetEventId(),
		Sequence: eventStamp.GetEventSequence(),
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  []*bridgev1.DeclarationReceipt{receipt},
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
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
	return nil
}

func verifyToolResultMembershipTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	resultEventType string,
	payloadJSON string,
) error {
	if modelRequestID == "" {
		return status.Error(codes.InvalidArgument, "model request id is required")
	}
	var payload runtimeToolResultEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return status.Error(codes.FailedPrecondition, "tool result event payload is invalid")
	}
	toolUseEventID, err := runtimeToolResultUseEventID(resultEventType, payload)
	if err != nil {
		return err
	}
	toolUseEventType := "agent.tool_use"
	if resultEventType == "agent.mcp_tool_result" {
		toolUseEventType = "agent.mcp_tool_use"
	}
	if toolUseEventID == "" {
		return status.Error(codes.FailedPrecondition, "tool result event is missing its tool-use identity")
	}
	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1
			  FROM session_events
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND session_thread_id = $3
			   AND event_id = $4
			   AND model_request_id = $5
			   AND type = $6
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventID,
		modelRequestID,
		toolUseEventType,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return status.Error(codes.FailedPrecondition, "tool result has no matching Tool Use")
	}
	if _, settled, err := toolResultForToolUseExistsTx(
		ctx,
		tx,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseEventType,
		toolUseEventID,
	); err != nil {
		return err
	} else if settled {
		return status.Error(codes.AlreadyExists, "Tool Use already has a terminal result")
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
	modelRequestID string,
	modelToolCallID string,
) error {
	if modelToolCallID == "" {
		return status.Error(codes.FailedPrecondition, "model tool call id is missing")
	}
	var occurrences int
	if err := tx.QueryRow(ctx,
		`SELECT count(*)
		   FROM session_messages AS message
		   JOIN session_events AS event
		     ON event.workspace_id = message.workspace_id
		    AND event.session_id = message.session_id
		    AND event.session_thread_id = message.session_thread_id
		    AND event.event_id = message.source_event_id
		  CROSS JOIN LATERAL jsonb_array_elements(
		    CASE
		      WHEN jsonb_typeof(message.data_json::jsonb -> 'parts') = 'array'
		      THEN message.data_json::jsonb -> 'parts'
		      ELSE '[]'::jsonb
		    END
		  ) AS part
		  WHERE event.workspace_id = $1
		    AND event.session_id = $2
		    AND event.session_thread_id = $3
		    AND event.model_request_id = $4
		    AND part ->> 'toolCallId' = $5`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		modelRequestID,
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

func writeEventOperationSourceKindTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
) (string, error) {
	if eventType != "session.status_running" {
		return eventType, nil
	}
	var role string
	if err := tx.QueryRow(ctx,
		`SELECT role
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&role); dbconnect.IsNoRows(err) {
		return "", closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "runtime thread is stale"))
	} else if err != nil {
		return "", err
	}
	if role != "main" {
		return "session.thread_status_running", nil
	}
	return eventType, nil
}

type mcpMaterializationIdentity struct {
	ToolUseEventID string
	MCPServerName  string
	ToolName       string
}

func consumeMCPMaterializationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventType string,
	payloadJSON string,
	handle string,
	now time.Time,
) (mcpMaterializationIdentity, error) {
	if eventType != "agent.mcp_tool_result" {
		return mcpMaterializationIdentity{}, status.Error(codes.InvalidArgument, "materialization handle requires an mcp tool result")
	}
	var resultPayload runtimeToolResultEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &resultPayload); err != nil ||
		resultPayload.MCPToolUseID == "" ||
		resultPayload.MCPToolUseID != handle {
		return mcpMaterializationIdentity{}, status.Error(codes.InvalidArgument, "mcp materialization handle does not match the tool result")
	}
	stored, ok, err := readRuntimeToolResultTx(ctx, tx, scope, handle)
	if err != nil {
		return mcpMaterializationIdentity{}, err
	}
	if !ok || stored.ToolKind != bridgeToolKindMCP || stored.MCPClaimStatus.String != mcpClaimStatusStored {
		return mcpMaterializationIdentity{}, status.Error(codes.FailedPrecondition, "mcp materialization is not staged")
	}
	mcpServerName, toolName, ok := strings.Cut(stored.ToolName, "/")
	if !ok || mcpServerName == "" || toolName == "" {
		return mcpMaterializationIdentity{}, status.Error(codes.Internal, "stored mcp tool identity is invalid")
	}
	attachmentRefs, err := mcpMaterializationAttachmentRefs(stored.ResultJSON)
	if err != nil {
		return mcpMaterializationIdentity{}, err
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
			handle,
			attachmentRef,
			now,
		)
		if err != nil {
			return mcpMaterializationIdentity{}, err
		}
		if !rowsAffected(update) {
			return mcpMaterializationIdentity{}, status.Error(codes.FailedPrecondition, "mcp materialization attachment is not staged")
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
		handle,
		now,
	)
	if err != nil {
		return mcpMaterializationIdentity{}, err
	}
	if !rowsAffected(update) {
		return mcpMaterializationIdentity{}, status.Error(codes.FailedPrecondition, "mcp materialization consume failed")
	}
	return mcpMaterializationIdentity{
		ToolUseEventID: handle,
		MCPServerName:  mcpServerName,
		ToolName:       toolName,
	}, nil
}

func mcpMaterializationAttachmentRefs(resultJSON string) ([]string, error) {
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

func logMCPMaterializationConsumed(
	logger *slog.Logger,
	scope *bridgev1.RuntimeScope,
	handle string,
	identity mcpMaterializationIdentity,
) {
	if logger == nil || scope == nil {
		return
	}
	logger.Info("bridge.mcp_materialization",
		slog.String("operation", "mcp_materialization"),
		slog.String("event.kind", "mcp_materialization_consumed"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("workspace.id", scope.GetWorkspaceId()),
		slog.String("session.id", scope.GetSessionId()),
		slog.String("session.thread.id", scope.GetSessionThreadId()),
		slog.String("mcp.tool_use_event_id", identity.ToolUseEventID),
		slog.String("mcp.server.name", identity.MCPServerName),
		slog.String("mcp.tool.name", identity.ToolName),
		slog.String("mcp.materialization_handle", handle),
		slog.String("outcome", "committed"),
	)
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
	projection runtimeToolProjectionPayload,
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
	if projection.ToolName != "web" || projection.MCPServerName != "" {
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

func normalizeRequestStartStamp(request *bridgev1.WriteEventRequest) (*bridgev1.RequestStartStamp, error) {
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
	return &bridgev1.RequestStartStamp{
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

func runtimeToolResultUseEventID(eventType string, payload runtimeToolResultEventPayload) (string, error) {
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

func runtimeToolProjectionFromDeclaration(
	eventID string,
	eventType string,
	payloadJSON string,
	drafts []*bridgev1.RuntimeMessageDraft,
	messageStamps []*bridgev1.DurableMessageStamp,
) (runtimeToolProjectionPayload, error) {
	if len(drafts) == 0 {
		return runtimeToolProjectionPayload{}, nil
	}
	if len(drafts) != 1 || len(messageStamps) != 1 || drafts[0] == nil || messageStamps[0] == nil {
		return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "runtime tool declaration is malformed")
	}
	draft := drafts[0]
	messageStamp := messageStamps[0]
	if len(draft.GetParts()) != len(messageStamp.GetParts()) {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "runtime tool declaration stamp set is incomplete")
	}
	targetToolUseEventID := ""
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		if eventID == "" {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "runtime tool declaration event id is missing")
		}
	case "agent.tool_result", "agent.mcp_tool_result":
		var payload runtimeToolResultEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "tool result event payload is invalid")
		}
		var err error
		targetToolUseEventID, err = runtimeToolResultUseEventID(eventType, payload)
		if err != nil {
			return runtimeToolProjectionPayload{}, err
		}
	default:
		return runtimeToolProjectionPayload{}, nil
	}
	var selected *runtimeToolProjectionPayload
	for index, partDraft := range draft.GetParts() {
		if partDraft == nil || partDraft.GetPartKind() != "tool" {
			continue
		}
		partStamp := messageStamp.GetParts()[index]
		if partStamp == nil || partStamp.GetRuntimeLocalPartId() != partDraft.GetRuntimeLocalPartId() {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "runtime tool declaration stamp is invalid")
		}
		var part struct {
			ToolCallID     string `json:"toolCallId"`
			ToolName       string `json:"toolName"`
			ToolUseEventID string `json:"toolUseEventId"`
			ToolEvent      *struct {
				Kind          string `json:"kind"`
				MCPServerName string `json:"mcpServerName"`
			} `json:"toolEvent"`
			State struct {
				Status string          `json:"status"`
				Input  json.RawMessage `json:"input"`
				Output *struct {
					Text      string `json:"text"`
					Truncated bool   `json:"truncated"`
				} `json:"output"`
				Error *struct {
					Type      string `json:"type"`
					Message   string `json:"message"`
					Retryable bool   `json:"retryable"`
				} `json:"error"`
			} `json:"state"`
		}
		if err := json.Unmarshal([]byte(partDraft.GetPartJson()), &part); err != nil ||
			part.ToolCallID == "" || part.ToolName == "" {
			return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "runtime tool declaration part is invalid")
		}
		selectedForEvent := false
		switch eventType {
		case "agent.tool_use", "agent.mcp_tool_use":
			selectedForEvent = part.ToolUseEventID == ""
		case "agent.tool_result", "agent.mcp_tool_result":
			selectedForEvent = part.ToolUseEventID == targetToolUseEventID
		}
		if !selectedForEvent {
			continue
		}
		input := json.RawMessage(`{}`)
		if len(part.State.Input) > 0 && string(part.State.Input) != "null" {
			var bounded struct {
				Value json.RawMessage `json:"value"`
			}
			if err := json.Unmarshal(part.State.Input, &bounded); err != nil {
				return runtimeToolProjectionPayload{}, status.Error(codes.InvalidArgument, "runtime tool declaration input is invalid")
			}
			if len(bounded.Value) > 0 && string(bounded.Value) != "null" {
				input = bounded.Value
			}
		}
		projection := runtimeToolProjectionPayload{
			MessageID:       messageStamp.GetMessageId(),
			PartID:          partStamp.GetPartId(),
			PartSequence:    int(partStamp.GetPartSequence()),
			ModelToolCallID: part.ToolCallID,
			ToolName:        part.ToolName,
			Input:           input,
			State:           part.State.Status,
			Output:          part.State.Output,
			Error:           part.State.Error,
		}
		if part.ToolEvent != nil && part.ToolEvent.Kind == "mcp" {
			projection.MCPServerName = part.ToolEvent.MCPServerName
		}
		if selected != nil {
			return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "runtime tool declaration has an ambiguous current tool part")
		}
		selected = &projection
	}
	if selected == nil {
		return runtimeToolProjectionPayload{}, status.Error(codes.FailedPrecondition, "runtime tool declaration is missing its current tool part")
	}
	return *selected, nil
}

func applyWriteEventToolBookkeepingTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventID string,
	eventType string,
	payloadJSON string,
	projection runtimeToolProjectionPayload,
	sandboxResultDigest string,
	now time.Time,
) error {
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		var payload runtimeToolUseEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return status.Error(codes.FailedPrecondition, "tool use event payload is invalid")
		}
		if projection.ModelToolCallID == "" || projection.ToolName == "" {
			return status.Error(codes.FailedPrecondition, "tool use declaration is missing its tool part")
		}
		if eventType == "agent.mcp_tool_use" &&
			(payload.MCPServerName == "" || projection.MCPServerName != payload.MCPServerName) {
			return status.Error(codes.FailedPrecondition, "MCP tool use declaration server is invalid")
		}
		if eventType == "agent.tool_use" && projection.MCPServerName != "" {
			return status.Error(codes.FailedPrecondition, "tool use declaration server is invalid")
		}
		switch payload.EvaluatedPermission {
		case "ask":
			inputJSON, err := runtimeToolEventInputJSON(payload.Input)
			if err != nil {
				return err
			}
			return upsertPendingToolApprovalTx(ctx, tx, scope, eventID, projection, inputJSON, now)
		case "allow", "deny":
			return nil
		default:
			return status.Error(codes.FailedPrecondition, "tool use permission is invalid")
		}
	case "agent.tool_result", "agent.mcp_tool_result":
		var payload runtimeToolResultEventPayload
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return status.Error(codes.FailedPrecondition, "tool result event payload is invalid")
		}
		toolUseEventID, err := runtimeToolResultUseEventID(eventType, payload)
		if err != nil {
			return err
		}
		if projection.ModelToolCallID == "" {
			return status.Error(codes.FailedPrecondition, "tool result declaration is incomplete")
		}
		if projection.State != "completed" && projection.State != "error" && projection.State != "cancelled" {
			return status.Error(codes.FailedPrecondition, "tool result declaration state is not terminal")
		}
		if eventType == "agent.mcp_tool_result" && projection.MCPServerName == "" {
			return status.Error(codes.FailedPrecondition, "MCP tool result declaration server is invalid")
		}
		if eventType == "agent.tool_result" && projection.MCPServerName != "" {
			return status.Error(codes.FailedPrecondition, "tool result declaration server is invalid")
		}
		if err := markPendingToolResultResolvedTx(ctx, tx, scope, toolUseEventID, eventID, now); err != nil {
			return err
		}
		if eventType == "agent.tool_result" {
			return consumeSandboxExecutionTx(ctx, tx, scope, toolUseEventID, eventID, projection, sandboxResultDigest, now)
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
	sandboxResultDigest string,
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
		if sandboxResultDigest != "" {
			return status.Error(codes.FailedPrecondition, "sandbox result digest has no execution")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if toolKind == bridgeToolKindSandboxBackground {
		if sandboxResultDigest != "" {
			return status.Error(codes.FailedPrecondition, "sandbox result digest does not belong to a background result")
		}
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
		if sandboxResultDigest != "" {
			return status.Error(codes.FailedPrecondition, "sandbox result digest does not belong to this tool result")
		}
		return nil
	}
	if !modelToolCallID.Valid || modelToolCallID.String != projection.ModelToolCallID || toolName != projection.ToolName {
		return status.Error(codes.FailedPrecondition, "sandbox tool result identity does not match its execution")
	}
	if !executionState.Valid || executionState.String != "terminal_unconsumed" || !resultJSON.Valid || !json.Valid([]byte(resultJSON.String)) {
		return status.Error(codes.FailedPrecondition, "sandbox tool execution is not ready for conversation settlement")
	}
	if sandboxResultDigest == "" || !resultDigest.Valid || resultDigest.String != sandboxResultDigest {
		return status.Error(codes.FailedPrecondition, "sandbox tool result digest does not match its execution")
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
	if reason != "pod_lost" && reason != "runtime_terminated" && reason != "cleanup_wait_expired" {
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
	// Alternate terminal writers preserve a provider result digest when one
	// exists; otherwise the terminal event they just authored is the result
	// evidence retained by the thin execution receipt.
	fallbackDigest := sha256Hex(terminalPayloadJSON)
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'consumed', result_json = NULL,
		        result_digest = COALESCE(NULLIF(result_digest, ''), $8),
		        consumed_by_terminal_event_id = $5,
		        consumption_reason = $6, updated_at = $7
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND tool_kind = 'sandbox_tool'
		    AND execution_state <> 'consumed'`,
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
