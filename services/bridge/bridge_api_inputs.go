package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge inputs protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) CommitInputs(ctx context.Context, request *bridgev1.CommitInputsRequest) (*bridgev1.CommitInputsResponse, error) {
	inputKind := defaultString(request.GetInputKind(), "messages")
	if request.GetRuntimeInputId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid commit inputs request")
	}
	if err := validateCommitInputsRequest(inputKind, request); err != nil {
		return nil, err
	}
	key := request.GetRuntimeInputId()
	declarationDigest, err := commitInputsDeclarationDigest(request, inputKind)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var ack *bridgev1.BridgeWriteAck
	var receipt *bridgev1.DeclarationReceipt
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_inputs", func(tx *dbconnect.Tx) error {
		// Serialize replay lookup before validating the current binding. A replacement
		// binding may recover an ACK lost by its predecessor, while concurrent writers
		// for the same session must still observe one committed operation.
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitInputs,
			inputKind,
			key,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "commit inputs receipt is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "commit inputs idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "commit inputs receipt is invalid")
			}
			ack = duplicateAck(key, "")
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		inboxStatus := ""
		if commitInputsKindUsesRuntimeInbox(inputKind) {
			var err error
			inboxStatus, err = lockAndValidateRuntimeInboxCommitTx(ctx, tx, request, inputKind)
			if err != nil {
				return err
			}
		}
		if inboxStatus == "committed" {
			return status.Error(codes.FailedPrecondition, "committed runtime input is missing idempotency state")
		}
		switch inputKind {
		case "messages":
			if err := markRuntimeInboxCommittedTx(ctx, tx, request.GetScope(), key, now); err != nil {
				return err
			}
			if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
				return err
			}
			receipt, err = commitInputDraftsTx(
				ctx,
				tx,
				request.GetScope(),
				inputKind,
				key,
				request.GetEventIds(),
				request.GetDrafts(),
				now,
			)
			if err != nil {
				return err
			}
			receipt.PendingAttachmentDeltaJson, err = loadCommittedInputAttachmentDeltaTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetEventIds(),
			)
			if err != nil {
				return err
			}
		case "interrupt_control":
			if err := requireCommitInputEventTypesTx(ctx, tx, request.GetScope(), request.GetEventIds(), "user.interrupt"); err != nil {
				return err
			}
			if err := cancelInterruptedPendingToolUsesTx(ctx, tx, request.GetScope(), now); err != nil {
				return err
			}
			if err := markRuntimeInboxCommittedTx(ctx, tx, request.GetScope(), key, now); err != nil {
				return err
			}
			if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
				return err
			}
		case "tool_confirmation":
			if err := markRuntimeInboxCommittedTx(ctx, tx, request.GetScope(), key, now); err != nil {
				return err
			}
			if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
				return err
			}
			if err := settleToolConfirmationEventsTx(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
				return err
			}
		case "inter_agent_message":
			if err := commitInterAgentMessageTx(ctx, tx, request.GetScope(), request.GetInterAgentMessageJson(), now); err != nil {
				return err
			}
		case "approval_review":
			if err := commitApprovalReviewInputTx(ctx, tx, request.GetScope(), request.GetApprovalReviewJson(), now); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "unsupported commit inputs kind")
		}
		if receipt == nil {
			receipt = &bridgev1.DeclarationReceipt{
				SessionThreadId: request.GetScope().GetSessionThreadId(),
				OperationKind:   bridgeOpCommitInputs,
				SourceKind:      inputKind,
				SourceId:        key,
			}
		}
		receipt.DeclarationDigest = declarationDigest
		resultJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitInputs,
			inputKind,
			key,
			declarationDigest,
			resultJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck(key, "")
		return nil
	}); err != nil {
		return nil, err
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	return &bridgev1.CommitInputsResponse{
		Ack: ack,
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  []*bridgev1.DeclarationReceipt{receipt},
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
}

func commitInputsKindUsesRuntimeInbox(inputKind string) bool {
	return inputKind == "messages" || inputKind == "interrupt_control" || inputKind == "tool_confirmation"
}

func lockAndValidateRuntimeInboxCommitTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.CommitInputsRequest, inputKind string) (string, error) {
	scope := request.GetScope()
	runtimeInputID := request.GetRuntimeInputId()
	row := tx.QueryRow(ctx,
		`SELECT session_id, session_thread_id, input_kind, event_ids_json,
		        sequence_from, sequence_to, status, binding_id, binding_generation, target_pod_uid
		   FROM session_runtime_inbox
		  WHERE workspace_id = $1
		    AND runtime_input_id = $2
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		runtimeInputID,
	)
	var inboxSessionID string
	var inboxThreadID string
	var inboxKind string
	var inboxEventIDsJSON string
	var inboxSequenceFrom sql.NullInt64
	var inboxSequenceTo sql.NullInt64
	var inboxStatus string
	var inboxBindingID string
	var inboxBindingGeneration int64
	var inboxTargetPodUID string
	if err := row.Scan(
		&inboxSessionID,
		&inboxThreadID,
		&inboxKind,
		&inboxEventIDsJSON,
		&inboxSequenceFrom,
		&inboxSequenceTo,
		&inboxStatus,
		&inboxBindingID,
		&inboxBindingGeneration,
		&inboxTargetPodUID,
	); dbconnect.IsNoRows(err) {
		return "", status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	} else if err != nil {
		return "", err
	}
	if inboxBindingID != scope.GetBinding().GetBindingId() ||
		inboxBindingGeneration != scope.GetBinding().GetBindingGeneration() ||
		inboxTargetPodUID != scope.GetBinding().GetTargetPodUid() ||
		(inboxStatus != "delivering" && inboxStatus != "accepted" && inboxStatus != "committed") {
		return "", status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	}
	var inboxEventIDs []string
	if err := json.Unmarshal([]byte(inboxEventIDsJSON), &inboxEventIDs); err != nil {
		return "", status.Error(codes.FailedPrecondition, "runtime inbox payload is invalid")
	}
	if inboxSessionID != scope.GetSessionId() ||
		inboxThreadID != scope.GetSessionThreadId() ||
		inboxKind != inputKind ||
		!sameBridgeStringSlice(inboxEventIDs, request.GetEventIds()) ||
		!sameBridgeSequence(inboxSequenceFrom, request.GetSequenceFrom()) ||
		!sameBridgeSequence(inboxSequenceTo, request.GetSequenceTo()) {
		return "", status.Error(codes.AlreadyExists, "commit inputs payload conflicts with runtime inbox")
	}
	return inboxStatus, nil
}

func markRuntimeInboxCommittedTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, runtimeInputID string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_inbox
		    SET status = 'committed',
		        committed_at = COALESCE(committed_at, $5),
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND runtime_input_id = $3
		    AND binding_id = $4
		    AND binding_generation = $6
		    AND target_pod_uid = $7
		    AND status IN ('delivering', 'accepted')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		runtimeInputID,
		scope.GetBinding().GetBindingId(),
		now,
		scope.GetBinding().GetBindingGeneration(),
		scope.GetBinding().GetTargetPodUid(),
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	}
	return nil
}

func sameBridgeStringSlice(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameBridgeSequence(expected sql.NullInt64, actual int64) bool {
	return expected.Valid == (actual > 0) && (!expected.Valid || expected.Int64 == actual)
}

func markSessionEventsProcessed(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventIDs []string, now time.Time) error {
	for _, eventID := range eventIDs {
		var revision int64
		var visibility string
		var sessionVisible bool
		err := tx.QueryRow(ctx,
			`UPDATE session_events
			    SET processed_at = $5,
			        updated_at = $5,
			        revision = revision + 1
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4
			    AND processed_at IS NULL
			  RETURNING revision, visibility, session_visible`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			eventID,
			now,
		).Scan(&revision, &visibility, &sessionVisible)
		if dbconnect.IsNoRows(err) {
			alreadyProcessed, exists, err := sessionEventProcessedState(ctx, tx, scope, eventID)
			if err != nil {
				return err
			}
			if !exists {
				return status.Error(codes.FailedPrecondition, "runtime input event is not committable")
			}
			if alreadyProcessed {
				continue
			}
			return status.Error(codes.FailedPrecondition, "runtime input event is not committable")
		}
		if err != nil {
			return err
		}
		if _, err := appendSessionEventStreamChangeForRevisionTx(ctx, tx, scope, eventID, revision, visibility, sessionVisible, now); err != nil {
			return err
		}
	}
	return nil
}

func loadCommittedInputAttachmentDeltaTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	eventIDs []string,
) ([]string, error) {
	result := make([]string, 0)
	for _, eventID := range eventIDs {
		var payloadJSON string
		if err := tx.QueryRow(ctx,
			`SELECT payload_json
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4
			    AND type = 'user.message'`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			eventID,
		).Scan(&payloadJSON); err != nil {
			return nil, err
		}
		var payload struct {
			Content []struct {
				Type   string `json:"type"`
				Source struct {
					Type   string `json:"type"`
					FileID string `json:"file_id"`
				} `json:"source"`
			} `json:"content"`
		}
		if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
			return nil, status.Error(codes.FailedPrecondition, "runtime input attachment payload is invalid")
		}
		seen := make(map[string]struct{})
		for _, part := range payload.Content {
			if (part.Type != "image" && part.Type != "document") ||
				part.Source.Type != "file" ||
				part.Source.FileID == "" {
				continue
			}
			if _, ok := seen[part.Source.FileID]; ok {
				continue
			}
			seen[part.Source.FileID] = struct{}{}
			var mime, filename sql.NullString
			if err := tx.QueryRow(ctx,
				`SELECT mime_type, filename
				   FROM files
				  WHERE workspace_id = $1
				    AND file_id = $2`,
				scope.GetWorkspaceId(),
				part.Source.FileID,
			).Scan(&mime, &filename); dbconnect.IsNoRows(err) {
				mime = sql.NullString{}
				filename = sql.NullString{}
			} else if err != nil {
				return nil, err
			}
			encoded, err := marshalBridgeJSON(bridgeLoadContextPendingAttachment{
				Origin: bridgeLoadContextAttachmentOrigin{
					FileBacked: &bridgeLoadContextFileAttachment{
						SourceEventID: eventID,
						FileID:        part.Source.FileID,
					},
				},
				Mime:     mime.String,
				Filename: filename.String,
			})
			if err != nil {
				return nil, err
			}
			result = append(result, encoded)
		}
	}
	return result, nil
}

func sessionEventProcessedState(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventID string) (bool, bool, error) {
	var processedAt sql.NullTime
	err := tx.QueryRow(ctx,
		`SELECT processed_at
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND event_id = $4`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
	).Scan(&processedAt)
	if dbconnect.IsNoRows(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return processedAt.Valid, true, nil
}

func requireCommitInputEventTypesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventIDs []string, expectedType string) error {
	for _, eventID := range eventIDs {
		var eventType string
		err := tx.QueryRow(ctx,
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
			eventID,
		).Scan(&eventType)
		if dbconnect.IsNoRows(err) {
			return status.Error(codes.FailedPrecondition, "runtime input event is not committable")
		}
		if err != nil {
			return err
		}
		if eventType != expectedType {
			return status.Error(codes.FailedPrecondition, "runtime input event type is not committable")
		}
	}
	return nil
}

func settleToolConfirmationEventsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, eventIDs []string, now time.Time) error {
	for _, eventID := range eventIDs {
		row := tx.QueryRow(ctx,
			`SELECT type, payload_json
			   FROM session_events
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND event_id = $4
			  FOR UPDATE`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			eventID,
		)
		var eventType string
		var payloadJSON string
		if err := row.Scan(&eventType, &payloadJSON); dbconnect.IsNoRows(err) {
			return status.Error(codes.FailedPrecondition, "runtime input event is not committable")
		} else if err != nil {
			return err
		}
		if eventType != "user.tool_confirmation" {
			return status.Error(codes.FailedPrecondition, "runtime input event type is not committable")
		}
		if err := recordPendingToolConfirmationDecisionTx(ctx, tx, scope, payloadJSON, now); err != nil {
			return err
		}
	}
	return nil
}

func cancelInterruptedPendingToolUsesTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) error {
	if _, err := lockThreadMutationTx(ctx, tx, scope); err != nil {
		return err
	}
	rows, err := tx.Query(ctx,
		`SELECT p.tool_use_event_id, p.model_tool_call_id, p.tool_name, p.input_json,
		        e.type, COALESCE(e.payload_json::jsonb ->> 'mcp_server_name', '')
		   FROM session_pending_tool_uses p
		   JOIN session_events e
		     ON e.workspace_id = p.workspace_id
		    AND e.session_id = p.session_id
		    AND e.session_thread_id = p.session_thread_id
		    AND e.event_id = p.tool_use_event_id
		  WHERE p.workspace_id = $1
		    AND p.session_id = $2
		    AND p.session_thread_id = $3
		    AND p.kind = 'approval'
		    AND p.status IN ('pending', 'resolving')
		  ORDER BY e.sequence ASC, p.tool_use_event_id ASC
		  FOR UPDATE OF p`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId())
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	waits := make([]cleanupPendingWait, 0)
	for rows.Next() {
		wait := cleanupPendingWait{ThreadID: scope.GetSessionThreadId()}
		if err := rows.Scan(&wait.ToolUseEventID, &wait.ModelToolCallID, &wait.ToolName, &wait.InputJSON, &wait.EventType, &wait.MCPServerName); err != nil {
			return err
		}
		waits = append(waits, wait)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	terminal := pendingToolTerminal{
		PartStatus: "cancelled",
		ErrorType:  "runtime_interrupted",
		Message:    "Tool call cancelled by user interrupt.",
	}
	for _, wait := range waits {
		eventID, err := insertPendingToolTerminalResultTx(ctx, tx, scope, wait, terminal, now)
		if err != nil {
			return err
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
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), wait.ToolUseEventID,
			eventID, now)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.FailedPrecondition, "interrupted pending tool use is stale")
		}
	}
	return nil
}

func validateCommitInputsRequest(inputKind string, request *bridgev1.CommitInputsRequest) error {
	switch inputKind {
	case "messages":
		if len(request.GetEventIds()) == 0 || len(request.GetDrafts()) != len(request.GetEventIds()) {
			return status.Error(codes.InvalidArgument, "message commit requires one user draft per event")
		}
		return nil
	case "interrupt_control":
		if len(request.GetEventIds()) != 1 {
			return status.Error(codes.InvalidArgument, "interrupt commit requires one event id")
		}
		return nil
	case "tool_confirmation":
		if len(request.GetEventIds()) != 1 {
			return status.Error(codes.InvalidArgument, "tool confirmation commit requires one event id")
		}
		return nil
	case "inter_agent_message":
		if request.GetInterAgentMessageJson() == "" || !json.Valid([]byte(request.GetInterAgentMessageJson())) {
			return status.Error(codes.InvalidArgument, "inter-agent message commit requires JSON payload")
		}
		return nil
	case "approval_review":
		if request.GetApprovalReviewJson() == "" || !json.Valid([]byte(request.GetApprovalReviewJson())) {
			return status.Error(codes.InvalidArgument, "approval review commit requires JSON payload")
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "unsupported commit inputs kind")
	}
}

type interAgentMessagePayload struct {
	DeliveryID           string          `json:"delivery_id"`
	SourceThreadID       string          `json:"source_thread_id"`
	SourceToolUseEventID string          `json:"source_tool_use_event_id"`
	Message              json.RawMessage `json:"message"`
	Presentation         string          `json:"presentation,omitempty"`
}

func commitInterAgentMessageTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, payloadJSON string, now time.Time) error {
	var payload interAgentMessagePayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return status.Error(codes.InvalidArgument, "inter-agent message payload must be JSON")
	}
	if payload.DeliveryID == "" || payload.SourceThreadID == "" || payload.SourceToolUseEventID == "" || len(payload.Message) == 0 || !json.Valid(payload.Message) {
		return status.Error(codes.InvalidArgument, "inter-agent message payload is incomplete")
	}
	if payload.Presentation == "" {
		payload.Presentation = "push"
	}
	if payload.Presentation != "push" && payload.Presentation != "pull" {
		return status.Error(codes.InvalidArgument, "inter-agent message presentation is invalid")
	}
	publicMessage, err := publicInterAgentMessageJSON(payload.Message)
	if err != nil {
		return err
	}
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if threadScope.role != "subagent" && threadScope.role != "main" {
		return status.Error(codes.FailedPrecondition, "inter-agent message must target a main or sub-agent thread")
	}
	_, existingPayloadJSON, ok, err := readReceivedInterAgentMessageTx(ctx, tx, scope, payload.DeliveryID)
	if err != nil {
		return err
	}
	if ok {
		return verifyInterAgentDeliveryReplay(existingPayloadJSON, payload)
	}
	if !threadReceivableTx(threadScope) {
		return status.Error(codes.FailedPrecondition, "target child thread is not receivable")
	}
	visibility, sessionVisible := threadScope.publicProjection("agent.thread_message_received")
	sourceTaskName, err := sessionThreadCallableTaskNameTx(ctx, tx, scope, payload.SourceThreadID)
	if err != nil {
		return err
	}
	eventPayloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "agent.thread_message_received",
		"delivery_id":              payload.DeliveryID,
		"source_thread_id":         payload.SourceThreadID,
		"source_task_name":         nullableJSONString(sourceTaskName),
		"source_tool_use_event_id": payload.SourceToolUseEventID,
		"message":                  publicMessage,
	})
	if err != nil {
		return err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'agent.thread_message_received', $6, $7, $8, $6, $9, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		eventPayloadJSON,
		visibility,
		sessionVisible,
		now,
	); err != nil {
		return err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now); err != nil {
		return err
	}
	if payload.Presentation == "pull" {
		return nil
	}
	return insertSessionMessageProjectionTx(ctx, tx, scope, eventID, "user", string(payload.Message), now)
}

func readReceivedInterAgentMessageTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, deliveryID string) (string, string, bool, error) {
	var eventID string
	var payloadJSON string
	err := tx.QueryRow(ctx,
		`SELECT event_id, payload_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND type = 'agent.thread_message_received'
		    AND payload_json::jsonb ->> 'delivery_id' = $4
		  ORDER BY sequence ASC
		  LIMIT 1
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		deliveryID,
	).Scan(&eventID, &payloadJSON)
	if dbconnect.IsNoRows(err) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return eventID, payloadJSON, true, nil
}

func verifyInterAgentDeliveryReplay(existingPayloadJSON string, next interAgentMessagePayload) error {
	var existing interAgentMessagePayload
	if err := json.Unmarshal([]byte(existingPayloadJSON), &existing); err != nil {
		return status.Error(codes.AlreadyExists, "inter-agent delivery replay conflicts with malformed existing payload")
	}
	existingMessage, err := publicInterAgentMessageJSON(existing.Message)
	if err != nil {
		return status.Error(codes.AlreadyExists, "inter-agent delivery replay conflicts with malformed existing message")
	}
	nextMessage, err := publicInterAgentMessageJSON(next.Message)
	if err != nil {
		return err
	}
	if existing.DeliveryID != next.DeliveryID ||
		existing.SourceThreadID != next.SourceThreadID ||
		existing.SourceToolUseEventID != next.SourceToolUseEventID ||
		normalizeJSONForCompare(existingMessage) != normalizeJSONForCompare(nextMessage) {
		return status.Error(codes.AlreadyExists, "inter-agent delivery idempotency conflict")
	}
	return nil
}

func publicSentInterAgentEventPayloadTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	raw string,
) (string, error) {
	var payload map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &payload) != nil || payload == nil {
		return "", status.Error(codes.InvalidArgument, "sent inter-agent event payload must be an object")
	}
	message, ok := payload["message"]
	if !ok {
		return "", status.Error(codes.InvalidArgument, "sent inter-agent event payload requires message")
	}
	publicMessage, err := publicInterAgentMessageJSON(message)
	if err != nil {
		return "", err
	}
	var targetThreadID string
	if json.Unmarshal(payload["target_thread_id"], &targetThreadID) != nil || targetThreadID == "" {
		return "", status.Error(codes.InvalidArgument, "sent inter-agent event payload requires target thread id")
	}
	targetTaskName, err := sessionThreadCallableTaskNameTx(ctx, tx, scope, targetThreadID)
	if err != nil {
		return "", err
	}
	targetTaskNameJSON, err := json.Marshal(nullableJSONString(targetTaskName))
	if err != nil {
		return "", err
	}
	payload["target_task_name"] = targetTaskNameJSON
	payload["message"] = publicMessage
	projected, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(projected), nil
}

func publicInterAgentMessageJSON(raw json.RawMessage) (json.RawMessage, error) {
	var message map[string]json.RawMessage
	if json.Unmarshal(raw, &message) != nil || message == nil {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message must be an object")
	}
	if content, exists := message["content"]; exists {
		if err := validatePublicInterAgentContent(content); err != nil {
			return nil, err
		}
		return raw, nil
	}
	partsJSON, exists := message["parts"]
	if !exists {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message requires content or Runtime parts")
	}
	var parts []map[string]json.RawMessage
	if !jsonArray(partsJSON) || json.Unmarshal(partsJSON, &parts) != nil {
		return nil, status.Error(codes.InvalidArgument, "inter-agent Runtime message parts must be an array")
	}
	content := make([]map[string]string, 0, len(parts))
	for _, part := range parts {
		partType, ok := requiredJSONString(part, "type")
		if !ok || partType != "text" {
			return nil, status.Error(codes.InvalidArgument, "inter-agent Runtime message parts must be text blocks")
		}
		text, ok := requiredJSONString(part, "text")
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "inter-agent Runtime text part requires text")
		}
		content = append(content, map[string]string{"type": "text", "text": text})
	}
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	message["content"] = contentJSON
	projected, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	return projected, nil
}

func validatePublicInterAgentContent(raw json.RawMessage) error {
	var blocks []map[string]json.RawMessage
	if !jsonArray(raw) || json.Unmarshal(raw, &blocks) != nil {
		return status.Error(codes.InvalidArgument, "inter-agent public message content must be an array")
	}
	for _, block := range blocks {
		blockType, ok := requiredJSONString(block, "type")
		if !ok {
			return status.Error(codes.InvalidArgument, "inter-agent public content block requires type")
		}
		switch blockType {
		case "text":
			if !onlyJSONFields(block, "type", "text") {
				return status.Error(codes.InvalidArgument, "inter-agent public text block has unsupported fields")
			}
			if _, ok := requiredJSONString(block, "text"); !ok {
				return status.Error(codes.InvalidArgument, "inter-agent public text block requires text")
			}
		case "image":
			if !onlyJSONFields(block, "type", "source") || validatePublicInterAgentSource(block["source"], false) != nil {
				return status.Error(codes.InvalidArgument, "inter-agent public image block is invalid")
			}
		case "document":
			if !onlyJSONFields(block, "type", "source", "context", "title") ||
				!optionalNullableJSONString(block, "context") ||
				!optionalNullableJSONString(block, "title") ||
				validatePublicInterAgentSource(block["source"], true) != nil {
				return status.Error(codes.InvalidArgument, "inter-agent public document block is invalid")
			}
		default:
			return status.Error(codes.InvalidArgument, "inter-agent public content block type is unsupported")
		}
	}
	return nil
}

func validatePublicInterAgentSource(raw json.RawMessage, document bool) error {
	var source map[string]json.RawMessage
	if json.Unmarshal(raw, &source) != nil || source == nil {
		return errors.New("content source must be an object")
	}
	sourceType, ok := requiredJSONString(source, "type")
	if !ok {
		return errors.New("content source requires type")
	}
	switch sourceType {
	case "base64":
		if !onlyJSONFields(source, "type", "data", "media_type") {
			return errors.New("base64 content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "data"); !ok {
			return errors.New("base64 content source requires data")
		}
		if _, ok := requiredJSONString(source, "media_type"); !ok {
			return errors.New("base64 content source requires media type")
		}
	case "url":
		if !onlyJSONFields(source, "type", "url") {
			return errors.New("URL content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "url"); !ok {
			return errors.New("URL content source requires URL")
		}
	case "file":
		if !onlyJSONFields(source, "type", "file_id") {
			return errors.New("file content source has unsupported fields")
		}
		if _, ok := requiredJSONString(source, "file_id"); !ok {
			return errors.New("file content source requires file id")
		}
	case "text":
		if !document || !onlyJSONFields(source, "type", "data", "media_type") {
			return errors.New("plain-text source is only valid for documents")
		}
		if _, ok := requiredJSONString(source, "data"); !ok {
			return errors.New("plain-text document source requires data")
		}
		mediaType, ok := requiredJSONString(source, "media_type")
		if !ok || mediaType != "text/plain" {
			return errors.New("plain-text document source requires text/plain media type")
		}
	default:
		return errors.New("content source type is unsupported")
	}
	return nil
}

func jsonArray(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '['
}

func requiredJSONString(object map[string]json.RawMessage, field string) (string, bool) {
	var value string
	raw, exists := object[field]
	if !exists || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}

func optionalNullableJSONString(object map[string]json.RawMessage, field string) bool {
	raw, exists := object[field]
	if !exists || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return true
	}
	var value string
	return json.Unmarshal(raw, &value) == nil
}

func onlyJSONFields(object map[string]json.RawMessage, fields ...string) bool {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	for field := range object {
		if _, ok := allowed[field]; !ok {
			return false
		}
	}
	return true
}

func commitApprovalReviewInputTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, payloadJSON string, now time.Time) error {
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if threadScope.role != "approval_reviewer" || threadScope.visibility != "internal" {
		return status.Error(codes.FailedPrecondition, "approval review input must target an internal reviewer thread")
	}
	messageJSON := approvalReviewMessageJSON(payloadJSON)
	return insertSessionMessageProjectionTx(ctx, tx, scope, id.New("approval_review_"), "user", messageJSON, now)
}

func threadReceivableTx(threadScope threadMutationScope) bool {
	switch threadScope.status {
	case "closed_for_runtime", "terminated", "failed":
		return false
	default:
		return true
	}
}

func normalizeJSONForCompare(raw json.RawMessage) string {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return string(raw)
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return string(raw)
	}
	return string(normalized)
}

func approvalReviewMessageJSON(payloadJSON string) string {
	var payload struct {
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err == nil && len(payload.Message) > 0 && json.Valid(payload.Message) {
		return string(payload.Message)
	}
	return payloadJSON
}

func recordPendingToolConfirmationDecisionTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, payloadJSON string, now time.Time) error {
	var payload struct {
		ToolUseID      string `json:"tool_use_id"`
		ToolUseEventID string `json:"tool_use_event_id"`
		Result         string `json:"result"`
		Decision       string `json:"decision"`
		DenyMessage    string `json:"deny_message"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return status.Error(codes.FailedPrecondition, "tool confirmation event is not committable")
	}
	toolUseID := defaultString(payload.ToolUseEventID, payload.ToolUseID)
	decision := defaultString(payload.Decision, payload.Result)
	if toolUseID == "" || (decision != "allow" && decision != "deny") || (decision == "allow" && payload.DenyMessage != "") {
		return status.Error(codes.FailedPrecondition, "tool confirmation event is not committable")
	}
	var denyMessage any
	if decision == "deny" && payload.DenyMessage != "" {
		denyMessage = payload.DenyMessage
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_pending_tool_uses
		    SET decision = $5,
		        deny_message = $6,
		        status = 'resolving',
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND tool_use_event_id = $4
		    AND kind = 'approval'
		    AND status = 'resolving'
		    AND (decision IS NULL OR (decision = $5 AND deny_message IS NOT DISTINCT FROM $6))`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		toolUseID,
		decision,
		denyMessage,
		now,
	)
	if err != nil {
		return err
	}
	if rowsAffected(result) {
		return nil
	}
	return status.Error(codes.FailedPrecondition, "tool confirmation pending row is not committable")
}
