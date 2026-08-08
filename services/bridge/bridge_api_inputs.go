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
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	sandboxToolCancelMaxAttempts = 5
)

// This file owns the Bridge inputs protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) CommitInputs(ctx context.Context, request *bridgev1.CommitInputsRequest) (*bridgev1.CommitInputsResponse, error) {
	logStartedAt := time.Now()
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
	var observation declarationApplicationObservation
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_inputs", func(tx *dbconnect.Tx) error {
		// Serialize replay lookup before validating the current binding. A replacement
		// binding may recover an ACK lost by its predecessor, while concurrent writers
		// for the same session must still observe one committed operation.
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
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
			observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		receipt, err = commitInputDeclarationTx(ctx, tx, request, inputKind, key, now)
		if err != nil {
			return err
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
		observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
		return err
	}); err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitInputs,
		inputKind,
		key,
		declarationDigest,
		ack,
		observation,
	)
	if inputKind == "interrupt_control" && receipt != nil {
		event := "thread_interrupt_completed"
		if observation.Disposition == bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY {
			event = "thread_interrupt_stale"
		}
		s.logCommittedThreadInterrupt(ctx, request, event, len(receipt.GetInterruptToolProjections()), time.Since(logStartedAt).Milliseconds())
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

func commitInputDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitInputsRequest,
	inputKind string,
	key string,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	inboxStatus := ""
	var approvalReviewEvent *bridgev1.DurableEventStamp
	if commitInputsKindUsesRuntimeInbox(inputKind) {
		var err error
		inboxStatus, err = lockAndValidateRuntimeInboxCommitTx(ctx, tx, request, inputKind)
		if err != nil {
			return nil, err
		}
	}
	if inboxStatus == "committed" {
		return nil, status.Error(codes.FailedPrecondition, "committed runtime input is missing idempotency state")
	}
	if expectedEventType := commitInputEventType(inputKind); expectedEventType != "" {
		if err := requireCommitInputEventTypesTx(ctx, tx, request.GetScope(), request.GetEventIds(), expectedEventType); err != nil {
			return nil, err
		}
	}
	if inputKind == "approval_review" {
		if err := requireApprovalReviewerInputTargetTx(ctx, tx, request.GetScope()); err != nil {
			return nil, err
		}
		var err error
		approvalReviewEvent, err = createApprovalReviewInputEventTx(
			ctx,
			tx,
			request.GetScope(),
			key,
			request.GetEventIds()[0],
			now,
		)
		if err != nil {
			return nil, err
		}
	}
	if inputKind == "agent_mail" {
		if err := requireAgentMailInputTargetTx(ctx, tx, request.GetScope()); err != nil {
			return nil, err
		}
	}
	if commitInputsKindUsesRuntimeInbox(inputKind) {
		if err := markRuntimeInboxCommittedTx(ctx, tx, request.GetScope(), key, now); err != nil {
			return nil, err
		}
	}
	if inputKind != "approval_review" {
		if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
			return nil, err
		}
	}
	if inputKind == "tool_confirmation" {
		if err := settleToolConfirmationEventsTx(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
			return nil, err
		}
	}
	receipt, err := commitInputCreatesTx(
		ctx,
		tx,
		request.GetScope(),
		inputKind,
		key,
		request.GetEventIds(),
		request.GetMessageCreates(),
		now,
	)
	if err != nil {
		return nil, err
	}
	if receipt == nil {
		receipt = &bridgev1.DeclarationReceipt{
			SessionThreadId: request.GetScope().GetSessionThreadId(),
			OperationKind:   bridgeOpCommitInputs,
			SourceKind:      inputKind,
			OperationId:     key,
		}
	}
	if approvalReviewEvent != nil {
		receipt.Events = []*bridgev1.DurableEventStamp{approvalReviewEvent}
	}
	if inputKind == "messages" {
		receipt.PendingAttachmentDeltaJson, err = loadCommittedInputAttachmentDeltaTx(
			ctx,
			tx,
			request.GetScope(),
			request.GetEventIds(),
		)
		if err != nil {
			return nil, err
		}
	}
	if inputKind == "interrupt_control" {
		receipt.InterruptToolProjections, err = settleInterruptedThreadToolsTx(
			ctx,
			tx,
			request.GetScope(),
			request.GetEventIds()[0],
			now,
		)
		if err != nil {
			return nil, err
		}
	}
	return receipt, nil
}

func createApprovalReviewInputEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	eventID string,
	now time.Time,
) (*bridgev1.DurableEventStamp, error) {
	if eventID == "" {
		return nil, status.Error(codes.InvalidArgument, "approval review event id is invalid")
	}
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":             "approval_review.input",
		"runtime_input_id": runtimeInputID,
	})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'approval_review.input', $6, 'internal', false, $7, $8, $8, $8)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		payloadJSON,
		runtimeInputID,
		now,
	); err != nil {
		return nil, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, "internal", false, now); err != nil {
		return nil, err
	}
	return &bridgev1.DurableEventStamp{
		SessionThreadId: scope.GetSessionThreadId(),
		EventId:         eventID,
		EventSequence:   sequence,
		Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
	}, nil
}

func commitInputsKindUsesRuntimeInbox(inputKind string) bool {
	switch inputKind {
	case "messages", "interrupt_control", "tool_confirmation", "agent_mail", "rejection":
		return true
	default:
		return false
	}
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
		if eventType != expectedType && (expectedType != "user.interrupt" || eventType != childInterruptRequestedEventType) {
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

func validateCommitInputsRequest(inputKind string, request *bridgev1.CommitInputsRequest) error {
	switch inputKind {
	case "messages":
		if len(request.GetEventIds()) == 0 || len(request.GetMessageCreates()) != len(request.GetEventIds()) {
			return status.Error(codes.InvalidArgument, "message commit requires one message create per event")
		}
		return nil
	case "interrupt_control":
		if len(request.GetEventIds()) != 1 || len(request.GetMessageCreates()) != 0 {
			return status.Error(codes.InvalidArgument, "interrupt commit requires one event id")
		}
		return nil
	case "tool_confirmation":
		if len(request.GetEventIds()) != 1 || len(request.GetMessageCreates()) != 1 {
			return status.Error(codes.InvalidArgument, "tool confirmation commit requires one approval message create")
		}
		return nil
	case "agent_mail":
		if len(request.GetEventIds()) != 1 || len(request.GetMessageCreates()) != 1 {
			return status.Error(codes.InvalidArgument, "agent mail commit requires one mail message create")
		}
		return nil
	case "approval_review":
		if len(request.GetEventIds()) != 1 || len(request.GetMessageCreates()) != 1 {
			return status.Error(codes.InvalidArgument, "approval review commit requires one reviewer message create")
		}
		return nil
	case "rejection":
		if len(request.GetEventIds()) == 0 || len(request.GetMessageCreates()) != len(request.GetEventIds()) {
			return status.Error(codes.InvalidArgument, "rejection commit requires one rejection message create per event")
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "unsupported commit inputs kind")
	}
}

func commitInputEventType(inputKind string) string {
	switch inputKind {
	case "messages":
		return "user.message"
	case "interrupt_control":
		return "user.interrupt"
	case "tool_confirmation":
		return "user.tool_confirmation"
	case "agent_mail":
		return "agent.thread_message_received"
	default:
		return ""
	}
}

func requireApprovalReviewerInputTargetTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if threadScope.role != "approval_reviewer" || threadScope.visibility != "internal" {
		return status.Error(codes.FailedPrecondition, "approval review input must target an internal reviewer thread")
	}
	return nil
}

func requireAgentMailInputTargetTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if threadScope.role != "main" && threadScope.role != "subagent" {
		return status.Error(codes.FailedPrecondition, "agent mail must target a main or sub-agent thread")
	}
	if !threadReceivableTx(threadScope) {
		return status.Error(codes.FailedPrecondition, "agent mail target is not receivable")
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
