package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const sandboxToolCancelMaxAttempts = 5

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
	if commitInputsKindUsesRuntimeInbox(inputKind) {
		var err error
		inboxStatus, err = lockAndValidateRuntimeInboxCommitTx(ctx, tx, request, inputKind)
		if err != nil {
			return nil, err
		}
		if inputKind == "interrupt_control" {
			if err := cancelInterruptedSandboxExecutionsTx(
				ctx,
				tx,
				request.GetScope(),
				request.GetSandboxExecutionToolUseEventIds(),
				now,
			); err != nil {
				return nil, err
			}
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
	if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
		return nil, err
	}
	if inputKind == "tool_confirmation" {
		if err := settleToolConfirmationEventsTx(ctx, tx, request.GetScope(), request.GetEventIds(), now); err != nil {
			return nil, err
		}
	}
	receipt, err := commitInputDraftsTx(
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
		return nil, err
	}
	if receipt == nil {
		receipt = &bridgev1.DeclarationReceipt{
			SessionThreadId: request.GetScope().GetSessionThreadId(),
			OperationKind:   bridgeOpCommitInputs,
			SourceKind:      inputKind,
			SourceId:        key,
		}
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
		receipt.PendingToolDeltaJson, err = cancelInterruptedPendingToolUsesTx(
			ctx,
			tx,
			request.GetScope(),
			request.GetEventIds()[0],
			request.GetPendingToolCancellations(),
			now,
		)
		if err != nil {
			return nil, err
		}
	}
	return receipt, nil
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

func cancelInterruptedPendingToolUsesTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	interruptEventID string,
	cancellations []*bridgev1.PendingToolCancellationDraft,
	now time.Time,
) ([]string, error) {
	expected, err := pendingApprovalToolUseIDsTx(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	provided := make([]string, 0, len(cancellations))
	for _, cancellation := range cancellations {
		provided = append(provided, cancellation.GetToolUseEventId())
	}
	sort.Strings(provided)
	if !sameBridgeStringSlice(expected, provided) {
		return nil, status.Error(codes.FailedPrecondition, "interrupt approval-tool coverage is incomplete")
	}
	deltas := make([]string, 0, len(cancellations))
	for _, cancellation := range cancellations {
		var statusValue string
		if err := tx.QueryRow(ctx,
			`SELECT status
			   FROM session_pending_tool_uses
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND tool_use_event_id = $4
			  FOR UPDATE`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
			cancellation.GetToolUseEventId(),
		).Scan(&statusValue); dbconnect.IsNoRows(err) {
			return nil, status.Error(codes.FailedPrecondition, "interrupted pending tool use is missing")
		} else if err != nil {
			return nil, err
		}
		if statusValue != "pending" && statusValue != "resolving" {
			return nil, status.Error(codes.FailedPrecondition, "interrupted pending tool use is stale")
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
			cancellation.GetToolUseEventId(),
			interruptEventID,
			now,
		)
		if err != nil {
			return nil, err
		}
		if !rowsAffected(result) {
			return nil, status.Error(codes.FailedPrecondition, "interrupted pending tool use is stale")
		}
		delta, err := marshalBridgeJSON(map[string]any{
			"result_event_id":   interruptEventID,
			"runtime_local_id":  cancellation.GetRuntimeLocalId(),
			"status":            "cancelled",
			"tool_use_event_id": cancellation.GetToolUseEventId(),
		})
		if err != nil {
			return nil, err
		}
		deltas = append(deltas, delta)
	}
	return deltas, nil
}

func pendingApprovalToolUseIDsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT p.tool_use_event_id
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
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []string
	for rows.Next() {
		var toolUseEventID string
		if err := rows.Scan(&toolUseEventID); err != nil {
			return nil, err
		}
		result = append(result, toolUseEventID)
	}
	return result, rows.Err()
}

type interruptedSandboxExecution struct {
	toolUseEventID                    string
	executionState                    string
	executionAttemptGeneration        int64
	waitingActivationOperationID      sql.NullString
	waitingMaterializationOperationID sql.NullString
	providerCommandReference          sql.NullString
}

type interruptedLifecycleOperation struct {
	operationID  string
	kind         string
	state        string
	queueJobID   sql.NullString
	queueKind    sql.NullString
	partitionKey sql.NullString
	dedupeKey    sql.NullString
}

func cancelInterruptedSandboxExecutionsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	requestedToolUseEventIDs []string,
	now time.Time,
) error {
	operationIDs, err := sandboxExecutionDependencyIDsTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	operations, err := lockInterruptedLifecycleOperationsTx(ctx, tx, scope.GetWorkspaceId(), operationIDs)
	if err != nil {
		return err
	}
	executions, err := lockInterruptedSandboxExecutionsTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	provided := append([]string(nil), requestedToolUseEventIDs...)
	sort.Strings(provided)
	expected := make([]string, 0, len(executions))
	for _, execution := range executions {
		expected = append(expected, execution.toolUseEventID)
	}
	if !sameBridgeStringSlice(expected, provided) {
		return status.Error(codes.FailedPrecondition, "interrupt sandbox-execution coverage is incomplete")
	}

	for _, execution := range executions {
		switch execution.executionState {
		case "pending", "preparing", "waiting_activation", "waiting_materialization":
			if err := terminalizeInterruptedSandboxExecutionTx(ctx, tx, scope, execution, "cancelled", "Sandbox tool execution was cancelled.", "cancelled", now); err != nil {
				return err
			}
		case "running":
			if !execution.providerCommandReference.Valid || execution.providerCommandReference.String == "" {
				if err := terminalizeInterruptedSandboxExecutionTx(ctx, tx, scope, execution, "sandbox_execution_outcome_unknown", "Sandbox tool execution outcome is unknown.", "error", now); err != nil {
					return err
				}
				continue
			}
			if err := requestSandboxExecutionCancellationTx(ctx, tx, scope, execution, now); err != nil {
				return err
			}
		case "terminal_unconsumed":
			// A durable completion already won; Runtime commits its ordinary Tool Result.
		default:
			return status.Error(codes.FailedPrecondition, "interrupt sandbox execution is not cancellable")
		}
	}
	return abandonUnneededInterruptedLifecycleOperationsTx(ctx, tx, scope, operations, now)
}

func sandboxExecutionDependencyIDsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]string, error) {
	rows, err := tx.Query(ctx,
		`SELECT waiting_activation_operation_id, waiting_materialization_operation_id
		   FROM session_runtime_tool_results
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_kind = 'sandbox_tool' AND execution_state <> 'consumed'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	seen := make(map[string]struct{})
	for rows.Next() {
		var activationID, materializationID sql.NullString
		if err := rows.Scan(&activationID, &materializationID); err != nil {
			return nil, err
		}
		if activationID.Valid {
			seen[activationID.String] = struct{}{}
		}
		if materializationID.Valid {
			seen[materializationID.String] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(seen))
	for operationID := range seen {
		result = append(result, operationID)
	}
	sort.Strings(result)
	return result, nil
}

func lockInterruptedLifecycleOperationsTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, operationIDs []string) ([]interruptedLifecycleOperation, error) {
	operations := make([]interruptedLifecycleOperation, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		operation := interruptedLifecycleOperation{operationID: operationID}
		if err := tx.QueryRow(ctx,
			`SELECT kind, state, queue_job_id, queue_kind, queue_partition_key, queue_dedupe_key
			   FROM sandbox_lifecycle_operations
			  WHERE workspace_id = $1 AND operation_id = $2
			  FOR UPDATE`,
			workspaceID, operationID,
		).Scan(&operation.kind, &operation.state, &operation.queueJobID, &operation.queueKind, &operation.partitionKey, &operation.dedupeKey); dbconnect.IsNoRows(err) {
			return nil, status.Error(codes.FailedPrecondition, "sandbox execution dependency is missing")
		} else if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func lockInterruptedSandboxExecutionsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) ([]interruptedSandboxExecution, error) {
	rows, err := tx.Query(ctx,
		`SELECT tool_use_event_id, execution_state, execution_attempt_generation,
		        waiting_activation_operation_id, waiting_materialization_operation_id,
		        provider_command_reference_json
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
	var executions []interruptedSandboxExecution
	for rows.Next() {
		var execution interruptedSandboxExecution
		if err := rows.Scan(
			&execution.toolUseEventID,
			&execution.executionState,
			&execution.executionAttemptGeneration,
			&execution.waitingActivationOperationID,
			&execution.waitingMaterializationOperationID,
			&execution.providerCommandReference,
		); err != nil {
			return nil, err
		}
		executions = append(executions, execution)
	}
	return executions, rows.Err()
}

func terminalizeInterruptedSandboxExecutionTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	execution interruptedSandboxExecution,
	errorKind string,
	safeMessage string,
	resultStatus string,
	now time.Time,
) error {
	resultJSON, err := marshalBridgeJSON(map[string]any{
		"error":  map[string]string{"kind": errorKind, "message": safeMessage},
		"status": resultStatus,
	})
	if err != nil {
		return err
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET execution_state = 'terminal_unconsumed', result_json = $6, result_digest = $7,
		        waiting_activation_operation_id = NULL, waiting_materialization_operation_id = NULL,
		        authorized_binding_revision = NULL, authorized_provider_resource_id = NULL,
		        preparation_deadline = NULL, provider_command_reference_json = NULL,
		        cancel_requested_at = NULL, cancel_state = NULL, cancel_submitted_at = NULL,
		        updated_at = $8
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND execution_attempt_generation = $5
		    AND execution_state <> 'consumed'`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID,
		execution.executionAttemptGeneration, resultJSON, sha256Hex(resultJSON), now.UTC(),
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.Aborted, "sandbox execution changed during interrupt settlement")
	}
	return nil
}

func requestSandboxExecutionCancellationTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, execution interruptedSandboxExecution, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_runtime_tool_results
		    SET cancel_requested_at = COALESCE(cancel_requested_at, $6),
		        cancel_state = COALESCE(cancel_state, 'pending'), updated_at = $6
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND tool_use_event_id = $4 AND execution_attempt_generation = $5
		    AND execution_state = 'running'
		    AND provider_command_reference_json IS NOT NULL`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID,
		execution.executionAttemptGeneration, now.UTC(),
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.Aborted, "sandbox execution changed during interrupt settlement")
	}
	payload, err := marshalBridgeJSON(map[string]string{
		"workspace_id": scope.GetWorkspaceId(), "session_id": scope.GetSessionId(),
		"session_thread_id": scope.GetSessionThreadId(), "tool_use_event_id": execution.toolUseEventID,
	})
	if err != nil {
		return err
	}
	workspaceID := workspace.ID(scope.GetWorkspaceId())
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: workspaceID, Kind: queue.KindSandboxToolCancel,
		PartitionKey:   queue.FormatSandboxCancelPartitionKey(workspaceID, scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID),
		DedupeKey:      queue.FormatSandboxToolCancelDedupeKey(workspaceID, scope.GetSessionId(), scope.GetSessionThreadId(), execution.toolUseEventID),
		PayloadVersion: 1, PayloadJSON: []byte(payload), MaxAttempts: sandboxToolCancelMaxAttempts, Now: now,
	})
	return err
}

func abandonUnneededInterruptedLifecycleOperationsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	operations []interruptedLifecycleOperation,
	now time.Time,
) error {
	for _, operation := range operations {
		var waiterCount int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM session_runtime_tool_results
			  WHERE workspace_id = $1 AND session_id = $2 AND tool_kind = 'sandbox_tool'
			    AND execution_state <> 'consumed'
			    AND (waiting_activation_operation_id = $3 OR waiting_materialization_operation_id = $3)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), operation.operationID,
		).Scan(&waiterCount); err != nil {
			return err
		}
		if waiterCount != 0 {
			continue
		}
		abandonable := (operation.kind == "create" || operation.kind == "start" || operation.kind == "replace") &&
			(operation.state == "pending" || operation.state == "waiting_artifact")
		abandonable = abandonable || operation.kind == "materialize" &&
			(operation.state == "pending" || operation.state == "waiting_activation")
		if !abandonable {
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE sandbox_lifecycle_operations
			    SET state = 'abandoned', completed_at = $3, updated_at = $3
			  WHERE workspace_id = $1 AND operation_id = $2 AND state = $4`,
			scope.GetWorkspaceId(), operation.operationID, now.UTC(), operation.state,
		); err != nil {
			return err
		}
		if operation.queueJobID.Valid && operation.queueKind.Valid && operation.partitionKey.Valid && operation.dedupeKey.Valid {
			if _, err := queue.CancelTx(ctx, tx, queue.TargetedCancelRequest{
				WorkspaceID: workspace.ID(scope.GetWorkspaceId()), JobID: operation.queueJobID.String,
				Kind: operation.queueKind.String, PartitionKey: operation.partitionKey.String,
				DedupeKey: operation.dedupeKey.String, Now: now,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func cancellationDraftNamesPendingTool(draft *bridgev1.RuntimeMessageDraft, toolUseEventID string) bool {
	if draft == nil {
		return false
	}
	for _, part := range draft.GetParts() {
		if part == nil || part.GetPartKind() != "tool" {
			continue
		}
		var payload struct {
			Type           string `json:"type"`
			ToolUseEventID string `json:"toolUseEventId"`
			State          struct {
				Status string `json:"status"`
			} `json:"state"`
		}
		if err := json.Unmarshal([]byte(part.GetPartJson()), &payload); err != nil {
			continue
		}
		if payload.Type == "tool" &&
			payload.ToolUseEventID == toolUseEventID &&
			payload.State.Status == "cancelled" {
			return true
		}
	}
	return false
}

func validatePendingToolCancellationDrafts(
	interruptEventID string,
	cancellations []*bridgev1.PendingToolCancellationDraft,
	drafts []*bridgev1.RuntimeMessageDraft,
) error {
	draftsByLocalID := make(map[string]*bridgev1.RuntimeMessageDraft, len(drafts))
	for _, draft := range drafts {
		if draft == nil || draft.GetRuntimeLocalId() == "" {
			return status.Error(codes.InvalidArgument, "pending tool cancellation draft is missing")
		}
		if _, exists := draftsByLocalID[draft.GetRuntimeLocalId()]; exists {
			return status.Error(codes.InvalidArgument, "pending tool cancellation draft is duplicated")
		}
		draftsByLocalID[draft.GetRuntimeLocalId()] = draft
	}
	seenTools := make(map[string]struct{}, len(cancellations))
	referencedDrafts := make(map[string]struct{}, len(drafts))
	for _, cancellation := range cancellations {
		if cancellation == nil || cancellation.GetToolUseEventId() == "" || cancellation.GetRuntimeLocalId() == "" {
			return status.Error(codes.InvalidArgument, "pending tool cancellation is invalid")
		}
		if _, exists := seenTools[cancellation.GetToolUseEventId()]; exists {
			return status.Error(codes.InvalidArgument, "pending tool cancellation is duplicated")
		}
		seenTools[cancellation.GetToolUseEventId()] = struct{}{}
		draft, ok := draftsByLocalID[cancellation.GetRuntimeLocalId()]
		if !ok ||
			draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION ||
			draft.GetSourceEventId() != interruptEventID ||
			!cancellationDraftNamesPendingTool(draft, cancellation.GetToolUseEventId()) {
			return status.Error(codes.InvalidArgument, "pending tool cancellation draft is missing")
		}
		referencedDrafts[cancellation.GetRuntimeLocalId()] = struct{}{}
	}
	if len(referencedDrafts) != len(draftsByLocalID) {
		return status.Error(codes.InvalidArgument, "pending tool cancellation draft is unmatched")
	}
	return nil
}

func validateCommitInputsRequest(inputKind string, request *bridgev1.CommitInputsRequest) error {
	switch inputKind {
	case "messages":
		if len(request.GetEventIds()) == 0 || len(request.GetDrafts()) != len(request.GetEventIds()) {
			return status.Error(codes.InvalidArgument, "message commit requires one user draft per event")
		}
		if len(request.GetPendingToolCancellations()) != 0 || len(request.GetSandboxExecutionToolUseEventIds()) != 0 {
			return status.Error(codes.InvalidArgument, "message commit cannot cancel pending tools")
		}
		return nil
	case "interrupt_control":
		if len(request.GetEventIds()) != 1 {
			return status.Error(codes.InvalidArgument, "interrupt commit requires one event id")
		}
		if err := validateInterruptSandboxExecutionIDs(request); err != nil {
			return err
		}
		return validatePendingToolCancellationDrafts(
			request.GetEventIds()[0],
			request.GetPendingToolCancellations(),
			request.GetDrafts(),
		)
	case "tool_confirmation":
		if len(request.GetEventIds()) != 1 || len(request.GetDrafts()) != 1 || len(request.GetPendingToolCancellations()) != 0 || len(request.GetSandboxExecutionToolUseEventIds()) != 0 {
			return status.Error(codes.InvalidArgument, "tool confirmation commit requires one approval draft")
		}
		return nil
	case "agent_mail":
		if len(request.GetEventIds()) != 1 || len(request.GetDrafts()) != 1 || len(request.GetPendingToolCancellations()) != 0 || len(request.GetSandboxExecutionToolUseEventIds()) != 0 {
			return status.Error(codes.InvalidArgument, "agent mail commit requires one mail draft")
		}
		return nil
	case "approval_review":
		if len(request.GetEventIds()) != 1 || len(request.GetDrafts()) != 1 || len(request.GetPendingToolCancellations()) != 0 || len(request.GetSandboxExecutionToolUseEventIds()) != 0 {
			return status.Error(codes.InvalidArgument, "approval review commit requires one reviewer draft")
		}
		return nil
	case "rejection":
		if len(request.GetEventIds()) == 0 || len(request.GetDrafts()) != len(request.GetEventIds()) || len(request.GetPendingToolCancellations()) != 0 || len(request.GetSandboxExecutionToolUseEventIds()) != 0 {
			return status.Error(codes.InvalidArgument, "rejection commit requires one rejection draft per event")
		}
		return nil
	default:
		return status.Error(codes.InvalidArgument, "unsupported commit inputs kind")
	}
}

func validateInterruptSandboxExecutionIDs(request *bridgev1.CommitInputsRequest) error {
	seen := make(map[string]struct{}, len(request.GetSandboxExecutionToolUseEventIds()))
	for _, toolUseEventID := range request.GetSandboxExecutionToolUseEventIds() {
		if toolUseEventID == "" {
			return status.Error(codes.InvalidArgument, "interrupt sandbox execution identity is invalid")
		}
		if _, ok := seen[toolUseEventID]; ok {
			return status.Error(codes.InvalidArgument, "interrupt sandbox execution identity is duplicated")
		}
		seen[toolUseEventID] = struct{}{}
	}
	for _, cancellation := range request.GetPendingToolCancellations() {
		if _, ok := seen[cancellation.GetToolUseEventId()]; ok {
			return status.Error(codes.InvalidArgument, "interrupt tool ownership sets overlap")
		}
	}
	return nil
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
