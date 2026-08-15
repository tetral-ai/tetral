package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge tasks protocol-family boundary.

// CommitTaskNotificationResult authors the conversation projection for a
// terminal background task. Sandbox Service owns terminal task settlement and
// the task-notification inbox birth; Bridge verifies the Runtime declaration
// against that stored terminal result before committing one event and message.
func (s *PostgreSQLBridgeAPIStore) CommitTaskNotificationResult(ctx context.Context, request *bridgev1.CommitTaskNotificationResultRequest) (*bridgev1.CommitTaskNotificationResultResponse, error) {
	if request.GetRuntimeInputId() == "" || request.GetDisposition() == bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "invalid task notification result request")
	}
	taskID := strings.TrimPrefix(request.GetRuntimeInputId(), "task_notification:")
	if taskID == request.GetRuntimeInputId() || taskID == "" || request.GetRuntimeInputId() != queue.FormatTaskNotificationRuntimeInputID(taskID) {
		return nil, status.Error(codes.InvalidArgument, "invalid task notification target")
	}
	key := taskID + ":" + request.GetRuntimeInputId()
	sourceID := stableRuntimeID("task_notification", request.GetRuntimeInputId(), taskID)
	declarationDigest, err := taskNotificationDeclarationDigest(request)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var duplicate bool
	var outcome string
	var assignedContextSequences []int64
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_task_notification_result", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpCommitTaskNotificationResult, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != declarationDigest {
				return status.Error(codes.AlreadyExists, "task notification idempotency conflict")
			}
			if existing.AckStatus == bridgeAckRejected && existing.ErrorCode == "task_notification_stale" {
				outcome = "stale"
				return nil
			}
			if existing.AckStatus == bridgeAckRejected && taskNotificationRejectionCode(existing.ErrorCode) {
				outcome = "rejected"
				return nil
			}
			return status.Error(codes.FailedPrecondition, "task notification operation is invalid")
		}
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitTaskNotificationResult,
			"task_notification",
			sourceID,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest != declarationDigest || existing.ReceiptJSON == "" {
				return status.Error(codes.AlreadyExists, "task notification idempotency conflict")
			}
			assignedContextSequences, err = unmarshalTaskNotificationReplay(existing.ReceiptJSON)
			if err != nil {
				return err
			}
			duplicate = true
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		facts, err := lockTaskNotificationSettlementFactsTx(ctx, tx, request, taskID)
		if err != nil {
			return err
		}
		if facts.SourceEventType != "agent.tool_use" && facts.SourceEventType != "agent.mcp_tool_use" {
			if err := rejectTaskNotificationDeclarationTx(ctx, tx, request, key, declarationDigest,
				"task_notification.durable_source", "task_notification_result_invalid", now); err != nil {
				return err
			}
			outcome = "rejected"
			return nil
		}
		if request.GetDisposition() == bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_DEFER {
			if err := deferTaskNotificationResultTx(ctx, tx, request.GetScope(), request.GetRuntimeInputId(), now); err != nil {
				return err
			}
			outcome = "deferred"
			return nil
		}
		if request.GetDisposition() == bridgev1.RuntimeInputDisposition_RUNTIME_INPUT_DISPOSITION_REJECT {
			return status.Error(codes.FailedPrecondition, "task notification rejection is not permitted for a valid durable result")
		}
		if facts.InboxStatus == "queued" || facts.InboxStatus == "parked" {
			closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId())
			if err != nil {
				return err
			}
			if closing || facts.ThreadStatus == "closed_for_runtime" {
				if err := deferTaskNotificationResultTx(ctx, tx, request.GetScope(), request.GetRuntimeInputId(), now); err != nil {
					return err
				}
				outcome = "deferred"
				return nil
			}
		}
		if facts.TerminalEventID.Valid {
			if facts.InboxStatus == "committed" {
				if err := insertTaskNotificationStaleOperationTx(ctx, tx, request, key, declarationDigest, now); err != nil {
					return err
				}
				outcome = "stale"
				return nil
			}
			if facts.InboxStatus != "delivering" && facts.InboxStatus != "accepted" {
				return status.Error(codes.FailedPrecondition, "task notification input is not deliverable")
			}
			result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET status='committed',
				committed_at=COALESCE(committed_at,$5),updated_at=$5
				WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3
				  AND input_kind='task_notification' AND status IN ('delivering','accepted')
				  AND session_thread_id=$4`,
				request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetRuntimeInputId(),
				request.GetScope().GetSessionThreadId(), now,
			)
			if err != nil {
				return err
			}
			if !rowsAffected(result) {
				return status.Error(codes.Aborted, "task notification Inbox authority changed during stale settlement")
			}
			if err := insertTaskNotificationStaleOperationTx(ctx, tx, request, key, declarationDigest, now); err != nil {
				return err
			}
			outcome = "stale"
			return nil
		}
		if facts.InboxStatus != "delivering" && facts.InboxStatus != "accepted" {
			return status.Error(codes.FailedPrecondition, "task notification input is not deliverable")
		}
		if !validBackgroundTaskTerminalStatus(facts.TaskStatus) || facts.StoredResultJSON == "" {
			if err := rejectTaskNotificationDeclarationTx(ctx, tx, request, key, declarationDigest,
				"task_notification.durable_result", "task_notification_result_invalid", now); err != nil {
				return err
			}
			outcome = "rejected"
			return nil
		}
		expectedResultJSON, err := canonicalTaskNotificationPayloadJSON(
			taskID, facts.SourceToolUseEventID, facts.TaskStatus, facts.StoredResultJSON,
		)
		if err != nil {
			if rejectErr := rejectTaskNotificationDeclarationTx(ctx, tx, request, key, declarationDigest,
				"task_notification.durable_result", "task_notification_result_invalid", now); rejectErr != nil {
				return rejectErr
			}
			outcome = "rejected"
			return nil
		}
		settled, assignedContextSequence, err := commitTaskNotificationDeclarationTx(
			ctx,
			tx,
			request,
			taskID,
			sourceID,
			facts,
			expectedResultJSON,
			now,
		)
		if err != nil {
			return err
		}
		if !settled {
			if err := insertTaskNotificationStaleOperationTx(ctx, tx, request, key, declarationDigest, now); err != nil {
				return err
			}
			outcome = "stale"
			return nil
		}
		result, err := tx.Exec(ctx,
			`UPDATE session_runtime_inbox
			    SET status = 'committed', committed_at = COALESCE(committed_at, $8), updated_at = $8
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
			    AND runtime_input_id = $4 AND binding_id = $5 AND binding_generation = $6
			    AND target_pod_uid = $7 AND input_kind = 'task_notification'
			    AND status IN ('delivering', 'accepted')`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(),
			request.GetRuntimeInputId(), request.GetScope().GetBinding().GetBindingId(),
			request.GetScope().GetBinding().GetBindingGeneration(), request.GetScope().GetBinding().GetTargetPodUid(), now,
		)
		if err != nil {
			return err
		}
		if !rowsAffected(result) {
			return status.Error(codes.Aborted, "task notification Inbox authority changed during commit")
		}
		assignedContextSequences = []int64{assignedContextSequence}
		receiptJSON, err := marshalTaskNotificationReplay(assignedContextSequences)
		if err != nil {
			return err
		}
		return insertTaskNotificationDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			sourceID,
			request.GetRuntimeInputId(),
			declarationDigest,
			receiptJSON,
			now,
		)
	}); err != nil {
		return nil, err
	}
	if outcome == "stale" {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Stale{Stale: &bridgev1.CommitTaskNotificationResultStale{}}}, nil
	}
	if outcome == "deferred" {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Deferred{Deferred: &bridgev1.CommitTaskNotificationResultDeferred{}}}, nil
	}
	if outcome == "rejected" {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Rejected{Rejected: &bridgev1.CommitTaskNotificationResultRejected{
			Reason: bridgev1.TaskNotificationRejectionReason_TASK_NOTIFICATION_REJECTION_REASON_DURABLE_RESULT_INVALID,
		}}}, nil
	}
	if len(assignedContextSequences) == 0 {
		return nil, status.Error(codes.Internal, "task notification result is unavailable")
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitTaskNotificationResult,
		"task_notification",
		sourceID,
		declarationDigest,
		duplicate,
		observation,
	)
	if !observation.Current {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Stale{Stale: &bridgev1.CommitTaskNotificationResultStale{}}}, nil
	}
	if duplicate {
		return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Duplicate{Duplicate: &bridgev1.CommitTaskNotificationResultDuplicate{AssignedContextSequences: assignedContextSequences}}}, nil
	}
	return &bridgev1.CommitTaskNotificationResultResponse{Outcome: &bridgev1.CommitTaskNotificationResultResponse_Committed{Committed: &bridgev1.CommitTaskNotificationResultCommitted{AssignedContextSequences: assignedContextSequences}}}, nil
}

type taskNotificationSettlementFacts struct {
	InboxStatus          string
	ThreadStatus         string
	TaskStatus           string
	SourceToolUseEventID string
	SourceEventType      string
	StoredResultJSON     string
	TerminalEventID      sql.NullString
}

func lockTaskNotificationSettlementFactsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitTaskNotificationResultRequest,
	taskID string,
) (taskNotificationSettlementFacts, error) {
	scope := request.GetScope()
	var facts taskNotificationSettlementFacts
	var inboxBindingID, inboxPodUID sql.NullString
	var inboxBindingGeneration sql.NullInt64
	err := tx.QueryRow(ctx, `SELECT inbox.status, thread.status, inbox.binding_id, inbox.binding_generation, inbox.target_pod_uid
		FROM session_runtime_inbox inbox
		JOIN session_threads thread
		  ON thread.workspace_id=inbox.workspace_id AND thread.session_id=inbox.session_id AND thread.id=inbox.session_thread_id
		WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.session_thread_id=$3
		  AND inbox.runtime_input_id=$4 AND inbox.input_kind='task_notification'
		FOR UPDATE OF inbox,thread`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), request.GetRuntimeInputId(),
	).Scan(&facts.InboxStatus, &facts.ThreadStatus, &inboxBindingID, &inboxBindingGeneration, &inboxPodUID)
	if dbconnect.IsNoRows(err) {
		return taskNotificationSettlementFacts{}, status.Error(codes.FailedPrecondition, "task notification Inbox custody is missing")
	}
	if err != nil {
		return taskNotificationSettlementFacts{}, err
	}
	if !inboxBindingID.Valid || inboxBindingID.String != scope.GetBinding().GetBindingId() ||
		!inboxBindingGeneration.Valid || inboxBindingGeneration.Int64 != scope.GetBinding().GetBindingGeneration() ||
		!inboxPodUID.Valid || inboxPodUID.String != scope.GetBinding().GetTargetPodUid() {
		return taskNotificationSettlementFacts{}, scopeSupersededError(status.Error(codes.FailedPrecondition, "task notification Inbox binding is stale"))
	}
	var storedResultJSON sql.NullString
	err = tx.QueryRow(ctx, `SELECT status, source_tool_use_event_id, terminal_result_json, terminal_event_id
		FROM session_background_tasks
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND task_id=$4
		FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), taskID,
	).Scan(&facts.TaskStatus, &facts.SourceToolUseEventID, &storedResultJSON, &facts.TerminalEventID)
	if dbconnect.IsNoRows(err) {
		return taskNotificationSettlementFacts{}, status.Error(codes.FailedPrecondition, "background task ownership is missing")
	}
	if err != nil {
		return taskNotificationSettlementFacts{}, err
	}
	if storedResultJSON.Valid {
		facts.StoredResultJSON = storedResultJSON.String
	}
	var sourceEventID string
	err = tx.QueryRow(ctx, `SELECT event_id, type FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4
		FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), facts.SourceToolUseEventID,
	).Scan(&sourceEventID, &facts.SourceEventType)
	if dbconnect.IsNoRows(err) {
		return taskNotificationSettlementFacts{}, status.Error(codes.FailedPrecondition, "background task source Tool Use is missing")
	}
	if err != nil {
		return taskNotificationSettlementFacts{}, err
	}
	return facts, nil
}

// The declaration RPC only moves Inbox custody. An active Queue job remains
// owned by Job Runner, which observes the parked fact and settles its lease.
func deferTaskNotificationResultTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	now time.Time,
) error {
	binding := runtimeBindingForDelivery{
		BindingID:         scope.GetBinding().GetBindingId(),
		BindingGeneration: scope.GetBinding().GetBindingGeneration(),
		PodUID:            scope.GetBinding().GetTargetPodUid(),
	}
	parked, err := parkTaskNotificationInboxTx(
		ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), runtimeInputID, binding, now,
	)
	if err != nil {
		return err
	}
	if !parked {
		return status.Error(codes.Aborted, "task notification Inbox authority changed during deferral")
	}
	return nil
}

func nullableSafeIntegerRaw(raw json.RawMessage, field string, nonNegative bool) (any, error) {
	value, err := nullableIntegerRaw(raw, field)
	if err != nil || value == nil {
		return value, err
	}
	integer := value.(int64)
	if integer < -9007199254740991 || integer > 9007199254740991 || (nonNegative && integer < 0) {
		return nil, errors.New(field + " must be a safe integer or null")
	}
	return integer, nil
}

func rejectTaskNotificationDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitTaskNotificationResultRequest,
	key string,
	requestDigest string,
	validatorID string,
	errorCode string,
	now time.Time,
) error {
	scope := request.GetScope()
	result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
		SET status='dead_lettered', updated_at=$8
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND runtime_input_id=$4
		  AND binding_id=$5 AND binding_generation=$6 AND target_pod_uid=$7
		  AND input_kind='task_notification' AND status IN ('delivering','accepted')`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), request.GetRuntimeInputId(),
		scope.GetBinding().GetBindingId(), scope.GetBinding().GetBindingGeneration(), scope.GetBinding().GetTargetPodUid(), now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		return status.Error(codes.Aborted, "task notification Inbox authority changed during rejection")
	}
	resultJSON, err := marshalBridgeJSON(map[string]any{"validator_id": validatorID})
	if err != nil {
		return err
	}
	return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
		Operation: bridgeOpCommitTaskNotificationResult, IdempotencyKey: key, RequestHash: requestDigest,
		AckStatus: bridgeAckRejected, RuntimeInputID: sql.NullString{String: request.GetRuntimeInputId(), Valid: true},
		ErrorCode: sql.NullString{String: errorCode, Valid: true}, ResultJSON: resultJSON, Now: now,
	})
}

func taskNotificationRejectionCode(errorCode string) bool {
	switch errorCode {
	case "task_notification_result_invalid", "task_notification_message_invalid", "task_notification_payload_mismatch", "task_notification_stale":
		return true
	default:
		return false
	}
}

func insertTaskNotificationStaleOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitTaskNotificationResultRequest,
	key string,
	requestDigest string,
	now time.Time,
) error {
	resultJSON, err := marshalBridgeJSON(map[string]any{"validator_id": "task_notification.stale_settlement"})
	if err != nil {
		return err
	}
	return insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
		Operation: bridgeOpCommitTaskNotificationResult, IdempotencyKey: key, RequestHash: requestDigest,
		AckStatus: bridgeAckRejected, RuntimeInputID: sql.NullString{String: request.GetRuntimeInputId(), Valid: true},
		ErrorCode: sql.NullString{String: "task_notification_stale", Valid: true}, ResultJSON: resultJSON, Now: now,
	})
}

func (s *PostgreSQLBridgeAPIStore) ReadCommandResult(ctx context.Context, request *bridgev1.ReadCommandResultRequest) (*bridgev1.ReadCommandResultResponse, error) {
	if request.GetScope() == nil || request.GetTaskId() == "" || request.GetToolUseEventId() == "" || request.GetOperationId() == "" || request.GetMaxOutputTokens() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid read command result request")
	}
	maxOutputTokens := positiveInt32(request.GetMaxOutputTokens())
	result, err := s.acceptAndAwaitBackgroundCommand(ctx, request.GetScope(), request.GetOperationId(), request.GetTaskId(), request.GetToolUseEventId(), "poll", "", maxOutputTokens)
	if err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.ReadCommandResultResponse{Outcome: &bridgev1.ReadCommandResultResponse_Stale{Stale: &bridgev1.CommandReadStale{}}}, nil
		}
		return nil, err
	}
	return &bridgev1.ReadCommandResultResponse{Outcome: &bridgev1.ReadCommandResultResponse_Completed{Completed: &bridgev1.CommandReadCompleted{ResultJson: result.ResultJSON}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) SendCommandInput(ctx context.Context, request *bridgev1.SendCommandInputRequest) (*bridgev1.SendCommandInputResponse, error) {
	if request.GetOperationId() == "" || request.GetTaskId() == "" || request.GetMaxOutputTokens() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid send command input request")
	}
	if strings.TrimSpace(request.GetInputJson()) == "" {
		return nil, status.Error(codes.InvalidArgument, "empty command input must use read command result")
	}
	if request.GetInputJson() != "" && !json.Valid([]byte(request.GetInputJson())) {
		return nil, status.Error(codes.InvalidArgument, "command input must be JSON")
	}
	if commandInputCharsEmpty(request.GetInputJson()) {
		return nil, status.Error(codes.InvalidArgument, "empty command chars must use read command result")
	}
	maxOutputTokens := positiveInt32(request.GetMaxOutputTokens())
	result, err := s.acceptAndAwaitBackgroundCommand(ctx, request.GetScope(), request.GetOperationId(), request.GetTaskId(), request.GetToolUseEventId(), "stdin", request.GetInputJson(), maxOutputTokens)
	if err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.SendCommandInputResponse{Outcome: &bridgev1.SendCommandInputResponse_Stale{Stale: &bridgev1.CommandInputStale{}}}, nil
		}
		return nil, err
	}
	if result.Duplicate {
		return &bridgev1.SendCommandInputResponse{Outcome: &bridgev1.SendCommandInputResponse_Duplicate{Duplicate: &bridgev1.CommandInputDuplicate{ResultJson: result.ResultJSON}}}, nil
	}
	return &bridgev1.SendCommandInputResponse{Outcome: &bridgev1.SendCommandInputResponse_Committed{Committed: &bridgev1.CommandInputCommitted{ResultJson: result.ResultJSON}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) CancelCommand(ctx context.Context, request *bridgev1.CancelCommandRequest) (*bridgev1.CancelCommandResponse, error) {
	if request.GetOperationId() == "" || request.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid cancel command request")
	}
	result, err := s.acceptAndAwaitBackgroundCancel(ctx, request.GetScope(), request.GetOperationId(), request.GetTaskId(), request.GetToolUseEventId(), request.GetReason())
	if err != nil {
		if isScopeSupersededError(err) {
			return &bridgev1.CancelCommandResponse{Outcome: &bridgev1.CancelCommandResponse_Stale{Stale: &bridgev1.CommandCancelStale{}}}, nil
		}
		return nil, err
	}
	if result.Duplicate {
		return &bridgev1.CancelCommandResponse{Outcome: &bridgev1.CancelCommandResponse_Duplicate{Duplicate: &bridgev1.CommandCancelDuplicate{ResultJson: result.ResultJSON}}}, nil
	}
	return &bridgev1.CancelCommandResponse{Outcome: &bridgev1.CancelCommandResponse_Committed{Committed: &bridgev1.CommandCancelCommitted{ResultJson: result.ResultJSON}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) acceptAndAwaitBackgroundCommand(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	operationID string,
	taskID string,
	toolUseEventID string,
	kind string,
	inputJSON string,
	maxOutputTokens int,
) (commandOperationResult, error) {
	if kind != "poll" && kind != "stdin" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background command kind is invalid")
	}
	requestID := operationID
	if requestID == "" || toolUseEventID == "" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background command identity is incomplete")
	}
	result := commandOperationResult{}
	now := s.now()
	err := s.withScopeTx(ctx, scope, "agentruntimebridge.accept_background_command", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, scope); err != nil {
			return err
		}
		authority, err := loadBackgroundCommandAuthorityTx(ctx, tx, scope, toolUseEventID)
		if err != nil {
			return err
		}
		if authority.Kind != kind || authority.TaskID != taskID || authority.MaxOutputTokens != maxOutputTokens {
			return status.Error(codes.FailedPrecondition, "background command does not match its durable Tool declaration")
		}
		if kind == "stdin" {
			callerInput, _, err := canonicalBackgroundCommandInput(inputJSON)
			if err != nil || callerInput != authority.InputJSON {
				return status.Error(codes.FailedPrecondition, "background command input does not match its durable Tool declaration")
			}
		}
		canonicalInput := authority.InputJSON
		inputHash := authority.InputHash
		terminalResult, err := loadBackgroundTaskForCommandAcceptanceTx(ctx, tx, scope, taskID, "")
		if err != nil {
			return err
		}
		var existingKind, existingState, existingRequestID, existingTaskID, existingInputHash, existingInputJSON string
		var existingResult sql.NullString
		var existingWriteSeq sql.NullInt64
		err = tx.QueryRow(ctx, `SELECT background_operation_kind, background_operation_state,
			background_request_id, background_task_id, normalized_input_hash, input_json,
			result_json, background_write_sequence
			FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND tool_use_event_id=$4
			FOR UPDATE`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(
			&existingKind, &existingState, &existingRequestID, &existingTaskID, &existingInputHash, &existingInputJSON,
			&existingResult, &existingWriteSeq,
		)
		if err == nil {
			if existingKind != kind || existingRequestID != requestID || existingTaskID != taskID || existingInputHash != inputHash || existingInputJSON != canonicalInput {
				return status.Error(codes.AlreadyExists, "background command idempotency conflict")
			}
			result.WriteSeq = existingWriteSeq.Int64
			result.Duplicate = true
			if existingState == "terminal" {
				switch {
				case existingResult.Valid:
					result.ResultJSON = existingResult.String
				case terminalResult != "":
					result.ResultJSON = terminalResult
				default:
					return status.Error(codes.Internal, "background command terminal result is missing")
				}
			}
			return nil
		}
		if !dbconnect.IsNoRows(err) {
			return err
		}
		var writeSequence int64
		if terminalResult == "" && kind == "stdin" {
			writeSequence, err = allocateBackgroundTaskWriteSequenceTx(ctx, tx, scope, taskID, now)
			if err != nil {
				return err
			}
			result.WriteSeq = writeSequence
		}
		state := "pending"
		var storedResult any
		var digest any
		if terminalResult != "" {
			state = "terminal"
			storedResult = terminalResult
			digest = bridgeRequestHash(terminalResult)
			result.ResultJSON = terminalResult
		}
		if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json, result_digest,
			background_operation_kind, background_operation_state, background_request_id,
			background_task_id, background_max_output_tokens, background_write_sequence,
			created_at, updated_at
		) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'write_stdin',$6,'committed',$7,$8,
			$9,$10,$11,$12,$13,$14,$15,$15)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID,
			inputHash, canonicalInput, storedResult, digest, kind, state, requestID, taskID,
			maxOutputTokens, nullablePositiveInt64(writeSequence), now,
		); err != nil {
			return err
		}
		if state == "terminal" {
			return nil
		}
		return enqueueBackgroundCommandTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, requestID, now)
	})
	if err != nil || result.ResultJSON != "" {
		return result, err
	}
	waited, err := s.waitForBackgroundCommandResult(ctx, scope, toolUseEventID)
	waited.Duplicate = result.Duplicate
	return waited, err
}

func (s *PostgreSQLBridgeAPIStore) acceptAndAwaitBackgroundCancel(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	operationID string,
	taskID string,
	toolUseEventID string,
	reason string,
) (commandOperationResult, error) {
	requestID := operationID
	if requestID == "" || toolUseEventID == "" {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background cancellation identity is incomplete")
	}
	inputJSON, err := marshalBridgeJSON(map[string]string{"reason": reason})
	if err != nil {
		return commandOperationResult{}, err
	}
	canonicalInput, inputHash, err := canonicalBackgroundCommandInput(inputJSON)
	if err != nil {
		return commandOperationResult{}, status.Error(codes.InvalidArgument, "background cancellation input must be JSON")
	}
	receiptID := backgroundCommandReceiptID(requestID)
	result := commandOperationResult{}
	now := s.now()
	err = s.withScopeTx(ctx, scope, "agentruntimebridge.accept_background_cancel", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
			return err
		}
		if err := lockThreadMutationOnlyTx(ctx, tx, scope); err != nil {
			return err
		}
		if err := verifyBackgroundCancelAuthorityTx(ctx, tx, scope, toolUseEventID); err != nil {
			return err
		}
		terminalResult, err := loadBackgroundTaskForCommandAcceptanceTx(ctx, tx, scope, taskID, toolUseEventID)
		if err != nil {
			return err
		}
		var existingKind, existingState, existingRequestID, existingTaskID, existingInputHash, existingInputJSON string
		var existingResult sql.NullString
		err = tx.QueryRow(ctx, `SELECT background_operation_kind, background_operation_state,
			background_request_id, background_task_id, normalized_input_hash, input_json, result_json
			FROM session_runtime_tool_results
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			  AND tool_use_event_id=$4 AND tool_kind='sandbox_background' FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), receiptID,
		).Scan(&existingKind, &existingState, &existingRequestID, &existingTaskID, &existingInputHash, &existingInputJSON, &existingResult)
		if err == nil {
			if existingKind != "cancel" || existingRequestID != requestID || existingTaskID != taskID ||
				existingInputHash != inputHash || existingInputJSON != canonicalInput {
				return status.Error(codes.AlreadyExists, "background cancellation idempotency conflict")
			}
			result.Duplicate = true
			if existingState == "terminal" {
				switch {
				case existingResult.Valid:
					result.ResultJSON = existingResult.String
				case terminalResult != "":
					result.ResultJSON = terminalResult
				default:
					return status.Error(codes.Internal, "background cancellation terminal result is missing")
				}
			}
			return nil
		}
		if !dbconnect.IsNoRows(err) {
			return err
		}
		state := "pending"
		var storedResult any
		var digest any
		if terminalResult != "" {
			state = "terminal"
			storedResult = terminalResult
			digest = bridgeRequestHash(terminalResult)
			result.ResultJSON = terminalResult
		}
		if _, err := tx.Exec(ctx, `INSERT INTO session_runtime_tool_results (
			workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
			normalized_input_hash, tool_name, input_json, ack_status, result_json, result_digest,
			background_operation_kind, background_operation_state, background_request_id,
			background_task_id, background_max_output_tokens, created_at, updated_at
		) VALUES ($1,$2,$3,$4,'sandbox_background',$5,'cancel_command',$6,'committed',$7,$8,
			'cancel',$9,$10,$11,0,$12,$12)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), receiptID,
			inputHash, canonicalInput, storedResult, digest, state, requestID, taskID, now); err != nil {
			return err
		}
		if state == "terminal" {
			return nil
		}
		return enqueueBackgroundCommandTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, requestID, now)
	})
	if err != nil || result.ResultJSON != "" {
		return result, err
	}
	waited, err := s.waitForBackgroundResult(ctx, scope, receiptID)
	waited.Duplicate = result.Duplicate
	return waited, err
}

func loadBackgroundTaskForCommandAcceptanceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, expectedSourceToolUseEventID string) (string, error) {
	var threadID, sourceToolUseEventID, statusValue string
	var terminalResult sql.NullString
	if err := tx.QueryRow(ctx, `SELECT session_thread_id, source_tool_use_event_id, status, terminal_result_json
		FROM session_background_tasks WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), taskID).Scan(&threadID, &sourceToolUseEventID, &statusValue, &terminalResult); dbconnect.IsNoRows(err) {
		return "", status.Error(codes.NotFound, "background task not found")
	} else if err != nil {
		return "", err
	}
	if threadID != scope.GetSessionThreadId() {
		return "", status.Error(codes.FailedPrecondition, "background task thread is stale")
	}
	if expectedSourceToolUseEventID != "" && sourceToolUseEventID != expectedSourceToolUseEventID {
		return "", status.Error(codes.FailedPrecondition, "background task does not belong to the selected Tool")
	}
	if statusValue != "running" {
		if !terminalResult.Valid || terminalResult.String == "" {
			return "", status.Error(codes.Internal, "background task terminal result is missing")
		}
		return terminalResult.String, nil
	}
	return "", nil
}

func allocateBackgroundTaskWriteSequenceTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, taskID string, now time.Time) (int64, error) {
	var writeSequence int64
	if err := tx.QueryRow(ctx, `UPDATE session_background_tasks
		SET stdin_write_sequence=stdin_write_sequence+1, updated_at=$4
		WHERE workspace_id=$1 AND session_id=$2 AND task_id=$3 AND status='running'
		RETURNING stdin_write_sequence`, scope.GetWorkspaceId(), scope.GetSessionId(), taskID, now.UTC()).Scan(&writeSequence); dbconnect.IsNoRows(err) {
		return 0, status.Error(codes.FailedPrecondition, "background task is not running")
	} else if err != nil {
		return 0, err
	}
	return writeSequence, nil
}

type backgroundCommandAuthority struct {
	Kind            string
	TaskID          string
	InputJSON       string
	InputHash       string
	MaxOutputTokens int
}

func loadBackgroundCommandAuthorityTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string) (backgroundCommandAuthority, error) {
	var payloadJSON string
	if err := tx.QueryRow(ctx, `SELECT payload_json FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND event_id=$4 AND type='agent.tool_use' FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(&payloadJSON); dbconnect.IsNoRows(err) {
		return backgroundCommandAuthority{}, status.Error(codes.FailedPrecondition, "durable background command Tool is missing")
	} else if err != nil {
		return backgroundCommandAuthority{}, err
	}
	var event runtimeToolUseEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &event); err != nil || event.Name != "write_stdin" || event.MCPServerName != "" {
		return backgroundCommandAuthority{}, status.Error(codes.FailedPrecondition, "durable background command Tool is invalid")
	}
	canonicalInput, inputHash, err := canonicalRunToolInput(string(event.Input))
	if err != nil {
		return backgroundCommandAuthority{}, status.Error(codes.FailedPrecondition, "durable background command input is invalid")
	}
	var input struct {
		SessionID       string  `json:"session_id"`
		Chars           *string `json:"chars"`
		MaxOutputTokens *int32  `json:"max_output_tokens"`
	}
	if err := json.Unmarshal([]byte(canonicalInput), &input); err != nil || input.SessionID == "" || (input.MaxOutputTokens != nil && *input.MaxOutputTokens < 0) {
		return backgroundCommandAuthority{}, status.Error(codes.FailedPrecondition, "durable background command input is invalid")
	}
	kind := "poll"
	if input.Chars != nil && *input.Chars != "" {
		kind = "stdin"
	}
	maxOutputTokens := 0
	if input.MaxOutputTokens != nil {
		maxOutputTokens = int(*input.MaxOutputTokens)
	}
	return backgroundCommandAuthority{
		Kind: kind, TaskID: input.SessionID, InputJSON: canonicalInput,
		InputHash: inputHash, MaxOutputTokens: maxOutputTokens,
	}, nil
}

func verifyBackgroundCancelAuthorityTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, toolUseEventID string) error {
	var payloadJSON string
	if err := tx.QueryRow(ctx, `SELECT payload_json FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND event_id=$4 AND type='agent.tool_use' FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), toolUseEventID).Scan(&payloadJSON); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "durable background cancellation Tool is missing")
	} else if err != nil {
		return err
	}
	var event runtimeToolUseEventPayload
	if err := json.Unmarshal([]byte(payloadJSON), &event); err != nil || event.Name != "exec_command" || event.MCPServerName != "" {
		return status.Error(codes.FailedPrecondition, "durable background cancellation Tool is invalid")
	}
	return nil
}

func enqueueBackgroundCommandTx(ctx context.Context, tx *dbconnect.Tx, workspaceID string, sessionID string, taskID string, requestID string, now time.Time) error {
	payload, err := json.Marshal(map[string]string{"workspace_id": workspaceID, "session_id": sessionID, "task_id": taskID, "request_id": requestID})
	if err != nil {
		return err
	}
	ws := workspace.ID(workspaceID)
	_, err = queue.EnqueueTx(ctx, tx, queue.EnqueueRequest{
		ID: queue.NewJobID(), WorkspaceID: ws, Kind: queue.KindSandboxBackgroundCommand,
		PartitionKey:   queue.FormatSandboxBackgroundPartitionKey(ws, sessionID, taskID),
		DedupeKey:      queue.FormatSandboxBackgroundCommandDedupeKey(ws, sessionID, taskID, requestID),
		PayloadVersion: 1, PayloadJSON: payload, MaxAttempts: 5, Now: now,
	})
	return err
}

func (s *PostgreSQLBridgeAPIStore) waitForBackgroundCommandResult(ctx context.Context, scope *bridgev1.RuntimeScope, toolUseEventID string) (commandOperationResult, error) {
	return s.waitForBackgroundResult(ctx, scope, toolUseEventID)
}

func backgroundCommandReceiptID(requestID string) string {
	return "background_receipt:" + requestID
}

func (s *PostgreSQLBridgeAPIStore) waitForBackgroundResult(ctx context.Context, scope *bridgev1.RuntimeScope, receiptID string) (commandOperationResult, error) {
	ticker := time.NewTicker(runtimeToolResultPollInterval)
	defer ticker.Stop()
	for {
		var result commandOperationResult
		var terminal bool
		err := s.withScopeReadOnlyTx(ctx, scope, "agentruntimebridge.await_background_command", func(tx *dbconnect.Tx) error {
			var operationState string
			var resultJSON sql.NullString
			var writeSequence sql.NullInt64
			err := tx.QueryRow(ctx, `SELECT receipt.background_operation_state,
				COALESCE(receipt.result_json, task.terminal_result_json),
				receipt.background_write_sequence
				FROM session_runtime_tool_results receipt
				JOIN session_background_tasks task
				  ON task.workspace_id=receipt.workspace_id
				 AND task.session_id=receipt.session_id
				 AND task.session_thread_id=receipt.session_thread_id
				 AND task.task_id=receipt.background_task_id
				WHERE receipt.workspace_id=$1 AND receipt.session_id=$2
				  AND receipt.session_thread_id=$3 AND receipt.tool_use_event_id=$4
				  AND receipt.tool_kind='sandbox_background'`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), receiptID).Scan(
				&operationState, &resultJSON, &writeSequence)
			if err != nil {
				return err
			}
			result.WriteSeq = writeSequence.Int64
			if operationState == "terminal" {
				if !resultJSON.Valid || resultJSON.String == "" {
					return status.Error(codes.Internal, "background command terminal result is missing")
				}
				result.ResultJSON = resultJSON.String
				terminal = true
			}
			return nil
		})
		if err != nil {
			return commandOperationResult{}, err
		}
		if terminal {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return commandOperationResult{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func canonicalBackgroundCommandInput(inputJSON string) (string, string, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(inputJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil || value == nil {
		return "", "", errors.New("background command input must be JSON")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(canonical)
	return string(canonical), hex.EncodeToString(digest[:]), nil
}

func nullablePositiveInt64(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}

func commandInputCharsEmpty(inputJSON string) bool {
	var payload struct {
		Chars *string `json:"chars"`
	}
	if err := json.Unmarshal([]byte(inputJSON), &payload); err != nil {
		return false
	}
	return payload.Chars != nil && *payload.Chars == ""
}

type commandOperationResult struct {
	ResultJSON string
	WriteSeq   int64
	Duplicate  bool
}

func positiveInt32(value int32) int {
	if value <= 0 {
		return 0
	}
	return int(value)
}

func validBackgroundTaskTerminalStatus(status string) bool {
	switch status {
	case "completed", "failed", "cancelled", "expired", "unknown_outcome":
		return true
	default:
		return false
	}
}

func terminalStatusFromResultJSON(resultJSON string) (string, error) {
	var payload struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(resultJSON), &payload); err != nil {
		return "", status.Error(codes.InvalidArgument, "task notification result must be JSON")
	}
	switch payload.Status {
	case "completed", "failed", "cancelled", "expired", "unknown_outcome":
		return payload.Status, nil
	default:
		return "", status.Error(codes.InvalidArgument, "task notification result status is invalid")
	}
}

func runtimeNotificationJSON(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (string, error) {
	canonicalResultJSON, err := canonicalTaskNotificationPayloadJSON(taskID, sourceToolUseEventID, terminalStatus, stripInternalProviderFields(resultJSON))
	if err != nil {
		canonicalResultJSON = `{"status":"failed","error_code":"invalid_result_json"}`
	}
	result := json.RawMessage(canonicalResultJSON)
	return marshalBridgeJSON(map[string]any{
		"type":                     "runtime_notification",
		"task_id":                  taskID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"status":                   runtimeTaskNotificationStatus(terminalStatus),
		"result":                   result,
	})
}

func commitTaskNotificationDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitTaskNotificationResultRequest,
	taskID string,
	sourceID string,
	facts taskNotificationSettlementFacts,
	resultJSON string,
	now time.Time,
) (bool, int64, error) {
	scope := request.GetScope()
	sourceToolUseEventID := facts.SourceToolUseEventID
	terminalStatus := facts.TaskStatus
	notificationJSON, err := runtimeNotificationJSON(
		taskID,
		sourceToolUseEventID,
		terminalStatus,
		resultJSON,
	)
	if err != nil {
		return false, 0, err
	}
	eventID := id.New("evt_")
	eventSequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return false, 0, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'runtime_notification', $6, 'internal', false, $7, $6, $8, $8)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		eventSequence,
		notificationJSON,
		sourceID,
		now,
	); err != nil {
		return false, 0, err
	}
	draft := runtimeTextContextDraft("runtime_notification", []string{resultJSON})
	draft.sourceEventID = eventID
	assigned, err := insertCommitInputContextDraftsTx(ctx, tx, scope, []commitInputContextDraft{draft}, now)
	if err != nil {
		return false, 0, err
	}
	if len(assigned) != 1 {
		return false, 0, status.Error(codes.Internal, "task notification context write is incomplete")
	}
	result, err := tx.Exec(ctx,
		`UPDATE session_background_tasks
		    SET terminal_event_id = $6,
		        updated_at = $7
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND task_id = $4
		    AND source_tool_use_event_id = $5
		    AND terminal_event_id IS NULL`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		taskID,
		sourceToolUseEventID,
		eventID,
		now,
	)
	if err != nil {
		return false, 0, err
	}
	if !rowsAffected(result) {
		return false, 0, status.Error(codes.Internal, "background task terminal event fence failed")
	}
	return true, assigned[0], nil
}

func insertTaskNotificationDeclarationOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceID string,
	runtimeInputID string,
	declarationDigest string,
	receiptJSON string,
	now time.Time,
) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO session_bridge_operations (
			workspace_id, session_id, session_thread_id, operation, source_kind,
			idempotency_key, request_hash, declaration_digest, receipt_json,
			ack_status, runtime_input_id, result_json, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, 'task_notification', $5, $6, $6, $7, $8, $9, '{}', $10, $10
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		bridgeOpCommitTaskNotificationResult,
		sourceID,
		declarationDigest,
		receiptJSON,
		bridgeAckCommitted,
		runtimeInputID,
		now,
	)
	return err
}

func marshalTaskNotificationReplay(assignedContextSequences []int64) (string, error) {
	encoded, err := protojson.Marshal(&bridgev1.CommitTaskNotificationResultCommitted{
		AssignedContextSequences: assignedContextSequences,
	})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func unmarshalTaskNotificationReplay(raw string) ([]int64, error) {
	committed := &bridgev1.CommitTaskNotificationResultCommitted{}
	if raw == "" || protojson.Unmarshal([]byte(raw), committed) != nil || len(committed.GetAssignedContextSequences()) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "task notification replay facts are invalid")
	}
	for _, sequence := range committed.GetAssignedContextSequences() {
		if sequence <= 0 {
			return nil, status.Error(codes.FailedPrecondition, "task notification replay facts are invalid")
		}
	}
	return append([]int64(nil), committed.GetAssignedContextSequences()...), nil
}

func canonicalTaskNotificationPayloadJSON(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (string, error) {
	value, err := canonicalTaskNotificationPayloadValue(taskID, sourceToolUseEventID, terminalStatus, resultJSON)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func canonicalTaskNotificationPayloadValue(taskID string, sourceToolUseEventID string, terminalStatus string, resultJSON string) (map[string]any, error) {
	if taskID == "" || sourceToolUseEventID == "" {
		return nil, errors.New("task notification source identity is incomplete")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(resultJSON), &object); err != nil || object == nil {
		return nil, errors.New("task notification result must be a JSON object")
	}
	factObject := object
	if rawResult, ok := object["result"]; ok {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(rawResult, &nested); err == nil && nested != nil {
			factObject = nested
		}
	}
	canonical := map[string]any{
		"task_id":                  taskID,
		"source_tool_use_event_id": sourceToolUseEventID,
		"status":                   runtimeTaskNotificationStatus(terminalStatus),
	}
	if canonical["status"] == "" {
		return nil, errors.New("task notification result status is invalid")
	}
	if raw, ok := factObject["exit_code"]; ok {
		value, err := nullableSafeIntegerRaw(raw, "task notification result exit_code", false)
		if err != nil {
			return nil, err
		}
		canonical["exit_code"] = value
	}
	for _, field := range []string{"stdout", "stderr"} {
		raw, ok := factObject[field]
		if !ok {
			return nil, errors.New("task notification result " + field + " is required")
		}
		stream, err := canonicalTaskNotificationStream(raw, "task notification result "+field)
		if err != nil {
			return nil, err
		}
		canonical[field] = stream
	}
	if err := fitTaskNotificationPayload(canonical); err != nil {
		return nil, err
	}
	return canonical, nil
}

func fitTaskNotificationPayload(payload map[string]any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if len(encoded) <= runtimeTaskNotificationPayloadMaxBytes {
		return nil
	}
	stdout, stdoutOK := payload["stdout"].(map[string]any)
	stderr, stderrOK := payload["stderr"].(map[string]any)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification streams are invalid")
	}
	stdoutText, stdoutOK := stdout["text"].(string)
	stderrText, stderrOK := stderr["text"].(string)
	if !stdoutOK || !stderrOK {
		return errors.New("task notification stream text is invalid")
	}
	stdoutBytes := len([]byte(stdoutText))
	stderrBytes := len([]byte(stderrText))
	bestStdout, bestStderr := "", ""
	bestFound := false
	for low, high := 0, stdoutBytes+stderrBytes; low <= high; {
		candidateBytes := low + (high-low)/2
		stdoutBudget, stderrBudget := splitTaskNotificationBudget(candidateBytes, stdoutBytes, stderrBytes)
		candidateStdout := taskNotificationHeadTail(stdoutText, stdoutBudget)
		candidateStderr := taskNotificationHeadTail(stderrText, stderrBudget)
		stdout["text"] = candidateStdout
		stderr["text"] = candidateStderr
		candidate, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return marshalErr
		}
		if len(candidate) <= runtimeTaskNotificationPayloadMaxBytes {
			bestStdout, bestStderr, bestFound = candidateStdout, candidateStderr, true
			low = candidateBytes + 1
		} else {
			high = candidateBytes - 1
		}
	}
	if !bestFound {
		return errors.New("task notification metadata exceeds runtime payload limit")
	}
	stdout["text"] = bestStdout
	stderr["text"] = bestStderr
	if bestStdout != stdoutText {
		stdout["truncated"] = true
	}
	if bestStderr != stderrText {
		stderr["truncated"] = true
	}
	return nil
}

func splitTaskNotificationBudget(total int, stdoutBytes int, stderrBytes int) (int, int) {
	visibleBytes := stdoutBytes + stderrBytes
	if visibleBytes == 0 || total <= 0 {
		return 0, 0
	}
	stdoutBudget := total * stdoutBytes / visibleBytes
	if stdoutBytes > 0 && stdoutBudget == 0 {
		stdoutBudget = 1
	}
	if stdoutBudget > total {
		stdoutBudget = total
	}
	return stdoutBudget, total - stdoutBudget
}

func taskNotificationHeadTail(value string, maxBytes int) string {
	if maxBytes <= 0 {
		return ""
	}
	if len([]byte(value)) <= maxBytes {
		return value
	}
	headBudget := maxBytes / 2
	tailBudget := maxBytes - headBudget
	headEnd := 0
	for index := range value {
		if index > headBudget {
			break
		}
		headEnd = index
	}
	if headBudget >= len(value) {
		headEnd = len(value)
	}
	tailStart := len(value)
	used := 0
	for tailStart > headEnd {
		_, size := utf8.DecodeLastRuneInString(value[:tailStart])
		if size == 0 || used+size > tailBudget {
			break
		}
		used += size
		tailStart -= size
	}
	return value[:headEnd] + value[tailStart:]
}

func canonicalTaskNotificationStream(raw json.RawMessage, field string) (map[string]any, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New(field + " must be a JSON object")
	}
	text, err := requiredStringRaw(object["text"], field+" text")
	if err != nil {
		return nil, err
	}
	truncated, err := requiredBoolRaw(object["truncated"], field+" truncated")
	if err != nil {
		return nil, err
	}
	canonical := map[string]any{
		"text":      text,
		"truncated": truncated,
	}
	for _, optional := range []struct {
		output string
		input  string
	}{
		{output: "original_bytes", input: "original_bytes"},
		{output: "original_bytes", input: "total_bytes"},
		{output: "original_lines", input: "original_lines"},
		{output: "original_lines", input: "total_lines"},
	} {
		if _, exists := canonical[optional.output]; exists {
			continue
		}
		rawOptional, ok := object[optional.input]
		if !ok {
			continue
		}
		value, err := nullableSafeIntegerRaw(rawOptional, field+" "+optional.input, true)
		if err != nil {
			return nil, err
		}
		canonical[optional.output] = value
	}
	return canonical, nil
}

func requiredStringRaw(raw json.RawMessage, field string) (string, error) {
	var value string
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return "", errors.New(field + " must be a string")
	}
	return value, nil
}

func requiredBoolRaw(raw json.RawMessage, field string) (bool, error) {
	var value bool
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &value) != nil {
		return false, errors.New(field + " must be a boolean")
	}
	return value, nil
}

func nullableIntegerRaw(raw json.RawMessage, field string) (any, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return nil, nil
	}
	var value int64
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return nil, errors.New(field + " must be an integer or null")
	}
	return value, nil
}
