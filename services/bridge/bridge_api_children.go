package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/childcontrol"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// This file owns the Bridge children protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) CreateChildThread(ctx context.Context, request *bridgev1.CreateChildThreadRequest) (*bridgev1.CreateChildThreadResponse, error) {
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	parentThreadID := defaultString(request.GetParentThreadId(), request.GetScope().GetSessionThreadId())
	role := defaultString(request.GetRole(), "subagent")
	if role != "subagent" && role != "approval_reviewer" {
		return nil, status.Error(codes.InvalidArgument, "invalid child thread role")
	}
	isTrunk := request.GetIsTrunk()
	if isTrunk && role != "approval_reviewer" {
		return nil, status.Error(codes.InvalidArgument, "is_trunk requires approval_reviewer role")
	}
	agentType := defaultChildAgentType(role, request.GetAgentType())
	forkTurns := defaultString(request.GetForkTurns(), "all")
	if err := validateChildThreadRequest(request, role, agentType, parentThreadID, forkTurns); err != nil {
		return nil, err
	}
	visibility := "public"
	if role == "approval_reviewer" {
		visibility = "internal"
	}
	key := childThreadIdempotencyKey(role, request.GetChildThreadId(), request.GetSourceToolUseEventId())
	childThreadID := request.GetChildThreadId()
	requestHash := bridgeRequestHash(
		bridgeOpCreateChildThread,
		parentThreadID,
		childThreadID,
		role,
		request.GetTaskName(),
		defaultString(request.GetMetadataJson(), "{}"),
		agentType,
		request.GetSourceToolUseEventId(),
		forkTurns,
		request.GetThreadContextPrefixJson(),
		boolHashPart(isTrunk),
		request.GetReviewerReviewId(),
	)
	now := s.now()
	var response *bridgev1.CreateChildThreadResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.create_child_thread", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpCreateChildThread, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "child thread idempotency conflict")
			}
			childID := childThreadID
			var existingResult createChildThreadResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &existingResult); err == nil && existingResult.ChildThreadID != "" {
				childID = existingResult.ChildThreadID
			}
			response = &bridgev1.CreateChildThreadResponse{Ack: duplicateAck("", ""), ChildThreadId: childID}
			return nil
		}
		if err := verifyChildParentThreadTx(ctx, tx, request.GetScope(), parentThreadID); err != nil {
			return err
		}
		if closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), parentThreadID); err != nil {
			return err
		} else if closing {
			return status.Error(codes.FailedPrecondition, "parent thread is closing")
		}
		if role == "approval_reviewer" && isTrunk {
			if err := demoteApprovalReviewerTrunkTx(ctx, tx, request.GetScope(), parentThreadID, now); err != nil {
				return err
			}
		}
		if err := verifyChildTaskNameAvailableTx(ctx, tx, request.GetScope(), parentThreadID, childThreadID, role, request.GetTaskName()); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_threads (
				workspace_id, id, session_id, parent_thread_id, role, visibility, status,
				agent_type, title, task_name, is_trunk, created_at, last_active_at, updated_at
				) VALUES ($1, $2, $3, $4, $5, $6, 'idle', $7, $8, $9, $10, $11, $11, $11)`,
			request.GetScope().GetWorkspaceId(),
			childThreadID,
			request.GetScope().GetSessionId(),
			parentThreadID,
			role,
			visibility,
			agentType,
			nullableSQLString(request.GetTaskName()),
			nullableSQLString(request.GetTaskName()),
			isTrunk,
			now,
		); err != nil {
			return err
		}
		childScope := scopeForThread(request.GetScope(), childThreadID)
		threadCreatedEventID, threadCreatedSequence, err := insertChildThreadCreatedEventTx(ctx, tx, childScope, parentThreadID, role, visibility, agentType, request, now)
		if err != nil {
			return err
		}
		if request.GetThreadContextPrefixJson() != "" {
			if err := insertThreadContextPrefixTx(ctx, tx, childScope, request.GetThreadContextPrefixJson(), now); err != nil {
				return err
			}
		}
		resultJSON, err := marshalBridgeJSON(createChildThreadResult{
			Status:                "created",
			ChildThreadID:         childThreadID,
			ThreadCreatedEventID:  threadCreatedEventID,
			ThreadCreatedSequence: threadCreatedSequence,
		})
		if err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpCreateChildThread,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			ResultJSON:     resultJSON,
			Now:            now,
		}); err != nil {
			return err
		}
		response = &bridgev1.CreateChildThreadResponse{Ack: committedAck("", ""), ChildThreadId: childThreadID}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) ResolveChildThread(ctx context.Context, request *bridgev1.ResolveChildThreadRequest) (*bridgev1.ResolveChildThreadResponse, error) {
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	threadJSON, err := s.readChildThread(ctx, request.GetScope(), request.GetChildThreadId(), bridgeOpResolveChildThread)
	if err != nil {
		return nil, err
	}
	return &bridgev1.ResolveChildThreadResponse{Ack: committedAck("", ""), ThreadJson: threadJSON}, nil
}

func (s *PostgreSQLBridgeAPIStore) ListChildThreads(ctx context.Context, request *bridgev1.ListChildThreadsRequest) (*bridgev1.ListChildThreadsResponse, error) {
	parentThreadID := defaultString(request.GetParentThreadId(), request.GetScope().GetSessionThreadId())
	var threads []string
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.list_child_threads", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT id, parent_thread_id, role, status, agent_type, task_name, created_at, updated_at, closed_at
			   FROM session_threads
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND parent_thread_id = $3
			    AND role = 'subagent'
			  ORDER BY created_at ASC, id ASC`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			parentThreadID,
		)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			threadJSON, err := scanThreadJSON(rows)
			if err != nil {
				return err
			}
			threads = append(threads, threadJSON)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return &bridgev1.ListChildThreadsResponse{Ack: committedAck("", ""), ThreadJson: threads}, nil
}

func (s *PostgreSQLBridgeAPIStore) ResolveInterAgentDelivery(ctx context.Context, request *bridgev1.ResolveInterAgentDeliveryRequest) (*bridgev1.ResolveInterAgentDeliveryResponse, error) {
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	now := s.now()
	var response *bridgev1.ResolveInterAgentDeliveryResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.resolve_inter_agent_delivery", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if _, _, err := readChildThreadForDeliveryTx(ctx, tx, request.GetScope(), request.GetChildThreadId()); err != nil {
			return err
		}
		var envelope storedAgentMailEnvelope
		var err error
		if request.GetDeliveryId() == "" {
			envelope, err = loadOldestUncommittedCompletionEnvelopeTx(
				ctx,
				tx,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				request.GetScope().GetSessionThreadId(),
				request.GetChildThreadId(),
			)
		} else {
			envelope, err = loadDeliverableAgentMailEnvelopeByDeliveryTx(
				ctx,
				tx,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				request.GetDeliveryId(),
			)
		}
		if err != nil {
			return err
		}
		currentThreadID := request.GetScope().GetSessionThreadId()
		childThreadID := request.GetChildThreadId()
		directInstruction := envelope.SourceThreadID == currentThreadID && envelope.TargetThreadID == childThreadID
		completionReturn := envelope.SourceThreadID == childThreadID && envelope.TargetThreadID == currentThreadID
		if !directInstruction && !completionReturn {
			return status.Error(codes.FailedPrecondition, "agent mail envelope does not match the parent-child relationship")
		}
		binding, err := readRuntimeBindingForDeliveryTx(
			ctx,
			tx,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
		)
		if err != nil {
			return err
		}
		targetScope := scopeForThread(request.GetScope(), envelope.TargetThreadID)
		admitted, err := admitAgentMailDeliveryTx(
			ctx,
			tx,
			targetScope,
			envelope,
			binding,
			now,
		)
		if err != nil {
			return err
		}
		response = &bridgev1.ResolveInterAgentDeliveryResponse{
			Ack:                  committedAck("", ""),
			DeliveryId:           envelope.DeliveryID,
			SourceThreadId:       envelope.SourceThreadID,
			TargetThreadId:       envelope.TargetThreadID,
			SourceToolUseEventId: envelope.SourceToolUseEventID,
			ReceivedEventId:      admitted.ReceivedEventID,
			ReceivedSequence:     admitted.ReceivedSequence,
			MessageJson:          string(envelope.MessageJSON),
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) MarkChildThreadClosed(ctx context.Context, request *bridgev1.MarkChildThreadClosedRequest) (*bridgev1.MarkChildThreadClosedResponse, error) {
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	now := s.now()
	command, err := parseChildLifecycleCommand(
		request.GetScope(),
		request.GetChildThreadId(),
		now,
		request.GetSource(),
		bridgeOpMarkChildThreadClosed,
		"close",
	)
	if err != nil {
		return nil, err
	}
	var (
		ack                *bridgev1.BridgeWriteAck
		receipts           []*bridgev1.DeclarationReceipt
		custodyTransitions childCloseCustodyTransitions
	)
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge."+bridgeOpMarkChildThreadClosed, func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := validateChildLifecycleSourceTx(ctx, tx, request.GetScope(), request.GetChildThreadId(), command); err != nil {
			return err
		}
		if existingReceipts, ok, err := readChildLifecycleOperationReceiptSetTx(
			ctx,
			tx,
			request.GetScope(),
			command,
			bridgeOpMarkChildThreadClosed,
		); err != nil {
			return err
		} else if ok {
			receipts = existingReceipts
			ack = duplicateAck("", command.operationID)
			return nil
		}
		targetIDs, err := childLifecycleSubtreeIDsTx(ctx, tx, request.GetScope(), request.GetChildThreadId())
		if err != nil {
			return err
		}
		if command.sourceKind == "tool_use" {
			if err := validateChildCloseCensusTx(ctx, tx, request, targetIDs, command.sourceCommandID); err != nil {
				return err
			}
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		lockedThreads := make(map[string]threadMutationScope, len(targetIDs))
		for _, targetID := range targetIDs {
			mutation, err := lockThreadMutationTx(ctx, tx, scopeForThread(request.GetScope(), targetID))
			if err != nil {
				return err
			}
			if mutation.role == "main" {
				return status.Error(codes.FailedPrecondition, "main thread cannot be marked as child")
			}
			lockedThreads[targetID] = mutation
		}
		custodyTransitions, err = settleChildCloseRuntimeInputsTx(ctx, tx, request.GetScope(), targetIDs, now)
		if err != nil {
			return err
		}
		for _, threadID := range targetIDs {
			childScope := scopeForThread(request.GetScope(), threadID)
			threadScope := lockedThreads[threadID]
			disposition := bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED
			switch threadScope.status {
			case "closed_for_runtime":
				// The per-target receipt still records this target in the frozen subtree.
			case "failed":
				// Closing a subtree records terminal members without rewriting their durable outcome.
				disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED
			case "terminated":
				disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
			default:
				if _, err := tx.Exec(ctx,
					`UPDATE session_threads
					    SET status='closed_for_runtime',
					        closed_at=COALESCE(closed_at, $4),
					        updated_at=$5
					  WHERE workspace_id=$1 AND session_id=$2 AND id=$3 AND role <> 'main'`,
					request.GetScope().GetWorkspaceId(),
					request.GetScope().GetSessionId(),
					threadID,
					command.requestedTime,
					now,
				); err != nil {
					return err
				}
				runtimeWriteID := stableRuntimeID("child_close_status", command.operationID, threadID)
				if err := insertChildThreadIdleStatusEventTx(ctx, tx, childScope, threadScope, runtimeWriteID, `{"type":"closed_for_runtime"}`, now); err != nil {
					return err
				}
				disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED
			}
			receipt := childLifecycleReceipt(childScope, command, bridgeOpMarkChildThreadClosed, threadID, disposition)
			receiptJSON, err := marshalDeclarationReceipt(receipt)
			if err != nil {
				return err
			}
			if err := insertBridgeDeclarationOperationTx(
				ctx,
				tx,
				childScope,
				bridgeOpMarkChildThreadClosed,
				command.operationSourceKind,
				command.operationID,
				command.declarationDigest,
				receiptJSON,
				now,
			); err != nil {
				return err
			}
			receipts = append(receipts, receipt)
		}
		ack = committedAck("", command.operationID)
		return nil
	}); err != nil {
		return nil, err
	}
	logRuntimeInputCustodyTransition(s.Logger, request.GetScope(), "accepted_to_parked", custodyTransitions.parked)
	logRuntimeInputCustodyTransition(s.Logger, request.GetScope(), "accepted_to_cancelled", custodyTransitions.cancelled)
	declaration, err := s.childLifecycleDeclarationResponse(ctx, request.GetScope(), receipts, ack)
	if err != nil {
		return nil, err
	}
	return &bridgev1.MarkChildThreadClosedResponse{Ack: ack, Declaration: declaration}, nil
}

// settleChildCloseRuntimeInputsTx transfers every live Inbox-backed input
// in the frozen subtree before hot release: task notifications become durable
// parked custody, while other input kinds are cancelled. Any active Queue job
// for the exact input is terminalized in the same transaction and loses its
// lease token.
type childCloseCustodyTransitions struct {
	parked    int
	cancelled int
}

func settleChildCloseRuntimeInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	targetIDs []string,
	now time.Time,
) (childCloseCustodyTransitions, error) {
	var transitions childCloseCustodyTransitions
	for _, targetID := range targetIDs {
		rows, err := tx.Query(ctx, `SELECT runtime_input_id,input_kind
			FROM session_runtime_inbox
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
			 AND status IN ('queued','delivering','accepted') AND input_kind <> 'approval_review'
			ORDER BY created_at,runtime_input_id FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), targetID)
		if err != nil {
			return childCloseCustodyTransitions{}, err
		}
		type closeInput struct{ runtimeInputID, inputKind string }
		var inputs []closeInput
		for rows.Next() {
			var input closeInput
			if err := rows.Scan(&input.runtimeInputID, &input.inputKind); err != nil {
				_ = rows.Close()
				return childCloseCustodyTransitions{}, err
			}
			inputs = append(inputs, input)
		}
		if err := rows.Close(); err != nil {
			return childCloseCustodyTransitions{}, err
		}
		binding := runtimeBindingForDelivery{
			BindingID: scope.GetBinding().GetBindingId(), BindingGeneration: scope.GetBinding().GetBindingGeneration(),
			PodUID: scope.GetBinding().GetTargetPodUid(),
		}
		for _, input := range inputs {
			if _, err := tx.Exec(ctx, `UPDATE queue_jobs
				SET status='cancelled',cancelled_at=$4,lease_token=NULL,leased_by=NULL,leased_at=NULL,leased_until=NULL,updated_at=$4
				WHERE workspace_id=$1 AND status IN ('pending','leased')
				 AND dedupe_key='runtime_input:' || $1 || ':' || $2 || ':' || $3`,
				scope.GetWorkspaceId(), scope.GetSessionId(), input.runtimeInputID, now); err != nil {
				return childCloseCustodyTransitions{}, err
			}
			if input.inputKind == "task_notification" {
				parked, err := parkTaskNotificationInboxTx(
					ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), targetID, input.runtimeInputID, binding, now,
				)
				if err != nil {
					return childCloseCustodyTransitions{}, err
				}
				if !parked {
					return childCloseCustodyTransitions{}, status.Error(codes.Aborted, "task notification Inbox authority changed during child close")
				}
				transitions.parked++
				continue
			}
			result, err := tx.Exec(ctx, `UPDATE session_runtime_inbox SET status='cancelled',updated_at=$4
				WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 AND status IN ('queued','delivering','accepted')`,
				scope.GetWorkspaceId(), scope.GetSessionId(), input.runtimeInputID, now)
			if err != nil {
				return childCloseCustodyTransitions{}, err
			}
			if !rowsAffected(result) {
				return childCloseCustodyTransitions{}, status.Error(codes.Aborted, "runtime Inbox authority changed during child close")
			}
			transitions.cancelled++
		}
	}
	return transitions, nil
}

func (s *PostgreSQLBridgeAPIStore) MarkChildThreadActive(ctx context.Context, request *bridgev1.MarkChildThreadActiveRequest) (*bridgev1.MarkChildThreadActiveResponse, error) {
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	now := s.now()
	command, err := parseChildLifecycleCommand(
		request.GetScope(),
		request.GetChildThreadId(),
		now,
		request.GetSource(),
		bridgeOpMarkChildThreadActive,
		"resume",
	)
	if err != nil {
		logThreadResumeRejected(s.Logger, request.GetScope(), request.GetChildThreadId(), err)
		return nil, err
	}
	var (
		ack      *bridgev1.BridgeWriteAck
		receipts []*bridgev1.DeclarationReceipt
	)
	err = s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge."+bridgeOpMarkChildThreadActive, func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := validateChildLifecycleSourceTx(ctx, tx, request.GetScope(), request.GetChildThreadId(), command); err != nil {
			return err
		}
		childScope := scopeForThread(request.GetScope(), request.GetChildThreadId())
		if existingReceipts, ok, err := readChildLifecycleOperationReceiptsTx(
			ctx,
			tx,
			request.GetScope(),
			[]string{request.GetChildThreadId()},
			command,
			bridgeOpMarkChildThreadActive,
		); err != nil {
			return err
		} else if ok {
			receipts = existingReceipts
			ack = duplicateAck("", command.operationID)
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, childScope)
		if err != nil {
			return err
		}
		if threadScope.role == "main" {
			return status.Error(codes.FailedPrecondition, "main thread cannot be marked as child")
		}
		if closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetChildThreadId()); err != nil {
			return err
		} else if closing {
			return status.Error(codes.FailedPrecondition, "child thread is closing")
		}
		disposition := bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE
		switch threadScope.status {
		case "failed":
			// Resume never revives a terminal child or changes its durable outcome.
			disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED
		case "terminated":
			disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
		case "closed_for_runtime":
			result, err := tx.Exec(ctx,
				`UPDATE session_threads
				    SET status = 'idle',
				        closed_at = NULL,
				        last_active_at = $4,
				        updated_at = $5
				  WHERE workspace_id = $1
				    AND session_id = $2
				    AND id = $3
				    AND role <> 'main'
				    AND status = 'closed_for_runtime'`,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				request.GetChildThreadId(),
				command.requestedTime,
				now,
			)
			if err != nil {
				return err
			}
			if !rowsAffected(result) {
				return status.Error(codes.FailedPrecondition, "child resume status update failed")
			}
			disposition = bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED
		}
		if disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED {
			if err := resumeTaskNotificationsTx(ctx, tx, childScope, now); err != nil {
				return err
			}
		}
		receipt := childLifecycleReceipt(childScope, command, bridgeOpMarkChildThreadActive, request.GetChildThreadId(), disposition)
		receiptJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			childScope,
			bridgeOpMarkChildThreadActive,
			command.operationSourceKind,
			command.operationID,
			command.declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		receipts = []*bridgev1.DeclarationReceipt{receipt}
		ack = committedAck("", command.operationID)
		return nil
	})
	if err != nil {
		logThreadResumeRejected(s.Logger, request.GetScope(), request.GetChildThreadId(), err)
		return nil, err
	}
	declaration, err := s.childLifecycleDeclarationResponse(ctx, request.GetScope(), receipts, ack)
	if err != nil {
		logThreadResumeRejected(s.Logger, request.GetScope(), request.GetChildThreadId(), err)
		return nil, err
	}
	return &bridgev1.MarkChildThreadActiveResponse{Ack: ack, Declaration: declaration}, nil
}

// resumeTaskNotificationsTx reactivates only durable notification inboxes after
// the Thread has passed its cold-resume validation. Queue birth and the status
// transition share the lifecycle transaction, so no stale close-time lease can
// race the resumed delivery.
func resumeTaskNotificationsTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) error {
	rows, err := tx.Query(ctx, `SELECT inbox.runtime_input_id, task.task_id
		FROM session_runtime_inbox inbox
		JOIN session_background_tasks task
		  ON task.workspace_id=inbox.workspace_id AND task.session_id=inbox.session_id
		 AND task.session_thread_id=inbox.session_thread_id
		 AND inbox.runtime_input_id='task_notification:' || task.task_id
		WHERE inbox.workspace_id=$1 AND inbox.session_id=$2 AND inbox.session_thread_id=$3
		 AND inbox.input_kind='task_notification' AND inbox.status='parked'
		 AND task.terminal_event_id IS NULL
		ORDER BY inbox.created_at, inbox.runtime_input_id
		FOR UPDATE OF inbox`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId())
	if err != nil {
		return err
	}
	type notification struct{ runtimeInputID, taskID string }
	var notifications []notification
	for rows.Next() {
		var value notification
		if err := rows.Scan(&value.runtimeInputID, &value.taskID); err != nil {
			_ = rows.Close()
			return err
		}
		notifications = append(notifications, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	ws := workspace.ID(scope.GetWorkspaceId())
	for _, notification := range notifications {
		if _, err := tx.Exec(ctx, `UPDATE session_runtime_inbox
			SET status='queued', binding_id=NULL, binding_generation=NULL, target_pod_uid=NULL, updated_at=$3
			WHERE workspace_id=$1 AND runtime_input_id=$2 AND status='parked'`,
			scope.GetWorkspaceId(), notification.runtimeInputID, now); err != nil {
			return err
		}
		request, err := queue.NewTaskNotificationRuntimeInputEnqueueRequest(ws, scope.GetSessionId(), scope.GetSessionThreadId(), notification.taskID, now)
		if err != nil {
			return err
		}
		if _, err := queue.EnqueueTx(ctx, tx, request); err != nil {
			return err
		}
	}
	return nil
}

type childLifecycleCommand struct {
	action              string
	sourceKind          string
	sourceCommandID     string
	operationSourceKind string
	operationID         string
	requestedAt         string
	requestedTime       time.Time
	declarationDigest   string
}

func parseChildLifecycleCommand(
	scope *bridgev1.RuntimeScope,
	childThreadID string,
	requestedTime time.Time,
	source *bridgev1.ChildLifecycleSource,
	operationKind string,
	action string,
) (childLifecycleCommand, error) {
	requestedAt := requestedTime.UTC().Format(time.RFC3339Nano)
	var sourceKind, sourceCommandID string
	switch identity := source.GetIdentity().(type) {
	case *bridgev1.ChildLifecycleSource_SourceToolUseEventId:
		sourceKind = "tool_use"
		sourceCommandID = identity.SourceToolUseEventId
	case *bridgev1.ChildLifecycleSource_ReviewerReviewId:
		sourceKind = "approval_review"
		sourceCommandID = identity.ReviewerReviewId
	default:
		return childLifecycleCommand{}, status.Error(codes.InvalidArgument, "child lifecycle source is required")
	}
	if sourceCommandID == "" {
		return childLifecycleCommand{}, status.Error(codes.InvalidArgument, "child lifecycle source identity is required")
	}
	operationSourceKind := "child_close_command"
	operationIDParts := []string{"child_tree_close", sourceCommandID, childThreadID}
	if action == "resume" {
		operationSourceKind = "child_resume_command"
		operationIDParts[0] = "child_resume"
	}
	operationID := stableRuntimeID(operationIDParts...)
	declarationDigest, err := childLifecycleDeclarationDigest(
		operationKind,
		action,
		scope.GetSessionThreadId(),
		childThreadID,
		sourceKind,
		sourceCommandID,
	)
	if err != nil {
		return childLifecycleCommand{}, err
	}
	return childLifecycleCommand{
		action:              action,
		sourceKind:          sourceKind,
		sourceCommandID:     sourceCommandID,
		operationSourceKind: operationSourceKind,
		operationID:         operationID,
		requestedAt:         requestedAt,
		requestedTime:       requestedTime.UTC(),
		declarationDigest:   declarationDigest,
	}, nil
}

func validateChildLifecycleSourceTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	childThreadID string,
	command childLifecycleCommand,
) error {
	if command.sourceKind == "approval_review" {
		if childThreadID != approvalReviewerSidecarThreadID(scope, scope.GetSessionThreadId(), command.sourceCommandID) {
			return status.Error(codes.FailedPrecondition, "child lifecycle reviewer source does not own the child")
		}
		var role, visibility, parentThreadID string
		if err := tx.QueryRow(ctx,
			`SELECT role, visibility, parent_thread_id
			   FROM session_threads
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND id = $3
			  FOR SHARE`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			childThreadID,
		).Scan(&role, &visibility, &parentThreadID); dbconnect.IsNoRows(err) {
			return status.Error(codes.FailedPrecondition, "child lifecycle reviewer thread is missing")
		} else if err != nil {
			return err
		}
		if role != "approval_reviewer" || visibility != "internal" || parentThreadID != scope.GetSessionThreadId() {
			return status.Error(codes.FailedPrecondition, "child lifecycle reviewer source does not match the durable reviewer thread")
		}
		if command.action == "close" {
			return validateSettledApprovalReviewerCloseTx(ctx, tx, scope, childThreadID, command.sourceCommandID)
		}
		return nil
	}
	var role, visibility, parentThreadID string
	if err := tx.QueryRow(ctx,
		`SELECT role, visibility, parent_thread_id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR SHARE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		childThreadID,
	).Scan(&role, &visibility, &parentThreadID); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "child lifecycle target is missing")
	} else if err != nil {
		return err
	}
	if role != "subagent" || visibility != "public" || parentThreadID != scope.GetSessionThreadId() {
		return status.Error(codes.FailedPrecondition, "child lifecycle tool source does not own the durable child")
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
			   AND type = 'agent.tool_use'
			   AND visibility = 'public'
		)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		command.sourceCommandID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return status.Error(codes.FailedPrecondition, "child lifecycle tool source is invalid")
	}
	return nil
}

func validateSettledApprovalReviewerCloseTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, childThreadID, reviewID string) error {
	var outcomeCount int
	err := tx.QueryRow(ctx, `SELECT COUNT(*)
		FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		 AND type IN ('approval_review.decision','approval_review.failure')
		 AND payload_json::jsonb->>'review_id'=$4`, scope.GetWorkspaceId(), scope.GetSessionId(), childThreadID, reviewID).Scan(&outcomeCount)
	if err != nil {
		return err
	}
	if outcomeCount > 1 {
		return status.Error(codes.FailedPrecondition, "approval reviewer close found competing durable outcomes")
	}
	outcomeSettled := outcomeCount == 1
	var cancelledRequestSettled bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1
		  FROM session_events end_event
		 WHERE end_event.workspace_id=$1
		   AND end_event.session_id=$2
		   AND end_event.session_thread_id=$3
		   AND end_event.type='span.model_request_end'
		   AND end_event.payload_json::jsonb->>'request_kind'='approval_reviewer'
		   AND end_event.payload_json::jsonb->>'error_kind'='runtime_interrupted'
		   AND end_event.payload_json::jsonb->>'finish_reason'='cancelled')`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childThreadID).Scan(&cancelledRequestSettled)
	if err != nil {
		return err
	}
	if !outcomeSettled && !cancelledRequestSettled {
		return status.Error(codes.FailedPrecondition, "approval reviewer close requires a durable outcome or cancelled request")
	}
	var unfinished bool
	err = tx.QueryRow(ctx, `SELECT
		EXISTS (
		 SELECT 1 FROM session_events start_event
		 WHERE start_event.workspace_id=$1 AND start_event.session_id=$2 AND start_event.session_thread_id=$3
		  AND start_event.type='span.model_request_start'
		  AND NOT EXISTS (SELECT 1 FROM session_events end_event
		   WHERE end_event.workspace_id=start_event.workspace_id AND end_event.session_id=start_event.session_id
		    AND end_event.session_thread_id=start_event.session_thread_id
		    AND end_event.model_request_id=start_event.model_request_id AND end_event.type='span.model_request_end'))
		OR EXISTS (
		 SELECT 1 FROM session_pending_tool_uses WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND status IN ('pending','resolving'))
		OR EXISTS (
		 SELECT 1 FROM session_runtime_inbox WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		  AND status IN ('queued','delivering','accepted','parked'))`,
		scope.GetWorkspaceId(), scope.GetSessionId(), childThreadID).Scan(&unfinished)
	if err != nil {
		return err
	}
	if unfinished {
		return status.Error(codes.FailedPrecondition, "approval reviewer close requires quiescent durable state")
	}
	return nil
}

func readChildLifecycleOperationReceiptSetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	callerScope *bridgev1.RuntimeScope,
	command childLifecycleCommand,
	operationKind string,
) ([]*bridgev1.DeclarationReceipt, bool, error) {
	rows, err := tx.Query(ctx,
		`SELECT session_thread_id, COALESCE(declaration_digest, ''), COALESCE(receipt_json, '')
		   FROM session_bridge_operations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND operation = $3
		    AND source_kind = $4
		    AND idempotency_key = $5
		  ORDER BY session_thread_id
		  FOR UPDATE`,
		callerScope.GetWorkspaceId(),
		callerScope.GetSessionId(),
		operationKind,
		command.operationSourceKind,
		command.operationID,
	)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rows.Close() }()
	var receipts []*bridgev1.DeclarationReceipt
	for rows.Next() {
		var targetID, declarationDigest, receiptJSON string
		if err := rows.Scan(&targetID, &declarationDigest, &receiptJSON); err != nil {
			return nil, false, err
		}
		if declarationDigest != command.declarationDigest || receiptJSON == "" {
			return nil, false, status.Error(codes.AlreadyExists, "child lifecycle idempotency conflict")
		}
		receipt, err := unmarshalDeclarationReceipt(receiptJSON)
		if err != nil || !validStoredChildLifecycleReceipt(receipt, targetID, operationKind, command) {
			return nil, false, status.Error(codes.FailedPrecondition, "child lifecycle receipt is invalid")
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return receipts, len(receipts) > 0, nil
}

func readChildLifecycleOperationReceiptsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	callerScope *bridgev1.RuntimeScope,
	targetIDs []string,
	command childLifecycleCommand,
	operationKind string,
) ([]*bridgev1.DeclarationReceipt, bool, error) {
	receipts := make([]*bridgev1.DeclarationReceipt, 0, len(targetIDs))
	existingCount := 0
	for _, targetID := range targetIDs {
		targetScope := scopeForThread(callerScope, targetID)
		existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			targetScope,
			operationKind,
			command.operationSourceKind,
			command.operationID,
		)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		existingCount++
		if existing.DeclarationDigest != command.declarationDigest || existing.ReceiptJSON == "" {
			return nil, false, status.Error(codes.AlreadyExists, "child lifecycle idempotency conflict")
		}
		receipt, err := unmarshalDeclarationReceipt(existing.ReceiptJSON)
		if err != nil || !validStoredChildLifecycleReceipt(receipt, targetID, operationKind, command) {
			return nil, false, status.Error(codes.FailedPrecondition, "child lifecycle receipt is invalid")
		}
		receipts = append(receipts, receipt)
	}
	if existingCount == 0 {
		return nil, false, nil
	}
	if existingCount != len(targetIDs) {
		return nil, false, status.Error(codes.FailedPrecondition, "child lifecycle receipt set is incomplete")
	}
	return receipts, true, nil
}

func validStoredChildLifecycleReceipt(
	receipt *bridgev1.DeclarationReceipt,
	targetID string,
	operationKind string,
	command childLifecycleCommand,
) bool {
	if receipt == nil ||
		receipt.GetSessionThreadId() != targetID ||
		receipt.GetOperationKind() != operationKind ||
		receipt.GetSourceKind() != command.operationSourceKind ||
		receipt.GetOperationId() != command.operationID ||
		receipt.GetDeclarationDigest() != command.declarationDigest ||
		len(receipt.GetChildLifecycle()) != 1 {
		return false
	}
	stamp := receipt.GetChildLifecycle()[0]
	if stamp.GetChildThreadId() != targetID || stamp.GetEffectiveAt() == "" {
		return false
	}
	if command.action == "close" {
		return stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED ||
			stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED ||
			stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED ||
			stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
	}
	return stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED ||
		stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE ||
		stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED ||
		stamp.GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
}

func childLifecycleReceipt(
	targetScope *bridgev1.RuntimeScope,
	command childLifecycleCommand,
	operationKind string,
	targetID string,
	disposition bridgev1.ChildLifecycleDisposition,
) *bridgev1.DeclarationReceipt {
	return &bridgev1.DeclarationReceipt{
		SessionThreadId:   targetScope.GetSessionThreadId(),
		OperationKind:     operationKind,
		SourceKind:        command.operationSourceKind,
		OperationId:       command.operationID,
		DeclarationDigest: command.declarationDigest,
		ChildLifecycle: []*bridgev1.ChildLifecycleStamp{{
			ChildThreadId: targetID,
			Disposition:   disposition,
			EffectiveAt:   command.requestedAt,
		}},
	}
}

func childLifecycleSubtreeIDsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	rootChildThreadID string,
) ([]string, error) {
	rows, err := tx.Query(ctx,
		`WITH RECURSIVE child_tree(id) AS (
			SELECT id
			  FROM session_threads
			 WHERE workspace_id = $1
			   AND session_id = $2
			   AND id = $3
			   AND role <> 'main'
			UNION ALL
			SELECT child.id
			  FROM session_threads child
			  JOIN child_tree parent ON child.parent_thread_id = parent.id
			 WHERE child.workspace_id = $1
			   AND child.session_id = $2
			   AND child.role <> 'main'
		)
		SELECT id FROM child_tree ORDER BY id`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		rootChildThreadID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var threadID string
		if err := rows.Scan(&threadID); err != nil {
			return nil, err
		}
		ids = append(ids, threadID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, status.Error(codes.NotFound, "child thread not found")
	}
	return ids, nil
}

func (s *PostgreSQLBridgeAPIStore) childLifecycleDeclarationResponse(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	receipts []*bridgev1.DeclarationReceipt,
	ack *bridgev1.BridgeWriteAck,
) (*bridgev1.DeclarationResponse, error) {
	observation, err := s.declarationApplicationObservation(ctx, scope)
	if err != nil {
		return nil, err
	}
	if len(receipts) > 0 {
		receipt := receipts[0]
		logRuntimeDeclaration(
			s.Logger,
			scope,
			receipt.GetOperationKind(),
			receipt.GetSourceKind(),
			receipt.GetOperationId(),
			receipt.GetDeclarationDigest(),
			ack,
			observation,
		)
	}
	return &bridgev1.DeclarationResponse{
		Receipts:                  receipts,
		ObservedBindingId:         observation.BindingID,
		ObservedBindingGeneration: observation.BindingGeneration,
		ApplicationDisposition:    observation.Disposition,
	}, nil
}

func defaultChildAgentType(role string, requested string) string {
	if requested != "" {
		return requested
	}
	if role == "approval_reviewer" {
		return "approval_reviewer"
	}
	return "general"
}

func childThreadIdempotencyKey(role string, childThreadID string, sourceToolUseEventID string) string {
	if role == "subagent" {
		return sourceToolUseEventID
	}
	return defaultString(sourceToolUseEventID, childThreadID)
}

type threadContextPrefixEnvelope struct {
	SourceParentThreadID  string            `json:"source_parent_thread_id"`
	ParentBoundaryEventID string            `json:"parent_boundary_event_id"`
	SourceToolUseEventID  string            `json:"source_tool_use_event_id,omitempty"`
	ReviewID              string            `json:"review_id,omitempty"`
	ForkTurns             string            `json:"fork_turns"`
	RuntimeMessages       []json.RawMessage `json:"runtime_messages_snapshot"`
}

// validateChildThreadRequest enforces the role-discriminated context-prefix source.
// The source identity differs by role because an approval-reviewer sidecar
// provably has no source_tool_use_event_id yet — its review resolves before the
// target tool's public agent.tool_use event exists:
//
//	role / shape                 required source identity        forbidden
//	subagent                     source_tool_use_event_id        review_id
//	approval_reviewer sidecar    review_id (must equal the       source_tool_use_event_id
//	                               corresponding approval_review
//	                               input's review_id)
//	approval_reviewer trunk      none — prefixless; fresh id per  any source or context prefix
//	                               parent hot lifetime;
//	                               at-most-one live trunk uses
//	                               demote-at-create succession
//
// Both context-prefix arms are strict: no empty-string stand-ins in either field. The
// source rides the CreateChildThread wire, so validation is shape/lineage/identity
// WITHIN-REQUEST only — it never joins a durable review-event row, which does not
// exist at creation time. A sidecar's source_parent_thread_id is the PUBLIC
// owning thread; the trunk supplies snapshot content only, never lineage.
func validateChildThreadRequest(request *bridgev1.CreateChildThreadRequest, role string, agentType string, parentThreadID string, forkTurns string) error {
	taskName := request.GetTaskName()
	sourceToolUseEventID := request.GetSourceToolUseEventId()
	reviewID := request.GetReviewerReviewId()
	prefixJSON := request.GetThreadContextPrefixJson()
	if role == "subagent" {
		if taskName == "" {
			return status.Error(codes.InvalidArgument, "sub-agent task_name is required")
		}
		if sourceToolUseEventID == "" {
			return status.Error(codes.InvalidArgument, "sub-agent source_tool_use_event_id is required")
		}
		switch agentType {
		case "general", "research", "worker":
		default:
			return status.Error(codes.InvalidArgument, "invalid sub-agent agent_type")
		}
		if prefixJSON == "" {
			return status.Error(codes.InvalidArgument, "sub-agent thread context prefix is required")
		}
		if reviewID != "" {
			return status.Error(codes.InvalidArgument, "sub-agent reviewer_review_id must be absent")
		}
		if !validForkTurns(forkTurns) {
			return status.Error(codes.InvalidArgument, "invalid sub-agent fork_turns")
		}
	}
	if role == "approval_reviewer" && request.GetIsTrunk() {
		if sourceToolUseEventID != "" || reviewID != "" || prefixJSON != "" {
			return status.Error(codes.InvalidArgument, "approval reviewer trunk must not carry a thread context prefix source")
		}
		return nil
	}
	if role == "approval_reviewer" {
		if sourceToolUseEventID != "" || reviewID == "" || prefixJSON == "" {
			return status.Error(codes.InvalidArgument, "approval reviewer sidecar requires only reviewer_review_id and a thread context prefix")
		}
		if !validForkTurns(forkTurns) {
			return status.Error(codes.InvalidArgument, "invalid approval reviewer fork_turns")
		}
		if request.GetChildThreadId() != approvalReviewerSidecarThreadID(request.GetScope(), parentThreadID, reviewID) {
			return status.Error(codes.InvalidArgument, "approval reviewer sidecar identity is invalid")
		}
	}
	var prefix threadContextPrefixEnvelope
	decoder := json.NewDecoder(strings.NewReader(prefixJSON))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&prefix); err != nil {
		return status.Error(codes.InvalidArgument, "thread context prefix must match the strict schema")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "thread context prefix must contain exactly one object")
	}
	if prefix.SourceParentThreadID != parentThreadID || prefix.ForkTurns != forkTurns || prefix.RuntimeMessages == nil {
		return status.Error(codes.InvalidArgument, "thread context prefix lineage or snapshot is invalid")
	}
	if prefix.ParentBoundaryEventID == "" {
		return status.Error(codes.InvalidArgument, "thread context prefix parent boundary is required")
	}
	if role == "subagent" && (prefix.SourceToolUseEventID != sourceToolUseEventID || prefix.ReviewID != "") {
		return status.Error(codes.InvalidArgument, "sub-agent thread context prefix source is invalid")
	}
	if role == "subagent" && prefix.ParentBoundaryEventID != sourceToolUseEventID {
		return status.Error(codes.InvalidArgument, "sub-agent thread context prefix boundary is invalid")
	}
	if role == "approval_reviewer" && (prefix.ReviewID != reviewID || prefix.SourceToolUseEventID != "") {
		return status.Error(codes.InvalidArgument, "approval reviewer thread context prefix source is invalid")
	}
	for _, message := range prefix.RuntimeMessages {
		if !json.Valid(message) {
			return status.Error(codes.InvalidArgument, "thread context prefix contains malformed message")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(message, &fields); err != nil {
			return status.Error(codes.InvalidArgument, "thread context prefix message must be an object")
		}
		if _, ok := fields["providerId"]; ok {
			return status.Error(codes.InvalidArgument, "thread context prefix message contains routing metadata")
		}
		if _, ok := fields["modelId"]; ok {
			return status.Error(codes.InvalidArgument, "thread context prefix message contains routing metadata")
		}
	}
	return nil
}

func approvalReviewerSidecarThreadID(scope *bridgev1.RuntimeScope, parentThreadID string, reviewID string) string {
	hash := sha256.New()
	for _, part := range []string{scope.GetWorkspaceId(), scope.GetSessionId(), parentThreadID, reviewID} {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return "thrd_aprv_sidecar_" + hex.EncodeToString(hash.Sum(nil))[:24]
}

func validForkTurns(value string) bool {
	if value == "none" || value == "all" {
		return true
	}
	if value == "" || value[0] == '0' {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func verifyChildParentThreadTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, parentThreadID string) error {
	row := tx.QueryRow(ctx,
		`SELECT status
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
	)
	var parentStatus string
	if err := row.Scan(&parentStatus); dbconnect.IsNoRows(err) {
		return status.Error(codes.FailedPrecondition, "parent thread is missing")
	} else if err != nil {
		return err
	}
	if parentStatus == "closed_for_runtime" || parentStatus == "terminated" || parentStatus == "failed" {
		return status.Error(codes.FailedPrecondition, "parent thread is not open")
	}
	return nil
}

func demoteApprovalReviewerTrunkTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, parentThreadID string, now time.Time) error {
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND parent_thread_id = $3
		    AND role = 'approval_reviewer'
		    AND is_trunk
		  LIMIT 1
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
	)
	var existingChildID string
	if err := row.Scan(&existingChildID); dbconnect.IsNoRows(err) {
		return nil
	} else if err != nil {
		return err
	}
	_, err := tx.Exec(ctx,
		`UPDATE session_threads
		    SET is_trunk = FALSE,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND parent_thread_id = $3
		    AND id = $4
		    AND role = 'approval_reviewer'
		    AND is_trunk`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
		existingChildID,
		now,
	)
	return err
}

func verifyChildTaskNameAvailableTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, parentThreadID string, childThreadID string, role string, taskName string) error {
	if role != "subagent" || taskName == "" {
		return nil
	}
	row := tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND parent_thread_id = $3
		    AND role = 'subagent'
		    AND task_name = $4
		  LIMIT 1
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		parentThreadID,
		taskName,
	)
	var existingChildID string
	if err := row.Scan(&existingChildID); dbconnect.IsNoRows(err) {
		return nil
	} else if err != nil {
		return err
	}
	if existingChildID == childThreadID {
		return nil
	}
	return status.Error(codes.AlreadyExists, "sub-agent task_name already exists")
}

func insertChildThreadCreatedEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	parentThreadID string,
	role string,
	visibility string,
	agentType string,
	request *bridgev1.CreateChildThreadRequest,
	now time.Time,
) (string, int64, error) {
	threadScope, err := lockThreadMutationTx(ctx, tx, scope)
	if err != nil {
		return "", 0, err
	}
	eventVisibility, sessionVisible := threadScope.publicProjection("session.thread_created")
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":                     "session.thread_created",
		"session_thread_id":        scope.GetSessionThreadId(),
		"parent_thread_id":         parentThreadID,
		"role":                     role,
		"visibility":               visibility,
		"agent_type":               agentType,
		"task_name":                nullableJSONString(nullableSQLString(request.GetTaskName())),
		"source_tool_use_event_id": nullableJSONString(nullableSQLString(request.GetSourceToolUseEventId())),
	})
	if err != nil {
		return "", 0, err
	}
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return "", 0, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'session.thread_created', $6, $7, $8, $6, $9, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		payloadJSON,
		eventVisibility,
		sessionVisible,
		now,
	); err != nil {
		return "", 0, err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, eventVisibility, sessionVisible, now); err != nil {
		return "", 0, err
	}
	return eventID, sequence, nil
}

func insertThreadContextPrefixTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, prefixJSON string, now time.Time) error {
	var prefix threadContextPrefixEnvelope
	if err := json.Unmarshal([]byte(prefixJSON), &prefix); err != nil {
		return status.Error(codes.InvalidArgument, "thread context prefix is invalid")
	}
	var boundaryThreadID string
	if err := tx.QueryRow(ctx,
		`SELECT session_thread_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND event_id = $3
		  FOR SHARE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		prefix.ParentBoundaryEventID,
	).Scan(&boundaryThreadID); err != nil {
		return err
	}
	if boundaryThreadID != prefix.SourceParentThreadID {
		return status.Error(codes.FailedPrecondition, "thread context prefix parent boundary does not belong to the parent thread")
	}
	entriesJSON, err := json.Marshal(prefix.RuntimeMessages)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_thread_context_prefixes (
			workspace_id, session_id, child_thread_id, parent_thread_id,
			parent_boundary_event_id, entries_json, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		prefix.SourceParentThreadID,
		prefix.ParentBoundaryEventID,
		string(entriesJSON),
		now,
	); err != nil {
		return err
	}
	return nil
}

func (s *PostgreSQLBridgeAPIStore) readChildThread(ctx context.Context, scope *bridgev1.RuntimeScope, childThreadID string, operation string) (string, error) {
	var threadJSON string
	if err := s.withScopeTx(ctx, scope, "agentruntimebridge."+operation, func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		row := tx.QueryRow(ctx,
			`SELECT id, parent_thread_id, role, status, agent_type, task_name, created_at, updated_at, closed_at
			   FROM session_threads
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND id = $3`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			childThreadID,
		)
		var err error
		threadJSON, err = scanThreadJSON(row)
		return err
	}); err != nil {
		return "", err
	}
	return threadJSON, nil
}

func readChildThreadForDeliveryTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, childThreadID string) (string, string, error) {
	row := tx.QueryRow(ctx,
		`SELECT id, parent_thread_id, role, status, agent_type, task_name, created_at, updated_at, closed_at
		   FROM session_threads
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND parent_thread_id = $4
		    AND role = 'subagent'`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		childThreadID,
		scope.GetSessionThreadId(),
	)
	threadJSON, statusValue, err := scanThreadJSONWithStatus(row)
	if err != nil {
		return "", "", err
	}
	return threadJSON, statusValue, nil
}

func scanThreadJSON(scanner interface {
	Scan(dest ...any) error
}) (string, error) {
	threadJSON, _, err := scanThreadJSONWithStatus(scanner)
	return threadJSON, err
}

func scanThreadJSONWithStatus(scanner interface {
	Scan(dest ...any) error
}) (string, string, error) {
	var threadID string
	var parentThreadID sql.NullString
	var role string
	var statusValue string
	var agentType string
	var taskName sql.NullString
	var createdAt time.Time
	var updatedAt time.Time
	var closedAt sql.NullTime
	if err := scanner.Scan(&threadID, &parentThreadID, &role, &statusValue, &agentType, &taskName, &createdAt, &updatedAt, &closedAt); dbconnect.IsNoRows(err) {
		return "", "", status.Error(codes.NotFound, "child thread not found")
	} else if err != nil {
		return "", "", err
	}
	threadJSON, err := marshalBridgeJSON(map[string]any{
		"session_thread_id": threadID,
		"parent_thread_id":  nullableJSONString(parentThreadID),
		"role":              role,
		"status":            statusValue,
		"agent_type":        agentType,
		"task_name":         nullableJSONString(taskName),
		"created_at":        createdAt.UTC().Format(time.RFC3339Nano),
		"updated_at":        updatedAt.UTC().Format(time.RFC3339Nano),
		"closed_at":         nullableJSONTime(closedAt),
	})
	return threadJSON, statusValue, err
}
