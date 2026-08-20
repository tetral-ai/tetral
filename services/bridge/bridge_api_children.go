package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

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

const (
	childCreateSourceSubagent        = "subagent_spawn"
	childCreateSourceReviewerTrunk   = "reviewer_trunk_ensure"
	childCreateSourceReviewerSidecar = "reviewer_sidecar_ensure"
	actorTaskNameMaxBytes            = 128
	actorParentMessageRefsMax        = 8192
)

func (s *PostgreSQLBridgeAPIStore) CreateSubagentThread(ctx context.Context, request *bridgev1.CreateSubagentThreadRequest) (response *bridgev1.CreateSubagentThreadResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "create_subagent_thread", request.GetSourceToolUseEventId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetSourceToolUseEventId() == "" || !validActorTaskName(request.GetTaskName()) ||
		!validActorIdentity(request.GetAgentType()) || !validActorInitialPrompt(request.GetInitialPrompt()) {
		return nil, status.Error(codes.InvalidArgument, "sub-agent source Tool, task name, initial prompt, and scope are required")
	}
	if !validParentMessageSequences(request.GetParentMessageSequences()) {
		return nil, status.Error(codes.InvalidArgument, "invalid sub-agent parent Message references")
	}
	parentMessageSequencesJSON, _ := json.Marshal(request.GetParentMessageSequences())
	requestHash := bridgeRequestHash(bridgeOpCreateChildThread, childCreateSourceSubagent, request.GetSourceToolUseEventId(), request.GetTaskName(), request.GetAgentType(), request.GetInitialPrompt(), string(parentMessageSequencesJSON))
	now := s.now()
	phase = "durable_transaction"
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.create_subagent_thread", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationBySourceTx(ctx, tx, request.GetScope(), bridgeOpCreateChildThread, childCreateSourceSubagent, request.GetSourceToolUseEventId()); err != nil {
			return err
		} else if ok {
			result, err := replayCreatedChild(existing, requestHash)
			if err != nil {
				return err
			}
			response = &bridgev1.CreateSubagentThreadResponse{Outcome: &bridgev1.CreateSubagentThreadResponse_Duplicate{Duplicate: &bridgev1.CreateSubagentThreadDuplicate{ChildThreadId: result.ChildThreadID}}}
			return nil
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId(), "child_create"); err != nil {
			return err
		}
		parentThreadID := request.GetScope().GetSessionThreadId()
		if err := requireOpenChildParentTx(ctx, tx, request.GetScope(), parentThreadID); err != nil {
			return err
		}
		prefix, err := loadDeclaredSubagentPrefixTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId(), request.GetParentMessageSequences())
		if err != nil {
			return err
		}
		childThreadID := id.New("thr_")
		if err := verifyChildTaskNameAvailableTx(ctx, tx, request.GetScope(), parentThreadID, childThreadID, "subagent", request.GetTaskName()); err != nil {
			return err
		}
		result, err := insertOwnedChildThreadTx(ctx, tx, request.GetScope(), childThreadID, parentThreadID, "subagent", "public", request.GetAgentType(), request.GetTaskName(), false, request.GetSourceToolUseEventId(), prefix, now)
		if err != nil {
			return err
		}
		envelope, err := appendDeclaredSubagentInitialEnvelopeTx(
			ctx, tx, request.GetScope(), childThreadID, request.GetSourceToolUseEventId(), request.GetTaskName(), request.GetInitialPrompt(), now,
		)
		if err != nil {
			return err
		}
		_, err = appendDeclaredSubagentInitialReceivedEventTx(ctx, tx, scopeForThread(request.GetScope(), childThreadID), envelope, now)
		if err != nil {
			return err
		}
		if err := persistCreatedChildOperationTx(ctx, tx, request.GetScope(), childCreateSourceSubagent, request.GetSourceToolUseEventId(), requestHash, result, now); err != nil {
			return err
		}
		response = &bridgev1.CreateSubagentThreadResponse{Outcome: &bridgev1.CreateSubagentThreadResponse_Committed{Committed: &bridgev1.CreateSubagentThreadCommitted{ChildThreadId: childThreadID}}}
		return nil
	})
	return response, err
}

func (s *PostgreSQLBridgeAPIStore) EnsureApprovalReviewerTrunk(ctx context.Context, request *bridgev1.EnsureApprovalReviewerTrunkRequest) (response *bridgev1.EnsureApprovalReviewerTrunkResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "ensure_approval_reviewer_trunk", request.GetEnsureOperationId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetEnsureOperationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reviewer trunk ensure operation is required")
	}
	requestHash := bridgeRequestHash(bridgeOpCreateChildThread, childCreateSourceReviewerTrunk, request.GetEnsureOperationId())
	now := s.now()
	phase = "durable_transaction"
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.ensure_approval_reviewer_trunk", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationBySourceTx(ctx, tx, request.GetScope(), bridgeOpCreateChildThread, childCreateSourceReviewerTrunk, request.GetEnsureOperationId()); err != nil {
			return err
		} else if ok {
			result, err := replayCreatedChild(existing, requestHash)
			if err != nil {
				return err
			}
			response = &bridgev1.EnsureApprovalReviewerTrunkResponse{Outcome: &bridgev1.EnsureApprovalReviewerTrunkResponse_Duplicate{Duplicate: &bridgev1.EnsureApprovalReviewerTrunkDuplicate{ReviewerThreadId: result.ChildThreadID}}}
			return nil
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		parentThreadID := request.GetScope().GetSessionThreadId()
		if err := requireOpenChildParentTx(ctx, tx, request.GetScope(), parentThreadID); err != nil {
			return err
		}
		if err := demoteApprovalReviewerTrunkTx(ctx, tx, request.GetScope(), parentThreadID, now); err != nil {
			return err
		}
		childThreadID := id.New("thrd_aprv_")
		result, err := insertOwnedChildThreadTx(ctx, tx, request.GetScope(), childThreadID, parentThreadID, "approval_reviewer", "internal", "approval_reviewer", "", true, "", nil, now)
		if err != nil {
			return err
		}
		if err := persistCreatedChildOperationTx(ctx, tx, request.GetScope(), childCreateSourceReviewerTrunk, request.GetEnsureOperationId(), requestHash, result, now); err != nil {
			return err
		}
		response = &bridgev1.EnsureApprovalReviewerTrunkResponse{Outcome: &bridgev1.EnsureApprovalReviewerTrunkResponse_Committed{Committed: &bridgev1.EnsureApprovalReviewerTrunkCommitted{ReviewerThreadId: childThreadID}}}
		return nil
	})
	return response, err
}

func (s *PostgreSQLBridgeAPIStore) EnsureApprovalReviewerSidecar(ctx context.Context, request *bridgev1.EnsureApprovalReviewerSidecarRequest) (response *bridgev1.EnsureApprovalReviewerSidecarResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "ensure_approval_reviewer_sidecar", request.GetReviewId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetReviewId() == "" {
		return nil, status.Error(codes.InvalidArgument, "reviewer sidecar review id is required")
	}
	requestHash := bridgeRequestHash(bridgeOpCreateChildThread, childCreateSourceReviewerSidecar, request.GetReviewId())
	now := s.now()
	phase = "durable_transaction"
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.ensure_approval_reviewer_sidecar", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationBySourceTx(ctx, tx, request.GetScope(), bridgeOpCreateChildThread, childCreateSourceReviewerSidecar, request.GetReviewId()); err != nil {
			return err
		} else if ok {
			result, err := replayCreatedChild(existing, requestHash)
			if err != nil {
				return err
			}
			response = &bridgev1.EnsureApprovalReviewerSidecarResponse{Outcome: &bridgev1.EnsureApprovalReviewerSidecarResponse_Duplicate{Duplicate: &bridgev1.EnsureApprovalReviewerSidecarDuplicate{ReviewerThreadId: result.ChildThreadID}}}
			return nil
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		parentThreadID := request.GetScope().GetSessionThreadId()
		if err := requireOpenChildParentTx(ctx, tx, request.GetScope(), parentThreadID); err != nil {
			return err
		}
		prefix, err := selectReviewerSidecarPrefixTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		childThreadID := id.New("thrd_aprv_sidecar_")
		result, err := insertOwnedChildThreadTx(ctx, tx, request.GetScope(), childThreadID, parentThreadID, "approval_reviewer", "internal", "approval_reviewer", "", false, "", prefix, now)
		if err != nil {
			return err
		}
		if err := persistCreatedChildOperationTx(ctx, tx, request.GetScope(), childCreateSourceReviewerSidecar, request.GetReviewId(), requestHash, result, now); err != nil {
			return err
		}
		response = &bridgev1.EnsureApprovalReviewerSidecarResponse{Outcome: &bridgev1.EnsureApprovalReviewerSidecarResponse_Committed{Committed: &bridgev1.EnsureApprovalReviewerSidecarCommitted{ReviewerThreadId: childThreadID}}}
		return nil
	})
	return response, err
}

// AdmitApprovalReviewInput creates the synchronous Inbox authority consumed by
// one hot reviewer execution. Review text remains Runtime-owned and arrives
// later through CommitInputs; this operation owns only durable target,
// idempotency, current binding, and accepted custody. Caller and scope
// authority are checked first, then an exact retained custody replay is resolved
// before a newly requested admission is tested against the Session barrier.
func (s *PostgreSQLBridgeAPIStore) AdmitApprovalReviewInput(ctx context.Context, request *bridgev1.AdmitApprovalReviewInputRequest) (response *bridgev1.AdmitApprovalReviewInputResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "admit_approval_review_input", request.GetReviewId(), phase, resultErr)
	}()
	if request.GetScope() == nil || !validActorIdentity(request.GetReviewerThreadId()) || !validActorIdentity(request.GetReviewId()) {
		return nil, status.Error(codes.InvalidArgument, "approval review admission identities are required")
	}
	runtimeInputID := approvalReviewRuntimeInputID(request.GetScope(), request.GetReviewId())
	committed := false
	now := s.now()
	phase = "durable_transaction"
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.admit_approval_review_input", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var existingSessionID, existingReviewerThreadID, existingInputKind, existingStatus, existingBindingID, existingPodUID string
		var existingBindingGeneration int64
		err := tx.QueryRow(ctx, `SELECT session_id,session_thread_id,input_kind,status,binding_id,binding_generation,target_pod_uid
			FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2 FOR UPDATE`,
			request.GetScope().GetWorkspaceId(), runtimeInputID,
		).Scan(&existingSessionID, &existingReviewerThreadID, &existingInputKind, &existingStatus, &existingBindingID, &existingBindingGeneration, &existingPodUID)
		if err == nil {
			if existingSessionID != request.GetScope().GetSessionId() || existingReviewerThreadID != request.GetReviewerThreadId() ||
				existingInputKind != "approval_review" || existingBindingID != request.GetScope().GetBinding().GetBindingId() ||
				existingBindingGeneration != request.GetScope().GetBinding().GetBindingGeneration() || existingPodUID != request.GetScope().GetBinding().GetTargetPodUid() {
				return status.Error(codes.AlreadyExists, "approval review admission idempotency conflict")
			}
			if _, err := validateApprovalReviewerAdmissionTargetTx(ctx, tx, request, true); err != nil {
				return err
			}
			if existingStatus == "accepted" || existingStatus == "committed" || existingStatus == "cancelled" {
				return nil
			}
		} else if !dbconnect.IsNoRows(err) {
			return err
		}
		if err := requireSessionMutationAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		isTrunk, err := validateApprovalReviewerAdmissionTargetTx(ctx, tx, request, false)
		if err != nil {
			return err
		}
		if !isTrunk {
			existing, ok, err := readBridgeOperationBySourceTx(ctx, tx, request.GetScope(), bridgeOpCreateChildThread, childCreateSourceReviewerSidecar, request.GetReviewId())
			if err != nil {
				return err
			}
			if !ok {
				return status.Error(codes.FailedPrecondition, "approval reviewer sidecar admission authority is missing")
			}
			var created createChildThreadResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &created); err != nil || created.ChildThreadID != request.GetReviewerThreadId() {
				return status.Error(codes.FailedPrecondition, "approval reviewer sidecar admission authority is invalid")
			}
		}
		result, err := tx.Exec(ctx, `INSERT INTO session_runtime_inbox (
			workspace_id,session_id,session_thread_id,runtime_input_id,input_kind,event_ids_json,
			status,binding_id,binding_generation,target_pod_uid,created_at,updated_at
		) VALUES ($1,$2,$3,$4,'approval_review','[]','accepted',$5,$6,$7,$8,$8)
		ON CONFLICT (workspace_id,runtime_input_id) DO NOTHING`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetReviewerThreadId(), runtimeInputID,
			request.GetScope().GetBinding().GetBindingId(), request.GetScope().GetBinding().GetBindingGeneration(), request.GetScope().GetBinding().GetTargetPodUid(), now,
		)
		if err != nil {
			return err
		}
		if rowsAffected(result) {
			committed = true
			return nil
		}
		var sessionID, reviewerThreadID, inputKind, inboxStatus, bindingID, podUID string
		var bindingGeneration int64
		if err := tx.QueryRow(ctx, `SELECT session_id,session_thread_id,input_kind,status,binding_id,binding_generation,target_pod_uid
			FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2 FOR UPDATE`,
			request.GetScope().GetWorkspaceId(), runtimeInputID,
		).Scan(&sessionID, &reviewerThreadID, &inputKind, &inboxStatus, &bindingID, &bindingGeneration, &podUID); err != nil {
			return err
		}
		if sessionID != request.GetScope().GetSessionId() || reviewerThreadID != request.GetReviewerThreadId() || inputKind != "approval_review" ||
			(inboxStatus != "accepted" && inboxStatus != "committed" && inboxStatus != "cancelled") || bindingID != request.GetScope().GetBinding().GetBindingId() ||
			bindingGeneration != request.GetScope().GetBinding().GetBindingGeneration() || podUID != request.GetScope().GetBinding().GetTargetPodUid() {
			return status.Error(codes.AlreadyExists, "approval review admission idempotency conflict")
		}
		return nil
	}); err != nil {
		if isSessionInterruptBarrierStaleError(err) {
			return &bridgev1.AdmitApprovalReviewInputResponse{Outcome: &bridgev1.AdmitApprovalReviewInputResponse_Stale{
				Stale: &bridgev1.AdmitApprovalReviewInputStale{},
			}}, nil
		}
		return nil, err
	}
	if committed {
		return &bridgev1.AdmitApprovalReviewInputResponse{Outcome: &bridgev1.AdmitApprovalReviewInputResponse_Committed{Committed: &bridgev1.AdmitApprovalReviewInputCommitted{RuntimeInputId: runtimeInputID}}}, nil
	}
	return &bridgev1.AdmitApprovalReviewInputResponse{Outcome: &bridgev1.AdmitApprovalReviewInputResponse_Duplicate{Duplicate: &bridgev1.AdmitApprovalReviewInputDuplicate{RuntimeInputId: runtimeInputID}}}, nil
}

func approvalReviewRuntimeInputID(scope *bridgev1.RuntimeScope, reviewID string) string {
	return strings.Replace(stableRuntimeID("approval_review_input", scope.GetWorkspaceId(), scope.GetSessionId(), reviewID), "stid_", "rin_", 1)
}

func validateApprovalReviewerAdmissionTargetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.AdmitApprovalReviewInputRequest,
	allowTerminal bool,
) (bool, error) {
	var parentThreadID, role, visibility, statusValue string
	var isTrunk bool
	if err := tx.QueryRow(ctx, `SELECT parent_thread_id,role,visibility,status,is_trunk
		FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND id=$3
		FOR UPDATE`, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetReviewerThreadId()).Scan(
		&parentThreadID, &role, &visibility, &statusValue, &isTrunk,
	); dbconnect.IsNoRows(err) {
		return false, status.Error(codes.FailedPrecondition, "approval reviewer admission target is missing")
	} else if err != nil {
		return false, err
	}
	if parentThreadID != request.GetScope().GetSessionThreadId() || role != "approval_reviewer" || visibility != "internal" ||
		(!allowTerminal && (statusValue == "closed_for_runtime" || statusValue == "failed" || statusValue == "terminated")) {
		return false, status.Error(codes.FailedPrecondition, "approval reviewer admission target is invalid")
	}
	return isTrunk, nil
}

func (s *PostgreSQLBridgeAPIStore) ResolveChildThread(ctx context.Context, request *bridgev1.ResolveChildThreadRequest) (response *bridgev1.ResolveChildThreadResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "resolve_child_thread", request.GetChildThreadId(), phase, resultErr)
	}()
	if request.GetChildThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "child thread id is required")
	}
	phase = "durable_read"
	child, err := s.readChildThreadFact(ctx, request.GetScope(), request.GetChildThreadId(), bridgeOpResolveChildThread)
	if err != nil {
		return nil, err
	}
	return &bridgev1.ResolveChildThreadResponse{Outcome: &bridgev1.ResolveChildThreadResponse_Resolved{Resolved: &bridgev1.ResolveChildThreadResolved{Child: child}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) ListChildThreads(ctx context.Context, request *bridgev1.ListChildThreadsRequest) (response *bridgev1.ListChildThreadsResponse, resultErr error) {
	parentThreadID := defaultString(request.GetParentThreadId(), request.GetScope().GetSessionThreadId())
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "list_child_threads", parentThreadID, phase, resultErr)
	}()
	var children []*bridgev1.ChildThreadFact
	phase = "durable_read"
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.list_child_threads", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		rows, err := tx.Query(ctx,
			`SELECT id, parent_thread_id, role, visibility, status, agent_type, task_name
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
			child, err := scanChildThreadFact(rows)
			if err != nil {
				return err
			}
			children = append(children, child)
		}
		return rows.Err()
	}); err != nil {
		return nil, err
	}
	return &bridgev1.ListChildThreadsResponse{Outcome: &bridgev1.ListChildThreadsResponse_Completed{Completed: &bridgev1.ListChildThreadsCompleted{Children: children}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) DeliverInterAgentMail(ctx context.Context, request *bridgev1.DeliverInterAgentMailRequest) (response *bridgev1.DeliverInterAgentMailResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "deliver_inter_agent_mail", request.GetDeliveryId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetDeliveryId() == "" || request.GetTargetThreadId() == "" || request.GetSourceToolUseEventId() == "" || request.GetContent() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent mail scope, identities, target, and content are required")
	}
	now := s.now()
	requestHash := bridgeRequestHash(
		bridgeOpDeliverInterAgentMail,
		request.GetDeliveryId(),
		request.GetTargetThreadId(),
		request.GetSourceToolUseEventId(),
		request.GetContent(),
	)
	phase = "durable_transaction"
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.deliver_inter_agent_mail", func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpDeliverInterAgentMail, request.GetDeliveryId()); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "agent mail delivery idempotency conflict")
			}
			response = &bridgev1.DeliverInterAgentMailResponse{Outcome: &bridgev1.DeliverInterAgentMailResponse_Duplicate{Duplicate: &bridgev1.DeliverInterAgentMailDuplicate{}}}
			return nil
		}
		barrier, active, err := activeSessionInterruptBarrierTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId())
		if err != nil {
			return err
		}
		if active {
			if request.GetScope().GetSessionThreadId() == barrier.sessionThreadID || request.GetTargetThreadId() != barrier.sessionThreadID {
				return sessionInterruptBarrierStaleError(status.Error(codes.FailedPrecondition, "session interrupt barrier is active"))
			}
			ctx = withInterAgentMailBarrierBirth(ctx, interAgentMailBarrierBirthAuthority{
				workspaceID:       request.GetScope().GetWorkspaceId(),
				sessionID:         request.GetScope().GetSessionId(),
				runtimeInputID:    barrier.runtimeInputID,
				interruptedThread: barrier.sessionThreadID,
				sourceThread:      request.GetScope().GetSessionThreadId(),
				targetThread:      request.GetTargetThreadId(),
			})
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId(), "child_message"); err != nil {
			return err
		}
		envelope, err := appendSubagentMailEnvelopeTx(
			ctx,
			tx,
			request.GetScope(),
			request.GetDeliveryId(),
			request.GetTargetThreadId(),
			request.GetSourceToolUseEventId(),
			request.GetContent(),
			now,
		)
		if err != nil {
			return err
		}
		binding, err := readRuntimeBindingForDeliveryTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId())
		if err != nil {
			return err
		}
		if _, err := admitAgentMailDeliveryTx(ctx, tx, scopeForThread(request.GetScope(), envelope.TargetThreadID), envelope, binding, now); err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation: bridgeOpDeliverInterAgentMail, IdempotencyKey: request.GetDeliveryId(), RequestHash: requestHash,
			AckStatus: bridgeAckCommitted, Now: now,
		}); err != nil {
			return err
		}
		response = &bridgev1.DeliverInterAgentMailResponse{Outcome: &bridgev1.DeliverInterAgentMailResponse_Committed{Committed: &bridgev1.DeliverInterAgentMailCommitted{}}}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

// ReadAgentMail projects the latest durable envelope owned by the addressed
// target. It never re-enters sender-scoped delivery or mutates Inbox custody.
func (s *PostgreSQLBridgeAPIStore) ReadAgentMail(ctx context.Context, request *bridgev1.ReadAgentMailRequest) (response *bridgev1.ReadAgentMailResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "read_agent_mail", request.GetSourceThreadId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetSourceThreadId() == "" {
		return nil, status.Error(codes.InvalidArgument, "agent mail target scope and source thread are required")
	}
	response = &bridgev1.ReadAgentMailResponse{}
	phase = "durable_read"
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.read_agent_mail", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var deliveryID, storedMessageJSON string
		err := tx.QueryRow(ctx,
			`SELECT sent.payload_json::jsonb ->> 'delivery_id',
			        (sent.payload_json::jsonb -> 'message')::text
			   FROM session_events sent
			   JOIN session_threads source
			     ON source.workspace_id=sent.workspace_id
			    AND source.session_id=sent.session_id
			    AND source.id=$4
			    AND source.role='subagent'
			    AND source.parent_thread_id=$3
			  WHERE sent.workspace_id=$1
			    AND sent.session_id=$2
			    AND sent.session_thread_id=$4
			    AND sent.type='agent.thread_message_sent'
			    AND sent.payload_json::jsonb ->> 'source_thread_id'=$4
			    AND sent.payload_json::jsonb ->> 'target_thread_id'=$3
			  ORDER BY sent.sequence DESC, sent.event_id DESC
			  LIMIT 1`,
			request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(), request.GetSourceThreadId(),
		).Scan(&deliveryID, &storedMessageJSON)
		if dbconnect.IsNoRows(err) {
			response.Outcome = &bridgev1.ReadAgentMailResponse_Empty{Empty: &bridgev1.ReadAgentMailEmpty{}}
			return nil
		}
		if err != nil {
			return err
		}
		if deliveryID == "" || storedMessageJSON == "" {
			return status.Error(codes.FailedPrecondition, "agent mail envelope is malformed")
		}
		messageJSON, err := validatedPublicInterAgentMessageJSON(json.RawMessage(storedMessageJSON))
		if err != nil {
			return err
		}
		content, err := agentMailContentFromPublicMessage(messageJSON)
		if err != nil {
			return err
		}
		response.Outcome = &bridgev1.ReadAgentMailResponse_Found{Found: &bridgev1.ReadAgentMailFound{
			DeliveryId: deliveryID, Content: content,
		}}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *PostgreSQLBridgeAPIStore) CloseChildControl(ctx context.Context, request *bridgev1.CloseChildControlRequest) (response *bridgev1.CloseChildControlResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "close_child_control", request.GetControlOperationId(), phase, resultErr)
	}()
	if request.GetScope() == nil || !validActorIdentity(request.GetControlOperationId()) {
		return nil, status.Error(codes.InvalidArgument, "child close control operation is required")
	}
	var childThreadID string
	phase = "derive_authority"
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.derive_child_close", func(tx *dbconnect.Tx) error {
		command, err := loadCommittedChildControlCommandTx(ctx, tx, request.GetScope(), request.GetControlOperationId())
		if err != nil {
			return err
		}
		if command.action != bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE || !command.includeDescendants {
			return status.Error(codes.FailedPrecondition, "control operation is not a child close")
		}
		childThreadID = command.rootChildThreadID
		return nil
	}); err != nil {
		return nil, err
	}
	phase = "durable_transaction"
	result, err := s.closeChildLifecycle(ctx, request.GetScope(), childThreadID, "tool_use", request.GetControlOperationId(), bridgeOpCloseChildControl)
	if err != nil {
		return nil, err
	}
	if result.stale {
		return &bridgev1.CloseChildControlResponse{Outcome: &bridgev1.CloseChildControlResponse_Stale{Stale: &bridgev1.CloseChildControlStale{}}}, nil
	}
	if result.duplicate {
		return &bridgev1.CloseChildControlResponse{Outcome: &bridgev1.CloseChildControlResponse_Duplicate{Duplicate: &bridgev1.CloseChildControlDuplicate{Children: result.children}}}, nil
	}
	return &bridgev1.CloseChildControlResponse{Outcome: &bridgev1.CloseChildControlResponse_Committed{Committed: &bridgev1.CloseChildControlCommitted{Children: result.children}}}, nil
}

func (s *PostgreSQLBridgeAPIStore) CloseApprovalReviewer(ctx context.Context, request *bridgev1.CloseApprovalReviewerRequest) (response *bridgev1.CloseApprovalReviewerResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "close_approval_reviewer", request.GetReviewId(), phase, resultErr)
	}()
	if request.GetScope() == nil || !validActorIdentity(request.GetReviewerThreadId()) || !validActorIdentity(request.GetReviewId()) {
		return nil, status.Error(codes.InvalidArgument, "reviewer close identities are required")
	}
	phase = "durable_transaction"
	result, err := s.closeChildLifecycle(ctx, request.GetScope(), request.GetReviewerThreadId(), "approval_review", request.GetReviewId(), bridgeOpCloseApprovalReviewer)
	if err != nil {
		return nil, err
	}
	if result.stale {
		return &bridgev1.CloseApprovalReviewerResponse{Outcome: &bridgev1.CloseApprovalReviewerResponse_Stale{Stale: &bridgev1.CloseApprovalReviewerStale{}}}, nil
	}
	if result.duplicate {
		return &bridgev1.CloseApprovalReviewerResponse{Outcome: &bridgev1.CloseApprovalReviewerResponse_Duplicate{Duplicate: &bridgev1.CloseApprovalReviewerDuplicate{}}}, nil
	}
	return &bridgev1.CloseApprovalReviewerResponse{Outcome: &bridgev1.CloseApprovalReviewerResponse_Committed{Committed: &bridgev1.CloseApprovalReviewerCommitted{}}}, nil
}

type closeChildLifecycleResult struct {
	children  []*bridgev1.ChildLifecycleResult
	duplicate bool
	stale     bool
}

func (s *PostgreSQLBridgeAPIStore) closeChildLifecycle(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
	childThreadID string,
	sourceKind string,
	sourceCommandID string,
	operationKind string,
) (*closeChildLifecycleResult, error) {
	now := s.now()
	command, err := parseChildLifecycleCommand(
		scope,
		childThreadID,
		now,
		sourceKind,
		sourceCommandID,
		operationKind,
		"close",
	)
	if err != nil {
		return nil, err
	}
	var (
		duplicate          bool
		children           []*bridgev1.ChildLifecycleResult
		custodyTransitions childCloseCustodyTransitions
	)
	if err := s.withScopeTx(ctx, scope, "agentruntimebridge."+operationKind, func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, scope); err != nil {
			return err
		}
		if err := validateChildLifecycleSourceTx(ctx, tx, scope, childThreadID, command); err != nil {
			return err
		}
		if existingChildren, ok, err := readChildLifecycleOperationResultSetTx(
			ctx,
			tx,
			scope,
			command,
			operationKind,
		); err != nil {
			return err
		} else if ok {
			children = existingChildren
			duplicate = true
			return nil
		}
		targetIDs, err := childLifecycleSubtreeIDsTx(ctx, tx, scope, childThreadID)
		if err != nil {
			return err
		}
		if command.sourceKind == "tool_use" {
			if err := validateChildCloseCensusTx(ctx, tx, scope, childThreadID, targetIDs, command.sourceCommandID); err != nil {
				return err
			}
		}
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		lockedThreads := make(map[string]threadMutationScope, len(targetIDs))
		for _, targetID := range targetIDs {
			mutation, err := lockThreadMutationTx(ctx, tx, scopeForThread(scope, targetID))
			if err != nil {
				return err
			}
			if mutation.role == "main" {
				return status.Error(codes.FailedPrecondition, "main thread cannot be marked as child")
			}
			lockedThreads[targetID] = mutation
		}
		custodyTransitions, err = settleChildCloseRuntimeInputsTx(ctx, tx, scope, targetIDs, now)
		if err != nil {
			return err
		}
		for _, threadID := range targetIDs {
			childScope := scopeForThread(scope, threadID)
			threadScope := lockedThreads[threadID]
			disposition := bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED
			switch threadScope.status {
			case "closed_for_runtime":
				// The per-target stored result still records this target in the frozen subtree.
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
					scope.GetWorkspaceId(),
					scope.GetSessionId(),
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
			child := &bridgev1.ChildLifecycleResult{ChildThreadId: threadID, Disposition: disposition}
			resultJSON, err := marshalBridgeJSON(childLifecycleStoredResult{ChildThreadID: threadID, Disposition: disposition.String()})
			if err != nil {
				return err
			}
			if err := insertBridgeDeclarationOperationTx(
				ctx,
				tx,
				childScope,
				operationKind,
				command.operationSourceKind,
				command.operationID,
				command.declarationDigest,
				resultJSON,
				now,
			); err != nil {
				return err
			}
			children = append(children, child)
		}
		return nil
	}); err != nil {
		if isSessionInterruptBarrierStaleError(err) {
			return &closeChildLifecycleResult{stale: true}, nil
		}
		return nil, err
	}
	logRuntimeInputCustodyTransition(s.Logger, scope, "accepted_to_parked", custodyTransitions.parked)
	logRuntimeInputCustodyTransition(s.Logger, scope, "accepted_to_cancelled", custodyTransitions.cancelled)
	current, err := s.runtimeScopeApplicationCurrent(ctx, scope)
	if err != nil {
		return nil, err
	}
	if !current {
		return &closeChildLifecycleResult{stale: true}, nil
	}
	return &closeChildLifecycleResult{
		children:  children,
		duplicate: duplicate,
	}, nil
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

func (s *PostgreSQLBridgeAPIStore) MarkChildThreadActive(ctx context.Context, request *bridgev1.MarkChildThreadActiveRequest) (response *bridgev1.MarkChildThreadActiveResponse, resultErr error) {
	phase := "validate"
	defer func() {
		logActorBoundaryRejected(s.Logger, request.GetScope(), "mark_child_thread_active", request.GetSourceToolUseEventId(), phase, resultErr)
	}()
	if request.GetScope() == nil || request.GetSourceToolUseEventId() == "" || !validActorIdentity(request.GetTargetChildThreadId()) {
		return nil, status.Error(codes.InvalidArgument, "child resume scope, source Tool identity, and target are required")
	}
	now := s.now()
	var (
		childThreadID string
		command       childLifecycleCommand
		duplicate     bool
		results       []*bridgev1.ChildLifecycleResult
	)
	phase = "durable_transaction"
	err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge."+bridgeOpMarkChildThreadActive, func(tx *dbconnect.Tx) error {
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		childThreadID = request.GetTargetChildThreadId()
		if err := validateDeclaredPublicSubagentTargetTx(ctx, tx, request.GetScope(), childThreadID); err != nil {
			return err
		}
		var err error
		command, err = parseChildLifecycleCommand(
			request.GetScope(), childThreadID, now, "tool_use", request.GetSourceToolUseEventId(),
			bridgeOpMarkChildThreadActive, "resume",
		)
		if err != nil {
			return err
		}
		childScope := scopeForThread(request.GetScope(), childThreadID)
		if existingResults, ok, err := readChildLifecycleOperationResultSetTx(
			ctx,
			tx,
			request.GetScope(),
			command,
			bridgeOpMarkChildThreadActive,
		); err != nil {
			return err
		} else if ok {
			if len(existingResults) != 1 {
				return status.Error(codes.FailedPrecondition, "child resume stored result set is invalid")
			}
			results = existingResults
			duplicate = true
			return nil
		}
		if err := lockExecutableToolRouteTx(ctx, tx, request.GetScope(), request.GetSourceToolUseEventId(), "child_resume"); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, childScope)
		if err != nil {
			return err
		}
		if threadScope.role == "main" {
			return status.Error(codes.FailedPrecondition, "main thread cannot be marked as child")
		}
		if closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), childThreadID); err != nil {
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
				childThreadID,
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
		resultJSON, err := marshalBridgeJSON(childLifecycleStoredResult{ChildThreadID: childThreadID, Disposition: disposition.String()})
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
			resultJSON,
			now,
		); err != nil {
			return err
		}
		results = []*bridgev1.ChildLifecycleResult{{ChildThreadId: childThreadID, Disposition: disposition}}
		return nil
	})
	if err != nil {
		if isSessionInterruptBarrierStaleError(err) {
			return &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Stale{Stale: &bridgev1.MarkChildThreadActiveStale{}}}, nil
		}
		return nil, err
	}
	phase = "observe_application"
	current, err := s.runtimeScopeApplicationCurrent(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	if !current {
		return &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Stale{Stale: &bridgev1.MarkChildThreadActiveStale{}}}, nil
	}
	if len(results) != 1 || results[0].GetDisposition() == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_UNSPECIFIED {
		return nil, status.Error(codes.FailedPrecondition, "child resume disposition is invalid")
	}
	disposition := results[0].GetDisposition()
	if !duplicate {
		return &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Committed{Committed: &bridgev1.MarkChildThreadActiveCommitted{Disposition: disposition}}}, nil
	}
	return &bridgev1.MarkChildThreadActiveResponse{Outcome: &bridgev1.MarkChildThreadActiveResponse_Duplicate{Duplicate: &bridgev1.MarkChildThreadActiveDuplicate{Disposition: disposition}}}, nil
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
	sourceKind string,
	sourceCommandID string,
	operationKind string,
	action string,
) (childLifecycleCommand, error) {
	requestedAt := requestedTime.UTC().Format(time.RFC3339Nano)
	if sourceKind != "tool_use" && sourceKind != "approval_review" {
		return childLifecycleCommand{}, status.Error(codes.InvalidArgument, "child lifecycle source is required")
	}
	if sourceCommandID == "" {
		return childLifecycleCommand{}, status.Error(codes.InvalidArgument, "child lifecycle source identity is required")
	}
	operationSourceKind := "child_close_command"
	operationIDParts := []string{"child_tree_close", sourceCommandID, childThreadID}
	operationID := ""
	if action == "resume" {
		operationSourceKind = "child_resume_command"
		operationIDParts = []string{"child_resume", sourceCommandID}
	} else if sourceKind == "tool_use" {
		// The Bridge-owned control operation is the sole close fence across
		// admission, completion, lifecycle results, and hot release.
		operationID = sourceCommandID
	}
	if operationID == "" {
		operationID = stableRuntimeID(operationIDParts...)
	}
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
		existing, ok, err := readBridgeOperationBySourceTx(ctx, tx, scope, bridgeOpCreateChildThread, childCreateSourceReviewerSidecar, command.sourceCommandID)
		if err != nil {
			return err
		}
		if !ok {
			return status.Error(codes.FailedPrecondition, "child lifecycle reviewer ensure operation is missing")
		}
		var created createChildThreadResult
		if err := json.Unmarshal([]byte(existing.ResultJSON), &created); err != nil || created.ChildThreadID == "" || childThreadID != created.ChildThreadID {
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
	if command.action == "close" {
		control, err := loadCommittedChildControlCommandTx(ctx, tx, scope, command.sourceCommandID)
		if err != nil {
			return err
		}
		if control.rootChildThreadID != childThreadID || control.action != bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE || !control.includeDescendants {
			return status.Error(codes.FailedPrecondition, "child lifecycle control operation does not own the durable child")
		}
		return nil
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

type childLifecycleStoredResult struct {
	ChildThreadID string `json:"childThreadId"`
	Disposition   string `json:"disposition"`
}

func readChildLifecycleOperationResultSetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	callerScope *bridgev1.RuntimeScope,
	command childLifecycleCommand,
	operationKind string,
) ([]*bridgev1.ChildLifecycleResult, bool, error) {
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
	var results []*bridgev1.ChildLifecycleResult
	for rows.Next() {
		var targetID, declarationDigest, receiptJSON string
		if err := rows.Scan(&targetID, &declarationDigest, &receiptJSON); err != nil {
			return nil, false, err
		}
		if declarationDigest != command.declarationDigest || receiptJSON == "" {
			return nil, false, status.Error(codes.AlreadyExists, "child lifecycle idempotency conflict")
		}
		result, err := unmarshalChildLifecycleStoredResult(receiptJSON, targetID, command.action)
		if err != nil {
			return nil, false, err
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	return results, len(results) > 0, nil
}

func unmarshalChildLifecycleStoredResult(raw, targetID, action string) (*bridgev1.ChildLifecycleResult, error) {
	var stored childLifecycleStoredResult
	if err := json.Unmarshal([]byte(raw), &stored); err != nil || stored.ChildThreadID != targetID {
		return nil, status.Error(codes.FailedPrecondition, "child lifecycle result is invalid")
	}
	disposition := bridgev1.ChildLifecycleDisposition(bridgev1.ChildLifecycleDisposition_value[stored.Disposition])
	valid := disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_FAILED ||
		disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_PRESERVED_TERMINATED
	if action == "close" {
		valid = valid || disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_CLOSED ||
			disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_CLOSED
	} else {
		valid = valid || disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_RESUMED ||
			disposition == bridgev1.ChildLifecycleDisposition_CHILD_LIFECYCLE_DISPOSITION_ALREADY_ACTIVE
	}
	if !valid {
		return nil, status.Error(codes.FailedPrecondition, "child lifecycle result is invalid")
	}
	return &bridgev1.ChildLifecycleResult{ChildThreadId: targetID, Disposition: disposition}, nil
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

type threadContextPrefixEnvelope struct {
	SourceParentThreadID  string                      `json:"sourceParentThreadId"`
	ParentBoundaryEventID string                      `json:"parentBoundaryEventId"`
	Entries               []bridgeRuntimeContextEntry `json:"entries"`
}

func replayCreatedChild(existing bridgeOperation, requestHash string) (createChildThreadResult, error) {
	if existing.RequestHash != requestHash {
		return createChildThreadResult{}, status.Error(codes.AlreadyExists, "child thread idempotency conflict")
	}
	var result createChildThreadResult
	if err := json.Unmarshal([]byte(existing.ResultJSON), &result); err != nil || result.ChildThreadID == "" {
		return createChildThreadResult{}, status.Error(codes.FailedPrecondition, "stored child thread result is invalid")
	}
	return result, nil
}

func requireOpenChildParentTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, parentThreadID string) error {
	if err := verifyChildParentThreadTx(ctx, tx, scope, parentThreadID); err != nil {
		return err
	}
	closing, err := childcontrol.ThreadOrAncestorClosingTx(ctx, tx, scope.GetWorkspaceId(), scope.GetSessionId(), parentThreadID)
	if err != nil {
		return err
	}
	if closing {
		return status.Error(codes.FailedPrecondition, "parent thread is closing")
	}
	return nil
}

// loadDeclaredSubagentPrefixTx snapshots exactly the ordered sealed parent
// Messages declared by Runtime. Bridge verifies only durable ownership,
// ordering, sealing, and the source Assistant boundary; it does not interpret
// fork instructions or select conversation turns.
func loadDeclaredSubagentPrefixTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, sourceToolUseEventID string, messageSequences []int64) (*threadContextPrefixEnvelope, error) {
	var modelRequestID string
	if err := tx.QueryRow(ctx, `SELECT model_request_id
		FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4
		 AND type='agent.tool_use' AND visibility='public' AND model_request_id IS NOT NULL
		FOR SHARE`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), sourceToolUseEventID).Scan(&modelRequestID); dbconnect.IsNoRows(err) {
		return nil, status.Error(codes.FailedPrecondition, "sub-agent source Tool boundary is missing")
	} else if err != nil {
		return nil, err
	}
	var boundarySequence int64
	if err := tx.QueryRow(ctx, `SELECT sequence FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND model_request_id=$4`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&boundarySequence); dbconnect.IsNoRows(err) {
		return nil, status.Error(codes.FailedPrecondition, "sub-agent source request has no durable Assistant context")
	} else if err != nil {
		return nil, err
	}
	entries := make([]bridgeRuntimeContextEntry, 0, len(messageSequences))
	if len(messageSequences) > 0 {
		rows, err := tx.Query(ctx, `WITH requested AS (
			SELECT sequence, ordinality
			FROM unnest($5::bigint[]) WITH ORDINALITY AS selected(sequence, ordinality)
		) SELECT m.kind,m.sequence,m.data_json,
			CASE WHEN m.kind <> 'assistant' THEN true
			     WHEN m.model_request_id IS NULL THEN false
			     ELSE EXISTS (
			       SELECT 1 FROM session_events ended
			       WHERE ended.workspace_id=m.workspace_id AND ended.session_id=m.session_id
			        AND ended.session_thread_id=m.session_thread_id
			        AND ended.model_request_id=m.model_request_id
			        AND ended.type='span.model_request_end'
			     )
			END AS sealed
		FROM requested
		JOIN session_messages m
		  ON m.workspace_id=$1 AND m.session_id=$2 AND m.session_thread_id=$3
		 AND m.sequence=requested.sequence
		WHERE m.sequence < $4
		ORDER BY requested.ordinality`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), boundarySequence, messageSequences)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var kind, raw string
			var sequence int64
			var sealed bool
			if err := rows.Scan(&kind, &sequence, &raw, &sealed); err != nil {
				return nil, err
			}
			if !sealed {
				return nil, status.Error(codes.FailedPrecondition, "sub-agent parent Message reference is not sealed")
			}
			parts, err := decodeStoredRuntimeContextParts(raw)
			if err != nil {
				return nil, err
			}
			entries = append(entries, bridgeRuntimeContextEntry{MessageSequence: sequence, ContextKind: kind, Parts: parts})
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(entries) != len(messageSequences) {
			return nil, status.Error(codes.FailedPrecondition, "sub-agent parent Message reference is missing or outside the source boundary")
		}
	}
	return &threadContextPrefixEnvelope{
		SourceParentThreadID: scope.GetSessionThreadId(), ParentBoundaryEventID: sourceToolUseEventID, Entries: entries,
	}, nil
}

func selectReviewerSidecarPrefixTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) (*threadContextPrefixEnvelope, error) {
	var trunkThreadID string
	if err := tx.QueryRow(ctx, `SELECT id FROM session_threads
		WHERE workspace_id=$1 AND session_id=$2 AND parent_thread_id=$3
		 AND role='approval_reviewer' AND is_trunk FOR SHARE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
	).Scan(&trunkThreadID); dbconnect.IsNoRows(err) {
		return nil, status.Error(codes.FailedPrecondition, "approval reviewer trunk is missing")
	} else if err != nil {
		return nil, err
	}
	var boundaryEventID string
	var boundarySequence int64
	err := tx.QueryRow(ctx, `SELECT ended.event_id, assistant.sequence
		FROM session_events ended
		JOIN session_messages assistant
		  ON assistant.workspace_id=ended.workspace_id AND assistant.session_id=ended.session_id
		 AND assistant.session_thread_id=ended.session_thread_id AND assistant.model_request_id=ended.model_request_id
		WHERE ended.workspace_id=$1 AND ended.session_id=$2 AND ended.session_thread_id=$3
		 AND ended.type='span.model_request_end'
		 AND NOT COALESCE((ended.payload_json::jsonb ->> 'is_error')::boolean, FALSE)
		 AND NOT EXISTS (SELECT 1 FROM session_events rescheduled
		   WHERE rescheduled.workspace_id=ended.workspace_id AND rescheduled.session_id=ended.session_id
		    AND rescheduled.session_thread_id=ended.session_thread_id AND rescheduled.model_request_id=ended.model_request_id
		    AND rescheduled.type IN ('session.status_rescheduled','session.thread_status_rescheduled'))
		ORDER BY ended.sequence DESC LIMIT 1 FOR SHARE OF ended, assistant`,
		scope.GetWorkspaceId(), scope.GetSessionId(), trunkThreadID,
	).Scan(&boundaryEventID, &boundarySequence)
	if dbconnect.IsNoRows(err) {
		boundarySequence = 0
		if err := tx.QueryRow(ctx, `SELECT event_id FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND type='session.thread_created'
			ORDER BY sequence ASC LIMIT 1 FOR SHARE`, scope.GetWorkspaceId(), scope.GetSessionId(), trunkThreadID).Scan(&boundaryEventID); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	entries, _, err := loadDurablePrefixEntriesThroughTx(ctx, tx, scopeForThread(scope, trunkThreadID), boundarySequence)
	if err != nil {
		return nil, err
	}
	return &threadContextPrefixEnvelope{SourceParentThreadID: trunkThreadID, ParentBoundaryEventID: boundaryEventID, Entries: entries}, nil
}

func loadDurablePrefixEntriesThroughTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, boundarySequence int64) ([]bridgeRuntimeContextEntry, []string, error) {
	if boundarySequence == 0 {
		return []bridgeRuntimeContextEntry{}, []string{}, nil
	}
	rows, err := tx.Query(ctx, `WITH latest_compaction AS (
		SELECT MAX(sequence) AS boundary_sequence FROM session_messages
		 WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND kind='compaction' AND sequence <= $4
	) SELECT m.kind,m.sequence,m.data_json,m.model_request_id,
		CASE
		  WHEN m.kind <> 'assistant' OR m.model_request_id IS NULL THEN 'sealed'
		  WHEN NOT EXISTS (
		    SELECT 1 FROM session_events ended
		     WHERE ended.workspace_id=m.workspace_id AND ended.session_id=m.session_id
		       AND ended.session_thread_id=m.session_thread_id AND ended.model_request_id=m.model_request_id
		       AND ended.type='span.model_request_end'
		  ) THEN 'open'
		  WHEN EXISTS (
		    SELECT 1 FROM session_events ended
		     WHERE ended.workspace_id=m.workspace_id AND ended.session_id=m.session_id
		       AND ended.session_thread_id=m.session_thread_id AND ended.model_request_id=m.model_request_id
		       AND ended.type='span.model_request_end'
		       AND ended.payload_json::jsonb ->> 'error_kind' = 'runtime_pod_lost'
		  ) THEN 'pod_lost'
		  ELSE 'sealed'
		END AS context_state,
		EXISTS (
		  SELECT 1 FROM session_events rescheduled
		   WHERE rescheduled.workspace_id=m.workspace_id AND rescheduled.session_id=m.session_id
		     AND rescheduled.session_thread_id=m.session_thread_id AND rescheduled.model_request_id=m.model_request_id
		     AND rescheduled.type IN ('session.status_rescheduled','session.thread_status_rescheduled')
		) AS request_rescheduled,
		(
		  (
		    EXISTS (
		      SELECT 1 FROM session_events tool_use
		       WHERE tool_use.workspace_id=m.workspace_id AND tool_use.session_id=m.session_id
		         AND tool_use.session_thread_id=m.session_thread_id AND tool_use.model_request_id=m.model_request_id
		         AND tool_use.type IN ('agent.tool_use','agent.mcp_tool_use')
		    )
		    OR EXISTS (
		      SELECT 1 FROM session_events repair
		       WHERE repair.workspace_id=m.workspace_id AND repair.session_id=m.session_id
		         AND repair.session_thread_id=m.session_thread_id AND repair.model_request_id=m.model_request_id
		         AND repair.type='agent.tool_result'
		         AND repair.payload_json::jsonb ->> 'repair_kind'='invalid_tool'
		    )
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM session_events tool_use
		     WHERE tool_use.workspace_id=m.workspace_id AND tool_use.session_id=m.session_id
		       AND tool_use.session_thread_id=m.session_thread_id AND tool_use.model_request_id=m.model_request_id
		       AND tool_use.type IN ('agent.tool_use','agent.mcp_tool_use')
		       AND NOT EXISTS (
		         SELECT 1 FROM session_events tool_result
		          WHERE tool_result.workspace_id=tool_use.workspace_id AND tool_result.session_id=tool_use.session_id
		            AND tool_result.session_thread_id=tool_use.session_thread_id
		            AND tool_result.type IN ('agent.tool_result','agent.mcp_tool_result')
		            AND COALESCE(
		                  tool_result.payload_json::jsonb ->> 'tool_use_event_id',
		                  tool_result.payload_json::jsonb ->> 'tool_use_id',
		                  tool_result.payload_json::jsonb ->> 'mcp_tool_use_id'
		                )=tool_use.event_id
		       )
		  )
		) AS complete_tool_repair
	FROM session_messages m CROSS JOIN latest_compaction c
	WHERE m.workspace_id=$1 AND m.session_id=$2 AND m.session_thread_id=$3 AND m.sequence <= $4
	 AND (c.boundary_sequence IS NULL OR m.sequence >= c.boundary_sequence)
	ORDER BY m.sequence`, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), boundarySequence)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()
	var entries []bridgeRuntimeContextEntry
	var kinds []string
	for rows.Next() {
		var kind, raw, contextState string
		var sequence int64
		var modelRequestID sql.NullString
		var requestRescheduled bool
		var completeToolRepair bool
		if err := rows.Scan(&kind, &sequence, &raw, &modelRequestID, &contextState, &requestRescheduled, &completeToolRepair); err != nil {
			return nil, nil, err
		}
		parts, err := decodeStoredRuntimeContextParts(raw)
		if err != nil {
			return nil, nil, err
		}
		switch contextState {
		case "sealed":
		case "pod_lost":
			if requestRescheduled || !completeToolRepair {
				continue
			}
		case "open":
			continue
		default:
			return nil, nil, status.Error(codes.FailedPrecondition, "durable context state is invalid")
		}
		entries = append(entries, bridgeRuntimeContextEntry{MessageSequence: sequence, ContextKind: kind, Parts: parts})
		kinds = append(kinds, kind)
	}
	return entries, kinds, rows.Err()
}

func validParentMessageSequences(values []int64) bool {
	if len(values) > actorParentMessageRefsMax {
		return false
	}
	var prior int64
	for _, value := range values {
		if value <= prior {
			return false
		}
		prior = value
	}
	return true
}

func validActorTaskName(value string) bool {
	return value == strings.TrimSpace(value) && value != "" && utf8.ValidString(value) && len([]byte(value)) <= actorTaskNameMaxBytes
}

func validActorInitialPrompt(value string) bool {
	return value == strings.TrimSpace(value) && value != "" && utf8.ValidString(value) && len([]byte(value)) <= AgentMailContentMaxBytes
}

func validActorIdentity(value string) bool {
	return value != "" && utf8.ValidString(value) && len([]byte(value)) <= 128
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

func insertOwnedChildThreadTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	parentScope *bridgev1.RuntimeScope,
	childThreadID string,
	parentThreadID string,
	role string,
	visibility string,
	agentType string,
	taskName string,
	isTrunk bool,
	sourceToolUseEventID string,
	prefix *threadContextPrefixEnvelope,
	now time.Time,
) (createChildThreadResult, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_threads (
			workspace_id, id, session_id, parent_thread_id, role, visibility, status,
			agent_type, title, task_name, is_trunk, created_at, last_active_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,'idle',$7,$8,$9,$10,$11,$11,$11)`,
		parentScope.GetWorkspaceId(), childThreadID, parentScope.GetSessionId(), parentThreadID,
		role, visibility, agentType, nullableSQLString(taskName), nullableSQLString(taskName), isTrunk, now,
	); err != nil {
		return createChildThreadResult{}, err
	}
	childScope := scopeForThread(parentScope, childThreadID)
	_, _, err := insertChildThreadCreatedEventTx(ctx, tx, childScope, parentThreadID, role, visibility, agentType, taskName, sourceToolUseEventID, now)
	if err != nil {
		return createChildThreadResult{}, err
	}
	if prefix != nil {
		if err := insertThreadContextPrefixTx(ctx, tx, childScope, prefix, now); err != nil {
			return createChildThreadResult{}, err
		}
	}
	return createChildThreadResult{ChildThreadID: childThreadID}, nil
}

func persistCreatedChildOperationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	sourceKind string,
	key string,
	requestHash string,
	result createChildThreadResult,
	now time.Time,
) error {
	resultJSON, err := marshalBridgeJSON(result)
	if err != nil {
		return err
	}
	return insertBridgeOperationTx(ctx, tx, scope, bridgeOperationInsert{
		Operation: bridgeOpCreateChildThread, SourceKind: sourceKind, IdempotencyKey: key,
		RequestHash: requestHash, AckStatus: bridgeAckCommitted, ResultJSON: resultJSON, Now: now,
	})
}

func insertChildThreadCreatedEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	parentThreadID string,
	role string,
	visibility string,
	agentType string,
	taskName string,
	sourceToolUseEventID string,
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
		"task_name":                nullableJSONString(nullableSQLString(taskName)),
		"source_tool_use_event_id": nullableJSONString(nullableSQLString(sourceToolUseEventID)),
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

func insertThreadContextPrefixTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, prefix *threadContextPrefixEnvelope, now time.Time) error {
	if prefix == nil || prefix.SourceParentThreadID == "" || prefix.ParentBoundaryEventID == "" || prefix.Entries == nil {
		return status.Error(codes.FailedPrecondition, "thread context prefix is invalid")
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
	entriesJSON, err := json.Marshal(prefix.Entries)
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

func (s *PostgreSQLBridgeAPIStore) readChildThreadFact(ctx context.Context, scope *bridgev1.RuntimeScope, childThreadID string, operation string) (*bridgev1.ChildThreadFact, error) {
	var child *bridgev1.ChildThreadFact
	if err := s.withScopeTx(ctx, scope, "agentruntimebridge."+operation, func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, scope); err != nil {
			return err
		}
		var err error
		child, err = scanChildThreadFact(tx.QueryRow(ctx,
			`SELECT id, parent_thread_id, role, visibility, status, agent_type, task_name
			   FROM session_threads
			  WHERE workspace_id=$1 AND session_id=$2 AND id=$3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), childThreadID,
		))
		return err
	}); err != nil {
		return nil, err
	}
	return child, nil
}

func scanChildThreadFact(scanner interface{ Scan(dest ...any) error }) (*bridgev1.ChildThreadFact, error) {
	var child bridgev1.ChildThreadFact
	var parentID, taskName sql.NullString
	if err := scanner.Scan(&child.ChildThreadId, &parentID, &child.Role, &child.Visibility, &child.Status, &child.AgentType, &taskName); dbconnect.IsNoRows(err) {
		return nil, status.Error(codes.NotFound, "child thread not found")
	} else if err != nil {
		return nil, err
	}
	child.ParentThreadId, child.TaskName = parentID.String, taskName.String
	return &child, nil
}
