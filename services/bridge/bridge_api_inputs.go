package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

const (
	sandboxToolCancelMaxAttempts = 5
	maxApprovalReviewTextParts   = 16
	maxApprovalReviewTextBytes   = 2 * 1024 * 1024
)

// This file owns the Bridge inputs protocol-family boundary.

func (s *PostgreSQLBridgeAPIStore) CommitInputs(ctx context.Context, request *bridgev1.CommitInputsRequest) (response *bridgev1.CommitInputsResponse, resultErr error) {
	logStartedAt := time.Now()
	evidence := runtimeDeclarationRejectionEvidence{
		Kind:        "identity",
		Operation:   bridgeOpCommitInputs,
		OperationID: request.GetRuntimeInputId(),
	}
	defer func() { logRuntimeDeclarationRejected(s.Logger, request.GetScope(), evidence, resultErr) }()
	if request.GetRuntimeInputId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid commit inputs request")
	}
	if err := validateApprovalReviewText(request.GetApprovalReviewText()); err != nil {
		return nil, err
	}
	key := request.GetRuntimeInputId()
	now := s.now()
	var observation declarationApplicationObservation
	var inputKind string
	var declarationDigest string
	var duplicate bool
	var typedResult *commitInputsTypedResult
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_inputs", func(tx *dbconnect.Tx) error {
		mutationCtx := ctx
		// Session authority precedes ordinary replay. Exact input receipts may replay
		// only while the declaring binding remains current and nonterminal.
		if err := lockRuntimeMutationSessionTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId()); err != nil {
			return err
		}
		evidence.Kind = "authorization"
		if err := verifyRuntimeDeclarationCaller(ctx, request.GetScope()); err != nil {
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		var err error
		inputKind, err = commitInputKindTx(ctx, tx, request)
		if err != nil {
			evidence.Kind = "schema"
			return err
		}
		evidence.MessageOrPart = inputKind
		declarationDigest, err = commitInputsDeclarationDigest(request, inputKind)
		if err != nil {
			return err
		}
		evidence.Kind = "canonicality"
		evidence.Kind = "transaction"
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
				return status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "commit inputs idempotency conflict")
			}
			typedResult, err = unmarshalCommitInputsReplay(inputKind, existing.ReceiptJSON)
			if err != nil {
				return err
			}
			duplicate = true
			observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
			return err
		}
		if inputKind == "interrupt_control" {
			if err := validateInterruptLeaseRefTx(ctx, tx, request.GetScope(), key, request.GetInterruptLeaseRef()); err != nil {
				return err
			}
			mutationCtx = withInterruptCloseout(ctx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), key)
		} else if request.GetInterruptLeaseRef() != nil {
			return status.Error(codes.InvalidArgument, "interrupt lease authority is not valid for ordinary input")
		} else if err := requireThreadInputDeliveryAllowedTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(mutationCtx, tx, request.GetScope())
		if err != nil {
			return err
		}
		evidence.ThreadRole = threadScope.role
		if inputKind == "approval_review" && (threadScope.role != "approval_reviewer" || threadScope.visibility != "internal") {
			evidence.Kind = "lineage"
		}
		typedResult, err = commitInputDeclarationTx(mutationCtx, tx, request, inputKind, key, now)
		if err != nil {
			return err
		}
		resultJSON, err := marshalCommitInputsReplay(inputKind, typedResult)
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
		observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
		return err
	}); err != nil {
		if isThreadInterruptBarrierStaleError(err) {
			return &bridgev1.CommitInputsResponse{Outcome: &bridgev1.CommitInputsResponse_BarrierStale{BarrierStale: &bridgev1.CommitInputsBarrierStale{}}}, nil
		}
		if isScopeSupersededError(err) && status.Code(err) == codes.FailedPrecondition {
			return &bridgev1.CommitInputsResponse{Outcome: &bridgev1.CommitInputsResponse_Stale{Stale: &bridgev1.CommitInputsStale{}}}, nil
		}
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitInputs,
		inputKind,
		key,
		declarationDigest,
		duplicate,
		observation,
	)
	if inputKind == "interrupt_control" && typedResult != nil {
		event := "thread_interrupt_completed"
		if !observation.Current {
			event = "thread_interrupt_stale"
		}
		s.logCommittedThreadInterrupt(ctx, request, event, len(typedResult.interruptToolResults), time.Since(logStartedAt).Milliseconds())
	}
	if !observation.Current {
		return &bridgev1.CommitInputsResponse{Outcome: &bridgev1.CommitInputsResponse_Stale{Stale: &bridgev1.CommitInputsStale{}}}, nil
	}
	if typedResult == nil {
		return nil, status.Error(codes.Internal, "commit inputs result is unavailable")
	}
	return &bridgev1.CommitInputsResponse{Outcome: &bridgev1.CommitInputsResponse_Committed{
		Committed: commitInputsCommittedResult(inputKind, typedResult),
	}}, nil
}

// validateApprovalReviewText bounds the only caller-authored CommitInputs
// content before Bridge begins the declaration transaction. The count and one
// aggregate UTF-8 byte budget apply to the complete ordered vector.
func validateApprovalReviewText(text []string) error {
	if len(text) > maxApprovalReviewTextParts {
		return status.Error(codes.InvalidArgument, "approval review content exceeds part count limit")
	}
	aggregateBytes := 0
	for _, part := range text {
		if !utf8.ValidString(part) {
			return status.Error(codes.InvalidArgument, "approval review content must be valid UTF-8")
		}
		aggregateBytes += len(part)
		if aggregateBytes > maxApprovalReviewTextBytes {
			return status.Error(codes.InvalidArgument, "approval review content exceeds aggregate byte limit")
		}
	}
	return nil
}

func commitInputDeclarationTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.CommitInputsRequest,
	inputKind string,
	key string,
	now time.Time,
) (*commitInputsTypedResult, error) {
	inboxStatus := ""
	eventIDs := []string(nil)
	contextDrafts := []commitInputContextDraft(nil)
	if commitInputsKindUsesRuntimeInbox(inputKind) {
		var err error
		inboxStatus, eventIDs, contextDrafts, err = lockAndBuildRuntimeInboxCommitTx(ctx, tx, request, inputKind)
		if err != nil {
			return nil, err
		}
	}
	if inboxStatus == "committed" {
		return nil, status.Error(codes.FailedPrecondition, "committed runtime input is missing idempotency state")
	}
	if expectedEventType := commitInputEventType(inputKind); expectedEventType != "" {
		if err := requireCommitInputEventTypesTx(ctx, tx, request.GetScope(), eventIDs, expectedEventType); err != nil {
			return nil, err
		}
	}
	if inputKind == "approval_review" {
		if err := requireApprovalReviewerInputTargetTx(ctx, tx, request.GetScope()); err != nil {
			return nil, err
		}
		approvalReviewEventID := id.New("evt_")
		if err := createApprovalReviewInputEventTx(
			ctx,
			tx,
			request.GetScope(),
			key,
			approvalReviewEventID,
			now,
		); err != nil {
			return nil, err
		}
		eventIDs = []string{approvalReviewEventID}
		if len(contextDrafts) != 1 {
			return nil, status.Error(codes.Internal, "approval review context is incomplete")
		}
		contextDrafts[0].sourceEventID = approvalReviewEventID
	}
	if inputKind == "agent_mail" {
		if err := requireAgentMailInputTargetTx(ctx, tx, request.GetScope()); err != nil {
			return nil, err
		}
	}
	if err := markRuntimeInboxCommittedTx(ctx, tx, request.GetScope(), key, now); err != nil {
		return nil, err
	}
	if inputKind != "approval_review" {
		if err := markSessionEventsProcessed(ctx, tx, request.GetScope(), eventIDs, now); err != nil {
			return nil, err
		}
	}
	if inputKind == "tool_confirmation" {
		if err := settleToolConfirmationEventsTx(ctx, tx, request.GetScope(), eventIDs, now); err != nil {
			return nil, err
		}
	}
	assignedSequences, err := insertCommitInputContextDraftsTx(
		ctx,
		tx,
		request.GetScope(),
		contextDrafts,
		now,
	)
	if err != nil {
		return nil, err
	}
	result := &commitInputsTypedResult{assignedContextSequences: assignedSequences}
	if inputKind == "messages" {
		result.pendingAttachmentJSON, err = loadCommittedInputAttachmentDeltaTx(
			ctx,
			tx,
			request.GetScope(),
			eventIDs,
		)
		if err != nil {
			return nil, err
		}
	}
	if inputKind == "interrupt_control" {
		if err := cancelAcceptedApprovalReviewInputsTx(ctx, tx, request.GetScope(), now); err != nil {
			return nil, err
		}
		result.interruptToolResults, err = settleInterruptedThreadToolsTx(
			ctx,
			tx,
			request.GetScope(),
			eventIDs[0],
			now,
		)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// An interrupt that wins after Reviewer admission takes over the accepted
// synchronous custody in the same transaction as its own durable closeout.
// A committed Reviewer input has already crossed the admission/result race and
// remains governed by the ordinary Reviewer outcome and interrupt cleanup.
func cancelAcceptedApprovalReviewInputsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	now time.Time,
) error {
	_, err := tx.Exec(ctx, `UPDATE session_runtime_inbox inbox
		SET status = 'cancelled', updated_at = $4
		WHERE inbox.workspace_id = $1
		  AND inbox.session_id = $2
		  AND inbox.input_kind = 'approval_review'
		  AND inbox.status = 'accepted'
		  AND EXISTS (
		      SELECT 1 FROM session_threads reviewer
		       WHERE reviewer.workspace_id = inbox.workspace_id
		         AND reviewer.session_id = inbox.session_id
		         AND reviewer.id = inbox.session_thread_id
		         AND reviewer.role = 'approval_reviewer'
		         AND reviewer.parent_thread_id = $3
		  )`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), now)
	return err
}

func createApprovalReviewInputEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	eventID string,
	now time.Time,
) error {
	if eventID == "" {
		return status.Error(codes.InvalidArgument, "approval review event id is invalid")
	}
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	payloadJSON, err := marshalBridgeJSON(map[string]any{
		"type":             "approval_review.input",
		"runtime_input_id": runtimeInputID,
	})
	if err != nil {
		return err
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
		return err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, "internal", false, now); err != nil {
		return err
	}
	return nil
}

func commitInputsKindUsesRuntimeInbox(inputKind string) bool {
	switch inputKind {
	case "messages", "interrupt_control", "tool_confirmation", "agent_mail", "approval_review", "rejection":
		return true
	default:
		return false
	}
}

func commitInputKindTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.CommitInputsRequest) (string, error) {
	var inputKind string
	err := tx.QueryRow(ctx, `SELECT input_kind FROM session_runtime_inbox
		WHERE workspace_id=$1 AND session_id=$2 AND runtime_input_id=$3 FOR UPDATE`,
		request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetRuntimeInputId(),
	).Scan(&inputKind)
	if dbconnect.IsNoRows(err) {
		return "", status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	}
	if err != nil {
		return "", err
	}
	if inputKind == "approval_review" {
		if len(request.GetApprovalReviewText()) == 0 {
			return "", status.Error(codes.InvalidArgument, "approval review content is required")
		}
	} else if len(request.GetApprovalReviewText()) != 0 {
		return "", status.Error(codes.InvalidArgument, "approval review content is not valid for Inbox input")
	}
	if !commitInputsKindUsesRuntimeInbox(inputKind) {
		return "", status.Error(codes.FailedPrecondition, "runtime input kind is not committable")
	}
	return inputKind, nil
}

type commitInputContextDraft struct {
	contextKind   string
	sourceEventID string
	parts         []map[string]any
}

func lockAndBuildRuntimeInboxCommitTx(ctx context.Context, tx *dbconnect.Tx, request *bridgev1.CommitInputsRequest, inputKind string) (string, []string, []commitInputContextDraft, error) {
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
		return "", nil, nil, status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	} else if err != nil {
		return "", nil, nil, err
	}
	if inboxBindingID != scope.GetBinding().GetBindingId() ||
		inboxBindingGeneration != scope.GetBinding().GetBindingGeneration() ||
		inboxTargetPodUID != scope.GetBinding().GetTargetPodUid() ||
		(inboxStatus != "delivering" && inboxStatus != "accepted" && inboxStatus != "committed") {
		return "", nil, nil, status.Error(codes.FailedPrecondition, "runtime input is not deliverable")
	}
	var inboxEventIDs []string
	if err := json.Unmarshal([]byte(inboxEventIDsJSON), &inboxEventIDs); err != nil {
		return "", nil, nil, status.Error(codes.FailedPrecondition, "runtime inbox payload is invalid")
	}
	if inboxSessionID != scope.GetSessionId() ||
		inboxThreadID != scope.GetSessionThreadId() ||
		inboxKind != inputKind {
		return "", nil, nil, status.Error(codes.AlreadyExists, "commit inputs target conflicts with runtime inbox")
	}
	_ = inboxSequenceFrom
	_ = inboxSequenceTo
	if inputKind == "approval_review" {
		return inboxStatus, inboxEventIDs, []commitInputContextDraft{runtimeTextContextDraft("user", request.GetApprovalReviewText())}, nil
	}
	creates, err := buildRuntimeInboxContextDraftsTx(ctx, tx, scope, inputKind, inboxEventIDs)
	if err != nil {
		return "", nil, nil, err
	}
	return inboxStatus, inboxEventIDs, creates, nil
}

func buildRuntimeInboxContextDraftsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	eventIDs []string,
) ([]commitInputContextDraft, error) {
	if inputKind == "interrupt_control" {
		return nil, nil
	}
	if inputKind == "rejection" && len(eventIDs) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "rejection input source is missing")
	}
	creates := make([]commitInputContextDraft, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		var eventType string
		var payloadJSON string
		if err := tx.QueryRow(ctx, `SELECT type,payload_json FROM session_events
			WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND event_id=$4 FOR UPDATE`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), eventID,
		).Scan(&eventType, &payloadJSON); err != nil {
			return nil, err
		}
		switch inputKind {
		case "messages":
			if eventType != "user.message" {
				return nil, status.Error(codes.FailedPrecondition, "message input source is invalid")
			}
			var payload struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				return nil, status.Error(codes.FailedPrecondition, "message input source is malformed")
			}
			texts := make([]string, 0, len(payload.Content))
			for _, content := range payload.Content {
				if content.Type == "text" {
					texts = append(texts, content.Text)
				}
			}
			if len(texts) != 0 {
				draft := runtimeTextContextDraft("user", texts)
				draft.sourceEventID = eventID
				creates = append(creates, draft)
			}
		case "rejection":
			if len(creates) == 0 {
				draft := runtimeTextContextDraft(
					"assistant", []string{"The session runtime could not accept this input."},
				)
				draft.sourceEventID = eventID
				creates = append(creates, draft)
			}
		case "tool_confirmation":
			if eventType != "user.tool_confirmation" {
				return nil, status.Error(codes.FailedPrecondition, "tool confirmation source is invalid")
			}
			var payload struct {
				Result      string  `json:"result"`
				DenyMessage *string `json:"deny_message"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil || (payload.Result != "allow" && payload.Result != "deny") {
				return nil, status.Error(codes.FailedPrecondition, "tool confirmation source is malformed")
			}
			text := "Approval allowed"
			if payload.Result == "deny" {
				text = "Approval denied"
				if payload.DenyMessage != nil && *payload.DenyMessage != "" {
					text += ": " + *payload.DenyMessage
				}
			}
			draft := runtimeTextContextDraft("user", []string{text})
			draft.sourceEventID = eventID
			creates = append(creates, draft)
		case "agent_mail":
			if eventType != "agent.thread_message_received" {
				return nil, status.Error(codes.FailedPrecondition, "agent mail source is invalid")
			}
			var envelope struct {
				Message json.RawMessage `json:"message"`
			}
			if err := json.Unmarshal([]byte(payloadJSON), &envelope); err != nil || len(envelope.Message) == 0 {
				return nil, status.Error(codes.FailedPrecondition, "agent mail source is malformed")
			}
			publicMessage, err := validatedPublicInterAgentMessageJSON(envelope.Message)
			if err != nil {
				return nil, err
			}
			content, err := agentMailContentFromPublicMessage(publicMessage)
			if err != nil {
				return nil, err
			}
			draft := runtimeTextContextDraft("user", []string{content})
			draft.sourceEventID = eventID
			creates = append(creates, draft)
		default:
			return nil, status.Error(codes.FailedPrecondition, "runtime input kind is not committable")
		}
	}
	return creates, nil
}

func runtimeTextContextDraft(contextKind string, texts []string) commitInputContextDraft {
	parts := make([]map[string]any, 0, len(texts))
	for _, text := range texts {
		parts = append(parts, map[string]any{"type": "text", "text": text})
	}
	return commitInputContextDraft{contextKind: contextKind, parts: parts}
}

func insertCommitInputContextDraftsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	drafts []commitInputContextDraft,
	now time.Time,
) ([]int64, error) {
	assigned := make([]int64, 0, len(drafts))
	for _, draft := range drafts {
		if draft.contextKind != "user" && draft.contextKind != "assistant" && draft.contextKind != "runtime_notification" {
			return nil, status.Error(codes.Internal, "runtime input context kind is invalid")
		}
		if len(draft.parts) == 0 {
			return nil, status.Error(codes.FailedPrecondition, "runtime input context is empty")
		}
		for _, part := range draft.parts {
			if err := validateStoredRuntimeContextPart(part); err != nil {
				return nil, status.Error(codes.FailedPrecondition, "runtime input context is malformed")
			}
		}
		var sequence int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1 FROM session_messages
			  WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&sequence); err != nil {
			return nil, err
		}
		dataJSON, err := runtimeContextDataJSON(draft.parts)
		if err != nil {
			return nil, err
		}
		var sourceEventID any
		if draft.sourceEventID != "" {
			sourceEventID = draft.sourceEventID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_messages (
				workspace_id,session_id,session_thread_id,message_id,sequence,kind,
				data_json,source_event_id,created_at,updated_at
			 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			id.New("msg_"), sequence, draft.contextKind, dataJSON, sourceEventID, now,
		); err != nil {
			return nil, err
		}
		assigned = append(assigned, sequence)
	}
	return assigned, nil
}

type commitInputsTypedResult struct {
	assignedContextSequences []int64
	interruptToolResults     []*bridgev1.RuntimeInterruptToolResult
	pendingAttachmentJSON    []string
}

func marshalCommitInputsReplay(inputKind string, result *commitInputsTypedResult) (string, error) {
	encoded, err := protojson.Marshal(commitInputsCommittedResult(inputKind, result))
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func unmarshalCommitInputsReplay(inputKind string, raw string) (*commitInputsTypedResult, error) {
	committed := &bridgev1.CommitInputsCommitted{}
	if raw == "" || protojson.Unmarshal([]byte(raw), committed) != nil {
		return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
	}
	result := &commitInputsTypedResult{}
	switch application := committed.GetApplication().(type) {
	case *bridgev1.CommitInputsCommitted_Context:
		if inputKind == "interrupt_control" || application.Context == nil {
			return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
		}
		for _, sequence := range application.Context.GetAssignedContextSequences() {
			if sequence <= 0 {
				return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
			}
		}
		result.assignedContextSequences = append([]int64(nil), application.Context.GetAssignedContextSequences()...)
		result.pendingAttachmentJSON = append([]string(nil), application.Context.GetPendingAttachmentJson()...)
	case *bridgev1.CommitInputsCommitted_Interrupt:
		if inputKind != "interrupt_control" || application.Interrupt == nil {
			return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
		}
		for _, toolResult := range application.Interrupt.GetInterruptToolResults() {
			if toolResult == nil || toolResult.GetToolUseEventId() == "" {
				return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
			}
			switch outcome := toolResult.GetOutcome().(type) {
			case *bridgev1.RuntimeInterruptToolResult_Error:
				if outcome.Error == nil || outcome.Error.GetErrorJson() == "" {
					return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
				}
			case *bridgev1.RuntimeInterruptToolResult_Cancelled:
				if outcome.Cancelled == nil {
					return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
				}
			default:
				return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
			}
		}
		result.interruptToolResults = append([]*bridgev1.RuntimeInterruptToolResult(nil), application.Interrupt.GetInterruptToolResults()...)
	default:
		return nil, status.Error(codes.FailedPrecondition, "commit inputs replay facts are invalid")
	}
	return result, nil
}

func commitInputsCommittedResult(inputKind string, result *commitInputsTypedResult) *bridgev1.CommitInputsCommitted {
	if inputKind == "interrupt_control" {
		return &bridgev1.CommitInputsCommitted{Application: &bridgev1.CommitInputsCommitted_Interrupt{
			Interrupt: &bridgev1.CommitInputsInterruptApplication{InterruptToolResults: result.interruptToolResults},
		}}
	}
	return &bridgev1.CommitInputsCommitted{Application: &bridgev1.CommitInputsCommitted_Context{
		Context: &bridgev1.CommitInputsContextApplication{
			AssignedContextSequences: result.assignedContextSequences,
			PendingAttachmentJson:    result.pendingAttachmentJSON,
		},
	}}
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
			var authorized bool
			var found bool
			if err := tx.QueryRow(ctx,
				`SELECT mime_type,
				        filename,
				        ((scope_type IS NULL AND scope_id IS NULL)
				          OR (scope_type = 'session' AND scope_id = $3)) AS authorized
				   FROM files
				  WHERE workspace_id = $1
				    AND file_id = $2`,
				scope.GetWorkspaceId(),
				part.Source.FileID,
				scope.GetSessionId(),
			).Scan(&mime, &filename, &authorized); dbconnect.IsNoRows(err) {
				mime = sql.NullString{}
				filename = sql.NullString{}
			} else if err != nil {
				return nil, err
			} else {
				found = true
			}
			if !found || !authorized {
				continue
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

func validatedPublicInterAgentMessageJSON(raw json.RawMessage) (json.RawMessage, error) {
	var message map[string]json.RawMessage
	if json.Unmarshal(raw, &message) != nil || message == nil {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message must be an object")
	}
	content, exists := message["content"]
	if !exists || len(message) != 1 {
		return nil, status.Error(codes.InvalidArgument, "inter-agent message requires only content")
	}
	if err := validatePublicInterAgentContent(content); err != nil {
		return nil, err
	}
	return raw, nil
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
