package agentruntimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
)

// This file owns the Bridge settlement protocol-family boundary.

// normalizedStableReasoningPart is a completed reasoning part in Bridge-internal
// form. Its Metadata field is the single carve-out to the no-provider-metadata
// projection rule: Metadata durably retains bounded provider-native provenance
// (Anthropic signature/redacted data, OpenAI encrypted-reasoning metadata) so a
// cold reload round-trips reasoning without stripping signatures. That metadata
// is Bridge-size-bounded, internal cold-start context only, and never surfaces
// on any public Event or message API.
type normalizedStableReasoningPart struct {
	ReasoningPartID string         `json:"reasoning_part_id"`
	ProviderPartID  string         `json:"provider_part_id"`
	PartSequence    int32          `json:"part_sequence"`
	Text            string         `json:"text"`
	Metadata        map[string]any `json:"metadata"`
	Truncated       bool           `json:"truncated"`
}

type normalizedStableReasoningSet struct {
	Parts           []normalizedStableReasoningPart
	CanonicalJSON   string
	StrictlyOrdered bool
}

func (set normalizedStableReasoningSet) ledgerJSON() any {
	if len(set.Parts) == 0 {
		return nil
	}
	return set.CanonicalJSON
}

type stableReasoningCarrier interface {
	GetStableReasoningParts() []*bridgev1.StableReasoningPart
}

func normalizeStableReasoningParts(request stableReasoningCarrier) (normalizedStableReasoningSet, error) {
	parts := request.GetStableReasoningParts()
	if len(parts) > MaxStableReasoningPartsPerRequest {
		return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "too many stable reasoning parts")
	}
	if requestEnd, ok := request.(*bridgev1.WriteRequestEndRequest); ok && len(parts) > 0 && (requestEnd.GetIsError() || requestEnd.GetReschedule() != nil) {
		return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "error or reschedule request end cannot carry stable reasoning")
	}
	normalized := normalizedStableReasoningSet{
		Parts:           make([]normalizedStableReasoningPart, 0, len(parts)),
		StrictlyOrdered: true,
	}
	partIDs := make(map[string]struct{}, len(parts))
	sequences := make(map[int32]struct{}, len(parts))
	aggregateBytes := 0
	var previousSequence int32
	for index, part := range parts {
		if part == nil || part.GetReasoningPartId() == "" || part.GetPartSequence() < 0 {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part identity is incomplete")
		}
		for _, value := range []string{part.GetReasoningPartId(), part.GetProviderPartId(), part.GetText(), part.GetMetadataJson()} {
			if !utf8.ValidString(value) {
				return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part is not UTF-8")
			}
		}
		if len(part.GetMetadataJson()) > 64*1024 {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part exceeds size bounds")
		}
		if _, exists := partIDs[part.GetReasoningPartId()]; exists {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part id is duplicated")
		}
		if _, exists := sequences[part.GetPartSequence()]; exists {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part sequence is duplicated")
		}
		if index > 0 && part.GetPartSequence() <= previousSequence {
			normalized.StrictlyOrdered = false
		}
		previousSequence = part.GetPartSequence()
		partIDs[part.GetReasoningPartId()] = struct{}{}
		sequences[part.GetPartSequence()] = struct{}{}

		metadataJSON := defaultString(part.GetMetadataJson(), "{}")
		var metadata map[string]any
		if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil || metadata == nil {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning metadata must be a JSON object")
		}
		canonicalMetadata, err := marshalBridgeDataJSON(metadata)
		if err != nil {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning metadata must be a JSON object")
		}
		if len(canonicalMetadata) > 64*1024 {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part exceeds size bounds")
		}
		// Runtime's stableReasoningMetadataJSON encoder produces metadataJSON;
		// count those exact transported bytes so both sides enforce one aggregate.
		// UPDATE-WITH: services/agent-runtime/packages/core/src/contracts/runtime.ts
		// (stableReasoningMetadataJSON).
		aggregateBytes += len(part.GetText()) + len(metadataJSON)
		if aggregateBytes > MaxStableReasoningBytesPerRequest {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning set exceeds aggregate size bound")
		}
		normalized.Parts = append(normalized.Parts, normalizedStableReasoningPart{
			ReasoningPartID: part.GetReasoningPartId(),
			ProviderPartID:  part.GetProviderPartId(),
			PartSequence:    part.GetPartSequence(),
			Text:            part.GetText(),
			Metadata:        metadata,
			Truncated:       part.GetTruncated(),
		})
	}
	canonicalSet, err := marshalBridgeDataJSON(normalized.Parts)
	if err != nil {
		return normalizedStableReasoningSet{}, err
	}
	normalized.CanonicalJSON = canonicalSet
	return normalized, nil
}

func validateStableReasoningBudget(parts []any) error {
	count := 0
	aggregateBytes := 0
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok || part["type"] != "reasoning" {
			continue
		}
		count++
		text, _ := part["text"].(string)
		metadataValue := part["providerMetadata"]
		if metadataValue == nil {
			metadataValue = map[string]any{}
		}
		// Keep durable-draft accounting byte-identical to the transported
		// metadata contract; HTML escaping would create a second size policy.
		// UPDATE-WITH: services/agent-runtime/packages/core/src/contracts/runtime.ts
		// (stableReasoningMetadataJSON).
		metadata, err := marshalBridgeDataJSON(metadataValue)
		if err != nil {
			return status.Error(codes.FailedPrecondition, "stable reasoning metadata is invalid")
		}
		aggregateBytes += len(text) + len(metadata)
	}
	if count > MaxStableReasoningPartsPerRequest || aggregateBytes > MaxStableReasoningBytesPerRequest {
		return status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
	}
	return nil
}

func validateStableReasoningSettlementSupersetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	settlement normalizedStableReasoningSet,
) error {
	rows, err := tx.Query(ctx,
		`SELECT stable_reasoning_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND stable_reasoning_json IS NOT NULL
		  ORDER BY sequence ASC, event_id ASC
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	settlementByID := make(map[string]normalizedStableReasoningPart, len(settlement.Parts))
	for _, part := range settlement.Parts {
		settlementByID[part.ReasoningPartID] = part
	}
	anchoredByID := make(map[string]normalizedStableReasoningPart)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return err
		}
		var anchored []normalizedStableReasoningPart
		if err := json.Unmarshal([]byte(encoded), &anchored); err != nil {
			return status.Error(codes.FailedPrecondition, "stable reasoning event ledger is invalid")
		}
		for _, part := range anchored {
			if existing, ok := anchoredByID[part.ReasoningPartID]; ok && !sameNormalizedStableReasoningPart(existing, part) {
				return status.Error(codes.FailedPrecondition, "stable reasoning event ledger is inconsistent")
			}
			anchoredByID[part.ReasoningPartID] = part
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for partID, anchored := range anchoredByID {
		candidate, ok := settlementByID[partID]
		if !ok || !sameNormalizedStableReasoningPart(anchored, candidate) {
			return status.Error(codes.AlreadyExists, "stable reasoning settlement omits or diverges from anchored content")
		}
	}
	return nil
}

func validateStableReasoningAnchorBudgetTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	modelRequestID string,
	anchor normalizedStableReasoningSet,
) error {
	rows, err := tx.Query(ctx,
		`SELECT stable_reasoning_json
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND stable_reasoning_json IS NOT NULL
		  ORDER BY sequence ASC, event_id ASC
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	partsByID := make(map[string]normalizedStableReasoningPart)
	add := func(part normalizedStableReasoningPart) error {
		if existing, ok := partsByID[part.ReasoningPartID]; ok && !sameNormalizedStableReasoningPart(existing, part) {
			return status.Error(codes.AlreadyExists, "stable reasoning anchor diverges from durable content")
		}
		partsByID[part.ReasoningPartID] = part
		return nil
	}
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return err
		}
		var durable []normalizedStableReasoningPart
		if err := json.Unmarshal([]byte(encoded), &durable); err != nil {
			return status.Error(codes.FailedPrecondition, "stable reasoning event ledger is invalid")
		}
		for _, part := range durable {
			if err := add(part); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, part := range anchor.Parts {
		if err := add(part); err != nil {
			return err
		}
	}
	aggregateBytes := 0
	for _, part := range partsByID {
		metadata, _ := json.Marshal(part.Metadata)
		aggregateBytes += len(part.Text) + len(metadata)
	}
	if len(partsByID) > MaxStableReasoningPartsPerRequest || aggregateBytes > MaxStableReasoningBytesPerRequest {
		return status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
	}
	return nil
}

func sameNormalizedStableReasoningPart(left, right normalizedStableReasoningPart) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return bytes.Equal(leftJSON, rightJSON)
}

func requestEndInterruptCommitRequest(
	request *bridgev1.WriteRequestEndRequest,
) (*bridgev1.CommitInputsRequest, string, error) {
	settlement := request.GetInterruptSettlement()
	cancellationDrafts := requestEndCancellationDrafts(request.GetDrafts())
	if settlement == nil {
		if len(cancellationDrafts) != 0 {
			return nil, "", status.Error(codes.InvalidArgument, "request end cancellation drafts require interrupt settlement")
		}
		return nil, "", nil
	}
	if !request.GetIsError() || request.GetErrorKind() != "runtime_interrupted" || request.GetReschedule() != nil {
		return nil, "", status.Error(codes.InvalidArgument, "request end interrupt settlement requires an interrupted terminal end")
	}
	interruptRequest := &bridgev1.CommitInputsRequest{
		Scope:                           request.GetScope(),
		RuntimeInputId:                  settlement.GetRuntimeInputId(),
		EventIds:                        settlement.GetEventIds(),
		SequenceFrom:                    settlement.GetSequenceFrom(),
		SequenceTo:                      settlement.GetSequenceTo(),
		InputKind:                       "interrupt_control",
		Drafts:                          cancellationDrafts,
		PendingToolCancellations:        settlement.GetPendingToolCancellations(),
		SandboxExecutionToolUseEventIds: settlement.GetSandboxExecutionToolUseEventIds(),
	}
	if interruptRequest.GetRuntimeInputId() == "" {
		return nil, "", status.Error(codes.InvalidArgument, "request end interrupt settlement is missing its runtime input")
	}
	if err := validateCommitInputsRequest("interrupt_control", interruptRequest); err != nil {
		return nil, "", err
	}
	digest, err := commitInputsDeclarationDigest(interruptRequest, "interrupt_control")
	if err != nil {
		return nil, "", err
	}
	return interruptRequest, digest, nil
}

func requestEndCancellationDrafts(drafts []*bridgev1.RuntimeMessageDraft) []*bridgev1.RuntimeMessageDraft {
	cancellations := make([]*bridgev1.RuntimeMessageDraft, 0, len(drafts))
	for _, draft := range drafts {
		if draft.GetDraftKind() == bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION {
			cancellations = append(cancellations, draft)
		}
	}
	return cancellations
}

func requestEndPrimaryDrafts(drafts []*bridgev1.RuntimeMessageDraft) []*bridgev1.RuntimeMessageDraft {
	primary := make([]*bridgev1.RuntimeMessageDraft, 0, len(drafts))
	for _, draft := range drafts {
		if draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_CANCELLATION {
			primary = append(primary, draft)
		}
	}
	return primary
}

func readRequestEndInterruptReceiptTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	runtimeInputID string,
	declarationDigest string,
) (*bridgev1.DeclarationReceipt, error) {
	existing, ok, err := readBridgeDeclarationOperationTx(
		ctx,
		tx,
		scope,
		bridgeOpCommitInputs,
		"interrupt_control",
		runtimeInputID,
	)
	if err != nil {
		return nil, err
	}
	if !ok || existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
		return nil, status.Error(codes.FailedPrecondition, "request end interrupt receipt is invalid")
	}
	if existing.DeclarationDigest != declarationDigest {
		return nil, status.Error(codes.AlreadyExists, "request end interrupt idempotency conflict")
	}
	receipt, err := unmarshalDeclarationReceipt(existing.ReceiptJSON)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "request end interrupt receipt is invalid")
	}
	return receipt, nil
}

func (s *PostgreSQLBridgeAPIStore) WriteRequestEnd(ctx context.Context, request *bridgev1.WriteRequestEndRequest) (*bridgev1.WriteRequestEndResponse, error) {
	if request.GetRuntimeWriteId() == "" || request.GetModelRequestId() == "" || request.GetModelRequestStartEventId() == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid request end write")
	}
	if err := validateRequestEndErrorKind(request); err != nil {
		return nil, err
	}
	if len(request.GetStableReasoningParts()) > 0 && (request.GetIsError() || request.GetReschedule() != nil) {
		return nil, status.Error(codes.InvalidArgument, "error or reschedule request end cannot carry stable reasoning")
	}
	stableReasoning, err := normalizeStableReasoningParts(request)
	if err != nil {
		return nil, err
	}
	if request.GetReschedule() != nil && !request.GetIsError() {
		return nil, status.Error(codes.InvalidArgument, "successful request end cannot reschedule")
	}
	interruptRequest, interruptDigest, err := requestEndInterruptCommitRequest(request)
	if err != nil {
		return nil, err
	}
	consumedTransientAttachments, err := normalizeConsumedAttachmentRefs(request.GetConsumedAttachmentRefs())
	if err != nil {
		return nil, err
	}
	consumedFileAttachments, err := normalizeConsumedFileAttachments(request.GetConsumedFileAttachments())
	if err != nil {
		return nil, err
	}
	if len(consumedTransientAttachments.Refs)+len(consumedFileAttachments.Pairs) > MaxProviderRequestAttachments {
		return nil, status.Error(codes.InvalidArgument, "too many consumed attachments")
	}
	usageJSON := defaultString(request.GetUsageJson(), "{}")
	if !json.Valid([]byte(usageJSON)) {
		return nil, status.Error(codes.InvalidArgument, "usage must be JSON")
	}
	providerUsageJSON, err := parseProviderUsageJSON(usageJSON)
	if err != nil {
		return nil, err
	}
	requestKind, err := normalizeRequestKind(request.GetRequestKind())
	if err != nil {
		return nil, err
	}
	if requestKind == requestKindCompactionSummary &&
		(len(consumedTransientAttachments.Refs) > 0 || len(consumedFileAttachments.Pairs) > 0) {
		return nil, status.Error(codes.InvalidArgument, "compaction request end cannot consume attachments")
	}
	reschedule, err := normalizeRequestEndReschedule(request, requestKind)
	if err != nil {
		return nil, err
	}
	finishReason := defaultString(request.GetFinishReason(), "unknown")
	usage, err := parseBridgeUsage(usageJSON)
	if err != nil {
		return nil, err
	}
	payloadJSON, err := modelRequestEndPayloadJSON(request, requestKind, finishReason, usage)
	if err != nil {
		return nil, err
	}
	key := request.GetModelRequestId()
	declarationDigest, err := writeRequestEndDeclarationDigest(
		request,
		requestKind,
		finishReason,
		usageJSON,
		stableReasoning.CanonicalJSON,
		consumedTransientAttachments.CanonicalJSON,
		consumedFileAttachments.CanonicalJSON,
	)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var (
		ack              *bridgev1.BridgeWriteAck
		receipt          *bridgev1.DeclarationReceipt
		interruptReceipt *bridgev1.DeclarationReceipt
		observation      declarationApplicationObservation
	)
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.write_request_end", func(tx *dbconnect.Tx) error {
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
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpWriteRequestEnd,
			"model_request",
			key,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "request end receipt is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "request end idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "request end receipt is invalid")
			}
			if interruptRequest != nil {
				interruptReceipt, err = readRequestEndInterruptReceiptTx(
					ctx,
					tx,
					request.GetScope(),
					interruptRequest.GetRuntimeInputId(),
					interruptDigest,
				)
				if err != nil {
					return err
				}
			}
			ack = duplicateAck("", request.GetRuntimeWriteId())
			observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
			return err
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if !stableReasoning.StrictlyOrdered {
			return status.Error(codes.InvalidArgument, "stable reasoning parts must be strictly ordered")
		}
		// Only a successful end claims the complete reasoning set. Error and
		// reschedule ends preserve prefixes already anchored by durable tools.
		if !request.GetIsError() && request.GetReschedule() == nil {
			if err := validateStableReasoningSettlementSupersetTx(ctx, tx, request.GetScope(), request.GetModelRequestId(), stableReasoning); err != nil {
				return err
			}
		}
		if err := verifyModelRequestStartTx(ctx, tx, request.GetScope(), request.GetModelRequestStartEventId(), request.GetModelRequestId(), requestKind); err != nil {
			return err
		}
		if _, exists, err := modelRequestEndExistsTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetScope().GetSessionThreadId(), request.GetModelRequestId()); err != nil {
			return err
		} else if exists {
			return status.Error(codes.AlreadyExists, "model request is already closed")
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		visibility, sessionVisible := threadScope.publicProjection("span.model_request_end")
		activeTransientAttachmentRefs, err := validateTransientAttachmentsForConsumptionTx(
			ctx,
			tx,
			request.GetScope(),
			consumedTransientAttachments.Refs,
		)
		if err != nil {
			return err
		}
		if err := validateFileAttachmentConsumptionsTx(ctx, tx, request.GetScope(), consumedFileAttachments.Pairs); err != nil {
			return err
		}
		if !request.GetIsError() && len(activeTransientAttachmentRefs) > 0 {
			if err := markTransientAttachmentsConsumedTx(ctx, tx, request.GetScope(), activeTransientAttachmentRefs, now); err != nil {
				return err
			}
		}
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
			) VALUES ($1, $2, $3, $4, $5, 'span.model_request_end', $6, $7, $8, $9, $10, $11, $6, $12, $12, $12)`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			eventID,
			sequence,
			payloadJSON,
			visibility,
			sessionVisible,
			request.GetRuntimeWriteId(),
			request.GetModelRequestId(),
			stableReasoning.ledgerJSON(),
			now,
		); err != nil {
			return err
		}
		if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
			return err
		}
		if !request.GetIsError() && len(consumedFileAttachments.Pairs) > 0 {
			if err := insertFileAttachmentConsumptionsTx(
				ctx,
				tx,
				request.GetScope(),
				eventID,
				consumedFileAttachments.Pairs,
			); err != nil {
				return err
			}
		}
		usageResult, err := tx.Exec(ctx,
			`INSERT INTO request_usage_details (
				workspace_id, session_id, session_thread_id, model_request_id, runtime_write_id,
				request_kind, input_total_tokens, input_uncached_tokens, input_cache_read_tokens,
				input_cache_write_tokens, output_total_tokens, output_reasoning_tokens, total_tokens,
				provider_usage_json, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT (workspace_id, session_id, model_request_id, runtime_write_id) DO NOTHING`,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			request.GetModelRequestId(),
			request.GetRuntimeWriteId(),
			requestKind,
			usage.InputTotal,
			usage.InputUncached,
			nullableInt64(usage.InputCacheRead),
			nullableInt64(usage.InputCacheWrite),
			usage.OutputTotal,
			nullableInt64(usage.OutputReasoning),
			nullableInt64(usage.Total),
			providerUsageJSON,
			now,
		)
		if err != nil {
			return err
		}
		// sessions.usage accumulates usage for every request kind, approval_reviewer
		// included. But a reviewer request settles on its own sidecar thread scope:
		// its usage stops here — it never updates the parent thread's
		// context-window usage hint and never projects into the parent
		// session_messages.
		if rowsAffected(usageResult) {
			if err := incrementSessionUsageTx(ctx, tx, request.GetScope(), usage, now); err != nil {
				return err
			}
		}
		var disposition *requestEndDispositionResult
		if reschedule != nil {
			disposition, err = s.applyRequestEndRescheduleTx(ctx, tx, request, requestKind, threadScope, reschedule, now)
			if err != nil {
				return err
			}
		}
		receipt, err = commitWriteRequestEndDeclarationTx(
			ctx,
			tx,
			request,
			threadScope,
			eventID,
			sequence,
			stableReasoning,
			now,
		)
		if err != nil {
			return err
		}
		receipt.RequestReschedule = requestEndDispositionStamp(requestKind, disposition)
		receipt.DeclarationDigest = declarationDigest
		if interruptRequest != nil {
			if _, ok, err := readBridgeDeclarationOperationTx(
				ctx,
				tx,
				request.GetScope(),
				bridgeOpCommitInputs,
				"interrupt_control",
				interruptRequest.GetRuntimeInputId(),
			); err != nil {
				return err
			} else if ok {
				return status.Error(codes.AlreadyExists, "interrupt input is already settled")
			}
			interruptReceipt, err = commitInputDeclarationTx(
				ctx,
				tx,
				interruptRequest,
				"interrupt_control",
				interruptRequest.GetRuntimeInputId(),
				now,
			)
			if err != nil {
				return err
			}
			interruptReceipt.DeclarationDigest = interruptDigest
			interruptReceiptJSON, err := marshalDeclarationReceipt(interruptReceipt)
			if err != nil {
				return err
			}
			if err := insertBridgeDeclarationOperationTx(
				ctx,
				tx,
				request.GetScope(),
				bridgeOpCommitInputs,
				"interrupt_control",
				interruptRequest.GetRuntimeInputId(),
				interruptDigest,
				interruptReceiptJSON,
				now,
			); err != nil {
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
			bridgeOpWriteRequestEnd,
			"model_request",
			key,
			declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck("", request.GetRuntimeWriteId())
		observation, err = declarationApplicationObservationTx(ctx, tx, request.GetScope())
		return err
	}); err != nil {
		return nil, err
	}
	if receipt == nil || len(receipt.GetEvents()) == 0 || (interruptRequest != nil && interruptReceipt == nil) {
		return nil, status.Error(codes.FailedPrecondition, "request end receipt is invalid")
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpWriteRequestEnd,
		"model_request",
		key,
		declarationDigest,
		ack,
		observation,
	)
	receipts := []*bridgev1.DeclarationReceipt{receipt}
	if interruptReceipt != nil {
		receipts = append(receipts, interruptReceipt)
	}
	return &bridgev1.WriteRequestEndResponse{
		Ack: ack,
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  receipts,
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
}

func (s *PostgreSQLBridgeAPIStore) applyRequestEndRescheduleTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.WriteRequestEndRequest,
	requestKind string,
	threadScope threadMutationScope,
	reschedule *normalizedRequestEndReschedule,
	now time.Time,
) (*requestEndDispositionResult, error) {
	providerAttempts, compactionAttempts, err := lockTurnRetryCountersTx(ctx, tx, request.GetScope(), now)
	if err != nil {
		return nil, err
	}
	current := providerAttempts
	budget := s.providerRescheduleBudget()
	if requestKind == requestKindCompactionSummary {
		current = compactionAttempts
		budget = s.compactionRescheduleBudget()
	}
	wouldBeAttempt := current + 1
	if int64(reschedule.Attempt) != wouldBeAttempt {
		return &requestEndDispositionResult{
			Status:       "denied",
			DenialReason: "attempt_mismatch",
			Attempt:      reschedule.Attempt,
		}, nil
	}
	if wouldBeAttempt > budget {
		return &requestEndDispositionResult{
			Status:       "denied",
			DenialReason: "budget_exhausted",
			Attempt:      reschedule.Attempt,
		}, nil
	}
	if err := incrementTurnRetryCounterTx(ctx, tx, request.GetScope(), requestKind, now); err != nil {
		return nil, err
	}
	if err := appendRequestRescheduledStatusTx(ctx, tx, request, threadScope, now); err != nil {
		return nil, err
	}
	effectiveDeadline := effectiveRequestEndRescheduleDeadline(now, reschedule)
	return &requestEndDispositionResult{
		Status:            "accepted",
		Attempt:           reschedule.Attempt,
		EffectiveDeadline: effectiveDeadline.UTC().Format(time.RFC3339Nano),
	}, nil
}

func lockTurnRetryCountersTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) (int64, int64, error) {
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_turn_retries (
			workspace_id, session_id, session_thread_id, provider_attempts, compaction_attempts, updated_at
		) VALUES ($1, $2, $3, 0, 0, $4)
		ON CONFLICT (workspace_id, session_id, session_thread_id) DO NOTHING`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	); err != nil {
		return 0, 0, err
	}
	var providerAttempts, compactionAttempts int64
	if err := tx.QueryRow(ctx,
		`SELECT provider_attempts, compaction_attempts
		   FROM session_turn_retries
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&providerAttempts, &compactionAttempts); err != nil {
		return 0, 0, err
	}
	return providerAttempts, compactionAttempts, nil
}

func incrementTurnRetryCounterTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, requestKind string, now time.Time) error {
	statement := `UPDATE session_turn_retries
		    SET provider_attempts = provider_attempts + 1,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`
	if requestKind == requestKindCompactionSummary {
		statement = `UPDATE session_turn_retries
		    SET compaction_attempts = compaction_attempts + 1,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`
	}
	_, err := tx.Exec(ctx, statement,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	)
	return err
}

func resetTurnRetryCountersTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, now time.Time) error {
	_, err := tx.Exec(ctx,
		`UPDATE session_turn_retries
		    SET provider_attempts = 0,
		        compaction_attempts = 0,
		        updated_at = $4
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		now,
	)
	return err
}

func idleStopReasonSettlesTurn(raw string) bool {
	var stopReason struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(raw), &stopReason); err != nil {
		return false
	}
	return stopReason.Type == "end_turn" || stopReason.Type == "retries_exhausted"
}

func appendRequestRescheduledStatusTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	request *bridgev1.WriteRequestEndRequest,
	threadScope threadMutationScope,
	now time.Time,
) error {
	eventType := "session.status_rescheduled"
	payloadJSON := `{"type":"session.status_rescheduled"}`
	if threadScope.role != "main" {
		eventType = "session.thread_status_rescheduled"
		var err error
		payloadJSON, err = threadStatusPayloadJSON(eventType, request.GetScope(), threadScope, "")
		if err != nil {
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
			visibility, session_visible, runtime_write_id, model_request_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $7, $12, $12, $12)`,
		request.GetScope().GetWorkspaceId(),
		request.GetScope().GetSessionId(),
		request.GetScope().GetSessionThreadId(),
		eventID,
		sequence,
		eventType,
		payloadJSON,
		visibility,
		sessionVisible,
		request.GetRuntimeWriteId(),
		request.GetModelRequestId(),
		now,
	); err != nil {
		return err
	}
	if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
		return err
	}
	if threadScope.role == "main" {
		return markPublicSessionReschedulingTx(ctx, tx, request.GetScope(), now)
	}
	return updateChildThreadStatusTx(ctx, tx, request.GetScope(), "rescheduling", now)
}

func (s *PostgreSQLBridgeAPIStore) FinishIdle(ctx context.Context, request *bridgev1.FinishIdleRequest) (*bridgev1.FinishIdleResponse, error) {
	if request.GetDurableTurnId() == "" {
		return nil, status.Error(codes.InvalidArgument, "durable turn id is required")
	}
	stopReasonJSON := defaultString(request.GetStopReasonJson(), `{"type":"end_turn"}`)
	if !json.Valid([]byte(stopReasonJSON)) {
		return nil, status.Error(codes.InvalidArgument, "idle stop reason must be JSON")
	}
	payloadJSON, err := idleStatusPayloadJSON(stopReasonJSON)
	if err != nil {
		return nil, err
	}
	const sourceKind = "turn_closeout"
	key := request.GetDurableTurnId()
	declarationDigest, err := finishIdleDeclarationDigest(request, stopReasonJSON)
	if err != nil {
		return nil, err
	}
	now := s.now()
	capture, err := s.ensureFinishIdleOutputCapture(ctx, request, sourceKind, key, declarationDigest, now)
	if err != nil {
		return nil, err
	}
	capture, err = s.waitForFinishIdleOutputCapture(ctx, request.GetScope(), key, capture)
	if err != nil {
		return nil, err
	}
	var (
		ack     *bridgev1.BridgeWriteAck
		receipt *bridgev1.DeclarationReceipt
	)
	var adoptedCapture adoptedOutputCapture
	err = s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
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
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpFinishIdle,
			sourceKind,
			key,
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "finish idle receipt is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "finish idle idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "finish idle receipt is invalid")
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
		openDurableTurnID, err := loadOpenDurableTurnIDTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if openDurableTurnID == nil || *openDurableTurnID != key {
			return scopeSupersededError(status.Error(codes.FailedPrecondition, "durable turn is not open"))
		}
		if threadScope.role != "subagent" && len(request.GetDrafts()) != 0 {
			return status.Error(codes.InvalidArgument, "completion mail is only valid for a sub-agent thread")
		}
		if len(request.GetDrafts()) > 1 {
			return status.Error(codes.InvalidArgument, "finish idle accepts at most one completion mail")
		}
		projectionEventType := "session.status_idle"
		if threadScope.role != "main" {
			projectionEventType = "session.thread_status_idle"
		}
		visibility, sessionVisible := threadScope.publicProjection(projectionEventType)
		adoptedCapture, err = adoptFinishIdleOutputCaptureTx(ctx, tx, request.GetScope(), key, capture.Generation, now)
		if err != nil {
			return err
		}
		if idleStopReasonSettlesTurn(stopReasonJSON) {
			if err := resetTurnRetryCountersTx(ctx, tx, request.GetScope(), now); err != nil {
				return err
			}
		}
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if threadScope.role != "main" {
			childPayloadJSON, err := threadStatusPayloadJSON("session.thread_status_idle", request.GetScope(), threadScope, stopReasonJSON)
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO session_events (
					workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
					visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
				) VALUES ($1, $2, $3, $4, $5, 'session.thread_status_idle', $6, $7, $8, $9, $6, $10, $10, $10)`,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				request.GetScope().GetSessionThreadId(),
				eventID,
				sequence,
				childPayloadJSON,
				visibility,
				sessionVisible,
				key,
				now,
			); err != nil {
				return err
			}
			if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
				return err
			}
			if err := updateChildThreadStatusTx(ctx, tx, request.GetScope(), "idle", now); err != nil {
				return err
			}
		} else {
			if _, err := tx.Exec(ctx,
				`INSERT INTO session_events (
					workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
					visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
				) VALUES ($1, $2, $3, $4, $5, 'session.status_idle', $6, $7, $8, $9, $10, $11, $11, $11)`,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				request.GetScope().GetSessionThreadId(),
				eventID,
				sequence,
				payloadJSON,
				visibility,
				sessionVisible,
				key,
				payloadJSON,
				now,
			); err != nil {
				return err
			}
			if _, err := appendSessionEventStreamChangeTx(ctx, tx, request.GetScope(), eventID, visibility, sessionVisible, now); err != nil {
				return err
			}
			cleanupAfter := now.Add(defaultIdleCleanupDelay)
			statusResult, err := tx.Exec(ctx,
				`INSERT INTO session_runtime_status (
					workspace_id, session_id, status, status_event_id, idle_since, running_since, active_seconds_total, cleanup_after,
					binding_id, binding_generation, created_at, updated_at
				) VALUES ($1, $2, 'idle', $3, $4, NULL, 0, $5, $6, $7, $8, $8)
				ON CONFLICT (workspace_id, session_id) DO UPDATE SET
					status = 'idle',
					status_event_id = EXCLUDED.status_event_id,
					idle_since = EXCLUDED.idle_since,
					active_seconds_total = session_runtime_status.active_seconds_total + CASE
						WHEN session_runtime_status.running_since IS NULL THEN 0
						ELSE GREATEST(0, EXTRACT(EPOCH FROM (EXCLUDED.updated_at - session_runtime_status.running_since)))
					END,
					running_since = NULL,
					cleanup_after = EXCLUDED.cleanup_after,
					cleanup_enqueued_at = NULL,
					cleanup_claimed_at = NULL,
					cleanup_job_id = NULL,
					binding_id = EXCLUDED.binding_id,
					binding_generation = EXCLUDED.binding_generation,
					updated_at = EXCLUDED.updated_at
					WHERE session_runtime_status.status <> 'terminated'`,
				request.GetScope().GetWorkspaceId(),
				request.GetScope().GetSessionId(),
				eventID,
				now,
				cleanupAfter,
				request.GetScope().GetBinding().GetBindingId(),
				request.GetScope().GetBinding().GetBindingGeneration(),
				now,
			)
			if err != nil {
				return err
			}
			if !rowsAffected(statusResult) {
				return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime session is already terminal"))
			}
			if err := markPublicSessionIdleTx(ctx, tx, request.GetScope(), now, now); err != nil {
				return err
			}
		}
		receipt = &bridgev1.DeclarationReceipt{
			SessionThreadId:   request.GetScope().GetSessionThreadId(),
			OperationKind:     bridgeOpFinishIdle,
			SourceKind:        sourceKind,
			SourceId:          key,
			DeclarationDigest: declarationDigest,
			Events: []*bridgev1.DurableEventStamp{{
				SessionThreadId: request.GetScope().GetSessionThreadId(),
				SourceEventId:   key,
				EventId:         eventID,
				EventSequence:   sequence,
				Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_CREATED,
			}},
			IdleCloseout: &bridgev1.IdleCloseoutStamp{
				DurableTurnId:     key,
				IdleEventId:       eventID,
				IdleEventSequence: sequence,
				CommittedIdleAt:   now.UTC().Format(time.RFC3339Nano),
			},
		}
		if len(request.GetDrafts()) == 1 {
			mailEvent, mailMessage, err := appendDeclaredCompletionMailTx(
				ctx,
				tx,
				request.GetScope(),
				threadScope,
				key,
				request.GetDrafts()[0],
				now,
			)
			if err != nil {
				return err
			}
			receipt.Events = append(receipt.Events, mailEvent)
			receipt.Messages = append(receipt.Messages, mailMessage)
		}
		if _, err := rearmPendingCompletionMailForThreadTx(
			ctx,
			tx,
			request.GetScope().GetWorkspaceId(),
			request.GetScope().GetSessionId(),
			request.GetScope().GetSessionThreadId(),
			now,
		); err != nil {
			return err
		}
		receiptJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpFinishIdle,
			sourceKind,
			key,
			declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck("", key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	logOutputCaptureSkips(s.Logger, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), adoptedCapture.Skipped)
	logOutputCaptureScanRecords(s.Logger, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), adoptedCapture.ScanRecords)
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpFinishIdle,
		sourceKind,
		key,
		declarationDigest,
		ack,
		observation,
	)
	return &bridgev1.FinishIdleResponse{
		Ack: ack,
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  []*bridgev1.DeclarationReceipt{receipt},
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
}

func logOutputCaptureSkips(logger *slog.Logger, workspaceID string, sessionID string, skipped []stagedOutputCaptureSkipped) {
	if logger == nil {
		return
	}
	for _, entry := range skipped {
		logger.Info("bridge.output_capture.file_skipped",
			slog.String("operation", "output_capture.finish_idle"),
			slog.String("event.kind", "output_capture.file_skipped"),
			slog.String("component", ServiceNameBridgeAPI),
			slog.String("workspace.id", workspaceID),
			slog.String("session.id", sessionID),
			slog.String("output.path", entry.SourcePath),
			slog.String("output.skip.reason", entry.Reason),
			slog.Int64("output.size_bytes", entry.SizeBytes),
		)
	}
}

func logOutputCaptureScanRecords(logger *slog.Logger, workspaceID string, sessionID string, records []stagedOutputCaptureScanRecord) {
	if logger == nil {
		return
	}
	for _, record := range records {
		logger.Info("bridge.output_capture.scan_record",
			slog.String("operation", "output_capture.finish_idle"),
			slog.String("event.kind", "output_capture.scan_record"),
			slog.String("component", ServiceNameBridgeAPI),
			slog.String("workspace.id", workspaceID),
			slog.String("session.id", sessionID),
			slog.String("output.parent_path", record.ParentPath),
			slog.String("output.scan.reason", record.Reason),
			slog.Int("output.scan.count", record.Count),
		)
	}
}

func (s *PostgreSQLBridgeAPIStore) CommitRuntimeTermination(ctx context.Context, request *bridgev1.CommitRuntimeTerminationRequest) (*bridgev1.CommitRuntimeTerminationResponse, error) {
	if request.GetRuntimeWriteId() == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime write id is required")
	}
	failure, failureJSON, err := parseRuntimeTerminationFailure(request.GetFailureJson())
	if err != nil {
		return nil, err
	}
	const sourceKind = "runtime_termination"
	declarationDigest, err := runtimeTerminationDeclarationDigest(request, failureJSON)
	if err != nil {
		return nil, err
	}
	now := s.now()
	var (
		ack     *bridgev1.BridgeWriteAck
		receipt *bridgev1.DeclarationReceipt
	)
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_runtime_termination", func(tx *dbconnect.Tx) error {
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
		if existing, ok, err := readBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitRuntimeTermination,
			sourceKind,
			request.GetRuntimeWriteId(),
		); err != nil {
			return err
		} else if ok {
			if existing.DeclarationDigest == "" || existing.ReceiptJSON == "" {
				return status.Error(codes.FailedPrecondition, "runtime termination receipt is invalid")
			}
			if existing.DeclarationDigest != declarationDigest {
				return status.Error(codes.AlreadyExists, "runtime termination idempotency conflict")
			}
			receipt, err = unmarshalDeclarationReceipt(existing.ReceiptJSON)
			if err != nil {
				return status.Error(codes.FailedPrecondition, "runtime termination receipt is invalid")
			}
			ack = duplicateAck("", request.GetRuntimeWriteId())
			return nil
		}
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		openDurableTurnID, err := loadOpenDurableTurnIDTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		if openDurableTurnID == nil || *openDurableTurnID != request.GetRuntimeWriteId() {
			return scopeSupersededError(status.Error(codes.FailedPrecondition, "runtime termination durable turn is not open"))
		}
		if err := closeRuntimeTerminationSpansTx(ctx, tx, request.GetScope(), failure, now); err != nil {
			return err
		}
		receipt, err = commitRuntimeTerminationDeclarationsTx(
			ctx,
			tx,
			request.GetScope(),
			threadScope,
			request.GetRuntimeWriteId(),
			request.GetDrafts(),
			request.GetPendingToolCancellations(),
			request.GetSandboxExecutionToolUseEventIds(),
			now,
		)
		if err != nil {
			return err
		}
		orphanToolUses, err := runtimeTerminationOrphanToolUsesTx(ctx, tx, request.GetScope(), false)
		if err != nil {
			return err
		}
		if len(orphanToolUses) != 0 {
			return status.Error(codes.FailedPrecondition, "runtime termination has an undeclared live tool use")
		}
		if threadScope.role == "main" {
			if err := closeRuntimeTerminatedSessionSiblingsTx(ctx, tx, request.GetScope(), request.GetRuntimeWriteId(), now); err != nil {
				return err
			}
		}
		errorStamp, err := appendRuntimeTerminationErrorTx(ctx, tx, request.GetScope(), threadScope, request.GetRuntimeWriteId(), failureJSON, now)
		if err != nil {
			return err
		}
		statusStamp, err := appendRuntimeTerminatedStatusTx(ctx, tx, request.GetScope(), threadScope, request.GetRuntimeWriteId(), now)
		if err != nil {
			return err
		}
		receipt.Events = append(receipt.Events, errorStamp, statusStamp)
		receipt.DeclarationDigest = declarationDigest
		receiptJSON, err := marshalDeclarationReceipt(receipt)
		if err != nil {
			return err
		}
		if err := insertBridgeDeclarationOperationTx(
			ctx,
			tx,
			request.GetScope(),
			bridgeOpCommitRuntimeTermination,
			sourceKind,
			request.GetRuntimeWriteId(),
			declarationDigest,
			receiptJSON,
			now,
		); err != nil {
			return err
		}
		ack = committedAck("", request.GetRuntimeWriteId())
		return nil
	}); err != nil {
		return nil, err
	}
	observation, err := s.declarationApplicationObservation(ctx, request.GetScope())
	if err != nil {
		return nil, err
	}
	logRuntimeDeclaration(
		s.Logger,
		request.GetScope(),
		bridgeOpCommitRuntimeTermination,
		sourceKind,
		request.GetRuntimeWriteId(),
		declarationDigest,
		ack,
		observation,
	)
	return &bridgev1.CommitRuntimeTerminationResponse{
		Ack: ack,
		Declaration: &bridgev1.DeclarationResponse{
			Receipts:                  []*bridgev1.DeclarationReceipt{receipt},
			ObservedBindingId:         observation.BindingID,
			ObservedBindingGeneration: observation.BindingGeneration,
			ApplicationDisposition:    observation.Disposition,
		},
	}, nil
}

type requestEndDispositionResult struct {
	Status            string `json:"status"`
	DenialReason      string `json:"denial_reason,omitempty"`
	Attempt           int32  `json:"attempt,omitempty"`
	EffectiveDeadline string `json:"effective_deadline,omitempty"`
}

func requestEndDispositionStamp(requestKind string, result *requestEndDispositionResult) *bridgev1.RequestRescheduleStamp {
	if result == nil {
		return nil
	}
	disposition := bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_UNSPECIFIED
	switch {
	case result.Status == "accepted":
		disposition = bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_ACCEPTED
	case result.DenialReason == "attempt_mismatch":
		disposition = bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_DENIED_ATTEMPT_MISMATCH
	case result.DenialReason == "budget_exhausted":
		disposition = bridgev1.RequestRescheduleDisposition_REQUEST_RESCHEDULE_DISPOSITION_DENIED_BUDGET_EXHAUSTED
	}
	return &bridgev1.RequestRescheduleStamp{
		Disposition:       disposition,
		RequestKind:       requestKind,
		Attempt:           int64(result.Attempt),
		EffectiveDeadline: result.EffectiveDeadline,
	}
}

type createChildThreadResult struct {
	Status                string `json:"status"`
	ChildThreadID         string `json:"child_thread_id"`
	ThreadCreatedEventID  string `json:"thread_created_event_id"`
	ThreadCreatedSequence int64  `json:"thread_created_sequence"`
}

func scopeForThread(scope *bridgev1.RuntimeScope, threadID string) *bridgev1.RuntimeScope {
	return &bridgev1.RuntimeScope{
		RequestId:       scope.GetRequestId(),
		WorkspaceId:     scope.GetWorkspaceId(),
		SessionId:       scope.GetSessionId(),
		SessionThreadId: threadID,
		Binding: &bridgev1.RuntimeBindingRef{
			BindingId:         scope.GetBinding().GetBindingId(),
			BindingGeneration: scope.GetBinding().GetBindingGeneration(),
			TargetPodUid:      scope.GetBinding().GetTargetPodUid(),
		},
	}
}

func modelRequestEndPayloadJSON(request *bridgev1.WriteRequestEndRequest, requestKind string, finishReason string, usage bridgeUsage) (string, error) {
	cacheRead := int64(0)
	if usage.InputCacheRead != nil {
		cacheRead = *usage.InputCacheRead
	}
	cacheWrite := int64(0)
	if usage.InputCacheWrite != nil {
		cacheWrite = *usage.InputCacheWrite
	}
	payload := map[string]any{
		"type":                   "span.model_request_end",
		"model_request_id":       request.GetModelRequestId(),
		"model_request_start_id": request.GetModelRequestStartEventId(),
		"request_kind":           requestKind,
		"is_error":               request.GetIsError(),
		"finish_reason":          finishReason,
		"model_usage": map[string]any{
			"input_tokens":                usage.InputTotal,
			"output_tokens":               usage.OutputTotal,
			"cache_creation_input_tokens": cacheWrite,
			"cache_read_input_tokens":     cacheRead,
			"speed":                       nil,
		},
		"request_usage": json.RawMessage(defaultString(request.GetUsageJson(), "{}")),
	}
	if request.GetErrorKind() != "" {
		payload["error_kind"] = request.GetErrorKind()
	}
	return marshalBridgeJSON(payload)
}

func normalizeRequestKind(value string) (string, error) {
	switch defaultString(value, requestKindAgentProviderRequest) {
	case requestKindAgentProviderRequest:
		return requestKindAgentProviderRequest, nil
	case requestKindCompactionSummary:
		return requestKindCompactionSummary, nil
	case requestKindApprovalReviewer:
		return requestKindApprovalReviewer, nil
	default:
		return "", status.Error(codes.InvalidArgument, "request_kind is invalid")
	}
}

func validateRequestEndErrorKind(request *bridgev1.WriteRequestEndRequest) error {
	errorKind := request.GetErrorKind()
	if !request.GetIsError() {
		if errorKind != "" {
			return status.Error(codes.InvalidArgument, "error_kind requires is_error")
		}
		return nil
	}
	switch errorKind {
	case "provider_error", "gateway_stream_error", "gateway_protocol_error", "runtime_interrupted", "runtime_persistence_error", "runtime_semantic_error", "runtime_pod_lost":
		return nil
	case "":
		return status.Error(codes.InvalidArgument, "error_kind is required for request errors")
	default:
		return status.Error(codes.InvalidArgument, "error_kind is invalid")
	}
}

type normalizedRequestEndReschedule struct {
	Attempt  int32
	Deadline time.Time
	Backoff  time.Duration
}

func normalizeRequestEndReschedule(request *bridgev1.WriteRequestEndRequest, requestKind string) (*normalizedRequestEndReschedule, error) {
	reschedule := request.GetReschedule()
	if reschedule == nil {
		return nil, nil
	}
	if !request.GetIsError() {
		return nil, status.Error(codes.InvalidArgument, "request end reschedule requires an error close")
	}
	if requestKind == requestKindApprovalReviewer {
		return nil, status.Error(codes.InvalidArgument, "approval reviewer requests cannot be rescheduled")
	}
	if reschedule.GetAttempt() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "reschedule attempt must be positive")
	}
	if reschedule.GetBackoffMs() < 0 {
		return nil, status.Error(codes.InvalidArgument, "reschedule backoff must be non-negative")
	}
	deadline, err := time.Parse(time.RFC3339Nano, reschedule.GetDeadline())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "reschedule deadline must be RFC3339")
	}
	backoff := time.Duration(reschedule.GetBackoffMs()) * time.Millisecond
	if reschedule.GetBackoffMs() > int64(maxRescheduleBackoff/time.Millisecond) {
		backoff = maxRescheduleBackoff
	}
	return &normalizedRequestEndReschedule{
		Attempt:  reschedule.GetAttempt(),
		Deadline: deadline.UTC(),
		Backoff:  backoff,
	}, nil
}

func effectiveRequestEndRescheduleDeadline(now time.Time, reschedule *normalizedRequestEndReschedule) time.Time {
	maximum := now.Add(maxRescheduleBackoff)
	backoffDeadline := now.Add(reschedule.Backoff)
	effective := reschedule.Deadline
	if maximum.Before(effective) {
		effective = maximum
	}
	if backoffDeadline.Before(effective) {
		effective = backoffDeadline
	}
	if effective.Before(now) {
		return now
	}
	return effective
}

func idleStatusPayloadJSON(stopReasonJSON string) (string, error) {
	return marshalBridgeJSON(map[string]any{
		"type":        "session.status_idle",
		"stop_reason": json.RawMessage(stopReasonJSON),
	})
}

func threadStatusPayloadJSON(eventType string, scope *bridgev1.RuntimeScope, threadScope threadMutationScope, stopReasonJSON string) (string, error) {
	payload := map[string]any{
		"type":              eventType,
		"session_thread_id": scope.GetSessionThreadId(),
		"task_name":         nullableJSONString(threadScope.taskName),
	}
	if stopReasonJSON != "" {
		payload["stop_reason"] = json.RawMessage(stopReasonJSON)
	}
	return marshalBridgeJSON(payload)
}

func insertChildThreadIdleStatusEventTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	threadScope threadMutationScope,
	runtimeWriteID string,
	stopReasonJSON string,
	now time.Time,
) error {
	var existingEventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id
		   FROM session_events
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND runtime_write_id = $4
		  LIMIT 1`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		runtimeWriteID,
	).Scan(&existingEventID)
	if err == nil {
		return nil
	}
	if !dbconnect.IsNoRows(err) {
		return err
	}
	payloadJSON, err := threadStatusPayloadJSON("session.thread_status_idle", scope, threadScope, stopReasonJSON)
	if err != nil {
		return err
	}
	visibility, sessionVisible := threadScope.publicProjection("session.thread_status_idle")
	eventID := id.New("evt_")
	sequence, err := nextSessionEventSequenceTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
		) VALUES ($1, $2, $3, $4, $5, 'session.thread_status_idle', $6, $7, $8, $9, $6, $10, $10, $10)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		eventID,
		sequence,
		payloadJSON,
		visibility,
		sessionVisible,
		runtimeWriteID,
		now,
	); err != nil {
		return err
	}
	_, err = appendSessionEventStreamChangeTx(ctx, tx, scope, eventID, visibility, sessionVisible, now)
	return err
}

func updateChildThreadStatusTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, statusValue string, now time.Time) error {
	result, err := tx.Exec(ctx,
		`UPDATE session_threads
		    SET status = $4,
		        last_active_at = $5,
		        updated_at = $5
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND id = $3
		    AND role <> 'main'
		    AND status NOT IN ('terminated', 'failed', 'archived', 'closed_for_runtime')`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		statusValue,
		now,
	)
	if err != nil {
		return err
	}
	if !rowsAffected(result) {
		var currentStatus string
		if err := tx.QueryRow(ctx,
			`SELECT status
			   FROM session_threads
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND id = $3
			    AND role <> 'main'`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
			scope.GetSessionThreadId(),
		).Scan(&currentStatus); dbconnect.IsNoRows(err) {
			return closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "child thread status update failed"))
		} else if err != nil {
			return err
		}
		if currentStatus == "closed_for_runtime" {
			return nil
		}
		if currentStatus == "terminated" || currentStatus == "failed" || currentStatus == "archived" {
			return scopeSupersededError(status.Error(codes.FailedPrecondition, "child thread is already terminal"))
		}
		return closeoutUnrepairableError(status.Error(codes.FailedPrecondition, "child thread status update failed"))
	}
	return nil
}

type bridgeUsage struct {
	InputTotal      int64
	InputUncached   int64
	InputCacheRead  *int64
	InputCacheWrite *int64
	OutputTotal     int64
	OutputReasoning *int64
	Total           *int64
}

func parseBridgeUsage(raw string) (bridgeUsage, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage must be a JSON object")
	}
	if payload == nil {
		return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage must be a JSON object")
	}
	allowed := map[string]struct{}{
		"input_tokens": {}, "input_uncached_tokens": {}, "cache_read_input_tokens": {},
		"cache_creation_input_tokens": {}, "output_tokens": {}, "reasoning_output_tokens": {},
		"total_tokens":        {},
		"provider_usage_json": {},
	}
	for key := range payload {
		if _, ok := allowed[key]; !ok {
			return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage contains an unknown field")
		}
	}
	inputTotal, _, err := nonnegativeUsageInteger(payload, "input_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	cacheRead, _, err := optionalNonnegativeUsageInteger(payload, "cache_read_input_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	cacheWrite, _, err := optionalNonnegativeUsageInteger(payload, "cache_creation_input_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	inputUncached, hasInputUncached, err := optionalNonnegativeUsageInteger(payload, "input_uncached_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	if inputUncached == nil {
		value := inputTotal - optionalInt64Value(cacheRead) - optionalInt64Value(cacheWrite)
		if value < 0 {
			return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage input token arithmetic is invalid")
		}
		inputUncached = &value
	}
	if hasInputUncached && *inputUncached+optionalInt64Value(cacheRead)+optionalInt64Value(cacheWrite) != inputTotal {
		return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage input token arithmetic is invalid")
	}
	outputTotal, _, err := nonnegativeUsageInteger(payload, "output_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	outputReasoning, _, err := optionalNonnegativeUsageInteger(payload, "reasoning_output_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	if optionalInt64Value(outputReasoning) > outputTotal {
		return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage output token arithmetic is invalid")
	}
	total, hasTotal, err := optionalNonnegativeUsageInteger(payload, "total_tokens")
	if err != nil {
		return bridgeUsage{}, err
	}
	if total == nil {
		value := inputTotal + outputTotal
		total = &value
	} else if hasTotal && *total != inputTotal+outputTotal {
		return bridgeUsage{}, status.Error(codes.InvalidArgument, "usage total token arithmetic is invalid")
	}
	return bridgeUsage{
		InputTotal:      inputTotal,
		InputUncached:   *inputUncached,
		InputCacheRead:  cacheRead,
		InputCacheWrite: cacheWrite,
		OutputTotal:     outputTotal,
		OutputReasoning: outputReasoning,
		Total:           total,
	}, nil
}

func nonnegativeUsageInteger(payload map[string]json.RawMessage, key string) (int64, bool, error) {
	value, present := payload[key]
	if !present {
		return 0, false, nil
	}
	var parsed int64
	if err := json.Unmarshal(value, &parsed); err != nil || parsed < 0 {
		return 0, true, status.Error(codes.InvalidArgument, "usage counters must be nonnegative integers")
	}
	return parsed, true, nil
}

func optionalNonnegativeUsageInteger(payload map[string]json.RawMessage, key string) (*int64, bool, error) {
	value, present := payload[key]
	if !present || string(value) == "null" {
		return nil, present, nil
	}
	parsed, _, err := nonnegativeUsageInteger(payload, key)
	if err != nil {
		return nil, true, err
	}
	return &parsed, true, nil
}

func parseProviderUsageJSON(raw string) (string, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return "{}", nil
	}
	value, ok := payload["provider_usage_json"]
	if !ok {
		return "{}", nil
	}
	var providerUsageJSON string
	if err := json.Unmarshal(value, &providerUsageJSON); err != nil {
		return "", status.Error(codes.InvalidArgument, "provider usage must be a JSON string")
	}
	if providerUsageJSON == "" {
		return "{}", nil
	}
	if !json.Valid([]byte(providerUsageJSON)) {
		return "", status.Error(codes.InvalidArgument, "provider usage must be JSON")
	}
	return providerUsageJSON, nil
}

func optionalInt64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func incrementSessionUsageJSON(current string, usage bridgeUsage) (string, error) {
	payload := make(map[string]any)
	if strings.TrimSpace(current) != "" {
		if err := json.Unmarshal([]byte(current), &payload); err != nil {
			return "", status.Error(codes.FailedPrecondition, "session usage projection is malformed")
		}
	}
	cacheCreation := make(map[string]any)
	if existing, ok := payload["cache_creation"].(map[string]any); ok {
		cacheCreation = existing
	}
	payload["request_count"] = jsonNumberAsInt64(payload["request_count"]) + 1
	payload["input_total_tokens"] = jsonNumberAsInt64(payload["input_total_tokens"]) + usage.InputTotal
	payload["input_uncached_tokens"] = jsonNumberAsInt64(payload["input_uncached_tokens"]) + usage.InputUncached
	payload["input_cache_read_tokens"] = jsonNumberAsInt64(payload["input_cache_read_tokens"]) + optionalInt64Value(usage.InputCacheRead)
	payload["input_cache_write_tokens"] = jsonNumberAsInt64(payload["input_cache_write_tokens"]) + optionalInt64Value(usage.InputCacheWrite)
	payload["output_total_tokens"] = jsonNumberAsInt64(payload["output_total_tokens"]) + usage.OutputTotal
	payload["output_reasoning_tokens"] = jsonNumberAsInt64(payload["output_reasoning_tokens"]) + optionalInt64Value(usage.OutputReasoning)
	payload["total_tokens"] = jsonNumberAsInt64(payload["total_tokens"]) + optionalInt64Value(usage.Total)
	payload["input_tokens"] = jsonNumberAsInt64(payload["input_tokens"]) + usage.InputTotal
	payload["output_tokens"] = jsonNumberAsInt64(payload["output_tokens"]) + usage.OutputTotal
	payload["cache_read_input_tokens"] = jsonNumberAsInt64(payload["cache_read_input_tokens"]) + optionalInt64Value(usage.InputCacheRead)
	cacheCreation["ephemeral_1h_input_tokens"] = jsonNumberAsInt64(cacheCreation["ephemeral_1h_input_tokens"]) + optionalInt64Value(usage.InputCacheWrite)
	if _, ok := cacheCreation["ephemeral_5m_input_tokens"]; !ok {
		cacheCreation["ephemeral_5m_input_tokens"] = int64(0)
	}
	payload["cache_creation"] = cacheCreation
	return marshalBridgeJSON(payload)
}

func incrementSessionServerToolUsageJSON(current string, usage normalizedServerToolUseUsage) (string, error) {
	payload := make(map[string]any)
	if strings.TrimSpace(current) != "" {
		decoder := json.NewDecoder(strings.NewReader(current))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			return "", status.Error(codes.FailedPrecondition, "session usage projection is malformed")
		}
	}
	serverToolUse := make(map[string]any)
	if existing, ok := payload["server_tool_use"].(map[string]any); ok {
		serverToolUse = existing
	} else if payload["server_tool_use"] != nil {
		return "", status.Error(codes.FailedPrecondition, "session usage projection is malformed")
	}
	webSearch, err := checkedUsageCounterAdd(payload["web_search_requests"], usage.WebSearchRequests)
	if err != nil {
		return "", err
	}
	webFetch, err := checkedUsageCounterAdd(payload["web_fetch_requests"], usage.WebFetchRequests)
	if err != nil {
		return "", err
	}
	publicSearch, err := checkedUsageCounterAdd(serverToolUse["web_search_requests"], usage.WebSearchRequests)
	if err != nil {
		return "", err
	}
	publicFetch, err := checkedUsageCounterAdd(serverToolUse["web_fetch_requests"], usage.WebFetchRequests)
	if err != nil {
		return "", err
	}
	payload["web_search_requests"] = webSearch
	payload["web_fetch_requests"] = webFetch
	serverToolUse["web_search_requests"] = publicSearch
	serverToolUse["web_fetch_requests"] = publicFetch
	payload["server_tool_use"] = serverToolUse
	return marshalBridgeJSON(payload)
}

func checkedUsageCounterAdd(current any, delta int64) (int64, error) {
	value := int64(0)
	switch typed := current.(type) {
	case nil:
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil || parsed < 0 {
			return 0, status.Error(codes.FailedPrecondition, "session usage projection is malformed")
		}
		value = parsed
	case float64:
		if typed < 0 || typed != float64(int64(typed)) {
			return 0, status.Error(codes.FailedPrecondition, "session usage projection is malformed")
		}
		value = int64(typed)
	default:
		return 0, status.Error(codes.FailedPrecondition, "session usage projection is malformed")
	}
	if delta < 0 || value > int64(^uint64(0)>>1)-delta {
		return 0, status.Error(codes.InvalidArgument, "server tool usage counters overflow")
	}
	return value + delta, nil
}

func jsonNumberAsInt64(value any) int64 {
	switch typed := value.(type) {
	case int64:
		if typed < 0 {
			return 0
		}
		return typed
	case int:
		if typed < 0 {
			return 0
		}
		return int64(typed)
	case float64:
		if typed < 0 {
			return 0
		}
		return int64(typed)
	case json.Number:
		number, err := typed.Int64()
		if err != nil || number < 0 {
			return 0
		}
		return number
	default:
		return 0
	}
}
