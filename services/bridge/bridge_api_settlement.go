package agentruntimebridge

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	"github.com/tetral-ai/tetral/services/bridge/internal/outputcapture"
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
		if len(part.GetText()) > 1024*1024 || len(part.GetMetadataJson()) > 64*1024 {
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
		canonicalMetadata, err := json.Marshal(metadata)
		if err != nil {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning metadata must be a JSON object")
		}
		if len(canonicalMetadata) > 64*1024 {
			return normalizedStableReasoningSet{}, status.Error(codes.InvalidArgument, "stable reasoning part exceeds size bounds")
		}
		aggregateBytes += len(part.GetText()) + len(canonicalMetadata)
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
	canonicalSet, err := json.Marshal(normalized.Parts)
	if err != nil {
		return normalizedStableReasoningSet{}, err
	}
	normalized.CanonicalJSON = string(canonicalSet)
	return normalized, nil
}

func stableReasoningPartJSON(scope *bridgev1.RuntimeScope, part normalizedStableReasoningPart, now time.Time) (json.RawMessage, error) {
	timestamp := now
	value := map[string]any{
		"id":               part.ReasoningPartID,
		"sessionId":        scope.GetSessionId(),
		"sequence":         part.PartSequence,
		"createdAt":        timestamp,
		"updatedAt":        timestamp,
		"type":             "reasoning",
		"text":             part.Text,
		"truncated":        part.Truncated,
		"status":           "completed",
		"completedAt":      timestamp,
		"providerMetadata": part.Metadata,
	}
	if part.ProviderPartID != "" {
		value["providerPartId"] = part.ProviderPartID
	}
	return json.Marshal(value)
}

func stableReasoningContentIdentity(part map[string]any) []byte {
	identity := make(map[string]any, len(part))
	for key, value := range part {
		switch key {
		case "messageId", "createdAt", "updatedAt", "startedAt", "completedAt":
			continue
		default:
			identity[key] = value
		}
	}
	encoded, _ := json.Marshal(identity)
	return encoded
}

func sameStableReasoningContent(left, right map[string]any) bool {
	return bytes.Equal(stableReasoningContentIdentity(left), stableReasoningContentIdentity(right))
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
		metadata, err := json.Marshal(part["providerMetadata"])
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
	var messageData string
	err := tx.QueryRow(ctx,
		`SELECT data_json
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3 AND model_request_id = $4
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&messageData)
	if dbconnect.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var message map[string]any
	if err := json.Unmarshal([]byte(messageData), &message); err != nil {
		return status.Error(codes.FailedPrecondition, "assistant message projection is invalid")
	}
	candidates := make(map[string]map[string]any, len(settlement.Parts))
	for _, normalized := range settlement.Parts {
		raw, err := stableReasoningPartJSON(scope, normalized, time.Time{})
		if err != nil {
			return err
		}
		var candidate map[string]any
		if err := json.Unmarshal(raw, &candidate); err != nil {
			return err
		}
		candidates[normalized.ReasoningPartID] = candidate
	}
	parts, _ := message["parts"].([]any)
	for _, rawPart := range parts {
		existing, ok := rawPart.(map[string]any)
		if !ok || existing["type"] != "reasoning" {
			continue
		}
		partID, _ := existing["id"].(string)
		candidate, ok := candidates[partID]
		if !ok || !sameStableReasoningContent(existing, candidate) {
			return status.Error(codes.AlreadyExists, "stable reasoning settlement omits or diverges from anchored content")
		}
	}
	return nil
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
	key := request.GetModelRequestId() + ":" + request.GetRuntimeWriteId()
	rescheduleAttempt := ""
	rescheduleDeadline := ""
	rescheduleBackoff := ""
	if request.GetReschedule() != nil {
		rescheduleAttempt = strconv.FormatInt(int64(request.GetReschedule().GetAttempt()), 10)
		rescheduleDeadline = request.GetReschedule().GetDeadline()
		rescheduleBackoff = strconv.FormatInt(request.GetReschedule().GetBackoffMs(), 10)
	}
	requestHash := bridgeRequestHash(
		bridgeOpWriteRequestEnd,
		key,
		request.GetModelRequestStartEventId(),
		requestKind,
		boolHashPart(request.GetIsError()),
		request.GetErrorKind(),
		finishReason,
		usageJSON,
		consumedTransientAttachments.CanonicalJSON,
		consumedFileAttachments.CanonicalJSON,
		rescheduleAttempt,
		rescheduleDeadline,
		rescheduleBackoff,
		stableReasoning.CanonicalJSON,
	)
	now := s.now()
	var response *bridgev1.WriteRequestEndResponse
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.write_request_end", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpWriteRequestEnd, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "request end idempotency conflict")
			}
			var stored writeRequestEndResult
			if err := json.Unmarshal([]byte(existing.ResultJSON), &stored); err != nil {
				return err
			}
			response = &bridgev1.WriteRequestEndResponse{
				Ack:                   duplicateAck("", request.GetRuntimeWriteId()),
				RescheduleDisposition: requestEndDispositionProto(stored.RescheduleDisposition),
			}
			return nil
		}
		if !stableReasoning.StrictlyOrdered {
			return status.Error(codes.InvalidArgument, "stable reasoning parts must be strictly ordered")
		}
		if err := validateStableReasoningSettlementSupersetTx(ctx, tx, request.GetScope(), request.GetModelRequestId(), stableReasoning); err != nil {
			return err
		}
		if err := verifyModelRequestStartTx(ctx, tx, request.GetScope(), request.GetModelRequestStartEventId(), request.GetModelRequestId()); err != nil {
			return err
		}
		if _, exists, err := modelRequestEndExistsTx(ctx, tx, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), request.GetModelRequestId()); err != nil {
			return err
		} else if exists {
			stored := writeRequestEndResult{RescheduleDisposition: &requestEndDispositionResult{
				Status:       "denied",
				DenialReason: "stale_terminal",
			}}
			if reschedule != nil {
				stored.RescheduleDisposition.Attempt = reschedule.Attempt
			}
			resultJSON, err := marshalBridgeJSON(stored)
			if err != nil {
				return err
			}
			if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
				Operation:      bridgeOpWriteRequestEnd,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckCommitted,
				RuntimeWriteID: sql.NullString{String: request.GetRuntimeWriteId(), Valid: true},
				ResultJSON:     resultJSON,
				Now:            now,
			}); err != nil {
				return err
			}
			response = &bridgev1.WriteRequestEndResponse{
				Ack:                   committedAck("", request.GetRuntimeWriteId()),
				RescheduleDisposition: requestEndDispositionProto(stored.RescheduleDisposition),
			}
			return nil
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
		for _, reasoningPart := range stableReasoning.Parts {
			partJSON, err := stableReasoningPartJSON(request.GetScope(), reasoningPart, now)
			if err != nil {
				return err
			}
			if _, err := mergeAssistantMessagePartTx(ctx, tx, request.GetScope(), request.GetModelRequestId(), "", partJSON, true, now); err != nil {
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
		stored := writeRequestEndResult{EventID: eventID, Sequence: sequence, RescheduleDisposition: disposition}
		resultJSON, err := marshalBridgeJSON(stored)
		if err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpWriteRequestEnd,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			RuntimeWriteID: sql.NullString{String: request.GetRuntimeWriteId(), Valid: true},
			ResultJSON:     resultJSON,
			Now:            now,
		}); err != nil {
			return err
		}
		response = &bridgev1.WriteRequestEndResponse{
			Ack:                   committedAck("", request.GetRuntimeWriteId()),
			RescheduleDisposition: requestEndDispositionProto(disposition),
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
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
	if request.GetRuntimeWriteId() == "" {
		return nil, status.Error(codes.InvalidArgument, "runtime write id is required")
	}
	idleSince, err := defaultTime(request.GetIdleSince(), s.now())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "idle since must be an RFC 3339 timestamp")
	}
	stopReasonJSON := defaultString(request.GetStopReasonJson(), `{"type":"end_turn"}`)
	if !json.Valid([]byte(stopReasonJSON)) {
		return nil, status.Error(codes.InvalidArgument, "idle stop reason must be JSON")
	}
	payloadJSON, err := idleStatusPayloadJSON(stopReasonJSON)
	if err != nil {
		return nil, err
	}
	key := request.GetRuntimeWriteId()
	requestHash := bridgeRequestHash(bridgeOpFinishIdle, key, idleSince.UTC().Format(time.RFC3339Nano), stopReasonJSON)
	now := s.now()
	var ack *bridgev1.BridgeWriteAck
	var cleanupCapture func()
	var skippedCapture []outputcapture.SkippedFile
	var captureScanRecords []outputcapture.ScanRecord
	var captureScanFailure *outputcapture.CaptureScanError
	var finishIdleErr error
	err = s.withScopeTxAndCleanup(ctx, request.GetScope(), "agentruntimebridge.finish_idle", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpFinishIdle, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "finish idle idempotency conflict")
			}
			ack = duplicateAck("", key)
			return nil
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		projectionEventType := "session.status_idle"
		if threadScope.role != "main" {
			projectionEventType = "session.thread_status_idle"
		}
		visibility, sessionVisible := threadScope.publicProjection(projectionEventType)
		if s.OutputCapturer == nil {
			return status.Error(codes.FailedPrecondition, "output capture is not installed")
		}
		target, err := outputCaptureTargetTx(ctx, tx, request.GetScope(), s.sandboxStatusFreshnessWindow(), s.resourceCredentialRefreshMargin(), now)
		if err != nil {
			if isOutputCapturePreparationRequeued(err) {
				finishIdleErr = err
				return nil
			}
			return err
		}
		captureResult, err := s.OutputCapturer.CaptureOutputs(ctx, tx, outputcapture.Request{
			WorkspaceID:    request.GetScope().GetWorkspaceId(),
			SessionID:      request.GetScope().GetSessionId(),
			RuntimeWriteID: key,
			Target:         target,
			CapturedAt:     now,
		})
		if err != nil {
			var scanErr *outputcapture.CaptureScanError
			if !errors.As(err, &scanErr) {
				return err
			}
			// Output capture is best-effort by design, but settlement is not:
			// only typed pre-persistence scan outcomes are swallowed here. The
			// error-level record is the alarm; every persistence error still
			// aborts FinishIdle.
			captureScanFailure = scanErr
		} else {
			cleanupCapture = captureResult.Cleanup
			skippedCapture = captureResult.Skipped
			captureScanRecords = captureResult.ScanRecords
		}
		if idleStopReasonSettlesTurn(stopReasonJSON) {
			if err := resetTurnRetryCountersTx(ctx, tx, request.GetScope(), now); err != nil {
				return err
			}
		}
		if threadScope.role != "main" {
			// Stop reason alone cannot identify a completed child turn: end_turn
			// also closes terminal-error and processed-interrupt paths. Decide
			// completion mail from the durable events visible in this transaction.
			completionDecision, err := classifyFinishIdleCompletionTx(ctx, tx, request.GetScope(), threadScope, stopReasonJSON)
			if err != nil {
				return err
			}
			childPayloadJSON, err := threadStatusPayloadJSON("session.thread_status_idle", request.GetScope(), threadScope, stopReasonJSON)
			if err != nil {
				return err
			}
			eventID := id.New("evt_")
			sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
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
			if err := appendCompletionMailTx(ctx, tx, request.GetScope(), threadScope, key, completionDecision, now); err != nil {
				return err
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
			resultJSON, err := marshalBridgeJSON(writeEventResult{EventID: eventID, Sequence: sequence})
			if err != nil {
				return err
			}
			if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
				Operation:      bridgeOpFinishIdle,
				IdempotencyKey: key,
				RequestHash:    requestHash,
				AckStatus:      bridgeAckCommitted,
				RuntimeWriteID: sql.NullString{String: key, Valid: true},
				ResultJSON:     resultJSON,
				Now:            now,
			}); err != nil {
				return err
			}
			ack = committedAck("", key)
			return nil
		}
		eventID := id.New("evt_")
		sequence, err := nextSessionEventSequenceTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
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
			idleSince,
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
		if err := markPublicSessionIdleTx(ctx, tx, request.GetScope(), idleSince, now); err != nil {
			return err
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
		resultJSON, err := marshalBridgeJSON(writeEventResult{EventID: eventID, Sequence: sequence})
		if err != nil {
			return err
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpFinishIdle,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			RuntimeWriteID: sql.NullString{String: key, Valid: true},
			ResultJSON:     resultJSON,
			Now:            now,
		}); err != nil {
			return err
		}
		ack = committedAck("", key)
		return nil
	}, func() {
		if cleanupCapture != nil {
			cleanupCapture()
		}
	})
	if err != nil {
		if cleanupCapture != nil {
			cleanupCapture()
		}
		return nil, err
	}
	if finishIdleErr != nil {
		return nil, finishIdleErr
	}
	logOutputCaptureFailure(s.Logger, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), captureScanFailure)
	logOutputCaptureSkips(s.Logger, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), skippedCapture)
	logOutputCaptureScanRecords(s.Logger, request.GetScope().GetWorkspaceId(), request.GetScope().GetSessionId(), captureScanRecords)
	return &bridgev1.FinishIdleResponse{Ack: ack}, nil
}

func logOutputCaptureFailure(logger *slog.Logger, workspaceID string, sessionID string, captureErr *outputcapture.CaptureScanError) {
	if logger == nil || captureErr == nil {
		return
	}
	logger.Error("bridge.output_capture.scan_failed",
		slog.String("operation", "output_capture.finish_idle"),
		slog.String("event.kind", "output_capture.scan_failed"),
		slog.String("component", ServiceNameBridgeAPI),
		slog.String("workspace.id", workspaceID),
		slog.String("session.id", sessionID),
		slog.String("error.class", "output_capture_scan_error"),
		slog.String("error.code", captureErr.Kind()),
		slog.String("error.capture_kind", captureErr.CaptureKind),
		slog.String("error.capture_detail", boundedOutputCaptureFailureDetail(captureErr.CaptureDetail)),
		slog.Bool("retryable", true),
		slog.String("alert.family", "output_capture"),
	)
}

const outputCaptureFailureDetailMaxBytes = 512

func boundedOutputCaptureFailureDetail(detail string) string {
	if len(detail) <= outputCaptureFailureDetailMaxBytes {
		return detail
	}
	end := outputCaptureFailureDetailMaxBytes
	for end > 0 && !utf8.ValidString(detail[:end]) {
		end--
	}
	return detail[:end]
}

func logOutputCaptureSkips(logger *slog.Logger, workspaceID string, sessionID string, skipped []outputcapture.SkippedFile) {
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

func logOutputCaptureScanRecords(logger *slog.Logger, workspaceID string, sessionID string, records []outputcapture.ScanRecord) {
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
	now := s.now()
	var ack *bridgev1.BridgeWriteAck
	if err := s.withScopeTx(ctx, request.GetScope(), "agentruntimebridge.commit_runtime_termination", func(tx *dbconnect.Tx) error {
		if err := verifyRuntimeScopeTx(ctx, tx, request.GetScope()); err != nil {
			return err
		}
		threadScope, err := lockThreadMutationTx(ctx, tx, request.GetScope())
		if err != nil {
			return err
		}
		target := "thread:" + request.GetScope().GetSessionThreadId()
		if threadScope.role == "main" {
			target = "session"
		}
		key := target + ":" + request.GetRuntimeWriteId()
		requestHash := bridgeRequestHash(bridgeOpCommitRuntimeTermination, key, failureJSON)
		if existing, ok, err := readBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOpCommitRuntimeTermination, key); err != nil {
			return err
		} else if ok {
			if existing.RequestHash != requestHash {
				return status.Error(codes.AlreadyExists, "runtime termination idempotency conflict")
			}
			ack = duplicateAck("", request.GetRuntimeWriteId())
			return nil
		}
		sessionWide := threadScope.role == "main"
		if err := closeRuntimeTerminationSpansTx(ctx, tx, request.GetScope(), sessionWide, failure, now); err != nil {
			return err
		}
		if err := settleRuntimeTerminationToolUsesTx(ctx, tx, request.GetScope(), sessionWide, now); err != nil {
			return err
		}
		if err := settleRuntimeTerminationPendingWaitsTx(ctx, tx, request.GetScope(), sessionWide, now); err != nil {
			return err
		}
		if err := appendRuntimeTerminationErrorTx(ctx, tx, request.GetScope(), threadScope, request.GetRuntimeWriteId(), failureJSON, now); err != nil {
			return err
		}
		if err := appendRuntimeTerminatedStatusTx(ctx, tx, request.GetScope(), threadScope, request.GetRuntimeWriteId(), now); err != nil {
			return err
		}
		if threadScope.role == "subagent" && threadScope.status != "closed_for_runtime" {
			if err := appendCompletionMailTx(ctx, tx, request.GetScope(), threadScope, request.GetRuntimeWriteId(), completionMailDecision{
				Kind:   completionMailErrored,
				Reason: failure.Message,
			}, now); err != nil {
				return err
			}
		}
		if err := insertBridgeOperationTx(ctx, tx, request.GetScope(), bridgeOperationInsert{
			Operation:      bridgeOpCommitRuntimeTermination,
			IdempotencyKey: key,
			RequestHash:    requestHash,
			AckStatus:      bridgeAckCommitted,
			RuntimeWriteID: sql.NullString{String: request.GetRuntimeWriteId(), Valid: true},
			Now:            now,
		}); err != nil {
			return err
		}
		ack = committedAck("", request.GetRuntimeWriteId())
		return nil
	}); err != nil {
		return nil, err
	}
	return &bridgev1.CommitRuntimeTerminationResponse{Ack: ack}, nil
}

type writeEventResult struct {
	EventID  string `json:"event_id"`
	Sequence int64  `json:"sequence"`
}

type writeRequestEndResult struct {
	EventID               string                       `json:"event_id,omitempty"`
	Sequence              int64                        `json:"sequence,omitempty"`
	RescheduleDisposition *requestEndDispositionResult `json:"reschedule_disposition,omitempty"`
}

type requestEndDispositionResult struct {
	Status            string `json:"status"`
	DenialReason      string `json:"denial_reason,omitempty"`
	Attempt           int32  `json:"attempt,omitempty"`
	EffectiveDeadline string `json:"effective_deadline,omitempty"`
}

func requestEndDispositionProto(result *requestEndDispositionResult) *bridgev1.RequestEndRescheduleDisposition {
	if result == nil {
		return nil
	}
	return &bridgev1.RequestEndRescheduleDisposition{
		Status:            result.Status,
		DenialReason:      result.DenialReason,
		Attempt:           result.Attempt,
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
