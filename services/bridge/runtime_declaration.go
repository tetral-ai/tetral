package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// stableRuntimeID derives the cross-runtime local identity used to correlate
// an unstamped declaration with its database-assigned receipt.
func stableRuntimeID(parts ...string) string {
	hasher := sha256.New()
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len([]byte(part)))) // #nosec G115 -- identifiers are bounded below uint32 at protocol validation.
		_, _ = hasher.Write(length[:])
		_, _ = hasher.Write([]byte(part))
	}
	return "stid_" + hex.EncodeToString(hasher.Sum(nil))
}

func commitInputsDeclarationDigest(request *bridgev1.CommitInputsRequest, inputKind string) (string, error) {
	drafts := make([]any, 0, len(request.GetDrafts()))
	for _, draft := range request.GetDrafts() {
		messageInfo, err := canonicalRuntimeDeclarationJSON(draft.GetMessageInfoJson())
		if err != nil {
			return "", status.Error(codes.InvalidArgument, "runtime message draft info is invalid")
		}
		parts := make([]any, 0, len(draft.GetParts()))
		for _, part := range draft.GetParts() {
			partJSON, err := canonicalRuntimeDeclarationJSON(part.GetPartJson())
			if err != nil {
				return "", status.Error(codes.InvalidArgument, "runtime part draft is invalid")
			}
			parts = append(parts, map[string]any{
				"ordinal":               part.GetOrdinal(),
				"part_json":             json.RawMessage(partJSON),
				"part_kind":             part.GetPartKind(),
				"runtime_local_part_id": part.GetRuntimeLocalPartId(),
			})
		}
		drafts = append(drafts, map[string]any{
			"draft_kind":       draft.GetDraftKind().String(),
			"message_info":     json.RawMessage(messageInfo),
			"ordinal":          draft.GetOrdinal(),
			"parts":            parts,
			"runtime_local_id": draft.GetRuntimeLocalId(),
			"source_event_id":  nullableDeclarationString(draft.GetSourceEventId()),
			"source_id":        draft.GetSourceId(),
			"source_kind":      draft.GetSourceKind(),
		})
	}
	raw, err := json.Marshal(map[string]any{
		"drafts":            drafts,
		"event_ids":         request.GetEventIds(),
		"input_kind":        inputKind,
		"operation_kind":    bridgeOpCommitInputs,
		"runtime_input_id":  request.GetRuntimeInputId(),
		"sequence_from":     request.GetSequenceFrom(),
		"sequence_to":       request.GetSequenceTo(),
		"session_thread_id": request.GetScope().GetSessionThreadId(),
	})
	if err != nil {
		return "", err
	}
	canonical, err := canonicalRunToolJSON(string(raw))
	if err != nil {
		return "", err
	}
	return sha256Hex(canonical), nil
}

func canonicalRuntimeDeclarationJSON(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("missing declaration JSON")
	}
	return canonicalRunToolJSON(raw)
}

func nullableDeclarationString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func commitInputDraftsTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	runtimeInputID string,
	eventIDs []string,
	drafts []*bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DeclarationReceipt, error) {
	events := make(map[string]*bridgev1.DurableEventStamp, len(eventIDs))
	for _, eventID := range eventIDs {
		var eventType string
		var eventSequence int64
		if err := tx.QueryRow(ctx,
			`SELECT type, sequence
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
		).Scan(&eventType, &eventSequence); err != nil {
			return nil, err
		}
		if inputKind == "messages" && eventType != "user.message" {
			return nil, status.Error(codes.FailedPrecondition, "message input event type is not committable")
		}
		events[eventID] = &bridgev1.DurableEventStamp{
			SessionThreadId: scope.GetSessionThreadId(),
			SourceEventId:   eventID,
			EventId:         eventID,
			EventSequence:   eventSequence,
			Disposition:     bridgev1.DurableEventDisposition_DURABLE_EVENT_DISPOSITION_EXISTING,
		}
	}

	messageStamps := make([]*bridgev1.DurableMessageStamp, 0, len(drafts))
	for index, draft := range drafts {
		stamp, err := insertRuntimeMessageDraftTx(ctx, tx, scope, inputKind, runtimeInputID, eventIDs, index, draft, now)
		if err != nil {
			return nil, err
		}
		messageStamps = append(messageStamps, stamp)
	}
	eventStamps := make([]*bridgev1.DurableEventStamp, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		eventStamps = append(eventStamps, events[eventID])
	}
	return &bridgev1.DeclarationReceipt{
		SessionThreadId: scope.GetSessionThreadId(),
		OperationKind:   bridgeOpCommitInputs,
		SourceKind:      inputKind,
		SourceId:        runtimeInputID,
		Events:          eventStamps,
		Messages:        messageStamps,
	}, nil
}

func lockThreadMutationOnlyTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope) error {
	var locked string
	return tx.QueryRow(ctx,
		`SELECT id
		   FROM session_threads
		  WHERE workspace_id = $1 AND session_id = $2 AND id = $3
		  FOR UPDATE`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&locked)
}

func insertRuntimeMessageDraftTx(
	ctx context.Context,
	tx *dbconnect.Tx,
	scope *bridgev1.RuntimeScope,
	inputKind string,
	runtimeInputID string,
	eventIDs []string,
	index int,
	draft *bridgev1.RuntimeMessageDraft,
	now time.Time,
) (*bridgev1.DurableMessageStamp, error) {
	if draft == nil || draft.GetDraftKind() == bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_UNSPECIFIED ||
		draft.GetOrdinal() < 0 || int(draft.GetOrdinal()) != index ||
		draft.GetSourceKind() != inputKind || draft.GetSourceId() != runtimeInputID ||
		!containsDeclarationString(eventIDs, draft.GetSourceEventId()) {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft identity is invalid")
	}
	if inputKind == "messages" && draft.GetDraftKind() != bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT {
		return nil, status.Error(codes.InvalidArgument, "message input requires user input drafts")
	}
	expectedMessageID := stableRuntimeID(
		"runtime_message_draft",
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		draft.GetSourceKind(),
		draft.GetSourceId(),
		runtimeDraftKindToken(draft.GetDraftKind()),
		strconv.FormatInt(int64(draft.GetOrdinal()), 10),
	)
	if draft.GetRuntimeLocalId() != expectedMessageID {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft id is invalid")
	}
	var messageInfo map[string]any
	if err := json.Unmarshal([]byte(draft.GetMessageInfoJson()), &messageInfo); err != nil || messageInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft info is invalid")
	}
	if _, ok := messageInfo["providerId"]; ok {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft contains routing metadata")
	}
	if _, ok := messageInfo["modelId"]; ok {
		return nil, status.Error(codes.InvalidArgument, "runtime message draft contains routing metadata")
	}
	if inputKind == "messages" && (messageInfo["role"] != "user" || messageInfo["origin"] != "user") {
		return nil, status.Error(codes.InvalidArgument, "user input draft role is invalid")
	}
	var messageSequence int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(sequence), 0) + 1
		   FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
	).Scan(&messageSequence); err != nil {
		return nil, err
	}
	messageID := id.New("msg_")
	timestamp := now.UTC().Format(time.RFC3339Nano)
	messageInfo["id"] = messageID
	messageInfo["sessionId"] = scope.GetSessionId()
	messageInfo["sequence"] = messageSequence
	messageInfo["createdAt"] = timestamp
	messageInfo["updatedAt"] = timestamp

	partStamps := make([]*bridgev1.DurablePartStamp, 0, len(draft.GetParts()))
	parts := make([]any, 0, len(draft.GetParts()))
	partKindOrdinal := make(map[string]int32)
	for partSequence, part := range draft.GetParts() {
		if part == nil || !validRuntimePartKind(part.GetPartKind()) || part.GetOrdinal() != partKindOrdinal[part.GetPartKind()] {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft order is invalid")
		}
		partKindOrdinal[part.GetPartKind()]++
		expectedPartID := stableRuntimeID(
			"runtime_message_part_draft",
			draft.GetRuntimeLocalId(),
			part.GetPartKind(),
			strconv.FormatInt(int64(part.GetOrdinal()), 10),
		)
		if part.GetRuntimeLocalPartId() != expectedPartID {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft id is invalid")
		}
		var partInfo map[string]any
		if err := json.Unmarshal([]byte(part.GetPartJson()), &partInfo); err != nil || partInfo == nil || partInfo["type"] != part.GetPartKind() {
			return nil, status.Error(codes.InvalidArgument, "runtime part draft payload is invalid")
		}
		durablePartID := id.New("part_")
		partInfo["id"] = durablePartID
		partInfo["sessionId"] = scope.GetSessionId()
		partInfo["messageId"] = messageID
		partInfo["sequence"] = partSequence
		partInfo["createdAt"] = timestamp
		partInfo["updatedAt"] = timestamp
		parts = append(parts, partInfo)
		partStamps = append(partStamps, &bridgev1.DurablePartStamp{
			RuntimeLocalPartId: part.GetRuntimeLocalPartId(),
			PartId:             durablePartID,
			MessageId:          messageID,
			PartSequence:       int64(partSequence),
			CreatedAt:          timestamp,
			UpdatedAt:          timestamp,
			Disposition:        bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		})
	}
	messageInfo["parts"] = parts
	dataJSON, err := json.Marshal(messageInfo)
	if err != nil {
		return nil, err
	}
	kind := "assistant"
	if draft.GetDraftKind() == bridgev1.RuntimeDraftKind_RUNTIME_DRAFT_KIND_USER_INPUT {
		kind = "user"
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			data_json, source_event_id, last_event_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $9)`,
		scope.GetWorkspaceId(),
		scope.GetSessionId(),
		scope.GetSessionThreadId(),
		messageID,
		messageSequence,
		kind,
		string(dataJSON),
		draft.GetSourceEventId(),
		now,
	); err != nil {
		return nil, err
	}
	return &bridgev1.DurableMessageStamp{
		RuntimeLocalId:  draft.GetRuntimeLocalId(),
		SessionThreadId: scope.GetSessionThreadId(),
		OwningEventId:   draft.GetSourceEventId(),
		MessageId:       messageID,
		MessageSequence: messageSequence,
		CreatedAt:       timestamp,
		UpdatedAt:       timestamp,
		Disposition:     bridgev1.DurableProjectionDisposition_DURABLE_PROJECTION_DISPOSITION_CREATED,
		Parts:           partStamps,
	}, nil
}

func runtimeDraftKindToken(kind bridgev1.RuntimeDraftKind) string {
	return strings.ToLower(strings.TrimPrefix(kind.String(), "RUNTIME_DRAFT_KIND_"))
}

func containsDeclarationString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func validRuntimePartKind(kind string) bool {
	switch kind {
	case "text", "reasoning", "tool", "step-start", "step-finish":
		return true
	default:
		return false
	}
}

func marshalDeclarationReceipt(receipt *bridgev1.DeclarationReceipt) (string, error) {
	raw, err := protojson.Marshal(receipt)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalDeclarationReceipt(raw string) (*bridgev1.DeclarationReceipt, error) {
	receipt := new(bridgev1.DeclarationReceipt)
	if err := protojson.Unmarshal([]byte(raw), receipt); err != nil {
		return nil, err
	}
	return receipt, nil
}

type declarationApplicationObservation struct {
	BindingID         string
	BindingGeneration int64
	Disposition       bridgev1.ReceiptApplicationDisposition
}

func (s *PostgreSQLBridgeAPIStore) declarationApplicationObservation(
	ctx context.Context,
	scope *bridgev1.RuntimeScope,
) (declarationApplicationObservation, error) {
	var observation declarationApplicationObservation
	err := s.withScopeReadOnlyTx(ctx, scope, "agentruntimebridge.commit_inputs", func(tx *dbconnect.Tx) error {
		var podUID string
		err := tx.QueryRow(ctx,
			`SELECT binding_id, binding_generation, agent_runtime_pod_uid
			   FROM session_runtime_bindings
			  WHERE workspace_id = $1
			    AND session_id = $2`,
			scope.GetWorkspaceId(),
			scope.GetSessionId(),
		).Scan(&observation.BindingID, &observation.BindingGeneration, &podUID)
		if dbconnect.IsNoRows(err) {
			observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
			return nil
		}
		if err != nil {
			return err
		}
		if observation.BindingID == scope.GetBinding().GetBindingId() &&
			observation.BindingGeneration == scope.GetBinding().GetBindingGeneration() &&
			podUID == scope.GetBinding().GetTargetPodUid() {
			observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_CURRENT_CUSTODY
		} else {
			observation.Disposition = bridgev1.ReceiptApplicationDisposition_RECEIPT_APPLICATION_DISPOSITION_STALE_CUSTODY
		}
		return nil
	})
	return observation, err
}
