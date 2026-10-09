package runtimecontrol

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/id"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// AssistantMessageHeader is the locked parts-mode header of one model
// request's assistant message. NextPartIndex is both the part count and the
// next zero-based index; the reasoning counters hold the request's admitted
// reasoning parts and charged bytes.
type AssistantMessageHeader struct {
	MessageID          string
	Sequence           int64
	NextPartIndex      int64
	ReasoningPartCount int64
	ReasoningBytes     int64
}

// AssistantPart is one new part, already canonical and validated by its
// owning writer. ReasoningBytes is the admitted charge of a reasoning part
// and zero for every other kind.
type AssistantPart struct {
	Value          map[string]any
	ReasoningBytes int64
}

// ReasoningBudget bounds a model request's cumulative reasoning count and
// bytes at declaration admission.
type ReasoningBudget struct {
	Parts int64
	Bytes int64
}

// AssistantPartsAppend describes one append under the caller's Session and
// Thread fence. A nil Header creates the message with SourceEventID as its
// source; otherwise Header is the row LockAssistantMessageHeaderTx locked in
// the same transaction. ReasoningBudget is set only for declaration admission.
type AssistantPartsAppend struct {
	Scope           *bridgev1.RuntimeScope
	ModelRequestID  string
	Header          *AssistantMessageHeader
	SourceEventID   string
	Parts           []AssistantPart
	ReasoningBudget *ReasoningBudget
	Now             time.Time
}

type encodedAssistantPart struct {
	kind            string
	modelToolCallID string
	dataJSON        string
}

// LockAssistantMessageHeaderTx locks the model request's assistant message
// header without reading any part content. It reports false when the request
// has no assistant message.
func LockAssistantMessageHeaderTx(ctx context.Context, tx *dbconnect.Tx, scope *bridgev1.RuntimeScope, modelRequestID string) (AssistantMessageHeader, bool, error) {
	var header AssistantMessageHeader
	var contentStorage string
	err := tx.QueryRow(ctx,
		`SELECT message_id, sequence, content_storage, next_part_index, reasoning_part_count, reasoning_bytes
		   FROM session_messages
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND session_thread_id = $3
		    AND model_request_id = $4
		    AND kind = 'assistant'
		  FOR UPDATE`,
		scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), modelRequestID,
	).Scan(&header.MessageID, &header.Sequence, &contentStorage, &header.NextPartIndex, &header.ReasoningPartCount, &header.ReasoningBytes)
	if dbconnect.IsNoRows(err) {
		return AssistantMessageHeader{}, false, nil
	}
	if err != nil {
		return AssistantMessageHeader{}, false, err
	}
	if contentStorage != "parts" || header.NextPartIndex <= 0 || header.ReasoningPartCount < 0 ||
		header.ReasoningBytes < 0 || header.ReasoningPartCount > header.NextPartIndex {
		return AssistantMessageHeader{}, false, status.Error(codes.FailedPrecondition, "durable assistant context is invalid")
	}
	return header, true, nil
}

// AppendAssistantMessagePartsTx appends new parts to one assistant message in
// the caller's transaction. It encodes each part once, allocates the next
// contiguous indexes, writes the header (creating it with its final counters,
// or advancing the locked counters once) and inserts every part in one
// statement. Existing parts are never read or rewritten. An empty delta on an
// existing header writes nothing.
func AppendAssistantMessagePartsTx(ctx context.Context, tx *dbconnect.Tx, request AssistantPartsAppend) (AssistantMessageHeader, error) {
	if request.Scope == nil || request.ModelRequestID == "" || (request.Header == nil && (len(request.Parts) == 0 || request.SourceEventID == "")) {
		return AssistantMessageHeader{}, status.Error(codes.InvalidArgument, "assistant context append is incomplete")
	}
	if len(request.Parts) == 0 {
		return *request.Header, nil
	}
	encoded, reasoningParts, reasoningBytes, err := encodeAssistantParts(request.Parts)
	if err != nil {
		return AssistantMessageHeader{}, err
	}
	var header AssistantMessageHeader
	if request.Header != nil {
		header = *request.Header
	}
	nextPartIndex, ok := checkedAdd(header.NextPartIndex, int64(len(encoded)))
	if !ok {
		return AssistantMessageHeader{}, status.Error(codes.FailedPrecondition, "durable assistant context is invalid")
	}
	totalReasoningParts, okParts := checkedAdd(header.ReasoningPartCount, reasoningParts)
	totalReasoningBytes, okBytes := checkedAdd(header.ReasoningBytes, reasoningBytes)
	if !okParts || !okBytes {
		return AssistantMessageHeader{}, status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
	}
	if budget := request.ReasoningBudget; budget != nil && (totalReasoningParts > budget.Parts || totalReasoningBytes > budget.Bytes) {
		return AssistantMessageHeader{}, status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
	}
	scope := request.Scope
	firstIndex := header.NextPartIndex
	if request.Header == nil {
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(sequence), 0) + 1
			   FROM session_messages
			  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
		).Scan(&header.Sequence); err != nil {
			return AssistantMessageHeader{}, err
		}
		header.MessageID = id.New("msg_")
		if _, err := tx.Exec(ctx,
			`INSERT INTO session_messages (
				workspace_id, session_id, session_thread_id, message_id, sequence, kind,
				content_storage, data_json, next_part_index, reasoning_part_count, reasoning_bytes,
				source_event_id, model_request_id, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, 'assistant', 'parts', NULL, $6, $7, $8, $9, $10, $11, $11)`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(),
			header.MessageID, header.Sequence, nextPartIndex, totalReasoningParts, totalReasoningBytes,
			request.SourceEventID, request.ModelRequestID, request.Now,
		); err != nil {
			return AssistantMessageHeader{}, err
		}
	} else {
		result, err := tx.Exec(ctx,
			`UPDATE session_messages
			    SET next_part_index = $5,
			        reasoning_part_count = $6,
			        reasoning_bytes = $7,
			        updated_at = $8
			  WHERE workspace_id = $1
			    AND session_id = $2
			    AND session_thread_id = $3
			    AND message_id = $4
			    AND content_storage = 'parts'
			    AND next_part_index = $9`,
			scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), header.MessageID,
			nextPartIndex, totalReasoningParts, totalReasoningBytes, request.Now, header.NextPartIndex,
		)
		if err != nil {
			return AssistantMessageHeader{}, err
		}
		if !RowsAffected(result) {
			return AssistantMessageHeader{}, status.Error(codes.FailedPrecondition, "assistant append lost its durable context")
		}
	}
	values := make([]string, 0, len(encoded))
	args := []any{scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), header.MessageID}
	for offset, part := range encoded {
		base := len(args)
		values = append(values, fmt.Sprintf("($1, $2, $3, $4, $%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4))
		var modelToolCallID any
		if part.modelToolCallID != "" {
			modelToolCallID = part.modelToolCallID
		}
		args = append(args, firstIndex+int64(offset), part.kind, modelToolCallID, part.dataJSON)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO session_message_parts (
			workspace_id, session_id, session_thread_id, message_id,
			part_index, part_kind, model_tool_call_id, data_json
		) VALUES `+strings.Join(values, ", "),
		args...,
	); err != nil {
		return AssistantMessageHeader{}, err
	}
	header.NextPartIndex = nextPartIndex
	header.ReasoningPartCount = totalReasoningParts
	header.ReasoningBytes = totalReasoningBytes
	return header, nil
}

// encodeAssistantParts derives each stored row from its one validated part:
// the kind and model call identity it carries, its raw JSON and the reasoning
// charge its writer admitted.
func encodeAssistantParts(parts []AssistantPart) ([]encodedAssistantPart, int64, int64, error) {
	encoded := make([]encodedAssistantPart, 0, len(parts))
	identities := make(map[[2]string]struct{}, len(parts))
	var reasoningParts, reasoningBytes int64
	for _, part := range parts {
		kind, _ := part.Value["type"].(string)
		modelToolCallID, _ := part.Value["modelToolCallId"].(string)
		switch kind {
		case "text", "reasoning":
			if _, present := part.Value["modelToolCallId"]; present {
				return nil, 0, 0, status.Error(codes.InvalidArgument, "runtime context part is invalid")
			}
		case "tool_call", "tool_result":
			if modelToolCallID == "" {
				return nil, 0, 0, status.Error(codes.InvalidArgument, "runtime context part is invalid")
			}
			identity := [2]string{kind, modelToolCallID}
			if _, duplicate := identities[identity]; duplicate {
				return nil, 0, 0, status.Error(codes.InvalidArgument, "assistant context delta repeats a Tool identity")
			}
			identities[identity] = struct{}{}
		default:
			return nil, 0, 0, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		if kind == "reasoning" {
			if part.ReasoningBytes < 0 {
				return nil, 0, 0, status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
			}
			var ok bool
			reasoningParts++
			if reasoningBytes, ok = checkedAdd(reasoningBytes, part.ReasoningBytes); !ok {
				return nil, 0, 0, status.Error(codes.InvalidArgument, "stable reasoning exceeds per-request budget")
			}
		} else if part.ReasoningBytes != 0 {
			return nil, 0, 0, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		dataJSON, err := json.Marshal(part.Value)
		if err != nil {
			return nil, 0, 0, status.Error(codes.InvalidArgument, "runtime context part is invalid")
		}
		encoded = append(encoded, encodedAssistantPart{kind: kind, modelToolCallID: modelToolCallID, dataJSON: string(dataJSON)})
	}
	return encoded, reasoningParts, reasoningBytes, nil
}

func checkedAdd(left int64, right int64) (int64, bool) {
	if right < 0 || left > math.MaxInt64-right {
		return 0, false
	}
	return left + right, true
}

// StoredMessageContentSQL selects the stored {"parts":[...]} document of the
// session_messages row aliased m, joined with StoredMessagePartsJoinSQL. An
// embedded message returns its data_json; a parts-mode message returns its
// rows assembled in index order only when they are exactly 0..next-1, and
// NULL otherwise. The assembled text is never cast or re-serialized.
const StoredMessageContentSQL = `CASE
	WHEN m.content_storage = 'embedded' THEN m.data_json
	WHEN stored_parts.part_count = m.next_part_index
	 AND stored_parts.first_index = 0
	 AND stored_parts.last_index = m.next_part_index - 1 THEN stored_parts.assembled
END`

// StoredMessagePartsJoinSQL aggregates one selected parts-mode message's
// rows by its (workspace_id, message_id) key; embedded messages read no part.
const StoredMessagePartsJoinSQL = `CROSS JOIN LATERAL (
	SELECT count(*) AS part_count,
	       min(part.part_index) AS first_index,
	       max(part.part_index) AS last_index,
	       '{"parts":[' || string_agg(part.data_json, ',' ORDER BY part.part_index) || ']}' AS assembled
	  FROM session_message_parts part
	 WHERE m.content_storage = 'parts'
	   AND part.workspace_id = m.workspace_id
	   AND part.message_id = m.message_id
) stored_parts`

// StoredMessageContentJSON returns a selected message's stored document. A
// parts-mode message whose rows are missing or not contiguous is malformed
// durable context, never an empty or embedded message.
func StoredMessageContentJSON(content sql.NullString) (string, error) {
	if !content.Valid {
		return "", status.Error(codes.FailedPrecondition, "durable context entry is malformed")
	}
	return content.String, nil
}
