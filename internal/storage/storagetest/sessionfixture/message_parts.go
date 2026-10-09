package sessionfixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

// MessageContentSQL is a scalar expression for the session_messages row
// aliased m: an embedded message's data_json, or a parts-mode message's part
// rows joined in index order into one {"parts":[...]} document. Tests use it
// to inspect stored content; production readers use the contiguity-checked
// runtimecontrol projection.
const MessageContentSQL = `COALESCE(m.data_json, (
	SELECT '{"parts":[' || string_agg(fixture_part.data_json, ',' ORDER BY fixture_part.part_index) || ']}'
	  FROM session_message_parts fixture_part
	 WHERE fixture_part.workspace_id = m.workspace_id AND fixture_part.message_id = m.message_id))`

// SeedAssistantMessagePartsForTest inserts a parts-mode assistant message of
// modelRequestID with the given raw part documents at indexes 0..n-1. Each
// part's kind and model call ID are read from the part; a reasoning part is
// counted and charged its text bytes plus its metadata JSON, an absent
// metadata object counting as {}.
func SeedAssistantMessagePartsForTest(
	t testing.TB,
	db *sql.DB,
	workspaceID string,
	sessionID string,
	threadID string,
	messageID string,
	sequence int64,
	sourceEventID any,
	modelRequestID string,
	parts ...string,
) {
	t.Helper()
	if len(parts) == 0 {
		t.Fatal("parts-mode assistant fixture needs at least one part")
	}
	type partShape struct {
		Type             string          `json:"type"`
		Text             string          `json:"text"`
		ModelToolCallID  string          `json:"modelToolCallId"`
		ProviderMetadata json.RawMessage `json:"providerMetadata"`
	}
	var reasoningParts, reasoningBytes int64
	shapes := make([]partShape, len(parts))
	for index, raw := range parts {
		if err := json.Unmarshal([]byte(raw), &shapes[index]); err != nil {
			t.Fatalf("decode fixture part %d: %v", index, err)
		}
		if shapes[index].Type == "reasoning" {
			reasoningParts++
			metadataBytes := len(shapes[index].ProviderMetadata)
			if metadataBytes == 0 {
				metadataBytes = len("{}")
			}
			reasoningBytes += int64(len(shapes[index].Text) + metadataBytes)
		}
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind,
			content_storage, data_json, next_part_index, reasoning_part_count, reasoning_bytes,
			source_event_id, model_request_id, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, 'assistant', 'parts', NULL, $6, $7, $8, $9, $10, now(), now())`,
		workspaceID, sessionID, threadID, messageID, sequence, len(parts), reasoningParts, reasoningBytes,
		sourceEventID, modelRequestID,
	); err != nil {
		t.Fatalf("seed parts-mode assistant message %s: %v", messageID, err)
	}
	for index, raw := range parts {
		var modelToolCallID any
		if shapes[index].ModelToolCallID != "" {
			modelToolCallID = shapes[index].ModelToolCallID
		}
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO session_message_parts (
				workspace_id, session_id, session_thread_id, message_id, part_index, part_kind, model_tool_call_id, data_json
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			workspaceID, sessionID, threadID, messageID, index, shapes[index].Type, modelToolCallID, raw,
		); err != nil {
			t.Fatalf("seed assistant message %s part %d: %v", messageID, index, err)
		}
	}
}

// AppendAssistantMessagePartsForTest appends raw part documents to the
// parts-mode assistant message of modelRequestID at its next indexes and
// advances its part count, as a writer's append would.
func AppendAssistantMessagePartsForTest(t testing.TB, db *sql.DB, workspaceID string, sessionID string, threadID string, modelRequestID string, parts ...string) {
	t.Helper()
	var messageID string
	var next int64
	if err := db.QueryRowContext(context.Background(),
		`UPDATE session_messages SET next_part_index = next_part_index + $5
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND model_request_id = $4 AND content_storage = 'parts'
		  RETURNING message_id, next_part_index - $5`,
		workspaceID, sessionID, threadID, modelRequestID, len(parts),
	).Scan(&messageID, &next); err != nil {
		t.Fatalf("advance assistant message of %s: %v", modelRequestID, err)
	}
	for offset, raw := range parts {
		var shape struct {
			Type            string `json:"type"`
			ModelToolCallID string `json:"modelToolCallId"`
		}
		if err := json.Unmarshal([]byte(raw), &shape); err != nil {
			t.Fatalf("decode appended fixture part: %v", err)
		}
		if shape.Type == "reasoning" {
			t.Fatal("appended fixture parts do not charge reasoning")
		}
		var modelToolCallID any
		if shape.ModelToolCallID != "" {
			modelToolCallID = shape.ModelToolCallID
		}
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO session_message_parts (
				workspace_id, session_id, session_thread_id, message_id, part_index, part_kind, model_tool_call_id, data_json
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			workspaceID, sessionID, threadID, messageID, next+int64(offset), shape.Type, modelToolCallID, raw,
		); err != nil {
			t.Fatalf("append assistant message %s part: %v", messageID, err)
		}
	}
}

// ReplaceAssistantMessagePartsForTest replaces every part of the parts-mode
// assistant message of modelRequestID with the given raw part documents, as a
// fixture shortcut for arranging committed content. Production writers never
// rewrite parts.
func ReplaceAssistantMessagePartsForTest(t testing.TB, db *sql.DB, workspaceID string, sessionID string, threadID string, modelRequestID string, parts ...string) {
	t.Helper()
	var messageID string
	var sequence int64
	var sourceEventID sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`DELETE FROM session_messages
		  WHERE workspace_id = $1 AND session_id = $2 AND session_thread_id = $3
		    AND model_request_id = $4 AND content_storage = 'parts'
		  RETURNING message_id, sequence, source_event_id`,
		workspaceID, sessionID, threadID, modelRequestID,
	).Scan(&messageID, &sequence, &sourceEventID); err != nil {
		t.Fatalf("remove assistant message of %s: %v", modelRequestID, err)
	}
	var source any
	if sourceEventID.Valid {
		source = sourceEventID.String
	}
	SeedAssistantMessagePartsForTest(t, db, workspaceID, sessionID, threadID, messageID, sequence, source, modelRequestID, parts...)
}
