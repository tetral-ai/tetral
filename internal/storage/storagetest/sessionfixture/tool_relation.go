package sessionfixture

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/tetral-ai/tetral/internal/eventwire"
)

// ToolEventRelation holds the relation columns that the production writer of a
// Tool event stores with it.
type ToolEventRelation struct {
	ModelRequestID  sql.NullString
	ModelToolCallID sql.NullString
	ToolUseEventID  sql.NullString
}

// ToolEventRelationForTest returns the relation columns for a fixture that
// inserts a Tool event directly. A seeded Tool Use belongs to request
// mreq_<event ID> and declares call call_<event ID>, the defaults
// SeedBridgeAPIAllowedToolRoute projects. An ordinary or MCP Tool Result
// references the Tool Use named by its public tool_use_id or mcp_tool_use_id
// payload key and belongs to that Tool Use's request; an invalid-tool repair
// result carries the call ID of its payload. Other event types carry none.
func ToolEventRelationForTest(t testing.TB, db *sql.DB, workspaceID string, eventID string, eventType string, payloadJSON string) ToolEventRelation {
	t.Helper()
	valid := func(value string) sql.NullString { return sql.NullString{String: value, Valid: true} }
	switch eventType {
	case "agent.tool_use", "agent.mcp_tool_use":
		return ToolEventRelation{ModelRequestID: valid("mreq_" + eventID), ModelToolCallID: valid("call_" + eventID)}
	case "agent.tool_result", "agent.mcp_tool_result":
	default:
		return ToolEventRelation{}
	}
	var payload struct {
		ToolUseID       string `json:"tool_use_id"`
		MCPToolUseID    string `json:"mcp_tool_use_id"`
		RepairKind      string `json:"repair_kind"`
		ModelToolCallID string `json:"model_tool_call_id"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode seeded Tool Result payload: %v", err)
	}
	if payload.RepairKind != "" {
		if eventType != "agent.tool_result" || payload.ModelToolCallID == "" {
			t.Fatalf("seeded invalid-tool repair %s needs its model_tool_call_id", eventID)
		}
		return ToolEventRelation{ModelRequestID: valid("mreq_" + eventID), ModelToolCallID: valid(payload.ModelToolCallID)}
	}
	toolUseEventID := payload.ToolUseID
	if eventType == "agent.mcp_tool_result" {
		toolUseEventID = payload.MCPToolUseID
	}
	if toolUseEventID == "" {
		t.Fatalf("seeded %s %s must name its Tool Use through its public payload key", eventType, eventID)
	}
	var modelRequestID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT model_request_id FROM session_events
		  WHERE workspace_id = $1 AND event_id = $2 AND type IN ('agent.tool_use','agent.mcp_tool_use')`,
		workspaceID, toolUseEventID,
	).Scan(&modelRequestID); err != nil {
		t.Fatalf("read Tool Use %s of seeded %s: %v", toolUseEventID, eventID, err)
	}
	return ToolEventRelation{ModelRequestID: valid(modelRequestID), ToolUseEventID: valid(toolUseEventID)}
}

// ToolRelationFact is one durable Tool event as its relation columns name it.
type ToolRelationFact struct {
	EventID         string
	EventType       string
	ThreadID        string
	ModelRequestID  string
	ModelToolCallID string
	ToolUseEventID  string
	Repair          bool
}

// RequireToolRelationFactsForTest reads every Tool event of the Session and
// fails unless each row's relation columns name exactly the identity its
// payload, projection and public projection carry:
//
//   - a Tool Use carries its projection's call ID and no Tool Use reference;
//   - an ordinary Tool Result references, in its own Thread and request, the
//     Tool Use named by public tool_use_id, carries that Tool Use's call ID in
//     its projection, has no call-ID column and no payload tool_use_event_id
//     key, and projects tool_use_id publicly;
//   - an MCP Tool Result does the same through mcp_tool_use_id only;
//   - the invalid-tool repair result carries its payload and projection call
//     ID and references no Tool Use.
//
// It returns the facts keyed by event ID.
func RequireToolRelationFactsForTest(t testing.TB, db *sql.DB, workspaceID string, sessionID string) map[string]ToolRelationFact {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		`SELECT event_id, type, session_thread_id, COALESCE(model_request_id, ''),
		        COALESCE(model_tool_call_id, ''), COALESCE(tool_use_event_id, ''),
		        payload_json, projection_json
		   FROM session_events
		  WHERE workspace_id = $1 AND session_id = $2
		    AND type IN ('agent.tool_use','agent.mcp_tool_use','agent.tool_result','agent.mcp_tool_result')
		  ORDER BY sequence, event_id`,
		workspaceID, sessionID,
	)
	if err != nil {
		t.Fatalf("read Tool relation rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	type row struct {
		fact               ToolRelationFact
		payload            map[string]json.RawMessage
		projectionCallID   string
		payloadJSON        string
		payloadCallID      string
		payloadRepairKind  string
		payloadUseID       string
		payloadMCPUseID    string
		payloadLegacyUseID bool
	}
	var all []row
	for rows.Next() {
		var current row
		var projectionJSON string
		if err := rows.Scan(&current.fact.EventID, &current.fact.EventType, &current.fact.ThreadID, &current.fact.ModelRequestID,
			&current.fact.ModelToolCallID, &current.fact.ToolUseEventID, &current.payloadJSON, &projectionJSON); err != nil {
			t.Fatalf("scan Tool relation row: %v", err)
		}
		if err := json.Unmarshal([]byte(current.payloadJSON), &current.payload); err != nil {
			t.Fatalf("decode Tool event %s payload: %v", current.fact.EventID, err)
		}
		var payload struct {
			ToolUseID       string `json:"tool_use_id"`
			MCPToolUseID    string `json:"mcp_tool_use_id"`
			RepairKind      string `json:"repair_kind"`
			ModelToolCallID string `json:"model_tool_call_id"`
		}
		var projection struct {
			ModelToolCallID string `json:"model_tool_call_id"`
		}
		if json.Unmarshal([]byte(current.payloadJSON), &payload) != nil || json.Unmarshal([]byte(projectionJSON), &projection) != nil {
			t.Fatalf("decode Tool event %s identity", current.fact.EventID)
		}
		_, current.payloadLegacyUseID = current.payload["tool_use_event_id"]
		current.payloadUseID, current.payloadMCPUseID = payload.ToolUseID, payload.MCPToolUseID
		current.payloadRepairKind, current.payloadCallID = payload.RepairKind, payload.ModelToolCallID
		current.projectionCallID = projection.ModelToolCallID
		current.fact.Repair = payload.RepairKind != ""
		all = append(all, current)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read Tool relation rows: %v", err)
	}
	facts := make(map[string]ToolRelationFact, len(all))
	for _, current := range all {
		facts[current.fact.EventID] = current.fact
	}
	for _, current := range all {
		fact := current.fact
		if current.payloadLegacyUseID {
			t.Fatalf("Tool event %s payload carries tool_use_event_id: %s", fact.EventID, current.payloadJSON)
		}
		switch {
		case fact.EventType == "agent.tool_use" || fact.EventType == "agent.mcp_tool_use":
			if fact.ModelToolCallID == "" || fact.ModelToolCallID != current.projectionCallID || fact.ToolUseEventID != "" {
				t.Fatalf("Tool Use %s relation = call %q use %q; projection call %q", fact.EventID, fact.ModelToolCallID, fact.ToolUseEventID, current.projectionCallID)
			}
			continue
		case fact.Repair:
			if fact.EventType != "agent.tool_result" || current.payloadRepairKind != "invalid_tool" || fact.ToolUseEventID != "" ||
				fact.ModelToolCallID == "" || fact.ModelToolCallID != current.payloadCallID || fact.ModelToolCallID != current.projectionCallID {
				t.Fatalf("repair %s relation = call %q use %q; payload call %q projection call %q", fact.EventID, fact.ModelToolCallID, fact.ToolUseEventID, current.payloadCallID, current.projectionCallID)
			}
			continue
		}
		wantUseType, publicKey, payloadUseID, otherKey := "agent.tool_use", "tool_use_id", current.payloadUseID, "mcp_tool_use_id"
		if fact.EventType == "agent.mcp_tool_result" {
			wantUseType, publicKey, payloadUseID, otherKey = "agent.mcp_tool_use", "mcp_tool_use_id", current.payloadMCPUseID, "tool_use_id"
		}
		use, ok := facts[fact.ToolUseEventID]
		if fact.ModelToolCallID != "" || fact.ToolUseEventID == "" || payloadUseID != fact.ToolUseEventID || !ok ||
			use.EventType != wantUseType || use.ThreadID != fact.ThreadID || use.ModelRequestID != fact.ModelRequestID ||
			current.projectionCallID != use.ModelToolCallID {
			t.Fatalf("result %s relation = call %q use %q; payload %s=%q projection call %q use %+v", fact.EventID,
				fact.ModelToolCallID, fact.ToolUseEventID, publicKey, payloadUseID, current.projectionCallID, use)
		}
		if _, exists := current.payload[otherKey]; exists {
			t.Fatalf("result %s payload carries %s: %s", fact.EventID, otherKey, current.payloadJSON)
		}
		public, err := eventwire.MarshalPublicEvent(fact.EventID, fact.EventType, json.RawMessage(current.payloadJSON), nil)
		if err != nil {
			t.Fatalf("project Tool Result %s publicly: %v", fact.EventID, err)
		}
		var projected map[string]any
		if err := json.Unmarshal(public, &projected); err != nil || projected[publicKey] != fact.ToolUseEventID || projected[otherKey] != nil {
			t.Fatalf("public Tool Result %s = %s; want %s=%s only", fact.EventID, public, publicKey, fact.ToolUseEventID)
		}
	}
	return facts
}
