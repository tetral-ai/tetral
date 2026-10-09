package agentruntimebridge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// partsStatementTracer records the SQL a traced Bridge store sends while
// recording is on.
type partsStatementTracer struct {
	mu         sync.Mutex
	recording  bool
	statements []string
}

func (tracer *partsStatementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	if tracer.recording {
		tracer.statements = append(tracer.statements, data.SQL)
	}
	tracer.mu.Unlock()
	return ctx
}

func (*partsStatementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *partsStatementTracer) record(t *testing.T, operation func()) []string {
	t.Helper()
	tracer.mu.Lock()
	tracer.recording, tracer.statements = true, nil
	tracer.mu.Unlock()
	operation()
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.recording = false
	return append([]string(nil), tracer.statements...)
}

func countStatements(statements []string, prefix string) int {
	count := 0
	for _, statement := range statements {
		if strings.HasPrefix(strings.TrimSpace(statement), prefix) {
			count++
		}
	}
	return count
}

// requireAppendReadsNoContent fails when an append reads stored message
// content or rewrites a part: the only statement naming a content column is
// the single parts INSERT.
func requireAppendReadsNoContent(t *testing.T, name string, statements []string) {
	t.Helper()
	inserts := 0
	for _, statement := range statements {
		trimmed := strings.TrimSpace(statement)
		if strings.HasPrefix(trimmed, "INSERT INTO session_message_parts") {
			inserts++
			continue
		}
		touchesMessages := strings.Contains(trimmed, "session_messages") || strings.Contains(trimmed, "session_message_parts")
		if touchesMessages && (strings.Contains(trimmed, "data_json") || strings.Contains(trimmed, "jsonb")) {
			t.Fatalf("%s read or rewrote stored message content: %s", name, trimmed)
		}
		if strings.HasPrefix(trimmed, "UPDATE session_message_parts") || strings.HasPrefix(trimmed, "DELETE FROM session_message_parts") {
			t.Fatalf("%s rewrote a stored part: %s", name, trimmed)
		}
	}
	if inserts != 1 {
		t.Fatalf("%s part INSERT statements = %d; want one multirow INSERT", name, inserts)
	}
}

type storedAssistantHeader struct {
	messageID      string
	sequence       int64
	next           int64
	reasoningParts int64
	reasoningBytes int64
	messages       int
}

type storedAssistantPart struct {
	kind            string
	modelToolCallID string
	dataJSON        string
}

func readStoredAssistant(t *testing.T, f contentDeclarationFixture, scope *bridgev1.RuntimeScope, modelRequestID string) (storedAssistantHeader, map[int64]storedAssistantPart) {
	t.Helper()
	var header storedAssistantHeader
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3),
		message_id, sequence, next_part_index, reasoning_part_count, reasoning_bytes
		FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND model_request_id=$4 AND content_storage='parts'`,
		scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, modelRequestID,
	).Scan(&header.messages, &header.messageID, &header.sequence, &header.next, &header.reasoningParts, &header.reasoningBytes); err != nil {
		t.Fatalf("read assistant header of %s: %v", modelRequestID, err)
	}
	rows, err := f.admin.QueryContext(f.ctx, `SELECT part_index, part_kind, COALESCE(model_tool_call_id, ''), data_json
		FROM session_message_parts WHERE workspace_id=$1 AND message_id=$2`, scope.WorkspaceId, header.messageID)
	if err != nil {
		t.Fatalf("read assistant parts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	parts := map[int64]storedAssistantPart{}
	for rows.Next() {
		var index int64
		var part storedAssistantPart
		if err := rows.Scan(&index, &part.kind, &part.modelToolCallID, &part.dataJSON); err != nil {
			t.Fatal(err)
		}
		parts[index] = part
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return header, parts
}

// loadedMessageParts returns the parts LoadContext reconstructs for the
// message at sequence.
func loadedMessageParts(t *testing.T, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope, sequence int64) []json.RawMessage {
	t.Helper()
	store.RuntimeBindingTokenHMACKey = []byte("message-parts-context-signing-key")
	response, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{Scope: scope})
	if err != nil {
		t.Fatalf("LoadContext: %v", err)
	}
	var loaded bridgeLoadContextPayload
	if err := json.Unmarshal([]byte(response.GetContextJson()), &loaded); err != nil {
		t.Fatalf("decode LoadContext: %v", err)
	}
	for _, message := range loaded.Messages {
		if message.MessageSequence == sequence {
			return message.Parts
		}
	}
	t.Fatalf("LoadContext omitted message %d: %s", sequence, response.GetContextJson())
	return nil
}

// prefixMessageParts returns the parts both prefix readers reconstruct for
// the message at sequence: the declared sub-agent prefix bounded by the
// source Tool Use, and the reviewer-sidecar durable prefix through sequence.
func prefixMessageParts(t *testing.T, store *PostgreSQLBridgeAPIStore, scope *bridgev1.RuntimeScope, sourceToolUseEventID string, sequence int64) ([]json.RawMessage, []json.RawMessage) {
	t.Helper()
	var declared, durable []json.RawMessage
	err := store.Client.WithWorkspaceTx(context.Background(), scope.GetWorkspaceId(), "agentruntimebridge.test_prefix", func(tx *dbconnect.Tx) error {
		envelope, err := loadDeclaredSubagentPrefixTx(context.Background(), tx, scope, sourceToolUseEventID, []int64{sequence})
		if err != nil {
			return err
		}
		if len(envelope.Entries) != 1 {
			t.Fatalf("declared prefix entries = %d; want 1", len(envelope.Entries))
		}
		declared = envelope.Entries[0].Parts
		entries, _, err := loadDurablePrefixEntriesThroughTx(context.Background(), tx, scope, sequence)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.MessageSequence == sequence {
				durable = entry.Parts
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("prefix readers: %v", err)
	}
	if durable == nil {
		t.Fatalf("durable prefix omitted message %d", sequence)
	}
	return declared, durable
}

func partIdentities(t *testing.T, parts []json.RawMessage) []string {
	t.Helper()
	identities := make([]string, 0, len(parts))
	for _, raw := range parts {
		var part struct {
			Type            string `json:"type"`
			Text            string `json:"text"`
			ModelToolCallID string `json:"modelToolCallId"`
		}
		if err := json.Unmarshal(raw, &part); err != nil {
			t.Fatalf("decode part %s: %v", raw, err)
		}
		identity := part.Type + ":" + part.ModelToolCallID
		if part.Type == "reasoning" || part.Type == "text" {
			identity = part.Type + ":" + part.Text
		}
		identities = append(identities, identity)
	}
	return identities
}

func installPartInsertFailure(t *testing.T, f contentDeclarationFixture, messageID string, index int64) func() {
	t.Helper()
	if _, err := f.admin.ExecContext(f.ctx, `CREATE FUNCTION fail_marked_part_insert() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'marked part insert fails' USING ERRCODE = 'P0001'; END; $$ LANGUAGE plpgsql`); err != nil {
		t.Fatalf("install part failure function: %v", err)
	}
	if _, err := f.admin.ExecContext(f.ctx, fmt.Sprintf(`CREATE TRIGGER fail_marked_part_insert BEFORE INSERT ON session_message_parts
		FOR EACH ROW WHEN (NEW.message_id = '%s' AND NEW.part_index = %d)
		EXECUTE FUNCTION fail_marked_part_insert()`, messageID, index)); err != nil {
		t.Fatalf("install part failure trigger: %v", err)
	}
	return func() {
		if _, err := f.admin.ExecContext(f.ctx, `DROP TRIGGER fail_marked_part_insert ON session_message_parts; DROP FUNCTION fail_marked_part_insert()`); err != nil {
			t.Fatalf("remove part failure trigger: %v", err)
		}
	}
}

// Settlement and declaration append parts in commit order without reading or
// rewriting earlier parts, and every context and prefix reader returns that
// order. A rolled-back append leaves its index unused for the next commit,
// and replay appends nothing.
func TestPostgreSQLAssistantPartsAppendInCommitOrder(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	tracer := &partsStatementTracer{}
	traced := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "bridge", tracer)))

	declarationA := sessionfixture.BridgeToolDeclarationForTest("call_parts_a", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute")
	declarationA.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{Text: "r"}}
	toolA := declareToolForRelationTest(t, f, f.scope, "parts-a", "request", declarationA)
	header, parts := readStoredAssistant(t, f, f.scope, "request")
	if header.next != 2 || header.reasoningParts != 1 || header.reasoningBytes != 3 || len(parts) != 2 ||
		parts[0].kind != "reasoning" || parts[1].kind != "tool_call" || parts[1].modelToolCallID != "call_parts_a" {
		t.Fatalf("first delta header/parts = %+v %+v; want reasoning,call A at 0,1 with next=2 count=1 bytes=3", header, parts)
	}
	firstParts := parts

	var toolB string
	statements := tracer.record(t, func() {
		response, err := traced.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
			Scope: f.scope, RuntimeWriteId: "parts-b", ModelRequestId: "request",
			ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest("call_parts_b", "Read", `{"path":"b.txt"}`, "allow", "sandbox_execute"),
		})
		if err != nil || response.GetCommitted() == nil {
			t.Fatalf("declare Tool B = %#v/%v", response, err)
		}
		toolB = response.GetCommitted().GetEventId()
	})
	requireAppendReadsNoContent(t, "Tool Use declaration", statements)
	if inserts, updates := countStatements(statements, "INSERT INTO session_events"), countStatements(statements, "UPDATE session_events"); inserts != 1 || updates != 0 {
		t.Fatalf("Tool Use declaration event INSERT/UPDATE statements = %d/%d; want one complete INSERT and no UPDATE", inserts, updates)
	}

	restore := installPartInsertFailure(t, f, header.messageID, 3)
	if _, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope,
		sessionfixture.BridgeCompletedToolSettlementForTest(toolB, "b done"))); err == nil {
		t.Fatal("settlement with a failing part INSERT committed")
	}
	restore()
	header, parts = readStoredAssistant(t, f, f.scope, "request")
	var results int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_events WHERE workspace_id=$1 AND tool_use_event_id=$2`,
		f.scope.WorkspaceId, toolB).Scan(&results); err != nil {
		t.Fatal(err)
	}
	if header.next != 3 || len(parts) != 3 || results != 0 {
		t.Fatalf("rolled-back settlement left next=%d parts=%d results=%d; want 3/3/0", header.next, len(parts), results)
	}

	settleB := sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolB, "b done"))
	statements = tracer.record(t, func() {
		response, err := traced.SettleToolResult(f.ctx, settleB)
		if err != nil {
			t.Fatalf("settle B: %v", err)
		}
		sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, response, "committed")
	})
	requireAppendReadsNoContent(t, "Tool settlement", statements)
	header, parts = readStoredAssistant(t, f, f.scope, "request")
	if header.next != 4 || parts[3].kind != "tool_result" || parts[3].modelToolCallID != "call_parts_b" {
		t.Fatalf("retried settlement header/part = %+v/%+v; want result B at index 3", header, parts[3])
	}
	replayed, err := f.store.SettleToolResult(f.ctx, proto.Clone(settleB).(*bridgev1.SettleToolResultRequest))
	if err != nil {
		t.Fatalf("replay settlement B: %v", err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, replayed, "duplicate")
	if replayHeader, replayParts := readStoredAssistant(t, f, f.scope, "request"); replayHeader != header || len(replayParts) != 4 {
		t.Fatalf("replay changed header %+v -> %+v or parts %d", header, replayHeader, len(replayParts))
	}

	settledA, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope,
		sessionfixture.BridgeCompletedToolSettlementForTest(toolA, "a done")))
	if err != nil {
		t.Fatalf("settle A: %v", err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settledA, "committed")
	sequence := header.sequence
	ended, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
		Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "stop", UsageJson: `{}`,
		TrailingContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
			{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "z"}}},
		}},
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{toolA, toolB},
		},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("request End with trailing reasoning = %#v/%v", ended, err)
	}
	header, parts = readStoredAssistant(t, f, f.scope, "request")
	if header.next != 6 || header.reasoningParts != 2 || header.reasoningBytes != 6 || header.messages != 1 || header.sequence != sequence || len(parts) != 6 {
		t.Fatalf("final header = %+v with %d parts; want next=6 count=2 bytes=6 in one message", header, len(parts))
	}
	for index, part := range firstParts {
		if parts[index] != part {
			t.Fatalf("part %d changed after later appends: %+v -> %+v", index, part, parts[index])
		}
	}

	seedBridgeAPIRequestStart(t, f.store, f.scope, "start-2", "request-2", runtimecontrol.RequestKindAgentProviderRequest, 1)
	source := declareToolForRelationTest(t, f, f.scope, "parts-source", "request-2",
		sessionfixture.BridgeToolDeclarationForTest("call_parts_source", "Read", `{"path":"c.txt"}`, "allow", "sandbox_execute"))
	want := "reasoning:r,tool_call:call_parts_a,tool_call:call_parts_b,tool_result:call_parts_b,tool_result:call_parts_a,reasoning:z"
	declared, durable := prefixMessageParts(t, f.store, f.scope, source, sequence)
	for name, reconstructed := range map[string][]json.RawMessage{
		"LoadContext":     loadedMessageParts(t, f.store, f.scope, sequence),
		"declared prefix": declared,
		"reviewer prefix": durable,
	} {
		if got := strings.Join(partIdentities(t, reconstructed), ","); got != want {
			t.Fatalf("%s parts = %s; want %s", name, got, want)
		}
	}
}

func reasoningMessageRequest(f contentDeclarationFixture, writeID string, reasoning ...*bridgev1.RuntimeContextReasoning) *bridgev1.WriteEventRequest {
	parts := make([]*bridgev1.RuntimeContextPart, 0, len(reasoning)+1)
	for _, part := range reasoning {
		parts = append(parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: part}})
	}
	parts = append(parts, &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "answer"}}})
	return &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: writeID, ModelRequestId: "request", EventType: "agent.message",
		PayloadJson:           `{"type":"agent.message","content":[{"type":"text","text":"answer"}]}`,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: parts},
		PreallocatedEventId:   bridgeString(fmt.Sprintf("evt_%x", sha256.Sum256([]byte(writeID)))[:36]),
	}
}

// Reasoning is charged once, at admission, against the request's stored
// counters: text bytes plus metadata in the transported JSON.stringify form,
// an absent object counting as {}. Every declaration path applies the same
// cumulative 16-part and 2 MiB budget.
func TestPostgreSQLAssistantReasoningBudgetIsChargedAtAdmission(t *testing.T) {
	t.Run("sixteen-empty-parts-then-repair-prefix", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		f.start(t)
		empty := make([]*bridgev1.RuntimeContextReasoning, 16)
		for index := range empty {
			empty[index] = &bridgev1.RuntimeContextReasoning{}
		}
		if response, err := f.store.WriteEvent(f.ctx, reasoningMessageRequest(f, "sixteen", empty...)); err != nil || response.GetCommitted() == nil {
			t.Fatalf("sixteen empty reasoning parts = %#v/%v", response, err)
		}
		header, _ := readStoredAssistant(t, f, f.scope, "request")
		if header.reasoningParts != 16 || header.reasoningBytes != 32 {
			t.Fatalf("sixteen empty reasoning counters = %d/%d; want 16/32", header.reasoningParts, header.reasoningBytes)
		}
		before := f.snapshot(t)
		repair := contentRepairRequest(f.scope)
		repair.ReasoningPrefixContextDelta.Parts[0].GetReasoning().ProviderMetadataJson = nil
		if _, err := f.store.CommitInternalToolRepair(f.ctx, repair); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("seventeenth reasoning part through repair prefix = %v; want InvalidArgument", err)
		}
		if f.snapshot(t) != before {
			t.Fatal("rejected repair prefix wrote an event, part, counter or receipt")
		}
	})

	metadata := `{"a":"<&` + "  " + `"}`
	decoded, err := runtimecontrol.DecodeRuntimeDeclarationObject(metadata)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := runtimecontrol.MarshalDataJSON(decoded)
	if err != nil {
		t.Fatal(err)
	}
	metadataCharge := len(runtimecontrol.RestoreJSONStringifySeparatorEscapes([]byte(canonical)))
	if htmlEscaped, _ := json.Marshal(decoded); len(htmlEscaped) == metadataCharge {
		t.Fatal("metadata fixture does not distinguish the transported encoding from json.Marshal")
	}
	for _, test := range []struct {
		name     string
		extra    int
		accepted bool
	}{
		{name: "exactly-two-mebibytes", extra: 0, accepted: true},
		{name: "one-byte-over", extra: 1, accepted: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			f.start(t)
			text := strings.Repeat("x", MaxStableReasoningBytesPerRequest-metadataCharge+test.extra)
			response, err := f.store.WriteEvent(f.ctx, reasoningMessageRequest(f, test.name,
				&bridgev1.RuntimeContextReasoning{Text: text, ProviderMetadataJson: bridgeString(metadata)}))
			if !test.accepted {
				if status.Code(err) != codes.InvalidArgument {
					t.Fatalf("reasoning one byte over budget = %#v/%v; want InvalidArgument", response, err)
				}
				var messages int
				if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_messages WHERE workspace_id=$1 AND session_id=$2`,
					f.scope.WorkspaceId, f.scope.SessionId).Scan(&messages); err != nil || messages != 0 {
					t.Fatalf("rejected reasoning left messages=%d err=%v", messages, err)
				}
				return
			}
			if err != nil || response.GetCommitted() == nil {
				t.Fatalf("reasoning at exactly the budget = %#v/%v", response, err)
			}
			header, _ := readStoredAssistant(t, f, f.scope, "request")
			if header.reasoningParts != 1 || header.reasoningBytes != MaxStableReasoningBytesPerRequest {
				t.Fatalf("budget counters = %d/%d; want 1/%d", header.reasoningParts, header.reasoningBytes, MaxStableReasoningBytesPerRequest)
			}
		})
	}

	t.Run("metadata-presence", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		f.start(t)
		if response, err := f.store.WriteEvent(f.ctx, reasoningMessageRequest(f, "presence",
			&bridgev1.RuntimeContextReasoning{Text: "x"},
			&bridgev1.RuntimeContextReasoning{Text: "y", ProviderMetadataJson: bridgeString(`{}`)},
		)); err != nil || response.GetCommitted() == nil {
			t.Fatalf("absent and empty metadata = %#v/%v", response, err)
		}
		header, parts := readStoredAssistant(t, f, f.scope, "request")
		if header.reasoningParts != 2 || header.reasoningBytes != 6 ||
			strings.Contains(parts[0].dataJSON, "providerMetadata") || !strings.Contains(parts[1].dataJSON, `"providerMetadata":{}`) {
			t.Fatalf("metadata presence header=%+v parts=%s / %s", header, parts[0].dataJSON, parts[1].dataJSON)
		}
		if _, err := f.store.WriteEvent(f.ctx, reasoningMessageRequest(f, "null-metadata",
			&bridgev1.RuntimeContextReasoning{Text: "z", ProviderMetadataJson: bridgeString(`null`)},
		)); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("null reasoning metadata = %v; want InvalidArgument", err)
		}
	})
}

// A parts-mode message whose rows are not exactly 0..next-1 is malformed
// durable context for LoadContext and both prefix readers; no reader
// substitutes an empty or embedded document.
func TestPostgreSQLNoncontiguousAssistantPartsFailContextReads(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	if response, err := f.store.WriteEvent(f.ctx, reasoningMessageRequest(f, "three-parts",
		&bridgev1.RuntimeContextReasoning{Text: "first"}, &bridgev1.RuntimeContextReasoning{Text: "second"},
	)); err != nil || response.GetCommitted() == nil {
		t.Fatalf("three-part message = %#v/%v", response, err)
	}
	header, _ := readStoredAssistant(t, f, f.scope, "request")
	sequence := header.sequence
	f.end(t, &sequence)
	seedBridgeAPIRequestStart(t, f.store, f.scope, "start-2", "request-2", runtimecontrol.RequestKindAgentProviderRequest, 1)
	source := declareToolForRelationTest(t, f, f.scope, "gap-source", "request-2",
		sessionfixture.BridgeToolDeclarationForTest("call_gap_source", "Read", `{"path":"c.txt"}`, "allow", "sandbox_execute"))
	if _, err := f.admin.ExecContext(f.ctx, `DELETE FROM session_message_parts WHERE workspace_id=$1 AND message_id=$2 AND part_index=1`,
		f.scope.WorkspaceId, header.messageID); err != nil {
		t.Fatalf("remove middle part: %v", err)
	}
	f.store.RuntimeBindingTokenHMACKey = []byte("message-parts-context-signing-key")
	if _, err := f.store.LoadContext(f.ctx, &bridgev1.LoadContextRequest{Scope: f.scope}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("LoadContext over parts 0,2 of 3 = %v; want FailedPrecondition", err)
	}
	err := f.store.Client.WithWorkspaceTx(f.ctx, f.scope.WorkspaceId, "agentruntimebridge.test_prefix", func(tx *dbconnect.Tx) error {
		if _, err := loadDeclaredSubagentPrefixTx(f.ctx, tx, f.scope, source, []int64{sequence}); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("declared prefix over parts 0,2 of 3 = %v; want FailedPrecondition", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = f.store.Client.WithWorkspaceTx(f.ctx, f.scope.WorkspaceId, "agentruntimebridge.test_prefix", func(tx *dbconnect.Tx) error {
		if _, _, err := loadDurablePrefixEntriesThroughTx(f.ctx, tx, f.scope, sequence); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("durable prefix over parts 0,2 of 3 = %v; want FailedPrecondition", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// requireToken fails unless raw contains token as a complete JSON token.
func requireToken(t *testing.T, name string, raw string, token string) {
	t.Helper()
	if !strings.Contains(raw, token) {
		t.Fatalf("%s lost token %s: %s", name, token, raw)
	}
}

// Parts are stored once as admitted and returned unchanged. Escaped U+0000 in
// assistant text and in completed and error Tool output, and numeric tokens
// that JSONB or float64 would rewrite, survive settlement, LoadContext, both
// prefix readers and a frozen child prefix; reasoning counters stay at their
// admitted charge; and later parent settlement does not change the child's
// stored prefix.
func TestPostgreSQLRawAssistantPartsSurviveSettlementAndReads(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	const nulText = "before\u0000after"
	message := &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: "nul-text", ModelRequestId: "request", EventType: "agent.message",
		PreallocatedEventId: bridgeString("evt_00000000000000000000000000000001"),
		PayloadJson:         `{"type":"agent.message","content":[{"type":"text","text":"before\u0000after"}]}`,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
			{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: nulText}}},
		}},
	}
	if response, err := f.store.WriteEvent(f.ctx, message); err != nil || response.GetCommitted() == nil {
		t.Fatalf("assistant text with escaped U+0000 = %#v/%v", response, err)
	}
	const numericInput = `{"n":9007199254740993,"z":-0,"e":1e+21}`
	completed := sessionfixture.BridgeToolDeclarationForTest("call_raw_completed", "Read", numericInput, "allow", "sandbox_execute")
	completed.LeadingReasoning = []*bridgev1.RuntimeContextReasoning{{Text: "why", ProviderMetadataJson: bridgeString(`{"k":"<v>"}`)}}
	completedUse := declareToolForRelationTest(t, f, f.scope, "raw-completed", "request", completed)
	failedUse := declareToolForRelationTest(t, f, f.scope, "raw-failed", "request",
		sessionfixture.BridgeToolDeclarationForTest("call_raw_failed", "Read", `{"path":"x"}`, "allow", "sandbox_execute"))
	header, _ := readStoredAssistant(t, f, f.scope, "request")
	admittedCharge := int64(len("why") + len(`{"k":"<v>"}`))
	sequence := header.sequence
	ended, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
		Scope: f.scope, RuntimeWriteId: "end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{
			Disposition: "completed", AssistantMessageSequence: &sequence, ToolUseEventIds: []string{completedUse, failedUse},
		},
	})
	if err != nil || ended.GetCommitted() == nil {
		t.Fatalf("request End = %#v/%v", ended, err)
	}

	seedBridgeAPIRequestStart(t, f.store, f.scope, "start-2", "request-2", runtimecontrol.RequestKindAgentProviderRequest, 1)
	spawn := declareToolForRelationTest(t, f, f.scope, "raw-spawn", "request-2",
		sessionfixture.BridgeToolDeclarationWithRouteForTest("call_raw_spawn", "spawn_agent", `{"task_name":"raw-child","agent_type":"worker","fork_turns":"all"}`, "allow"))
	spawned, err := f.store.CreateSubagentThread(f.ctx, &bridgev1.CreateSubagentThreadRequest{
		Scope: f.scope, SourceToolUseEventId: spawn, TaskName: "raw-child", AgentType: "worker",
		InitialPrompt: "inspect the frozen prefix", ParentMessageSequences: []int64{sequence},
	})
	if err != nil || spawned.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("create child over the raw message = %#v/%v", spawned, err)
	}
	childID := spawned.GetCommitted().GetChildThreadId()
	readPrefix := func() string {
		var entries string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT entries_json FROM session_thread_context_prefixes
			WHERE workspace_id=$1 AND session_id=$2 AND child_thread_id=$3`, f.scope.WorkspaceId, f.scope.SessionId, childID).Scan(&entries); err != nil {
			t.Fatalf("read frozen child prefix: %v", err)
		}
		return entries
	}
	frozen := readPrefix()

	for _, settlement := range []*bridgev1.RuntimeToolSettlement{
		{ToolUseEventId: completedUse, Outcome: &bridgev1.RuntimeToolSettlement_Completed{Completed: &bridgev1.RuntimeToolCompleted{
			OutputJson: `{"text":"out\u0000put","truncated":false}`,
		}}},
		{ToolUseEventId: failedUse, Outcome: &bridgev1.RuntimeToolSettlement_Error{Error: &bridgev1.RuntimeToolError{
			ErrorJson: `{"type":"tool_error","message":"bad\u0000thing"}`,
		}}},
	} {
		response, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, settlement))
		if err != nil {
			t.Fatalf("settle %s with escaped U+0000: %v", settlement.GetToolUseEventId(), err)
		}
		sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, response, "committed")
	}
	header, _ = readStoredAssistant(t, f, f.scope, "request")
	if header.next != 6 || header.reasoningParts != 1 || header.reasoningBytes != admittedCharge {
		t.Fatalf("settled header = %+v; want six parts and the admitted reasoning charge %d", header, admittedCharge)
	}
	if after := readPrefix(); after != frozen {
		t.Fatalf("parent settlement changed the frozen child prefix:\n%s\n%s", frozen, after)
	}
	var childParts, childIdentities int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_message_parts WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3
		   AND (model_tool_call_id IS NOT NULL OR tool_use_event_id IS NOT NULL))`,
		f.scope.WorkspaceId, f.scope.SessionId, childID).Scan(&childParts, &childIdentities); err != nil || childParts != 0 || childIdentities != 0 {
		t.Fatalf("child prefix created parts=%d Tool identities=%d err=%v; want none", childParts, childIdentities, err)
	}

	declared, durable := prefixMessageParts(t, f.store, f.scope, spawn, sequence)
	var frozenEntries []bridgeRuntimeContextEntry
	if err := json.Unmarshal([]byte(frozen), &frozenEntries); err != nil || len(frozenEntries) != 1 {
		t.Fatalf("decode frozen prefix %s: %v", frozen, err)
	}
	for name, parts := range map[string][]json.RawMessage{
		"LoadContext":     loadedMessageParts(t, f.store, f.scope, sequence),
		"declared prefix": declared,
		"reviewer prefix": durable,
		"frozen prefix":   frozenEntries[0].Parts,
	} {
		joined := "[" + joinRaw(parts) + "]"
		requireToken(t, name, joined, `"n":9007199254740993`)
		requireToken(t, name, joined, `"z":-0`)
		requireToken(t, name, joined, `"e":1e+21`)
		var decoded []map[string]any
		if err := json.Unmarshal([]byte(joined), &decoded); err != nil {
			t.Fatalf("decode %s parts: %v", name, err)
		}
		if text, _ := decoded[0]["text"].(string); text != nulText {
			t.Fatalf("%s assistant text = %q; want %q", name, text, nulText)
		}
		if name == "frozen prefix" {
			continue
		}
		outputs := map[string]string{}
		for _, part := range decoded {
			if part["type"] != "tool_result" {
				continue
			}
			result, _ := part["result"].(map[string]any)
			callID, _ := part["modelToolCallId"].(string)
			if output, ok := result["output"].(map[string]any); ok {
				outputs[callID], _ = output["text"].(string)
			}
			if failure, ok := result["error"].(map[string]any); ok {
				outputs[callID], _ = failure["message"].(string)
			}
		}
		if outputs["call_raw_completed"] != "out\u0000put" || outputs["call_raw_failed"] != "bad\u0000thing" {
			t.Fatalf("%s Tool outputs = %q; want both escaped U+0000 values", name, outputs)
		}
	}
}

func joinRaw(parts []json.RawMessage) string {
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, string(part))
	}
	return strings.Join(values, ",")
}
