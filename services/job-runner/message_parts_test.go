package jobrunner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
)

type retainStatementTracer struct {
	mu         sync.Mutex
	statements []string
}

func (tracer *retainStatementTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	tracer.statements = append(tracer.statements, strings.TrimSpace(data.SQL))
	tracer.mu.Unlock()
	return ctx
}

func (*retainStatementTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *retainStatementTracer) take() []string {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	statements := tracer.statements
	tracer.statements = nil
	return statements
}

func countRetainStatements(statements []string, prefix string) int {
	count := 0
	for _, statement := range statements {
		if strings.HasPrefix(statement, prefix) {
			count++
		}
	}
	return count
}

type retainedPart struct {
	kind            string
	modelToolCallID string
	dataJSON        string
}

func readRetainedParts(t *testing.T, admin *sql.DB, messageID string) (int64, int64, int64, []retainedPart) {
	t.Helper()
	var next, reasoningParts, reasoningBytes int64
	if err := admin.QueryRowContext(context.Background(), `SELECT next_part_index, reasoning_part_count, reasoning_bytes
		FROM session_messages WHERE workspace_id='default' AND message_id=$1`, messageID).Scan(&next, &reasoningParts, &reasoningBytes); err != nil {
		t.Fatalf("read retained header: %v", err)
	}
	rows, err := admin.QueryContext(context.Background(), `SELECT part_kind, COALESCE(model_tool_call_id,''), data_json
		FROM session_message_parts WHERE workspace_id='default' AND message_id=$1 ORDER BY part_index`, messageID)
	if err != nil {
		t.Fatalf("read retained parts: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var parts []retainedPart
	for rows.Next() {
		var part retainedPart
		if err := rows.Scan(&part.kind, &part.modelToolCallID, &part.dataJSON); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return next, reasoningParts, reasoningBytes, parts
}

// Pod-loss repair appends only the Tool facts that immutable Tool events prove
// and the Assistant message lacks, in event order, selecting candidates by
// scalar identity and continuing across bounded pages. It never reads, decodes
// or rewrites an existing part, leaves the admitted reasoning counters alone,
// keeps numeric input tokens, and a repeated repair writes nothing.
func TestPostgreSQLPodLossRetainAppendsOnlyMissingToolFacts(t *testing.T) {
	ctx := context.Background()
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	tracer := &retainStatementTracer{}
	runner := dbconnect.NewClientForTesting(storagetest.OpenWorkloadDB(t, admin, "job_runner").OpenWorkload(t, "job_runner", tracer))
	const (
		sessionID = "sesn_retain_parts"
		threadID  = "thr_retain_parts"
		request   = "mreq_retain_parts"
		messageID = "msg_retain_parts"
	)
	sessionfixture.SeedBridgeAPISession(t, admin, "default", sessionID, threadID)
	pagedPairs := (runtimePodLostRetainPageSize - 4) / 2
	repairSequence := int64(8 + 2*pagedPairs)
	use := func(callID, input string) string {
		return `{"event_type":"agent.tool_use","evaluated_permission":"allow","model_tool_call_id":"` + callID +
			`","tool_name":"Read","provider_input":` + input + `,"canonical_execution_input":` + input + `,"route_capability":"sandbox_execute","state":"running"}`
	}
	errorResult := func(callID, message string) string {
		return `{"model_tool_call_id":"` + callID + `","tool_name":"Read","provider_input":{},"canonical_execution_input":{},"state":"error","error":{"type":"tool_error","message":"` + message + `","retryable":false}}`
	}
	const numericInput = `{"n":9007199254740993,"z":-0,"e":1e+21}`
	if _, err := admin.ExecContext(ctx, `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		model_request_id, model_tool_call_id, tool_use_event_id, projection_json, created_at, updated_at
	) VALUES
	('default',$1,$2,'evt_retain_a',2,'agent.tool_use','{"type":"agent.tool_use","name":"Read","input":{"path":"a"}}',$3,'call_retain_a',NULL,$4,now(),now()),
	('default',$1,$2,'evt_retain_a_result',3,'agent.tool_result','{"type":"agent.tool_result","tool_use_id":"evt_retain_a"}',$3,NULL,'evt_retain_a',$5,now(),now()),
	('default',$1,$2,'evt_retain_c',4,'agent.tool_use','{"type":"agent.tool_use","name":"Read","input":{"path":"c"}}',$3,'call_retain_c',NULL,$6,now(),now()),
	('default',$1,$2,'evt_retain_b',5,'agent.tool_use','{"type":"agent.tool_use","name":"Read","input":{}}',$3,'call_retain_b',NULL,$7,now(),now()),
	('default',$1,$2,'evt_retain_c_result',6,'agent.tool_result','{"type":"agent.tool_result","tool_use_id":"evt_retain_c"}',$3,NULL,'evt_retain_c',$8,now(),now()),
	('default',$1,$2,'evt_retain_b_result',7,'agent.tool_result','{"type":"agent.tool_result","tool_use_id":"evt_retain_b"}',$3,NULL,'evt_retain_b',$9,now(),now()),
	('default',$1,$2,'evt_retain_repair',$11,'agent.tool_result','{"type":"agent.tool_result","repair_kind":"invalid_tool"}',$3,'call_retain_repair',NULL,$10,now(),now())`,
		sessionID, threadID, request,
		use("call_retain_a", `{"path":"a"}`),
		errorResult("call_retain_a", "a failed"),
		use("call_retain_c", `{"path":"c"}`),
		use("call_retain_b", numericInput),
		errorResult("call_retain_c", "c failed"),
		`{"model_tool_call_id":"call_retain_b","tool_name":"Read","provider_input":{},"canonical_execution_input":{},"state":"completed","output":{"text":"b done","truncated":false}}`,
		`{"model_tool_call_id":"call_retain_repair","tool_name":"unknown","provider_input":{"q":"x"},"canonical_execution_input":{"q":"x"},"state":"error","error":{"type":"provider_tool_protocol_error","message":"invalid tool","retryable":false}}`,
		repairSequence,
	); err != nil {
		t.Fatalf("seed immutable Tool events: %v", err)
	}
	// Answered Tool Uses between call B's result and the synthetic repair
	// make the repair call the last missing fact of the first page and its
	// result the first fact of the second page.
	want := []string{"tool_call:call_retain_b", "tool_result:call_retain_c", "tool_result:call_retain_b"}
	for pair := range pagedPairs {
		callID := fmt.Sprintf("call_retain_page_%03d", pair)
		useEventID := fmt.Sprintf("evt_retain_page_%03d", pair)
		if _, err := admin.ExecContext(ctx, `INSERT INTO session_events (
			workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
			model_request_id, model_tool_call_id, tool_use_event_id, projection_json, created_at, updated_at
		) VALUES
		('default',$1,$2,$3,$4,'agent.tool_use','{"type":"agent.tool_use","name":"Read","input":{}}',$5,$6,NULL,$7,now(),now()),
		('default',$1,$2,$8,$9,'agent.tool_result',$10,$5,NULL,$3,$11,now(),now())`,
			sessionID, threadID, useEventID, int64(8+2*pair), request, callID, use(callID, `{}`),
			useEventID+"_result", int64(9+2*pair), `{"type":"agent.tool_result","tool_use_id":"`+useEventID+`"}`,
			errorResult(callID, "page failed"),
		); err != nil {
			t.Fatalf("seed paged Tool pair %d: %v", pair, err)
		}
		want = append(want, "tool_call:"+callID, "tool_result:"+callID)
	}
	want = append(want, "tool_call:call_retain_repair", "tool_result:call_retain_repair")
	if len(want) != runtimePodLostRetainPageSize+1 || want[runtimePodLostRetainPageSize-1] != "tool_call:call_retain_repair" {
		t.Fatalf("fixture does not split the repair pair at the page boundary: %d missing facts", len(want))
	}
	existing := []string{
		`{"type":"text","text":"committed text"}`,
		`{"type":"reasoning","text":"r"}`,
		`{"type":"tool_call","modelToolCallId":"call_retain_a","toolName":"Read","canonicalInput":{"path":"a"}}`,
		`{"type":"tool_result","modelToolCallId":"call_retain_a","result":{"type":"error","error":{"type":"tool_error","message":"a failed","retryable":false}}}`,
		`{"type":"tool_call","modelToolCallId":"call_retain_c","toolName":"Read","canonicalInput":{"path":"c"}}`,
	}
	sessionfixture.SeedAssistantMessagePartsForTest(t, admin, "default", sessionID, threadID, messageID, 1, nil, request, existing...)
	retain := func(modelRequestID string) {
		t.Helper()
		if err := runner.WithWorkspaceTx(ctx, "default", "test.retain_parts", func(tx *dbconnect.Tx) error {
			return retainRuntimePodLostToolPairsTx(ctx, tx, "default", sessionID,
				runtimecontrol.OpenRequestStart{SessionThreadID: threadID, ModelRequestID: modelRequestID}, time.Now().UTC())
		}); err != nil {
			t.Fatalf("retain %s: %v", modelRequestID, err)
		}
	}

	tracer.take()
	retain(request)
	statements := tracer.take()
	if inserts := countRetainStatements(statements, "INSERT INTO session_message_parts"); inserts != 2 {
		t.Fatalf("first repair part INSERT statements = %d; want one per bounded page, two", inserts)
	}
	for _, statement := range statements {
		if !strings.HasPrefix(statement, "INSERT INTO session_message_parts") && strings.Contains(statement, "data_json") {
			t.Fatalf("repair read stored message content: %s", statement)
		}
	}
	next, reasoningParts, reasoningBytes, parts := readRetainedParts(t, admin, messageID)
	wantNext := int64(len(existing) + len(want))
	if next != wantNext || int64(len(parts)) != wantNext || reasoningParts != 1 || reasoningBytes != 3 {
		t.Fatalf("repaired header next=%d parts=%d reasoning=%d/%d; want %d/%d/1/3", next, len(parts), reasoningParts, reasoningBytes, wantNext, wantNext)
	}
	for index, raw := range existing {
		if parts[index].dataJSON != raw {
			t.Fatalf("existing part %d rewritten: %s -> %s", index, raw, parts[index].dataJSON)
		}
	}
	var appended []string
	for _, part := range parts[len(existing):] {
		appended = append(appended, part.kind+":"+part.modelToolCallID)
	}
	if got, wantAppended := strings.Join(appended, ","), strings.Join(want, ","); got != wantAppended {
		t.Fatalf("repair appended %s; want only the missing facts in event order %s", got, wantAppended)
	}
	for _, token := range []string{`"n":9007199254740993`, `"z":-0`, `"e":1e+21`} {
		if !strings.Contains(parts[len(existing)].dataJSON, token) {
			t.Fatalf("repaired call lost numeric token %s: %s", token, parts[len(existing)].dataJSON)
		}
	}
	var content string
	if err := admin.QueryRowContext(ctx, `SELECT `+sessionfixture.MessageContentSQL+` FROM session_messages m WHERE workspace_id='default' AND message_id=$1`, messageID).Scan(&content); err != nil {
		t.Fatal(err)
	}
	if _, err := runtimecontrol.DecodeStoredRuntimeContextParts(content); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("context read of the repaired {text,truncated} completion = %v; want the existing FailedPrecondition", err)
	}

	retain(request)
	statements = tracer.take()
	if inserts, updates := countRetainStatements(statements, "INSERT INTO session_message_parts"), countRetainStatements(statements, "UPDATE session_messages"); inserts != 0 || updates != 0 {
		t.Fatalf("repeated repair part INSERT/header UPDATE = %d/%d; want 0/0", inserts, updates)
	}
	if again, _, _, _ := readRetainedParts(t, admin, messageID); again != next {
		t.Fatalf("repeated repair moved next %d -> %d", next, again)
	}

	retain("mreq_retain_absent")
	var absent int
	if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM session_messages WHERE workspace_id='default' AND model_request_id='mreq_retain_absent'`).Scan(&absent); err != nil || absent != 0 {
		t.Fatalf("repair without an Assistant message created %d messages/%v", absent, err)
	}
}
