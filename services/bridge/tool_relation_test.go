package agentruntimebridge

import (
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/runtimecontrol"
	"github.com/tetral-ai/tetral/internal/storage/storagetest/sessionfixture"
	"github.com/tetral-ai/tetral/internal/workspace"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

// declareToolForRelationTest commits one Tool declaration in the fixture's
// open request through the production WriteEvent path.
func declareToolForRelationTest(t *testing.T, f contentDeclarationFixture, scope *bridgev1.RuntimeScope, writeID string, modelRequestID string, declaration *bridgev1.RuntimeToolDeclaration) string {
	t.Helper()
	response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
		Scope: scope, RuntimeWriteId: writeID, ModelRequestId: modelRequestID, ToolDeclaration: declaration,
	})
	if err != nil || response.GetCommitted() == nil {
		t.Fatalf("declare Tool %s = %#v/%v; want committed", writeID, response, err)
	}
	return response.GetCommitted().GetEventId()
}

func relationResultFor(t *testing.T, facts map[string]sessionfixture.ToolRelationFact, toolUseEventID string) sessionfixture.ToolRelationFact {
	t.Helper()
	var found []sessionfixture.ToolRelationFact
	for _, fact := range facts {
		if fact.ToolUseEventID == toolUseEventID {
			found = append(found, fact)
		}
	}
	if len(found) != 1 {
		t.Fatalf("results referencing Tool Use %s = %+v; want exactly one", toolUseEventID, found)
	}
	return found[0]
}

func relationRuntimeWriteID(t *testing.T, f contentDeclarationFixture, eventID string) string {
	t.Helper()
	var writeID sql.NullString
	if err := f.admin.QueryRowContext(f.ctx, `SELECT runtime_write_id FROM session_events WHERE event_id=$1`, eventID).Scan(&writeID); err != nil {
		t.Fatalf("read result %s writer identity: %v", eventID, err)
	}
	return writeID.String
}

// Every Bridge Tool event writer derives its relation columns, payload and
// projection from one in-memory fact, under the real Bridge role. The
// declaration writer inserts the complete projection once: a test trigger on
// the marked Tool Use rows rejects any later rewrite of the row.
func TestPostgreSQLBridgeToolRelationWritersDeriveOneFact(t *testing.T) {
	t.Run("declaration settlement and repair", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		f.start(t)
		if _, err := f.admin.ExecContext(f.ctx, `CREATE FUNCTION reject_tool_use_rewrite() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'Tool Use row rewritten after its declaration INSERT'; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER reject_tool_use_rewrite BEFORE UPDATE ON session_events
			FOR EACH ROW WHEN (OLD.model_tool_call_id LIKE 'call_relation_%' AND (NEW.projection_json IS DISTINCT FROM OLD.projection_json
				OR NEW.payload_json IS DISTINCT FROM OLD.payload_json OR NEW.model_tool_call_id IS DISTINCT FROM OLD.model_tool_call_id))
			EXECUTE FUNCTION reject_tool_use_rewrite()`); err != nil {
			t.Fatalf("install Tool Use rewrite guard: %v", err)
		}
		toolUse := declareToolForRelationTest(t, f, f.scope, "relation-tool", "request",
			sessionfixture.BridgeToolDeclarationForTest("call_relation_tool", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
		mcpUse := declareToolForRelationTest(t, f, f.scope, "relation-mcp", "request",
			bridgeMCPToolDeclarationForTest("call_relation_mcp", "search", "github", `{"q":"x"}`, "allow"))
		repairRequest := contentRepairRequest(f.scope)
		repairRequest.ModelToolCallId = "call_relation_repair"
		repairRequest.RepairKey = internalToolRepairKey("request", "call_relation_repair", "unknown")
		repaired, err := f.store.CommitInternalToolRepair(f.ctx, repairRequest)
		if err != nil || repaired.GetCommitted() == nil {
			t.Fatalf("invalid-tool repair = %#v/%v; want committed", repaired, err)
		}
		for _, settlement := range []*bridgev1.RuntimeToolSettlement{
			sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "read"),
			sessionfixture.BridgeErrorToolSettlementForTest(mcpUse, "connector failed"),
		} {
			response, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, settlement))
			if err != nil {
				t.Fatalf("settle %s: %v", settlement.GetToolUseEventId(), err)
			}
			sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, response, "committed")
		}
		facts := sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
		if facts[toolUse].ModelToolCallID != "call_relation_tool" || facts[mcpUse].ModelToolCallID != "call_relation_mcp" {
			t.Fatalf("declared call IDs = %+v / %+v", facts[toolUse], facts[mcpUse])
		}
		if result := relationResultFor(t, facts, toolUse); result.EventType != "agent.tool_result" {
			t.Fatalf("ordinary result = %+v", result)
		}
		if result := relationResultFor(t, facts, mcpUse); result.EventType != "agent.mcp_tool_result" {
			t.Fatalf("MCP result = %+v", result)
		}
		repair := facts[repaired.GetCommitted().GetRepairEventId()]
		if !repair.Repair || repair.ModelToolCallID != "call_relation_repair" || repair.ToolUseEventID != "" || len(facts) != 5 {
			t.Fatalf("repair fact = %+v among %d Tool events", repair, len(facts))
		}
		var projection map[string]json.RawMessage
		var projectionJSON string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT projection_json FROM session_events WHERE event_id=$1`, mcpUse).Scan(&projectionJSON); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(projectionJSON), &projection); err != nil {
			t.Fatal(err)
		}
		keys := make([]string, 0, len(projection))
		for key := range projection {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		if strings.Join(keys, ",") != "canonical_execution_input,evaluated_permission,event_type,mcp_server_name,model_tool_call_id,provider_input,route_capability,tool_name" ||
			string(projection["mcp_server_name"]) != `"github"` {
			t.Fatalf("MCP Tool Use projection = %s; want the complete declaration projection", projectionJSON)
		}
	})

	t.Run("interrupt", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		seedBridgeAPIOpenDurableTurn(t, f.admin, f.scope, "evt_relation_interrupt_run")
		f.start(t)
		toolUse := declareToolForRelationTest(t, f, f.scope, "relation-interrupt-tool", "request",
			sessionfixture.BridgeToolDeclarationForTest("call_relation_interrupt", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
		sequence := sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId)
		seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "evt_relation_interrupt", sequence, "user.interrupt", `{}`)
		sessionfixture.SeedBridgeAPIRuntimeInbox(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "rin_relation_interrupt",
			"interrupt_control", `["evt_relation_interrupt"]`, "accepted", f.scope.Binding.BindingId, f.scope.Binding.TargetPodUid, sequence, sequence)
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "queue", nil)))
		enqueueInterruptExhaustionJob(t, queueStore, f.scope.SessionId, f.scope.SessionThreadId, "rin_relation_interrupt", "interrupt_control",
			"evt_relation_interrupt", sequence, queue.DefaultMaxAttempts, time.Now().UTC())
		lease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
			WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "relation-interrupt",
			MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
		})
		ended, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
			Scope: f.scope, RuntimeWriteId: "relation-interrupt-end", ModelRequestId: "request",
			FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
			ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "interrupted", ToolUseEventIds: []string{toolUse}},
			InterruptSettlement:      &bridgev1.RequestEndInterruptSettlement{RuntimeInputId: "rin_relation_interrupt", InterruptLeaseRef: sessionfixture.BridgeInterruptLeaseRef(lease)},
		})
		if err != nil || ended.GetCommitted() == nil || len(ended.GetCommitted().GetInterruptToolResults()) != 1 {
			t.Fatalf("interrupt request end = %#v/%v; want one interrupt Tool result", ended, err)
		}
		facts := sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
		result := relationResultFor(t, facts, toolUse)
		if relationRuntimeWriteID(t, f, result.EventID) != runtimecontrol.StableRuntimeID("interrupt_tool_result", "evt_relation_interrupt", toolUse) {
			t.Fatalf("interrupt result %+v was not written by the interrupt writer", result)
		}
	})

	t.Run("runtime termination", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		const childID = "thr_relation_termination_child"
		sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID)
		childScope := runtimecontrol.ScopeForThread(f.scope, childID)
		seedBridgeAPIOpenDurableTurn(t, f.admin, childScope, "evt_relation_child_run")
		seedBridgeAPIRequestStart(t, f.store, childScope, "relation-child-start", "child-request", runtimecontrol.RequestKindAgentProviderRequest, 0)
		childUse := declareToolForRelationTest(t, f, childScope, "relation-child-tool", "child-request",
			sessionfixture.BridgeToolDeclarationForTest("call_relation_child", "Read", `{"path":"child.txt"}`, "allow", "sandbox_execute"))
		seedBridgeAPIOpenDurableTurn(t, f.admin, f.scope, "relation-termination")
		if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_runtime_status (
			workspace_id,session_id,status,running_since,active_seconds_total,binding_id,binding_generation,created_at,updated_at
		) VALUES ($1,$2,'running',now(),0,$3,$4,now(),now())`, f.scope.WorkspaceId, f.scope.SessionId,
			f.scope.Binding.BindingId, f.scope.Binding.BindingGeneration); err != nil {
			t.Fatalf("seed running Runtime residency: %v", err)
		}
		f.start(t)
		mainUse := declareToolForRelationTest(t, f, f.scope, "relation-main-tool", "request",
			sessionfixture.BridgeToolDeclarationForTest("call_relation_main", "Read", `{"path":"main.txt"}`, "allow", "sandbox_execute"))
		if _, err := f.store.CommitRuntimeTermination(f.ctx, &bridgev1.CommitRuntimeTerminationRequest{
			Scope: f.scope, RuntimeWriteId: "relation-termination",
			FailureJson: `{"type":"provider","code":"provider_invalid_request","message":"Main failed.","retryable":false,"fatal":true,"retryStatus":{"type":"terminal"}}`,
		}); err != nil {
			t.Fatalf("CommitRuntimeTermination: %v", err)
		}
		facts := sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
		mainResult := relationResultFor(t, facts, mainUse)
		if relationRuntimeWriteID(t, f, mainResult.EventID) != runtimecontrol.StableRuntimeID("runtime_termination_tool_result", "relation-termination", mainUse) {
			t.Fatalf("main result %+v was not written by the Thread termination writer", mainResult)
		}
		childResult := relationResultFor(t, facts, childUse)
		if childResult.ThreadID != childID || relationRuntimeWriteID(t, f, childResult.EventID) != "rwrite_session_termination_tool_relation-termination_"+childUse {
			t.Fatalf("child result %+v was not written by the shared terminal writer", childResult)
		}
	})
}

// A Tool Use whose input or MCP server name PostgreSQL JSONB cannot store is
// rejected with InvalidArgument and writes nothing: escaped U+0000, an
// execution-input-only unpaired surrogate and a number below PostgreSQL's
// numeric range reach the storability CHECK; an unpaired surrogate in the
// provider input is already rejected by declaration validation.
// Message content does not affect declaration: assistant text and a Tool
// Result carrying escaped U+0000 commit, and a later Tool Use in the same
// Thread still commits because no declaration reads message history.
func TestPostgreSQLWriteEventToolUseStorabilityAndMessageIndependence(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	nul := string(rune(0))
	text, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: "nul-text", ModelRequestId: "request", EventType: "agent.message",
		PreallocatedEventId: bridgeString("evt_00000000000000000000000000000001"),
		PayloadJson:         `{"type":"agent.message","content":[{"type":"text","text":"a\u0000b"}]}`,
		AssistantContextDelta: &bridgev1.RuntimeContextDelta{Parts: []*bridgev1.RuntimeContextPart{
			{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: "a" + nul + "b"}}},
		}},
	})
	if err != nil || text.GetCommitted() == nil {
		t.Fatalf("assistant text with escaped U+0000 = %#v/%v; want committed", text, err)
	}
	distinct := sessionfixture.BridgeToolDeclarationForTest("call_storable_distinct", "Read", `{"q":"\ud800"}`, "allow", "sandbox_execute")
	distinct.DistinctProviderInputJson = bridgeString(`{"q":"provider"}`)
	for _, tc := range []struct {
		name        string
		declaration *bridgev1.RuntimeToolDeclaration
	}{
		{"ordinary input U+0000", sessionfixture.BridgeToolDeclarationForTest("call_storable_nul", "Read", `{"q":"a\u0000b"}`, "allow", "sandbox_execute")},
		{"execution input only", distinct},
		{"MCP server name U+0000", bridgeMCPToolDeclarationForTest("call_storable_server", "search", "git"+nul+"hub", `{"q":"x"}`, "allow")},
		{"unpaired surrogate", sessionfixture.BridgeToolDeclarationForTest("call_storable_surrogate", "Read", `{"q":"\udc00"}`, "allow", "sandbox_execute")},
		{"numeric range", sessionfixture.BridgeToolDeclarationForTest("call_storable_number", "Read", `{"n":1e-16384}`, "allow", "sandbox_execute")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := f.snapshot(t)
			response, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
				Scope: f.scope, RuntimeWriteId: "storable-" + tc.declaration.GetModelToolCallId(), ModelRequestId: "request", ToolDeclaration: tc.declaration,
			})
			if status.Code(err) != codes.InvalidArgument || response != nil {
				t.Fatalf("unstorable Tool Use = %#v/%v; want InvalidArgument", response, err)
			}
			if after := f.snapshot(t); after != before {
				t.Fatal("rejected Tool Use changed events, messages, changes or receipts")
			}
		})
	}
	sequence := text.GetCommitted().GetAssignedMessageSequence()
	f.end(t, &sequence)
	seedBridgeAPIRequestStart(t, f.store, f.scope, "after-nul-start", "after-nul", runtimecontrol.RequestKindAgentProviderRequest, sequence)
	toolUse := declareToolForRelationTest(t, f, f.scope, "after-nul-tool", "after-nul",
		sessionfixture.BridgeToolDeclarationForTest("call_after_nul", "Read", `{"q":"ok"}`, "allow", "sandbox_execute"))
	errorJSON := `{"type":"tool_error","message":"partial a\u0000b","retryable":false}`
	settled, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, &bridgev1.RuntimeToolSettlement{
		ToolUseEventId: toolUse,
		Outcome:        &bridgev1.RuntimeToolSettlement_Cancelled{Cancelled: &bridgev1.RuntimeToolCancelled{ErrorJson: &errorJSON}},
	}))
	if err != nil {
		t.Fatalf("settle Tool Result with escaped U+0000: %v", err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")
	var resultProjection string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT projection_json FROM session_events
		WHERE workspace_id=$1 AND session_id=$2 AND type='agent.tool_result' AND tool_use_event_id=$3`,
		f.scope.WorkspaceId, f.scope.SessionId, toolUse).Scan(&resultProjection); err != nil {
		t.Fatalf("read committed Tool Result: %v", err)
	}
	if !strings.Contains(resultProjection, `\u0000`) {
		t.Fatalf("committed Tool Result projection = %s; want escaped U+0000", resultProjection)
	}
}

// Call-ID identity is the Thread's unique index: reuse after settlement and
// compaction is AlreadyExists without any effect, another Thread may declare
// the same ID, an exact replay returns the original identity, and a changed
// raw spelling under the same operation is the existing replay conflict.
func TestPostgreSQLToolCallIdentityIsThreadScopedAcrossCompaction(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	original := &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: "identity-c1", ModelRequestId: "request",
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest("c1", "Read", `{"n":1}`, "allow", "sandbox_execute"),
	}
	first, err := f.store.WriteEvent(f.ctx, original)
	if err != nil || first.GetCommitted() == nil {
		t.Fatalf("declare c1 = %#v/%v", first, err)
	}
	toolUse := first.GetCommitted().GetEventId()
	settled, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "done")))
	if err != nil {
		t.Fatal(err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")
	assistant := first.GetCommitted().GetAssignedMessageSequence()
	if _, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
		Scope: f.scope, RuntimeWriteId: "identity-end", ModelRequestId: "request", FinishReason: "tool_calls", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "completed", AssistantMessageSequence: &assistant, ToolUseEventIds: []string{toolUse}},
	}); err != nil {
		t.Fatalf("seal c1 request: %v", err)
	}
	seedBridgeAPIRequestStart(t, f.store, f.scope, "identity-compact-start", "compact", runtimecontrol.RequestKindCompactionSummary, assistant)
	compacted, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
		Scope: f.scope, RuntimeWriteId: "identity-compact-end", ModelRequestId: "compact", FinishReason: "end_turn", UsageJson: `{}`,
		ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "compacted"},
		CompactionContext:        sessionfixture.BridgeTextContextDeltaForTest("summary"), CompactedThroughMessageSequence: &assistant,
		CompactionEventPayloadJson: `{"type":"agent.thread_context_compacted"}`,
	})
	if err != nil || compacted.GetCommitted() == nil {
		t.Fatalf("compact = %#v/%v", compacted, err)
	}
	checkpoint := assistant + 1
	seedBridgeAPIRequestStart(t, f.store, f.scope, "identity-later-start", "later", runtimecontrol.RequestKindAgentProviderRequest, checkpoint)
	before := f.snapshot(t)
	reused, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{
		Scope: f.scope, RuntimeWriteId: "identity-c1-again", ModelRequestId: "later",
		ToolDeclaration: sessionfixture.BridgeToolDeclarationForTest("c1", "Read", `{"n":2}`, "allow", "sandbox_execute"),
	})
	if status.Code(err) != codes.AlreadyExists || reused != nil {
		t.Fatalf("reuse c1 after compaction = %#v/%v; want AlreadyExists", reused, err)
	}
	replayed, err := f.store.WriteEvent(f.ctx, original)
	if err != nil || replayed.GetDuplicate().GetEventId() != toolUse || replayed.GetDuplicate().GetAssignedMessageSequence() != assistant {
		t.Fatalf("exact c1 replay = %#v/%v; want original identity", replayed, err)
	}
	respelled := proto.Clone(original).(*bridgev1.WriteEventRequest)
	respelled.ToolDeclaration.PublicExecutionInputJson = `{"n":1.0}`
	if response, err := f.store.WriteEvent(f.ctx, respelled); status.Code(err) != codes.AlreadyExists || response != nil {
		t.Fatalf("respelled c1 replay = %#v/%v; want replay conflict", response, err)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("call-ID conflict or replay changed events, messages, changes or receipts")
	}

	const siblingID = "thr_identity_sibling"
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, siblingID)
	sibling := runtimecontrol.ScopeForThread(f.scope, siblingID)
	seedBridgeAPIRequestStart(t, f.store, sibling, "identity-sibling-start", "sibling", runtimecontrol.RequestKindAgentProviderRequest, 0)
	siblingUse := declareToolForRelationTest(t, f, sibling, "identity-sibling-c1", "sibling",
		sessionfixture.BridgeToolDeclarationForTest("c1", "Read", `{"n":1}`, "allow", "sandbox_execute"))
	facts := sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
	if facts[siblingUse].ModelToolCallID != "c1" || facts[siblingUse].ThreadID != siblingID || facts[toolUse].ModelToolCallID != "c1" {
		t.Fatalf("Thread-scoped c1 declarations = %+v / %+v", facts[toolUse], facts[siblingUse])
	}
}

// The relation columns are checked by scalar constraints the workload cannot
// bypass: one result per Tool Use, a same-Thread Tool Use target, and the
// presence table. The synthetic repair is the only result with a call ID.
func TestPostgreSQLToolRelationConstraintsRejectMalformedRows(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	toolUse := declareToolForRelationTest(t, f, f.scope, "constraint-tool", "request",
		sessionfixture.BridgeToolDeclarationForTest("call_constraint", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
	settled, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "done")))
	if err != nil {
		t.Fatal(err)
	}
	sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settled, "committed")
	const otherThreadID = "thr_constraint_other"
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, otherThreadID)
	client := dbconnect.NewClientForTesting(f.workload.DB)
	insert := func(threadID, eventID, eventType, payload string, modelRequestID, callID, toolUseID any) error {
		return client.WithWorkspaceTx(f.ctx, f.scope.WorkspaceId, "test.tool_relation_constraint", func(tx *dbconnect.Tx) error {
			_, err := tx.Exec(f.ctx, `INSERT INTO session_events (
				workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
				model_request_id, model_tool_call_id, tool_use_event_id, created_at, updated_at
			) VALUES ($1,$2,$3,$4,(SELECT COALESCE(MAX(sequence),0)+1 FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3),
				$5,$6,$7,$8,$9,now(),now())`,
				f.scope.WorkspaceId, f.scope.SessionId, threadID, eventID, eventType, payload, modelRequestID, callID, toolUseID)
			return err
		})
	}
	for _, tc := range []struct {
		name, thread, eventType, payload string
		request, call, use               any
		code, constraint                 string
	}{
		{"second result", f.scope.SessionThreadId, "agent.tool_result", `{"tool_use_id":"` + toolUse + `"}`, "request", nil, toolUse, "23505", "idx_session_events_tool_result_unique"},
		{"missing Tool Use", f.scope.SessionThreadId, "agent.tool_result", `{"tool_use_id":"evt_missing"}`, "request", nil, "evt_missing", "23503", "session_events_tool_use_event_fkey"},
		{"wrong Thread", otherThreadID, "agent.tool_result", `{"tool_use_id":"` + toolUse + `"}`, "request", nil, toolUse, "23503", "session_events_tool_use_event_fkey"},
		{"Tool Use without call ID", f.scope.SessionThreadId, "agent.tool_use", `{}`, "request", nil, nil, "23514", "session_events_tool_relation_shape"},
		{"Tool Use without request", f.scope.SessionThreadId, "agent.tool_use", `{}`, nil, "call_constraint_no_request", nil, "23514", "session_events_tool_relation_shape"},
		{"Tool Use with reference", f.scope.SessionThreadId, "agent.tool_use", `{}`, "request", "call_constraint_reference", toolUse, "23514", "session_events_tool_relation_shape"},
		{"result without relation", f.scope.SessionThreadId, "agent.tool_result", `{}`, "request", nil, nil, "23514", "session_events_tool_relation_shape"},
		{"result with both", f.scope.SessionThreadId, "agent.tool_result", `{}`, "request", "call_constraint_both", toolUse, "23514", "session_events_tool_relation_shape"},
		{"MCP result with call ID", f.scope.SessionThreadId, "agent.mcp_tool_result", `{}`, "request", "call_constraint_mcp", nil, "23514", "session_events_tool_relation_shape"},
		{"message with call ID", f.scope.SessionThreadId, "agent.message", `{}`, "request", "call_constraint_message", nil, "23514", "session_events_tool_relation_shape"},
		{"message with reference", f.scope.SessionThreadId, "agent.message", `{}`, "request", nil, toolUse, "23514", "session_events_tool_relation_shape"},
		{"empty call ID", f.scope.SessionThreadId, "agent.tool_use", `{}`, "request", "", nil, "23514", "session_events_tool_relation_shape"},
		{"unstorable Tool Use", f.scope.SessionThreadId, "agent.tool_use", `{"input":"a\u0000b"}`, "request", "call_constraint_nul", nil, "22P05", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := insert(tc.thread, "evt_constraint_"+strings.ReplaceAll(tc.name, " ", "_"), tc.eventType, tc.payload, tc.request, tc.call, tc.use)
			var pgError *pgconn.PgError
			if !errors.As(err, &pgError) || pgError.Code != tc.code || pgError.ConstraintName != tc.constraint {
				t.Fatalf("malformed %s = %v; want SQLSTATE %s on %q", tc.name, err, tc.code, tc.constraint)
			}
		})
	}

	repairRequest := contentRepairRequest(f.scope)
	repaired, err := f.store.CommitInternalToolRepair(f.ctx, repairRequest)
	if err != nil || repaired.GetCommitted() == nil {
		t.Fatalf("invalid-tool repair = %#v/%v", repaired, err)
	}
	repairID := repaired.GetCommitted().GetRepairEventId()
	var callParts, errorResults, references, repairCalls, unfinished int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_messages m CROSS JOIN LATERAL jsonb_array_elements(m.data_json::jsonb->'parts') part
		  WHERE m.workspace_id=$1 AND m.session_id=$2 AND part->>'type'='tool_call' AND part->>'modelToolCallId'='call'),
		(SELECT count(*) FROM session_messages m CROSS JOIN LATERAL jsonb_array_elements(m.data_json::jsonb->'parts') part
		  WHERE m.workspace_id=$1 AND m.session_id=$2 AND part->>'type'='tool_result' AND part->>'modelToolCallId'='call' AND part->'result'->>'type'='error'),
		(SELECT count(*) FROM session_events WHERE event_id=$3 AND tool_use_event_id IS NOT NULL),
		(SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND model_tool_call_id='call'),
		(SELECT count(*) FROM session_events tool WHERE tool.workspace_id=$1 AND tool.session_id=$2
		  AND tool.type IN ('agent.tool_use','agent.mcp_tool_use')
		  AND NOT EXISTS (SELECT 1 FROM session_events result WHERE result.workspace_id=tool.workspace_id
		    AND result.session_id=tool.session_id AND result.session_thread_id=tool.session_thread_id
		    AND result.tool_use_event_id=tool.event_id))`,
		f.scope.WorkspaceId, f.scope.SessionId, repairID).Scan(&callParts, &errorResults, &references, &repairCalls, &unfinished); err != nil {
		t.Fatalf("read repair relation: %v", err)
	}
	if callParts != 1 || errorResults != 1 || references != 0 || repairCalls != 1 || unfinished != 0 {
		t.Fatalf("repair relation = calls %d errors %d references %d call rows %d unfinished %d", callParts, errorResults, references, repairCalls, unfinished)
	}
	if !sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)[repairID].Repair {
		t.Fatal("repair event is not classified by its call-ID column")
	}
}

// A unique violation raised by a Tool event INSERT maps by constraint name.
// The result index cannot be reached after a held Session fence, so a forced
// violation is an internal invariant error, attempted once, that rolls back
// every fact of the settlement. A repair reusing a declared call ID is the
// existing AlreadyExists.
func TestPostgreSQLToolRelationUniqueViolationsMapByConstraint(t *testing.T) {
	f := newContentDeclarationFixture(t)
	f.start(t)
	toolUse := declareToolForRelationTest(t, f, f.scope, "mapping-tool", "request",
		sessionfixture.BridgeToolDeclarationForTest("call_mapping", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
	if _, err := f.admin.ExecContext(f.ctx, `CREATE SEQUENCE forced_result_conflicts;
		CREATE FUNCTION force_result_conflict() RETURNS trigger SECURITY DEFINER SET search_path = pg_catalog, public AS $$
		BEGIN
			PERFORM nextval('public.forced_result_conflicts');
			RAISE EXCEPTION 'forced result conflict' USING ERRCODE = 'unique_violation', CONSTRAINT = 'idx_session_events_tool_result_unique';
		END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER force_result_conflict BEFORE INSERT ON session_events
		FOR EACH ROW WHEN (NEW.type = 'agent.tool_result' AND NEW.tool_use_event_id IS NOT NULL) EXECUTE FUNCTION force_result_conflict()`); err != nil {
		t.Fatalf("install forced result conflict: %v", err)
	}
	before := f.snapshot(t)
	response, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "done")))
	if status.Code(err) != codes.Internal || response != nil {
		t.Fatalf("forced result conflict = %#v/%v; want internal invariant error", response, err)
	}
	var attempts int64
	var routeStatus string
	if err := f.admin.QueryRowContext(f.ctx, `SELECT (SELECT last_value FROM forced_result_conflicts),
		(SELECT status FROM session_pending_tool_uses WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3)`,
		f.scope.WorkspaceId, f.scope.SessionId, toolUse).Scan(&attempts, &routeStatus); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || routeStatus != "resolving" {
		t.Fatalf("forced conflict attempts %d route %s; want one attempt and an untouched route", attempts, routeStatus)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("forced result conflict left events, messages, changes or receipts")
	}

	repairRequest := contentRepairRequest(f.scope)
	repairRequest.ModelToolCallId = "call_mapping"
	repairRequest.RepairKey = internalToolRepairKey("request", "call_mapping", "unknown")
	if repaired, err := f.store.CommitInternalToolRepair(f.ctx, repairRequest); status.Code(err) != codes.AlreadyExists || repaired != nil {
		t.Fatalf("repair reusing a declared call ID = %#v/%v; want AlreadyExists", repaired, err)
	}
	if after := f.snapshot(t); after != before {
		t.Fatal("rejected repair left events, messages, changes or receipts")
	}
}

// A compaction checkpoint admits only text parts: empty text and text at the
// existing byte bound are accepted; reasoning, Tool parts, nil parts, absent
// or nil text and text over the bound are InvalidArgument before anything
// commits, leaving the previous checkpoint, the inherited prefix and the
// receipts intact.
func TestPostgreSQLCompactionAdmitsOnlyTextParts(t *testing.T) {
	f := newContentDeclarationFixture(t)
	const childID = "thr_compaction_text_child"
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID)
	running, err := f.store.WriteEvent(f.ctx, &bridgev1.WriteEventRequest{Scope: f.scope, RuntimeWriteId: "compaction-parent-running", EventType: "session.status_running", PayloadJson: `{"type":"session.status_running"}`})
	if err != nil || running.GetCommitted() == nil {
		t.Fatalf("parent turn = %#v/%v", running, err)
	}
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_thread_context_prefixes
		(workspace_id,session_id,child_thread_id,parent_thread_id,parent_boundary_event_id,entries_json,created_at)
		VALUES ($1,$2,$3,$4,$5,'[]',now())`, f.scope.WorkspaceId, f.scope.SessionId, childID, f.scope.SessionThreadId, running.GetCommitted().GetEventId()); err != nil {
		t.Fatalf("seed inherited prefix: %v", err)
	}
	child := runtimecontrol.ScopeForThread(f.scope, childID)
	prefix := &bridgev1.PrefixConsumptionDraft{ChildThreadId: childID, ParentBoundaryEventId: running.GetCommitted().GetEventId()}
	text := func(value string) *bridgev1.RuntimeContextPart {
		return &bridgev1.RuntimeContextPart{Content: &bridgev1.RuntimeContextPart_Text{Text: &bridgev1.RuntimeContextText{Text: value}}}
	}
	end := func(scope *bridgev1.RuntimeScope, request string, writeID string, boundary int64, consume *bridgev1.PrefixConsumptionDraft, parts []*bridgev1.RuntimeContextPart) (*bridgev1.WriteRequestEndResponse, error) {
		return f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
			Scope: scope, RuntimeWriteId: writeID, ModelRequestId: request, FinishReason: "end_turn", UsageJson: `{}`,
			ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "compacted"},
			CompactionContext:        &bridgev1.RuntimeContextDelta{Parts: parts}, CompactedThroughMessageSequence: &boundary,
			CompactionEventPayloadJson: `{"type":"agent.thread_context_compacted"}`, PrefixConsumption: consume,
		})
	}
	// checkpointState names every checkpoint of the Session and the prefix's
	// consuming checkpoint.
	checkpointState := func(t *testing.T) string {
		t.Helper()
		var state string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT
			COALESCE((SELECT string_agg(session_thread_id || ':' || sequence || ':' || data_json, '|' ORDER BY session_thread_id, sequence)
			  FROM session_messages WHERE workspace_id=$1 AND session_id=$2 AND kind='compaction'), '') || '#' ||
			COALESCE((SELECT consumed_by_checkpoint_message_id FROM session_thread_context_prefixes WHERE workspace_id=$1 AND child_thread_id=$3), '')`,
			f.scope.WorkspaceId, f.scope.SessionId, childID).Scan(&state); err != nil {
			t.Fatalf("read checkpoints: %v", err)
		}
		return state
	}
	maximum := strings.Repeat("x", runtimecontrol.RuntimeContextTextJSONMaxBytes-2)
	rejected := []struct {
		name  string
		parts []*bridgev1.RuntimeContextPart
	}{
		{"reasoning", []*bridgev1.RuntimeContextPart{text("summary"), {Content: &bridgev1.RuntimeContextPart_Reasoning{Reasoning: &bridgev1.RuntimeContextReasoning{Text: "thought", ProviderMetadataJson: bridgeString(`{}`)}}}}},
		{"tool call", []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolCall{ToolCall: &bridgev1.RuntimeContextToolCall{ModelToolCallId: "call_compacted", ToolName: "Read", ProviderInputJson: `{}`}}}}},
		{"tool result", []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_ToolResult{ToolResult: &bridgev1.RuntimeContextToolResult{ModelToolCallId: "call_compacted", Outcome: &bridgev1.RuntimeContextToolResult_Cancelled{Cancelled: &bridgev1.RuntimeContextToolCancelled{}}}}}}},
		{"nil part", []*bridgev1.RuntimeContextPart{text("summary"), nil}},
		{"absent content", []*bridgev1.RuntimeContextPart{{}}},
		{"nil text", []*bridgev1.RuntimeContextPart{{Content: &bridgev1.RuntimeContextPart_Text{}}}},
		{"empty delta", nil},
		{"over bound", []*bridgev1.RuntimeContextPart{text(maximum + "x")}},
	}
	assertRejected := func(t *testing.T, scope *bridgev1.RuntimeScope, request string, boundary int64, consume *bridgev1.PrefixConsumptionDraft) {
		t.Helper()
		for _, tc := range rejected {
			before, beforeCheckpoints := f.snapshot(t), checkpointState(t)
			response, err := end(scope, request, request+"-"+strings.ReplaceAll(tc.name, " ", "-"), boundary, consume, tc.parts)
			if status.Code(err) != codes.InvalidArgument || response != nil {
				t.Fatalf("%s compaction with %s = %#v/%v; want InvalidArgument", request, tc.name, response, err)
			}
			if f.snapshot(t) != before || checkpointState(t) != beforeCheckpoints {
				t.Fatalf("%s compaction with %s changed events, messages, changes, receipts, checkpoints or the prefix", request, tc.name)
			}
		}
	}

	seedBridgeAPIRequestStart(t, f.store, child, "child-compact-start", "child-compact", runtimecontrol.RequestKindCompactionSummary, 0)
	assertRejected(t, child, "child-compact", 0, prefix)
	if response, err := end(child, "child-compact", "child-compact-end", 0, prefix, []*bridgev1.RuntimeContextPart{text(""), text("summary")}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("text compaction = %#v/%v; want committed", response, err)
	}
	childState := checkpointState(t)
	if !strings.HasPrefix(childState, childID+`:1:{"parts":[{"text":"","type":"text"},{"text":"summary","type":"text"}]}#`) || strings.HasSuffix(childState, "#") {
		t.Fatalf("child checkpoint and prefix = %s; want two stored text parts consuming the prefix", childState)
	}

	seedBridgeAPIRequestStart(t, f.store, f.scope, "main-first-start", "main-first", runtimecontrol.RequestKindCompactionSummary, 0)
	if response, err := end(f.scope, "main-first", "main-first-end", 0, nil, []*bridgev1.RuntimeContextPart{text("first summary")}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("first main compaction = %#v/%v; want committed", response, err)
	}
	seedBridgeAPIRequestStart(t, f.store, f.scope, "main-second-start", "main-second", runtimecontrol.RequestKindCompactionSummary, 1)
	assertRejected(t, f.scope, "main-second", 1, nil)
	if response, err := end(f.scope, "main-second", "main-second-end", 1, nil, []*bridgev1.RuntimeContextPart{text(maximum)}); err != nil || response.GetCommitted() == nil {
		t.Fatalf("compaction at the text bound = %#v/%v; want committed", response, err)
	}
	var mainCheckpoints int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT count(*) FROM session_messages
		WHERE workspace_id=$1 AND session_id=$2 AND session_thread_id=$3 AND kind='compaction'`,
		f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId).Scan(&mainCheckpoints); err != nil || mainCheckpoints != 2 {
		t.Fatalf("main checkpoints = %d/%v; want 2", mainCheckpoints, err)
	}
}

// seedTerminalSandboxExecutionForRelationTest records a finished, unconsumed
// Sandbox execution of the Tool Use, as Sandbox Service leaves it.
func seedTerminalSandboxExecutionForRelationTest(t *testing.T, f contentDeclarationFixture, toolUseEventID, modelToolCallID, inputJSON string) {
	t.Helper()
	const resultJSON = `{"status":"success","stdout":"done"}`
	if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_runtime_tool_results (
		workspace_id, session_id, session_thread_id, tool_use_event_id, tool_kind,
		normalized_input_hash, tool_name, input_json, ack_status, result_json,
		model_tool_call_id, execution_state, execution_attempt_generation,
		result_digest, created_at, updated_at
	) VALUES ($1, $2, $3, $4, 'sandbox_tool', $5, 'Read', $6, 'committed', $7, $8, 'terminal_unconsumed', 1, $9, now(), now())`,
		f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, toolUseEventID,
		runtimecontrol.Sha256Hex(inputJSON), inputJSON, resultJSON, modelToolCallID, runtimecontrol.Sha256Hex(resultJSON)); err != nil {
		t.Fatalf("seed terminal Sandbox execution: %v", err)
	}
}

// toolResultCustody reads, for one Tool Use, its referencing results, the
// stored result parts of its call, and its Sandbox execution consumption.
func toolResultCustody(t *testing.T, f contentDeclarationFixture, toolUseEventID, modelToolCallID string) (results int, resultParts int, executionState string, consumedBy string, resultEventID string) {
	t.Helper()
	var consumed, result sql.NullString
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3),
		(SELECT count(*) FROM session_messages m CROSS JOIN LATERAL jsonb_array_elements(m.data_json::jsonb->'parts') part
		  WHERE m.workspace_id=$1 AND m.session_id=$2 AND part->>'type'='tool_result' AND part->>'modelToolCallId'=$4),
		(SELECT execution_state FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3),
		(SELECT consumed_by_terminal_event_id FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3),
		(SELECT MAX(event_id) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3)`,
		f.scope.WorkspaceId, f.scope.SessionId, toolUseEventID, modelToolCallID).Scan(&results, &resultParts, &executionState, &consumed, &result); err != nil {
		t.Fatalf("read Tool result custody: %v", err)
	}
	return results, resultParts, executionState, consumed.String, result.String
}

// Terminal writers for one Tool Use serialize on the Session fence. Whichever
// commits first writes the one referenced result, the one result part and the
// one consumption of its Sandbox execution; a later settlement is stale unless
// it replays its own receipt, and a later interrupt finds nothing unfinished.
// A failure after the result part and event are written but before the
// settlement receipt rolls every new fact and the consumption back.
func TestPostgreSQLToolResultHasOneWinnerAndRollsBackAsOneTransaction(t *testing.T) {
	setup := func(t *testing.T, callID string) (contentDeclarationFixture, string, func() *bridgev1.SettleToolResultResponse, func() *bridgev1.WriteRequestEndCommitted) {
		t.Helper()
		f := newContentDeclarationFixture(t)
		seedBridgeAPIOpenDurableTurn(t, f.admin, f.scope, "evt_race_run")
		f.start(t)
		toolUse := declareToolForRelationTest(t, f, f.scope, "race-tool", "request",
			sessionfixture.BridgeToolDeclarationForTest(callID, "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
		seedTerminalSandboxExecutionForRelationTest(t, f, toolUse, callID, `{"path":"a.txt"}`)
		queueStore := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(f.workload.OpenWorkload(t, "queue", nil)))
		settle := func() *bridgev1.SettleToolResultResponse {
			t.Helper()
			response, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "done")))
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			return response
		}
		// interrupt admits the interrupt input only now, so the settlement it
		// races is not already fenced by an accepted interrupt barrier.
		interrupt := func() *bridgev1.WriteRequestEndCommitted {
			t.Helper()
			sequence := sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId)
			seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "evt_race_interrupt", sequence, "user.interrupt", `{}`)
			sessionfixture.SeedBridgeAPIRuntimeInbox(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "rin_race_interrupt",
				"interrupt_control", `["evt_race_interrupt"]`, "accepted", f.scope.Binding.BindingId, f.scope.Binding.TargetPodUid, sequence, sequence)
			enqueueInterruptExhaustionJob(t, queueStore, f.scope.SessionId, f.scope.SessionThreadId, "rin_race_interrupt", "interrupt_control",
				"evt_race_interrupt", sequence, queue.DefaultMaxAttempts, time.Now().UTC())
			lease := mustLeaseBridgeQueueJob(t, queueStore, queue.LeaseRequest{
				WorkspaceID: workspace.DefaultID, Kinds: []string{queue.KindRuntimeInput}, LeaseOwner: "race-interrupt",
				MaxJobs: 1, LeaseDuration: time.Minute, Now: time.Now().UTC(),
			})
			response, err := f.store.WriteRequestEnd(f.ctx, &bridgev1.WriteRequestEndRequest{
				Scope: f.scope, RuntimeWriteId: "race-interrupt-end", ModelRequestId: "request",
				FinishReason: "cancelled", UsageJson: `{}`, IsError: true, ErrorKind: "runtime_interrupted",
				ProviderContextRetention: &bridgev1.ProviderContextRetention{Disposition: "interrupted", ToolUseEventIds: []string{toolUse}},
				InterruptSettlement:      &bridgev1.RequestEndInterruptSettlement{RuntimeInputId: "rin_race_interrupt", InterruptLeaseRef: sessionfixture.BridgeInterruptLeaseRef(lease)},
			})
			if err != nil || response.GetCommitted() == nil {
				t.Fatalf("interrupt = %#v/%v; want committed", response, err)
			}
			return response.GetCommitted()
		}
		return f, toolUse, settle, interrupt
	}
	requireOneConsumedResult := func(t *testing.T, f contentDeclarationFixture, toolUse, callID string) string {
		t.Helper()
		results, parts, state, consumedBy, resultID := toolResultCustody(t, f, toolUse, callID)
		if results != 1 || parts != 1 || state != "consumed" || consumedBy != resultID {
			t.Fatalf("Tool custody = results %d parts %d execution %s consumed by %q (result %q)", results, parts, state, consumedBy, resultID)
		}
		sessionfixture.RequireToolRelationFactsForTest(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId)
		return resultID
	}

	t.Run("interrupt first", func(t *testing.T) {
		f, toolUse, settle, interrupt := setup(t, "call_race_interrupt")
		if committed := interrupt(); len(committed.GetInterruptToolResults()) != 1 {
			t.Fatalf("winning interrupt Tool results = %d; want 1", len(committed.GetInterruptToolResults()))
		}
		resultID := requireOneConsumedResult(t, f, toolUse, "call_race_interrupt")
		if response := settle(); response.GetStale() == nil {
			t.Fatalf("losing settlement = %#v; want stale", response)
		}
		if requireOneConsumedResult(t, f, toolUse, "call_race_interrupt") != resultID {
			t.Fatal("losing settlement replaced the winning result")
		}
	})

	t.Run("settlement first", func(t *testing.T) {
		f, toolUse, settle, interrupt := setup(t, "call_race_settlement")
		sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settle(), "committed")
		resultID := requireOneConsumedResult(t, f, toolUse, "call_race_settlement")
		if committed := interrupt(); len(committed.GetInterruptToolResults()) != 0 {
			t.Fatalf("losing interrupt Tool results = %d; want 0", len(committed.GetInterruptToolResults()))
		}
		sessionfixture.BridgeRequireToolSettlementOutcomeForTest(t, settle(), "duplicate")
		if requireOneConsumedResult(t, f, toolUse, "call_race_settlement") != resultID {
			t.Fatal("losing interrupt or settlement replay replaced the winning result")
		}
	})

	t.Run("failure before receipt", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		f.start(t)
		toolUse := declareToolForRelationTest(t, f, f.scope, "rollback-tool", "request",
			sessionfixture.BridgeToolDeclarationForTest("call_rollback", "Read", `{"path":"a.txt"}`, "allow", "sandbox_execute"))
		seedTerminalSandboxExecutionForRelationTest(t, f, toolUse, "call_rollback", `{"path":"a.txt"}`)
		if _, err := f.admin.ExecContext(f.ctx, `CREATE FUNCTION fail_settlement_receipt() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'settlement receipt failed' USING ERRCODE = 'P0001'; END; $$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_settlement_receipt BEFORE INSERT ON session_bridge_operations
			FOR EACH ROW WHEN (NEW.operation = 'settle_tool_result') EXECUTE FUNCTION fail_settlement_receipt()`); err != nil {
			t.Fatalf("install receipt failure: %v", err)
		}
		before := f.snapshot(t)
		response, err := f.store.SettleToolResult(f.ctx, sessionfixture.BridgeToolSettlementRequestForTest(f.scope, sessionfixture.BridgeCompletedToolSettlementForTest(toolUse, "done")))
		var pgError *pgconn.PgError
		if !errors.As(err, &pgError) || pgError.Code != "P0001" || response != nil {
			t.Fatalf("settlement with failing receipt = %#v/%v; want the receipt failure", response, err)
		}
		results, parts, state, consumedBy, _ := toolResultCustody(t, f, toolUse, "call_rollback")
		var routeStatus string
		if err := f.admin.QueryRowContext(f.ctx, `SELECT status FROM session_pending_tool_uses WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3`,
			f.scope.WorkspaceId, f.scope.SessionId, toolUse).Scan(&routeStatus); err != nil {
			t.Fatal(err)
		}
		if results != 0 || parts != 0 || state != "terminal_unconsumed" || consumedBy != "" || routeStatus != "resolving" || f.snapshot(t) != before {
			t.Fatalf("failed settlement left results %d parts %d execution %s consumed by %q route %s", results, parts, state, consumedBy, routeStatus)
		}
	})
}

// answerToolUseForRelationTest records a result for the Tool Use without
// touching its route, so only the result relation makes it non-executable.
func answerToolUseForRelationTest(t *testing.T, admin *sql.DB, scope *bridgev1.RuntimeScope, toolUseEventID string) {
	t.Helper()
	seedBridgeAPIEvent(t, admin, scope.GetWorkspaceId(), scope.GetSessionId(), scope.GetSessionThreadId(), "evt_answer_"+toolUseEventID,
		sessionfixture.NextBridgeAPIEventSequenceForTest(t, admin, scope.GetSessionId(), scope.GetSessionThreadId()),
		"agent.tool_result", `{"tool_use_id":"`+toolUseEventID+`","content":[{"type":"text","text":"answered"}],"is_error":false}`)
}

// The Sandbox-serving entrypoints share the result relation through
// ToolResultForToolUseExistsTx and lockExecutableToolRouteTx. Under the real
// Bridge role, a Tool Use with an allowed route but a recorded result admits no
// Sandbox execution, background command or memory effect.
func TestPostgreSQLSandboxEntrypointsTreatAnsweredToolUseAsSettled(t *testing.T) {
	effects := func(t *testing.T, f contentDeclarationFixture, toolUseEventID string) (receipts, jobs, memories int) {
		t.Helper()
		if err := f.admin.QueryRowContext(f.ctx, `SELECT
			(SELECT count(*) FROM session_runtime_tool_results WHERE workspace_id=$1 AND session_id=$2 AND tool_use_event_id=$3),
			(SELECT count(*) FROM queue_jobs WHERE workspace_id=$1 AND kind IN ('sandbox_tool_execute','sandbox_background_command')),
			(SELECT count(*) FROM memories WHERE workspace_id=$1)`,
			f.scope.WorkspaceId, f.scope.SessionId, toolUseEventID).Scan(&receipts, &jobs, &memories); err != nil {
			t.Fatalf("read Sandbox effects: %v", err)
		}
		return receipts, jobs, memories
	}
	t.Run("AcceptSandboxExecution", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		scope := f.scope
		sessionfixture.SeedReadySandboxForSharedToolExecution(t, f.admin, scope.WorkspaceId, scope.SessionId)
		toolUse := writeDurableOrdinaryToolUseForTest(t, f.store, scope, "mreq_answered_sandbox", "call_answered_sandbox", "Read", `{"path":"a.txt"}`)
		answerToolUseForRelationTest(t, f.admin, scope, toolUse)
		response, err := f.store.AcceptSandboxExecution(f.ctx, &bridgev1.AcceptSandboxExecutionRequest{Scope: scope, ToolUseEventId: toolUse})
		if status.Code(err) != codes.FailedPrecondition || response != nil {
			t.Fatalf("answered Sandbox execution = %#v/%v; want FailedPrecondition", response, err)
		}
		if receipts, jobs, _ := effects(t, f, toolUse); receipts != 0 || jobs != 0 {
			t.Fatalf("answered Sandbox execution effects = receipts %d jobs %d", receipts, jobs)
		}
	})
	t.Run("SendCommandInput", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		scope := f.scope
		toolUse := writeDurableOrdinaryToolUseForTest(t, f.store, scope, "mreq_answered_stdin", "call_answered_stdin", "write_stdin",
			`{"session_id":"task_answered_stdin","chars":"hello"}`)
		seedBridgeAPIBackgroundTask(t, f.admin, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, scope.Binding.BindingId, "task_answered_stdin", "evt_answered_stdin_source")
		answerToolUseForRelationTest(t, f.admin, scope, toolUse)
		response, err := f.store.SendCommandInput(f.ctx, &bridgev1.SendCommandInputRequest{
			Scope: scope, TaskId: "task_answered_stdin", ToolUseEventId: toolUse, OperationId: "cmdop_answered_stdin",
		})
		if status.Code(err) != codes.FailedPrecondition || response != nil {
			t.Fatalf("answered background command = %#v/%v; want FailedPrecondition", response, err)
		}
		if receipts, jobs, _ := effects(t, f, toolUse); receipts != 0 || jobs != 0 {
			t.Fatalf("answered background command effects = receipts %d jobs %d", receipts, jobs)
		}
	})
	t.Run("RunMemory", func(t *testing.T) {
		f := newContentDeclarationFixture(t)
		scope := f.scope
		sessionfixture.SeedBridgeAPIWritableMemoryStore(t, f.admin, scope.WorkspaceId, scope.SessionId, "memstore_answered")
		toolUse := writeDurableOrdinaryToolUseForTest(t, f.store, scope, "mreq_answered_memory", "call_answered_memory", "memory",
			`{"action":"create","path":"answered.md","content":"owned"}`)
		answerToolUseForRelationTest(t, f.admin, scope, toolUse)
		response, err := f.store.RunMemory(f.ctx, &bridgev1.RunMemoryRequest{Scope: scope, ToolUseEventId: toolUse})
		if status.Code(err) != codes.FailedPrecondition || response != nil {
			t.Fatalf("answered memory effect = %#v/%v; want FailedPrecondition", response, err)
		}
		if receipts, _, memories := effects(t, f, toolUse); receipts != 0 || memories != 0 {
			t.Fatalf("answered memory effects = receipts %d memories %d", receipts, memories)
		}
	})
}

// CommitTaskNotificationResult reaches the child-control fence through
// ThreadOrAncestorClosingTx. A committed close control fences its child until
// the source Tool Use has a result; a source that does not exist is not
// terminal and is not an error.
func TestPostgreSQLTaskNotificationCloseFenceFollowsSourceToolResult(t *testing.T) {
	for _, source := range []string{"answered", "unanswered", "missing"} {
		t.Run(source, func(t *testing.T) {
			f := newContentDeclarationFixture(t)
			const childID = "thr_close_fence_child"
			sessionfixture.SeedBridgeAPIChildThread(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, childID)
			const sourceID = "evt_close_fence_source"
			if source != "missing" {
				seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, sourceID,
					sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId),
					"agent.tool_use", `{"name":"close_agent","input":{"task_name":"task_`+childID+`"},"evaluated_permission":"allow"}`)
			}
			if source == "answered" {
				answerToolUseForRelationTest(t, f.admin, f.scope, sourceID)
			}
			seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, childID, "evt_close_fence_control", 1,
				"agent.thread_interrupt_requested",
				`{"root_child_thread_id":"`+childID+`","action":"close","source_tool_use_event_id":"`+sourceID+`","runtime_input_id":"close_fence_input","disposition":"pending_control"}`)
			sessionfixture.SeedBridgeAPIRuntimeInbox(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, childID, "close_fence_input",
				"interrupt_control", `[]`, "committed", f.scope.Binding.BindingId, f.scope.Binding.TargetPodUid, 1, 1)
			const taskID = "task_close_fence"
			seedBridgeAPINotifiableBackgroundTask(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, childID, f.scope.Binding.BindingId, taskID, "evt_close_fence_task_source")
			settleBridgeAPIBackgroundTask(t, f.admin, f.scope.SessionId, taskID, "completed",
				`{"task_id":"task_close_fence","source_tool_use_event_id":"evt_close_fence_task_source","status":"completed","stdout":{"text":"done","truncated":false},"stderr":{"text":"","truncated":false}}`)
			inputID := "task_notification:" + taskID
			sessionfixture.SeedBridgeAPIRuntimeInbox(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, childID, inputID,
				"task_notification", `[]`, "parked", f.scope.Binding.BindingId, f.scope.Binding.TargetPodUid, 0, 0)
			response, err := f.store.CommitTaskNotificationResult(f.ctx, &bridgev1.CommitTaskNotificationResultRequest{
				Scope: runtimecontrol.ScopeForThread(f.scope, childID), RuntimeInputId: inputID,
			})
			var inboxStatus string
			if readErr := f.admin.QueryRowContext(f.ctx, `SELECT status FROM session_runtime_inbox WHERE workspace_id=$1 AND runtime_input_id=$2`,
				f.scope.WorkspaceId, inputID).Scan(&inboxStatus); readErr != nil {
				t.Fatal(readErr)
			}
			if source == "answered" {
				// Unfenced, the parked notification reaches its ordinary
				// deliverability rule instead of parking again.
				if status.Code(err) != codes.FailedPrecondition || !strings.Contains(status.Convert(err).Message(), "not deliverable") || response != nil {
					t.Fatalf("answered source = %#v/%v Inbox %s; want unfenced deliverability rejection", response, err, inboxStatus)
				}
				return
			}
			if err != nil || response.GetParked() == nil || inboxStatus != "parked" {
				t.Fatalf("%s source = %#v/%v Inbox %s; want parked", source, response, err, inboxStatus)
			}
		})
	}
}

// The parent's child-control and agent-mail paths treat their source Tool Use
// as terminal only when a result references it through the relation column.
// Under the real Bridge role, a result for another Tool Use in the same Thread
// leaves an admitted close pending for AwaitChildInterrupt and
// CloseChildControl and lets agent-mail declaration proceed; once each source
// has its own result, all three are rejected as terminal and change nothing.
func TestPostgreSQLChildControlAndAgentMailSourcesFollowToolResult(t *testing.T) {
	f := newContentDeclarationFixture(t)
	scope := f.scope
	const (
		childID     = "thr_source_result_child"
		mailChildID = "thr_source_result_mail_child"
		controlID   = "evt_source_result_close"
		mailID      = "evt_source_result_mail"
		otherID     = "evt_source_result_other"
		content     = "mail from the source Tool Use"
	)
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, childID)
	sessionfixture.SeedBridgeAPIChildThread(t, f.admin, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, mailChildID)
	for _, source := range []struct{ id, payload string }{
		{controlID, `{"type":"agent.tool_use","name":"close_agent","input":{"task_name":"task_` + childID + `"}}`},
		{mailID, `{"type":"agent.tool_use","name":"send_message","input":{"task_name":"task_` + mailChildID + `","message":"` + content + `"}}`},
		{otherID, `{"type":"agent.tool_use","name":"Read","input":{"path":"a.txt"}}`},
	} {
		seedBridgeAPIEvent(t, f.admin, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, source.id,
			sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, scope.SessionId, scope.SessionThreadId), "agent.tool_use", source.payload)
	}
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_events SET visibility='public' WHERE workspace_id=$1 AND session_id=$2 AND event_id=$3`,
		scope.WorkspaceId, scope.SessionId, controlID); err != nil {
		t.Fatalf("make control source public: %v", err)
	}
	sessionfixture.SeedBridgeAPIAllowedToolRoute(t, f.admin, scope.WorkspaceId, scope.SessionId, scope.SessionThreadId, controlID)
	answerToolUseForRelationTest(t, f.admin, scope, otherID)
	admitted, err := f.store.AdmitChildInterrupt(f.ctx, &bridgev1.AdmitChildInterruptRequest{
		Scope: scope, SourceToolUseEventId: controlID, TargetChildThreadId: childID,
		Action: bridgev1.ChildControlAction_CHILD_CONTROL_ACTION_CLOSE,
	})
	if err != nil || admitted.GetCommitted() == nil {
		t.Fatalf("admit child close = %#v/%v; want committed", admitted, err)
	}
	operationID := admitted.GetCommitted().GetControlOperationId()
	await := func() (*bridgev1.AwaitChildInterruptResponse, error) {
		return f.store.AwaitChildInterrupt(f.ctx, &bridgev1.AwaitChildInterruptRequest{Scope: scope, ControlOperationId: operationID})
	}
	closeChild := func() (*bridgev1.CloseChildControlResponse, error) {
		return f.store.CloseChildControl(f.ctx, &bridgev1.CloseChildControlRequest{Scope: scope, ControlOperationId: operationID})
	}
	// Mail declaration runs in a Bridge transaction that is always rolled back.
	rollback := errors.New("roll back the agent-mail declaration")
	declareMail := func() (runtimecontrol.StoredAgentMailEnvelope, error) {
		var envelope runtimecontrol.StoredAgentMailEnvelope
		err := f.store.withScopeTx(f.ctx, scope, "test.mail_declaration", func(tx *dbconnect.Tx) error {
			declared, err := appendSubagentMailEnvelopeTx(f.ctx, tx, scope, runtimecontrol.AgentMailDeliveryID(mailID, mailChildID),
				mailChildID, mailID, content, time.Now().UTC())
			if err != nil {
				return err
			}
			envelope = declared
			return rollback
		})
		return envelope, err
	}
	failedPrecondition := func(err error, message string) bool {
		return status.Code(err) == codes.FailedPrecondition && status.Convert(err).Message() == message
	}

	if response, err := await(); status.Code(err) != codes.DeadlineExceeded || response != nil {
		t.Fatalf("await with an unanswered source = %#v/%v; want the pending interrupt", response, err)
	}
	if response, err := closeChild(); !failedPrecondition(err, "child close interrupt is still pending") || response != nil {
		t.Fatalf("close with an unanswered source = %#v/%v; want the pending interrupt", response, err)
	}
	if envelope, err := declareMail(); !errors.Is(err, rollback) || envelope.TargetThreadID != mailChildID {
		t.Fatalf("mail from an unanswered source = %#v/%v; want a declared envelope", envelope, err)
	}

	answerToolUseForRelationTest(t, f.admin, scope, controlID)
	answerToolUseForRelationTest(t, f.admin, scope, mailID)
	if response, err := await(); !failedPrecondition(err, "child_control_source_terminal") || response != nil {
		t.Fatalf("await with an answered source = %#v/%v; want the terminal source", response, err)
	}
	if response, err := closeChild(); !failedPrecondition(err, "child_control_source_terminal") || response != nil {
		t.Fatalf("close with an answered source = %#v/%v; want the terminal source", response, err)
	}
	if envelope, err := declareMail(); !failedPrecondition(err, "agent mail source Tool Use is terminal") || envelope.TargetThreadID != "" {
		t.Fatalf("mail from an answered source = %#v/%v; want the terminal source", envelope, err)
	}
	var childStatus string
	var closeReceipts, sentMail int
	if err := f.admin.QueryRowContext(f.ctx, `SELECT
		(SELECT status FROM session_threads WHERE workspace_id=$1 AND session_id=$2 AND id=$3),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id=$1 AND session_id=$2 AND operation=$4),
		(SELECT count(*) FROM session_events WHERE workspace_id=$1 AND session_id=$2 AND type='agent.thread_message_sent')`,
		scope.WorkspaceId, scope.SessionId, childID, bridgeOpCloseChildControl).Scan(&childStatus, &closeReceipts, &sentMail); err != nil {
		t.Fatalf("read child-control effects: %v", err)
	}
	if childStatus == "closed_for_runtime" || closeReceipts != 0 || sentMail != 0 {
		t.Fatalf("child status %s, close receipts %d, sent mail %d; want the child open and no receipt or mail", childStatus, closeReceipts, sentMail)
	}
}

// A child's frozen prefix includes a Pod-lost Assistant message only when its
// request's Tool Calls are complete. Completeness is read from the relation:
// a Tool Use answered through tool_use_event_id, or the invalid-tool repair
// recognized by its call-ID column. An unanswered Tool Use excludes the
// message.
func TestPostgreSQLPodLostPrefixEligibilityFollowsToolRelation(t *testing.T) {
	f := newContentDeclarationFixture(t)
	call := func(id string) string {
		return `{"type":"tool_call","modelToolCallId":"` + id + `","toolName":"Read","canonicalInput":{}}`
	}
	result := func(id string) string {
		return `{"type":"tool_result","modelToolCallId":"` + id + `","result":{"type":"error","error":{"type":"runtime_pod_lost","message":"lost","retryable":false}}}`
	}
	for i, message := range []struct{ request, parts string }{
		{"mreq_prefix_answered", call("call_prefix_answered") + "," + result("call_prefix_answered")},
		{"mreq_prefix_unanswered", call("call_prefix_unanswered")},
		{"mreq_prefix_repaired", call("call_prefix_repaired") + "," + result("call_prefix_repaired")},
	} {
		if _, err := f.admin.ExecContext(f.ctx, `INSERT INTO session_messages (
			workspace_id,session_id,session_thread_id,message_id,sequence,kind,data_json,model_request_id,created_at,updated_at
		) VALUES ($1,$2,$3,$4,$5,'assistant',$6,$7,now(),now())`, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId,
			"msg_"+message.request, i+1, `{"parts":[`+message.parts+`]}`, message.request); err != nil {
			t.Fatalf("seed prefix message: %v", err)
		}
		seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "evt_end_"+message.request,
			sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId),
			"span.model_request_end", `{"is_error":true,"error_kind":"runtime_pod_lost"}`)
		if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_events SET model_request_id=$2 WHERE event_id=$1`, "evt_end_"+message.request, message.request); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []string{"mreq_prefix_answered", "mreq_prefix_unanswered"} {
		toolUse := "evt_use_" + request
		seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, toolUse,
			sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId),
			"agent.tool_use", `{"name":"Read","input":{}}`)
		if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_events SET model_request_id=$2 WHERE event_id=$1`, toolUse, request); err != nil {
			t.Fatal(err)
		}
	}
	answerToolUseForRelationTest(t, f.admin, f.scope, "evt_use_mreq_prefix_answered")
	seedBridgeAPIEvent(t, f.admin, f.scope.WorkspaceId, f.scope.SessionId, f.scope.SessionThreadId, "evt_repair_prefix",
		sessionfixture.NextBridgeAPIEventSequenceForTest(t, f.admin, f.scope.SessionId, f.scope.SessionThreadId),
		"agent.tool_result", `{"type":"agent.tool_result","model_tool_call_id":"call_prefix_repaired","tool_name":"Read","repair_kind":"invalid_tool"}`)
	if _, err := f.admin.ExecContext(f.ctx, `UPDATE session_events SET model_request_id='mreq_prefix_repaired' WHERE event_id='evt_repair_prefix'`); err != nil {
		t.Fatal(err)
	}
	var sequences []int64
	if err := dbconnect.NewClientForTesting(f.workload.DB).WithWorkspaceTx(f.ctx, f.scope.WorkspaceId, "test.pod_lost_prefix", func(tx *dbconnect.Tx) error {
		entries, _, err := loadDurablePrefixEntriesThroughTx(f.ctx, tx, f.scope, 3)
		for _, entry := range entries {
			sequences = append(sequences, entry.MessageSequence)
		}
		return err
	}); err != nil {
		t.Fatalf("load frozen prefix: %v", err)
	}
	if !slices.Equal(sequences, []int64{1, 3}) {
		t.Fatalf("Pod-lost prefix messages = %v; want the answered and repaired requests only", sequences)
	}
}
