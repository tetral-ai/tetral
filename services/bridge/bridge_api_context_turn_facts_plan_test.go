package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

type closedTurnPlanStats struct {
	sharedBlocks      float64
	maxRows           float64
	maxLoops          float64
	sessionEventScans int
	sessionEventSeq   int
}

type loadContextQueryInvocation struct {
	SQL  string
	Args []any
}

type loadContextQueryTracer struct {
	mu          sync.Mutex
	invocations []loadContextQueryInvocation
}

func (tracer *loadContextQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !strings.Contains(data.SQL, "session_events") {
		return ctx
	}
	tracer.mu.Lock()
	tracer.invocations = append(tracer.invocations, loadContextQueryInvocation{
		SQL:  data.SQL,
		Args: append([]any(nil), data.Args...),
	})
	tracer.mu.Unlock()
	return ctx
}

func (*loadContextQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *loadContextQueryTracer) snapshot() []loadContextQueryInvocation {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	result := make([]loadContextQueryInvocation, len(tracer.invocations))
	copy(result, tracer.invocations)
	return result
}

func TestClosedTurnFactPlansStayBoundedAcrossRetainedHistory(t *testing.T) {
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedBridgeAPISession(t, admin, "default", "sesn_closed_plan_background", "thr_closed_plan_background")
	if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, projection_json, created_at, updated_at, processed_at
	)
	SELECT 'default', 'sesn_closed_plan_background', 'thr_closed_plan_background',
	       'evt_closed_plan_background_' || value, value, 'agent.message', '{}',
	       'internal', false, 'rwrite_closed_plan_background_' || value, '{}', now(), now(), now()
	  FROM generate_series(1, 16384) value`); err != nil {
		t.Fatalf("seed plan background: %v", err)
	}
	stats := make([]closedTurnPlanStats, 0, 2)
	var wantStatementStats []closedTurnPlanStats
	var wantCensus []string
	var wantFactIDs []string
	for _, historySize := range []int{64, 8192} {
		sessionID := fmt.Sprintf("sesn_closed_plan_%d", historySize)
		threadID := fmt.Sprintf("thr_closed_plan_%d", historySize)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		bindingID := fmt.Sprintf("bind_closed_plan_%d", historySize)
		podUID := fmt.Sprintf("pod_closed_plan_%d", historySize)
		seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
		floor := int64(historySize + 1)
		seedClosedTurnPlanHistory(t, admin, sessionID, threadID, historySize)
		seedClosedTurnPostFloorNoise(t, admin, sessionID, threadID, floor, historySize)
		if _, err := admin.ExecContext(context.Background(), `UPDATE session_threads
			SET status='closed_for_runtime', closed_at=now(), updated_at=now()
			WHERE workspace_id='default' AND session_id=$1 AND id=$2`, sessionID, threadID); err != nil {
			t.Fatalf("close plan thread %d: %v", historySize, err)
		}
		if _, err := admin.ExecContext(context.Background(), `INSERT INTO session_messages (
			workspace_id, session_id, session_thread_id, message_id, sequence, kind, data_json,
			source_event_id, model_request_id, created_at, updated_at
		) VALUES
		('default',$1,$2,$3,1,'compaction','{"parts":[{"type":"text","text":"summary"}]}',$4,NULL,now(),now()),
		('default',$1,$2,'msg_closed_plan_assistant_'||$5,2,'assistant','{"parts":[{"type":"text","text":"retained tool pair"}]}',NULL,'mreq_closed_plan_'||$5,now(),now())`,
			sessionID, threadID, fmt.Sprintf("msg_closed_plan_%d", historySize), fmt.Sprintf("evt_closed_plan_compacted_%d", historySize), fmt.Sprint(historySize)); err != nil {
			t.Fatalf("seed plan compaction Message %d: %v", historySize, err)
		}
		if _, err := admin.ExecContext(context.Background(), `ANALYZE session_events`); err != nil {
			t.Fatalf("analyze closed turn history %d: %v", historySize, err)
		}

		openPlan := explainClosedTurnPlan(t, runtime, loadOpenDurableTurnIDSQL,
			"default", sessionID, threadID)
		turnPlan := explainClosedTurnPlan(t, runtime, loadContextTurnEventsSQL,
			"default", sessionID, threadID, floor, "", `[]`, `[]`, "closed_for_runtime")
		compactionPlan := explainClosedTurnPlan(t, runtime, loadContextCompactionEventFloorSQL,
			"default", sessionID, threadID, fmt.Sprintf(`["evt_closed_plan_compacted_%d"]`, historySize))
		combined := collectClosedTurnPlanStats(openPlan)
		mergeClosedTurnPlanStats(&combined, collectClosedTurnPlanStats(turnPlan))
		mergeClosedTurnPlanStats(&combined, collectClosedTurnPlanStats(compactionPlan))
		requiredSubplans := []struct {
			plan            map[string]any
			name            string
			requiredIndexes []string
			requireFloor    bool
		}{
			{openPlan, "CTE latest_running", []string{"idx_session_events_thread_running_sequence"}, false},
			{turnPlan, "CTE latest_thread_request_start", []string{"idx_session_events_thread_type_sequence", "idx_session_events_thread_request_type"}, true},
			{turnPlan, "CTE previous_close_before_request", []string{"idx_session_events_thread_close_sequence"}, false},
			{turnPlan, "CTE selected_thread_running", []string{"idx_session_events_thread_running_sequence"}, false},
			{turnPlan, "CTE selected_thread_request_end", []string{"idx_session_events_thread_type_sequence", "idx_session_events_thread_request_type"}, false},
			{turnPlan, "CTE selected_thread_latest_idle", []string{"idx_session_events_thread_close_sequence"}, false},
			{turnPlan, "CTE current_reschedule", []string{"idx_session_events_thread_type_sequence", "idx_session_events_thread_request_type"}, false},
		}
		for _, required := range requiredSubplans {
			assertNamedSessionEventPlanBound(t, required.plan, required.name,
				required.requiredIndexes, required.requireFloor, floor)
		}
		assertSessionEventPlanBound(t, compactionPlan, "compaction", 4, 1)
		assertPlanUsesIndexes(t, compactionPlan, "compaction",
			"idx_session_events_thread_type_sequence", "idx_session_events_thread_request_type")
		floorFiltered := planRowsRemovedByFloorFilter(turnPlan, floor)
		if combined.sessionEventSeq != 0 || combined.sessionEventScans == 0 ||
			combined.maxLoops > 1 || combined.maxRows > 12 || floorFiltered != 0 {
			t.Fatalf("closed turn plan %d is not bounded (floor-filtered=%.0f): %#v\nopen=%s\nturn=%s\ncompaction=%s",
				historySize, floorFiltered, combined, encodePlanForFailure(openPlan), encodePlanForFailure(turnPlan), encodePlanForFailure(compactionPlan))
		}
		tracedDB, tracer := openLoadContextTracedDB(t, runtime)
		store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(tracedDB))
		store.RuntimeBindingTokenHMACKey = []byte("closed-turn-plan-key")
		loaded, err := store.LoadContext(context.Background(), &bridgev1.LoadContextRequest{
			Scope: bridgeAPIScope(sessionID, threadID, bindingID, 1, podUID),
		})
		if err != nil {
			t.Fatalf("full LoadContext %d: %v", historySize, err)
		}
		var payload bridgeLoadContextPayload
		if err := json.Unmarshal([]byte(loaded.GetContextJson()), &payload); err != nil {
			t.Fatalf("decode full LoadContext %d: %v", historySize, err)
		}
		factIDs := make([]string, 0, len(payload.TurnFacts.Events))
		retainedToolUses := 0
		retainedToolResults := 0
		for _, event := range payload.TurnFacts.Events {
			factIDs = append(factIDs, strings.TrimSuffix(event.EventID, fmt.Sprint(historySize)))
			if event.ToolUse != nil {
				retainedToolUses++
			}
			if event.ToolResult != nil {
				retainedToolResults++
			}
		}
		if retainedToolUses != 1 || retainedToolResults != 1 {
			t.Fatalf("full LoadContext retained Tool pair = uses:%d results:%d; want 1/1", retainedToolUses, retainedToolResults)
		}
		invocations := tracer.snapshot()
		census := make([]string, 0, len(invocations))
		statementStats := make([]closedTurnPlanStats, 0, len(invocations))
		toolResultIdentityLookups := 0
		for _, invocation := range invocations {
			normalizedSQL := strings.Join(strings.Fields(invocation.SQL), " ")
			census = append(census, normalizedSQL)
			if strings.HasPrefix(normalizedSQL, "SELECT COALESCE(model_request_id, ''), payload_json, projection_json FROM session_events") {
				toolResultIdentityLookups++
			}
			if strings.HasPrefix(normalizedSQL, "SELECT projection_json FROM session_events") &&
				strings.Contains(normalizedSQL, "session.status_rescheduled") {
				t.Fatalf("full LoadContext performed a per-Request-End reschedule lookup: %s", normalizedSQL)
			}
			plan := explainClosedTurnPlan(t, runtime, invocation.SQL, invocation.Args...)
			planStats := collectClosedTurnPlanStats(plan)
			if planStats.sessionEventSeq != 0 || planStats.maxLoops > 1 || planStats.maxRows > 12 {
				t.Fatalf("full LoadContext statement is unbounded at history %d: %#v sql=%s plan=%s",
					historySize, planStats, census[len(census)-1], encodePlanForFailure(plan))
			}
			statementStats = append(statementStats, planStats)
		}
		if toolResultIdentityLookups != 1 {
			t.Fatalf("full LoadContext Tool Result identity lookups = %d; want one for one retained pair", toolResultIdentityLookups)
		}
		if historySize == 64 {
			wantCensus = census
			wantFactIDs = factIDs
			wantStatementStats = statementStats
		} else if fmt.Sprint(census) != fmt.Sprint(wantCensus) || fmt.Sprint(factIDs) != fmt.Sprint(wantFactIDs) {
			t.Fatalf("full LoadContext changed with pre-floor history: census=%d/%d facts=%v/%v",
				len(wantCensus), len(census), wantFactIDs, factIDs)
		} else {
			for index, statement := range statementStats {
				baseline := wantStatementStats[index]
				if statement.sessionEventScans != baseline.sessionEventScans ||
					statement.maxRows != baseline.maxRows || statement.maxLoops != baseline.maxLoops ||
					statement.sharedBlocks-baseline.sharedBlocks > 24 {
					t.Fatalf("full LoadContext statement %d grew with retained history: small=%#v large=%#v sql=%s",
						index, baseline, statement, census[index])
				}
			}
		}
		stats = append(stats, combined)
	}
	if delta := stats[1].sharedBlocks - stats[0].sharedBlocks; delta > 24 {
		t.Fatalf("8192-row closed history used %.0f additional shared blocks; want at most 24 (small=%#v large=%#v)",
			delta, stats[0], stats[1])
	}
}

func openLoadContextTracedDB(t *testing.T, runtime *sql.DB) (*sql.DB, *loadContextQueryTracer) {
	t.Helper()
	tracer := &loadContextQueryTracer{}
	return storagetest.OpenRuntimeRoleDBWithTracer(t, runtime, tracer), tracer
}

func planRowsRemovedByFloorFilter(plan map[string]any, floor int64) float64 {
	var removed float64
	floorText := fmt.Sprintf("sequence >= '%d'", floor)
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if relation, _ := node["Relation Name"].(string); relation != "session_events" {
			return
		}
		filter, _ := node["Filter"].(string)
		indexCond, _ := node["Index Cond"].(string)
		if !strings.Contains(filter, floorText) || strings.Contains(indexCond, floorText) {
			return
		}
		rows, _ := node["Rows Removed by Filter"].(float64)
		removed += rows
	})
	return removed
}

func seedClosedTurnPlanHistory(t *testing.T, db *sql.DB, sessionID, threadID string, historySize int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	)
	SELECT 'default', $1, $2, 'evt_closed_plan_old_' || $3 || '_' || value, value,
	       'span.model_request_start',
	       '{"type":"span.model_request_start","model_request_id":"mreq_closed_plan_old_' || $3 || '_' || value || '"}',
	       'internal', false, 'rwrite_closed_plan_old_' || $3 || '_' || value,
	       'mreq_closed_plan_old_' || $3 || '_' || value,
	       '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}', now(), now(), now()
	  FROM generate_series(1, $3) value`, sessionID, threadID, historySize); err != nil {
		t.Fatalf("seed %d pre-floor events: %v", historySize, err)
	}
	floor := int64(historySize + 1)
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	) VALUES
	('default',$1,$2,'evt_closed_plan_running_'||$4,$3,'session.thread_status_running','{"type":"session.thread_status_running"}','internal',false,'rwrite_closed_plan_running_'||$4,NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_start_'||$4,$3+1,'span.model_request_start','{"type":"span.model_request_start","model_request_id":"mreq_closed_plan_'||$4||'"}','internal',false,'rwrite_closed_plan_start_'||$4,'mreq_closed_plan_'||$4,'{"context_through_message_sequence":1,"request_kind":"agent_provider_request"}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_tool_'||$4,$3+2,'agent.tool_use','{"type":"agent.tool_use"}','internal',false,'rwrite_closed_plan_tool_'||$4,'mreq_closed_plan_'||$4,'{"model_tool_call_id":"call_closed_plan_'||$4||'","tool_name":"Bash"}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_result_'||$4,$3+3,'agent.tool_result','{"type":"agent.tool_result","tool_use_event_id":"evt_closed_plan_tool_'||$4||'"}','internal',false,'rwrite_closed_plan_result_'||$4,'mreq_closed_plan_'||$4,'{"state":"completed"}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_end_'||$4,$3+4,'span.model_request_end','{"model_request_start_id":"evt_closed_plan_start_'||$4||'","is_error":false,"provider_context_retention":{"disposition":"completed","assistant_message_sequence":2,"tool_use_event_ids":["evt_closed_plan_tool_'||$4||'"],"repair_event_ids":[]}}','internal',false,'rwrite_closed_plan_end_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_idle_'||$4,$3+5,'session.thread_status_idle','{"type":"session.thread_status_idle","stop_reason":{"type":"end_turn"}}','internal',false,'rwrite_closed_plan_idle_'||$4,NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_close_'||$4,$3+6,'session.thread_status_idle','{"type":"session.thread_status_idle","stop_reason":{"type":"end_turn"}}','internal',false,'rwrite_closed_plan_close_'||$4,NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_tail_'||$4,$3+7,'agent.message','{}','internal',false,'rwrite_closed_plan_tail_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_compacted_'||$4,$3+8,'agent.thread_context_compacted','{}','internal',false,'rwrite_closed_plan_compacted_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now())`,
		sessionID, threadID, floor, fmt.Sprint(historySize)); err != nil {
		t.Fatalf("seed post-floor closed turn: %v", err)
	}
}

func seedClosedTurnPostFloorNoise(t *testing.T, db *sql.DB, sessionID, threadID string, floor int64, historySize int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	)
	SELECT 'default', $1, $2, 'evt_closed_plan_noise_' || $4 || '_' || value,
	       $3 + 100 + value, 'agent.tool_result',
	       '{"type":"agent.tool_result","tool_use_id":"evt_unrelated_tool_' || $4 || '_' || value || '","content":[]}',
	       'internal', false, 'rwrite_closed_plan_noise_' || $4 || '_' || value,
	       'mreq_closed_plan_noise_' || $4 || '_' || value,
	       '{"context_through_message_sequence":0,"request_kind":"agent_provider_request"}',
	       now(), now(), now()
	  FROM generate_series(1, $4) value`, sessionID, threadID, floor, historySize); err != nil {
		t.Fatalf("seed %d post-floor irrelevant events: %v", historySize, err)
	}
}

func explainClosedTurnPlan(t *testing.T, db *sql.DB, query string, args ...any) map[string]any {
	t.Helper()
	if len(args) == 0 {
		t.Fatal("closed turn plan requires workspace identity")
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin closed turn plan transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(context.Background(), `SELECT set_config('tetral.workspace_id',$1,true)`, fmt.Sprint(args[0])); err != nil {
		t.Fatalf("set closed turn plan workspace: %v", err)
	}
	var raw string
	if err := tx.QueryRowContext(context.Background(),
		"EXPLAIN (ANALYZE, BUFFERS, COSTS OFF, TIMING OFF, SUMMARY OFF, FORMAT JSON) "+query,
		args...).Scan(&raw); err != nil {
		t.Fatalf("explain closed turn query: %v", err)
	}
	var documents []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(raw), &documents); err != nil || len(documents) != 1 {
		t.Fatalf("decode closed turn plan: %v raw=%s", err, raw)
	}
	return documents[0].Plan
}

func collectClosedTurnPlanStats(plan map[string]any) closedTurnPlanStats {
	var stats closedTurnPlanStats
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if relation, _ := node["Relation Name"].(string); relation != "session_events" {
			return
		}
		stats.sessionEventScans++
		nodeType, _ := node["Node Type"].(string)
		if nodeType == "Seq Scan" {
			stats.sessionEventSeq++
		}
		rows, _ := node["Actual Rows"].(float64)
		loops, _ := node["Actual Loops"].(float64)
		stats.maxRows = max(stats.maxRows, rows)
		stats.maxLoops = max(stats.maxLoops, loops)
		sharedHits, _ := node["Shared Hit Blocks"].(float64)
		sharedReads, _ := node["Shared Read Blocks"].(float64)
		stats.sharedBlocks += sharedHits + sharedReads
	})
	return stats
}

func findNamedPlan(plan map[string]any, subplanName string) map[string]any {
	if name, _ := plan["Subplan Name"].(string); name == subplanName {
		return plan
	}
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		childPlan, _ := child.(map[string]any)
		if childPlan == nil {
			continue
		}
		if found := findNamedPlan(childPlan, subplanName); found != nil {
			return found
		}
	}
	return nil
}

func assertNamedSessionEventPlanBound(
	t *testing.T,
	plan map[string]any,
	subplanName string,
	requiredIndexes []string,
	requireFloor bool,
	floor int64,
) {
	t.Helper()
	subplan := findNamedPlan(plan, subplanName)
	if subplan == nil {
		t.Fatalf("missing owning subplan %s: %s", subplanName, encodePlanForFailure(plan))
	}
	assertSessionEventPlanBound(t, subplan, subplanName, 12, 1)
	assertPlanUsesAnyIndex(t, subplan, subplanName, requiredIndexes...)
	if requireFloor && !planUsesFloor(subplan, floor) {
		t.Fatalf("owning subplan %s does not apply compaction floor %d: %s",
			subplanName, floor, encodePlanForFailure(subplan))
	}
}

func assertSessionEventPlanBound(
	t *testing.T,
	plan map[string]any,
	owner string,
	maxRows float64,
	maxLoops float64,
) {
	t.Helper()
	scans := 0
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if relation, _ := node["Relation Name"].(string); relation != "session_events" {
			return
		}
		scans++
		if !planHasIndex(node) {
			t.Fatalf("owner %s has a session_events scan without its own index: %s",
				owner, encodePlanForFailure(node))
		}
		rows, _ := node["Actual Rows"].(float64)
		loops, _ := node["Actual Loops"].(float64)
		if rows > maxRows || loops > maxLoops {
			t.Fatalf("owner %s scan exceeded bound rows=%.0f/%.0f loops=%.0f/%.0f: %s",
				owner, rows, maxRows, loops, maxLoops, encodePlanForFailure(node))
		}
	})
	if scans == 0 {
		t.Fatalf("owner %s has no session_events scan: %s", owner, encodePlanForFailure(plan))
	}
}

func planHasIndex(plan map[string]any) bool {
	found := false
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if indexName, _ := node["Index Name"].(string); indexName != "" {
			found = true
		}
	})
	return found
}

func assertPlanUsesIndexes(t *testing.T, plan map[string]any, owner string, required ...string) {
	t.Helper()
	seen := make(map[string]bool, len(required))
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if indexName, _ := node["Index Name"].(string); indexName != "" {
			seen[indexName] = true
		}
	})
	for _, indexName := range required {
		if !seen[indexName] {
			t.Fatalf("owner %s does not use required index %s: %s",
				owner, indexName, encodePlanForFailure(plan))
		}
	}
}

func assertPlanUsesAnyIndex(t *testing.T, plan map[string]any, owner string, required ...string) {
	t.Helper()
	seen := make(map[string]bool, len(required))
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		if indexName, _ := node["Index Name"].(string); indexName != "" {
			seen[indexName] = true
		}
	})
	for _, indexName := range required {
		if seen[indexName] {
			return
		}
	}
	t.Fatalf("owner %s does not use any required index %v: %s",
		owner, required, encodePlanForFailure(plan))
}

func planUsesFloor(plan map[string]any, floor int64) bool {
	floorText := fmt.Sprintf("sequence >= '%d'", floor)
	found := false
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		indexCondition, _ := node["Index Cond"].(string)
		filter, _ := node["Filter"].(string)
		if strings.Contains(indexCondition, floorText) && !strings.Contains(filter, floorText) {
			found = true
		}
	})
	return found
}

func mergeClosedTurnPlanStats(target *closedTurnPlanStats, other closedTurnPlanStats) {
	target.sharedBlocks += other.sharedBlocks
	target.maxRows = max(target.maxRows, other.maxRows)
	target.maxLoops = max(target.maxLoops, other.maxLoops)
	target.sessionEventScans += other.sessionEventScans
	target.sessionEventSeq += other.sessionEventSeq
}

func encodePlanForFailure(plan map[string]any) string {
	raw, _ := json.Marshal(plan)
	return string(raw)
}
