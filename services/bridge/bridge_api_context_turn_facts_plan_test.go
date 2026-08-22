package agentruntimebridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tetral-ai/tetral/internal/storage/storagetest"
)

type closedTurnPlanStats struct {
	sharedBlocks      float64
	maxRows           float64
	maxLoops          float64
	sessionEventScans int
	sessionEventSeq   int
	latestCloseIndex  bool
	latestCloseAgg    bool
	latestCloseLimit  bool
}

func TestClosedTurnFactPlansStayBoundedAboveCompactionFloor(t *testing.T) {
	_, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
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
	for _, historySize := range []int{64, 8192} {
		sessionID := fmt.Sprintf("sesn_closed_plan_%d", historySize)
		threadID := fmt.Sprintf("thr_closed_plan_%d", historySize)
		seedBridgeAPISession(t, admin, "default", sessionID, threadID)
		floor := int64(historySize + 1)
		seedClosedTurnPlanHistory(t, admin, sessionID, threadID, historySize)
		if _, err := admin.ExecContext(context.Background(), `ANALYZE session_events`); err != nil {
			t.Fatalf("analyze closed turn history %d: %v", historySize, err)
		}

		openPlan := explainClosedTurnPlan(t, admin, loadOpenDurableTurnIDSQL,
			"default", sessionID, threadID, floor)
		turnPlan := explainClosedTurnPlan(t, admin, loadContextTurnEventsSQL,
			"default", sessionID, threadID, floor, "", `[]`, `[]`, "closed_for_runtime")
		combined := collectClosedTurnPlanStats(openPlan)
		mergeClosedTurnPlanStats(&combined, collectClosedTurnPlanStats(turnPlan))
		combined.latestCloseIndex = planSubtreeUsesIndex(openPlan, "CTE latest_close")
		combined.latestCloseAgg = planSubtreeHasNodeType(openPlan, "CTE latest_close", "Aggregate")
		combined.latestCloseLimit = planSubplanRootType(openPlan, "CTE latest_close") == "Limit"
		if combined.sessionEventSeq != 0 || combined.sessionEventScans == 0 ||
			combined.maxLoops > 1 || combined.maxRows > 12 || !combined.latestCloseIndex || combined.latestCloseAgg || !combined.latestCloseLimit {
			t.Fatalf("closed turn plan %d is not bounded: %#v\nopen=%s\nturn=%s",
				historySize, combined, encodePlanForFailure(openPlan), encodePlanForFailure(turnPlan))
		}
		if !planUsesCompactionFloor(openPlan, floor) || !planUsesCompactionFloor(turnPlan, floor) {
			t.Fatalf("closed turn plan %d does not carry the compaction floor", historySize)
		}
		stats = append(stats, combined)
	}
	if delta := stats[1].sharedBlocks - stats[0].sharedBlocks; delta > 8 {
		t.Fatalf("8192-row closed history used %.0f additional shared blocks; want at most 8 (small=%#v large=%#v)",
			delta, stats[0], stats[1])
	}
}

func seedClosedTurnPlanHistory(t *testing.T, db *sql.DB, sessionID, threadID string, historySize int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id, session_id, session_thread_id, event_id, sequence, type, payload_json,
		visibility, session_visible, runtime_write_id, model_request_id, projection_json,
		created_at, updated_at, processed_at
	)
	SELECT 'default', $1, $2, 'evt_closed_plan_old_' || $3 || '_' || value, value,
	       'session.thread_status_idle', '{"type":"session.thread_status_idle"}',
	       'internal', false, 'rwrite_closed_plan_old_' || $3 || '_' || value, NULL, '{}', now(), now(), now()
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
	('default',$1,$2,'evt_closed_plan_tool_'||$4,$3+2,'agent.tool_use','{}','internal',false,'rwrite_closed_plan_tool_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_result_'||$4,$3+3,'agent.tool_result','{}','internal',false,'rwrite_closed_plan_result_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_end_'||$4,$3+4,'span.model_request_end','{"model_request_start_id":"evt_closed_plan_start_'||$4||'","is_error":false,"provider_context_retention":{"disposition":"none","tool_use_event_ids":[],"repair_event_ids":[]}}','internal',false,'rwrite_closed_plan_end_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_idle_'||$4,$3+5,'session.thread_status_idle','{"type":"session.thread_status_idle"}','internal',false,'rwrite_closed_plan_idle_'||$4,NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_close_'||$4,$3+6,'session.thread_status_idle','{"type":"session.thread_status_idle"}','internal',false,'rwrite_closed_plan_close_'||$4,NULL,'{}',now(),now(),now()),
	('default',$1,$2,'evt_closed_plan_tail_'||$4,$3+7,'agent.message','{}','internal',false,'rwrite_closed_plan_tail_'||$4,'mreq_closed_plan_'||$4,'{}',now(),now(),now())`,
		sessionID, threadID, floor, fmt.Sprint(historySize)); err != nil {
		t.Fatalf("seed post-floor closed turn: %v", err)
	}
}

func explainClosedTurnPlan(t *testing.T, db *sql.DB, query string, args ...any) map[string]any {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(context.Background(),
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

func planSubtreeUsesIndex(plan map[string]any, subplanName string) bool {
	if name, _ := plan["Subplan Name"].(string); name == subplanName {
		found := false
		walkPostgreSQLPlan(plan, func(node map[string]any) {
			if indexName, _ := node["Index Name"].(string); indexName != "" {
				found = true
			}
		})
		return found
	}
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		childPlan, _ := child.(map[string]any)
		if childPlan != nil && planSubtreeUsesIndex(childPlan, subplanName) {
			return true
		}
	}
	return false
}

func planSubtreeHasNodeType(plan map[string]any, subplanName, nodeType string) bool {
	if name, _ := plan["Subplan Name"].(string); name == subplanName {
		found := false
		walkPostgreSQLPlan(plan, func(node map[string]any) {
			if current, _ := node["Node Type"].(string); current == nodeType {
				found = true
			}
		})
		return found
	}
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		childPlan, _ := child.(map[string]any)
		if childPlan != nil && planSubtreeHasNodeType(childPlan, subplanName, nodeType) {
			return true
		}
	}
	return false
}

func planSubplanRootType(plan map[string]any, subplanName string) string {
	if name, _ := plan["Subplan Name"].(string); name == subplanName {
		nodeType, _ := plan["Node Type"].(string)
		return nodeType
	}
	children, _ := plan["Plans"].([]any)
	for _, child := range children {
		childPlan, _ := child.(map[string]any)
		if childPlan == nil {
			continue
		}
		if nodeType := planSubplanRootType(childPlan, subplanName); nodeType != "" {
			return nodeType
		}
	}
	return ""
}

func planUsesCompactionFloor(plan map[string]any, floor int64) bool {
	floorText := fmt.Sprintf("sequence >= '%d'", floor)
	found := false
	walkPostgreSQLPlan(plan, func(node map[string]any) {
		condition, _ := node["Index Cond"].(string)
		if strings.Contains(condition, floorText) {
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
	target.latestCloseIndex = target.latestCloseIndex || other.latestCloseIndex
}

func encodePlanForFailure(plan map[string]any) string {
	raw, _ := json.Marshal(plan)
	return string(raw)
}
