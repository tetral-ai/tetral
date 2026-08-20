package agentruntimebridge

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	bridgev1 "github.com/tetral-ai/tetral/services/bridge/gen/tetral/bridge/v1"
)

func TestPostgreSQLAtomicSubagentCreationCommitsAndReplaysWholeInitialLineage(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_subagent"
		parentID  = "thr_atomic_subagent_parent"
		bindingID = "bind_atomic_subagent"
		podUID    = "pod_atomic_subagent"
		prompt    = "perform the opening delegated task"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_atomic_subagent", "call_atomic_subagent", "spawn_agent",
		`{"task_name":"provider-owned-name","agent_type":"general","fork_turns":"none","prompt":"provider-owned-prompt"}`)
	request := &bridgev1.CreateSubagentThreadRequest{
		Scope: scope, SourceToolUseEventId: toolUseID, TaskName: "runtime-worker", AgentType: "worker", ForkTurns: "all", InitialPrompt: prompt,
	}
	created, err := store.CreateSubagentThread(context.Background(), request)
	if err != nil || created.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("atomic child creation = %#v/%v", created, err)
	}
	childID := created.GetCommitted().GetChildThreadId()
	assertAtomicSubagentLineage(t, admin, sessionID, parentID, childID, toolUseID, prompt, 1)

	replayed, err := store.CreateSubagentThread(context.Background(), request)
	if err != nil || replayed.GetDuplicate().GetChildThreadId() != childID {
		t.Fatalf("atomic child replay = %#v/%v; want %s", replayed, err, childID)
	}
	assertAtomicSubagentLineage(t, admin, sessionID, parentID, childID, toolUseID, prompt, 1)

	conflict := *request
	conflict.InitialPrompt = "different opening input"
	if response, err := store.CreateSubagentThread(context.Background(), &conflict); status.Code(err) != codes.AlreadyExists || response != nil {
		t.Fatalf("conflicting atomic replay = %#v/%v; want AlreadyExists", response, err)
	}
}

func TestPostgreSQLAtomicSubagentCreationRollsBackEveryOwnedFact(t *testing.T) {
	runtimeDB, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	const (
		sessionID = "sesn_atomic_subagent_rollback"
		parentID  = "thr_atomic_subagent_rollback_parent"
		bindingID = "bind_atomic_subagent_rollback"
		podUID    = "pod_atomic_subagent_rollback"
	)
	seedBridgeAPISession(t, admin, "default", sessionID, parentID)
	seedBridgeAPIRuntimeBinding(t, admin, "default", sessionID, bindingID, 1, podUID)
	store := NewPostgreSQLBridgeAPIStore(dbconnect.NewClientForTesting(runtimeDB))
	scope := bridgeAPIScope(sessionID, parentID, bindingID, 1, podUID)
	toolUseID := writeDurableOrdinaryToolUseForTest(t, store, scope, "mreq_atomic_subagent_rollback", "call_atomic_subagent_rollback", "spawn_agent",
		`{"task_name":"rollback-worker","prompt":"rollback opening"}`)
	if _, err := admin.ExecContext(context.Background(), `CREATE FUNCTION fail_atomic_subagent_queue() RETURNS trigger AS $$
		BEGIN
			IF NEW.payload_json::jsonb->>'session_id' = 'sesn_atomic_subagent_rollback' THEN
				RAISE EXCEPTION 'injected atomic subagent Queue failure';
			END IF;
			RETURN NEW;
		END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_atomic_subagent_queue BEFORE INSERT ON queue_jobs
		FOR EACH ROW EXECUTE FUNCTION fail_atomic_subagent_queue()`); err != nil {
		t.Fatalf("install atomic rollback trigger: %v", err)
	}
	request := &bridgev1.CreateSubagentThreadRequest{
		Scope: scope, SourceToolUseEventId: toolUseID, TaskName: "rollback-worker", AgentType: "worker", ForkTurns: "all", InitialPrompt: "rollback opening",
	}
	if response, err := store.CreateSubagentThread(context.Background(), request); err == nil || response != nil {
		t.Fatalf("injected atomic child failure = %#v/%v; want rollback", response, err)
	}
	assertAtomicSubagentLineage(t, admin, sessionID, parentID, "", toolUseID, "", 0)
	if _, err := admin.ExecContext(context.Background(), `DROP TRIGGER fail_atomic_subagent_queue ON queue_jobs; DROP FUNCTION fail_atomic_subagent_queue()`); err != nil {
		t.Fatalf("remove atomic rollback trigger: %v", err)
	}
	created, err := store.CreateSubagentThread(context.Background(), request)
	if err != nil || created.GetCommitted().GetChildThreadId() == "" {
		t.Fatalf("atomic child after rollback = %#v/%v", created, err)
	}
	assertAtomicSubagentLineage(t, admin, sessionID, parentID, created.GetCommitted().GetChildThreadId(), toolUseID, "rollback opening", 1)
}

func assertAtomicSubagentLineage(t *testing.T, admin interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, sessionID, parentID, childID, toolUseID, prompt string, want int) {
	t.Helper()
	var children, prefixes, sent, received, inbox, queueJobs, operations int
	var receipt, storedContent string
	err := admin.QueryRowContext(context.Background(), `SELECT
		(SELECT count(*) FROM session_threads WHERE workspace_id='default' AND session_id=$1 AND parent_thread_id=$2 AND role='subagent'),
		(SELECT count(*) FROM session_thread_context_prefixes WHERE workspace_id='default' AND session_id=$1),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_sent' AND payload_json::jsonb->>'source_tool_use_event_id'=$3),
		(SELECT count(*) FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received' AND payload_json::jsonb->>'source_tool_use_event_id'=$3),
		(SELECT count(*) FROM session_runtime_inbox WHERE workspace_id='default' AND session_id=$1 AND input_kind='agent_mail'),
		(SELECT count(*) FROM queue_jobs WHERE workspace_id='default' AND payload_json::jsonb->>'session_id'=$1 AND payload_json::jsonb->>'input_kind'='agent_mail'),
		(SELECT count(*) FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),
		COALESCE((SELECT result_json FROM session_bridge_operations WHERE workspace_id='default' AND session_id=$1 AND operation='create_child_thread' AND source_kind='subagent_spawn'),''),
		COALESCE((SELECT payload_json::jsonb->'message'->'content'->0->>'text' FROM session_events WHERE workspace_id='default' AND session_id=$1 AND type='agent.thread_message_received'), '')`,
		sessionID, parentID, toolUseID).Scan(&children, &prefixes, &sent, &received, &inbox, &queueJobs, &operations, &receipt, &storedContent)
	if err != nil {
		t.Fatalf("read atomic child lineage: %v", err)
	}
	if children != want || prefixes != want || sent != want || received != want || inbox != want || queueJobs != want || operations != want {
		t.Fatalf("atomic lineage children/prefix/sent/received/inbox/queue/receipt = %d/%d/%d/%d/%d/%d/%d; want all %d",
			children, prefixes, sent, received, inbox, queueJobs, operations, want)
	}
	if want == 1 && (!strings.Contains(receipt, childID) || storedContent != prompt) {
		t.Fatalf("atomic lineage receipt/content = %s/%q; want child %s prompt %q", receipt, storedContent, childID, prompt)
	}
}
