package tetralsandbox

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/queue"
	sandboxdriver "github.com/tetral-ai/tetral/internal/sandbox/driver"
	"github.com/tetral-ai/tetral/internal/workspace"
	agentruntimev1 "github.com/tetral-ai/tetral/services/agent-runtime/gen/tetral/agent_runtime/v1"
	agentruntimebridge "github.com/tetral-ai/tetral/services/bridge"
	tetralqueue "github.com/tetral-ai/tetral/services/queue"
)

type runtimeInputExhaustionWorkspaceLister struct{}

func (runtimeInputExhaustionWorkspaceLister) ListIDs(context.Context) ([]workspace.ID, error) {
	return []workspace.ID{"ws_execution_store"}, nil
}

type runtimeInputExhaustionSender struct{ calls int }

func (s *runtimeInputExhaustionSender) SendRuntimeCommand(context.Context, agentruntimebridge.RuntimePodTarget, *agentruntimev1.RuntimeInputCommandRequest) (*agentruntimev1.RuntimeInputCommandResponse, error) {
	s.calls++
	return &agentruntimev1.RuntimeInputCommandResponse{Status: agentruntimev1.RuntimeCommandStatus_RUNTIME_COMMAND_STATUS_ACCEPTED}, nil
}

type runtimeInputExhaustionDeliverer struct {
	direct agentruntimebridge.RuntimePodDirectDeliverer
}

func (d runtimeInputExhaustionDeliverer) DeliverRuntimeJob(ctx context.Context, job agentruntimebridge.RuntimeJob) (agentruntimebridge.RuntimeDeliveryResult, error) {
	return d.direct.DeliverRuntimeJob(ctx, job)
}

func (d runtimeInputExhaustionDeliverer) FinalizeRuntimeDelivery(ctx context.Context, job agentruntimebridge.RuntimeJob, result agentruntimebridge.RuntimeDeliveryResult) (agentruntimebridge.RuntimeDeliveryResult, error) {
	return d.direct.FinalizeRuntimeDelivery(ctx, job, result)
}

func (d runtimeInputExhaustionDeliverer) ReplayRuntimeDeliveryFinalization(ctx context.Context, job agentruntimebridge.RuntimeJob) (agentruntimebridge.RuntimeDeliveryResult, bool, error) {
	return d.direct.ReplayRuntimeDeliveryFinalization(ctx, job)
}

func TestPostgreSQLTaskNotificationProducerAndJobRunnerTerminalizeQueuedInbox(t *testing.T) {
	runtimeDB, adminDB := newSandboxServiceTestDB(t)
	seedSandboxExecutionStoreFixture(t, adminDB)
	seedBackgroundTaskFromExecution(t, runtimeDB, adminDB)
	if _, err := adminDB.ExecContext(context.Background(), `INSERT INTO session_events (
		workspace_id,session_id,session_thread_id,event_id,sequence,type,payload_json,created_at,updated_at
	) VALUES ('ws_execution_store','sesn_execution_store','thr_execution_store','evt_execution_a',1,
		'agent.tool_use','{"type":"agent.tool_use","name":"exec_command","input":{}}',now(),now())`); err != nil {
		t.Fatalf("seed background task source event: %v", err)
	}
	client := dbconnect.NewClientForTesting(runtimeDB)
	producer := NewPostgreSQLSandboxBackgroundCommandStore(client)
	ctx := sandboxTestQueueContext(t, runtimeDB)
	work, current, err := producer.LoadReconcile(ctx, SandboxBackgroundReconcileJob{
		WorkspaceID: "ws_execution_store", SessionID: "sesn_execution_store", TaskID: "task_execution", ReconcileGeneration: 1,
	})
	if err != nil || !current {
		t.Fatalf("load background task producer state = current %t error %v", current, err)
	}
	if err := producer.SettleTask(ctx, work, sandboxdriver.CommandResult{
		TerminalStatus: "completed", ResultJSON: `{"status":"completed","result":{"stdout":"done"}}`,
	}, time.Now().UTC()); err != nil {
		t.Fatalf("settle background task producer: %v", err)
	}
	const agentConfig = `{"name":"agent","model":"anthropic/claude-opus-4-8","tools":[{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}],"skills":[],"metadata":{}}`
	const installedTools = `{"tools":[{"type":"tetral_agent_toolset","family":"claude"},{"type":"mcp_toolset","mcp_server_name":"github"}],"mcp_servers":[{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp/"}]}`
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE agent_versions SET config_json=$1
		WHERE workspace_id='ws_execution_store' AND id='agentver_env_store'`, agentConfig); err != nil {
		t.Fatalf("configure MCP agent: %v", err)
	}
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE sessions SET installed_tools_json=$1
		WHERE workspace_id='ws_execution_store' AND id='sesn_execution_store'`, installedTools); err != nil {
		t.Fatalf("configure installed MCP tools: %v", err)
	}
	if _, err := adminDB.ExecContext(context.Background(), `UPDATE queue_jobs SET max_attempts=1, available_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id='ws_execution_store' AND kind='runtime_input'
		AND payload_json::jsonb ->> 'runtime_input_id'='task_notification:task_execution'`); err != nil {
		t.Fatalf("configure final task-notification attempt: %v", err)
	}

	sender := &runtimeInputExhaustionSender{}
	deliveryStore := agentruntimebridge.NewPostgreSQLRuntimeDeliveryStore(client, 9090)
	runner := &agentruntimebridge.JobRunner{
		Queue:      tetralqueue.NewServer(queue.NewPostgreSQLStore(client), nil),
		Workspaces: runtimeInputExhaustionWorkspaceLister{},
		Deliverer:  runtimeInputExhaustionDeliverer{direct: agentruntimebridge.RuntimePodDirectDeliverer{Store: deliveryStore, Sender: sender}},
		Config:     agentruntimebridge.JobRunnerConfig{MaxJobs: 1, LeaseDuration: time.Minute, HeartbeatInterval: time.Hour},
	}
	if err := runner.RunOnce(context.Background()); err != nil {
		t.Fatalf("run final task-notification attempt: %v", err)
	}
	var inboxStatus, queueStatus, errorKind string
	var terminalEventID sql.NullString
	var sessionErrors int
	if err := adminDB.QueryRowContext(context.Background(), `SELECT
		(SELECT status FROM session_runtime_inbox WHERE workspace_id='ws_execution_store' AND runtime_input_id='task_notification:task_execution'),
		(SELECT status FROM queue_jobs WHERE workspace_id='ws_execution_store' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'runtime_input_id'='task_notification:task_execution'),
		(SELECT last_error_kind FROM queue_jobs WHERE workspace_id='ws_execution_store' AND kind='runtime_input'
		  AND payload_json::jsonb ->> 'runtime_input_id'='task_notification:task_execution'),
		(SELECT terminal_event_id FROM session_background_tasks WHERE workspace_id='ws_execution_store' AND task_id='task_execution'),
		(SELECT count(*) FROM session_events WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store' AND type='session.error')`).Scan(
		&inboxStatus, &queueStatus, &errorKind, &terminalEventID, &sessionErrors,
	); err != nil {
		t.Fatalf("read task-notification exhaustion settlement: %v", err)
	}
	if inboxStatus != "dead_lettered" || queueStatus != queue.StatusDeadLettered || errorKind != "runtime_delivery_exhausted" ||
		terminalEventID.Valid || sessionErrors != 1 || sender.calls != 0 {
		t.Fatalf("task notification settlement = Inbox %s Queue %s/%s terminal event %v errors %d Runtime calls %d", inboxStatus, queueStatus, errorKind, terminalEventID, sessionErrors, sender.calls)
	}
	var pending int
	if err := adminDB.QueryRowContext(context.Background(), `SELECT count(*) FROM session_runtime_inbox
		WHERE workspace_id='ws_execution_store' AND session_id='sesn_execution_store'
		AND status IN ('queued','delivering','accepted','parked')`).Scan(&pending); err != nil {
		t.Fatalf("count live task-notification custody: %v", err)
	}
	if pending != 0 {
		t.Fatalf("live task-notification custody after exhaustion = %d; want zero", pending)
	}
}
