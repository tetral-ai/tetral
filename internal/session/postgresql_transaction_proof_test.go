package session

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/tetral-ai/tetral/internal/agent"
	"github.com/tetral-ai/tetral/internal/dbconnect"
	"github.com/tetral-ai/tetral/internal/files"
	"github.com/tetral-ai/tetral/internal/queue"
	"github.com/tetral-ai/tetral/internal/storage/storagetest"
	"github.com/tetral-ai/tetral/internal/workspace"
)

func TestPostgreSQLSessionCreateAdmitsPreparationAndSessionPrepareJobForReadyArtifact(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_session_create_source", 5)
	service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
	service.sessionIDStrategy = fixedSessionIDs("sesn_prepare_ready")
	service.threadIDStrategy = fixedSessionIDs("thread_prepare_ready")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_prepare_ready")
	service.fileIDStrategy = fixedSessionIDs("file_prepare_ready")
	mountPath := "/workspace/input.txt"

	created, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:      string(ResourceTypeFile),
			FileID:    "file_session_create_source",
			MountPath: &mountPath,
		}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID != "sesn_prepare_ready" || created.Status != StatusIdle {
		t.Fatalf("created session = %+v; want admitted idle session", created)
	}
	assertMainThreadPinned(t, env.admin, workspace.DefaultID, "sesn_prepare_ready", "thread_prepare_ready")
	assertSessionRuntimeStatus(t, env.admin, workspace.DefaultID, "sesn_prepare_ready", "idle", "2026-05-11T12:00:00Z")
	preparation := loadSessionPreparation(t, env.admin, workspace.DefaultID, "sesn_prepare_ready")
	if preparation.status != "pending" || preparation.environmentGeneration != 1 || preparation.environmentID != "env_test" {
		t.Fatalf("preparation = %+v; want pending env_test generation 1", preparation)
	}
	if !strings.HasPrefix(preparation.preparationAttemptID, "prep_") || !strings.HasPrefix(preparation.sandboxID, "sandbox_") {
		t.Fatalf("preparation identities = %+v; want prep_/sandbox_ identities", preparation)
	}
	job := loadQueueJobByDedupe(t, env.admin, workspace.DefaultID, queue.FormatSessionPrepareDedupeKey(workspace.DefaultID, "sesn_prepare_ready", preparation.preparationAttemptID))
	if job.kind != queue.KindSessionPrepare || job.partitionKey != queue.FormatSessionPartitionKey(workspace.DefaultID, "sesn_prepare_ready") || job.status != queue.StatusPending {
		t.Fatalf("queue job = %+v; want pending canonical session_prepare", job)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode queue payload: %v", err)
	}
	if payload["workspace_id"] != string(workspace.DefaultID) || payload["session_id"] != "sesn_prepare_ready" || payload["preparation_attempt_id"] != preparation.preparationAttemptID {
		t.Fatalf("queue payload = %#v; want preparation identity", payload)
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM sandboxes WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), "sesn_prepare_ready") != 0 {
		t.Fatal("session create allocated a sandbox row; sandbox allocation belongs to Sandbox Service session_prepare")
	}
}

func TestPostgreSQLSessionCreateWaitsForEnvironmentArtifactBeforeSessionPrepare(t *testing.T) {
	for _, tc := range []struct {
		name           string
		artifactStatus string
	}{
		{name: "pending_artifact", artifactStatus: "pending"},
		{name: "building_artifact", artifactStatus: "building"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newSessionPostgreSQLProofEnv(t)
			if tc.artifactStatus != "" {
				seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, tc.artifactStatus)
			}
			service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
			sessionID := "sesn_" + tc.name
			service.sessionIDStrategy = fixedSessionIDs(sessionID)
			service.threadIDStrategy = fixedSessionIDs("thread_" + tc.name)

			if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
			}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			preparation := loadSessionPreparation(t, env.admin, workspace.DefaultID, sessionID)
			if preparation.status != "waiting_environment" {
				t.Fatalf("preparation status = %q; want waiting_environment", preparation.status)
			}
			if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2`, string(workspace.DefaultID), queue.KindSessionPrepare); got != 0 {
				t.Fatalf("session_prepare jobs = %d; want none until environment_ready_fanout", got)
			}
		})
	}
}

func TestPostgreSQLSessionCreateRejectsMissingCurrentEnvironmentArtifactAtomically(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
	service.sessionIDStrategy = fixedSessionIDs("sesn_missing_artifact")
	service.threadIDStrategy = fixedSessionIDs("thread_missing_artifact")

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	})
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Create error = %v; want validation error", err)
	}
	for _, table := range []string{"sessions", "session_threads", "session_preparations", "queue_jobs"} {
		if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM `+table+` WHERE workspace_id = $1`, string(workspace.DefaultID)); got != 0 {
			t.Fatalf("%s rows = %d; want transaction rollback", table, got)
		}
	}
}

func TestPostgreSQLSessionCreateRejectsFailedEnvironmentArtifact(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "failed")
	service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
	service.sessionIDStrategy = fixedSessionIDs("sesn_failed_artifact")
	service.threadIDStrategy = fixedSessionIDs("thread_failed_artifact")

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	})
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("Create err = %T %v; want ValidationError", err, err)
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM sessions WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sesn_failed_artifact") != 0 {
		t.Fatal("session row persisted after failed environment artifact rejection")
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM session_preparations WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), "sesn_failed_artifact") != 0 {
		t.Fatal("preparation row persisted after failed environment artifact rejection")
	}
}

func TestPostgreSQLSessionCreateRollsBackAfterSessionFileIdentityInsert(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_session_rollback_a", 5)
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_session_rollback_b", 6)
	service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
	service.sessionIDStrategy = fixedSessionIDs("sesn_session_rollback")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_session_duplicate", "sesrsc_session_duplicate")
	service.fileIDStrategy = fixedSessionIDs("file_session_rollback_created_a", "file_session_rollback_created_b")
	firstMount := "/workspace/first.txt"
	secondMount := "/workspace/second.txt"
	beforeObjects := sessionRowCount(t, env.admin, `SELECT count(*) FROM file_objects WHERE workspace_id = $1`, string(workspace.DefaultID))

	_, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{
			{Type: string(ResourceTypeFile), FileID: "file_session_rollback_a", MountPath: &firstMount},
			{Type: string(ResourceTypeFile), FileID: "file_session_rollback_b", MountPath: &secondMount},
		},
	})
	if err == nil {
		t.Fatal("Create succeeded despite duplicate resource id")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("Create error = %T %v; want session conflict from resource persistence", err, err)
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM sessions WHERE workspace_id = $1 AND id = $2`, string(workspace.DefaultID), "sesn_session_rollback") != 0 {
		t.Fatal("session row persisted after duplicate resource rollback")
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM session_preparations WHERE workspace_id = $1 AND session_id = $2`, string(workspace.DefaultID), "sesn_session_rollback") != 0 {
		t.Fatal("preparation row persisted after duplicate resource rollback")
	}
	if sessionRowCount(t, env.admin, `SELECT count(*) FROM files WHERE workspace_id = $1 AND scope_id = $2`, string(workspace.DefaultID), "sesn_session_rollback") != 0 {
		t.Fatal("session file identity persisted after duplicate resource rollback")
	}
	if afterObjects := sessionRowCount(t, env.admin, `SELECT count(*) FROM file_objects WHERE workspace_id = $1`, string(workspace.DefaultID)); afterObjects != beforeObjects {
		t.Fatalf("file object count = %d; want unchanged %d", afterObjects, beforeObjects)
	}
}

func TestPostgreSQLSessionActiveFileResourcesRequireSessionScopedIdentity(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	seedSessionSourceFile(t, env.admin, workspace.DefaultID, "file_identity_source", 5)
	service := env.newService(time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC))
	service.sessionIDStrategy = fixedSessionIDs("sesn_identity_scope")
	service.threadIDStrategy = fixedSessionIDs("thread_identity_scope")
	service.resourceIDStrategy = fixedSessionIDs("sesrsc_identity_scope")
	service.fileIDStrategy = fixedSessionIDs("file_identity_session")

	created, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
		Resources: []ResourceRequest{{
			Type:   string(ResourceTypeFile),
			FileID: "file_identity_source",
		}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(created.Resources) != 1 {
		t.Fatalf("created resources = %+v; want visible file resource", created.Resources)
	}
	if _, err := env.admin.ExecContext(context.Background(),
		`UPDATE files SET deleted_at = $1 WHERE workspace_id = $2 AND file_id = $3`,
		"2026-05-11T12:30:00Z", string(workspace.DefaultID), "file_identity_session"); err != nil {
		t.Fatalf("tombstone session-scoped identity: %v", err)
	}
	reloaded, err := service.Get(context.Background(), workspace.DefaultID, created.ID)
	if err != nil {
		t.Fatalf("Get after tombstone: %v", err)
	}
	if len(reloaded.Resources) != 0 {
		t.Fatalf("visible resources after session identity tombstone = %+v; want none", reloaded.Resources)
	}
}

func TestPostgreSQLSessionUpdateApprovalModeAdvancesConfigGenerationAndQueuesPatch(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	createTime := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	service := env.newService(createTime)
	service.sessionIDStrategy = fixedSessionIDs("sesn_approval_update")
	service.threadIDStrategy = fixedSessionIDs("thread_approval_update")

	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	service = env.newService(createTime.Add(time.Minute))
	if _, err := service.Update(context.Background(), workspace.DefaultID, "sesn_approval_update", UpdateRequest{
		ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
	}); err != nil {
		t.Fatalf("Update approval_mode: %v", err)
	}

	var approvalMode string
	var configGeneration int64
	if err := env.admin.QueryRowContext(context.Background(),
		`SELECT approval_mode, config_generation
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		"sesn_approval_update",
	).Scan(&approvalMode, &configGeneration); err != nil {
		t.Fatalf("load session runtime config: %v", err)
	}
	if approvalMode != "full_access" || configGeneration != 2 {
		t.Fatalf("session runtime config = mode %q generation %d; want full_access generation 2", approvalMode, configGeneration)
	}
	job := loadQueueJobByDedupe(t, env.admin, workspace.DefaultID, queue.FormatRuntimeConfigUpdateDedupeKey(workspace.DefaultID, "sesn_approval_update", "2"))
	if job.kind != queue.KindRuntimeConfigUpdate || job.partitionKey != queue.FormatSessionPartitionKey(workspace.DefaultID, "sesn_approval_update") || job.status != queue.StatusPending {
		t.Fatalf("runtime config job = %+v; want pending runtime_config_update in session partition", job)
	}
	var payload struct {
		WorkspaceID      string `json:"workspace_id"`
		SessionID        string `json:"session_id"`
		ConfigGeneration int64  `json:"config_generation"`
	}
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime config payload: %v", err)
	}
	if payload.WorkspaceID != string(workspace.DefaultID) || payload.SessionID != "sesn_approval_update" || payload.ConfigGeneration != 2 {
		t.Fatalf("runtime config payload = %+v; want generation references", payload)
	}
	assertJSONKeys(t, job.payloadJSON, "config_generation", "session_id", "workspace_id")
}

func TestPostgreSQLSessionUpdateAgentToolsWritesInstalledConfigAndRuntimePatch(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	createTime := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	service := env.newService(createTime)
	service.sessionIDStrategy = fixedSessionIDs("sesn_agent_config_update")
	service.threadIDStrategy = fixedSessionIDs("thread_agent_config_update")

	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	service = env.newService(createTime.Add(time.Minute))
	tools := agent.RawArray{
		json.RawMessage(`{"type":"tetral_agent_toolset","family":"claude"}`),
		json.RawMessage(`{"type":"mcp_toolset","mcp_server_name":"github","default_config":{"enabled":false,"permission_policy":{"type":"always_ask"}},"configs":[{"name":"github_search","enabled":true,"permission_policy":{"type":"always_allow"}}]}`),
	}
	mcpServers := agent.RawArray{json.RawMessage(`{"type":"url","name":"github","url":"https://api.githubcopilot.com/mcp"}`)}
	response, err := service.Update(context.Background(), workspace.DefaultID, "sesn_agent_config_update", UpdateRequest{
		ToolsPatch:      &tools,
		MCPServersPatch: &mcpServers,
	})
	if err != nil {
		t.Fatalf("Update agent tools: %v", err)
	}
	if len(response.Agent.MCPServers) != 1 || !strings.Contains(string(response.Agent.MCPServers[0]), `"https://api.githubcopilot.com/mcp/"`) {
		t.Fatalf("response mcp_servers = %s; want canonical catalog URL", string(mustMarshalJSONForSessionProof(t, response.Agent.MCPServers)))
	}

	var configGeneration int64
	var installedToolsJSON string
	if err := env.admin.QueryRowContext(context.Background(),
		`SELECT config_generation, installed_tools_json
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		"sesn_agent_config_update",
	).Scan(&configGeneration, &installedToolsJSON); err != nil {
		t.Fatalf("load session installed runtime config: %v", err)
	}
	if configGeneration != 2 || !strings.Contains(installedToolsJSON, `"mcp_toolset"`) || !strings.Contains(installedToolsJSON, `"mcp_servers"`) {
		t.Fatalf("installed runtime config = generation %d json %s; want generation 2 mcp config", configGeneration, installedToolsJSON)
	}
	job := loadQueueJobByDedupe(t, env.admin, workspace.DefaultID, queue.FormatRuntimeConfigUpdateDedupeKey(workspace.DefaultID, "sesn_agent_config_update", "2"))
	var payload struct {
		WorkspaceID      string `json:"workspace_id"`
		SessionID        string `json:"session_id"`
		ConfigGeneration int64  `json:"config_generation"`
	}
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		t.Fatalf("decode runtime config payload: %v", err)
	}
	if payload.WorkspaceID != string(workspace.DefaultID) ||
		payload.SessionID != "sesn_agent_config_update" ||
		payload.ConfigGeneration != 2 {
		t.Fatalf("runtime config payload = %+v; want generation references", payload)
	}
	assertJSONKeys(t, job.payloadJSON, "config_generation", "session_id", "workspace_id")
}

func TestPostgreSQLSessionUpdateApprovalModeRejectsActiveRuntimeQueueWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind string
	}{
		{name: "runtime_input", kind: queue.KindRuntimeInput},
		{name: "runtime_config_update", kind: queue.KindRuntimeConfigUpdate},
		{name: "cleanup_session", kind: queue.KindCleanupSession},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newSessionPostgreSQLProofEnv(t)
			seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
			createTime := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
			service := env.newService(createTime)
			sessionID := "sesn_approval_block_" + tc.name
			service.sessionIDStrategy = fixedSessionIDs(sessionID)
			service.threadIDStrategy = fixedSessionIDs("thread_approval_block_" + tc.name)
			if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
			}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			seedActiveRuntimeQueueJob(t, env.admin, workspace.DefaultID, sessionID, tc.kind, tc.name)

			_, err := service.Update(context.Background(), workspace.DefaultID, sessionID, UpdateRequest{
				ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
			})
			if err == nil {
				t.Fatal("Update approval_mode succeeded; want conflict")
			}
			var conflict *ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("err = %T %v; want ConflictError", err, err)
			}
			if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2 AND payload_json LIKE '%"approval_mode"%'`, string(workspace.DefaultID), queue.KindRuntimeConfigUpdate); got != 0 {
				t.Fatalf("runtime config update jobs = %d; want none after conflict", got)
			}
		})
	}
}

func TestPostgreSQLSessionUpdateApprovalModeRejectsUnsettledRuntimeInbox(t *testing.T) {
	for _, inboxStatus := range []string{"accepted", "delivering", "parked"} {
		t.Run(inboxStatus, func(t *testing.T) {
			env := newSessionPostgreSQLProofEnv(t)
			seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
			createTime := time.Date(2026, 5, 11, 12, 30, 0, 0, time.UTC)
			service := env.newService(createTime)
			sessionID := "sesn_approval_inbox_" + inboxStatus
			threadID := "thread_approval_inbox_" + inboxStatus
			service.sessionIDStrategy = fixedSessionIDs(sessionID)
			service.threadIDStrategy = fixedSessionIDs(threadID)
			if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
				Agent:         AgentReference{ID: "agent_test"},
				EnvironmentID: "env_test",
			}); err != nil {
				t.Fatalf("Create: %v", err)
			}
			now := createTime.Add(time.Minute)
			if _, err := env.admin.ExecContext(context.Background(),
				`INSERT INTO session_runtime_inbox (
					workspace_id, session_id, session_thread_id, runtime_input_id, input_kind,
					event_ids_json, sequence_from, sequence_to, status, binding_id,
					binding_generation, target_pod_uid, created_at, updated_at
				) VALUES ($1, $2, $3, $4, 'messages', '[]', 1, 1, $5, $7, 1, $8, $6, $6)`,
				string(workspace.DefaultID),
				sessionID,
				threadID,
				"rin_approval_inbox_"+inboxStatus,
				inboxStatus,
				now,
				"bind_approval_inbox_"+inboxStatus,
				"pod_approval_inbox_"+inboxStatus,
			); err != nil {
				t.Fatalf("seed %s runtime inbox: %v", inboxStatus, err)
			}

			_, err := service.Update(context.Background(), workspace.DefaultID, sessionID, UpdateRequest{
				ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
			})
			var conflict *ConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("Update with %s inbox err = %T %v; want ConflictError", inboxStatus, err, err)
			}
		})
	}
}

func TestPostgreSQLSessionUpdateApprovalModeRejectsMissingRuntimeStatus(t *testing.T) {
	env := newSessionPostgreSQLProofEnv(t)
	seedEnvironmentArtifact(t, env.admin, workspace.DefaultID, "env_test", 1, "ready")
	createTime := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
	service := env.newService(createTime)
	service.sessionIDStrategy = fixedSessionIDs("sesn_approval_missing_status")
	service.threadIDStrategy = fixedSessionIDs("thread_approval_missing_status")
	if _, err := service.Create(context.Background(), workspace.DefaultID, CreateRequest{
		Agent:         AgentReference{ID: "agent_test"},
		EnvironmentID: "env_test",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := env.admin.ExecContext(context.Background(),
		`DELETE FROM session_runtime_status WHERE workspace_id = $1 AND session_id = $2`,
		string(workspace.DefaultID),
		"sesn_approval_missing_status",
	); err != nil {
		t.Fatalf("delete runtime status: %v", err)
	}

	_, err := env.newService(createTime.Add(time.Minute)).Update(context.Background(), workspace.DefaultID, "sesn_approval_missing_status", UpdateRequest{
		ApprovalMode: approvalModePtr(ApprovalModeFullAccess),
	})
	if err == nil {
		t.Fatal("Update approval_mode succeeded; want conflict")
	}
	var conflict *ConflictError
	if !errors.As(err, &conflict) || !conflict.InvalidRequest {
		t.Fatalf("Update approval_mode err = %T %v; want invalid_request conflict", err, err)
	}
	var approvalMode string
	var configGeneration int64
	if err := env.admin.QueryRowContext(context.Background(),
		`SELECT approval_mode, config_generation
		   FROM sessions
		  WHERE workspace_id = $1 AND id = $2`,
		string(workspace.DefaultID),
		"sesn_approval_missing_status",
	).Scan(&approvalMode, &configGeneration); err != nil {
		t.Fatalf("load session runtime config: %v", err)
	}
	if approvalMode != "ask_for_approval" || configGeneration != 1 {
		t.Fatalf("session runtime config = mode %q generation %d; want unchanged ask_for_approval generation 1", approvalMode, configGeneration)
	}
	if got := sessionRowCount(t, env.admin, `SELECT count(*) FROM queue_jobs WHERE workspace_id = $1 AND kind = $2 AND payload_json LIKE '%"approval_mode"%'`, string(workspace.DefaultID), queue.KindRuntimeConfigUpdate); got != 0 {
		t.Fatalf("runtime config update jobs = %d; want none after missing runtime status conflict", got)
	}
}

type sessionPostgreSQLProofEnv struct {
	runtime *sql.DB
	admin   *sql.DB
}

func newSessionPostgreSQLProofEnv(t *testing.T) *sessionPostgreSQLProofEnv {
	t.Helper()
	if os.Getenv(storagetest.EnvTestDatabaseURL) == "" {
		t.Skip(storagetest.EnvTestDatabaseURL + " is not set")
	}
	runtime, admin := storagetest.NewPostgreSQLDBWithAdmin(t)
	seedSessionReferences(t, admin, workspace.DefaultID)
	return &sessionPostgreSQLProofEnv{runtime: runtime, admin: admin}
}

func (e *sessionPostgreSQLProofEnv) newService(now time.Time) *Service {
	runtimeClient := dbconnect.NewClientForTesting(e.runtime)
	fileStore := files.NewPostgreSQLStore(runtimeClient, nil)
	sessionStore := NewPostgreSQLSessionStore(
		runtimeClient,
		WithPageTokenSecret([]byte("session-proof-secret-12345")),
	)
	return NewService(
		testAgents{},
		testEnvironments{},
		files.NewService(fileStore),
		testMemories{},
		&recordingVaultValidator{},
		sessionStore,
		testSessionEncryptor{},
		WithClock(func() time.Time { return now }),
	)
}

func seedSessionReferences(t *testing.T, db *sql.DB, workspaceID workspace.ID) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO workspaces (id, type, name, created_at)
		 VALUES ($1, 'workspace', $1, '2026-01-01T00:00:00Z')
		 ON CONFLICT DO NOTHING`,
		string(workspaceID),
	); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agents (workspace_id, id, name, version, created_at, updated_at)
		 VALUES ($1, 'agent_test', 'Session Proof Agent', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID),
	); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO agent_versions (workspace_id, id, agent_id, version, config_json, config_hash, created_at)
		 VALUES ($1, 'agv_session_proof_1', 'agent_test', 1, '{"name":"Session Proof Agent","model":"anthropic/claude-opus-4-8"}', 'hash-session-proof-1', '2026-01-01T00:00:00Z')
		 ON CONFLICT (agent_id, version) DO NOTHING`,
		string(workspaceID),
	); err != nil {
		t.Fatalf("seed agent version: %v", err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environments (workspace_id, id, name, config_json, current_generation, created_at, updated_at)
		 VALUES ($1, 'env_test', 'Session Proof Environment', '{}', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		 ON CONFLICT (id) DO NOTHING`,
		string(workspaceID),
	); err != nil {
		t.Fatalf("seed environment: %v", err)
	}
}

func seedEnvironmentArtifact(t *testing.T, db *sql.DB, workspaceID workspace.ID, environmentID string, generation int64, status string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO environment_artifacts (
			workspace_id, environment_id, generation, status, provider,
			provider_artifact_ref, normalized_config_hash, artifact_input_hash,
			runtime_network_policy_json, packages_json, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'daytona', $5, 'hash-config', 'hash-artifact', '{"type":"unrestricted"}', '{}', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
		ON CONFLICT (workspace_id, environment_id, generation) DO UPDATE SET
			status = EXCLUDED.status,
			provider_artifact_ref = EXCLUDED.provider_artifact_ref,
			updated_at = EXCLUDED.updated_at`,
		string(workspaceID), environmentID, generation, status, "artifact_"+environmentID,
	); err != nil {
		t.Fatalf("seed environment artifact: %v", err)
	}
}

func seedSessionSourceFile(t *testing.T, db *sql.DB, workspaceID workspace.ID, fileID string, sizeBytes int64) {
	t.Helper()
	objectID := "fobj_" + fileID
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO file_objects (workspace_id, object_id, blob_key, size_bytes, sha256, created_at)
		 VALUES ($1, $2, $3, $4, 'sha', '2026-01-01T00:00:00Z')`,
		string(workspaceID), objectID, "files/"+string(workspaceID)+"/"+objectID, sizeBytes,
	); err != nil {
		t.Fatalf("seed file object %s: %v", objectID, err)
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO files (workspace_id, file_id, object_id, filename, mime_type, downloadable, created_at)
		 VALUES ($1, $2, $3, $4, 'text/plain', false, '2026-01-01T00:00:00Z')`,
		string(workspaceID), fileID, objectID, fileID+".txt",
	); err != nil {
		t.Fatalf("seed source file %s: %v", fileID, err)
	}
}

func fixedSessionIDs(values ...string) func() string {
	index := 0
	return func() string {
		if index >= len(values) {
			return values[len(values)-1]
		}
		value := values[index]
		index++
		return value
	}
}

func mustMarshalJSONForSessionProof(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	return encoded
}

func sessionRowCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	return count
}

func assertMainThreadPinned(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, threadID string) {
	t.Helper()
	var mainThreadID string
	if err := db.QueryRowContext(context.Background(),
		`SELECT main_thread_id FROM sessions WHERE workspace_id = $1 AND id = $2`,
		string(workspaceID), sessionID,
	).Scan(&mainThreadID); err != nil {
		t.Fatalf("load main_thread_id: %v", err)
	}
	if mainThreadID != threadID {
		t.Fatalf("main_thread_id = %q; want %q", mainThreadID, threadID)
	}
	if got := sessionRowCount(t, db, `SELECT count(*) FROM session_threads WHERE workspace_id = $1 AND session_id = $2 AND id = $3`, string(workspaceID), sessionID, threadID); got != 1 {
		t.Fatalf("primary thread rows = %d; want 1", got)
	}
}

func assertSessionRuntimeStatus(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, wantStatus string, wantIdleSince string) {
	t.Helper()
	var status string
	var idleSince sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT status, idle_since
		   FROM session_runtime_status
		  WHERE workspace_id = $1 AND session_id = $2`,
		string(workspaceID), sessionID,
	).Scan(&status, &idleSince); err != nil {
		t.Fatalf("load session_runtime_status: %v", err)
	}
	if status != wantStatus {
		t.Fatalf("runtime status = %q; want %q", status, wantStatus)
	}
	if wantIdleSince == "" {
		if idleSince.Valid {
			t.Fatalf("idle_since = %q; want NULL", idleSince.String)
		}
		return
	}
	if !idleSince.Valid || idleSince.String != wantIdleSince {
		t.Fatalf("idle_since = %v; want %q", idleSince, wantIdleSince)
	}
}

func seedActiveRuntimeQueueJob(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, kind string, suffix string) {
	t.Helper()
	now := "2026-05-11T12:30:00Z"
	payload := map[string]any{
		"workspace_id": string(workspaceID),
		"session_id":   sessionID,
	}
	dedupeKey := kind + ":" + string(workspaceID) + ":" + sessionID + ":" + suffix
	switch kind {
	case queue.KindRuntimeInput:
		payload["session_thread_id"] = "thread_" + suffix
		payload["runtime_input_id"] = "rin_" + suffix
		payload["event_ids"] = []string{"sevt_" + suffix}
		payload["sequence_from"] = 1
		payload["sequence_to"] = 1
		payload["input_kind"] = "messages"
		payload["preparation_attempt_id"] = "prep_" + suffix
		dedupeKey = queue.FormatRuntimeInputDedupeKey(workspaceID, sessionID, "rin_"+suffix)
	case queue.KindRuntimeConfigUpdate:
		payload["config_generation"] = 7
		dedupeKey = queue.FormatRuntimeConfigUpdateDedupeKey(workspaceID, sessionID, "7")
	case queue.KindCleanupSession:
		payload["cleanup_job_id"] = "cleanup_" + suffix
		dedupeKey = queue.FormatCleanupSessionDedupeKey(workspaceID, sessionID, "cleanup_"+suffix)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal active queue payload: %v", err)
	}
	availableAt, err := time.Parse(time.RFC3339, now)
	if err != nil {
		t.Fatalf("parse active queue time: %v", err)
	}
	store := queue.NewPostgreSQLStore(dbconnect.NewClientForTesting(db))
	if _, err := store.Enqueue(context.Background(), queue.EnqueueRequest{
		ID:             "qjob_active",
		WorkspaceID:    workspaceID,
		Kind:           kind,
		PartitionKey:   queue.FormatSessionPartitionKey(workspaceID, sessionID),
		DedupeKey:      dedupeKey,
		PayloadVersion: 1,
		PayloadJSON:    payloadJSON,
		MaxAttempts:    10,
		Now:            availableAt,
	}); err != nil {
		t.Fatalf("seed active queue job: %v", err)
	}
}

type preparationRow struct {
	preparationAttemptID  string
	environmentID         string
	environmentGeneration int64
	sandboxID             string
	status                string
}

func loadSessionPreparation(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) preparationRow {
	t.Helper()
	var row preparationRow
	if err := db.QueryRowContext(context.Background(),
		`SELECT preparation_attempt_id, environment_id, environment_generation, sandbox_id, status
		   FROM session_preparations
			  WHERE workspace_id = $1 AND session_id = $2
			    AND superseded_at IS NULL
			  ORDER BY created_at DESC, preparation_attempt_id DESC
			  LIMIT 1`,
		string(workspaceID), sessionID,
	).Scan(&row.preparationAttemptID, &row.environmentID, &row.environmentGeneration, &row.sandboxID, &row.status); err != nil {
		t.Fatalf("load session preparation: %v", err)
	}
	return row
}

func markSessionPreparationStatus(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string, status string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_preparations
		    SET status = $4,
		        resource_cred_expires_at = '2026-05-11T13:00:00Z',
		        resource_roots_json = '[{"path":"/mnt/session/uploads/file","mode":"read"}]',
		        ready_at = CASE WHEN $4 = 'ready' THEN '2026-05-11T12:05:00Z' ELSE ready_at END,
		        updated_at = '2026-05-11T12:05:00Z'
		  WHERE workspace_id = $1 AND session_id = $2 AND preparation_attempt_id = $3`,
		string(workspaceID), sessionID, preparationAttemptID, status); err != nil {
		t.Fatalf("mark preparation %s: %v", status, err)
	}
}

func ackSessionPrepareJobs(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE queue_jobs
		    SET status = 'acknowledged',
		        updated_at = '2026-05-11T12:06:00Z'
		  WHERE workspace_id = $1
		    AND dedupe_key = $2
		    AND status IN ('pending', 'leased')`,
		string(workspaceID), queue.FormatSessionPrepareDedupeKey(workspaceID, sessionID, preparationAttemptID)); err != nil {
		t.Fatalf("ack session_prepare jobs: %v", err)
	}
}

func assertPendingSessionPrepareJob(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string) {
	t.Helper()
	if got := sessionRowCount(t, db,
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND partition_key = $3
		    AND dedupe_key = $4
		    AND status = 'pending'`,
		string(workspaceID),
		queue.KindSessionPrepare,
		queue.FormatSessionPartitionKey(workspaceID, sessionID),
		queue.FormatSessionPrepareDedupeKey(workspaceID, sessionID, preparationAttemptID),
	); got != 1 {
		t.Fatalf("pending session_prepare jobs = %d; want 1", got)
	}
}

func assertSessionPreparationSuperseded(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string) {
	t.Helper()
	var supersededAt sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT superseded_at
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		string(workspaceID),
		sessionID,
		preparationAttemptID,
	).Scan(&supersededAt); err != nil {
		t.Fatalf("read preparation superseded_at: %v", err)
	}
	if !supersededAt.Valid || supersededAt.String == "" {
		t.Fatalf("preparation %s superseded_at = %v; want non-null", preparationAttemptID, supersededAt)
	}
	if got := sessionRowCount(t, db,
		`SELECT count(*)
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND superseded_at IS NULL
		    AND status IN ('pending', 'waiting_environment', 'preparing', 'ready')`,
		string(workspaceID),
		sessionID,
	); got != 1 {
		t.Fatalf("active preparation attempts = %d; want exactly 1", got)
	}
}

func assertSessionPreparationClearedResourceProjectionState(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string) {
	t.Helper()
	assertSessionPreparationResourceProjectionState(t, db, workspaceID, sessionID, preparationAttemptID, "")
}

func assertSessionPreparationResourceProjectionState(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string, preparationAttemptID string, wantExpiresAt string) {
	t.Helper()
	var expiresAt sql.NullString
	var rootsJSON sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT resource_cred_expires_at, resource_roots_json
		   FROM session_preparations
		  WHERE workspace_id = $1
		    AND session_id = $2
		    AND preparation_attempt_id = $3`,
		string(workspaceID),
		sessionID,
		preparationAttemptID,
	).Scan(&expiresAt, &rootsJSON); err != nil {
		t.Fatalf("read preparation resource projection state: %v", err)
	}
	if wantExpiresAt == "" {
		if expiresAt.Valid {
			t.Fatalf("resource_cred_expires_at = %v; want cleared after resource mutation", expiresAt)
		}
	} else if !expiresAt.Valid || expiresAt.String != wantExpiresAt {
		t.Fatalf("resource_cred_expires_at = %v; want %q", expiresAt, wantExpiresAt)
	}
	if rootsJSON.Valid {
		t.Fatalf("resource_roots_json = %v; want cleared after resource mutation", rootsJSON)
	}
}

func sessionPrepareJobCount(t *testing.T, db *sql.DB, workspaceID workspace.ID, sessionID string) int {
	t.Helper()
	return sessionRowCount(t, db,
		`SELECT count(*)
		   FROM queue_jobs
		  WHERE workspace_id = $1
		    AND kind = $2
		    AND partition_key = $3`,
		string(workspaceID),
		queue.KindSessionPrepare,
		queue.FormatSessionPartitionKey(workspaceID, sessionID),
	)
}

type queueJobRow struct {
	kind         string
	partitionKey string
	status       string
	payloadJSON  string
}

func assertJSONKeys(t *testing.T, raw string, keys ...string) {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		t.Fatalf("decode JSON object: %v", err)
	}
	if len(object) != len(keys) {
		t.Fatalf("JSON keys = %v; want exactly %v", object, keys)
	}
	for _, key := range keys {
		if _, ok := object[key]; !ok {
			t.Fatalf("JSON object missing key %q: %s", key, raw)
		}
	}
}

func loadQueueJobByDedupe(t *testing.T, db *sql.DB, workspaceID workspace.ID, dedupeKey string) queueJobRow {
	t.Helper()
	var row queueJobRow
	if err := db.QueryRowContext(context.Background(),
		`SELECT kind, partition_key, status, payload_json
		   FROM queue_jobs
		  WHERE workspace_id = $1 AND dedupe_key = $2`,
		string(workspaceID), dedupeKey,
	).Scan(&row.kind, &row.partitionKey, &row.status, &row.payloadJSON); err != nil {
		t.Fatalf("load queue job: %v", err)
	}
	return row
}
